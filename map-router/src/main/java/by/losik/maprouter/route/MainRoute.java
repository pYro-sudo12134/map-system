package by.losik.maprouter.route;

import by.losik.maprouter.exception.UnauthorizedException;
import by.losik.maprouter.processor.JwtAuthProcessor;
import by.losik.maprouter.service.RedisService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.util.json.JsonObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class MainRoute extends RouteBuilder {

    private final JwtAuthProcessor jwtAuthProcessor;
    private final RedisService redisService;
    private final ObjectMapper objectMapper;

    @Value("${aws.s3.bucket:voice-records}")
    private String bucketName;

    @Value("${aws.sqs.rag.name}")
    private String ragQueueName;

    @Value("${rate.limit.user.global.max-requests:200}")
    private int globalMaxRequests;

    @Value("${rate.limit.user.global.time-period-ms:1000}")
    private int globalTimePeriodMs;

    @Value("${rate.limit.user.get-result.max-requests:100}")
    private int getResultMaxRequests;

    @Value("${rate.limit.user.get-result.time-period-ms:1000}")
    private int getResultTimePeriodMs;

    @Value("${rate.limit.user.per-user.max-requests:20}")
    private int perUserMaxRequests;

    @Value("${rate.limit.user.per-user.time-period-ms:1000}")
    private int perUserTimePeriodMs;

    @Value("${resilience4j.circuitbreaker.instances.transcribe.failure-rate-threshold:50}")
    private float transcribeFailureRateThreshold;

    @Value("${resilience4j.circuitbreaker.instances.transcribe.sliding-window-size:10}")
    private int transcribeSlidingWindowSize;

    @Value("${resilience4j.circuitbreaker.instances.transcribe.minimum-number-of-calls:5}")
    private int transcribeMinimumNumberOfCalls;

    @Value("${resilience4j.circuitbreaker.instances.transcribe.wait-duration-in-open-state:30000}")
    private int transcribeWaitDurationInOpenState;

    @Value("${resilience4j.circuitbreaker.instances.transcribe.permitted-number-of-calls-in-half-open-state:3}")
    private int transcribePermittedNumberOfCallsInHalfOpenState;

    @Value("${resilience4j.circuitbreaker.instances.s3-upload.failure-rate-threshold:40}")
    private float s3FailureRateThreshold;

    @Value("${resilience4j.circuitbreaker.instances.s3-upload.sliding-window-size:10}")
    private int s3SlidingWindowSize;

    @Value("${resilience4j.circuitbreaker.instances.s3-upload.minimum-number-of-calls:5}")
    private int s3MinimumNumberOfCalls;

    @Value("${resilience4j.circuitbreaker.instances.s3-upload.wait-duration-in-open-state:15000}")
    private int s3WaitDurationInOpenState;

    private static final String REQUEST_ID = "requestId";
    private static final String USER_ID = "userId";
    private static final String LANGUAGE = "language";

    @Override
    public void configure() {
        onException(UnauthorizedException.class)
                .handled(true)
                .process(exchange -> {
                    Exception e = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                    log.warn("Unauthorized: {}", e.getMessage());
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 401);
                    exchange.getMessage().setBody(new JsonObject().put("error", e.getMessage()));
                });

        onException(IllegalArgumentException.class)
                .handled(true)
                .process(exchange -> {
                    Exception e = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                    log.warn("Bad request: {}", e.getMessage());
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 400);
                    exchange.getMessage().setBody(new JsonObject().put("error", e.getMessage()));
                });

        onException(CallNotPermittedException.class)
                .handled(true)
                .process(exchange -> {
                    log.error("Circuit breaker is OPEN - request rejected");
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 503);
                    exchange.getMessage().setHeader("Retry-After", 30);
                    exchange.getMessage().setBody(new JsonObject()
                            .put("error", "Service temporarily unavailable"));
                });

        onException(Exception.class)
                .handled(true)
                .process(exchange -> {
                    Exception e = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                    log.error("Internal error: {}", e.getMessage(), e);
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
                    exchange.getMessage().setBody(new JsonObject().put("error", "Internal server error"));
                });

        from("servlet:/v1/result/{requestId}?httpMethodRestrict=GET")
                .routeId("get-result")
                .throttle(getResultMaxRequests)
                .timePeriodMillis(getResultTimePeriodMs)
                .asyncDelayed()
                .process(this::processGetResult);

        from("servlet:/v1/route?httpMethodRestrict=POST")
                .routeId("user-route")
                .throttle(globalMaxRequests)
                .timePeriodMillis(globalTimePeriodMs)
                .asyncDelayed()
                .rejectExecution(true)
                .process(jwtAuthProcessor)
                .process(this::prepareRequest)
                .choice()
                .when(exchange -> exchange.getProperty("skipTranscribe") == null)
                .circuitBreaker()
                .resilience4jConfiguration()
                .failureRateThreshold(s3FailureRateThreshold)
                .slidingWindowSize(s3SlidingWindowSize)
                .minimumNumberOfCalls(s3MinimumNumberOfCalls)
                .waitDurationInOpenState(s3WaitDurationInOpenState)
                .permittedNumberOfCallsInHalfOpenState(2)
                .timeoutEnabled(true)
                .timeoutDuration(10000)
                .end()
                .to("aws2-s3://%s?deleteAfterWrite=false".formatted(bucketName))
                .onFallback()
                .process(exchange -> {
                    String requestId = exchange.getProperty(REQUEST_ID, String.class);
                    log.error("S3 circuit breaker fallback for request: {}", requestId);
                    redisService.saveError(requestId, "S3 upload service temporarily unavailable");
                    exchange.setProperty("s3UploadFailed", true);
                    exchange.setProperty("skipTranscribe", true);

                    String userId = exchange.getProperty(USER_ID, String.class);
                    String language = exchange.getProperty(LANGUAGE, String.class);

                    JsonObject sqsMessage = new JsonObject();
                    sqsMessage.put("request_id", requestId);
                    sqsMessage.put("text", "Audio upload failed, please try text input");
                    sqsMessage.put("user_id", userId);
                    sqsMessage.put("language", language);
                    sqsMessage.put("error", "s3_upload_unavailable");
                    exchange.getMessage().setBody(sqsMessage);
                })
                .end()

                .circuitBreaker()
                .resilience4jConfiguration()
                .failureRateThreshold(transcribeFailureRateThreshold)
                .slidingWindowSize(transcribeSlidingWindowSize)
                .minimumNumberOfCalls(transcribeMinimumNumberOfCalls)
                .waitDurationInOpenState(transcribeWaitDurationInOpenState)
                .permittedNumberOfCallsInHalfOpenState(transcribePermittedNumberOfCallsInHalfOpenState)
                .timeoutEnabled(true)
                .timeoutDuration(60000)
                .end()
                .process(this::startTranscriptionJob)
                .to("aws2-transcribe://startTranscriptionJob")
                .to("aws2-transcribe://getTranscriptionJob")
                .process(this::sendToSqs)
                .onFallback()
                .process(exchange -> {
                    String requestId = exchange.getProperty(REQUEST_ID, String.class);
                    log.error("Transcribe circuit breaker fallback for request: {}", requestId);
                    redisService.saveError(requestId, "Transcription service temporarily unavailable");

                    exchange.setProperty("skipTranscribe", true);
                    String userId = exchange.getProperty(USER_ID, String.class);
                    String language = exchange.getProperty(LANGUAGE, String.class);

                    JsonObject sqsMessage = new JsonObject();
                    sqsMessage.put("request_id", requestId);
                    sqsMessage.put("text", "Transcription temporarily unavailable, please try text input");
                    sqsMessage.put("user_id", userId);
                    sqsMessage.put("language", language);
                    sqsMessage.put("error", "transcription_unavailable");
                    exchange.getMessage().setBody(sqsMessage);
                })
                .endChoice()
                .otherwise()
                .process(this::sendToSqs)
                .end()
                .to("aws2-sqs:%s".formatted(ragQueueName))
                .process(this::sendResponse)
                .marshal().json()
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(202));
    }

    private void processGetResult(@NonNull Exchange exchange) {
        String requestId = exchange.getIn().getHeader("requestId", String.class);

        if (requestId == null) {
            requestId = exchange.getProperty("requestId", String.class);
        }

        if (requestId == null) {
            String path = exchange.getIn().getHeader(Exchange.HTTP_PATH, String.class);
            if (path != null) {
                String[] parts = path.split("/");
                if (parts.length > 0) {
                    requestId = parts[parts.length - 1];
                }
            }
        }

        log.info("Getting result for requestId: {}", requestId);
        Map<String, Object> result = redisService.getResult(requestId);
        log.info("Retrieved result: {}", result);

        Optional.ofNullable(result)
                .ifPresentOrElse(exchangeResult -> {
                    try {
                        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
                        exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
                        String jsonResponse = objectMapper.writeValueAsString(exchangeResult);
                        exchange.getMessage().setBody(jsonResponse);
                    } catch (Exception e) {
                        log.error("Failed to serialize response", e);
                        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
                        exchange.getMessage().setBody("{\"error\":\"Internal server error\"}");
                    }
                }, () -> {
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 404);
                    exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
                    exchange.getMessage().setBody("{\"error\":\"Request not found\"}");
                });
    }

    @SuppressWarnings("unchecked")
    private void prepareRequest(@NonNull Exchange exchange) throws Exception {
        String body = exchange.getIn().getBody(String.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> request = objectMapper.readValue(body, Map.class);

        String userId = exchange.getProperty(USER_ID, String.class);
        String requestId = redisService.generateRequestId();

        String text = (String) request.get("text");
        String voice = (String) request.get("voice");
        String language = (String) request.getOrDefault("language", "ru");

        exchange.setProperty(REQUEST_ID, requestId);
        exchange.setProperty(USER_ID, userId);
        exchange.setProperty(LANGUAGE, language);

        Double lat = exchange.getIn().getHeader("X-Lat", Double.class);
        Double lon = exchange.getIn().getHeader("X-Lon", Double.class);

        Map<String, Double> userLocation = null;
        if (lat != null && lon != null) {
            userLocation = Map.of("lat", lat, "lon", lon);
            exchange.setProperty("userLocation", userLocation);
            log.debug("User location from headers: lat={}, lon={}", lat, lon);
        }

        boolean hasVoice = voice != null && !voice.isEmpty();
        boolean hasText = text != null && !text.isEmpty();

        if (hasVoice && !hasText) {
            byte[] audioData = Base64.getDecoder().decode(voice);
            String s3Key = "temp/%s/audio.wav".formatted(requestId);
            exchange.setProperty("s3Key", s3Key);
            exchange.getMessage().setBody(audioData);
        } else if (hasText && !hasVoice) {
            redisService.savePending(requestId);
            exchange.setProperty("skipTranscribe", true);

            JsonObject sqsMessage = new JsonObject();
            sqsMessage.put("request_id", requestId);
            sqsMessage.put("text", text);
            sqsMessage.put("user_id", userId);
            sqsMessage.put("language", language);

            Optional.ofNullable(userLocation).ifPresent(loc -> {
                JsonObject location = new JsonObject();
                location.put("lat", loc.get("lat"));
                location.put("lon", loc.get("lon"));
                sqsMessage.put("user_location", location);
            });

            exchange.getMessage().setBody(sqsMessage);
        } else {
            throw new IllegalArgumentException("Exactly one of 'text' or 'voice' must be provided, not both or none");
        }
    }

    private void startTranscriptionJob(@NonNull Exchange exchange) {
        String requestId = exchange.getProperty(REQUEST_ID, String.class);
        String s3Key = exchange.getProperty("s3Key", String.class);
        String language = exchange.getProperty(LANGUAGE, String.class);
        String jobName = "transcribe-%s-%d".formatted(requestId, System.currentTimeMillis());

        exchange.getMessage().setHeader("CamelAwsTranscribeTranscriptionJobName", jobName);
        exchange.getMessage().setHeader("CamelAwsTranscribeMediaFormat", "wav");
        exchange.getMessage().setHeader("CamelAwsTranscribeMediaUri", "s3://%s/%s".formatted(bucketName, s3Key));
        exchange.getMessage().setHeader("CamelAwsTranscribeLanguageCode", language);
        exchange.setProperty("jobName", jobName);
    }

    private void sendToSqs(@NonNull Exchange exchange) {
        String requestId = exchange.getProperty(REQUEST_ID, String.class);
        String userId = exchange.getProperty(USER_ID, String.class);
        String language = exchange.getProperty(LANGUAGE, String.class);

        @SuppressWarnings("unchecked")
        Map<String, Double> userLocation = exchange.getProperty("userLocation", Map.class);

        if (exchange.getProperty("skipTranscribe") == null && exchange.getProperty("s3UploadFailed") == null) {
            redisService.savePending(requestId);
        }

        if (exchange.getProperty("skipTranscribe") == null && exchange.getIn().getBody() != null) {
            String transcriptText = exchange.getIn().getBody(String.class);

            if (transcriptText == null || transcriptText.isEmpty()) {
                log.warn("Empty transcript result for request: {}", requestId);
                redisService.saveError(requestId, "Empty transcription result");
                throw new RuntimeException("Empty transcript result");
            }

            JsonObject sqsMessage = new JsonObject();
            sqsMessage.put("request_id", requestId);
            sqsMessage.put("text", transcriptText);
            sqsMessage.put("user_id", userId);
            sqsMessage.put("language", language);

            Optional.ofNullable(userLocation).ifPresent(loc -> {
                JsonObject location = new JsonObject();
                location.put("lat", loc.get("lat"));
                location.put("lon", loc.get("lon"));
                sqsMessage.put("user_location", location);
            });

            exchange.getMessage().setBody(sqsMessage);
        }
    }

    private void sendResponse(@NonNull Exchange exchange) {
        String requestId = exchange.getProperty(REQUEST_ID, String.class);
        Map<String, Object> response = new HashMap<>();
        response.put("request_id", requestId);
        response.put("status", "pending");

        exchange.getMessage().setBody(response);
        exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
    }
}