package by.losik.maprouter.route;

import by.losik.maprouter.exception.UnauthorizedException;
import by.losik.maprouter.processor.JwtAuthProcessor;
import by.losik.maprouter.service.RedisService;
import com.fasterxml.jackson.databind.ObjectMapper;
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
                .process(this::processGetResult);

        from("servlet:/v1/route?httpMethodRestrict=POST")
                .routeId("user-route")
                .process(jwtAuthProcessor)
                .process(this::prepareRequest)
                .choice()
                .when(exchange -> exchange.getProperty("skipTranscribe") == null)
                .to("aws2-s3://%s?deleteAfterWrite=false".formatted(bucketName))
                .process(this::startTranscriptionJob)
                .to("aws2-transcribe://startTranscriptionJob")
                .to("aws2-transcribe://getTranscriptionJob")
                .process(this::sendToSqs)
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
        Map<String, Object> request = objectMapper.readValue(body, Map.class);

        String userId = exchange.getProperty(USER_ID, String.class);
        String requestId = redisService.generateRequestId();

        Object textObj = request.get("text");
        Object voiceObj = request.get("voice");

        String text = textObj != null ? textObj.toString() : null;
        String voice = voiceObj != null ? voiceObj.toString() : null;
        String language = Optional.ofNullable((String) request.get("language")).orElse("ru");

        exchange.setProperty(REQUEST_ID, requestId);
        exchange.setProperty(USER_ID, userId);
        exchange.setProperty(LANGUAGE, language);

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

        if (exchange.getProperty("skipTranscribe") == null) {
            redisService.savePending(requestId);
        }

        String transcriptText = exchange.getIn().getBody(String.class);

        Optional.ofNullable(transcriptText)
                .filter(t -> !t.isEmpty())
                .orElseThrow(() -> new RuntimeException("Empty transcript result"));

        JsonObject sqsMessage = new JsonObject();
        sqsMessage.put("request_id", requestId);
        sqsMessage.put("text", transcriptText);
        sqsMessage.put("user_id", userId);
        sqsMessage.put("language", language);
        exchange.getMessage().setBody(sqsMessage);
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