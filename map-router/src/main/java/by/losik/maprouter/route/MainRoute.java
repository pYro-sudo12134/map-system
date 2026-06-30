package by.losik.maprouter.route;

import by.losik.maprouter.exception.UnauthorizedException;
import by.losik.maprouter.processor.JwtAuthProcessor;
import by.losik.maprouter.service.RedisService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.StringToClassMapItem;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.SchemaProperty;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
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
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
@Tag(name = "Map Router", description = "Main API endpoints for route processing and result retrieval")
@SecurityRequirement(name = "jwtAuth")
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

    @Operation(
            summary = "Get route processing result",
            description = "Retrieve the result of a previously submitted route processing request by ID. " +
                    "Use the request_id returned from the initial POST request.",
            operationId = "getResult",
            tags = {"Map Router"}
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Result retrieved successfully",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(
                                                    key = "request_id",
                                                    value = String.class
                                            ),
                                            @StringToClassMapItem(
                                                    key = "status",
                                                    value = String.class
                                            ),
                                            @StringToClassMapItem(
                                                    key = "result",
                                                    value = Object.class
                                            ),
                                            @StringToClassMapItem(
                                                    key = "error",
                                                    value = String.class
                                            )
                                    }
                            ),
                            examples = {
                                    @ExampleObject(
                                            name = "Completed result",
                                            value = "{\"request_id\":\"abc123\",\"status\":\"completed\",\"result\":{\"distance\":5.2,\"duration\":15,\"path\":[[51.5074,-0.1278],[51.5074,-0.1278]]}}"
                                    ),
                                    @ExampleObject(
                                            name = "Pending result",
                                            value = "{\"request_id\":\"abc123\",\"status\":\"pending\"}"
                                    ),
                                    @ExampleObject(
                                            name = "Error result",
                                            value = "{\"request_id\":\"abc123\",\"status\":\"error\",\"error\":\"Processing failed\"}"
                                    )
                            }
                    )
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "Request ID not found",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(
                                                    key = "error",
                                                    value = String.class
                                            )
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"error\":\"Request not found\"}")
                    )
            ),
            @ApiResponse(
                    responseCode = "429",
                    description = "Rate limit exceeded. Maximum 100 requests per second.",
                    content = @Content
            ),
            @ApiResponse(
                    responseCode = "500",
                    description = "Internal server error",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(
                                                    key = "error",
                                                    value = String.class
                                            )
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"error\":\"Internal server error\"}")
                    )
            )
    })
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

    @Operation(
            summary = "Submit route processing request",
            description = "Submit a text or voice request for route processing. The request is queued for asynchronous processing. " +
                    "Exactly one of 'text' or 'voice' must be provided. Use the returned request_id to check the result status.",
            operationId = "processRoute",
            tags = {"Map Router"}
    )
    @io.swagger.v3.oas.annotations.parameters.RequestBody(
            description = "Route request payload - exactly one of 'text' or 'voice' must be provided",
            required = true,
            content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(
                            type = "object",
                            properties = {
                                    @StringToClassMapItem(
                                            key = "text",
                                            value = String.class
                                    ),
                                    @StringToClassMapItem(
                                            key = "voice",
                                            value = String.class
                                    ),
                                    @StringToClassMapItem(
                                            key = "language",
                                            value = String.class
                                    )
                            },
                            requiredProperties = {"userId"}
                    ),
                    schemaProperties = {
                            @SchemaProperty(
                                    name = "text",
                                    schema = @Schema(
                                            type = "string",
                                            description = "Text input for route processing (alternative to voice). If provided, voice must be empty.",
                                            example = "How do I get to the nearest pharmacy?"
                                    )
                            ),
                            @SchemaProperty(
                                    name = "voice",
                                    schema = @Schema(
                                            type = "string",
                                            format = "base64",
                                            description = "Base64 encoded audio file (WAV format) for voice route processing. If provided, text must be empty.",
                                            example = "U3RyaW5nIGlzIHRoZSBmaWxlIGNvbnRlbnQ="
                                    )
                            ),
                            @SchemaProperty(
                                    name = "language",
                                    schema = @Schema(
                                            type = "string",
                                            description = "Language code for transcription (default: ru). Supported: ru, en, etc.",
                                            example = "ru",
                                            defaultValue = "ru"
                                    )
                            )
                    },
                    examples = {
                            @ExampleObject(
                                    name = "Text request example",
                                    description = "Submit a text-based route request",
                                    value = "{\"text\":\"How do I get to the nearest pharmacy?\",\"language\":\"en\"}"
                            ),
                            @ExampleObject(
                                    name = "Voice request example",
                                    description = "Submit a voice-based route request with base64 encoded audio",
                                    value = "{\"voice\":\"U3RyaW5nIGlzIHRoZSBmaWxlIGNvbnRlbnQ=\",\"language\":\"ru\"}"
                            ),
                            @ExampleObject(
                                    name = "Text request with default language",
                                    description = "Submit a text request using default language (ru)",
                                    value = "{\"text\":\"Как пройти до ближайшей аптеки?\"}"
                            )
                    }
            )
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "202",
                    description = "Request accepted for processing",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(
                                                    key = "request_id",
                                                    value = UUID.class
                                            ),
                                            @StringToClassMapItem(
                                                    key = "status",
                                                    value = String.class
                                            )
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"request_id\":\"abc123\",\"status\":\"pending\"}")
                    )
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Bad request - missing required fields or invalid format",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(
                                                    key = "error",
                                                    value = String.class
                                            )
                                    }
                            ),
                            examples = {
                                    @ExampleObject(
                                            name = "Both or none provided",
                                            value = "{\"error\":\"Exactly one of 'text' or 'voice' must be provided, not both or none\"}"
                                    ),
                                    @ExampleObject(
                                            name = "Invalid base64",
                                            value = "{\"error\":\"Invalid base64 encoding\"}"
                                    )
                            }
                    )
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized - invalid or missing JWT token",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(
                                                    key = "error",
                                                    value = String.class
                                            )
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"error\":\"Invalid or missing JWT token\"}")
                    )
            ),
            @ApiResponse(
                    responseCode = "429",
                    description = "Rate limit exceeded. Maximum 200 requests per second globally.",
                    content = @Content
            ),
            @ApiResponse(
                    responseCode = "503",
                    description = "Service temporarily unavailable (circuit breaker open). Please retry after 30 seconds.",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(
                                                    key = "error",
                                                    value = String.class
                                            )
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"error\":\"Service temporarily unavailable\"}")
                    )
            ),
            @ApiResponse(
                    responseCode = "500",
                    description = "Internal server error",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(
                                                    key = "error",
                                                    value = String.class
                                            )
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"error\":\"Internal server error\"}")
                    )
            )
    })
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

    @Parameter(
            name = "X-Lat",
            description = "User's latitude for location-based routing",
            schema = @Schema(type = "number", format = "double", example = "51.5074")
    )
    @Parameter(
            name = "X-Lon",
            description = "User's longitude for location-based routing",
            schema = @Schema(type = "number", format = "double", example = "-0.1278")
    )
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

    @Operation(
            summary = "Send transcribed text to SQS (internal)",
            description = "Internal processor that sends the transcribed text to SQS queue for further processing",
            hidden = true
    )
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