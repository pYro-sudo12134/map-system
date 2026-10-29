package by.losik.maprouter.route;

import by.losik.maprouter.exception.ForbiddenException;
import by.losik.maprouter.exception.UnauthorizedException;
import by.losik.maprouter.processor.JwtAuthProcessor;
import by.losik.maprouter.processor.PollTranscriptionProcessor;
import by.losik.maprouter.processor.PrepareTranscriptionPollProcessor;
import by.losik.maprouter.service.RedisService;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

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
    private final PollTranscriptionProcessor pollTranscriptionProcessor;
    private final PrepareTranscriptionPollProcessor prepareTranscriptionPollProcessor;
    private final S3Client s3Client;

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

    @Value("${transcribe.poll.interval-ms:2000}")
    private long transcribePollIntervalMs;

    @Value("${transcribe.poll.max-attempts:60}")
    private int transcribePollMaxAttempts;

    private static final String REQUEST_ID = "requestId";
    private static final String USER_ID = "userId";
    private static final String LANGUAGE = "language";
    private static final String TRANSCRIBE_LANGUAGE = "transcribeLanguage";

    @Override
    public void configure() {
        onException(UnauthorizedException.class)
                .handled(true)
                .process(exchange -> {
                    Exception e = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                    log.warn("Unauthorized: {}", e.getMessage());
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 401);
                    exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json; charset=UTF-8");
                    exchange.getMessage().setBody(
                            objectMapper.writeValueAsString(Map.of("error", e.getMessage())));
                });

        onException(ForbiddenException.class)
                .handled(true)
                .process(exchange -> {
                    Exception e = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                    log.warn("Forbidden: {}", e.getMessage());
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 403);
                    exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json; charset=UTF-8");
                    exchange.getMessage().setBody(
                            objectMapper.writeValueAsString(Map.of("error", e.getMessage())));
                });

        onException(IllegalArgumentException.class)
                .handled(true)
                .process(exchange -> {
                    Exception e = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                    log.warn("Bad request: {}", e.getMessage());
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 400);
                    exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json; charset=UTF-8");
                    exchange.getMessage().setBody(
                            objectMapper.writeValueAsString(Map.of("error", e.getMessage())));
                });

        onException(CallNotPermittedException.class)
                .handled(true)
                .process(exchange -> {
                    log.error("Circuit breaker is OPEN - request rejected");
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 503);
                    exchange.getMessage().setHeader("Retry-After", 30);
                    exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json; charset=UTF-8");
                    exchange.getMessage().setBody(
                            objectMapper.writeValueAsString(Map.of("error", "Internal server error")));
                });

        onException(Exception.class)
                .handled(true)
                .process(exchange -> {
                    Exception e = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                    log.error("Internal error: {}", e.getMessage(), e);
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
                    exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json; charset=UTF-8");
                    exchange.getMessage().setBody(
                            objectMapper.writeValueAsString(Map.of("error", "Internal server error")));
                });

        from("servlet:/v1/result/{requestId}?httpMethodRestrict=GET")
                .routeId("get-result")
                .throttle(getResultMaxRequests)
                .timePeriodMillis(getResultTimePeriodMs)
                .process(jwtAuthProcessor)
                .process(this::processGetResult);

        from("servlet:/v1/route?httpMethodRestrict=POST")
                .routeId("user-route")
                .throttle(globalMaxRequests)
                .timePeriodMillis(globalTimePeriodMs)
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
                    try {
                        exchange.getMessage().setBody(objectMapper.writeValueAsString(sqsMessage));
                    } catch (JsonProcessingException e) {
                        log.error("Failed to serialize fallback SQS message", e);
                        exchange.getMessage().setBody("{\"error\":\"s3_upload_unavailable\"}");
                    }
                    exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json; charset=UTF-8");
                })
                .end()
                .end()
                .end()

                .choice()
                .when(exchange -> exchange.getProperty("skipTranscribe") == null)
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
                .to("aws2-transcribe://startTranscriptionJob?operation=startTranscriptionJob")
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
                    try {
                        exchange.getMessage().setBody(objectMapper.writeValueAsString(sqsMessage));
                    } catch (JsonProcessingException e) {
                        log.error("Failed to serialize fallback SQS message", e);
                        exchange.getMessage().setBody("{\"error\":\"transcription_unavailable\"}");
                    }
                    exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json; charset=UTF-8");
                })
                .end()
                .end()
                .end()

                .choice()
                .when(exchange -> exchange.getProperty("skipTranscribe") == null)
                .setProperty("transcriptionPending", constant(true))
                .setProperty("pollAttempt", constant(0))
                .loopDoWhile(exchange ->
                        Boolean.TRUE.equals(exchange.getProperty("transcriptionPending", Boolean.class))
                                && exchange.getProperty("pollAttempt", Integer.class) < transcribePollMaxAttempts)
                .process(exchange -> {
                    Integer attempt = exchange.getProperty("pollAttempt", Integer.class);
                    exchange.setProperty("pollAttempt", attempt + 1);
                })
                .delay(transcribePollIntervalMs)
                .process(prepareTranscriptionPollProcessor)
                .to("aws2-transcribe://getTranscriptionJob?operation=getTranscriptionJob")
                .process(pollTranscriptionProcessor)
                .end()
                .choice()
                .when(exchange -> Boolean.TRUE.equals(exchange.getProperty("transcriptionPending", Boolean.class)))
                .throwException(new IllegalStateException(
                        "Transcription timed out after " + transcribePollMaxAttempts + " attempts"))
                .end()
                .end()
                .end()

                .process(this::sendToSqs)
                .to("aws2-sqs:%s".formatted(ragQueueName))
                .process(this::sendResponse)
                .marshal().json();
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
                                            @StringToClassMapItem(key = "request_id", value = String.class),
                                            @StringToClassMapItem(key = "status", value = String.class),
                                            @StringToClassMapItem(key = "result", value = Object.class),
                                            @StringToClassMapItem(key = "error", value = String.class)
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
                                            @StringToClassMapItem(key = "error", value = String.class)
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
                                            @StringToClassMapItem(key = "error", value = String.class)
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"error\":\"Internal server error\"}")
                    )
            )
    })
    private void processGetResult(@NonNull Exchange exchange) {
        String userId = exchange.getProperty("userId", String.class);
        String requestId = extractRequestId(exchange);

        if (requestId == null) {
            respond(exchange, 400, Map.of("error", "Missing requestId"));
            return;
        }

        log.debug("Getting result for requestId: {}, userId: {}", requestId, userId);
        Map<String, Object> result = redisService.getResult(requestId);
        log.debug("Retrieved result for requestId: {}, status: {}", requestId,
                result != null ? result.get("status") : "null");

        if (result == null) {
            respond(exchange, 404, Map.of("error", "Request not found"));
            return;
        }

        Object owner = result.get("user_id");
        if (owner != null && !owner.equals(userId)) {
            respond(exchange, 404, Map.of("error", "Request not found"));
            return;
        }

        result.put("request_id", requestId);
        respond(exchange, 200, result);
    }

    private String extractRequestId(@NonNull Exchange exchange) {
        String id = exchange.getIn().getHeader("requestId", String.class);
        if (id != null) return id;
        id = exchange.getProperty("requestId", String.class);
        if (id != null) return id;
        String path = exchange.getIn().getHeader(Exchange.HTTP_PATH, String.class);
        if (path != null) {
            String[] parts = path.split("/");
            return parts[parts.length - 1];
        }
        return null;
    }

    private void respond(@NonNull Exchange exchange, int status, Object body) {
        try {
            exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, status);
            exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json; charset=UTF-8");
            exchange.getMessage().setBody(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            log.error("Failed to serialize response", e);
            exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
            exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json; charset=UTF-8");
            exchange.getMessage().setBody("{\"error\":\"Internal server error\"}");
        }
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
                                    @StringToClassMapItem(key = "text", value = String.class),
                                    @StringToClassMapItem(key = "voice", value = String.class),
                                    @StringToClassMapItem(key = "language", value = String.class)
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
                                            description = "Language code (short form like 'ru', 'en' will be normalized to 'ru-RU', 'en-US'). Supported: ru, en, de, fr, es, it, ja, ko, pt, zh, uk, pl, nl, tr, hi, ca, cs, fa, gu, kk, te, uz, vi.",
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
                                            @StringToClassMapItem(key = "request_id", value = UUID.class),
                                            @StringToClassMapItem(key = "status", value = String.class)
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
                                            @StringToClassMapItem(key = "error", value = String.class)
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
                                            @StringToClassMapItem(key = "error", value = String.class)
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
                                            @StringToClassMapItem(key = "error", value = String.class)
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
                                            @StringToClassMapItem(key = "error", value = String.class)
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
        String rawLanguage = (String) request.getOrDefault("language", "ru");
        String transcribeLanguage = normalizeLanguage(rawLanguage);

        exchange.setProperty(REQUEST_ID, requestId);
        exchange.setProperty(USER_ID, userId);
        exchange.setProperty(LANGUAGE, rawLanguage);
        exchange.setProperty(TRANSCRIBE_LANGUAGE, transcribeLanguage);

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

        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 202);

        if (hasVoice && !hasText) {
            byte[] audioData = Base64.getDecoder().decode(voice);
            String s3Key = "temp/%s/audio.wav".formatted(requestId);
            exchange.getMessage().setHeader("CamelAwsS3Key", s3Key);
            exchange.setProperty("s3Key", s3Key);
            exchange.getMessage().setBody(audioData);
        } else if (hasText && !hasVoice) {
            redisService.savePending(requestId);
            exchange.setProperty("skipTranscribe", true);

            JsonObject sqsMessage = new JsonObject();
            sqsMessage.put("request_id", requestId);
            sqsMessage.put("text", text);
            sqsMessage.put("user_id", userId);
            sqsMessage.put("language", rawLanguage);

            Optional.ofNullable(userLocation).ifPresent(loc -> {
                JsonObject location = new JsonObject();
                location.put("lat", loc.get("lat"));
                location.put("lon", loc.get("lon"));
                sqsMessage.put("user_location", location);
            });

            exchange.getMessage().setBody(objectMapper.writeValueAsString(sqsMessage));
            exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json; charset=UTF-8");
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
        String transcribeLanguage = exchange.getProperty(TRANSCRIBE_LANGUAGE, String.class);
        if (transcribeLanguage == null) {
            transcribeLanguage = normalizeLanguage(exchange.getProperty(LANGUAGE, String.class));
        }
        String jobName = "transcribe-%s-%d".formatted(requestId, System.currentTimeMillis());
        String mediaUri = "s3://%s/%s".formatted(bucketName, s3Key);

        exchange.getIn().setHeader("CamelAwsTranscribeTranscriptionJobName", jobName);
        exchange.getIn().setHeader("CamelAwsTranscribeMediaFormat", "wav");
        exchange.getIn().setHeader("CamelAwsTranscribeMediaUri", mediaUri);
        exchange.getIn().setHeader("CamelAwsTranscribeLanguageCode", transcribeLanguage);

        exchange.getMessage().setHeader("CamelAwsTranscribeTranscriptionJobName", jobName);
        exchange.getMessage().setHeader("CamelAwsTranscribeMediaFormat", "wav");
        exchange.getMessage().setHeader("CamelAwsTranscribeMediaUri", mediaUri);
        exchange.getMessage().setHeader("CamelAwsTranscribeLanguageCode", transcribeLanguage);

        exchange.setProperty("jobName", jobName);
        log.info("Starting transcription: jobName={}, mediaUri={}, lang={}", jobName, mediaUri, transcribeLanguage);
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

        log.info("=== sendToSqs: requestId={}, skipTranscribe={}, s3UploadFailed={}",
                requestId,
                exchange.getProperty("skipTranscribe"),
                exchange.getProperty("s3UploadFailed"));

        if (exchange.getProperty("skipTranscribe") == null && exchange.getProperty("s3UploadFailed") == null) {
            redisService.savePending(requestId);
        }

        if (exchange.getProperty("skipTranscribe") == null) {
            String jobName = exchange.getProperty("jobName", String.class);
            if (jobName == null || jobName.isEmpty()) {
                throw new IllegalStateException("jobName is missing, cannot fetch transcript");
            }

            String transcriptText;
            try {
                transcriptText = downloadTranscriptFromS3(jobName);
            } catch (Exception e) {
                log.error("Failed to fetch transcript for request {}, job {}", requestId, jobName, e);
                redisService.saveError(requestId, "Transcription result unavailable");
                JsonObject sqsMessage = new JsonObject();
                sqsMessage.put("request_id", requestId);
                sqsMessage.put("text", "Transcription failed, please try text input");
                sqsMessage.put("user_id", userId);
                sqsMessage.put("language", language);
                sqsMessage.put("error", "transcript_download_failed");

                try {
                    exchange.getMessage().setBody(objectMapper.writeValueAsString(sqsMessage));
                } catch (JsonProcessingException exception) {
                    log.error("Failed to serialize SQS message", exception);
                    throw new RuntimeException("Failed to serialize SQS message", exception);
                }
                exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json; charset=UTF-8");
                return;
            }

            if (transcriptText.isEmpty()) {
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

            try {
                exchange.getMessage().setBody(objectMapper.writeValueAsString(sqsMessage));
            } catch (JsonProcessingException e) {
                log.error("Failed to serialize SQS message", e);
                throw new RuntimeException("Failed to serialize SQS message", e);
            }
            exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json; charset=UTF-8");
        }
    }

    private String downloadTranscriptFromS3(String jobName) {
        String key = jobName + ".json";
        try {
            byte[] body = s3Client.getObjectAsBytes(
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build()
            ).asByteArray();

            var node = objectMapper.readTree(body);
            String text = node.path("results")
                    .path("transcripts")
                    .path(0)
                    .path("transcript")
                    .asText();

            if (text == null || text.isBlank()) {
                throw new IllegalStateException("Transcript is empty for s3://" + bucketName + "/" + key);
            }
            return text;
        } catch (S3Exception e) {
            throw new RuntimeException(
                    "Failed to fetch transcript from s3://" + bucketName + "/" + key
                            + " (HTTP " + e.statusCode() + ")", e);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to download transcript from s3://" + bucketName + "/" + key, e);
        }
    }

    private static String normalizeLanguage(String lang) {
        if (lang == null || lang.isBlank()) return "ru-RU";
        if (lang.contains("-")) return lang;
        return switch (lang.toLowerCase()) {
            case "ru" -> "ru-RU";
            case "en" -> "en-US";
            case "de" -> "de-DE";
            case "fr" -> "fr-FR";
            case "es" -> "es-ES";
            case "it" -> "it-IT";
            case "ja" -> "ja-JP";
            case "ko" -> "ko-KR";
            case "pt" -> "pt-BR";
            case "zh" -> "zh-CN";
            case "uk" -> "uk-UA";
            case "pl" -> "pl-PL";
            case "nl" -> "nl-NL";
            case "tr" -> "tr-TR";
            case "hi" -> "hi-IN";
            case "ca" -> "ca-ES";
            case "cs" -> "cs-CZ";
            case "fa" -> "fa-IR";
            case "gu" -> "gu-IN";
            case "kk" -> "kk-KZ";
            case "te" -> "te-IN";
            case "uz" -> "uz-UZ";
            case "vi" -> "vi-VN";
            default -> lang + "-" + lang.toUpperCase();
        };
    }

    private void sendResponse(@NonNull Exchange exchange) {
        String requestId = exchange.getProperty(REQUEST_ID, String.class);
        Map<String, Object> response = new HashMap<>();
        response.put("request_id", requestId);
        response.put("status", "pending");

        exchange.getMessage().setBody(response);
        exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json; charset=UTF-8");
        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 202);
    }
}