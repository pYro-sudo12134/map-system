package by.losik.maprouter.route;

import by.losik.maprouter.service.RedisService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.StringToClassMapItem;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
@Tag(name = "SQS Results Consumer", description = "Internal consumer for processing results from SQS queue (not exposed as REST API)")
public class SqsResultsRoute extends RouteBuilder {

    private final RedisService redisService;
    private final ObjectMapper objectMapper;

    @Value("${aws.sqs.results.name}")
    private String resultsQueueName;

    @Value("${rate.limit.sqs-consumer.max-requests:30}")
    private int sqsConsumerMaxRequests;

    @Value("${rate.limit.sqs-consumer.time-period-ms:1000}")
    private int sqsConsumerTimePeriodMs;

    @Value("${resilience4j.circuitbreaker.instances.sqs-processor.failure-rate-threshold:40}")
    private float failureRateThreshold;

    @Value("${resilience4j.circuitbreaker.instances.sqs-processor.sliding-window-size:20}")
    private int slidingWindowSize;

    @Value("${resilience4j.circuitbreaker.instances.sqs-processor.minimum-number-of-calls:10}")
    private int minimumNumberOfCalls;

    @Value("${resilience4j.circuitbreaker.instances.sqs-processor.wait-duration-in-open-state:60000}")
    private int waitDurationInOpenState;

    @Value("${resilience4j.circuitbreaker.instances.sqs-processor.permitted-number-of-calls-in-half-open-state:5}")
    private int permittedNumberOfCallsInHalfOpenState;

    @Override
    public void configure() {
        onException(Exception.class)
                .handled(true)
                .process(exchange -> {
                    Exception e = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                    String body = exchange.getIn().getBody(String.class);
                    log.error("Failed to process SQS message: {}. Body: {}", e.getMessage(), body, e);

                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
                    exchange.getMessage().setBody(Map.of("error", e.getMessage()));
                });

        from("aws2-sqs:%s?deleteAfterRead=true".formatted(resultsQueueName))
                .routeId("sqs-results-consumer")
                .throttle(sqsConsumerMaxRequests)
                .timePeriodMillis(sqsConsumerTimePeriodMs)
                .asyncDelayed()
                .circuitBreaker()
                .resilience4jConfiguration()
                .failureRateThreshold(failureRateThreshold)
                .slidingWindowSize(slidingWindowSize)
                .minimumNumberOfCalls(minimumNumberOfCalls)
                .waitDurationInOpenState(waitDurationInOpenState)
                .permittedNumberOfCallsInHalfOpenState(permittedNumberOfCallsInHalfOpenState)
                .timeoutEnabled(true)
                .timeoutDuration(5000)
                .end()
                .process(this::processSqsMessage)
                .onFallback()
                .process(this::processSqsFallback)
                .end();
    }

    @Operation(
            summary = "Process SQS result message (internal)",
            description = "Internal endpoint that consumes messages from SQS results queue and stores them in Redis. This is not a public REST API.",
            operationId = "processSqsResult",
            hidden = true
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Message processed successfully",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(key = "request_id", value = String.class),
                                            @StringToClassMapItem(key = "result", value = Object.class)
                                    }
                            ),
                            examples = {
                                    @ExampleObject(
                                            name = "Success result",
                                            value = "{\"request_id\":\"abc123\",\"result\":{\"distance\":5.2,\"duration\":15,\"path\":[[51.5074,-0.1278],[51.5074,-0.1278]]}}"
                                    ),
                                    @ExampleObject(
                                            name = "Error result",
                                            value = "{\"request_id\":\"abc123\",\"error\":\"Processing failed\"}"
                                    )
                            }
                    )
            ),
            @ApiResponse(
                    responseCode = "500",
                    description = "Failed to process SQS message",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(key = "error", value = String.class)
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"error\":\"Failed to parse SQS message\"}")
                    )
            )
    })
    private void processSqsMessage(@NonNull Exchange exchange) {
        String body = exchange.getIn().getBody(String.class);
        log.debug("Received SQS message: {}", body);

        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> result = objectMapper.readValue(body, Map.class);

            Optional.ofNullable((String) result.get("request_id"))
                    .ifPresentOrElse(
                            requestId -> processResult(requestId, result),
                            () -> log.warn("Message without request_id: {}", body)
                    );
        } catch (Exception e) {
            log.error("Failed to parse SQS message: {}", body, e);
            throw new RuntimeException("Failed to parse SQS message", e);
        }
    }

    private void processSqsFallback(@NonNull Exchange exchange) {
        Exception cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        String body = exchange.getIn().getBody(String.class);

        log.error("Circuit breaker fallback for SQS message: {}, cause: {}",
                body, cause != null ? cause.getMessage() : "unknown");

        exchange.getMessage().setHeader("CamelAwsSqsDelay", 30000);
        exchange.getMessage().setBody(body);

        exchange.getMessage().setHeader("CamelAwsSqsDeleteAfterRead", false);
    }

    private void processResult(String requestId, @NonNull Map<String, Object> result) {
        try {
            if (result.containsKey("error") && result.get("error") != null) {
                String error = (String) result.get("error");
                redisService.saveError(requestId, error);
                log.warn("Error result for request {}: {}", requestId, error);
            } else {
                Object responseResult = result.get("result");
                if (responseResult != null) {
                    redisService.saveResult(requestId, responseResult);
                    log.info("Saved result for request: {}", requestId);
                } else {
                    log.warn("Result without data for request: {}", requestId);
                    redisService.saveError(requestId, "Empty result from FaaS");
                }
            }
        } catch (Exception e) {
            log.error("Failed to save result for request {}: {}", requestId, e.getMessage(), e);
        }
    }
}