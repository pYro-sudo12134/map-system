package by.losik.maprouter.route;

import by.losik.maprouter.service.RedisService;
import com.fasterxml.jackson.databind.ObjectMapper;
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
public class SqsResultsRoute extends RouteBuilder {

    private final RedisService redisService;
    private final ObjectMapper objectMapper;

    @Value("${aws.sqs.results.name}")
    private String resultsQueueName;

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
                .process(exchange -> {
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
                });
    }

    private void processResult(String requestId, @NonNull Map<String, Object> result) {
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
    }
}