package by.losik.maprouter.route;

import by.losik.maprouter.exception.ForbiddenException;
import by.losik.maprouter.exception.UnauthorizedException;
import by.losik.maprouter.processor.AdminAuthProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.util.json.JsonObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class AdminRoute extends RouteBuilder {

    private final AdminAuthProcessor adminAuthProcessor;

    @Value("${aws.sqs.sync.name}")
    private String syncQueueName;

    @Value("${rate.limit.admin.full-sync.max-requests:2}")
    private int fullSyncMaxRequests;

    @Value("${rate.limit.admin.full-sync.time-period-ms:60000}")
    private int fullSyncTimePeriodMs;

    @Value("${rate.limit.admin.nodes-sync.max-requests:10}")
    private int nodesSyncMaxRequests;

    @Value("${rate.limit.admin.nodes-sync.time-period-ms:60000}")
    private int nodesSyncTimePeriodMs;

    @Value("${rate.limit.admin.edges-sync.max-requests:10}")
    private int edgesSyncMaxRequests;

    @Value("${rate.limit.admin.edges-sync.time-period-ms:60000}")
    private int edgesSyncTimePeriodMs;

    @Override
    public void configure() {
        onException(UnauthorizedException.class)
                .handled(true)
                .process(exchange -> {
                    Exception e = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, UnauthorizedException.class);
                    log.error("Unauthorized: {}", e.getMessage());
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 401);
                    exchange.getMessage().setBody(new JsonObject().put("error", e.getMessage()));
                });

        onException(ForbiddenException.class)
                .handled(true)
                .process(exchange -> {
                    Exception e = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, ForbiddenException.class);
                    log.error("Forbidden: {}", e.getMessage());
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 403);
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

        from("servlet:/v1/admin/sync/full?httpMethodRestrict=POST")
                .routeId("admin-full-sync")
                .process(adminAuthProcessor)
                .throttle(fullSyncMaxRequests)
                .timePeriodMillis(fullSyncTimePeriodMs)
                .asyncDelayed()
                .setBody(constant("{\"sync_type\":\"full\"}"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .to("aws2-sqs:%s".formatted(syncQueueName))
                .process(exchange -> {
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 202);
                    exchange.getMessage().setBody("{\"status\":\"full_sync_started\"}");
                    exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
                });

        from("servlet:/v1/admin/sync/nodes?httpMethodRestrict=POST")
                .routeId("admin-nodes-sync")
                .process(adminAuthProcessor)
                .throttle(nodesSyncMaxRequests)
                .timePeriodMillis(nodesSyncTimePeriodMs)
                .asyncDelayed()
                .setBody(constant("{\"sync_type\":\"nodes\"}"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .to("aws2-sqs:%s".formatted(syncQueueName))
                .process(exchange -> {
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 202);
                    exchange.getMessage().setBody("{\"status\":\"nodes_sync_started\"}");
                    exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
                });

        from("servlet:/v1/admin/sync/edges?httpMethodRestrict=POST")
                .routeId("admin-edges-sync")
                .process(adminAuthProcessor)
                .throttle(edgesSyncMaxRequests)
                .timePeriodMillis(edgesSyncTimePeriodMs)
                .asyncDelayed()
                .setBody(constant("{\"sync_type\":\"edges\"}"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .to("aws2-sqs:%s".formatted(syncQueueName))
                .process(exchange -> {
                    exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 202);
                    exchange.getMessage().setBody("{\"status\":\"edges_sync_started\"}");
                    exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
                });
    }
}