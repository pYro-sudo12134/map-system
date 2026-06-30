package by.losik.maprouter.route;

import by.losik.maprouter.exception.ForbiddenException;
import by.losik.maprouter.exception.UnauthorizedException;
import by.losik.maprouter.processor.AdminAuthProcessor;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.StringToClassMapItem;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
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
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@Tag(name = "Admin", description = "Administrative endpoints for managing data synchronization")
@SecurityRequirement(name = "adminAuth")
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
                .process(this::handleFullSyncResponse);

        from("servlet:/v1/admin/sync/nodes?httpMethodRestrict=POST")
                .routeId("admin-nodes-sync")
                .process(adminAuthProcessor)
                .throttle(nodesSyncMaxRequests)
                .timePeriodMillis(nodesSyncTimePeriodMs)
                .asyncDelayed()
                .setBody(constant("{\"sync_type\":\"nodes\"}"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .to("aws2-sqs:%s".formatted(syncQueueName))
                .process(this::handleNodesSyncResponse);

        from("servlet:/v1/admin/sync/edges?httpMethodRestrict=POST")
                .routeId("admin-edges-sync")
                .process(adminAuthProcessor)
                .throttle(edgesSyncMaxRequests)
                .timePeriodMillis(edgesSyncTimePeriodMs)
                .asyncDelayed()
                .setBody(constant("{\"sync_type\":\"edges\"}"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .to("aws2-sqs:%s".formatted(syncQueueName))
                .process(this::handleEdgesSyncResponse);
    }

    @Operation(
            summary = "Trigger full synchronization",
            description = "Initiates a full synchronization of all data (nodes and edges) from the primary data source to the cache.",
            operationId = "syncFull"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "202",
                    description = "Full synchronization started successfully",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(key = "status", value = String.class)
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"status\":\"full_sync_started\"}")
                    )
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized - invalid or missing admin credentials",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(key = "error", value = String.class)
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"error\":\"Invalid admin credentials\"}")
                    )
            ),
            @ApiResponse(
                    responseCode = "403",
                    description = "Forbidden - insufficient permissions",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(key = "error", value = String.class)
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"error\":\"Access denied\"}")
                    )
            ),
            @ApiResponse(
                    responseCode = "429",
                    description = "Rate limit exceeded (max 2 requests per minute)",
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
    private void handleFullSyncResponse(Exchange exchange) {
        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 202);
        exchange.getMessage().setBody("{\"status\":\"full_sync_started\"}");
        exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
    }

    @Operation(
            summary = "Trigger nodes synchronization",
            description = "Initiates synchronization of nodes only from the primary data source to the cache.",
            operationId = "syncNodes"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "202",
                    description = "Nodes synchronization started successfully",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(key = "status", value = String.class)
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"status\":\"nodes_sync_started\"}")
                    )
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized - invalid or missing admin credentials",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(key = "error", value = String.class)
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"error\":\"Invalid admin credentials\"}")
                    )
            ),
            @ApiResponse(
                    responseCode = "403",
                    description = "Forbidden - insufficient permissions",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(key = "error", value = String.class)
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"error\":\"Access denied\"}")
                    )
            ),
            @ApiResponse(
                    responseCode = "429",
                    description = "Rate limit exceeded (max 10 requests per minute)",
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
    private void handleNodesSyncResponse(Exchange exchange) {
        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 202);
        exchange.getMessage().setBody("{\"status\":\"nodes_sync_started\"}");
        exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
    }

    @Operation(
            summary = "Trigger edges synchronization",
            description = "Initiates synchronization of edges only from the primary data source to the cache.",
            operationId = "syncEdges"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "202",
                    description = "Edges synchronization started successfully",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(key = "status", value = String.class)
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"status\":\"edges_sync_started\"}")
                    )
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized - invalid or missing admin credentials",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(key = "error", value = String.class)
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"error\":\"Invalid admin credentials\"}")
                    )
            ),
            @ApiResponse(
                    responseCode = "403",
                    description = "Forbidden - insufficient permissions",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(
                                    type = "object",
                                    properties = {
                                            @StringToClassMapItem(key = "error", value = String.class)
                                    }
                            ),
                            examples = @ExampleObject(value = "{\"error\":\"Access denied\"}")
                    )
            ),
            @ApiResponse(
                    responseCode = "429",
                    description = "Rate limit exceeded (max 10 requests per minute)",
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
    private void handleEdgesSyncResponse(Exchange exchange) {
        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 202);
        exchange.getMessage().setBody("{\"status\":\"edges_sync_started\"}");
        exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
    }
}