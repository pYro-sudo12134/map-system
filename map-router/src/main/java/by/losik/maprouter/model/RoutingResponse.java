package by.losik.maprouter.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RoutingResponse {
    @JsonProperty("request_id")
    private String requestId;

    private String status;
    private Object result;
    private String error;

    @JsonProperty("created_at")
    private Long createdAt;
}