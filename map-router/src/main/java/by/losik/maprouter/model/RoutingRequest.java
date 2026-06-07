package by.losik.maprouter.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class RoutingRequest {
    private String text;
    private String voice;
    private String language = "ru";

    @JsonProperty("user_id")
    private String userId;
}