package by.losik.authservice.dto;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;

@Data
@Builder
public class ValidationErrorResponse {
    private Instant timestamp;
    private String message;
    private int status;
    private Map<String, String> errors;
}
