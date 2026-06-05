package by.losik.authservice.dto;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
public class UserResponse {
    private Long id;
    private String username;
    private String role;
    private boolean isActive;
    private Instant createdAt;
    private Instant updatedAt;
}
