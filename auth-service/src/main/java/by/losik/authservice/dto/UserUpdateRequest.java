package by.losik.authservice.dto;

import lombok.Data;

@Data
public class UserUpdateRequest {

    private String password;

    private String role;

    private Boolean isActive;
}
