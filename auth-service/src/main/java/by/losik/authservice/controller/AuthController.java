package by.losik.authservice.controller;

import by.losik.authservice.dto.AuthResponse;
import by.losik.authservice.dto.LoginRequest;
import by.losik.authservice.dto.UserCreateRequest;
import by.losik.authservice.dto.UserResponse;
import by.losik.authservice.dto.UserUpdateRequest;
import by.losik.authservice.entity.UserEntity;
import by.losik.authservice.mapper.UserMapper;
import by.losik.authservice.service.AuthService;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;
    private final UserMapper userMapper;

    @PostMapping("/login")
    @Operation(summary = "Login user", description = "Login user")
    @RateLimiter(name = "loginEndpoint", fallbackMethod = "fallbackRateLimit")
    public ResponseEntity<AuthResponse> login(@Valid @RequestBody LoginRequest request) {
        return ResponseEntity.ok(authService.login(request));
    }

    @PostMapping("/register")
    @Operation(summary = "Register new user", description = "Create a new user account")
    @RateLimiter(name = "authController", fallbackMethod = "fallbackRateLimit")
    public ResponseEntity<UserResponse> register(@Valid @RequestBody UserCreateRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(userMapper.toResponse(authService.register(request)));
    }

    @PutMapping("/users/{id}")
    @Operation(summary = "Update user", description = "Update user's credentials")
    @PreAuthorize("hasRole('ADMIN')")
    @RateLimiter(name = "authController", fallbackMethod = "fallbackRateLimit")
    public ResponseEntity<UserResponse> updateUser(
            @PathVariable Long id,
            @Valid @RequestBody UserUpdateRequest request) {
        return ResponseEntity.ok(userMapper.toResponse(authService.updateUser(id, request)));
    }

    @GetMapping("/me")
    @Operation(summary = "Get current user", description = "Get information on current user")
    public ResponseEntity<UserResponse> getCurrentUser() {
        Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        if (principal instanceof UserEntity userEntity) {
            return ResponseEntity.ok(userMapper.toResponse(authService.getById(userEntity.getId())));
        }
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
    }

    @PostMapping("/refresh")
    @Operation(summary = "Refresh user token", description = "Refresh access token with refresh token")
    @RateLimiter(name = "authController", fallbackMethod = "fallbackRateLimit")
    public ResponseEntity<AuthResponse> refresh(@RequestHeader("Authorization") @NonNull String refreshToken) {
        return ResponseEntity.ok(authService.refreshToken(refreshToken.substring(7)));
    }

    @SuppressWarnings("unused")
    public ResponseEntity<?> fallbackRateLimit(RequestNotPermitted ex) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body("Too many requests. Please try again later.");
    }
}
