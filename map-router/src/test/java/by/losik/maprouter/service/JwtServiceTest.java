package by.losik.maprouter.service;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class JwtServiceTest {

    private JwtService jwtService;
    private final String secret = "testSecretKeyForJWTThatIsAtLeast32CharactersLong";

    @BeforeEach
    void setUp() {
        jwtService = new JwtService();
        ReflectionTestUtils.setField(jwtService, "secret", secret);
    }

    @Test
    void shouldValidateValidToken() {
        String token = generateToken("user123", "USER", System.currentTimeMillis() + 3600000);
        assertTrue(jwtService.isValid(token));
        assertFalse(jwtService.isExpired(token));
        assertEquals("user123", jwtService.extractUserId(token));
        assertEquals("USER", jwtService.extractRole(token));
    }

    @Test
    void shouldRejectExpiredToken() {
        String token = generateToken("user123", "USER", System.currentTimeMillis() - 3600000);
        assertFalse(jwtService.isValid(token));
        assertTrue(jwtService.isExpired(token));
    }

    @Test
    void shouldRejectInvalidToken() {
        assertFalse(jwtService.isValid("invalid.token.here"));
        assertFalse(jwtService.isAdmin("invalid.token.here"));
    }

    @Test
    void shouldRecognizeAdmin() {
        String token = generateToken("admin123", "ADMIN", System.currentTimeMillis() + 3600000);
        assertTrue(jwtService.isAdmin(token));
    }

    private String generateToken(String userId, String role, long expiration) {
        SecretKey key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
        return Jwts.builder()
                .subject(userId)
                .claim("role", role)
                .expiration(new Date(expiration))
                .signWith(key)
                .compact();
    }
}