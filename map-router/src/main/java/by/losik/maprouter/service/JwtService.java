package by.losik.maprouter.service;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;

@Service
public class JwtService {

    @Value("${jwt.secret:defaultSecretKeyForDevOnly}")
    private String secret;

    private @NonNull SecretKey getSigningKey() {
        return Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }

    public Claims extractClaims(@NonNull String token) {
        String jwt = token.startsWith("Bearer ") ? token.substring(7) : token;
        return Jwts.parser()
                .verifyWith(getSigningKey())
                .build()
                .parseSignedClaims(jwt)
                .getPayload();
    }

    public String extractRole(String token) {
        return extractClaims(token).get("role", String.class);
    }

    public String extractUserId(String token) {
        return extractClaims(token).getSubject();
    }

    public Date extractExpiration(String token) {
        return extractClaims(token).getExpiration();
    }

    public boolean isExpired(String token) {
        try {
            Date expiration = extractExpiration(token);
            return expiration.before(new Date());
        } catch (Exception e) {
            return true;
        }
    }

    public boolean isAdmin(String token) {
        if (token == null || token.isEmpty() || isExpired(token)) return false;
        try {
            return "ADMIN".equals(extractRole(token));
        } catch (Exception e) {
            return false;
        }
    }

    public boolean isValid(String token) {
        if (token == null || token.isEmpty()) return false;
        try {
            return !isExpired(token);
        } catch (Exception e) {
            return false;
        }
    }
}