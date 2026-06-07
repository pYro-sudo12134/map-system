package by.losik.maprouter.processor;

import by.losik.maprouter.exception.UnauthorizedException;
import by.losik.maprouter.service.JwtService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class JwtAuthProcessor implements Processor {

    private final JwtService jwtService;

    @Override
    public void process(@NonNull Exchange exchange) {
        String authHeader = exchange.getIn().getHeader("Authorization", String.class);

        if (authHeader == null || authHeader.isEmpty()) {
            log.warn("Missing Authorization header");
            throw new UnauthorizedException("Missing Authorization header");
        }

        if (!jwtService.isValid(authHeader)) {
            log.warn("Invalid JWT token");
            throw new UnauthorizedException("Invalid or expired token");
        }

        String userId = jwtService.extractUserId(authHeader);
        String role = jwtService.extractRole(authHeader);

        exchange.setProperty("userId", userId);
        exchange.setProperty("userRole", role);
        exchange.setProperty("authFailed", false);

        log.debug("Authenticated user: {} with role: {}", userId, role);
    }
}