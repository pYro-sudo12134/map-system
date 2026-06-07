package by.losik.maprouter.processor;

import by.losik.maprouter.exception.ForbiddenException;
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
public class AdminAuthProcessor implements Processor {

    private final JwtService jwtService;

    @Override
    public void process(@NonNull Exchange exchange) {
        String authHeader = exchange.getIn().getHeader("Authorization", String.class);

        if (authHeader == null || authHeader.isEmpty()) {
            log.warn("Missing Authorization header for admin endpoint");
            throw new UnauthorizedException("Missing Authorization header");
        }

        if (!jwtService.isValid(authHeader)) {
            log.warn("Invalid JWT token for admin endpoint");
            throw new UnauthorizedException("Invalid or expired token");
        }

        if (!jwtService.isAdmin(authHeader)) {
            log.warn("User is not admin, access denied");
            throw new ForbiddenException("Admin access required");
        }

        String userId = jwtService.extractUserId(authHeader);
        exchange.setProperty("adminId", userId);
        exchange.setProperty("authFailed", false);

        log.debug("Admin authenticated: {}", userId);
    }
}