package by.losik.maprouter.exception;

public class UnauthorizedException extends AuthException {
    public UnauthorizedException(String message) {
        super(message, 401);
    }
}
