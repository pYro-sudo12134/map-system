package by.losik.maprouter.exception;

public class ForbiddenException extends AuthException {
    public ForbiddenException(String message) {
        super(message, 403);
    }
}
