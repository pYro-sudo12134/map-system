package by.losik.maprouter.exception;

public class AuthException extends RuntimeException {
    public AuthException(String message) {
        super(message);
    }

    public AuthException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    private int statusCode = 401;

    public int getStatusCode() { return statusCode; }
}
