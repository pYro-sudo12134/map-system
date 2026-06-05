package by.losik.authservice.exception;

public class UsernameNotFoundException extends RuntimeException {
    public UsernameNotFoundException(String username) {
        super("User not found: " + username);
    }
}
