package by.losik.authservice.entity;

public enum Role {
    USER, ADMIN, OPERATOR;

    public String getAuthority() {
        return "ROLE_" + this.name();
    }
}