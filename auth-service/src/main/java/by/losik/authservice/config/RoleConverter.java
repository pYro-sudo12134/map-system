package by.losik.authservice.config;

import by.losik.authservice.entity.Role;
import by.losik.authservice.exception.InvalidRoleException;
import by.losik.authservice.exception.NullRoleException;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.util.Optional;

@Converter(autoApply = true)
public class RoleConverter implements AttributeConverter<Role, String> {

    @Override
    public String convertToDatabaseColumn(Role role) {
        return Optional.ofNullable(role)
                .map(Enum::name)
                .orElseThrow(NullRoleException::new);
    }

    @Override
    public Role convertToEntityAttribute(String dbData) {
        return Optional.ofNullable(dbData)
                .map(data -> {
                    try {
                        return Role.valueOf(data);
                    } catch (IllegalArgumentException e) {
                        throw new InvalidRoleException();
                    }
                })
                .orElseThrow(NullRoleException::new);
    }
}