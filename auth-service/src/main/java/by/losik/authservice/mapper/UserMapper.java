package by.losik.authservice.mapper;

import by.losik.authservice.dto.UserCreateRequest;
import by.losik.authservice.dto.UserResponse;
import by.losik.authservice.entity.Role;
import by.losik.authservice.entity.UserEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

@Mapper(componentModel = "spring", imports = {BCryptPasswordEncoder.class})
public interface UserMapper {

    @Mapping(target = "id", source = "id")
    @Mapping(target = "username", source = "username")
    @Mapping(target = "role", source = "role", qualifiedByName = "roleToString")
    @Mapping(target = "isActive", source = "active")
    @Mapping(target = "createdAt", source = "createdAt")
    @Mapping(target = "updatedAt", source = "updatedAt")
    UserResponse toResponse(UserEntity user);

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "password", expression = "java(new BCryptPasswordEncoder().encode(request.getPassword()))")
    @Mapping(target = "role", expression = "java(by.losik.authservice.entity.Role.valueOf(request.getRole()))")
    @Mapping(target = "active", constant = "true")
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "updatedAt", ignore = true)
    UserEntity toEntity(UserCreateRequest request);

    @SuppressWarnings("unused")
    @Named("roleToString")
    default String roleToString(Role role) {
        return role != null ? role.name() : null;
    }
}

