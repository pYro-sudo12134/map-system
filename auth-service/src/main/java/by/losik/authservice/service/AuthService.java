package by.losik.authservice.service;

import by.losik.authservice.dto.AuthResponse;
import by.losik.authservice.dto.LoginRequest;
import by.losik.authservice.dto.UserCreateRequest;
import by.losik.authservice.dto.UserUpdateRequest;
import by.losik.authservice.entity.Role;
import by.losik.authservice.entity.UserEntity;
import by.losik.authservice.exception.InvalidTokenException;
import by.losik.authservice.exception.UserNotFoundException;
import by.losik.authservice.exception.UsernameAlreadyExistsException;
import by.losik.authservice.exception.UsernameNotFoundException;
import by.losik.authservice.mapper.UserMapper;
import by.losik.authservice.repository.UserRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.lang.NonNull;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
public class AuthService implements UserDetailsService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtService jwtService;
    private final AuthenticationManager authenticationManager;
    private final UserMapper userMapper;

    public AuthService(
            UserRepository userRepository,
            PasswordEncoder passwordEncoder,
            JwtService jwtService,
            @Lazy AuthenticationManager authenticationManager,
            UserMapper userMapper) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.jwtService = jwtService;
        this.authenticationManager = authenticationManager;
        this.userMapper = userMapper;
    }

    public AuthResponse login(@NonNull LoginRequest request) {
        authenticationManager.authenticate(
                new UsernamePasswordAuthenticationToken(request.getUsername(), request.getPassword())
        );

        UserDetails user = loadUserByUsername(request.getUsername());
        String accessToken = jwtService.generateToken(user);
        String refreshToken = jwtService.generateRefreshToken(user);

        return AuthResponse.builder()
                .accessToken(accessToken)
                .refreshToken(refreshToken)
                .username(user.getUsername())
                .role(user.getAuthorities().iterator().next().toString())
                .build();
    }

    public UserEntity register(@NonNull UserCreateRequest request) {
        if (userRepository.existsByUsername(request.getUsername())) {
            throw new UsernameAlreadyExistsException(request.getUsername());
        }

        UserEntity user = userMapper.toEntity(request);

        return userRepository.save(user);
    }

    public AuthResponse refreshToken(String refreshToken) {
        String username = jwtService.extractUsername(refreshToken);
        UserDetails user = loadUserByUsername(username);

        if (jwtService.isTokenValid(refreshToken, user)) {
            String newAccessToken = jwtService.generateToken(user);
            return AuthResponse.builder()
                    .accessToken(newAccessToken)
                    .refreshToken(refreshToken)
                    .username(username)
                    .role(user.getAuthorities().iterator().next().toString())
                    .build();
        }
        throw new InvalidTokenException();
    }

    public UserEntity updateUser(Long userId, @NonNull UserUpdateRequest request) {
        UserEntity user = userRepository.findById(userId)
                .orElseThrow(() -> new UserNotFoundException(userId));

        Optional.ofNullable(request.getPassword())
                .filter(pwd -> !pwd.isBlank())
                .ifPresent(pwd -> user.setPassword(passwordEncoder.encode(pwd)));

        Optional.ofNullable(request.getRole())
                .ifPresent(role -> user.setRole(Role.valueOf(role)));

        Optional.ofNullable(request.getIsActive())
                .ifPresent(user::setActive);

        return userRepository.save(user);
    }

    public UserEntity getById(Long userId) {
        return userRepository.findById(userId)
                .orElseThrow(() -> new UserNotFoundException(userId));
    }

    @Override
    public UserDetails loadUserByUsername(String username) {
        return userRepository.findByUsername(username)
                .orElseThrow(() -> new UsernameNotFoundException(username));
    }
}