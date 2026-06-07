package by.losik.maprouter.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisService {

    private final RedisTemplate<String, Object> redisTemplate;
    private static final Duration TTL = Duration.ofHours(1);

    public String generateRequestId() {
        return UUID.randomUUID().toString();
    }

    public void savePending(String requestId) {
        saveStatus(requestId, "pending", null, null);
    }

    public void saveResult(String requestId, Object result) {
        saveStatus(requestId, "completed", result, null);
    }

    public void saveError(String requestId, String error) {
        saveStatus(requestId, "error", null, error);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getResult(String requestId) {
        String key = "route:" + requestId;
        Object value = redisTemplate.opsForValue().get(key);
        log.info("Getting result for key: {}, value: {}", key, value);
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return null;
    }

    public String getStatus(String requestId) {
        Map<String, Object> result = getResult(requestId);
        return result != null ? (String) result.get("status") : null;
    }

    private void saveStatus(String requestId, String status, Object data, String error) {
        String key = "route:" + requestId;
        Map<String, Object> value = new HashMap<>();
        value.put("status", status);
        value.put("timestamp", System.currentTimeMillis());

        if (data != null) {
            value.put("result", data);
        }
        if (error != null) {
            value.put("error", error);
        }

        redisTemplate.opsForValue().set(key, value, TTL);
        log.debug("Saved status for {}: {}", requestId, status);
    }
}