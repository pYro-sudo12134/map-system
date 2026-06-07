package by.losik.maprouter.route;

import by.losik.maprouter.service.RedisService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.sqs.SqsClient;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MainRouteIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private RedisService redisService;

    @Autowired
    private ObjectMapper objectMapper;

    private String validJwt;

    @BeforeAll
    static void setupQueues() {
        createQueueStatic("SQS_RAG");
        createQueueStatic("SQS_RESULTS");
        createQueueStatic("SQS_SYNC");
        createBucketStatic("voice-records");
    }

    @BeforeEach
    void setUp() {
        validJwt = generateToken("user123", "USER");
    }

    @Test
    void shouldReturn202ForValidTextRequest() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(validJwt);

        Map<String, String> body = Map.of("text", "как доехать до центра");
        HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);

        ResponseEntity<String> response = restTemplate.postForEntity("/api/v1/route", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).contains("request_id");
    }

    @Test
    void shouldReturn401WithoutToken() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, String> body = Map.of("text", "как доехать до центра");
        HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);

        ResponseEntity<String> response = restTemplate.postForEntity("/api/v1/route", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void shouldReturn400ForEmptyRequest() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(validJwt);

        Map<String, Object> body = new HashMap<>();
        body.put("text", null);
        body.put("voice", null);

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);

        ResponseEntity<String> response = restTemplate.postForEntity("/api/v1/route", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void shouldReturn404ForUnknownRequestId() {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(validJwt);

        ResponseEntity<String> response = restTemplate.exchange(
                "/api/v1/result/unknown-id",
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void shouldReturnResultAfterProcessing() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(validJwt);

        Map<String, String> requestBody = Map.of("text", "как доехать до центра");
        HttpEntity<Map<String, String>> request = new HttpEntity<>(requestBody, headers);

        ResponseEntity<String> postResponse = restTemplate.postForEntity("/api/v1/route", request, String.class);
        assertThat(postResponse.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        String responseBodyStr = postResponse.getBody();
        assertThat(responseBodyStr).isNotNull();

        System.out.println("Raw response: " + responseBodyStr);
        System.out.println("Response class: " + responseBodyStr.getClass());

        String requestId = extractRequestId(responseBodyStr);
        assertThat(requestId).isNotNull();

        Map<String, Object> faasResult = Map.of(
                "request_id", requestId,
                "result", Map.of(
                        "path", "Moscow",
                        "distance", "10km",
                        "duration", "15min",
                        "steps", List.of("Start", "Go straight", "Turn left", "Arrive")
                )
        );

        try (SqsClient sqsClient = createSqsClient()) {
            String queueUrl = sqsClient.getQueueUrl(q -> q.queueName("SQS_RESULTS")).queueUrl();
            sqsClient.sendMessage(q -> {
                        try {
                            q
                                    .queueUrl(queueUrl)
                                    .messageBody(objectMapper.writeValueAsString(faasResult));
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        }

        Thread.sleep(1000);

        ResponseEntity<String> getResponse = restTemplate.exchange(
                "/api/v1/result/" + requestId,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class
        );

        assertThat(getResponse.getStatusCode()).isEqualTo(HttpStatus.OK);

        Map<String, Object> result = objectMapper.readValue(getResponse.getBody(), Map.class);
        assertThat(result.get("status")).isEqualTo("completed");
        assertThat(result.get("result")).isNotNull();

        Map<String, Object> resultData = (Map<String, Object>) result.get("result");
        assertThat(resultData.get("path")).isEqualTo("Moscow");
        assertThat(resultData.get("distance")).isEqualTo("10km");
    }

    private String extractRequestId(String responseBody) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\"request_id\":\"([^\"]+)\"");
        java.util.regex.Matcher matcher = pattern.matcher(responseBody);
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new RuntimeException("Cannot extract request_id from: " + responseBody);
    }

    private String generateToken(String userId, String role) {
        String secret = "testSecretKeyForJWTThatIsAtLeast32CharactersLong";
        SecretKey key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
        return Jwts.builder()
                .subject(userId)
                .claim("role", role)
                .expiration(new Date(System.currentTimeMillis() + 3600000))
                .signWith(key)
                .compact();
    }

    @Test
    void shouldHandleErrorResult() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(validJwt);

        Map<String, String> requestBody = Map.of("text", "как доехать до центра");
        HttpEntity<Map<String, String>> request = new HttpEntity<>(requestBody, headers);

        ResponseEntity<String> postResponse = restTemplate.postForEntity("/api/v1/route", request, String.class);
        assertThat(postResponse.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        String requestId = extractRequestId(postResponse.getBody());

        Map<String, Object> errorResult = Map.of(
                "request_id", requestId,
                "error", "Failed to process request in FaaS"
        );

        try (SqsClient sqsClient = createSqsClient()) {
            String queueUrl = sqsClient.getQueueUrl(q -> q.queueName("SQS_RESULTS")).queueUrl();
            sqsClient.sendMessage(q -> {
                        try {
                            q
                                    .queueUrl(queueUrl)
                                    .messageBody(objectMapper.writeValueAsString(errorResult));
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        }

        Thread.sleep(1000);

        ResponseEntity<String> getResponse = restTemplate.exchange(
                "/api/v1/result/" + requestId,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class
        );

        assertThat(getResponse.getStatusCode()).isEqualTo(HttpStatus.OK);

        Map<String, Object> result = objectMapper.readValue(getResponse.getBody(), Map.class);
        assertThat(result.get("status")).isEqualTo("error");
        assertThat(result.get("error")).isEqualTo("Failed to process request in FaaS");
    }
}