package by.losik.maprouter.route;

import com.fasterxml.jackson.core.type.TypeReference;
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
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AdminRouteIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private String adminJwt;
    private String userJwt;

    @BeforeAll
    static void setupQueues() {
        createQueueStatic("SQS_RAG");
        createQueueStatic("SQS_RESULTS");
        createQueueStatic("SQS_SYNC");
        createBucketStatic("voice-records");
    }

    @BeforeEach
    void setUp() {
        adminJwt = generateToken("admin123", "ADMIN");
        userJwt = generateToken("user123", "USER");

        try (SqsClient sqsClient = createSqsClient()) {
            String queueUrl = sqsClient.getQueueUrl(q -> q.queueName("SQS_SYNC")).queueUrl();
            sqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(queueUrl).build());
        }
    }

    @Test
    void shouldAllowAdminToTriggerFullSync() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(adminJwt);

        HttpEntity<Void> request = new HttpEntity<>(headers);

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/admin/sync/full",
                request,
                String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).contains("full_sync_started");
    }

    @Test
    void shouldAllowAdminToTriggerNodesSync() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(adminJwt);

        HttpEntity<Void> request = new HttpEntity<>(headers);

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/admin/sync/nodes",
                request,
                String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).contains("nodes_sync_started");
    }

    @Test
    void shouldAllowAdminToTriggerEdgesSync() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(adminJwt);

        HttpEntity<Void> request = new HttpEntity<>(headers);

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/admin/sync/edges",
                request,
                String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).contains("edges_sync_started");
    }

    @Test
    void shouldDenyAccessToNonAdminUser() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(userJwt);

        HttpEntity<Void> request = new HttpEntity<>(headers);

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/admin/sync/full",
                request,
                String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    void shouldDenyAccessWithoutToken() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<Void> request = new HttpEntity<>(headers);

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/admin/sync/full",
                request,
                String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void shouldSendMessageToSyncQueue() throws Exception {
        try (SqsClient sqsClient = createSqsClient()) {
            String queueUrl = sqsClient.getQueueUrl(q -> q.queueName("SQS_SYNC")).queueUrl();

            sqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(queueUrl).build());

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(adminJwt);

            HttpEntity<Void> request = new HttpEntity<>(headers);
            ResponseEntity<String> response = restTemplate.postForEntity(
                    "/api/v1/admin/sync/full",
                    request,
                    String.class
            );

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            assertThat(response.getBody()).isNotNull();

            Thread.sleep(2000);

            List<Message> messages = sqsClient.receiveMessage(q -> q
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(5)
            ).messages();

            GetQueueAttributesResponse attrs = sqsClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                    .build());

            System.out.println("Messages in queue (approx): " + attrs.attributes()
                    .get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));

            assertThat(messages).isNotEmpty();

            String body = messages.get(0).body();
            assertThat(body).isNotNull();

            Map<String, Object> messageBody = objectMapper.readValue(body, new TypeReference<>() {});
            assertThat(messageBody.get("sync_type")).isEqualTo("full");
        }
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
}