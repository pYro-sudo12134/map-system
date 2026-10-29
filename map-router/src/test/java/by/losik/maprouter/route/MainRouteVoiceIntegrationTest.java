package by.losik.maprouter.route;

import by.losik.maprouter.processor.PollTranscriptionProcessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;

import javax.crypto.SecretKey;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@Import(MainRouteVoiceIntegrationTest.TranscribeMockConfig.class)
class MainRouteVoiceIntegrationTest extends BaseIntegrationTest {

    private static final String JWT_SECRET = "testSecretKeyForJWTThatIsAtLeast32CharactersLong";
    private static final String RAG_QUEUE = "SQS_RAG";
    private static final String BUCKET = "voice-records";

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private String validJwt;

    @BeforeAll
    static void setupInfra() {
        createQueueStatic(RAG_QUEUE);
        createQueueStatic("SQS_RESULTS");
        createQueueStatic("SQS_SYNC");
        createBucketStatic(BUCKET);
    }

    @BeforeEach
    void setUp() {
        validJwt = generateToken("user123", "USER");
        try (SqsClient sqs = createSqsClient()) {
            String url = sqs.getQueueUrl(q -> q.queueName(RAG_QUEUE)).queueUrl();
            sqs.purgeQueue(PurgeQueueRequest.builder().queueUrl(url).build());
        }
        createBucketStatic(BUCKET);
    }

    @Test
    void textRequestShouldGoStraightToSqs() throws Exception {
        HttpHeaders headers = jsonAuthHeaders();
        Map<String, String> body = Map.of(
                "text", "как доехать до центра",
                "language", "ru"
        );

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/route", new HttpEntity<>(body, headers), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        String requestId = extractRequestId(response.getBody());

        Map<String, Object> msg = awaitSqsMessage(RAG_QUEUE, 30);
        assertThat(msg.get("request_id")).isEqualTo(requestId);
        assertThat(msg.get("text")).isEqualTo("как доехать до центра");
        assertThat(msg.get("user_id")).isEqualTo("user123");
        assertThat(msg.get("language")).isEqualTo("ru");
        assertThat(msg).doesNotContainKey("error");
    }

    @Test
    void voiceRequestShouldTranscribeAndSendToSqs() throws Exception {
        String voiceB64 = loadSampleWavAsBase64();

        HttpHeaders headers = jsonAuthHeaders();
        Map<String, String> body = Map.of(
                "voice", voiceB64,
                "language", "ru"
        );

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/route", new HttpEntity<>(body, headers), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        String requestId = extractRequestId(response.getBody());
        assertThat(requestId).isNotBlank();

        try (S3Client s3 = createS3ClientStatic()) {
            var list = s3.listObjectsV2(b -> b.bucket(BUCKET).prefix("temp/" + requestId + "/"));
            assertThat(list.contents())
                    .as("audio file must be uploaded to S3")
                    .isNotEmpty();
            var obj = s3.getObject(b -> b.bucket(BUCKET).key(list.contents().get(0).key()));
            assertThat(obj.response().contentLength())
                    .as("uploaded file must not be empty")
                    .isGreaterThan(44);
        }

        Map<String, Object> msg = awaitSqsMessage(RAG_QUEUE, 90);

        assertThat(msg.get("request_id")).isEqualTo(requestId);
        assertThat(msg.get("user_id")).isEqualTo("user123");
        assertThat(msg.get("language")).isEqualTo("ru");
        assertThat(msg)
                .as("successful transcribe path must not contain 'error'")
                .doesNotContainKey("error");
        assertThat(msg.get("text")).asString()
                .as("transcribed text must be non-empty")
                .isNotBlank();
    }

    @Test
    void voiceRequestShouldFallbackWhenS3UploadFails() throws Exception {
        deleteBucketAndObjects(BUCKET);
        try {
            String voiceB64 = loadSampleWavAsBase64();
            HttpHeaders headers = jsonAuthHeaders();
            Map<String, String> body = Map.of("voice", voiceB64, "language", "ru");

            ResponseEntity<String> response = restTemplate.postForEntity(
                    "/api/v1/route", new HttpEntity<>(body, headers), String.class);

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            String requestId = extractRequestId(response.getBody());

            Map<String, Object> msg = awaitSqsMessage(RAG_QUEUE, 30);
            assertThat(msg.get("request_id")).isEqualTo(requestId);
            assertThat(msg.get("error")).isEqualTo("s3_upload_unavailable");
            assertThat(msg.get("language")).isEqualTo("ru");
        } finally {
            createBucketStatic(BUCKET);
        }
    }

    @Test
    void voiceRequestShouldFallbackWhenTranscribeFails() throws Exception {
        String voiceB64 = loadSampleWavAsBase64();

        HttpHeaders headers = jsonAuthHeaders();
        Map<String, String> body = Map.of(
                "voice", voiceB64,
                "language", "xx-XX"
        );

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/route", new HttpEntity<>(body, headers), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        String requestId = extractRequestId(response.getBody());

        Map<String, Object> msg = awaitSqsMessage(RAG_QUEUE, 30);
        assertThat(msg.get("request_id")).isEqualTo(requestId);
        assertThat(msg.get("error")).isEqualTo("transcription_unavailable");
        assertThat(msg.get("language")).isEqualTo("xx-XX");
    }

    @Test
    void invalidBase64VoiceShouldReturn400() {
        HttpHeaders headers = jsonAuthHeaders();
        Map<String, String> body = Map.of("voice", "!!!not-base64!!!", "language", "ru");

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/route", new HttpEntity<>(body, headers), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).contains("error");
    }

    private HttpHeaders jsonAuthHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(validJwt);
        return headers;
    }

    private String generateToken(String userId, String role) {
        SecretKey key = Keys.hmacShaKeyFor(JWT_SECRET.getBytes(StandardCharsets.UTF_8));
        return Jwts.builder()
                .subject(userId)
                .claim("role", role)
                .expiration(new Date(System.currentTimeMillis() + 3_600_000))
                .signWith(key)
                .compact();
    }

    private String extractRequestId(String body) {
        var p = java.util.regex.Pattern.compile("\"request_id\"\\s*:\\s*\"([^\"]+)\"");
        var m = p.matcher(body);
        if (!m.find()) throw new AssertionError("No request_id in body: " + body);
        return m.group(1);
    }

    private Map<String, Object> awaitSqsMessage(String queueName, int timeoutSeconds) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            try (SqsClient sqs = createSqsClient()) {
                String url = sqs.getQueueUrl(q -> q.queueName(queueName)).queueUrl();
                List<Message> messages = sqs.receiveMessage(r -> r
                        .queueUrl(url)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(2)
                        .visibilityTimeout(5)
                ).messages();
                if (!messages.isEmpty()) {
                    return objectMapper.readValue(messages.get(0).body(), new TypeReference<>() {});
                }
            }
            Thread.sleep(500);
        }
        throw new AssertionError("No message in queue " + queueName + " within " + timeoutSeconds + "s");
    }

    private static String loadSampleWavAsBase64() throws Exception {
        try (InputStream in = MainRouteVoiceIntegrationTest.class
                .getClassLoader().getResourceAsStream("test.wav")) {
            if (in == null) {
                throw new IllegalStateException("test.wav not found in test resources");
            }
            return Base64.getEncoder().encodeToString(in.readAllBytes());
        }
    }

    private void deleteBucketAndObjects(String bucket) {
        try (S3Client s3 = createS3ClientStatic()) {
            var list = s3.listObjectsV2(b -> b.bucket(bucket));
            for (var obj : list.contents()) {
                s3.deleteObject(b -> b.bucket(bucket).key(obj.key()));
            }
            s3.deleteBucket(b -> b.bucket(bucket));
        } catch (Exception ignored) {
        }
    }

    @TestConfiguration
    static class TranscribeMockConfig {
        @Bean
        @Primary
        PollTranscriptionProcessor mockPollTranscriptionProcessor() {
            return new PollTranscriptionProcessor() {
                @Override
                public void process(org.apache.camel.Exchange exchange) {
                    exchange.setProperty("transcriptUri", null);
                    exchange.getMessage().setBody("как доехать до центра");
                    exchange.setProperty("transcriptionPending", false);
                }
            };
        }

        @Bean
        @Primary
        S3Client mockS3Client(S3Client realS3Client) {
            S3Client mock = Mockito.mock(S3Client.class);

            Mockito.when(mock.getObjectAsBytes((GetObjectRequest) Mockito.argThat((GetObjectRequest req) ->
                            req != null && req.key() != null && req.key().startsWith("transcribe-"))))
                    .thenAnswer(inv -> {
                        String body = """
                            {"jobName":"ignored","status":"COMPLETED",
                             "results":{"transcripts":[{"transcript":"как доехать до центра"}],
                                        "items":[]}}
                            """;
                        return ResponseBytes.fromByteArray(
                                GetObjectResponse.builder().build(),
                                body.getBytes(StandardCharsets.UTF_8));
                    });

            Mockito.when(mock.getObjectAsBytes((GetObjectRequest) Mockito.argThat((GetObjectRequest req) ->
                            req != null && (req.key() == null || !req.key().startsWith("transcribe-")))))
                    .thenAnswer(inv -> realS3Client.getObjectAsBytes((GetObjectRequest) inv.getArgument(0)));

            Mockito.when(mock.getObject((GetObjectRequest) Mockito.any()))
                    .thenAnswer(inv -> realS3Client.getObject((GetObjectRequest) inv.getArgument(0)));

            Mockito.when(mock.listObjectsV2((ListObjectsV2Request) Mockito.any()))
                    .thenAnswer(inv -> realS3Client.listObjectsV2((ListObjectsV2Request) inv.getArgument(0)));

            Mockito.when(mock.putObject((PutObjectRequest) Mockito.any(), Mockito.any(RequestBody.class)))
                    .thenAnswer(inv -> realS3Client.putObject((PutObjectRequest) inv.getArgument(0), (RequestBody) inv.getArgument(1)));

            return mock;
        }
    }
}