package by.losik.maprouter.route;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.lang.NonNull;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = BaseIntegrationTest.Initializer.class)
public abstract class BaseIntegrationTest {

    @Container
    protected static final LocalStackContainer localstack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:latest")
    )
            .withServices(SQS, S3)
            .withEnv("AWS_ACCESS_KEY_ID", "test")
            .withEnv("AWS_SECRET_ACCESS_KEY", "test")
            .withEnv("AWS_DEFAULT_REGION", "us-east-1")
            .withEnv("SERVICES", "sqs,s3")
            .waitingFor(Wait.forLogMessage(".*Ready\\.\n", 1));

    @Container
    protected static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    static {
        redis.start();
        localstack.start();

        try { Thread.sleep(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        System.out.println("=== REDIS CONTAINER STARTED ===");
        System.out.println("Host: " + redis.getHost());
        System.out.println("Port: " + redis.getFirstMappedPort());
        System.out.println("================================");
    }

    protected static SqsClient createSqsClientStatic() {
        return SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(SQS))
                .region(Region.of("us-east-1"))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")
                ))
                .build();
    }

    protected static S3Client createS3ClientStatic() {
        return S3Client.builder()
                .endpointOverride(localstack.getEndpointOverride(S3))
                .region(Region.of("us-east-1"))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")
                ))
                .build();
    }

    protected static void createQueueStatic(String queueName) {
        try (SqsClient client = createSqsClientStatic()) {
            client.createQueue(q -> q.queueName(queueName));
        } catch (Exception e) {
            System.err.println("Failed to create queue " + queueName + ": " + e.getMessage());
        }
    }

    protected static void createBucketStatic(String bucketName) {
        try (S3Client client = createS3ClientStatic()) {
            client.createBucket(b -> b.bucket(bucketName));
        }
    }

    protected SqsClient createSqsClient() {
        return createSqsClientStatic();
    }

    static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(@NonNull ConfigurableApplicationContext context) {
            String redisHost = redis.getHost();
            int redisPort = redis.getFirstMappedPort();
            String sqsEndpoint = localstack.getEndpointOverride(SQS).toString();
            String s3Endpoint = localstack.getEndpointOverride(S3).toString();

            TestPropertyValues.of(
                    "aws.endpoint=" + sqsEndpoint,
                    "aws.region=us-east-1",
                    "aws.access-key=test",
                    "aws.secret-key=test",

                    "camel.component.aws2-sqs.override-endpoint=true",
                    "camel.component.aws2-sqs.uri-endpoint-override=" + sqsEndpoint,
                    "camel.component.aws2-s3.override-endpoint=true",
                    "camel.component.aws2-s3.uri-endpoint-override=" + s3Endpoint,
                    "camel.component.aws2-transcribe.override-endpoint=true",
                    "camel.component.aws2-transcribe.uri-endpoint-override=" + sqsEndpoint,

                    "spring.data.redis.host=" + redisHost,
                    "spring.data.redis.port=" + redisPort,
                    "spring.data.redis.timeout=5000ms",
                    "spring.data.redis.lettuce.pool.max-active=10",
                    "spring.data.redis.lettuce.pool.max-idle=5",
                    "spring.data.redis.lettuce.pool.min-idle=2",

                    "server.port=0",
                    "camel.servlet.mapping.context-path=/api/*",
                    "jwt.secret=testSecretKeyForJWTThatIsAtLeast32CharactersLong",

                    "rate.limit.user.global.max-requests=1000",
                    "rate.limit.user.global.time-period-ms=1000",
                    "rate.limit.user.get-result.max-requests=1000",
                    "rate.limit.user.get-result.time-period-ms=1000",
                    "rate.limit.admin.full-sync.max-requests=1000",
                    "rate.limit.admin.full-sync.time-period-ms=1000",
                    "rate.limit.admin.nodes-sync.max-requests=1000",
                    "rate.limit.admin.nodes-sync.time-period-ms=1000",
                    "rate.limit.admin.edges-sync.max-requests=1000",
                    "rate.limit.admin.edges-sync.time-period-ms=1000",
                    "rate.limit.sqs-consumer.max-requests=1000",
                    "rate.limit.sqs-consumer.time-period-ms=1000",

                    "resilience4j.circuitbreaker.instances.admin-sync.failure-rate-threshold=99",
                    "resilience4j.circuitbreaker.instances.transcribe.failure-rate-threshold=99",
                    "resilience4j.circuitbreaker.instances.s3-upload.failure-rate-threshold=99",
                    "resilience4j.circuitbreaker.instances.sqs-processor.failure-rate-threshold=99"
            ).applyTo(context);
        }
    }
}