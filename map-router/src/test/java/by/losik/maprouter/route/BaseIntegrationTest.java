package by.losik.maprouter.route;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
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
            .withEnv("AWS_DEFAULT_REGION", "us-east-1");

    @Container
    protected static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    static {
        redis.start();
        localstack.start();
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
        SqsClient client = createSqsClientStatic();
        client.createQueue(q -> q.queueName(queueName));
        client.close();
    }

    protected static void createBucketStatic(String bucketName) {
        S3Client client = createS3ClientStatic();
        client.createBucket(b -> b.bucket(bucketName));
        client.close();
    }

    protected SqsClient createSqsClient() {
        return createSqsClientStatic();
    }

    static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext context) {
            String redisHost = redis.getHost();
            int redisPort = redis.getFirstMappedPort();
            String localstackEndpoint = localstack.getEndpointOverride(SQS).toString();

            String sqsEndpoint = localstack.getEndpointOverride(SQS).toString();

            TestPropertyValues.of(
                    "aws.endpoint=" + localstackEndpoint,
                    "aws.region=us-east-1",
                    "aws.access-key=test",
                    "aws.secret-key=test",

                    "camel.component.aws2-sqs.override-endpoint=true",
                    "camel.component.aws2-sqs.uri-endpoint-override=" + sqsEndpoint,
                    "camel.component.aws2-sqs.endpoint-override=" + sqsEndpoint,

                    "camel.component.aws2-s3.override-endpoint=true",
                    "camel.component.aws2-s3.uri-endpoint-override=" + localstack.getEndpointOverride(S3).toString(),

                    "camel.component.aws2-transcribe.override-endpoint=true",
                    "camel.component.aws2-transcribe.uri-endpoint-override=" + localstackEndpoint,

                    "spring.data.redis.host=" + redisHost,
                    "spring.data.redis.port=" + redisPort,
                    "spring.data.redis.password=",
                    "server.port=0",
                    "camel.servlet.mapping.context-path=/api/*",

                    "aws.sqs.endpoint=" + sqsEndpoint,
                    "jwt.secret=testSecretKeyForJWTThatIsAtLeast32CharactersLong"
            ).applyTo(context);
        }
    }
}