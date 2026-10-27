//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/testcontainers/testcontainers-go"
	neo4jcontainer "github.com/testcontainers/testcontainers-go/modules/neo4j"
	oscontainer "github.com/testcontainers/testcontainers-go/modules/opensearch"
	"github.com/testcontainers/testcontainers-go/wait"
)

type localstackEnv struct {
	Endpoint  string
	TopicARN  string
	QueueURL  string
	SNSClient *sns.Client
	SQSClient *sqs.Client
}

func startLocalStack(t *testing.T, ctx context.Context) (*localstackEnv, func()) {
	req := testcontainers.ContainerRequest{
		Image:        "localstack/localstack:3.5",
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"SERVICES": "sns,sqs",
		},
		WaitingFor: wait.ForHTTP("/_localstack/health").
			WithPort("4566/tcp").
			WithStartupTimeout(90 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start localstack: %v", err)
	}

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("localstack host: %v", err)
	}
	port, err := c.MappedPort(ctx, "4566")
	if err != nil {
		t.Fatalf("localstack port: %v", err)
	}
	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	staticCreds := aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
	})

	snsClient := sns.New(sns.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		Credentials:  staticCreds,
	})
	sqsClient := sqs.New(sqs.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		Credentials:  staticCreds,
	})

	topic, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("test-topic")})
	if err != nil {
		t.Fatalf("create sns topic: %v", err)
	}

	queue, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("test-queue")})
	if err != nil {
		t.Fatalf("create sqs queue: %v", err)
	}
	qAttrs, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queue.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("get queue arn: %v", err)
	}
	queueARN := qAttrs.Attributes["QueueArn"]

	if _, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topic.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
	}); err != nil {
		t.Fatalf("subscribe sqs to sns: %v", err)
	}

	ls := &localstackEnv{
		Endpoint:  endpoint,
		TopicARN:  *topic.TopicArn,
		QueueURL:  *queue.QueueUrl,
		SNSClient: snsClient,
		SQSClient: sqsClient,
	}

	cleanup := func() { _ = c.Terminate(ctx) }
	return ls, cleanup
}

func receiveSNSMessage(t *testing.T, ctx context.Context, ls *localstackEnv, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := ls.SQSClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(ls.QueueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     2,
		})
		if err != nil {
			t.Fatalf("receive sqs message: %v", err)
		}
		if len(out.Messages) == 0 {
			continue
		}
		var envelope struct {
			Message string `json:"Message"`
		}
		if err := json.Unmarshal([]byte(*out.Messages[0].Body), &envelope); err != nil {
			t.Fatalf("decode sqs envelope: %v", err)
		}
		return envelope.Message
	}
	t.Fatalf("timed out waiting for SNS message on SQS after %s", timeout)
	return ""
}

func setupNeo4jOnly(t *testing.T, ctx context.Context) (neo4j.DriverWithContext, func()) {
	neo4jC, err := neo4jcontainer.Run(ctx,
		"neo4j:5.16",
		neo4jcontainer.WithAdminPassword("testpass"),
	)
	if err != nil {
		t.Fatalf("start neo4j: %v", err)
	}

	uri, err := neo4jC.BoltUrl(ctx)
	if err != nil {
		t.Fatalf("neo4j uri: %v", err)
	}

	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth("neo4j", "testpass", ""))
	if err != nil {
		t.Fatalf("neo4j driver: %v", err)
	}

	for i := 0; i < 30; i++ {
		sess := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
		_, err = sess.Run(ctx, "RETURN 1", nil)
		_ = sess.Close(ctx)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}

	cleanup := func() {
		_ = driver.Close(ctx)
		_ = neo4jC.Terminate(ctx)
	}
	return driver, cleanup
}

func setupTestInfra(t *testing.T) (neo4j.DriverWithContext, *oscontainer.OpenSearchContainer, *localstackEnv, context.Context, func()) {
	ctx := context.Background()

	neo4jC, err := neo4jcontainer.Run(ctx,
		"neo4j:5.16",
		neo4jcontainer.WithAdminPassword("testpass"),
	)
	if err != nil {
		t.Fatalf("start neo4j: %v", err)
	}

	uri, err := neo4jC.BoltUrl(ctx)
	if err != nil {
		t.Fatalf("neo4j uri: %v", err)
	}

	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth("neo4j", "testpass", ""))
	if err != nil {
		t.Fatalf("neo4j driver: %v", err)
	}

	for i := 0; i < 30; i++ {
		sess := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
		_, err = sess.Run(ctx, "RETURN 1", nil)
		_ = sess.Close(ctx)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}

	osC, err := oscontainer.Run(ctx, "opensearchproject/opensearch:2.11.0")
	if err != nil {
		t.Fatalf("start opensearch: %v", err)
	}

	ls, stopLS := startLocalStack(t, ctx)

	cleanup := func() {
		stopLS()
		_ = driver.Close(ctx)
		_ = neo4jC.Terminate(ctx)
		_ = osC.Terminate(ctx)
	}

	return driver, osC, ls, ctx, cleanup
}

func createOSClient(t *testing.T, c *oscontainer.OpenSearchContainer) *opensearch.Client {
	addr, err := c.Address(context.Background())
	if err != nil {
		addr = "http://localhost:9200"
	}
	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{addr},
		Username:  "admin",
		Password:  "admin",
	})
	if err != nil {
		t.Fatalf("os client: %v", err)
	}
	return client
}

func TestMapFaaS_ShortestPath(t *testing.T) {
	driver, osC, ls, ctx, cleanup := setupTestInfra(t)
	defer cleanup()

	SetOSClient(createOSClient(t, osC))
	SetNeo4jDriver(driver)
	SetNodesIndex("nodes")
	SetSNSClient(ls.SNSClient)
	SetSNSTopicARN(ls.TopicARN)

	sess := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer sess.Close(ctx)

	_, err := sess.Run(ctx, `
		CREATE (a:Node {name: 'A', lat: 0, lon: 0, type: 'node'}),
		       (b:Node {name: 'B', lat: 1, lon: 1, type: 'node'}),
		       (a)-[:ROAD {distance: 10, road_type: 'city', max_speed: 50, gradient: 0}]->(b)
	`, nil)
	if err != nil {
		t.Fatal(err)
	}

	req := map[string]interface{}{
		"request_id": "test-1",
		"parsed_query": map[string]interface{}{
			"query_type": "shortest_path",
			"params": map[string]interface{}{
				"from_ref": "A",
				"to_ref":   "B",
			},
		},
	}
	body, _ := json.Marshal(req)
	res, err := Handle(ctx, body)
	if err != nil {
		t.Fatal(err)
	}

	var resp Response
	if err := json.Unmarshal([]byte(res), &resp); err != nil {
		t.Fatalf("bad resp: %s, err: %v", res, err)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	if resp.Result == nil {
		t.Fatal("expected result")
	}

	raw := receiveSNSMessage(t, ctx, ls, 15*time.Second)
	var published struct {
		RequestID string          `json:"request_id"`
		Result    json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal([]byte(raw), &published); err != nil {
		t.Fatalf("bad SNS payload %q: %v", raw, err)
	}
	if published.RequestID != "test-1" {
		t.Fatalf("SNS request_id: want test-1, got %q", published.RequestID)
	}
	if len(published.Result) == 0 {
		t.Fatal("SNS result payload is empty")
	}
}

func TestMapFaaS_FilterBy_NoInjection(t *testing.T) {
	ctx := context.Background()

	driver, cleanup := setupNeo4jOnly(t, ctx)
	defer cleanup()

	SetNeo4jDriver(driver)

	sess := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer sess.Close(ctx)

	_, err := sess.Run(ctx, `
		CREATE (a:Node {name: 'A', lat: 0, lon: 0, type: 'node'}),
		       (b:Node {name: 'B', lat: 1, lon: 1, type: 'node'}),
		       (a)-[:ROAD {id: 'r1', distance: 10, road_type: 'city', max_speed: 50, gradient: 0}]->(b)
	`, nil)
	if err != nil {
		t.Fatal(err)
	}

	req := map[string]interface{}{
		"request_id": "test-2",
		"parsed_query": map[string]interface{}{
			"query_type": "filter_by",
			"params": map[string]interface{}{
				"road_type": "city' OR 1=1 WITH 1 as a MATCH (n) DETACH DELETE n //",
			},
		},
	}
	body, _ := json.Marshal(req)
	res, err := Handle(ctx, body)
	if err != nil {
		t.Fatal(err)
	}

	var resp Response
	if err := json.Unmarshal([]byte(res), &resp); err != nil {
		t.Fatalf("bad resp: %s", res)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}

	countRes, err := sess.Run(ctx, "MATCH (n) RETURN count(n) AS c", nil)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := countRes.Single(ctx)
	if err != nil {
		t.Fatal(err)
	}
	count, ok := rec.Values[0].(int64)
	if !ok {
		t.Fatalf("unexpected count type: %T", rec.Values[0])
	}
	if count != 2 {
		t.Fatalf("injection executed: expected 2 nodes, got %d", count)
	}
}
