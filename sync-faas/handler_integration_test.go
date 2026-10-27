//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/testcontainers/testcontainers-go"
	neo4jcontainer "github.com/testcontainers/testcontainers-go/modules/neo4j"
	oscontainer "github.com/testcontainers/testcontainers-go/modules/opensearch"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startLocalStackForSQS(t *testing.T, ctx context.Context) (*sqs.Client, string, func()) {
	req := testcontainers.ContainerRequest{
		Image:        "localstack/localstack:3.5",
		ExposedPorts: []string{"4566/tcp"},
		Env:          map[string]string{"SERVICES": "sqs"},
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

	client := sqs.New(sqs.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	})

	q, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("SQS_SYNC")})
	if err != nil {
		t.Fatalf("create sqs queue: %v", err)
	}

	cleanup := func() { _ = c.Terminate(ctx) }
	return client, *q.QueueUrl, cleanup
}

func setupSyncInfra(t *testing.T) (neo4j.DriverWithContext, *oscontainer.OpenSearchContainer, *sqs.Client, string, context.Context, func()) {
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

	sqsCl, qURL, stopLS := startLocalStackForSQS(t, ctx)

	cleanup := func() {
		stopLS()
		_ = driver.Close(ctx)
		_ = neo4jC.Terminate(ctx)
		_ = osC.Terminate(ctx)
	}

	return driver, osC, sqsCl, qURL, ctx, cleanup
}

func createOSClientSync(t *testing.T, c *oscontainer.OpenSearchContainer) *opensearch.Client {
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

func receiveUntilEmpty(t *testing.T, ctx context.Context, client *sqs.Client, queueURL string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		})
		if err != nil {
			t.Fatalf("receive sqs message: %v", err)
		}
		if len(out.Messages) == 0 {
			return
		}
		_, _ = client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(queueURL),
			ReceiptHandle:     out.Messages[0].ReceiptHandle,
			VisibilityTimeout: 0,
		})
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("message was not deleted from queue within %s", timeout)
}

func TestSyncFaaS_ConsumesFullSyncAndDeletes(t *testing.T) {
	driver, osC, sqsCl, qURL, ctx, cleanup := setupSyncInfra(t)
	defer cleanup()

	ResetIndicesEnsured()
	testOSClient := createOSClientSync(t, osC)
	SetOSClient(testOSClient)
	SetNeo4jDriver(driver)
	SetNodesIndex("nodes")
	SetEdgesIndex("edges")
	SetSQSClient(sqsCl)
	SetSyncQueueURL(qURL)
	SetVisibilityTimeout(1)
	ResetIndicesEnsured()
	sess := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer sess.Close(ctx)

	_, err := sess.Run(ctx, `
		CREATE (a:Node {id: 'n1', name: 'A', lat: 0.0, lon: 0.0, type: 'junction'}),
		       (b:Node {id: 'n2', name: 'B', lat: 1.0, lon: 1.0, type: 'junction'}),
		       (a)-[:ROAD {id: 'e1', distance: 10.0, road_type: 'city', max_speed: 50, gradient: 0.0}]->(b)
	`, nil)
	if err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(SyncRequest{SyncType: "full"})
	if _, err := sqsCl.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(qURL),
		MessageBody: aws.String(string(body)),
	}); err != nil {
		t.Fatalf("send sqs message: %v", err)
	}

	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	processed, err := ConsumeOnce(cctx)
	if err != nil {
		t.Fatalf("ConsumeOnce: %v", err)
	}
	if !processed {
		t.Fatal("expected a message to be processed, got none")
	}

	time.Sleep(2 * time.Second)
	receiveUntilEmpty(t, ctx, sqsCl, qURL, 5*time.Second)

	for _, idx := range []string{"nodes", "edges"} {
		refreshRes, err := testOSClient.Indices.Refresh(
			testOSClient.Indices.Refresh.WithIndex(idx),
		)
		if err != nil {
			t.Fatalf("refresh %s: %v", idx, err)
		}
		_ = refreshRes.Body.Close()

		res, err := testOSClient.Count(testOSClient.Count.WithIndex(idx))
		if err != nil {
			t.Fatalf("count %s: %v", idx, err)
		}
		var cr struct {
			Count int64 `json:"count"`
		}
		if err := json.NewDecoder(res.Body).Decode(&cr); err != nil {
			_ = res.Body.Close()
			t.Fatalf("decode count %s: %v", idx, err)
		}
		_ = res.Body.Close()
		if cr.Count == 0 {
			t.Fatalf("index %s is empty after sync", idx)
		}
	}
}

func TestSyncFaaS_FailedSyncDoesNotDelete(t *testing.T) {
	driver, osC, sqsCl, qURL, ctx, cleanup := setupSyncInfra(t)
	defer cleanup()

	ResetIndicesEnsured()
	SetOSClient(createOSClientSync(t, osC))
	SetNeo4jDriver(driver)
	SetNodesIndex("nodes")
	SetEdgesIndex("edges")
	SetSQSClient(sqsCl)
	SetSyncQueueURL(qURL)
	SetVisibilityTimeout(1)

	body := `{"sync_type":"totally-bogus"}`
	if _, err := sqsCl.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(qURL),
		MessageBody: aws.String(body),
	}); err != nil {
		t.Fatalf("send sqs message: %v", err)
	}

	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	processed, err := ConsumeOnce(cctx)
	if err != nil {
		t.Fatalf("ConsumeOnce: %v", err)
	}
	if !processed {
		t.Fatal("expected a message to be processed, got none")
	}

	deadline := time.Now().Add(15 * time.Second)
	var redelivered bool
	for time.Now().Before(deadline) {
		out, err := sqsCl.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		})
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		if len(out.Messages) > 0 {
			redelivered = true
			_, _ = sqsCl.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(qURL),
				ReceiptHandle: out.Messages[0].ReceiptHandle,
			})
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !redelivered {
		t.Fatal("message was not redelivered after failed sync; ConsumeOnce may have deleted it")
	}
}
