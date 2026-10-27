package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

var (
	sqsClient         *sqs.Client
	goQueueURL        string
	visibilityTimeout int32
)

func SetSQSClient(c *sqs.Client)   { sqsClient = c }
func SetGoQueueURL(u string)       { goQueueURL = u }
func SetVisibilityTimeout(v int32) { visibilityTimeout = v }

func init() {
	goQueueURL = os.Getenv("SQS_GO_QUEUE_URL")
	awsEndpoint := os.Getenv("AWS_ENDPOINT_URL")

	visibilityTimeout = 30
	if v := os.Getenv("SQS_VISIBILITY_TIMEOUT"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil && n > 0 {
			visibilityTimeout = int32(n)
		} else {
			log.Printf("Invalid SQS_VISIBILITY_TIMEOUT=%q, using default %d", v, visibilityTimeout)
		}
	}

	if goQueueURL == "" && awsEndpoint == "" {
		return
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Printf("Failed to load AWS config: %v", err)
		return
	}

	var opts []func(*sqs.Options)
	if awsEndpoint != "" {
		cfg.Credentials = aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				Source:          "static",
			}, nil
		})
		if cfg.Region == "" {
			cfg.Region = "us-east-1"
		}
		opts = append(opts, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(awsEndpoint)
		})
	}

	sqsClient = sqs.NewFromConfig(cfg, opts...)
}

func ConsumeOnce(ctx context.Context) (bool, error) {
	if sqsClient == nil || goQueueURL == "" {
		return false, fmt.Errorf("SQS is not configured (SQS_GO_QUEUE_URL unset and no client)")
	}

	out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(goQueueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     20,
		VisibilityTimeout:   visibilityTimeout,
	})
	if err != nil {
		return false, fmt.Errorf("receive message: %w", err)
	}
	if len(out.Messages) == 0 {
		return false, nil
	}

	msg := out.Messages[0]
	body := aws.ToString(msg.Body)

	log.Printf("Received map request: %s", body)

	resp, handleErr := Handle(ctx, []byte(body))
	if handleErr != nil {
		return true, fmt.Errorf("handle: %w", handleErr)
	}

	var envelope struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(resp), &envelope); err != nil {
		return true, fmt.Errorf("decode handler response %q: %w", resp, err)
	}

	if envelope.Error != "" {
		log.Printf("Map request failed (%s), leaving message for redelivery: %s",
			envelope.Error, resp)
		return true, nil
	}

	if _, err := sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(goQueueURL),
		ReceiptHandle: msg.ReceiptHandle,
	}); err != nil {
		return true, fmt.Errorf("delete message: %w", err)
	}

	log.Printf("Map request completed, message deleted")
	return true, nil
}
