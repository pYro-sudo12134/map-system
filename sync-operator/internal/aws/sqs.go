package aws

import (
	"context"
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSReader struct {
	client   *sqs.Client
	queueURL string
}

func NewSQSReader(cfg aws.Config, queueURL string) *SQSReader {
	return &SQSReader{
		client:   sqs.NewFromConfig(cfg),
		queueURL: queueURL,
	}
}

func (r *SQSReader) GetQueueLength() (int32, error) {
	input := &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(r.queueURL),
		AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessages"},
	}

	result, err := r.client.GetQueueAttributes(context.Background(), input)
	if err != nil {
		log.Printf("Failed to get queue attributes: %v", err)
		return 0, err
	}

	lengthStr := result.Attributes["ApproximateNumberOfMessages"]
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		log.Printf("Failed to parse queue length: %v", err)
		return 0, err
	}

	return int32(length), nil
}
