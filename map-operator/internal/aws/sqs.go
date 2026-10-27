package aws

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSReader struct {
	client   *sqs.Client
	queueURL string
}

func NewSQSReader(cfg aws.Config, queueURL, endpoint string) *SQSReader {
	var opts []func(*sqs.Options)
	if endpoint != "" {
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
			o.BaseEndpoint = aws.String(endpoint)
		})
	}

	return &SQSReader{
		client:   sqs.NewFromConfig(cfg, opts...),
		queueURL: queueURL,
	}
}

func (r *SQSReader) GetQueueLength(ctx context.Context) (int32, error) {
	input := &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(r.queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	}

	result, err := r.client.GetQueueAttributes(ctx, input)
	if err != nil {
		return 0, fmt.Errorf("get queue attributes: %w", err)
	}

	lengthStr, ok := result.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
	if !ok {
		return 0, fmt.Errorf("ApproximateNumberOfMessages attribute missing in response")
	}

	length, err := strconv.ParseInt(lengthStr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse ApproximateNumberOfMessages %q: %w", lengthStr, err)
	}

	return int32(length), nil
}
