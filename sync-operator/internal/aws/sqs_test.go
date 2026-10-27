package aws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func newTestReader(t *testing.T, srv *httptest.Server) *SQSReader {
	t.Helper()
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
		HTTPClient:  srv.Client(),
	}
	return NewSQSReader(cfg, "http://localstack:4566/000000000000/SQS_SYNC", srv.URL)
}

func serverWithAttributes(t *testing.T, attrs map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := json.Marshal(map[string]any{"Attributes": attrs})
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
}

func TestGetQueueLength_Valid(t *testing.T) {
	srv := serverWithAttributes(t, map[string]string{"ApproximateNumberOfMessages": "42"})
	defer srv.Close()

	length, err := newTestReader(t, srv).GetQueueLength(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if length != 42 {
		t.Fatalf("expected 42, got %d", length)
	}
}

func TestGetQueueLength_Zero(t *testing.T) {
	srv := serverWithAttributes(t, map[string]string{"ApproximateNumberOfMessages": "0"})
	defer srv.Close()

	length, err := newTestReader(t, srv).GetQueueLength(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if length != 0 {
		t.Fatalf("expected 0, got %d", length)
	}
}

func TestGetQueueLength_MissingAttribute(t *testing.T) {
	srv := serverWithAttributes(t, map[string]string{})
	defer srv.Close()

	length, err := newTestReader(t, srv).GetQueueLength(context.Background())
	if err == nil {
		t.Fatalf("expected error for missing attribute, got length %d", length)
	}
	if length != 0 {
		t.Fatalf("expected length 0 on error, got %d", length)
	}
}

func TestGetQueueLength_NonNumeric(t *testing.T) {
	srv := serverWithAttributes(t, map[string]string{"ApproximateNumberOfMessages": "abc"})
	defer srv.Close()

	length, err := newTestReader(t, srv).GetQueueLength(context.Background())
	if err == nil {
		t.Fatalf("expected error for non-numeric value, got length %d", length)
	}
	if length != 0 {
		t.Fatalf("expected length 0 on error, got %d", length)
	}
}

func TestGetQueueLength_ApiError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"__type":"InternalServerError","message":"boom"}`))
	}))
	defer srv.Close()

	length, err := newTestReader(t, srv).GetQueueLength(context.Background())
	if err == nil {
		t.Fatalf("expected error for 5xx response, got length %d", length)
	}
	if length != 0 {
		t.Fatalf("expected length 0 on error, got %d", length)
	}
}

func TestGetQueueLength_ContextCanceled(t *testing.T) {
	srv := serverWithAttributes(t, map[string]string{"ApproximateNumberOfMessages": "1"})
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	length, err := newTestReader(t, srv).GetQueueLength(ctx)
	if err == nil {
		t.Fatalf("expected context error, got length %d", length)
	}
}
