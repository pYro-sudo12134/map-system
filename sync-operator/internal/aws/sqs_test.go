package aws

import (
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
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
		BaseEndpoint: aws.String(srv.URL),
		HTTPClient:   srv.Client(),
	}
	return NewSQSReader(cfg, "http://localstack:4566/000000000000/SQS_SYNC")
}

func serverWithAttributes(attrs map[string]string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := json.Marshal(map[string]any{"Attributes": attrs})
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
}

func TestGetQueueLength_Valid(t *testing.T) {
	srv := serverWithAttributes(map[string]string{"ApproximateNumberOfMessages": "42"})
	defer srv.Close()

	reader := newTestReader(t, srv)

	length, err := reader.GetQueueLength()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if length != 42 {
		t.Fatalf("expected 42, got %d", length)
	}
}

func TestGetQueueLength_Zero(t *testing.T) {
	srv := serverWithAttributes(map[string]string{"ApproximateNumberOfMessages": "0"})
	defer srv.Close()

	reader := newTestReader(t, srv)

	length, err := reader.GetQueueLength()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if length != 0 {
		t.Fatalf("expected 0, got %d", length)
	}
}

func TestGetQueueLength_MissingAttribute(t *testing.T) {
	srv := serverWithAttributes(map[string]string{})
	defer srv.Close()

	reader := newTestReader(t, srv)

	length, err := reader.GetQueueLength()
	if err == nil {
		t.Fatalf("expected error for missing attribute, got length %d", length)
	}
	if length != 0 {
		t.Fatalf("expected length 0 on error, got %d", length)
	}
}

func TestGetQueueLength_NonNumeric(t *testing.T) {
	srv := serverWithAttributes(map[string]string{"ApproximateNumberOfMessages": "abc"})
	defer srv.Close()

	reader := newTestReader(t, srv)

	length, err := reader.GetQueueLength()
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
		w.Write([]byte(`{"__type":"InternalServerError","message":"boom"}`))
	}))
	defer srv.Close()

	reader := newTestReader(t, srv)

	length, err := reader.GetQueueLength()
	if err == nil {
		t.Fatalf("expected error for 5xx response, got length %d", length)
	}
	if length != 0 {
		t.Fatalf("expected length 0 on error, got %d", length)
	}
}
