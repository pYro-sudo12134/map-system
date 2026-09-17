package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestSetQueueLength(t *testing.T) {
	SetQueueLength("SQS_GO", 42)

	if got := testutil.ToFloat64(QueueLength.WithLabelValues("SQS_GO")); got != 42 {
		t.Fatalf("expected 42, got %v", got)
	}

	SetQueueLength("SQS_GO", 7)

	if got := testutil.ToFloat64(QueueLength.WithLabelValues("SQS_GO")); got != 7 {
		t.Fatalf("expected 7 after update, got %v", got)
	}

	SetQueueLength("OTHER", 5)

	if got := testutil.ToFloat64(QueueLength.WithLabelValues("SQS_GO")); got != 7 {
		t.Fatalf("labels must be isolated; expected 7 for SQS_GO, got %v", got)
	}
}

func TestIncScrapeErrors(t *testing.T) {
	IncScrapeErrors("SQS_GO")
	IncScrapeErrors("SQS_GO")

	if got := testutil.ToFloat64(ScrapeErrors.WithLabelValues("SQS_GO")); got != 2 {
		t.Fatalf("expected 2, got %v", got)
	}

	IncScrapeErrors("OTHER")

	if got := testutil.ToFloat64(ScrapeErrors.WithLabelValues("SQS_GO")); got != 2 {
		t.Fatalf("labels must be isolated; expected 2 for SQS_GO, got %v", got)
	}
}

func TestSetLastScrapeTimestamp(t *testing.T) {
	SetLastScrapeTimestamp("SQS_GO", 1234.5)

	if got := testutil.ToFloat64(LastScrapeTimestamp.WithLabelValues("SQS_GO")); got != 1234.5 {
		t.Fatalf("expected 1234.5, got %v", got)
	}
}
