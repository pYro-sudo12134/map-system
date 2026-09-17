package collector

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeReader struct {
	length int32
	err    error
	calls  atomic.Int32
}

func (f *fakeReader) GetQueueLength() (int32, error) {
	f.calls.Add(1)
	return f.length, f.err
}

type fakeExporter struct {
	mu          sync.Mutex
	lengths     map[string]int32
	timestamps  map[string]float64
	errorCounts map[string]int
}

func newFakeExporter() *fakeExporter {
	return &fakeExporter{
		lengths:     make(map[string]int32),
		timestamps:  make(map[string]float64),
		errorCounts: make(map[string]int),
	}
}

func (f *fakeExporter) SetQueueLength(queueName string, length int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lengths[queueName] = length
}

func (f *fakeExporter) IncScrapeErrors(queueName string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errorCounts[queueName]++
}

func (f *fakeExporter) SetLastScrapeTimestamp(queueName string, timestamp float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.timestamps[queueName] = timestamp
}

func TestCollect_Success(t *testing.T) {
	reader := &fakeReader{length: 42}
	exporter := newFakeExporter()
	col := NewCollector(Config{QueueName: "SQS_GO", CheckInterval: time.Hour}, reader, exporter)

	col.collect()

	exporter.mu.Lock()
	defer exporter.mu.Unlock()

	if got := exporter.lengths["SQS_GO"]; got != 42 {
		t.Fatalf("SetQueueLength: expected 42, got %d", got)
	}
	if got := exporter.timestamps["SQS_GO"]; got == 0 {
		t.Fatal("SetLastScrapeTimestamp: expected non-zero timestamp")
	}
	if got := exporter.errorCounts["SQS_GO"]; got != 0 {
		t.Fatalf("IncScrapeErrors: expected 0, got %d", got)
	}
	if got := reader.calls.Load(); got != 1 {
		t.Fatalf("expected 1 reader call, got %d", got)
	}
}

func TestCollect_ReaderError(t *testing.T) {
	reader := &fakeReader{err: errors.New("connection refused")}
	exporter := newFakeExporter()
	col := NewCollector(Config{QueueName: "SQS_GO", CheckInterval: time.Hour}, reader, exporter)

	col.collect()

	exporter.mu.Lock()
	defer exporter.mu.Unlock()

	if got := exporter.errorCounts["SQS_GO"]; got != 1 {
		t.Fatalf("IncScrapeErrors: expected 1, got %d", got)
	}
	if _, exists := exporter.lengths["SQS_GO"]; exists {
		t.Fatal("SetQueueLength should not be called on error")
	}
	if _, exists := exporter.timestamps["SQS_GO"]; exists {
		t.Fatal("SetLastScrapeTimestamp should not be called on error")
	}
}

func TestCollect_ZeroLength(t *testing.T) {
	reader := &fakeReader{length: 0}
	exporter := newFakeExporter()
	col := NewCollector(Config{QueueName: "TEST", CheckInterval: time.Hour}, reader, exporter)

	col.collect()

	exporter.mu.Lock()
	defer exporter.mu.Unlock()

	if got := exporter.lengths["TEST"]; got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestStart_ImmediateCollectAndStop(t *testing.T) {
	reader := &fakeReader{length: 7}
	exporter := newFakeExporter()
	col := NewCollector(Config{QueueName: "SQS_GO", CheckInterval: time.Hour}, reader, exporter)

	col.Start()

	exporter.mu.Lock()
	got := exporter.lengths["SQS_GO"]
	exporter.mu.Unlock()
	if got != 7 {
		t.Fatalf("expected immediate collect with length 7, got %d", got)
	}

	col.Stop()
}

func TestStart_TickerPolling(t *testing.T) {
	reader := &fakeReader{length: 1}
	exporter := newFakeExporter()
	col := NewCollector(Config{QueueName: "SQS_GO", CheckInterval: 5 * time.Millisecond}, reader, exporter)

	col.Start()
	time.Sleep(50 * time.Millisecond)
	col.Stop()

	calls := int(reader.calls.Load())
	if calls < 2 {
		t.Fatalf("expected at least 2 collects (1 immediate + polling), got %d", calls)
	}
}

func TestStop_TwicePanics(t *testing.T) {
	reader := &fakeReader{length: 0}
	exporter := newFakeExporter()
	col := NewCollector(Config{QueueName: "Q", CheckInterval: time.Hour}, reader, exporter)

	col.Start()
	col.Stop()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on double stop")
		}
	}()
	col.Stop()
}
