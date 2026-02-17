package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"better-cdc/internal/metrics"
	"better-cdc/internal/model"
	"better-cdc/internal/parser"
	"better-cdc/internal/publisher"
	"better-cdc/internal/wal"

	"go.uber.org/zap"
)

// testMetrics is a singleton to avoid duplicate Prometheus registration
var (
	testMetrics     *metrics.Metrics
	testMetricsOnce sync.Once
)

func getTestMetrics() *metrics.Metrics {
	testMetricsOnce.Do(func() {
		testMetrics = metrics.GlobalMetrics
	})
	return testMetrics
}

// mockBatchPublisher implements publisher.BatchPublisher for testing retry logic
type mockBatchPublisher struct {
	// Track call counts
	publishBatchCalls atomic.Int32
	waitForAcksCalls  atomic.Int32

	// Configure behavior per attempt
	failuresPerAttempt [][]int // indices of items to fail per attempt
	publishBatchErrors []error // error to return from PublishBatchAsync per attempt
}

func newMockBatchPublisher() *mockBatchPublisher {
	return &mockBatchPublisher{
		failuresPerAttempt: [][]int{},
		publishBatchErrors: []error{},
	}
}

func (m *mockBatchPublisher) Connect() error { return nil }
func (m *mockBatchPublisher) Close() error   { return nil }
func (m *mockBatchPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	return nil
}
func (m *mockBatchPublisher) PublishWithRetries(ctx context.Context, subject string, data []byte, maxRetries int) error {
	return nil
}

func (m *mockBatchPublisher) PublishBatchAsync(ctx context.Context, items []publisher.PublishItem) ([]*publisher.PendingAck, error) {
	attempt := int(m.publishBatchCalls.Add(1)) - 1

	// Return error if configured for this attempt
	if attempt < len(m.publishBatchErrors) && m.publishBatchErrors[attempt] != nil {
		return nil, m.publishBatchErrors[attempt]
	}

	pending := make([]*publisher.PendingAck, len(items))
	for i := range items {
		pending[i] = publisher.NewPendingAck(items[i].Subject, items[i].EventID, items[i].TxID)
	}
	return pending, nil
}

func (m *mockBatchPublisher) WaitForAcks(ctx context.Context, pending []*publisher.PendingAck, items []publisher.PublishItem, timeout time.Duration) (*publisher.BatchResult, error) {
	attempt := int(m.waitForAcksCalls.Add(1)) - 1

	result := &publisher.BatchResult{
		Total:       len(pending),
		FailedItems: make([]int, 0),
	}

	// Determine which items fail based on attempt
	failSet := make(map[int]bool)
	if attempt < len(m.failuresPerAttempt) {
		for _, idx := range m.failuresPerAttempt[attempt] {
			failSet[idx] = true
		}
	}

	var lastContiguousIdx = -1
	var contiguousBroken bool
	for i, pend := range pending {
		if failSet[i] {
			result.Failed++
			result.FailedItems = append(result.FailedItems, i)
			pend.SetError(errors.New("mock failure"))
			if result.FirstError == nil {
				result.FirstError = errors.New("mock failure")
			}
			contiguousBroken = true
		} else {
			pend.SetAcked(true)
			result.Succeeded++
			if !contiguousBroken {
				lastContiguousIdx = i
			}
		}
		pend.Close()
	}

	if lastContiguousIdx >= 0 && len(items) > lastContiguousIdx {
		pos := items[lastContiguousIdx].Position
		result.LastSuccessPosition = &pos
	}

	var err error
	if result.Failed > 0 {
		err = result.FirstError
	}
	return result, err
}

func TestPublishWithRetry_AllSucceedFirstAttempt(t *testing.T) {
	mock := newMockBatchPublisher()
	// No failures configured = all succeed

	e := &Engine{
		logger:       zap.NewNop(),
		batchTimeout: time.Second,
		promMetrics:  getTestMetrics(),
	}

	items := []publisher.PublishItem{
		{Subject: "test.1", EventID: "1", Position: model.WALPosition{LSN: "0/1"}},
		{Subject: "test.2", EventID: "2", Position: model.WALPosition{LSN: "0/2"}},
		{Subject: "test.3", EventID: "3", Position: model.WALPosition{LSN: "0/3"}},
	}

	result, err := e.publishWithRetry(context.Background(), mock, items)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if result.Succeeded != 3 {
		t.Errorf("expected 3 succeeded, got %d", result.Succeeded)
	}
	if result.Failed != 0 {
		t.Errorf("expected 0 failed, got %d", result.Failed)
	}
	if mock.publishBatchCalls.Load() != 1 {
		t.Errorf("expected 1 publish call, got %d", mock.publishBatchCalls.Load())
	}
}

func TestPublishWithRetry_PartialFailureRecovery(t *testing.T) {
	mock := newMockBatchPublisher()
	// First attempt: items 1 and 2 fail
	// Second attempt: item 1 fails (only 1 and 2 are retried)
	// Third attempt: all succeed
	mock.failuresPerAttempt = [][]int{
		{1, 2}, // First: fail items at index 1, 2
		{0},    // Second: fail item at index 0 (which is original item 1)
		{},     // Third: all succeed
	}

	e := &Engine{
		logger:       zap.NewNop(),
		batchTimeout: time.Second,
		promMetrics:  getTestMetrics(),
	}

	items := []publisher.PublishItem{
		{Subject: "test.0", EventID: "0", Position: model.WALPosition{LSN: "0/0"}},
		{Subject: "test.1", EventID: "1", Position: model.WALPosition{LSN: "0/1"}},
		{Subject: "test.2", EventID: "2", Position: model.WALPosition{LSN: "0/2"}},
	}

	result, err := e.publishWithRetry(context.Background(), mock, items)

	if err != nil {
		t.Errorf("expected no error after retry, got %v", err)
	}
	if result.Succeeded != 3 {
		t.Errorf("expected 3 succeeded, got %d", result.Succeeded)
	}
	if result.Failed != 0 {
		t.Errorf("expected 0 failed after retries, got %d", result.Failed)
	}
	// Should have 3 attempts (initial + 2 retries)
	if mock.publishBatchCalls.Load() != 3 {
		t.Errorf("expected 3 publish calls, got %d", mock.publishBatchCalls.Load())
	}
}

func TestPublishWithRetry_ExhaustedRetries(t *testing.T) {
	mock := newMockBatchPublisher()
	// All attempts fail item 1
	mock.failuresPerAttempt = [][]int{
		{1}, // First: fail item 1
		{0}, // Second: fail (item 1 is now at index 0)
		{0}, // Third: still fail
		{0}, // Fourth: still fail (this is the last retry)
	}

	e := &Engine{
		logger:       zap.NewNop(),
		batchTimeout: time.Second,
		promMetrics:  getTestMetrics(),
	}

	items := []publisher.PublishItem{
		{Subject: "test.0", EventID: "0", Position: model.WALPosition{LSN: "0/0"}},
		{Subject: "test.1", EventID: "1", Position: model.WALPosition{LSN: "0/1"}},
	}

	result, err := e.publishWithRetry(context.Background(), mock, items)

	if err == nil {
		t.Error("expected error after exhausting retries")
	}
	if result.Succeeded != 1 {
		t.Errorf("expected 1 succeeded, got %d", result.Succeeded)
	}
	if result.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", result.Failed)
	}
	// Should have maxPublishRetries + 1 attempts
	if mock.publishBatchCalls.Load() != int32(maxPublishRetries+1) {
		t.Errorf("expected %d publish calls, got %d", maxPublishRetries+1, mock.publishBatchCalls.Load())
	}
}

func TestPublishWithRetry_ContextCancellation(t *testing.T) {
	mock := newMockBatchPublisher()
	mock.failuresPerAttempt = [][]int{
		{0, 1, 2}, // All fail first attempt
	}

	e := &Engine{
		logger:       zap.NewNop(),
		batchTimeout: time.Second,
		promMetrics:  getTestMetrics(),
	}

	items := []publisher.PublishItem{
		{Subject: "test.0", EventID: "0", Position: model.WALPosition{LSN: "0/0"}},
		{Subject: "test.1", EventID: "1", Position: model.WALPosition{LSN: "0/1"}},
		{Subject: "test.2", EventID: "2", Position: model.WALPosition{LSN: "0/2"}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	result, err := e.publishWithRetry(ctx, mock, items)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	// First attempt should complete, but retry should be skipped due to context
	if result.Total != 3 {
		t.Errorf("expected total 3, got %d", result.Total)
	}
}

func TestPublishWithRetry_PublishBatchError(t *testing.T) {
	mock := newMockBatchPublisher()
	// First two attempts error at PublishBatchAsync level, third succeeds
	mock.publishBatchErrors = []error{
		errors.New("connection refused"),
		errors.New("timeout"),
		nil, // Success
	}

	e := &Engine{
		logger:       zap.NewNop(),
		batchTimeout: time.Second,
		promMetrics:  getTestMetrics(),
	}

	items := []publisher.PublishItem{
		{Subject: "test.0", EventID: "0", Position: model.WALPosition{LSN: "0/0"}},
	}

	result, err := e.publishWithRetry(context.Background(), mock, items)

	if err != nil {
		t.Errorf("expected success after retries, got %v", err)
	}
	if result.Succeeded != 1 {
		t.Errorf("expected 1 succeeded, got %d", result.Succeeded)
	}
	// Should have 3 attempts
	if mock.publishBatchCalls.Load() != 3 {
		t.Errorf("expected 3 publish calls, got %d", mock.publishBatchCalls.Load())
	}
}

func TestCalculateBackoff(t *testing.T) {
	e := &Engine{}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 1 * time.Second},  // 2^0 = 1s
		{2, 2 * time.Second},  // 2^1 = 2s
		{3, 4 * time.Second},  // 2^2 = 4s
		{4, 8 * time.Second},  // 2^3 = 8s (max)
		{5, 8 * time.Second},  // capped at max
		{10, 8 * time.Second}, // capped at max
	}

	for _, tt := range tests {
		result := e.calculateBackoff(tt.attempt)
		if result != tt.expected {
			t.Errorf("calculateBackoff(%d) = %v, want %v", tt.attempt, result, tt.expected)
		}
	}
}

func TestBuildFinalResult(t *testing.T) {
	e := &Engine{}

	items := []publisher.PublishItem{
		{Subject: "test.0", Position: model.WALPosition{LSN: "0/0"}},
		{Subject: "test.1", Position: model.WALPosition{LSN: "0/1"}},
		{Subject: "test.2", Position: model.WALPosition{LSN: "0/2"}},
		{Subject: "test.3", Position: model.WALPosition{LSN: "0/3"}},
	}

	// Items 0 and 2 succeeded, 1 and 3 failed
	succeeded := []bool{true, false, true, false}
	testErr := errors.New("test error")

	result := e.buildFinalResult(items, succeeded, testErr)

	if result.Total != 4 {
		t.Errorf("expected total 4, got %d", result.Total)
	}
	if result.Succeeded != 2 {
		t.Errorf("expected 2 succeeded, got %d", result.Succeeded)
	}
	if result.Failed != 2 {
		t.Errorf("expected 2 failed, got %d", result.Failed)
	}
	if len(result.FailedItems) != 2 || result.FailedItems[0] != 1 || result.FailedItems[1] != 3 {
		t.Errorf("expected failed items [1, 3], got %v", result.FailedItems)
	}
	if result.FirstError != testErr {
		t.Errorf("expected test error, got %v", result.FirstError)
	}
	// Last successful position should be item 0 (last contiguous success from start)
	// Item 1 failed, so we cannot checkpoint past it even though item 2 succeeded.
	if result.LastSuccessPosition == nil || result.LastSuccessPosition.LSN != "0/0" {
		t.Errorf("expected last success position 0/0 (contiguous prefix), got %v", result.LastSuccessPosition)
	}
}

// mockErrorReader implements wal.Reader and wal.ErrorReporter for testing
// fatal error propagation from the reader to the engine.
type mockErrorReader struct {
	fatalErr error
}

var _ wal.Reader = (*mockErrorReader)(nil)
var _ wal.ErrorReporter = (*mockErrorReader)(nil)

func (m *mockErrorReader) Start(context.Context) error { return nil }
func (m *mockErrorReader) ReadWAL(context.Context, model.WALPosition) (<-chan *parser.RawMessage, error) {
	return nil, nil
}
func (m *mockErrorReader) GetCurrentPosition(context.Context) (model.WALPosition, error) {
	return model.WALPosition{}, nil
}
func (m *mockErrorReader) Stop(context.Context) error { return nil }
func (m *mockErrorReader) Err() error                 { return m.fatalErr }

// mockErrorParser implements parser.Parser and parser.ErrorReporter for testing
// fatal error propagation from the parser to the engine.
type mockErrorParser struct {
	fatalErr error
}

var _ parser.Parser = (*mockErrorParser)(nil)
var _ parser.ErrorReporter = (*mockErrorParser)(nil)

func (m *mockErrorParser) Parse(context.Context, <-chan *parser.RawMessage) (<-chan *model.WALEvent, error) {
	return nil, nil
}
func (m *mockErrorParser) Err() error { return m.fatalErr }

func TestRunBatched_ReaderFatalError(t *testing.T) {
	mockReader := &mockErrorReader{fatalErr: errors.New("replication slot \"cdc_slot\" does not exist")}

	e := &Engine{
		reader:       mockReader,
		logger:       zap.NewNop(),
		batchSize:    100,
		batchTimeout: time.Second,
		promMetrics:  getTestMetrics(),
	}

	// Simulate the reader goroutine closing the stream due to fatal error.
	stream := make(chan *model.WALEvent)
	close(stream)

	err := e.runBatched(context.Background(), stream)
	if err == nil {
		t.Fatal("expected error from fatal reader failure, got nil")
	}
	if !strings.Contains(err.Error(), "replication stopped") {
		t.Errorf("expected 'replication stopped' error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "cdc_slot") {
		t.Errorf("expected underlying error to be preserved, got: %v", err)
	}
}

func TestRunBatched_GracefulShutdown(t *testing.T) {
	// A reader without ErrorReporter (or one returning nil) should result in nil from runBatched.
	mockReader := &mockErrorReader{fatalErr: nil}

	e := &Engine{
		reader:       mockReader,
		logger:       zap.NewNop(),
		batchSize:    100,
		batchTimeout: time.Second,
		promMetrics:  getTestMetrics(),
	}

	stream := make(chan *model.WALEvent)
	close(stream)

	err := e.runBatched(context.Background(), stream)
	if err != nil {
		t.Errorf("expected nil error for graceful shutdown, got: %v", err)
	}
}

func TestRunBatched_ParserFatalError(t *testing.T) {
	mockReader := &mockErrorReader{fatalErr: nil}
	mockParser := &mockErrorParser{fatalErr: errors.New("decode wal2json failed: invalid character")}

	e := &Engine{
		reader:       mockReader,
		parser:       mockParser,
		logger:       zap.NewNop(),
		batchSize:    100,
		batchTimeout: time.Second,
		promMetrics:  getTestMetrics(),
	}

	stream := make(chan *model.WALEvent)
	close(stream)

	err := e.runBatched(context.Background(), stream)
	if err == nil {
		t.Fatal("expected error from fatal parser failure, got nil")
	}
	if !strings.Contains(err.Error(), "parser stopped") {
		t.Errorf("expected 'parser stopped' error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "decode wal2json failed") {
		t.Errorf("expected underlying parser error to be preserved, got: %v", err)
	}
}

func TestCheckpointPositionForCommit(t *testing.T) {
	tests := []struct {
		name string
		evt  *model.WALEvent
		want string
	}{
		{
			name: "uses position when present",
			evt: &model.WALEvent{
				Position: model.WALPosition{LSN: "0/ABC"},
				LSN:      "0/DEF",
			},
			want: "0/ABC",
		},
		{
			name: "falls back to LSN when position is empty",
			evt: &model.WALEvent{
				LSN: "0/DEF",
			},
			want: "0/DEF",
		},
		{
			name: "empty when both are missing",
			evt:  &model.WALEvent{},
			want: "",
		},
		{
			name: "empty on nil event",
			evt:  nil,
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checkpointPositionForCommit(tt.evt)
			if got.LSN != tt.want {
				t.Fatalf("checkpointPositionForCommit() = %q, want %q", got.LSN, tt.want)
			}
		})
	}
}
