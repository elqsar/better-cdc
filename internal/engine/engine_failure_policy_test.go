package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"better-cdc/internal/checkpoint"
	"better-cdc/internal/metrics"
	"better-cdc/internal/model"
	"better-cdc/internal/publisher"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func newFailurePolicyEngine(mock *mockBatchPublisher, policy FailurePolicy) *Engine {
	return &Engine{
		publisher:         mock,
		logger:            zap.NewNop(),
		batchTimeout:      time.Second,
		maxPublishRetries: 3,
		failurePolicy:     policy,
		dlqSubjectPrefix:  "cdc.dlq",
		database:          "postgres",
		promMetrics:       getTestMetrics(),
	}
}

func poisonItems() []publisher.PublishItem {
	return []publisher.PublishItem{
		{Subject: "cdc.postgres.public.accounts", EventID: "0", Position: model.WALPosition{LSN: "0/0"}, Schema: "public", Table: "accounts", Operation: "INSERT"},
		{Subject: "cdc.postgres.public.accounts", EventID: "1", Data: []byte(`{"big":"payload"}`), Position: model.WALPosition{LSN: "0/1"}, Schema: "public", Table: "accounts", Operation: "INSERT", TxID: 7},
		{Subject: "cdc.postgres.public.accounts", EventID: "2", Position: model.WALPosition{LSN: "0/2"}, Schema: "public", Table: "accounts", Operation: "INSERT"},
	}
}

func TestPublishWithRetry_PermanentErrorCrashPolicyStopsWithoutRetries(t *testing.T) {
	mock := newMockBatchPublisher()
	mock.failureErr = nats.ErrMaxPayload
	mock.failuresPerAttempt = [][]int{
		{},  // item 0 succeeds
		{0}, // item 1 fails permanently
	}

	e := newFailurePolicyEngine(mock, FailurePolicyCrash)

	result, err := e.publishWithRetry(context.Background(), mock, poisonItems())

	if !errors.Is(err, nats.ErrMaxPayload) {
		t.Fatalf("expected ErrMaxPayload, got %v", err)
	}
	// Item 1 must not be retried and item 2 must never be published.
	wantBatches := [][]string{{"0"}, {"1"}}
	if !equalStringBatches(mock.publishedBatches, wantBatches) {
		t.Errorf("expected batches %v, got %v", wantBatches, mock.publishedBatches)
	}
	if result.Succeeded != 1 || result.Failed != 2 {
		t.Errorf("expected 1 succeeded / 2 failed, got %d/%d", result.Succeeded, result.Failed)
	}
	if len(mock.publishSubjects) != 0 {
		t.Errorf("expected no dead-letter publish under crash policy, got %v", mock.publishSubjects)
	}
}

func TestPublishWithRetry_PermanentErrorDLQPolicyQuarantinesAndContinues(t *testing.T) {
	mock := newMockBatchPublisher()
	mock.failureErr = nats.ErrMaxPayload
	mock.failuresPerAttempt = [][]int{
		{},  // item 0 succeeds
		{0}, // item 1 fails permanently -> quarantined
		{},  // item 2 succeeds
	}

	e := newFailurePolicyEngine(mock, FailurePolicyDLQ)

	result, err := e.publishWithRetry(context.Background(), mock, poisonItems())

	if err != nil {
		t.Fatalf("expected quarantine to swallow the poison item, got %v", err)
	}
	if result.Succeeded != 3 || result.Failed != 0 {
		t.Errorf("expected all items handled, got %d succeeded / %d failed", result.Succeeded, result.Failed)
	}
	// No retry for the poison item; item 2 still published after it.
	wantBatches := [][]string{{"0"}, {"1"}, {"2"}}
	if !equalStringBatches(mock.publishedBatches, wantBatches) {
		t.Errorf("expected batches %v, got %v", wantBatches, mock.publishedBatches)
	}
	if len(mock.publishSubjects) != 1 || mock.publishSubjects[0] != "cdc.dlq.postgres.public.accounts" {
		t.Fatalf("expected one dead-letter publish to cdc.dlq.postgres.public.accounts, got %v", mock.publishSubjects)
	}
	if mock.publishMsgIDs[0] != "dlq-1" {
		t.Errorf("expected dedup msg id dlq-1, got %q", mock.publishMsgIDs[0])
	}
	record := string(mock.publishPayloads[0])
	for _, want := range []string{`"event_id":"1"`, `"lsn":"0/1"`, `"txid":7`, `"table":"accounts"`, "maximum payload exceeded", `{\"big\":\"payload\"}`} {
		if !strings.Contains(record, want) {
			t.Errorf("dead-letter record missing %q: %s", want, record)
		}
	}
	if result.LastSuccessPosition == nil || result.LastSuccessPosition.LSN != "0/2" {
		t.Errorf("expected last success position 0/2, got %v", result.LastSuccessPosition)
	}
}

func TestPublishWithRetry_PermanentErrorSkipPolicyContinuesWithoutDLQ(t *testing.T) {
	mock := newMockBatchPublisher()
	mock.failureErr = nats.ErrMaxPayload
	mock.failuresPerAttempt = [][]int{
		{},  // item 0 succeeds
		{0}, // item 1 fails permanently -> skipped
		{},  // item 2 succeeds
	}

	e := newFailurePolicyEngine(mock, FailurePolicySkip)

	result, err := e.publishWithRetry(context.Background(), mock, poisonItems())

	if err != nil {
		t.Fatalf("expected skip policy to swallow the poison item, got %v", err)
	}
	if result.Succeeded != 3 || result.Failed != 0 {
		t.Errorf("expected all items handled, got %d succeeded / %d failed", result.Succeeded, result.Failed)
	}
	if len(mock.publishSubjects) != 0 {
		t.Errorf("expected no dead-letter publish under skip policy, got %v", mock.publishSubjects)
	}
}

func TestPublishWithRetry_TransientExhaustionStillFailsUnderDLQPolicy(t *testing.T) {
	mock := newMockBatchPublisher()
	// Default transient failure error; item 0 fails on every attempt.
	mock.failuresPerAttempt = [][]int{{0}, {0}, {0}, {0}}

	e := newFailurePolicyEngine(mock, FailurePolicyDLQ)
	e.maxPublishRetries = 2

	_, err := e.publishWithRetry(context.Background(), mock, poisonItems()[:1])

	if err == nil {
		t.Fatal("expected transient exhaustion to fail even under dlq policy")
	}
	if len(mock.publishSubjects) != 0 {
		t.Errorf("expected no dead-letter publish for transient failure, got %v", mock.publishSubjects)
	}
	// All retries must still be attempted for transient errors.
	if got := mock.publishBatchCalls.Load(); got != 3 {
		t.Errorf("expected 3 attempts, got %d", got)
	}
}

func TestPublishWithRetry_UnorderedPermanentErrorDLQPolicy(t *testing.T) {
	mock := newMockBatchPublisher()
	mock.failureErr = nats.ErrMaxPayload
	mock.failuresPerAttempt = [][]int{
		{1}, // item 1 fails permanently while 0 and 2 succeed
	}

	e := newFailurePolicyEngine(mock, FailurePolicyDLQ)
	e.unsafeUnorderedAsyncPublish = true

	result, err := e.publishWithRetry(context.Background(), mock, poisonItems())

	if err != nil {
		t.Fatalf("expected quarantine to swallow the poison item, got %v", err)
	}
	if result.Succeeded != 3 || result.Failed != 0 {
		t.Errorf("expected all items handled, got %d succeeded / %d failed", result.Succeeded, result.Failed)
	}
	// The poison item must not be retried.
	wantBatches := [][]string{{"0", "1", "2"}}
	if !equalStringBatches(mock.publishedBatches, wantBatches) {
		t.Errorf("expected batches %v, got %v", wantBatches, mock.publishedBatches)
	}
	if len(mock.publishSubjects) != 1 || mock.publishSubjects[0] != "cdc.dlq.postgres.public.accounts" {
		t.Errorf("expected one dead-letter publish, got %v", mock.publishSubjects)
	}
}

func TestFlushWithBatchPublish_PermanentFailureDLQAdvancesCheckpoint(t *testing.T) {
	mock := newMockBatchPublisher()
	mock.failureErr = nats.ErrMaxPayload
	mock.failuresPerAttempt = [][]int{{0}}

	reader := &mockAcknowledgerReader{}
	store := &mockStore{}
	ckpt := checkpoint.NewManager(store, time.Hour, zap.NewNop())
	e := newFailurePolicyEngine(mock, FailurePolicyDLQ)
	e.reader = reader
	e.transformer = &mockTransformer{}
	e.checkpointer = ckpt
	e.eventsProcessed = metrics.NewRateCounter("events_per_second")
	e.batchesPublished = metrics.NewCounter("batches_published")
	e.batchLatency = metrics.NewHistogram("batch_latency_us", []uint64{100, 500, 1000})
	e.transformLatency = metrics.NewHistogram("transform_latency_ns", []uint64{100, 500, 1000})

	commitPos := model.WALPosition{LSN: "0/30"}
	batch := []*model.WALEvent{
		{
			Operation:     model.OperationInsert,
			Schema:        "public",
			Table:         "accounts",
			NewValues:     map[string]interface{}{"id": 1},
			TransactionID: "1",
			Position:      commitPos,
			LSN:           "0/20",
			TxID:          1,
		},
		{
			Commit:   true,
			Position: commitPos,
			LSN:      "0/20",
			TxID:     1,
		},
	}

	if err := e.flushWithBatchPublish(context.Background(), batch, batch[len(batch)-1], mock); err != nil {
		t.Fatalf("expected quarantined batch to succeed, got %v", err)
	}
	if got := ckpt.LastAcked().LSN; got != "0/30" {
		t.Fatalf("expected checkpoint to advance past poison event to 0/30, got %q", got)
	}
	if len(mock.publishSubjects) != 1 {
		t.Fatalf("expected one dead-letter publish, got %v", mock.publishSubjects)
	}
}

// failingTransformer always fails, simulating a deterministic transform bug.
type failingTransformer struct{}

func (failingTransformer) Transform(context.Context, *model.WALEvent) (*model.CDCEvent, error) {
	return nil, errors.New("unsupported column type")
}

func TestFlushWithBatchPublish_TransformFailureQuarantinedUnderDLQ(t *testing.T) {
	mock := newMockBatchPublisher()

	reader := &mockAcknowledgerReader{}
	store := &mockStore{}
	ckpt := checkpoint.NewManager(store, time.Hour, zap.NewNop())
	e := newFailurePolicyEngine(mock, FailurePolicyDLQ)
	e.reader = reader
	e.transformer = failingTransformer{}
	e.checkpointer = ckpt
	e.eventsProcessed = metrics.NewRateCounter("events_per_second")
	e.batchesPublished = metrics.NewCounter("batches_published")
	e.batchLatency = metrics.NewHistogram("batch_latency_us", []uint64{100, 500, 1000})
	e.transformLatency = metrics.NewHistogram("transform_latency_ns", []uint64{100, 500, 1000})

	commitPos := model.WALPosition{LSN: "0/40"}
	batch := []*model.WALEvent{
		{
			Operation: model.OperationInsert,
			Schema:    "public",
			Table:     "accounts",
			NewValues: map[string]interface{}{"id": 1},
			Position:  commitPos,
			LSN:       "0/35",
			TxID:      2,
		},
		{
			Commit:   true,
			Position: commitPos,
			LSN:      "0/35",
			TxID:     2,
		},
	}

	if err := e.flushWithBatchPublish(context.Background(), batch, batch[len(batch)-1], mock); err != nil {
		t.Fatalf("expected transform failure to be quarantined, got %v", err)
	}
	if got := ckpt.LastAcked().LSN; got != "0/40" {
		t.Fatalf("expected checkpoint 0/40, got %q", got)
	}
	if len(mock.publishSubjects) != 1 || mock.publishSubjects[0] != "cdc.dlq.postgres.public.accounts" {
		t.Fatalf("expected dead-letter publish for transform failure, got %v", mock.publishSubjects)
	}
	record := string(mock.publishPayloads[0])
	if !strings.Contains(record, "unsupported column type") {
		t.Errorf("dead-letter record missing transform error: %s", record)
	}

	// Under the default crash policy the same failure must still stop the engine.
	crashEngine := newFailurePolicyEngine(newMockBatchPublisher(), FailurePolicyCrash)
	crashEngine.transformer = failingTransformer{}
	crashEngine.checkpointer = ckpt
	crashEngine.transformLatency = metrics.NewHistogram("transform_latency_ns", []uint64{100})
	if err := crashEngine.flushWithBatchPublish(context.Background(), batch[:1], batch[0], newMockBatchPublisher()); err == nil {
		t.Fatal("expected transform failure to be fatal under crash policy")
	}
}
