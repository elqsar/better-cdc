package publisher

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"better-cdc/internal/model"

	"go.uber.org/zap"
)

// Publisher pushes CDCEvents to NATS JetStream.
type Publisher interface {
	Connect() error
	Publish(ctx context.Context, subject string, data []byte) error
	PublishWithRetries(ctx context.Context, subject string, data []byte, maxRetries int) error
	Close() error
}

// PublishItem represents a single item to publish in a batch.
type PublishItem struct {
	Subject  string
	Data     []byte
	EventID  string
	TxID     uint64
	Position model.WALPosition
}

// PendingAck represents an in-flight publish awaiting acknowledgment.
//
// Thread safety: The acked field uses atomic.Bool and is safe to read at any time.
// The Err field is written by a background goroutine and must only be read after
// waiting on the done channel (via Wait method or direct channel read).
type PendingAck struct {
	Subject string
	EventID string
	TxID    uint64
	acked   atomic.Bool // thread-safe, can be read at any time
	Err     error       // safe to read only after done is closed
	done    chan struct{}
}

// IsAcked returns whether the publish was acknowledged. Thread-safe.
func (p *PendingAck) IsAcked() bool {
	return p.acked.Load()
}

// setAcked marks the publish as acknowledged. Thread-safe.
func (p *PendingAck) setAcked(v bool) {
	p.acked.Store(v)
}

// Wait blocks until the ack is resolved or context is cancelled.
func (p *PendingAck) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return p.Err
	}
}

// GetErr returns the error if the ack goroutine has completed, or nil if it
// is still in-flight. This is safe to call concurrently — it only reads Err
// after the done channel is closed, which provides a happens-before guarantee.
func (p *PendingAck) GetErr() error {
	select {
	case <-p.done:
		return p.Err
	default:
		return nil
	}
}

// NewPendingAck creates a new PendingAck for testing purposes.
func NewPendingAck(subject, eventID string, txID uint64) *PendingAck {
	return &PendingAck{
		Subject: subject,
		EventID: eventID,
		TxID:    txID,
		done:    make(chan struct{}),
	}
}

// SetAcked sets the acked status (for testing).
func (p *PendingAck) SetAcked(v bool) {
	p.acked.Store(v)
}

// SetError sets the error (for testing).
func (p *PendingAck) SetError(err error) {
	p.Err = err
}

// Close closes the done channel to signal completion (for testing).
func (p *PendingAck) Close() {
	select {
	case <-p.done:
		// Already closed
	default:
		close(p.done)
	}
}

// BatchPublisher extends Publisher with batch async capabilities for high throughput.
type BatchPublisher interface {
	Publisher

	// PublishBatchAsync sends multiple messages asynchronously.
	// Returns immediately with pending acks to monitor.
	PublishBatchAsync(ctx context.Context, items []PublishItem) ([]*PendingAck, error)

	// WaitForAcks blocks until all pending acks are resolved or timeout.
	// Returns a BatchResult with detailed per-item status.
	WaitForAcks(ctx context.Context, pending []*PendingAck, items []PublishItem, timeout time.Duration) (*BatchResult, error)
}

// BatchResult contains the detailed result of a batch publish operation.
type BatchResult struct {
	// Total number of items in the batch
	Total int
	// Number of successfully acknowledged items
	Succeeded int
	// Number of failed items
	Failed int
	// LastSuccessPosition is the WAL position of the last successfully acked item (in order).
	// This can be used to checkpoint progress even on partial failures.
	LastSuccessPosition *model.WALPosition
	// FirstError is the first error encountered, if any
	FirstError error
	// FailedItems contains indices of failed items for potential retry
	FailedItems []int
}

// IsComplete returns true if all items succeeded
func (r *BatchResult) IsComplete() bool {
	return r.Failed == 0 && r.Succeeded == r.Total
}

// IsPartialSuccess returns true if some but not all items succeeded
func (r *BatchResult) IsPartialSuccess() bool {
	return r.Succeeded > 0 && r.Failed > 0
}

// NoopPublisher is a stub that records the last subject published.
type NoopPublisher struct {
	LastSubject string
	logger      *zap.Logger
}

func NewNoopPublisher() *NoopPublisher {
	return &NoopPublisher{logger: zap.NewNop()}
}

func (p *NoopPublisher) Connect() error { return nil }

func (p *NoopPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	_ = ctx
	p.LastSubject = subject
	_ = data
	p.logger.Debug("noop publisher invoked", zap.String("subject", subject))
	return nil
}

func (p *NoopPublisher) PublishWithRetries(ctx context.Context, subject string, data []byte, maxRetries int) error {
	_ = ctx
	_ = maxRetries
	return p.Publish(ctx, subject, data)
}

func (p *NoopPublisher) Close() error { return nil }

// SubjectForEvent builds subject cdc.{database}.{schema}.{table}.
func SubjectForEvent(database string, evt *model.CDCEvent) (string, error) {
	if evt == nil {
		return "", fmt.Errorf("nil event")
	}
	// Use strings.Builder to avoid fmt.Sprintf allocation
	var sb strings.Builder
	sb.Grow(len("cdc.") + len(database) + 1 + len(evt.Schema) + 1 + len(evt.Table))
	sb.WriteString("cdc.")
	sb.WriteString(database)
	sb.WriteByte('.')
	sb.WriteString(evt.Schema)
	sb.WriteByte('.')
	sb.WriteString(evt.Table)
	return sb.String(), nil
}
