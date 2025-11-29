package publisher

import (
	"context"
	"fmt"
	"strings"
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
// Thread safety: The Acked and Err fields are written by a background goroutine
// and must only be read after waiting on the done channel (via Wait method or
// direct channel read). Reading these fields before done is closed results in
// a data race.
type PendingAck struct {
	Subject string
	EventID string
	TxID    uint64
	Acked   bool  // safe to read only after done is closed
	Err     error // safe to read only after done is closed
	done    chan struct{}
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

// BatchPublisher extends Publisher with batch async capabilities for high throughput.
type BatchPublisher interface {
	Publisher

	// PublishBatchAsync sends multiple messages asynchronously.
	// Returns immediately with pending acks to monitor.
	PublishBatchAsync(ctx context.Context, items []PublishItem) ([]*PendingAck, error)

	// WaitForAcks blocks until all pending acks are resolved or timeout.
	// Returns the count of successful acks and first error encountered.
	WaitForAcks(ctx context.Context, pending []*PendingAck, timeout time.Duration) (int, error)
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
