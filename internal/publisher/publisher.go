package publisher

import (
	"context"
	"fmt"

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
	return fmt.Sprintf("cdc.%s.%s.%s", database, evt.Schema, evt.Table), nil
}
