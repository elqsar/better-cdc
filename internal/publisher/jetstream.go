package publisher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// JetStreamPublisher publishes messages to NATS JetStream with ack handling.
type JetStreamPublisher struct {
	opts   JetStreamOptions
	nc     *nats.Conn
	js     nats.JetStreamContext
	logger *zap.Logger
}

type JetStreamOptions struct {
	URLs           []string
	Username       string
	Password       string
	ConnectTimeout time.Duration
	PublishTimeout time.Duration
	StreamName     string
	StreamSubjects []string
}

func NewJetStreamPublisher(opts JetStreamOptions, logger *zap.Logger) *JetStreamPublisher {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &JetStreamPublisher{opts: opts, logger: logger}
}

func (p *JetStreamPublisher) Connect() error {
	if len(p.opts.URLs) == 0 {
		return fmt.Errorf("no NATS URLs provided")
	}
	natsOpts := []nats.Option{
		nats.Timeout(p.opts.ConnectTimeout),
		nats.Name("better-cdc-publisher"),
		nats.MaxReconnects(-1), // Retry forever
		nats.ReconnectWait(2 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			p.logger.Warn("NATS disconnected", zap.Error(err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			p.logger.Info("NATS reconnected", zap.String("url", nc.ConnectedUrl()))
		}),
	}
	if p.opts.Username != "" {
		natsOpts = append(natsOpts, nats.UserInfo(p.opts.Username, p.opts.Password))
	}

	connectURL := strings.Join(p.opts.URLs, ",")
	nc, err := nats.Connect(connectURL, natsOpts...)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		_ = nc.Drain()
		return fmt.Errorf("jetstream: %w", err)
	}
	p.nc = nc
	p.js = js
	if err := p.ensureStream(); err != nil {
		_ = p.nc.Drain()
		return err
	}
	p.logger.Info("connected to nats jetstream", zap.Strings("urls", p.opts.URLs), zap.String("stream", p.streamName()))
	return nil
}

func (p *JetStreamPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	if p.js == nil {
		return fmt.Errorf("jetstream not connected")
	}
	pa, err := p.js.PublishAsync(subject, data)
	if err != nil {
		return fmt.Errorf("publish async: %w", err)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ack := <-pa.Ok():
		if ack == nil {
			return fmt.Errorf("nil ack received")
		}
		p.logger.Debug("published to jetstream", zap.String("subject", subject), zap.String("stream", ack.Stream), zap.Uint64("seq", ack.Sequence))
		return nil
	case err := <-pa.Err():
		return fmt.Errorf("publish ack error: %w", err)
	case <-time.After(p.publishTimeout()):
		return fmt.Errorf("publish ack timeout")
	}
}

func (p *JetStreamPublisher) PublishWithRetries(ctx context.Context, subject string, data []byte, maxRetries int) error {
	var lastErr error
	for i := 0; i <= maxRetries; i++ {
		if err := p.Publish(ctx, subject, data); err != nil {
			lastErr = err
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff(i)):
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("publish failed after retries: %w", lastErr)
}

func (p *JetStreamPublisher) Close() error {
	if p.nc != nil {
		p.logger.Info("closing nats connection")
		return p.nc.Drain()
	}
	return nil
}

func backoff(attempt int) time.Duration {
	if attempt < 0 {
		return time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func (p *JetStreamPublisher) publishTimeout() time.Duration {
	if p.opts.PublishTimeout > 0 {
		return p.opts.PublishTimeout
	}
	return 5 * time.Second
}

func (p *JetStreamPublisher) ensureStream() error {
	if p.js == nil {
		return fmt.Errorf("jetstream not initialized")
	}
	streamName := p.opts.StreamName
	if streamName == "" {
		streamName = "CDC"
	}
	subjects := p.opts.StreamSubjects
	if len(subjects) == 0 {
		subjects = []string{"cdc.>"}
	}

	if _, err := p.js.StreamInfo(streamName); err == nil {
		p.logger.Debug("stream already exists", zap.String("stream", streamName))
		return nil
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		return fmt.Errorf("lookup stream: %w", err)
	}

	if _, err := p.js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  subjects,
		Retention: nats.LimitsPolicy,
	}); err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	p.logger.Info("created jetstream stream", zap.String("stream", streamName), zap.Strings("subjects", subjects))
	return nil
}

func (p *JetStreamPublisher) streamName() string {
	if p.opts.StreamName != "" {
		return p.opts.StreamName
	}
	return "CDC"
}
