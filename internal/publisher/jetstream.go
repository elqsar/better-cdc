package publisher

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"better-cdc/internal/metrics"
	"better-cdc/internal/model"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// JetStreamPublisher publishes messages to NATS JetStream with ack handling.
// Implements both Publisher and BatchPublisher interfaces.
type JetStreamPublisher struct {
	opts         JetStreamOptions
	nc           *nats.Conn
	js           nats.JetStreamContext
	logger       *zap.Logger
	publishedCnt *metrics.Counter
	ackFailCnt   *metrics.Counter
	promMetrics  *metrics.Metrics
}

type JetStreamOptions struct {
	URLs            []string
	Username        string
	Password        string
	ConnectTimeout  time.Duration
	PublishTimeout  time.Duration
	StreamName      string
	StreamSubjects  []string
	StreamStorage   string        // "file" or "memory" (default: file)
	StreamReplicas  int           // Number of replicas (default: 1)
	StreamMaxAge    time.Duration // Max age for messages (default: 72h)
	DuplicateWindow time.Duration // De-duplication window (default: 2m)
}

func NewJetStreamPublisher(opts JetStreamOptions, logger *zap.Logger) *JetStreamPublisher {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &JetStreamPublisher{
		opts:         opts,
		logger:       logger,
		publishedCnt: metrics.NewCounter("jetstream_published"),
		ackFailCnt:   metrics.NewCounter("jetstream_ack_failures"),
		promMetrics:  metrics.GlobalMetrics,
	}
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

func (p *JetStreamPublisher) Publish(ctx context.Context, subject string, data []byte, eventID string) error {
	if p.js == nil {
		return fmt.Errorf("jetstream not connected")
	}
	var opts []nats.PubOpt
	if eventID != "" {
		opts = append(opts, nats.MsgId(eventID))
	}
	pa, err := p.js.PublishAsync(subject, data, opts...)
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

func (p *JetStreamPublisher) PublishWithRetries(ctx context.Context, subject string, data []byte, maxRetries int, eventID string) error {
	var lastErr error
	for i := 0; i <= maxRetries; i++ {
		if err := p.Publish(ctx, subject, data, eventID); err != nil {
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

func (p *JetStreamPublisher) Ready(context.Context) error {
	if p.nc == nil || p.js == nil {
		return fmt.Errorf("jetstream not connected")
	}
	if !p.nc.IsConnected() {
		return fmt.Errorf("nats connection status is %s", p.nc.Status().String())
	}
	return p.validateStream()
}

func backoff(attempt int) time.Duration {
	if attempt < 0 {
		return time.Second
	}
	const maxAttempt = 3 // cap at 8 seconds (2^3)
	if attempt > maxAttempt {
		attempt = maxAttempt
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
	expected := p.expectedStreamConfig()
	streamName := expected.Name

	if info, err := p.js.StreamInfo(streamName); err == nil {
		if err := validateStreamConfig(&info.Config, expected); err != nil {
			return fmt.Errorf("existing stream %q does not match configuration: %w", streamName, err)
		}
		p.logger.Debug("stream already exists", zap.String("stream", streamName))
		return nil
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		return fmt.Errorf("lookup stream: %w", err)
	}

	if _, err := p.js.AddStream(expected); err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	p.logger.Info("created jetstream stream",
		zap.String("stream", streamName),
		zap.Strings("subjects", expected.Subjects),
		zap.String("storage", p.opts.StreamStorage),
		zap.Int("replicas", expected.Replicas),
		zap.Duration("max_age", expected.MaxAge),
		zap.Duration("duplicate_window", expected.Duplicates))
	return nil
}

func (p *JetStreamPublisher) streamName() string {
	if p.opts.StreamName != "" {
		return p.opts.StreamName
	}
	return "CDC"
}

func (p *JetStreamPublisher) validateStream() error {
	info, err := p.js.StreamInfo(p.streamName())
	if err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("stream %q not found", p.streamName())
		}
		return fmt.Errorf("lookup stream: %w", err)
	}
	if err := validateStreamConfig(&info.Config, p.expectedStreamConfig()); err != nil {
		return fmt.Errorf("stream validation failed: %w", err)
	}
	return nil
}

func (p *JetStreamPublisher) expectedStreamConfig() *nats.StreamConfig {
	subjects := append([]string(nil), p.opts.StreamSubjects...)
	if len(subjects) == 0 {
		subjects = []string{"cdc.>"}
	}

	storage := nats.FileStorage
	if strings.EqualFold(p.opts.StreamStorage, "memory") {
		storage = nats.MemoryStorage
	}
	replicas := p.opts.StreamReplicas
	if replicas < 1 {
		replicas = 1
	}
	maxAge := p.opts.StreamMaxAge
	if maxAge <= 0 {
		maxAge = 72 * time.Hour
	}
	dupWindow := p.opts.DuplicateWindow
	if dupWindow <= 0 {
		dupWindow = 2 * time.Minute
	}

	return &nats.StreamConfig{
		Name:       p.streamName(),
		Subjects:   subjects,
		Retention:  nats.LimitsPolicy,
		Storage:    storage,
		Replicas:   replicas,
		MaxAge:     maxAge,
		Duplicates: dupWindow,
	}
}

func validateStreamConfig(actual *nats.StreamConfig, expected *nats.StreamConfig) error {
	if actual == nil {
		return fmt.Errorf("stream config is nil")
	}
	if expected == nil {
		return fmt.Errorf("expected stream config is nil")
	}
	if actual.Name != expected.Name {
		return fmt.Errorf("name mismatch: got %q want %q", actual.Name, expected.Name)
	}
	if !sameSubjects(actual.Subjects, expected.Subjects) {
		return fmt.Errorf("subjects mismatch: got %v want %v", actual.Subjects, expected.Subjects)
	}
	if actual.Retention != expected.Retention {
		return fmt.Errorf("retention mismatch: got %v want %v", actual.Retention, expected.Retention)
	}
	if actual.Storage != expected.Storage {
		return fmt.Errorf("storage mismatch: got %v want %v", actual.Storage, expected.Storage)
	}
	if actual.Replicas != expected.Replicas {
		return fmt.Errorf("replicas mismatch: got %d want %d", actual.Replicas, expected.Replicas)
	}
	if actual.MaxAge != expected.MaxAge {
		return fmt.Errorf("max age mismatch: got %v want %v", actual.MaxAge, expected.MaxAge)
	}
	if actual.Duplicates != expected.Duplicates {
		return fmt.Errorf("duplicate window mismatch: got %v want %v", actual.Duplicates, expected.Duplicates)
	}
	return nil
}

func sameSubjects(actual []string, expected []string) bool {
	a := append([]string(nil), actual...)
	e := append([]string(nil), expected...)
	sort.Strings(a)
	sort.Strings(e)
	return slices.Equal(a, e)
}

// PublishBatchAsync sends multiple messages asynchronously without waiting for acks.
// Returns immediately with pending acks to monitor.
func (p *JetStreamPublisher) PublishBatchAsync(ctx context.Context, items []PublishItem) ([]*PendingAck, error) {
	if p.js == nil {
		return nil, fmt.Errorf("jetstream not connected")
	}

	pending := make([]*PendingAck, 0, len(items))

	for _, item := range items {
		select {
		case <-ctx.Done():
			return pending, ctx.Err()
		default:
		}

		pend := &PendingAck{
			Subject: item.Subject,
			EventID: item.EventID,
			TxID:    item.TxID,
			done:    make(chan struct{}),
		}

		var opts []nats.PubOpt
		if item.EventID != "" {
			opts = append(opts, nats.MsgId(item.EventID))
		}
		pa, err := p.js.PublishAsync(item.Subject, item.Data, opts...)
		if err != nil {
			pend.Err = err
			close(pend.done)
			pending = append(pending, pend)
			p.ackFailCnt.Inc()
			p.promMetrics.JetstreamAckFailure.Inc()
			continue
		}

		pending = append(pending, pend)

		// Launch goroutine to await this specific ack
		go func(pend *PendingAck, pa nats.PubAckFuture, subject string) {
			defer close(pend.done)
			select {
			case <-ctx.Done():
				pend.Err = ctx.Err()
				p.ackFailCnt.Inc()
				p.promMetrics.JetstreamAckFailure.Inc()
			case ack := <-pa.Ok():
				if ack != nil {
					pend.setAcked(true)
					p.publishedCnt.Inc()
					p.promMetrics.JetstreamPublished.Inc()
					p.logger.Debug("async ack received",
						zap.String("subject", subject),
						zap.Uint64("seq", ack.Sequence))
				} else {
					pend.Err = fmt.Errorf("nil ack")
					p.ackFailCnt.Inc()
					p.promMetrics.JetstreamAckFailure.Inc()
				}
			case err := <-pa.Err():
				pend.Err = err
				p.ackFailCnt.Inc()
				p.promMetrics.JetstreamAckFailure.Inc()
			case <-time.After(p.publishTimeout()):
				pend.Err = fmt.Errorf("ack timeout")
				p.ackFailCnt.Inc()
				p.promMetrics.JetstreamAckFailure.Inc()
			}
		}(pend, pa, item.Subject)
	}

	return pending, nil
}

// WaitForAcks blocks until all pending acks are resolved or timeout.
// Returns a BatchResult with detailed per-item status.
func (p *JetStreamPublisher) WaitForAcks(ctx context.Context, pending []*PendingAck, items []PublishItem, timeout time.Duration) (*BatchResult, error) {
	result := &BatchResult{
		Total:       len(pending),
		FailedItems: make([]int, 0),
	}

	if len(pending) == 0 {
		return result, nil
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	// Track which items completed (vs timed out)
	completed := make([]bool, len(pending))

	for i, pend := range pending {
		select {
		case <-ctx.Done():
			// Context cancelled - count what we have so far
			result.FirstError = ctx.Err()
			p.countResults(pending, items, completed, result)
			return result, ctx.Err()
		case <-deadline.C:
			// Timeout - tally actual results, then build error with accurate counts
			p.countResults(pending, items, completed, result)
			result.FirstError = fmt.Errorf("timeout waiting for acks: %d/%d resolved", result.Succeeded, len(pending))
			return result, result.FirstError
		case <-pend.done:
			completed[i] = true
			if pend.IsAcked() {
				result.Succeeded++
			} else {
				result.Failed++
				result.FailedItems = append(result.FailedItems, i)
				if pend.Err != nil && result.FirstError == nil {
					result.FirstError = pend.Err
				}
			}
		}
	}

	// All completed - find the last successful position
	result.LastSuccessPosition = p.findLastSuccessPosition(pending, items)

	return result, result.FirstError
}

// countResults tallies the final results after timeout/cancellation.
func (p *JetStreamPublisher) countResults(pending []*PendingAck, items []PublishItem, completed []bool, result *BatchResult) {
	result.Succeeded = 0
	result.Failed = 0
	result.FailedItems = result.FailedItems[:0]

	for i, pend := range pending {
		if pend.IsAcked() {
			result.Succeeded++
		} else {
			result.Failed++
			result.FailedItems = append(result.FailedItems, i)
		}
		// Mark incomplete items as failed
		if !completed[i] && !pend.IsAcked() {
			if result.FirstError == nil {
				result.FirstError = fmt.Errorf("item %d did not complete", i)
			}
		}
	}

	result.LastSuccessPosition = p.findLastSuccessPosition(pending, items)
}

// findLastSuccessPosition finds the WAL position of the last contiguously
// successful item from the start. Only checkpoint up to the last position
// where all preceding items also succeeded, to avoid skipping failed events.
func (p *JetStreamPublisher) findLastSuccessPosition(pending []*PendingAck, items []PublishItem) *model.WALPosition {
	var lastPos *model.WALPosition

	for i, pend := range pending {
		if !pend.IsAcked() || i >= len(items) {
			break
		}
		pos := items[i].Position
		lastPos = &pos
	}

	return lastPos
}
