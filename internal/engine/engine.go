package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"better-cdc/internal/checkpoint"
	"better-cdc/internal/metrics"
	"better-cdc/internal/model"
	"better-cdc/internal/parser"
	"better-cdc/internal/publisher"
	"better-cdc/internal/transformer"
	"better-cdc/internal/wal"

	"go.uber.org/zap"
)

// jsonBufPool provides reusable buffers for JSON marshaling.
var jsonBufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

// marshalCDCEvent marshals a CDCEvent using a pooled buffer.
func marshalCDCEvent(evt *model.CDCEvent) ([]byte, error) {
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer jsonBufPool.Put(buf)

	enc := json.NewEncoder(buf)
	if err := enc.Encode(evt); err != nil {
		return nil, err
	}

	// Copy result since buffer is pooled; remove trailing newline from Encode
	data := buf.Bytes()
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

// Engine coordinates the end-to-end CDC flow.
type Engine struct {
	reader       wal.Reader
	parser       parser.Parser
	transformer  transformer.Transformer
	publisher    publisher.Publisher
	checkpointer *checkpoint.Manager
	database     string
	batchSize    int
	batchTimeout time.Duration
	logger       *zap.Logger

	// Throughput metrics (legacy, kept for backward compatibility)
	eventsProcessed  *metrics.RateCounter
	batchesPublished *metrics.Counter
	batchLatency     *metrics.Histogram
	transformLatency *metrics.Histogram

	// Prometheus metrics
	promMetrics *metrics.Metrics
}

func NewEngine(reader wal.Reader, parser parser.Parser, transformer transformer.Transformer, publisher publisher.Publisher, checkpointer *checkpoint.Manager, database string, batchSize int, batchTimeout time.Duration, logger *zap.Logger) *Engine {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Engine{
		reader:       reader,
		parser:       parser,
		transformer:  transformer,
		publisher:    publisher,
		checkpointer: checkpointer,
		database:     database,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		logger:       logger,
		// Initialize throughput metrics (legacy)
		eventsProcessed:  metrics.NewRateCounter("events_per_second"),
		batchesPublished: metrics.NewCounter("batches_published"),
		batchLatency:     metrics.NewHistogram("batch_latency_us", []uint64{100, 500, 1000, 5000, 10000, 50000}),
		transformLatency: metrics.NewHistogram("transform_latency_ns", []uint64{100, 500, 1000, 5000, 10000}),
		// Use global Prometheus metrics
		promMetrics: metrics.GlobalMetrics,
	}
}

// Run starts streaming from the provided WAL position.
func (e *Engine) Run(ctx context.Context, start model.WALPosition) error {
	e.logger.Info("engine starting", zap.String("start_lsn", start.LSN), zap.Int("batch_size", e.batchSize), zap.Duration("batch_timeout", e.batchTimeout))
	if err := e.reader.Start(ctx); err != nil {
		return fmt.Errorf("start reader: %w", err)
	}
	defer e.reader.Stop(ctx)

	rawStream, err := e.reader.ReadWAL(ctx, start)
	if err != nil {
		return fmt.Errorf("read wal: %w", err)
	}

	parsedStream, err := e.parser.Parse(ctx, rawStream)
	if err != nil {
		return fmt.Errorf("parse wal: %w", err)
	}

	if err := e.publisher.Connect(); err != nil {
		return fmt.Errorf("publisher connect: %w", err)
	}
	defer e.publisher.Close()

	return e.runBatched(ctx, parsedStream)
}

func (e *Engine) runBatched(ctx context.Context, stream <-chan *model.WALEvent) error {
	batch := make([]*model.WALEvent, 0, e.batchSize)
	timer := time.NewTimer(e.batchTimeout)
	defer timer.Stop()

	// Check if publisher supports batch operations for high throughput
	batchPub, hasBatch := e.publisher.(publisher.BatchPublisher)
	if hasBatch {
		e.logger.Info("batch publisher detected, using async batch publishing")
	}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		e.logger.Debug("flushing batch", zap.Int("count", len(batch)))

		last := batch[len(batch)-1]

		var err error
		if hasBatch {
			err = e.flushWithBatchPublish(ctx, batch, last, batchPub)
		} else {
			err = e.flushSequential(ctx, batch, last)
		}

		batch = batch[:0]
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(e.batchTimeout)
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return flush()
		case evt, ok := <-stream:
			if !ok {
				return flush()
			}
			batch = append(batch, evt)
			if evt.Commit {
				if err := flush(); err != nil {
					return err
				}
				continue
			}
			if e.batchSize > 0 && len(batch) >= e.batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// Retry configuration for transient publish failures.
const (
	maxPublishRetries = 3
	baseRetryBackoff  = time.Second
	maxRetryBackoff   = 8 * time.Second
)

// flushWithBatchPublish uses async batch publishing with collected acks for high throughput.
// Includes retry logic with exponential backoff for transient failures.
func (e *Engine) flushWithBatchPublish(ctx context.Context, batch []*model.WALEvent, last *model.WALEvent, batchPub publisher.BatchPublisher) error {
	batchStart := time.Now()

	// Phase 1: Transform and prepare all items
	items := make([]publisher.PublishItem, 0, len(batch))
	cdcEvents := make([]*model.CDCEvent, 0, len(batch)) // Track for pool release

	for _, evt := range batch {
		if evt.Begin || evt.Commit {
			continue
		}

		transformStart := time.Now()
		cdcEvt, err := e.transformer.Transform(ctx, evt)
		transformLatencyNs := uint64(time.Since(transformStart).Nanoseconds())
		e.transformLatency.Observe(transformLatencyNs)
		e.promMetrics.TransformLatency.Observe(transformLatencyNs)
		if err != nil {
			// Release any already-transformed events on error
			for _, event := range cdcEvents {
				model.ReleaseCDCEvent(event)
			}
			return fmt.Errorf("transform event: %w", err)
		}
		cdcEvents = append(cdcEvents, cdcEvt)

		subject, err := publisher.SubjectForEvent(e.database, cdcEvt)
		if err != nil {
			for _, event := range cdcEvents {
				model.ReleaseCDCEvent(event)
			}
			return fmt.Errorf("build subject: %w", err)
		}

		payload, err := marshalCDCEvent(cdcEvt)
		if err != nil {
			for _, event := range cdcEvents {
				model.ReleaseCDCEvent(event)
			}
			return fmt.Errorf("marshal event: %w", err)
		}

		items = append(items, publisher.PublishItem{
			Subject:  subject,
			Data:     payload,
			EventID:  cdcEvt.EventID,
			TxID:     evt.TxID,
			Position: evt.Position,
		})
	}

	// Release CDCEvents back to pool after marshaling (data is copied)
	for _, evt := range cdcEvents {
		model.ReleaseCDCEvent(evt)
	}

	if len(items) == 0 {
		// Only BEGIN/COMMIT markers, still checkpoint if needed
		if last.Commit {
			if err := e.checkpointer.MaybeFlush(ctx, last.Position, true, time.Now()); err != nil {
				return fmt.Errorf("checkpoint: %w", err)
			}
			e.logger.Debug("checkpointed lsn", zap.String("lsn", last.Position.LSN))
		}
		return nil
	}

	// Phase 2 & 3: Publish with retry logic for transient failures
	result, err := e.publishWithRetry(ctx, batchPub, items)

	// Record metrics for what succeeded (even on partial failure)
	if result.Succeeded > 0 {
		e.eventsProcessed.Add(uint64(result.Succeeded))
		e.promMetrics.EventsTotal.Add(uint64(result.Succeeded))
	}
	e.batchesPublished.Inc()
	batchLatencyUs := uint64(time.Since(batchStart).Microseconds())
	e.batchLatency.Observe(batchLatencyUs)
	e.promMetrics.BatchesPublished.Inc()
	e.promMetrics.BatchLatency.Observe(batchLatencyUs)
	e.promMetrics.EventsPerSecond.Set(int64(e.eventsProcessed.Rate()))

	// Handle partial success: checkpoint what we can
	if result.IsPartialSuccess() {
		e.promMetrics.PartialBatchFailures.Inc()
		e.logger.Warn("partial batch success after retries - checkpointing last successful position",
			zap.Int("succeeded", result.Succeeded),
			zap.Int("failed", result.Failed),
			zap.Int("total", result.Total),
			zap.Error(result.FirstError))

		// Checkpoint the last successfully acked position to avoid replaying those events
		if result.LastSuccessPosition != nil && result.LastSuccessPosition.LSN != "" {
			if cpErr := e.checkpointer.MaybeFlush(ctx, *result.LastSuccessPosition, true, time.Now()); cpErr != nil {
				e.logger.Error("failed to checkpoint partial success",
					zap.Error(cpErr),
					zap.String("lsn", result.LastSuccessPosition.LSN))
			} else {
				e.logger.Info("checkpointed partial batch success",
					zap.String("lsn", result.LastSuccessPosition.LSN),
					zap.Int("succeeded", result.Succeeded))
			}
		}

		return fmt.Errorf("partial batch failure: %d/%d succeeded: %w", result.Succeeded, result.Total, result.FirstError)
	}

	// Complete failure (no successes)
	if err != nil {
		e.logger.Error("batch ack failures after retries",
			zap.Error(err),
			zap.Int("succeeded", result.Succeeded),
			zap.Int("total", result.Total))
		return fmt.Errorf("batch ack: %w", err)
	}

	e.logger.Debug("batch published",
		zap.Int("count", len(items)),
		zap.Int("acked", result.Succeeded))

	// Phase 4: Checkpoint only on commit boundaries (full success)
	if last.Commit {
		if err := e.checkpointer.MaybeFlush(ctx, last.Position, true, time.Now()); err != nil {
			return fmt.Errorf("checkpoint: %w", err)
		}
		e.logger.Debug("checkpointed lsn", zap.String("lsn", last.Position.LSN))
	}

	return nil
}

// publishWithRetry attempts to publish items with retry logic for transient failures.
// It retries only the failed items with exponential backoff.
func (e *Engine) publishWithRetry(ctx context.Context, batchPub publisher.BatchPublisher, items []publisher.PublishItem) (*publisher.BatchResult, error) {
	timeout := e.publishTimeout()

	// Track original indices for proper position tracking across retries
	type indexedItem struct {
		originalIdx int
		item        publisher.PublishItem
	}

	// Initialize with all items to publish
	itemsToPublish := make([]indexedItem, len(items))
	for i, item := range items {
		itemsToPublish[i] = indexedItem{originalIdx: i, item: item}
	}

	// Track success status for each original item
	succeeded := make([]bool, len(items))
	var lastError error

	for attempt := 0; attempt <= maxPublishRetries; attempt++ {
		if len(itemsToPublish) == 0 {
			break
		}

		// Apply backoff before retry (not on first attempt)
		if attempt > 0 {
			backoff := e.calculateBackoff(attempt)
			e.promMetrics.PublishRetries.Inc()
			e.logger.Warn("retrying failed publishes",
				zap.Int("attempt", attempt),
				zap.Int("items", len(itemsToPublish)),
				zap.Duration("backoff", backoff))

			select {
			case <-ctx.Done():
				return e.buildFinalResult(items, succeeded, lastError), ctx.Err()
			case <-time.After(backoff):
			}
		}

		// Extract just the items for publishing
		pubItems := make([]publisher.PublishItem, len(itemsToPublish))
		for i, indexed := range itemsToPublish {
			pubItems[i] = indexed.item
		}

		// Publish batch asynchronously
		pending, err := batchPub.PublishBatchAsync(ctx, pubItems)
		if err != nil {
			// Context cancelled - don't retry
			if ctx.Err() != nil {
				return e.buildFinalResult(items, succeeded, err), ctx.Err()
			}
			lastError = err
			continue
		}

		// Wait for acks
		result, err := batchPub.WaitForAcks(ctx, pending, pubItems, timeout)

		// Context cancelled - return what we have
		if ctx.Err() != nil {
			// Mark successes from this round
			for i, indexed := range itemsToPublish {
				if i < len(pending) && pending[i].IsAcked() {
					succeeded[indexed.originalIdx] = true
				}
			}
			return e.buildFinalResult(items, succeeded, ctx.Err()), ctx.Err()
		}

		// Track successes and collect failures for next retry
		var failedItems []indexedItem
		for i, indexed := range itemsToPublish {
			if i < len(pending) && pending[i].IsAcked() {
				succeeded[indexed.originalIdx] = true
			} else {
				failedItems = append(failedItems, indexed)
				if i < len(pending) && pending[i].Err != nil && lastError == nil {
					lastError = pending[i].Err
				}
			}
		}

		// All succeeded
		if len(failedItems) == 0 {
			return e.buildFinalResult(items, succeeded, nil), nil
		}

		// Update error from result if we don't have one yet
		if lastError == nil && result.FirstError != nil {
			lastError = result.FirstError
		}

		// Prepare for next retry with only failed items
		itemsToPublish = failedItems

		e.logger.Debug("publish attempt completed",
			zap.Int("attempt", attempt),
			zap.Int("succeeded_this_round", len(pubItems)-len(failedItems)),
			zap.Int("failed_this_round", len(failedItems)))
	}

	// Exhausted retries - return final state
	return e.buildFinalResult(items, succeeded, lastError), lastError
}

// buildFinalResult constructs a BatchResult from the success tracking array.
func (e *Engine) buildFinalResult(items []publisher.PublishItem, succeeded []bool, lastError error) *publisher.BatchResult {
	result := &publisher.BatchResult{
		Total:       len(items),
		FailedItems: make([]int, 0),
		FirstError:  lastError,
	}

	var lastSuccessIdx = -1
	for i, ok := range succeeded {
		if ok {
			result.Succeeded++
			lastSuccessIdx = i
		} else {
			result.Failed++
			result.FailedItems = append(result.FailedItems, i)
		}
	}

	// Set last successful position for partial checkpointing
	if lastSuccessIdx >= 0 && items[lastSuccessIdx].Position.LSN != "" {
		pos := items[lastSuccessIdx].Position
		result.LastSuccessPosition = &pos
	}

	return result
}

// calculateBackoff returns the backoff duration for a given retry attempt.
func (e *Engine) calculateBackoff(attempt int) time.Duration {
	backoff := baseRetryBackoff * time.Duration(1<<(attempt-1))
	if backoff > maxRetryBackoff {
		backoff = maxRetryBackoff
	}
	return backoff
}

// flushSequential is the original sequential publish logic (fallback).
func (e *Engine) flushSequential(ctx context.Context, batch []*model.WALEvent, last *model.WALEvent) error {
	batchStart := time.Now()
	var eventCount int

	for _, evt := range batch {
		if evt.Begin || evt.Commit {
			continue
		}

		transformStart := time.Now()
		cdcEvt, err := e.transformer.Transform(ctx, evt)
		transformLatencyNs := uint64(time.Since(transformStart).Nanoseconds())
		e.transformLatency.Observe(transformLatencyNs)
		e.promMetrics.TransformLatency.Observe(transformLatencyNs)
		if err != nil {
			return fmt.Errorf("transform event: %w", err)
		}
		subject, err := publisher.SubjectForEvent(e.database, cdcEvt)
		if err != nil {
			model.ReleaseCDCEvent(cdcEvt)
			return fmt.Errorf("build subject: %w", err)
		}
		payload, err := marshalCDCEvent(cdcEvt)
		if err != nil {
			model.ReleaseCDCEvent(cdcEvt)
			return fmt.Errorf("marshal event: %w", err)
		}
		// Release event after marshaling
		model.ReleaseCDCEvent(cdcEvt)

		if err := e.publisher.PublishWithRetries(ctx, subject, payload, 3); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		eventCount++
		e.logger.Debug("published event", zap.String("subject", subject), zap.String("lsn", evt.LSN), zap.Uint64("txid", evt.TxID), zap.String("table", evt.Table), zap.String("op", string(evt.Operation)))
	}

	// Record metrics
	if eventCount > 0 {
		e.eventsProcessed.Add(uint64(eventCount))
		e.batchesPublished.Inc()
		batchLatencyUs := uint64(time.Since(batchStart).Microseconds())
		e.batchLatency.Observe(batchLatencyUs)

		e.promMetrics.EventsTotal.Add(uint64(eventCount))
		e.promMetrics.BatchesPublished.Inc()
		e.promMetrics.BatchLatency.Observe(batchLatencyUs)
		e.promMetrics.EventsPerSecond.Set(int64(e.eventsProcessed.Rate()))
	}

	// Checkpoint only on commit boundaries to ensure transactional consistency.
	if last.Commit {
		if err := e.checkpointer.MaybeFlush(ctx, last.Position, true, time.Now()); err != nil {
			return fmt.Errorf("checkpoint: %w", err)
		}
		e.logger.Debug("checkpointed lsn", zap.String("lsn", last.Position.LSN))
	}
	return nil
}

// publishTimeout returns the timeout for waiting on batch acks.
func (e *Engine) publishTimeout() time.Duration {
	timeout := e.batchTimeout * 3
	if timeout < 5*time.Second {
		timeout = 5 * time.Second
	}
	return timeout
}

// EngineMetrics contains throughput metrics for external access.
type EngineMetrics struct {
	EventsPerSecond        float64
	EventsTotal            uint64
	BatchesPublished       uint64
	BatchLatencyMeanUs     float64
	TransformLatencyMeanNs float64
}

// Metrics returns current throughput metrics.
func (e *Engine) Metrics() EngineMetrics {
	return EngineMetrics{
		EventsPerSecond:        e.eventsProcessed.Rate(),
		EventsTotal:            e.eventsProcessed.Total(),
		BatchesPublished:       e.batchesPublished.Value(),
		BatchLatencyMeanUs:     e.batchLatency.Mean(),
		TransformLatencyMeanNs: e.transformLatency.Mean(),
	}
}
