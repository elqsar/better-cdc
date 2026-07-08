package engine

import (
	"context"
	"encoding/json"
	"fmt"
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

// marshalCDCEvent marshals a CDCEvent to JSON.
func marshalCDCEvent(evt *model.CDCEvent) ([]byte, error) {
	return json.Marshal(evt)
}

// FailurePolicy controls how the engine handles events that fail to publish
// with a permanent (non-retryable) error. Transient failures always stop the
// engine after retries regardless of policy, so an outage never skips data.
type FailurePolicy string

const (
	// FailurePolicyCrash stops the engine (default). The process exits and
	// replays the same events on restart.
	FailurePolicyCrash FailurePolicy = "crash"
	// FailurePolicyDLQ publishes a dead-letter record and continues.
	FailurePolicyDLQ FailurePolicy = "dlq"
	// FailurePolicySkip logs, counts, and continues.
	FailurePolicySkip FailurePolicy = "skip"
)

// Engine coordinates the end-to-end CDC flow.
type Engine struct {
	reader                      wal.Reader
	parser                      parser.Parser
	transformer                 transformer.Transformer
	publisher                   publisher.Publisher
	checkpointer                *checkpoint.Manager
	database                    string
	batchSize                   int
	batchTimeout                time.Duration
	maxPublishRetries           int
	unsafeUnorderedAsyncPublish bool
	failurePolicy               FailurePolicy
	dlqSubjectPrefix            string
	logger                      *zap.Logger

	// Throughput metrics (legacy, kept for backward compatibility)
	eventsProcessed  *metrics.RateCounter
	batchesPublished *metrics.Counter
	batchLatency     *metrics.Histogram
	transformLatency *metrics.Histogram

	// Prometheus metrics
	promMetrics *metrics.Metrics
}

func NewEngine(reader wal.Reader, parser parser.Parser, transformer transformer.Transformer, publisher publisher.Publisher, checkpointer *checkpoint.Manager, database string, batchSize int, batchTimeout time.Duration, maxPublishRetries int, unsafeUnorderedAsyncPublish bool, failurePolicy FailurePolicy, dlqSubjectPrefix string, logger *zap.Logger) *Engine {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Engine{
		reader:                      reader,
		parser:                      parser,
		transformer:                 transformer,
		publisher:                   publisher,
		checkpointer:                checkpointer,
		database:                    database,
		batchSize:                   batchSize,
		batchTimeout:                batchTimeout,
		maxPublishRetries:           maxPublishRetries,
		unsafeUnorderedAsyncPublish: unsafeUnorderedAsyncPublish,
		failurePolicy:               failurePolicy,
		dlqSubjectPrefix:            dlqSubjectPrefix,
		logger:                      logger,
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
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), e.shutdownTimeout())
		defer cancel()
		_ = e.reader.Stop(stopCtx)
	}()

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
	defer e.publisher.Close() //nolint:errcheck

	return e.runBatched(ctx, parsedStream)
}

func (e *Engine) runBatched(ctx context.Context, stream <-chan *model.WALEvent) error {
	batch := make([]*model.WALEvent, 0, e.batchSize)
	timer := time.NewTimer(e.batchTimeout)
	defer timer.Stop()

	// Check if publisher supports batch operations for high throughput
	batchPub, hasBatch := e.publisher.(publisher.BatchPublisher)
	if hasBatch {
		if e.unsafeUnorderedAsyncPublish {
			e.logger.Warn("batch publisher detected, using unsafe unordered async batch publishing")
		} else {
			e.logger.Info("batch publisher detected, using ordered batch publishing")
		}
	}

	flush := func(flushCtx context.Context) error {
		if len(batch) == 0 {
			return nil
		}
		e.logger.Debug("flushing batch", zap.Int("count", len(batch)))

		last := batch[len(batch)-1]

		var err error
		if hasBatch {
			err = e.flushWithBatchPublish(flushCtx, batch, last, batchPub)
		} else {
			err = e.flushSequential(flushCtx, batch, last)
		}

		// Release WALEvents back to pool. At this point all event data has
		// been marshaled to JSON bytes and CDCEvents have been released.
		for _, evt := range batch {
			model.ReleaseWALEvent(evt)
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
			shutdownCtx, cancel := context.WithTimeout(context.Background(), e.shutdownTimeout())
			defer cancel()
			if err := flush(shutdownCtx); err != nil {
				return err
			}
			if err := e.flushPendingCheckpoint(shutdownCtx); err != nil {
				return err
			}
			return nil
		case evt, ok := <-stream:
			if !ok {
				if err := flush(ctx); err != nil {
					return err
				}
				if err := e.flushPendingCheckpoint(ctx); err != nil {
					return err
				}
				if ep, ok := e.parser.(parser.ErrorReporter); ok {
					if perr := ep.Err(); perr != nil {
						return fmt.Errorf("parser stopped: %w", perr)
					}
				}
				if er, ok := e.reader.(wal.ErrorReporter); ok {
					if rerr := er.Err(); rerr != nil {
						return fmt.Errorf("replication stopped: %w", rerr)
					}
				}
				return nil
			}
			batch = append(batch, evt)
			if evt.Commit {
				if err := flush(ctx); err != nil {
					return err
				}
				continue
			}
			if e.batchSize > 0 && len(batch) >= e.batchSize {
				if err := flush(ctx); err != nil {
					return err
				}
			}
		case <-timer.C:
			if err := flush(ctx); err != nil {
				return err
			}
		}
	}
}

// Retry configuration for transient publish failures.
const (
	baseRetryBackoff = time.Second
	maxRetryBackoff  = 8 * time.Second
)

// quarantinesPoison reports whether the engine continues past permanently
// failing events instead of stopping.
func (e *Engine) quarantinesPoison() bool {
	return e.failurePolicy == FailurePolicyDLQ || e.failurePolicy == FailurePolicySkip
}

// quarantine handles a permanently failing event under the dlq/skip policy:
// it dead-letters the record (dlq mode), logs, and counts. The event is then
// treated as handled so the checkpoint can advance past it. A failed
// dead-letter publish is returned as an error so the caller falls back to
// crashing rather than dropping the event silently.
func (e *Engine) quarantine(ctx context.Context, rec *publisher.DeadLetterRecord) error {
	rec.Database = e.database
	rec.QuarantinedAt = time.Now()
	if e.failurePolicy == FailurePolicyDLQ {
		if err := publisher.PublishDeadLetter(ctx, e.publisher, e.dlqSubjectPrefix, rec); err != nil {
			return err
		}
	}
	e.promMetrics.EventsQuarantined.Inc()
	e.logger.Error("event quarantined due to permanent publish failure",
		zap.String("policy", string(e.failurePolicy)),
		zap.String("event_id", rec.EventID),
		zap.String("subject", rec.Subject),
		zap.String("schema", rec.Schema),
		zap.String("table", rec.Table),
		zap.String("operation", rec.Operation),
		zap.String("lsn", rec.LSN),
		zap.Uint64("txid", rec.TxID),
		zap.Int("payload_size", rec.PayloadSize),
		zap.String("error", rec.Error))
	return nil
}

// deadLetterRecordFromItem builds a dead-letter record for an item that
// permanently failed to publish.
func deadLetterRecordFromItem(item publisher.PublishItem, cause error) *publisher.DeadLetterRecord {
	rec := &publisher.DeadLetterRecord{
		EventID:   item.EventID,
		Subject:   item.Subject,
		Schema:    item.Schema,
		Table:     item.Table,
		Operation: item.Operation,
		LSN:       item.Position.LSN,
		TxID:      item.TxID,
		Error:     cause.Error(),
	}
	rec.SetPayload(item.Data)
	return rec
}

// deadLetterRecordFromWALEvent builds a dead-letter record for an event that
// failed before a publishable payload existed (transform/subject/marshal).
func deadLetterRecordFromWALEvent(evt *model.WALEvent, cause error) *publisher.DeadLetterRecord {
	return &publisher.DeadLetterRecord{
		Schema:    evt.Schema,
		Table:     evt.Table,
		Operation: string(evt.Operation),
		LSN:       evt.Position.LSN,
		TxID:      evt.TxID,
		Error:     cause.Error(),
	}
}

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
			// Transform failures are deterministic, so retrying or crashing
			// replays the same failure; quarantine when the policy allows it.
			if e.quarantinesPoison() {
				if qErr := e.quarantine(ctx, deadLetterRecordFromWALEvent(evt, fmt.Errorf("transform event: %w", err))); qErr == nil {
					continue
				}
			}
			// Release any already-transformed events on error
			for _, event := range cdcEvents {
				model.ReleaseCDCEvent(event)
			}
			return fmt.Errorf("transform event: %w", err)
		}
		cdcEvents = append(cdcEvents, cdcEvt)

		subject, err := publisher.SubjectForEvent(e.database, cdcEvt)
		if err != nil {
			if e.quarantinesPoison() {
				if qErr := e.quarantine(ctx, deadLetterRecordFromWALEvent(evt, fmt.Errorf("build subject: %w", err))); qErr == nil {
					continue
				}
			}
			for _, event := range cdcEvents {
				model.ReleaseCDCEvent(event)
			}
			return fmt.Errorf("build subject: %w", err)
		}

		payload, err := marshalCDCEvent(cdcEvt)
		if err != nil {
			if e.quarantinesPoison() {
				if qErr := e.quarantine(ctx, deadLetterRecordFromWALEvent(evt, fmt.Errorf("marshal event: %w", err))); qErr == nil {
					continue
				}
			}
			for _, event := range cdcEvents {
				model.ReleaseCDCEvent(event)
			}
			return fmt.Errorf("marshal event: %w", err)
		}

		items = append(items, publisher.PublishItem{
			Subject:   subject,
			Data:      payload,
			EventID:   cdcEvt.EventID,
			TxID:      evt.TxID,
			Position:  evt.Position,
			Schema:    cdcEvt.Schema,
			Table:     cdcEvt.Table,
			Operation: cdcEvt.Operation,
		})
	}

	// Release CDCEvents back to pool after marshaling (data is copied)
	for _, evt := range cdcEvents {
		model.ReleaseCDCEvent(evt)
	}

	if len(items) == 0 {
		// Only BEGIN/COMMIT markers, still checkpoint if needed
		if last.Commit {
			cpPos := checkpointPositionForCommit(last)
			if err := e.checkpointer.MaybeFlush(ctx, cpPos, true, time.Now()); err != nil {
				return fmt.Errorf("checkpoint: %w", err)
			}
			e.maybeAcknowledgeCheckpoint()
			e.logger.Debug("checkpointed lsn", zap.String("lsn", cpPos.LSN))
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

	// Handle partial success without advancing the checkpoint. WAL feedback is
	// only safe at commit boundaries after the whole flushed batch succeeds.
	if result.IsPartialSuccess() {
		e.promMetrics.PartialBatchFailures.Inc()
		e.logger.Warn("partial batch success after retries - checkpoint not advanced",
			zap.Int("succeeded", result.Succeeded),
			zap.Int("failed", result.Failed),
			zap.Int("total", result.Total),
			zap.Error(result.FirstError))
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
		cpPos := checkpointPositionForCommit(last)
		if err := e.checkpointer.MaybeFlush(ctx, cpPos, true, time.Now()); err != nil {
			return fmt.Errorf("checkpoint: %w", err)
		}
		e.maybeAcknowledgeCheckpoint()
		e.logger.Debug("checkpointed lsn", zap.String("lsn", cpPos.LSN))
	}

	return nil
}

// publishWithRetry attempts to publish items with retry logic for transient failures.
func (e *Engine) publishWithRetry(ctx context.Context, batchPub publisher.BatchPublisher, items []publisher.PublishItem) (*publisher.BatchResult, error) {
	if e.unsafeUnorderedAsyncPublish {
		return e.publishUnorderedWithRetry(ctx, batchPub, items)
	}
	return e.publishOrderedWithRetry(ctx, batchPub, items)
}

// publishOrderedWithRetry publishes one item at a time and waits for its ack
// before advancing. This preserves CDC ordering because later items are never
// committed to JetStream before earlier items have been acknowledged.
func (e *Engine) publishOrderedWithRetry(ctx context.Context, batchPub publisher.BatchPublisher, items []publisher.PublishItem) (*publisher.BatchResult, error) {
	timeout := e.publishTimeout()
	succeeded := make([]bool, len(items))

	for idx, item := range items {
		var lastError error
		var permanent bool
		pubItems := []publisher.PublishItem{item}

		for attempt := 0; attempt <= e.maxPublishRetries; attempt++ {
			if attempt > 0 {
				backoff := e.calculateBackoff(attempt)
				e.promMetrics.PublishRetries.Inc()
				e.logger.Warn("retrying failed publish",
					zap.Int("attempt", attempt),
					zap.Int("item_index", idx),
					zap.String("subject", item.Subject),
					zap.Duration("backoff", backoff))

				select {
				case <-ctx.Done():
					return e.buildFinalResult(items, succeeded, lastError), ctx.Err()
				case <-time.After(backoff):
				}
			}

			pending, err := batchPub.PublishBatchAsync(ctx, pubItems)
			if err != nil {
				if ctx.Err() != nil {
					return e.buildFinalResult(items, succeeded, err), ctx.Err()
				}
				lastError = err
				if publisher.IsPermanentPublishError(lastError) {
					permanent = true
					break
				}
				continue
			}

			result, _ := batchPub.WaitForAcks(ctx, pending, pubItems, timeout)

			if ctx.Err() != nil {
				if len(pending) > 0 && pending[0].IsAcked() {
					succeeded[idx] = true
				}
				return e.buildFinalResult(items, succeeded, ctx.Err()), ctx.Err()
			}

			if len(pending) > 0 && pending[0].IsAcked() {
				succeeded[idx] = true
				lastError = nil
				break
			}

			if len(pending) > 0 {
				if pErr := pending[0].GetErr(); pErr != nil {
					lastError = pErr
				}
			}
			if lastError == nil && result.FirstError != nil {
				lastError = result.FirstError
			}
			if lastError == nil {
				lastError = fmt.Errorf("publish item %d was not acknowledged", idx)
			}
			if publisher.IsPermanentPublishError(lastError) {
				// Retrying a poison message cannot succeed; stop burning
				// retries and let the failure policy decide.
				permanent = true
				break
			}
		}

		if !succeeded[idx] {
			if permanent && e.quarantinesPoison() {
				if qErr := e.quarantine(ctx, deadLetterRecordFromItem(item, lastError)); qErr == nil {
					succeeded[idx] = true
					continue
				} else {
					lastError = fmt.Errorf("quarantine after permanent failure %v: %w", lastError, qErr)
				}
			}
			return e.buildFinalResult(items, succeeded, lastError), lastError
		}
	}

	return e.buildFinalResult(items, succeeded, nil), nil
}

// publishUnorderedWithRetry retries only failed items with exponential backoff.
// It is unsafe for CDC ordering because later items can be committed before
// earlier failed items are retried.
func (e *Engine) publishUnorderedWithRetry(ctx context.Context, batchPub publisher.BatchPublisher, items []publisher.PublishItem) (*publisher.BatchResult, error) {
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

	for attempt := 0; attempt <= e.maxPublishRetries; attempt++ {
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
		result, _ := batchPub.WaitForAcks(ctx, pending, pubItems, timeout)

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
				continue
			}
			var pErr error
			if i < len(pending) {
				pErr = pending[i].GetErr()
			}
			if publisher.IsPermanentPublishError(pErr) {
				// Retrying a poison message cannot succeed; quarantine it or
				// stop immediately depending on the failure policy.
				if e.quarantinesPoison() {
					if qErr := e.quarantine(ctx, deadLetterRecordFromItem(indexed.item, pErr)); qErr != nil {
						qErr = fmt.Errorf("quarantine after permanent failure %v: %w", pErr, qErr)
						return e.buildFinalResult(items, succeeded, qErr), qErr
					}
					succeeded[indexed.originalIdx] = true
					continue
				}
				return e.buildFinalResult(items, succeeded, pErr), pErr
			}
			failedItems = append(failedItems, indexed)
			if pErr != nil && lastError == nil {
				lastError = pErr
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

	var lastContiguousIdx = -1
	var contiguousBroken bool
	for i, ok := range succeeded {
		if ok {
			result.Succeeded++
			if !contiguousBroken {
				lastContiguousIdx = i
			}
		} else {
			result.Failed++
			result.FailedItems = append(result.FailedItems, i)
			contiguousBroken = true
		}
	}

	// Set last successful position for partial checkpointing.
	// Only checkpoint up to the last contiguous success from the start
	// to avoid skipping failed events that precede later successes.
	if lastContiguousIdx >= 0 && items[lastContiguousIdx].Position.LSN != "" {
		pos := items[lastContiguousIdx].Position
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
			if e.quarantinesPoison() {
				if qErr := e.quarantine(ctx, deadLetterRecordFromWALEvent(evt, fmt.Errorf("transform event: %w", err))); qErr == nil {
					continue
				}
			}
			return fmt.Errorf("transform event: %w", err)
		}
		subject, err := publisher.SubjectForEvent(e.database, cdcEvt)
		if err != nil {
			model.ReleaseCDCEvent(cdcEvt)
			if e.quarantinesPoison() {
				if qErr := e.quarantine(ctx, deadLetterRecordFromWALEvent(evt, fmt.Errorf("build subject: %w", err))); qErr == nil {
					continue
				}
			}
			return fmt.Errorf("build subject: %w", err)
		}
		payload, err := marshalCDCEvent(cdcEvt)
		if err != nil {
			model.ReleaseCDCEvent(cdcEvt)
			if e.quarantinesPoison() {
				if qErr := e.quarantine(ctx, deadLetterRecordFromWALEvent(evt, fmt.Errorf("marshal event: %w", err))); qErr == nil {
					continue
				}
			}
			return fmt.Errorf("marshal event: %w", err)
		}
		eventID := cdcEvt.EventID
		// Release event after marshaling
		model.ReleaseCDCEvent(cdcEvt)

		if err := e.publisher.PublishWithRetries(ctx, subject, payload, e.maxPublishRetries, eventID); err != nil {
			if publisher.IsPermanentPublishError(err) && e.quarantinesPoison() {
				rec := deadLetterRecordFromWALEvent(evt, err)
				rec.EventID = eventID
				rec.Subject = subject
				rec.SetPayload(payload)
				if qErr := e.quarantine(ctx, rec); qErr == nil {
					continue
				} else {
					return fmt.Errorf("quarantine after permanent failure %v: %w", err, qErr)
				}
			}
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
		cpPos := checkpointPositionForCommit(last)
		if err := e.checkpointer.MaybeFlush(ctx, cpPos, true, time.Now()); err != nil {
			return fmt.Errorf("checkpoint: %w", err)
		}
		e.maybeAcknowledgeCheckpoint()
		e.logger.Debug("checkpointed lsn", zap.String("lsn", cpPos.LSN))
	}
	return nil
}

func checkpointPositionForCommit(last *model.WALEvent) model.WALPosition {
	if last == nil {
		return model.WALPosition{}
	}
	if last.Position.LSN != "" {
		return last.Position
	}
	if last.LSN != "" {
		return model.WALPosition{LSN: last.LSN}
	}
	return model.WALPosition{}
}

func (e *Engine) maybeAcknowledgeCheckpoint() {
	if e.checkpointer == nil {
		return
	}
	ack, ok := e.reader.(wal.Acknowledger)
	if !ok {
		return
	}
	pos := e.checkpointer.LastAcked()
	if pos.LSN == "" {
		return
	}
	if err := ack.SetAckedPosition(pos); err != nil {
		e.logger.Warn("failed to update replication acked position", zap.String("lsn", pos.LSN), zap.Error(err))
	}
}

func (e *Engine) flushPendingCheckpoint(ctx context.Context) error {
	if e.checkpointer == nil {
		return nil
	}
	if err := e.checkpointer.FlushPending(ctx, time.Now()); err != nil {
		return fmt.Errorf("flush pending checkpoint: %w", err)
	}
	e.maybeAcknowledgeCheckpoint()
	return nil
}

// publishTimeout returns the timeout for waiting on batch acks.
func (e *Engine) publishTimeout() time.Duration {
	timeout := max(e.batchTimeout*3, 5*time.Second)
	return timeout
}

func (e *Engine) shutdownTimeout() time.Duration {
	return max(e.publishTimeout()*2, 10*time.Second)
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
