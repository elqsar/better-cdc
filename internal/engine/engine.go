package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"better-cdc/internal/checkpoint"
	"better-cdc/internal/model"
	"better-cdc/internal/parser"
	"better-cdc/internal/publisher"
	"better-cdc/internal/transformer"
	"better-cdc/internal/wal"

	"go.uber.org/zap"
)

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

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		e.logger.Debug("flushing batch", zap.Int("count", len(batch)))

		// Capture last event before processing to safely check for commit boundary.
		last := batch[len(batch)-1]

		// Flush preserves ordering within a batch; commit markers should align with WAL boundaries.
		for _, evt := range batch {
			if evt.Begin || evt.Commit {
				continue
			}
			cdcEvt, err := e.transformer.Transform(ctx, evt)
			if err != nil {
				return fmt.Errorf("transform event: %w", err)
			}
			subject, err := publisher.SubjectForEvent(e.database, cdcEvt)
			if err != nil {
				return fmt.Errorf("build subject: %w", err)
			}
			payload, err := json.Marshal(cdcEvt)
			if err != nil {
				return fmt.Errorf("marshal event: %w", err)
			}
			if err := e.publisher.PublishWithRetries(ctx, subject, payload, 3); err != nil {
				return fmt.Errorf("publish: %w", err)
			}
			e.logger.Debug("published event", zap.String("subject", subject), zap.String("lsn", evt.LSN), zap.Uint64("txid", evt.TxID), zap.String("table", evt.Table), zap.String("op", string(evt.Operation)))
		}

		// Checkpoint only on commit boundaries to ensure transactional consistency.
		// Events published before a commit may be re-delivered on restart, which is
		// acceptable for at-least-once semantics; consumers must handle idempotency.
		if last.Commit {
			if err := e.checkpointer.MaybeFlush(ctx, last.Position, true, time.Now()); err != nil {
				return fmt.Errorf("checkpoint: %w", err)
			}
			e.logger.Debug("checkpointed lsn", zap.String("lsn", last.Position.LSN))
		}
		batch = batch[:0]
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(e.batchTimeout)
		return nil
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
