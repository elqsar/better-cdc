package wal

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"better-cdc/internal/metrics"
	"better-cdc/internal/model"
	"better-cdc/internal/parser"

	"go.uber.org/zap"
)

type replicationStartFunc func(context.Context, pglogrepl.LSN) error
type replicationLoopFunc func(context.Context, pglogrepl.LSN, chan<- *parser.RawMessage) (pglogrepl.LSN, error)

type fatalReplicationError struct {
	err error
}

func (e fatalReplicationError) Error() string {
	return e.err.Error()
}

func (e fatalReplicationError) Unwrap() error {
	return e.err
}

// Reader streams logical replication changes from PostgreSQL.
type Reader interface {
	Start(ctx context.Context) error
	ReadWAL(ctx context.Context, position model.WALPosition) (<-chan *parser.RawMessage, error)
	GetCurrentPosition(ctx context.Context) (model.WALPosition, error)
	Stop(ctx context.Context) error
}

// SlotConfig captures replication slot settings to align with Postgres 15 logical decoding.
type SlotConfig struct {
	SlotName     string
	Plugin       string // pgoutput or wal2json
	Publications []string
	DatabaseURL  string
	TableFilter  map[string]struct{} // schema.table allowlist; empty means all
}

// PGReader streams logical replication; supports wal2json (today) and leaves a hook for pgoutput.
type PGReader struct {
	slot       SlotConfig
	conn       *pgconn.PgConn
	errs       *metrics.Counter
	logger     *zap.Logger
	bufferSize int // Output channel buffer size for throughput optimization
}

func NewPGReader(slot SlotConfig, bufferSize int, logger *zap.Logger) *PGReader {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &PGReader{
		slot:       slot,
		errs:       metrics.NewCounter("replication_errors"),
		logger:     logger,
		bufferSize: bufferSize,
	}
}

func (r *PGReader) Start(ctx context.Context) error {
	if r.slot.DatabaseURL == "" {
		return fmt.Errorf("missing database url for replication")
	}
	cfg, err := pgconn.ParseConfig(r.slot.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse db url: %w", err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect replication: %w", err)
	}
	r.conn = conn
	r.logger.Info("replication connection established", zap.String("slot", r.slot.SlotName), zap.String("plugin", r.slot.Plugin))
	return nil
}

func (r *PGReader) ReadWAL(ctx context.Context, position model.WALPosition) (<-chan *parser.RawMessage, error) {
	if r.conn == nil {
		return nil, fmt.Errorf("replication connection not started")
	}

	startFn, loopFn, pluginName, err := r.replicationHandlers()
	if err != nil {
		return nil, err
	}

	startLSN := pglogrepl.LSN(0)
	if position.LSN != "" {
		if lsn, err := pglogrepl.ParseLSN(position.LSN); err == nil {
			startLSN = lsn
		}
	}

	r.logger.Info("starting replication",
		zap.String("plugin", pluginName),
		zap.String("lsn", startLSN.String()),
		zap.Int("buffer_size", r.bufferSize))

	out := make(chan *parser.RawMessage, r.bufferSize)
	go r.runReplicationLoop(ctx, startLSN, pluginName, startFn, loopFn, out)
	return out, nil
}

func (r *PGReader) GetCurrentPosition(ctx context.Context) (model.WALPosition, error) {
	if r.conn == nil {
		return model.WALPosition{}, fmt.Errorf("replication connection not started")
	}
	sys, err := pglogrepl.IdentifySystem(ctx, r.conn)
	if err != nil {
		return model.WALPosition{}, fmt.Errorf("identify system: %w", err)
	}
	return model.WALPosition{LSN: sys.XLogPos.String()}, nil
}

func (r *PGReader) Stop(ctx context.Context) error {
	if r.conn != nil {
		r.logger.Info("stopping replication connection")
	}
	return r.resetConnection(ctx)
}

func (r *PGReader) replicationHandlers() (replicationStartFunc, replicationLoopFunc, string, error) {
	switch r.slot.Plugin {
	case "", "wal2json":
		return r.startWal2JSON, r.loopWal2JSON, "wal2json", nil
	case "pgoutput":
		return r.startPGOutput, r.loopPGOutput, "pgoutput", nil
	}
	return nil, nil, "", fmt.Errorf("unsupported plugin: %s", r.slot.Plugin)
}

func (r *PGReader) runReplicationLoop(ctx context.Context, startLSN pglogrepl.LSN, plugin string, startFn replicationStartFunc, loopFn replicationLoopFunc, out chan<- *parser.RawMessage) {
	defer close(out)

	resumeLSN := startLSN
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		if r.conn == nil {
			if err := r.Start(ctx); err != nil {
				if isFatalReplicationError(err) {
					r.logger.Error("replication connection failed", zap.String("plugin", plugin), zap.Error(err))
					return
				}
				r.logger.Warn("replication connection failed, will retry", zap.String("plugin", plugin), zap.Error(err))
				backoff = r.sleepWithBackoff(ctx, backoff, maxBackoff)
				continue
			}
		}

		if err := startFn(ctx, resumeLSN); err != nil {
			if ctx.Err() != nil {
				return
			}
			if isFatalReplicationError(err) {
				r.logger.Error("replication start failed", zap.String("plugin", plugin), zap.Error(err))
				return
			}
			r.logger.Warn("replication start failed, will retry", zap.String("plugin", plugin), zap.Error(err), zap.String("lsn", resumeLSN.String()))
			_ = r.resetConnection(ctx)
			backoff = r.sleepWithBackoff(ctx, backoff, maxBackoff)
			continue
		}
		backoff = time.Second

		lastLSN, err := loopFn(ctx, resumeLSN, out)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			return
		}
		if isFatalReplicationError(err) {
			r.logger.Error("replication loop stopped due to fatal error", zap.String("plugin", plugin), zap.Error(err))
			return
		}
		if lastLSN != 0 {
			resumeLSN = lastLSN
		}
		r.logger.Warn("replication loop error, reconnecting", zap.String("plugin", plugin), zap.Error(err), zap.String("resume_lsn", resumeLSN.String()))
		_ = r.resetConnection(ctx)
		backoff = r.sleepWithBackoff(ctx, backoff, maxBackoff)
	}
}

func (r *PGReader) startWal2JSON(ctx context.Context, startLSN pglogrepl.LSN) error {
	pluginArgs := []string{
		"\"pretty-print\" 'false'",
		"\"include-xids\" 'true'",
		"\"include-timestamp\" 'true'",
		"\"format-version\" '2'",
	}

	if err := pglogrepl.StartReplication(ctx, r.conn, r.slot.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	return nil
}

func (r *PGReader) loopWal2JSON(ctx context.Context, startLSN pglogrepl.LSN, out chan<- *parser.RawMessage) (pglogrepl.LSN, error) {
	lastLSN := startLSN
	standbyTimeout := 45 * time.Second
	standbyDeadline := time.Now().Add(standbyTimeout)

	for {
		if ctx.Err() != nil {
			return lastLSN, ctx.Err()
		}
		msgCtx, cancel := context.WithDeadline(ctx, standbyDeadline)
		msg, err := r.conn.ReceiveMessage(msgCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				standbyDeadline = time.Now().Add(standbyTimeout)
				continue
			}
			if ctx.Err() != nil {
				return lastLSN, ctx.Err()
			}
			return lastLSN, fmt.Errorf("receive replication message: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			return lastLSN, fatalReplicationError{fmt.Errorf("replication error response: %s", m.Message)}
		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				r.logger.Info("replication copydata empty payload")
				continue
			}
			switch m.Data[0] {
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(m.Data[1:])
				if err != nil {
					r.errs.Inc()
					r.logger.Warn("parse xlog data failed", zap.Error(err))
					continue
				}
				lastLSN = xld.WALStart
				// Copy data to avoid race condition - pglogrepl reuses the buffer
				dataCopy := make([]byte, len(xld.WALData))
				copy(dataCopy, xld.WALData)
				raw := &parser.RawMessage{
					Plugin:   parser.PluginWal2JSON,
					WALStart: xld.WALStart,
					Data:     dataCopy,
				}
				select {
				case <-ctx.Done():
					return lastLSN, ctx.Err()
				case out <- raw:
				}
				standbyDeadline = time.Now().Add(standbyTimeout)
				if err := r.sendStandbyStatus(ctx, lastLSN, false); err != nil {
					r.errs.Inc()
					r.logger.Warn("send standby status failed", zap.Error(err))
				}
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					r.errs.Inc()
					r.logger.Warn("parse keepalive failed", zap.Error(err))
					continue
				}
				if pkm.ServerWALEnd > lastLSN {
					lastLSN = pkm.ServerWALEnd
				}
				standbyDeadline = time.Now().Add(standbyTimeout)
				if err := r.sendStandbyStatus(ctx, lastLSN, pkm.ReplyRequested); err != nil {
					r.errs.Inc()
					r.logger.Warn("send standby status failed", zap.Error(err))
				}
			default:
				r.logger.Warn("unexpected replication copydata", zap.Uint8("id", m.Data[0]))
			}
		default:
			r.logger.Warn("unexpected replication message", zap.String("type", fmt.Sprintf("%T", m)))
		}
	}
}

func (r *PGReader) startPGOutput(ctx context.Context, startLSN pglogrepl.LSN) error {
	args := []string{"proto_version '1'"}
	if len(r.slot.Publications) > 0 {
		args = append(args, fmt.Sprintf("publication_names '%s'", joinPublications(r.slot.Publications)))
	}
	if err := pglogrepl.StartReplication(ctx, r.conn, r.slot.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: args,
	}); err != nil {
		return fmt.Errorf("start replication pgoutput: %w", err)
	}
	return nil
}

func (r *PGReader) loopPGOutput(ctx context.Context, startLSN pglogrepl.LSN, out chan<- *parser.RawMessage) (pglogrepl.LSN, error) {
	lastLSN := startLSN
	standbyTimeout := 45 * time.Second
	standbyDeadline := time.Now().Add(standbyTimeout)

	for {
		if ctx.Err() != nil {
			return lastLSN, ctx.Err()
		}
		msgCtx, cancel := context.WithDeadline(ctx, standbyDeadline)
		msg, err := r.conn.ReceiveMessage(msgCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				standbyDeadline = time.Now().Add(standbyTimeout)
				continue
			}
			if ctx.Err() != nil {
				return lastLSN, ctx.Err()
			}
			return lastLSN, fmt.Errorf("receive replication message: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			return lastLSN, fatalReplicationError{fmt.Errorf("replication error response: %s", m.Message)}
		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				continue
			}
			switch m.Data[0] {
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(m.Data[1:])
				if err != nil {
					r.errs.Inc()
					r.logger.Warn("parse xlog data failed", zap.Error(err))
					continue
				}
				lastLSN = xld.WALStart
				// Copy data to avoid race condition - pglogrepl reuses the buffer
				dataCopy := make([]byte, len(xld.WALData))
				copy(dataCopy, xld.WALData)
				raw := &parser.RawMessage{
					Plugin:   parser.PluginPGOutput,
					WALStart: xld.WALStart,
					Data:     dataCopy,
				}
				select {
				case <-ctx.Done():
					return lastLSN, ctx.Err()
				case out <- raw:
				}
				standbyDeadline = time.Now().Add(standbyTimeout)
				if err := r.sendStandbyStatus(ctx, lastLSN, false); err != nil {
					r.errs.Inc()
					r.logger.Warn("send standby status failed", zap.Error(err))
				}
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					r.errs.Inc()
					r.logger.Warn("parse keepalive failed", zap.Error(err))
					continue
				}
				if pkm.ServerWALEnd > lastLSN {
					lastLSN = pkm.ServerWALEnd
				}
				standbyDeadline = time.Now().Add(standbyTimeout)
				if err := r.sendStandbyStatus(ctx, lastLSN, pkm.ReplyRequested); err != nil {
					r.errs.Inc()
					r.logger.Warn("send standby status failed", zap.Error(err))
				}
			default:
				r.logger.Warn("unexpected replication copydata", zap.Uint8("id", m.Data[0]))
			}
		default:
			r.logger.Warn("unexpected replication message", zap.String("type", fmt.Sprintf("%T", m)))
		}
	}
}

func (r *PGReader) resetConnection(ctx context.Context) error {
	if r.conn == nil {
		return nil
	}
	err := r.conn.Close(ctx)
	if err != nil {
		r.logger.Warn("close replication connection failed", zap.Error(err))
	}
	r.conn = nil
	return err
}

func (r *PGReader) sleepWithBackoff(ctx context.Context, backoff, max time.Duration) time.Duration {
	delay := withJitter(backoff)
	select {
	case <-ctx.Done():
		return backoff
	case <-time.After(delay):
	}
	return nextBackoff(backoff, max)
}

func isFatalReplicationError(err error) bool {
	if err == nil {
		return false
	}
	var fatal fatalReplicationError
	if errors.As(err, &fatal) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return isFatalPgError(pgErr)
	}
	return false
}

func isFatalPgError(err *pgconn.PgError) bool {
	if err == nil {
		return false
	}
	if strings.HasPrefix(err.Code, "28") { // invalid auth
		return true
	}
	switch err.Code {
	case "42501", // insufficient privilege
		"42704": // undefined object (e.g., slot missing)
		return true
	default:
		return false
	}
}

func nextBackoff(current, max time.Duration) time.Duration {
	if current <= 0 {
		return time.Second
	}
	next := current * 2
	if next > max {
		return max
	}
	return next
}

func withJitter(base time.Duration) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	spread := base / 2
	extra := time.Duration(rand.Int63n(int64(spread) + 1))
	return base + extra
}

func (r *PGReader) sendStandbyStatus(ctx context.Context, lsn pglogrepl.LSN, requestReply bool) error {
	if lsn == 0 {
		return nil
	}
	return pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
		ReplyRequested:   requestReply,
	})
}

func joinPublications(pubs []string) string {
	return strings.Join(pubs, ",")
}
