package wal

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/elqsar/better-cdc/internal/budget"
	"github.com/elqsar/better-cdc/internal/metrics"
	"github.com/elqsar/better-cdc/internal/model"
	"github.com/elqsar/better-cdc/internal/parser"

	"go.uber.org/zap"
)

const maxBackoff = 30 * time.Second

var replicationStandbyTimeoutNanos atomic.Int64

type (
	replicationStartFunc func(context.Context, pglogrepl.LSN) error
	replicationLoopFunc  func(context.Context, pglogrepl.LSN, chan<- *parser.RawMessage) (pglogrepl.LSN, error)
)

type fatalReplicationError struct {
	err error
}

func (e fatalReplicationError) Error() string {
	return e.err.Error()
}

func (e fatalReplicationError) Unwrap() error {
	return e.err
}

// Acknowledger lets the engine tell the WAL reader the highest durably processed position
// that is safe to acknowledge back to PostgreSQL.
type Acknowledger interface {
	SetAckedPosition(pos model.WALPosition) error
}

// ErrorReporter exposes the fatal error (if any) that caused the reader to stop.
// The engine checks this when the WAL stream channel closes to distinguish
// a fatal failure from a normal shutdown.
type ErrorReporter interface {
	Err() error
}

var sendStandbyStatusUpdate = pglogrepl.SendStandbyStatusUpdate
var startReplication = pglogrepl.StartReplication
var receiveReplicationMessage = func(ctx context.Context, conn *pgconn.PgConn) (pgproto3.BackendMessage, error) {
	return conn.ReceiveMessage(ctx)
}
var isReplicationReceiveTimeout = pgconn.Timeout
var closeReplicationConn = func(ctx context.Context, conn *pgconn.PgConn) error {
	return conn.Close(ctx)
}

func init() {
	replicationStandbyTimeoutNanos.Store(int64(45 * time.Second))
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
	Metrics          *metrics.Metrics
	FeedbackInterval time.Duration
	MaxBufferBytes   int64
	SlotName         string
	Plugin           string // pgoutput or wal2json
	Publications     []string
	DatabaseURL      string
	TableFilter      map[string]struct{} // schema.table allowlist; empty means all
}

// PGReader streams logical replication; supports wal2json (today) and leaves a hook for pgoutput.
type PGReader struct {
	slot        SlotConfig
	conn        *pgconn.PgConn
	errs        *metrics.Counter
	logger      *zap.Logger
	bufferSize  int // Output channel buffer size for throughput optimization
	promMetrics *metrics.Metrics

	ackedLSN     atomic.Uint64
	active       atomic.Bool
	receivedLSN  atomic.Uint64
	lastReceived atomic.Int64
	lastAcked    atomic.Int64
	rawBudget    *budget.Budget

	// After ReadWAL spawns the replication goroutine, that goroutine is the
	// sole owner of conn: it reconnects, sends the final standby status, and
	// closes the connection on exit. Stop cancels loopCancel and waits on
	// loopDone instead of touching conn.
	loopCancel context.CancelFunc
	loopDone   chan struct{}

	mu       sync.Mutex
	fatalErr error
}

func NewPGReader(slot SlotConfig, bufferSize int, logger *zap.Logger) *PGReader {
	if logger == nil {
		logger = zap.NewNop()
	}
	limit := slot.MaxBufferBytes
	if limit <= 0 {
		limit = 64 << 20
	}
	return &PGReader{
		rawBudget:   budget.New(limit),
		slot:        slot,
		errs:        metrics.NewCounter("replication_errors"),
		logger:      logger,
		bufferSize:  bufferSize,
		promMetrics: metrics.OrNew(slot.Metrics),
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
		lsn, err := pglogrepl.ParseLSN(position.LSN)
		if err != nil {
			return nil, fmt.Errorf("invalid start LSN: %w", err)
		}
		startLSN = lsn
	}
	r.setAckedLSN(startLSN)

	r.logger.Info("starting replication",
		zap.String("plugin", pluginName),
		zap.String("lsn", startLSN.String()),
		zap.Int("buffer_size", r.bufferSize))

	out := make(chan *parser.RawMessage, r.bufferSize)
	loopCtx, cancel := context.WithCancel(ctx)
	r.loopCancel = cancel
	r.loopDone = make(chan struct{})
	go func() {
		defer close(r.loopDone)
		defer cancel()
		r.runReplicationLoop(loopCtx, startLSN, pluginName, startFn, loopFn, out)
	}()
	return out, nil
}

// GetCurrentPosition reports the server's current WAL position. It must only
// be called before ReadWAL: once streaming starts, the replication goroutine
// owns the connection and it is in CopyBoth mode, where IdentifySystem is
// not valid.
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
	finalCtx := ctx
	if finalCtx == nil || finalCtx.Err() != nil {
		var cancel context.CancelFunc
		finalCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}
	if r.loopDone != nil {
		// The replication goroutine owns the connection: cancel it and wait
		// for its exit path to send the final standby status and close.
		r.loopCancel()
		select {
		case <-r.loopDone:
			return nil
		case <-finalCtx.Done():
			return fmt.Errorf("wait for replication loop to stop: %w", finalCtx.Err())
		}
	}
	// ReadWAL was never called, so no other goroutine touches conn.
	if r.conn != nil {
		r.logger.Info("stopping replication connection")
		if err := r.sendStandbyStatus(finalCtx, true); err != nil {
			r.logger.Warn("final standby status failed", zap.Error(err))
		}
	}
	return r.resetConnection(finalCtx)
}

func (r *PGReader) replicationHandlers() (replicationStartFunc, replicationLoopFunc, string, error) {
	switch r.slot.Plugin {
	case "wal2json":
		return r.startWal2JSON, r.loopWal2JSON, "wal2json", nil
	case "", "pgoutput":
		return r.startPGOutput, r.loopPGOutput, "pgoutput", nil
	}
	return nil, nil, "", fmt.Errorf("unsupported plugin: %s", r.slot.Plugin)
}

func (r *PGReader) runReplicationLoop(ctx context.Context, startLSN pglogrepl.LSN, plugin string, startFn replicationStartFunc, loopFn replicationLoopFunc, out chan<- *parser.RawMessage) {
	defer close(out)
	defer r.active.Store(false)
	defer r.cleanupConnection()

	resumeLSN := startLSN
	backoff := time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		if r.conn == nil {
			if err := r.Start(ctx); err != nil {
				if isFatalReplicationError(err) {
					r.setFatalError(err)
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
				r.setFatalError(err)
				r.logger.Error("replication start failed", zap.String("plugin", plugin), zap.Error(err))
				return
			}
			r.logger.Warn("replication start failed, will retry", zap.String("plugin", plugin), zap.Error(err), zap.String("lsn", resumeLSN.String()))
			_ = r.resetConnection(ctx)
			backoff = r.sleepWithBackoff(ctx, backoff, maxBackoff)
			continue
		}
		backoff = time.Second
		select {
		case <-ctx.Done():
			return
		case out <- &parser.RawMessage{Reset: true}:
		}
		r.active.Store(true)
		lastLSN, err := loopFn(ctx, resumeLSN, out)
		r.active.Store(false)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			return
		}
		if isFatalReplicationError(err) {
			r.setFatalError(err)
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

	if err := startReplication(ctx, r.conn, r.slot.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	return nil
}

func (r *PGReader) loopWal2JSON(ctx context.Context, startLSN pglogrepl.LSN, out chan<- *parser.RawMessage) (pglogrepl.LSN, error) {
	return r.loopMessages(ctx, parser.PluginWal2JSON, out)
}

func (r *PGReader) startPGOutput(ctx context.Context, startLSN pglogrepl.LSN) error {
	args := []string{"proto_version '1'"}
	if len(r.slot.Publications) > 0 {
		args = append(args, fmt.Sprintf("publication_names '%s'", joinPublications(r.slot.Publications)))
	}
	if err := startReplication(ctx, r.conn, r.slot.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: args,
	}); err != nil {
		return fmt.Errorf("start replication pgoutput: %w", err)
	}
	return nil
}

func (r *PGReader) loopPGOutput(ctx context.Context, startLSN pglogrepl.LSN, out chan<- *parser.RawMessage) (pglogrepl.LSN, error) {
	return r.loopMessages(ctx, parser.PluginPGOutput, out)
}

func (r *PGReader) loopMessages(ctx context.Context, plugin parser.Plugin, out chan<- *parser.RawMessage) (pglogrepl.LSN, error) {
	standbyTimeout := r.standbyTimeout()
	if r.slot.FeedbackInterval > 0 && r.slot.FeedbackInterval < standbyTimeout {
		standbyTimeout = r.slot.FeedbackInterval
	}
	standbyDeadline := time.Now().Add(standbyTimeout)

	for {
		if ctx.Err() != nil {
			return r.currentAckedLSN(), ctx.Err()
		}
		msgCtx, cancel := context.WithDeadline(ctx, standbyDeadline)
		msg, err := receiveReplicationMessage(msgCtx, r.conn)
		cancel()
		if err != nil {
			if isReplicationReceiveTimeout(err) {
				if ctx.Err() != nil {
					return r.currentAckedLSN(), ctx.Err()
				}
				standbyDeadline, err = r.handleStandbyTimeout(ctx, standbyTimeout)
				if err != nil {
					return r.currentAckedLSN(), err
				}
				continue
			}
			if ctx.Err() != nil {
				return r.currentAckedLSN(), ctx.Err()
			}
			return r.currentAckedLSN(), fmt.Errorf("receive replication message: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			return r.currentAckedLSN(), pgconn.ErrorResponseToPgError(m)
		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				return r.currentAckedLSN(), fatalReplicationError{fmt.Errorf("empty replication frame")}
			}
			switch m.Data[0] {
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(m.Data[1:])
				if err != nil {
					r.errs.Inc()
					r.promMetrics.ReplicationErrors.Inc()
					return r.currentAckedLSN(), fatalReplicationError{fmt.Errorf("parse xlog data: %w", err)}
				}
				r.receivedLSN.Store(uint64(xld.WALStart))
				r.lastReceived.Store(time.Now().Unix())
				release, err := r.acquireRaw(ctx, int64(len(xld.WALData))+128, standbyTimeout, &standbyDeadline)
				if err != nil {
					return r.currentAckedLSN(), err
				}
				// Copy data to avoid race condition - pglogrepl reuses the buffer
				dataCopy := make([]byte, len(xld.WALData))
				copy(dataCopy, xld.WALData)
				raw := &parser.RawMessage{
					Plugin:       plugin,
					ReleaseBytes: release,
					WALStart:     xld.WALStart,
					Data:         dataCopy,
				}
				if err := r.deliverRaw(ctx, out, raw, standbyTimeout, &standbyDeadline); err != nil {
					release()
					return r.currentAckedLSN(), err
				}
				if time.Now().After(standbyDeadline) {
					standbyDeadline = time.Now().Add(standbyTimeout)
					if err := r.sendStandbyStatus(ctx, false); err != nil {
						r.errs.Inc()
						r.promMetrics.ReplicationErrors.Inc()
						return r.currentAckedLSN(), fmt.Errorf("standby feedback: %w", err)
					}
				}
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					r.errs.Inc()
					r.promMetrics.ReplicationErrors.Inc()
					return r.currentAckedLSN(), fatalReplicationError{fmt.Errorf("parse keepalive: %w", err)}
				}
				r.lastReceived.Store(time.Now().Unix())
				if pkm.ReplyRequested || time.Now().After(standbyDeadline) {
					standbyDeadline = time.Now().Add(standbyTimeout)
					if err := r.sendStandbyStatus(ctx, pkm.ReplyRequested); err != nil {
						r.errs.Inc()
						r.promMetrics.ReplicationErrors.Inc()
						return r.currentAckedLSN(), fmt.Errorf("standby feedback: %w", err)
					}
				}
			default:
				return r.currentAckedLSN(), fatalReplicationError{fmt.Errorf("unexpected replication frame %q", m.Data[0])}
			}
		case *pgproto3.CommandComplete, *pgproto3.CopyDone, *pgproto3.ReadyForQuery:
			// PostgreSQL may finish CopyBoth cleanly during shutdown. Replay from
			// the acknowledged commit on a new session, never from received WAL.
			return r.currentAckedLSN(), fmt.Errorf("replication session completed")
		case *pgproto3.NoticeResponse:
		// Notices contain no change data.
		default:
			return r.currentAckedLSN(), fatalReplicationError{fmt.Errorf("unexpected replication message %T", m)}
		}
	}
}

// cleanupConnection sends a final standby status and closes the connection.
// It runs on the replication goroutine's exit path so that goroutine stays
// the sole owner of conn; the context is fresh because the loop usually
// exits precisely because its own context was canceled.
func (r *PGReader) cleanupConnection() {
	if r.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.logger.Info("stopping replication connection")
	if err := r.sendStandbyStatus(ctx, true); err != nil {
		r.logger.Warn("final standby status failed", zap.Error(err))
	}
	_ = r.resetConnection(ctx)
}

func (r *PGReader) resetConnection(ctx context.Context) error {
	if r.conn == nil {
		return nil
	}
	err := closeReplicationConn(ctx, r.conn)
	if err != nil {
		r.logger.Warn("close replication connection failed", zap.Error(err))
	}
	r.conn = nil
	return err
}

func (r *PGReader) standbyTimeout() time.Duration {
	return time.Duration(replicationStandbyTimeoutNanos.Load())
}

func (r *PGReader) handleStandbyTimeout(ctx context.Context, standbyTimeout time.Duration) (time.Time, error) {
	if err := r.sendStandbyStatus(ctx, false); err != nil {
		r.errs.Inc()
		r.promMetrics.ReplicationErrors.Inc()
		return time.Time{}, fmt.Errorf("standby feedback: %w", err)
	}
	return time.Now().Add(standbyTimeout), nil
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
		"42704", // undefined object (e.g., slot missing)
		"55000", // required WAL removed / invalid slot state
		"22023": // invalid replication configuration
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
	extra := time.Duration(rand.Int64N(int64(spread) + 1))
	return base + extra
}

func (r *PGReader) sendStandbyStatus(ctx context.Context, requestReply bool) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	lsn := r.currentAckedLSN()
	if lsn == 0 && !requestReply {
		return nil
	}
	return sendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
		ReplyRequested:   requestReply,
	})
}

func joinPublications(pubs []string) string {
	return strings.ReplaceAll(strings.Join(pubs, ","), "'", "''")
}

func (r *PGReader) SetAckedPosition(pos model.WALPosition) error {
	if pos.LSN == "" {
		return nil
	}
	lsn, err := pglogrepl.ParseLSN(pos.LSN)
	if err != nil {
		return fmt.Errorf("parse acked lsn %q: %w", pos.LSN, err)
	}
	r.setAckedLSN(lsn)
	return nil
}

func (r *PGReader) currentAckedLSN() pglogrepl.LSN {
	return pglogrepl.LSN(r.ackedLSN.Load())
}

func (r *PGReader) setAckedLSN(lsn pglogrepl.LSN) {
	for {
		current := r.currentAckedLSN()
		if lsn <= current {
			return
		}
		if r.ackedLSN.CompareAndSwap(uint64(current), uint64(lsn)) {
			r.lastAcked.Store(time.Now().Unix())
			return
		}
	}
}

func (r *PGReader) setFatalError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fatalErr == nil {
		r.fatalErr = err
	}
}

// Err returns the fatal error that caused the reader to stop, or nil.
func (r *PGReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fatalErr
}

// Ready reflects this instance's streaming session, not merely database reachability.
func (r *PGReader) Ready(context.Context) error {
	if err := r.Err(); err != nil {
		return err
	}
	if last := max(r.lastReceived.Load(), r.lastAcked.Load()); last > 0 && time.Now().Unix()-last > 90 {
		return fmt.Errorf("replication progress is stale")
	}
	if !r.active.Load() {
		return fmt.Errorf("replication session is not active")
	}
	return nil
}
func (r *PGReader) Progress() (uint64, uint64, int64, int64) {
	return r.receivedLSN.Load(), r.ackedLSN.Load(), r.lastReceived.Load(), r.lastAcked.Load()
}

// These waits run on the connection owner, so feedback never races ReceiveMessage.
func (r *PGReader) acquireRaw(ctx context.Context, n int64, interval time.Duration, deadline *time.Time) (func(), error) {
	for {
		waitCtx, cancel := context.WithDeadline(ctx, *deadline)
		release, err := r.rawBudget.Acquire(waitCtx, n)
		cancel()
		if err == nil {
			return release, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			return nil, fatalReplicationError{err}
		}
		if err := r.sendStandbyStatus(ctx, true); err != nil {
			return nil, fmt.Errorf("feedback while waiting for raw budget: %w", err)
		}
		*deadline = time.Now().Add(interval)
	}
}
func (r *PGReader) deliverRaw(ctx context.Context, out chan<- *parser.RawMessage, raw *parser.RawMessage, interval time.Duration, deadline *time.Time) error {
	timer := time.NewTimer(time.Until(*deadline))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- raw:
			return nil
		case <-timer.C:
			if err := r.sendStandbyStatus(ctx, true); err != nil {
				return fmt.Errorf("feedback while waiting for raw channel: %w", err)
			}
			*deadline = time.Now().Add(interval)
			timer.Reset(time.Until(*deadline))
		}
	}
}
