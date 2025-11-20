package wal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"

	"better-cdc/internal/metrics"
	"better-cdc/internal/model"

	"go.uber.org/zap"
)

type relationInfo struct {
	ID          uint32
	Schema      string
	Table       string
	Columns     []string
	ColumnTypes []uint32
}

type txBuffer struct {
	xid        uint32
	beginLSN   pglogrepl.LSN
	commitLSN  pglogrepl.LSN
	commitTime time.Time
	events     []*model.WALEvent
}

type replicationStartFunc func(context.Context, pglogrepl.LSN) error
type replicationLoopFunc func(context.Context, pglogrepl.LSN, chan<- *model.WALEvent) (pglogrepl.LSN, error)

type fatalReplicationError struct {
	err error
}

var jitterRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func (e fatalReplicationError) Error() string {
	return e.err.Error()
}

func (e fatalReplicationError) Unwrap() error {
	return e.err
}

// Reader streams logical replication changes from PostgreSQL.
type Reader interface {
	Start(ctx context.Context) error
	ReadWAL(ctx context.Context, position model.WALPosition) (<-chan *model.WALEvent, error)
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
	slot      SlotConfig
	conn      *pgconn.PgConn
	typeMap   *pgtype.Map
	relations map[uint32]relationInfo
	tx        *txBuffer
	lagGauge  *metrics.Gauge
	errs      *metrics.Counter
	logger    *zap.Logger
}

func NewPGReader(slot SlotConfig, logger *zap.Logger) *PGReader {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &PGReader{
		slot:     slot,
		lagGauge: metrics.NewGauge("replication_lag_ms"),
		errs:     metrics.NewCounter("decode_errors"),
		logger:   logger,
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
	r.typeMap = pgtype.NewMap()
	r.relations = make(map[uint32]relationInfo)
	r.logger.Info("replication connection established", zap.String("slot", r.slot.SlotName), zap.String("plugin", r.slot.Plugin))
	return nil
}

func (r *PGReader) ReadWAL(ctx context.Context, position model.WALPosition) (<-chan *model.WALEvent, error) {
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

	r.logger.Info("starting replication", zap.String("plugin", pluginName), zap.String("lsn", startLSN.String()))

	out := make(chan *model.WALEvent)
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

func (r *PGReader) runReplicationLoop(ctx context.Context, startLSN pglogrepl.LSN, plugin string, startFn replicationStartFunc, loopFn replicationLoopFunc, out chan<- *model.WALEvent) {
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
		"\"pretty-print\" '0'",
		"\"write-in-chunks\" '1'",
		"\"format-version\" '2'",
	}

	if err := pglogrepl.StartReplication(ctx, r.conn, r.slot.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	return nil
}

func (r *PGReader) loopWal2JSON(ctx context.Context, startLSN pglogrepl.LSN, out chan<- *model.WALEvent) (pglogrepl.LSN, error) {
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
				events, err := decodeWal2JSON(uint64(xld.WALStart), xld.WALData)
				if err != nil {
					r.errs.Inc()
					r.logger.Warn("decode wal2json failed", zap.Error(err))
					continue
				}
				for _, evt := range events {
					if !evt.CommitTime.IsZero() {
						lag := time.Since(evt.CommitTime).Milliseconds()
						r.lagGauge.Set(lag)
					}
					if r.logger != nil {
						r.logger.Debug("wal2json event", zap.String("lsn", evt.LSN), zap.Uint64("txid", evt.TxID), zap.String("table", evt.Table), zap.String("op", string(evt.Operation)))
					}
					select {
					case <-ctx.Done():
						return lastLSN, ctx.Err()
					case out <- evt:
					}
				}
				standbyDeadline = time.Now().Add(standbyTimeout)
				if err := r.sendStandbyStatus(ctx, lastLSN, false); err != nil {
					r.logger.Warn("send standby status failed", zap.Error(err))
				}
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					r.logger.Warn("parse keepalive failed", zap.Error(err))
					continue
				}
				if pkm.ServerWALEnd > lastLSN {
					lastLSN = pkm.ServerWALEnd
				}
				standbyDeadline = time.Now().Add(standbyTimeout)
				if err := r.sendStandbyStatus(ctx, lastLSN, pkm.ReplyRequested); err != nil {
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

func (r *PGReader) loopPGOutput(ctx context.Context, startLSN pglogrepl.LSN, out chan<- *model.WALEvent) (pglogrepl.LSN, error) {
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
				if err := r.handlePGOutputMessage(ctx, xld.WALData, out); err != nil {
					r.logger.Warn("pgoutput handle error", zap.Error(err))
				}
				standbyDeadline = time.Now().Add(standbyTimeout)
				if err := r.sendStandbyStatus(ctx, lastLSN, false); err != nil {
					r.logger.Warn("send standby status failed", zap.Error(err))
				}
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					r.logger.Warn("parse keepalive failed", zap.Error(err))
					continue
				}
				if pkm.ServerWALEnd > lastLSN {
					lastLSN = pkm.ServerWALEnd
				}
				standbyDeadline = time.Now().Add(standbyTimeout)
				if err := r.sendStandbyStatus(ctx, lastLSN, pkm.ReplyRequested); err != nil {
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
		r.tx = nil
		return nil
	}
	err := r.conn.Close(ctx)
	if err != nil {
		r.logger.Warn("close replication connection failed", zap.Error(err))
	}
	r.conn = nil
	r.tx = nil
	r.relations = make(map[uint32]relationInfo)
	r.typeMap = nil
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
	extra := time.Duration(jitterRand.Int63n(int64(spread) + 1))
	return base + extra
}

// decodeWal2JSON converts wal2json payload into WALEvents (format-version 2).
func decodeWal2JSON(walStart uint64, data []byte) ([]*model.WALEvent, error) {
	var envelope wal2JSONEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("unmarshal wal2json: %w", err)
	}
	var events []*model.WALEvent
	beginEvt := &model.WALEvent{
		Begin:         true,
		Position:      model.WALPosition{LSN: pglogrepl.LSN(walStart).String()},
		LSN:           pglogrepl.LSN(walStart).String(),
		TransactionID: fmt.Sprintf("%d", envelope.XID),
		TxID:          uint64(envelope.XID),
	}
	events = append(events, beginEvt)

	var lastEvent *model.WALEvent
	for _, ch := range envelope.Change {
		ev := &model.WALEvent{
			Position:      model.WALPosition{LSN: pglogrepl.LSN(walStart).String()},
			Timestamp:     envelope.Timestamp,
			CommitTime:    envelope.Timestamp,
			TransactionID: fmt.Sprintf("%d", envelope.XID),
			LSN:           pglogrepl.LSN(walStart).String(),
			TxID:          uint64(envelope.XID),
			Schema:        ch.Schema,
			Table:         ch.Table,
		}
		switch ch.Kind {
		case "insert":
			ev.Operation = model.OperationInsert
			ev.NewValues = toMap(ch.ColumnNames, ch.ColumnValues)
		case "update":
			ev.Operation = model.OperationUpdate
			ev.OldValues = toMap(ch.OldKeys.KeyNames, ch.OldKeys.KeyValues)
			ev.NewValues = toMap(ch.ColumnNames, ch.ColumnValues)
		case "delete":
			ev.Operation = model.OperationDelete
			ev.OldValues = toMap(ch.OldKeys.KeyNames, ch.OldKeys.KeyValues)
		default:
			ev.Operation = model.OperationType(ch.Kind)
		}
		events = append(events, ev)
		lastEvent = ev
	}
	if lastEvent != nil {
		commitEvt := &model.WALEvent{
			Commit:        true,
			Position:      model.WALPosition{LSN: lastEvent.Position.LSN},
			LSN:           lastEvent.Position.LSN,
			CommitTime:    envelope.Timestamp,
			TransactionID: fmt.Sprintf("%d", envelope.XID),
			TxID:          uint64(envelope.XID),
		}
		events = append(events, commitEvt)
	}
	return events, nil
}

// wal2JSONEnvelope matches wal2json format version 2 output.
type wal2JSONEnvelope struct {
	XID       int64            `json:"xid"`
	Timestamp time.Time        `json:"timestamp"`
	Change    []wal2JSONChange `json:"change"`
}

type wal2JSONChange struct {
	Kind         string        `json:"kind"`
	Schema       string        `json:"schema"`
	Table        string        `json:"table"`
	ColumnNames  []string      `json:"columnnames"`
	ColumnValues []interface{} `json:"columnvalues"`
	OldKeys      wal2JSONKeys  `json:"oldkeys"`
}

type wal2JSONKeys struct {
	KeyNames  []string      `json:"keynames"`
	KeyValues []interface{} `json:"keyvalues"`
}

func toMap(keys []string, vals []interface{}) map[string]interface{} {
	if len(keys) == 0 || len(vals) == 0 || len(keys) != len(vals) {
		return nil
	}
	m := make(map[string]interface{}, len(keys))
	for i, k := range keys {
		m[k] = vals[i]
	}
	return m
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

func (r *PGReader) handlePGOutputMessage(ctx context.Context, walData []byte, out chan<- *model.WALEvent) error {
	logical, err := pglogrepl.Parse(walData)
	if err != nil {
		r.errs.Inc()
		return fmt.Errorf("parse pgoutput: %w", err)
	}
	switch m := logical.(type) {
	case *pglogrepl.RelationMessage:
		cols := make([]string, 0, len(m.Columns))
		types := make([]uint32, 0, len(m.Columns))
		for _, c := range m.Columns {
			cols = append(cols, c.Name)
			types = append(types, c.DataType)
		}
		r.relations[m.RelationID] = relationInfo{
			ID:          m.RelationID,
			Schema:      m.Namespace,
			Table:       m.RelationName,
			Columns:     cols,
			ColumnTypes: types,
		}
		r.logger.Debug("pgoutput relation", zap.Uint32("rel_id", m.RelationID), zap.String("schema", m.Namespace), zap.String("table", m.RelationName))
	case *pglogrepl.BeginMessage:
		r.tx = &txBuffer{
			xid:      m.Xid,
			beginLSN: pglogrepl.LSN(m.FinalLSN),
		}
		evt := &model.WALEvent{
			Begin: true,
			LSN:   pglogrepl.LSN(m.FinalLSN).String(),
			TxID:  uint64(m.Xid),
		}
		r.logger.Debug("pgoutput begin", zap.String("lsn", evt.LSN), zap.Uint64("txid", evt.TxID))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- evt:
		}
	case *pglogrepl.CommitMessage:
		if r.tx == nil {
			return nil
		}
		r.tx.commitLSN = pglogrepl.LSN(m.CommitLSN)
		r.tx.commitTime = m.CommitTime
		if !m.CommitTime.IsZero() {
			lag := time.Since(m.CommitTime).Milliseconds()
			r.lagGauge.Set(lag)
		}
		for _, evt := range r.tx.events {
			evt.CommitTime = r.tx.commitTime
			evt.Position = model.WALPosition{LSN: r.tx.commitLSN.String()}
			evt.LSN = r.tx.commitLSN.String()
			evt.TxID = uint64(r.tx.xid)
			evt.Timestamp = r.tx.commitTime
			evt.TransactionID = fmt.Sprintf("%d", r.tx.xid)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- evt:
			}
		}
		commitEvt := &model.WALEvent{
			Commit:     true,
			LSN:        r.tx.commitLSN.String(),
			CommitTime: r.tx.commitTime,
			TxID:       uint64(r.tx.xid),
		}
		r.logger.Debug("pgoutput commit", zap.String("lsn", commitEvt.LSN), zap.Uint64("txid", commitEvt.TxID))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- commitEvt:
		}
		r.tx = nil
	case *pglogrepl.InsertMessage:
		if m.Tuple == nil {
			return nil
		}
		evt := r.buildEventFromTuple(m.RelationID, m.Tuple.Columns, model.OperationInsert)
		if evt != nil {
			r.bufferEvent(evt)
		}
	case *pglogrepl.UpdateMessage:
		if m.NewTuple == nil {
			return nil
		}
		evt := r.buildEventFromTuple(m.RelationID, m.NewTuple.Columns, model.OperationUpdate)
		if evt != nil {
			var oldCols []*pglogrepl.TupleDataColumn
			if m.OldTuple != nil {
				oldCols = m.OldTuple.Columns
			}
			evt.OldValues = r.tupleColumnMap(r.lookupRelation(m.RelationID), oldCols)
			r.bufferEvent(evt)
		}
	case *pglogrepl.DeleteMessage:
		if m.OldTuple == nil {
			return nil
		}
		evt := r.buildEventFromTuple(m.RelationID, m.OldTuple.Columns, model.OperationDelete)
		if evt != nil {
			r.bufferEvent(evt)
		}
	}
	return nil
}

func (r *PGReader) bufferEvent(evt *model.WALEvent) {
	if r.tx == nil {
		r.tx = &txBuffer{}
	}
	r.tx.events = append(r.tx.events, evt)
}

func (r *PGReader) lookupRelation(relID uint32) relationInfo {
	if rel, ok := r.relations[relID]; ok {
		return rel
	}
	return relationInfo{}
}

func (r *PGReader) buildEventFromTuple(relID uint32, cols []*pglogrepl.TupleDataColumn, op model.OperationType) *model.WALEvent {
	rel := r.lookupRelation(relID)
	if rel.ID == 0 {
		return nil
	}
	if r.tx == nil {
		r.tx = &txBuffer{}
	}
	tableKey := rel.Schema + "." + rel.Table
	if len(r.slot.TableFilter) > 0 {
		if _, ok := r.slot.TableFilter[tableKey]; !ok {
			return nil
		}
	}

	evt := &model.WALEvent{
		Schema:        rel.Schema,
		Table:         rel.Table,
		Operation:     op,
		TxID:          uint64(r.tx.xid),
		LSN:           r.tx.beginLSN.String(),
		TransactionID: fmt.Sprintf("%d", r.tx.xid),
	}
	switch op {
	case model.OperationInsert, model.OperationUpdate:
		evt.NewValues = r.tupleColumnMap(rel, cols)
	case model.OperationDelete:
		evt.OldValues = r.tupleColumnMap(rel, cols)
	}
	return evt
}

func (r *PGReader) tupleColumnMap(rel relationInfo, cols []*pglogrepl.TupleDataColumn) map[string]interface{} {
	if len(rel.Columns) == 0 {
		return nil
	}
	length := len(rel.Columns)
	if len(cols) < length {
		length = len(cols)
	}
	out := make(map[string]interface{}, length)
	for i := 0; i < length; i++ {
		col := cols[i]
		var oid uint32
		if i < len(rel.ColumnTypes) {
			oid = rel.ColumnTypes[i]
		}
		switch col.DataType {
		case 'n': // null
			out[rel.Columns[i]] = nil
		case 't': // text (pgoutput uses text format)
			out[rel.Columns[i]] = r.decodeColumn(oid, col.Data)
		case 'u': // unchanged toast
			// skip unchanged column
		default:
			out[rel.Columns[i]] = string(col.Data)
		}
	}
	return out
}

func (r *PGReader) decodeColumn(oid uint32, data []byte) interface{} {
	if len(data) == 0 {
		return nil
	}
	if r.typeMap == nil {
		return string(data)
	}
	if dt, ok := r.typeMap.TypeForOID(oid); ok {
		val, err := dt.Codec.DecodeValue(r.typeMap, oid, pgtype.TextFormatCode, data)
		if err != nil {
			r.errs.Inc()
			r.logger.Error("decode column error:", zap.Error(err), zap.Uint32("oid", oid))
			return string(data)
		}
		return val
	}
	return string(data)
}

func joinPublications(pubs []string) string {
	return strings.Join(pubs, ",")
}
