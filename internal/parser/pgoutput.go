package parser

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"

	"better-cdc/internal/metrics"
	"better-cdc/internal/model"
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
	streaming  bool // true when buffer limit exceeded, events emitted immediately
}

// PGOutputConfig configures parsing for pgoutput.
type PGOutputConfig struct {
	TableFilter     map[string]struct{} // schema.table allowlist; empty means all
	Logger          *zap.Logger
	BufferSize      int // Output channel buffer size for throughput optimization
	MaxTxBufferSize int // Max events to buffer per transaction (0 = unlimited)
}

// PGOutputParser decodes pgoutput plugin messages into WALEvents.
type PGOutputParser struct {
	tableFilter     map[string]struct{}
	typeMap         *pgtype.Map
	relations       map[uint32]relationInfo
	tx              *txBuffer
	logger          *zap.Logger
	lagGauge        *metrics.Gauge
	errs            *metrics.Counter
	bufferSize      int
	maxTxBufferSize int
	promMetrics     *metrics.Metrics
}

func NewPGOutputParser(cfg PGOutputConfig) *PGOutputParser {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &PGOutputParser{
		tableFilter:     cfg.TableFilter,
		typeMap:         pgtype.NewMap(),
		relations:       make(map[uint32]relationInfo),
		logger:          logger,
		lagGauge:        metrics.NewGauge("replication_lag_ms"),
		errs:            metrics.NewCounter("decode_errors"),
		bufferSize:      cfg.BufferSize,
		maxTxBufferSize: cfg.MaxTxBufferSize,
		promMetrics:     metrics.GlobalMetrics,
	}
}

func (p *PGOutputParser) Parse(ctx context.Context, stream <-chan *RawMessage) (<-chan *model.WALEvent, error) {
	out := make(chan *model.WALEvent, p.bufferSize)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-stream:
				if !ok {
					return
				}
				if msg == nil {
					continue
				}
				if msg.Plugin != PluginPGOutput && msg.Plugin != "" {
					continue
				}
				logical, err := pglogrepl.Parse(msg.Data)
				if err != nil {
					p.errs.Inc()
					p.promMetrics.DecodeErrors.Inc()
					p.logger.Warn("parse pgoutput message failed", zap.Error(err))
					continue
				}
				if err := p.handlePGOutputMessage(ctx, logical, out); err != nil {
					p.logger.Warn("pgoutput handle error", zap.Error(err))
				}
			}
		}
	}()
	return out, nil
}

func (p *PGOutputParser) handlePGOutputMessage(ctx context.Context, logical pglogrepl.Message, out chan<- *model.WALEvent) error {
	switch m := logical.(type) {
	case *pglogrepl.RelationMessage:
		cols := make([]string, 0, len(m.Columns))
		types := make([]uint32, 0, len(m.Columns))
		for _, c := range m.Columns {
			cols = append(cols, c.Name)
			types = append(types, c.DataType)
		}
		p.relations[m.RelationID] = relationInfo{
			ID:          m.RelationID,
			Schema:      m.Namespace,
			Table:       m.RelationName,
			Columns:     cols,
			ColumnTypes: types,
		}
		p.logger.Debug("pgoutput relation", zap.Uint32("rel_id", m.RelationID), zap.String("schema", m.Namespace), zap.String("table", m.RelationName))
	case *pglogrepl.BeginMessage:
		p.tx = &txBuffer{
			xid:      m.Xid,
			beginLSN: pglogrepl.LSN(m.FinalLSN),
		}
		evt := &model.WALEvent{
			Begin: true,
			LSN:   pglogrepl.LSN(m.FinalLSN).String(),
			TxID:  uint64(m.Xid),
		}
		p.logger.Debug("pgoutput begin", zap.String("lsn", evt.LSN), zap.Uint64("txid", evt.TxID))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- evt:
		}
	case *pglogrepl.CommitMessage:
		if p.tx == nil {
			return nil
		}
		p.tx.commitLSN = pglogrepl.LSN(m.CommitLSN)
		p.tx.commitTime = m.CommitTime
		if !m.CommitTime.IsZero() {
			lag := time.Since(m.CommitTime).Milliseconds()
			p.lagGauge.Set(lag)
			p.promMetrics.ReplicationLag.Set(lag)
		}
		// If in streaming mode, events were already sent without commit metadata.
		// In normal mode, emit buffered events with full commit info.
		if !p.tx.streaming {
			for _, evt := range p.tx.events {
				evt.CommitTime = p.tx.commitTime
				evt.Position = model.WALPosition{LSN: p.tx.commitLSN.String()}
				evt.LSN = p.tx.commitLSN.String()
				evt.TxID = uint64(p.tx.xid)
				evt.Timestamp = p.tx.commitTime
				evt.TransactionID = fmt.Sprintf("%d", p.tx.xid)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- evt:
				}
			}
		} else {
			p.logger.Debug("pgoutput commit (streamed transaction)",
				zap.String("lsn", p.tx.commitLSN.String()),
				zap.Uint64("txid", uint64(p.tx.xid)))
		}
		// Reset buffer size gauge
		p.promMetrics.TxBufferSize.Set(0)

		commitEvt := &model.WALEvent{
			Commit:     true,
			LSN:        p.tx.commitLSN.String(),
			CommitTime: p.tx.commitTime,
			TxID:       uint64(p.tx.xid),
		}
		p.logger.Debug("pgoutput commit", zap.String("lsn", commitEvt.LSN), zap.Uint64("txid", commitEvt.TxID))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- commitEvt:
		}
		p.tx = nil
	case *pglogrepl.InsertMessage:
		if m.Tuple == nil {
			return nil
		}
		evt := p.buildEventFromTuple(m.RelationID, m.Tuple.Columns, model.OperationInsert)
		if evt != nil {
			if err := p.bufferOrStreamEvent(ctx, evt, out); err != nil {
				return err
			}
		}
	case *pglogrepl.UpdateMessage:
		if m.NewTuple == nil {
			return nil
		}
		evt := p.buildEventFromTuple(m.RelationID, m.NewTuple.Columns, model.OperationUpdate)
		if evt != nil {
			var oldCols []*pglogrepl.TupleDataColumn
			if m.OldTuple != nil {
				oldCols = m.OldTuple.Columns
			}
			evt.OldValues = p.tupleColumnMap(p.lookupRelation(m.RelationID), oldCols)
			if err := p.bufferOrStreamEvent(ctx, evt, out); err != nil {
				return err
			}
		}
	case *pglogrepl.DeleteMessage:
		if m.OldTuple == nil {
			return nil
		}
		evt := p.buildEventFromTuple(m.RelationID, m.OldTuple.Columns, model.OperationDelete)
		if evt != nil {
			if err := p.bufferOrStreamEvent(ctx, evt, out); err != nil {
				return err
			}
		}
	}
	return nil
}

// bufferOrStreamEvent either buffers the event or streams it immediately if limit exceeded.
// Returns error only if streaming and context is cancelled.
func (p *PGOutputParser) bufferOrStreamEvent(ctx context.Context, evt *model.WALEvent, out chan<- *model.WALEvent) error {
	if p.tx == nil {
		p.tx = &txBuffer{}
	}

	// Check if we're already in streaming mode
	if p.tx.streaming {
		// Stream event immediately
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- evt:
		}
		return nil
	}

	// Check if we should switch to streaming mode
	if p.maxTxBufferSize > 0 && len(p.tx.events) >= p.maxTxBufferSize {
		// Switch to streaming mode
		p.tx.streaming = true
		p.promMetrics.TxBufferOverflows.Inc()
		p.logger.Warn("transaction buffer limit exceeded, switching to streaming mode",
			zap.Uint32("xid", p.tx.xid),
			zap.Int("buffered_events", len(p.tx.events)),
			zap.Int("limit", p.maxTxBufferSize))

		// Flush all buffered events first
		for _, bufferedEvt := range p.tx.events {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- bufferedEvt:
			}
		}
		// Clear the buffer (events are now sent)
		p.tx.events = nil
		p.promMetrics.TxBufferSize.Set(0)

		// Now stream the current event
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- evt:
		}
		return nil
	}

	// Normal buffering
	p.tx.events = append(p.tx.events, evt)
	p.promMetrics.TxBufferSize.Set(int64(len(p.tx.events)))
	return nil
}

func (p *PGOutputParser) lookupRelation(relID uint32) relationInfo {
	if rel, ok := p.relations[relID]; ok {
		return rel
	}
	return relationInfo{}
}

func (p *PGOutputParser) buildEventFromTuple(relID uint32, cols []*pglogrepl.TupleDataColumn, op model.OperationType) *model.WALEvent {
	rel := p.lookupRelation(relID)
	if rel.ID == 0 {
		return nil
	}
	if p.tx == nil {
		p.tx = &txBuffer{}
	}
	tableKey := rel.Schema + "." + rel.Table
	if len(p.tableFilter) > 0 {
		if _, ok := p.tableFilter[tableKey]; !ok {
			return nil
		}
	}

	evt := &model.WALEvent{
		Schema:        rel.Schema,
		Table:         rel.Table,
		Operation:     op,
		TxID:          uint64(p.tx.xid),
		LSN:           p.tx.beginLSN.String(),
		TransactionID: fmt.Sprintf("%d", p.tx.xid),
	}
	switch op {
	case model.OperationInsert, model.OperationUpdate:
		evt.NewValues = p.tupleColumnMap(rel, cols)
	case model.OperationDelete:
		evt.OldValues = p.tupleColumnMap(rel, cols)
	}
	return evt
}

func (p *PGOutputParser) tupleColumnMap(rel relationInfo, cols []*pglogrepl.TupleDataColumn) map[string]interface{} {
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
			out[rel.Columns[i]] = p.decodeColumn(oid, col.Data)
		case 'u': // unchanged toast
			// skip unchanged column
		default:
			out[rel.Columns[i]] = string(col.Data)
		}
	}
	return out
}

func (p *PGOutputParser) decodeColumn(oid uint32, data []byte) interface{} {
	if len(data) == 0 {
		return nil
	}
	if p.typeMap == nil {
		return string(data)
	}
	if dt, ok := p.typeMap.TypeForOID(oid); ok {
		val, err := dt.Codec.DecodeValue(p.typeMap, oid, pgtype.TextFormatCode, data)
		if err != nil {
			p.errs.Inc()
			p.promMetrics.DecodeErrors.Inc()
			p.logger.Error("decode column error", zap.Error(err), zap.Uint32("oid", oid))
			return string(data)
		}
		return val
	}
	return string(data)
}
