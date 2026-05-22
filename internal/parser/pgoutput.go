package parser

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
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
	rawMsgs    [][]byte
	spill      *txSpill
}

type txSpill struct {
	file *os.File
	path string
}

func newTxSpill() (*txSpill, error) {
	file, err := os.CreateTemp("", "better-cdc-pgoutput-*")
	if err != nil {
		return nil, fmt.Errorf("create tx spill file: %w", err)
	}
	return &txSpill{
		file: file,
		path: file.Name(),
	}, nil
}

func (s *txSpill) Write(raw []byte) error {
	if s == nil {
		return nil
	}
	var size [8]byte
	binary.LittleEndian.PutUint64(size[:], uint64(len(raw)))
	if _, err := s.file.Write(size[:]); err != nil {
		return fmt.Errorf("write tx spill size: %w", err)
	}
	if _, err := s.file.Write(raw); err != nil {
		return fmt.Errorf("write tx spill payload: %w", err)
	}
	return nil
}

func (s *txSpill) Replay(fn func([]byte) error) error {
	if s == nil {
		return nil
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek tx spill: %w", err)
	}
	var size [8]byte
	for {
		_, err := io.ReadFull(s.file, size[:])
		if err != nil {
			if err == io.EOF {
				return nil
			}
			if err == io.ErrUnexpectedEOF {
				return fmt.Errorf("read tx spill size: %w", err)
			}
			return fmt.Errorf("read tx spill size: %w", err)
		}
		raw := make([]byte, binary.LittleEndian.Uint64(size[:]))
		if _, err := io.ReadFull(s.file, raw); err != nil {
			return fmt.Errorf("read tx spill payload: %w", err)
		}
		if err := fn(raw); err != nil {
			return err
		}
	}
}

func (s *txSpill) CloseAndRemove() error {
	if s == nil {
		return nil
	}
	closeErr := s.file.Close()
	removeErr := os.Remove(s.path)
	if closeErr != nil {
		return fmt.Errorf("close tx spill: %w", closeErr)
	}
	if removeErr != nil && !os.IsNotExist(removeErr) {
		return fmt.Errorf("remove tx spill: %w", removeErr)
	}
	return nil
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

	mu       sync.Mutex
	fatalErr error
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
		defer p.cleanupTx()
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
					fatal := fmt.Errorf("parse pgoutput message failed: %w", err)
					p.setFatalError(fatal)
					p.logger.Error("parse pgoutput message failed", zap.Error(fatal))
					return
				}
				if err := p.handlePGOutputMessage(ctx, msg.Data, logical, out); err != nil {
					if ctx.Err() != nil {
						return
					}
					fatal := fmt.Errorf("pgoutput handle failed: %w", err)
					p.setFatalError(fatal)
					p.logger.Error("pgoutput handle failed", zap.Error(fatal))
					return
				}
			}
		}
	}()
	return out, nil
}

func (p *PGOutputParser) setFatalError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fatalErr == nil {
		p.fatalErr = err
	}
}

// Err returns the fatal parse error that caused the parser to stop, or nil.
func (p *PGOutputParser) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fatalErr
}

func (p *PGOutputParser) handlePGOutputMessage(ctx context.Context, rawData []byte, logical pglogrepl.Message, out chan<- *model.WALEvent) error {
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
		checkpointLSN := pglogrepl.LSN(m.TransactionEndLSN)
		if checkpointLSN == 0 {
			checkpointLSN = p.tx.commitLSN
		}
		checkpointPos := model.WALPosition{LSN: checkpointLSN.String()}
		p.tx.commitTime = m.CommitTime
		if !m.CommitTime.IsZero() {
			lag := time.Since(m.CommitTime).Milliseconds()
			p.lagGauge.Set(lag)
			p.promMetrics.ReplicationLag.Set(lag)
		}
		if p.tx.spill == nil {
			for i, evt := range p.tx.events {
				if evt == nil {
					continue
				}
				p.enrichEventForCommit(evt, checkpointPos)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- evt:
					p.tx.events[i] = nil
				}
			}
		} else {
			if err := p.emitSpilledEvents(ctx, checkpointPos, out); err != nil {
				return err
			}
			p.logger.Debug("pgoutput commit (spilled transaction)",
				zap.String("lsn", p.tx.commitLSN.String()),
				zap.Uint64("txid", uint64(p.tx.xid)))
		}
		// Reset buffer size gauge
		p.promMetrics.TxBufferSize.Set(0)

		commitEvt := &model.WALEvent{
			Commit:     true,
			Position:   checkpointPos,
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
		if err := p.finishTx(); err != nil {
			return err
		}
	case *pglogrepl.InsertMessage:
		if m.Tuple == nil {
			return nil
		}
		evt := p.buildEventFromTuple(m.RelationID, m.Tuple.Columns, model.OperationInsert)
		if evt != nil {
			if err := p.bufferOrSpillEvents(ctx, rawData, []*model.WALEvent{evt}); err != nil {
				return err
			}
		}
	case *pglogrepl.UpdateMessage:
		if m.NewTuple == nil {
			return nil
		}
		evt := p.buildEventFromTuple(m.RelationID, m.NewTuple.Columns, model.OperationUpdate)
		if evt != nil {
			if m.OldTuple != nil {
				p.populateTupleColumnMap(evt.OldValues, p.lookupRelation(m.RelationID), m.OldTuple.Columns)
			}
			if err := p.bufferOrSpillEvents(ctx, rawData, []*model.WALEvent{evt}); err != nil {
				return err
			}
		}
	case *pglogrepl.DeleteMessage:
		if m.OldTuple == nil {
			return nil
		}
		evt := p.buildEventFromTuple(m.RelationID, m.OldTuple.Columns, model.OperationDelete)
		if evt != nil {
			if err := p.bufferOrSpillEvents(ctx, rawData, []*model.WALEvent{evt}); err != nil {
				return err
			}
		}
	case *pglogrepl.TruncateMessage:
		events := p.buildTruncateEvents(m.RelationIDs)
		if len(events) > 0 {
			if err := p.bufferOrSpillEvents(ctx, rawData, events); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *PGOutputParser) cleanupTx() {
	if p.tx == nil {
		return
	}
	for _, evt := range p.tx.events {
		if evt != nil {
			model.ReleaseWALEvent(evt)
		}
	}
	p.tx.events = nil
	p.tx.rawMsgs = nil
	if p.tx.spill != nil {
		if err := p.tx.spill.CloseAndRemove(); err != nil {
			p.logger.Warn("cleanup tx spill failed", zap.Error(err))
		}
		p.tx.spill = nil
	}
	p.promMetrics.TxBufferSize.Set(0)
	p.tx = nil
}

func (p *PGOutputParser) finishTx() error {
	if p.tx == nil {
		return nil
	}
	p.tx.events = nil
	p.tx.rawMsgs = nil
	if p.tx.spill != nil {
		if err := p.tx.spill.CloseAndRemove(); err != nil {
			return err
		}
		p.tx.spill = nil
	}
	p.promMetrics.TxBufferSize.Set(0)
	p.tx = nil
	return nil
}

func (p *PGOutputParser) enrichEventForCommit(evt *model.WALEvent, checkpointPos model.WALPosition) {
	evt.CommitTime = p.tx.commitTime
	evt.Position = checkpointPos
	evt.LSN = p.tx.commitLSN.String()
	evt.TxID = uint64(p.tx.xid)
	evt.Timestamp = p.tx.commitTime
	evt.TransactionID = fmt.Sprintf("%d", p.tx.xid)
}

func (p *PGOutputParser) emitSpilledEvents(ctx context.Context, checkpointPos model.WALPosition, out chan<- *model.WALEvent) error {
	return p.tx.spill.Replay(func(raw []byte) error {
		logical, err := pglogrepl.Parse(raw)
		if err != nil {
			return fmt.Errorf("parse spilled pgoutput message failed: %w", err)
		}
		events, err := p.buildEventsForReplay(logical)
		if err != nil {
			return err
		}
		for _, evt := range events {
			p.enrichEventForCommit(evt, checkpointPos)
			select {
			case <-ctx.Done():
				model.ReleaseWALEvent(evt)
				return ctx.Err()
			case out <- evt:
			}
		}
		return nil
	})
}

func (p *PGOutputParser) buildEventsForReplay(logical pglogrepl.Message) ([]*model.WALEvent, error) {
	switch m := logical.(type) {
	case *pglogrepl.InsertMessage:
		if m.Tuple == nil {
			return nil, nil
		}
		evt := p.buildEventFromTuple(m.RelationID, m.Tuple.Columns, model.OperationInsert)
		if evt == nil {
			return nil, nil
		}
		return []*model.WALEvent{evt}, nil
	case *pglogrepl.UpdateMessage:
		if m.NewTuple == nil {
			return nil, nil
		}
		evt := p.buildEventFromTuple(m.RelationID, m.NewTuple.Columns, model.OperationUpdate)
		if evt == nil {
			return nil, nil
		}
		if m.OldTuple != nil {
			p.populateTupleColumnMap(evt.OldValues, p.lookupRelation(m.RelationID), m.OldTuple.Columns)
		}
		return []*model.WALEvent{evt}, nil
	case *pglogrepl.DeleteMessage:
		if m.OldTuple == nil {
			return nil, nil
		}
		evt := p.buildEventFromTuple(m.RelationID, m.OldTuple.Columns, model.OperationDelete)
		if evt == nil {
			return nil, nil
		}
		return []*model.WALEvent{evt}, nil
	case *pglogrepl.TruncateMessage:
		return p.buildTruncateEvents(m.RelationIDs), nil
	default:
		return nil, fmt.Errorf("unexpected spilled pgoutput message type %T", logical)
	}
}

// bufferOrSpillEvents either buffers transaction events in memory or spills them for replay on commit.
func (p *PGOutputParser) bufferOrSpillEvents(ctx context.Context, rawData []byte, events []*model.WALEvent) error {
	if p.tx == nil {
		p.tx = &txBuffer{}
	}
	if len(events) == 0 {
		return nil
	}

	if p.tx.spill != nil {
		if err := p.tx.spill.Write(rawData); err != nil {
			return err
		}
		for _, evt := range events {
			model.ReleaseWALEvent(evt)
		}
		return nil
	}

	if p.maxTxBufferSize > 0 && len(p.tx.events)+len(events) > p.maxTxBufferSize {
		spill, err := newTxSpill()
		if err != nil {
			return err
		}
		p.tx.spill = spill
		p.promMetrics.TxBufferOverflows.Inc()
		p.logger.Warn("transaction buffer limit exceeded, spilling transaction until commit",
			zap.Uint32("xid", p.tx.xid),
			zap.Int("buffered_events", len(p.tx.events)),
			zap.Int("limit", p.maxTxBufferSize))

		for _, raw := range p.tx.rawMsgs {
			if err := p.tx.spill.Write(raw); err != nil {
				return err
			}
		}
		for _, bufferedEvt := range p.tx.events {
			model.ReleaseWALEvent(bufferedEvt)
		}
		p.tx.events = nil
		p.tx.rawMsgs = nil
		p.promMetrics.TxBufferSize.Set(0)

		if err := p.tx.spill.Write(rawData); err != nil {
			return err
		}
		for _, evt := range events {
			model.ReleaseWALEvent(evt)
		}
		return nil
	}

	select {
	case <-ctx.Done():
		for _, evt := range events {
			model.ReleaseWALEvent(evt)
		}
		return ctx.Err()
	default:
	}

	p.tx.events = append(p.tx.events, events...)
	p.tx.rawMsgs = append(p.tx.rawMsgs, append([]byte(nil), rawData...))
	p.promMetrics.TxBufferSize.Set(int64(len(p.tx.events)))
	return nil
}

func (p *PGOutputParser) lookupRelation(relID uint32) relationInfo {
	if rel, ok := p.relations[relID]; ok {
		return rel
	}
	return relationInfo{}
}

func (p *PGOutputParser) buildTruncateEvents(relIDs []uint32) []*model.WALEvent {
	events := make([]*model.WALEvent, 0, len(relIDs))
	for _, relID := range relIDs {
		rel := p.lookupRelation(relID)
		evt := p.buildRelationEvent(rel, model.OperationDDL)
		if evt != nil {
			events = append(events, evt)
		}
	}
	return events
}

func (p *PGOutputParser) buildEventFromTuple(relID uint32, cols []*pglogrepl.TupleDataColumn, op model.OperationType) *model.WALEvent {
	rel := p.lookupRelation(relID)
	evt := p.buildRelationEvent(rel, op)
	if evt == nil {
		return nil
	}

	switch op {
	case model.OperationInsert, model.OperationUpdate:
		p.populateTupleColumnMap(evt.NewValues, rel, cols)
	case model.OperationDelete:
		p.populateTupleColumnMap(evt.OldValues, rel, cols)
	}
	return evt
}

func (p *PGOutputParser) buildRelationEvent(rel relationInfo, op model.OperationType) *model.WALEvent {
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

	evt := model.AcquireWALEvent()
	evt.Schema = rel.Schema
	evt.Table = rel.Table
	evt.Operation = op
	evt.TxID = uint64(p.tx.xid)
	evt.LSN = p.tx.beginLSN.String()
	evt.TransactionID = fmt.Sprintf("%d", p.tx.xid)
	return evt
}

// populateTupleColumnMap populates the given map with tuple column data.
// Returns the map for convenience, or nil if relation has no columns.
func (p *PGOutputParser) populateTupleColumnMap(out map[string]interface{}, rel relationInfo, cols []*pglogrepl.TupleDataColumn) map[string]interface{} {
	if len(rel.Columns) == 0 {
		return nil
	}
	length := len(rel.Columns)
	if len(cols) < length {
		length = len(cols)
	}
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
