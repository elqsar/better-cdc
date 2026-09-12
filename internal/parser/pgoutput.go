package parser

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"

	"better-cdc/internal/budget"
	"better-cdc/internal/metrics"
	"better-cdc/internal/model"
)

type relationInfo struct {
	ID          uint32
	Schema      string
	Table       string
	Columns     []string
	ColumnTypes []uint32
	KeyColumns  []bool
}

type txBuffer struct {
	xid         uint32
	beginLSN    pglogrepl.LSN
	commitLSN   pglogrepl.LSN
	commitTime  time.Time
	memoryBytes int64
	events      []*model.WALEvent
	rawMsgs     [][]byte
	spill       *txSpill
}

type txSpill struct {
	file           *os.File
	path           string
	written, limit int64
}

func newTxSpill(dir string, limit int64) (*txSpill, error) {
	file, err := os.CreateTemp(dir, "better-cdc-pgoutput-*")
	if err != nil {
		return nil, fmt.Errorf("create tx spill file: %w", err)
	}
	return &txSpill{
		file:  file,
		path:  file.Name(),
		limit: limit,
	}, nil
}

func (s *txSpill) Write(raw []byte) error {
	if s == nil {
		return nil
	}
	if int64(len(raw))+8 > s.limit-s.written {
		return fmt.Errorf("transaction spill byte limit exceeded")
	}
	s.written += int64(len(raw)) + 8
	metrics.Pilot.SpillBytes.Set(s.written)
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
		n := binary.LittleEndian.Uint64(size[:])
		if n > uint64(s.limit) {
			return fmt.Errorf("invalid spill record length %d", n)
		}
		raw := make([]byte, int(n))
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
	MaxBufferBytes  int64
	MaxTxBytes      int64
	MaxSpillBytes   int64
	SpillDir        string
	TableFilter     map[string]struct{} // schema.table allowlist; empty means all
	Logger          *zap.Logger
	BufferSize      int // Output channel buffer size for throughput optimization
	MaxTxBufferSize int // Max events to buffer per transaction (0 = unlimited)
}

// PGOutputParser decodes pgoutput plugin messages into WALEvents.
type PGOutputParser struct {
	maxBufferBytes, maxTxBytes, maxSpillBytes int64
	spillDir                                  string
	outputBudget                              *budget.Budget
	tableFilter                               map[string]struct{}
	typeMap                                   *pgtype.Map
	relations                                 map[uint32]relationInfo
	tx                                        *txBuffer
	logger                                    *zap.Logger
	lagGauge                                  *metrics.Gauge
	errs                                      *metrics.Counter
	bufferSize                                int
	maxTxBufferSize                           int
	promMetrics                               *metrics.Metrics

	mu       sync.Mutex
	fatalErr error
}

func NewPGOutputParser(cfg PGOutputConfig) *PGOutputParser {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.MaxBufferBytes <= 0 {
		cfg.MaxBufferBytes = 64 << 20
	}
	if cfg.MaxTxBytes <= 0 {
		cfg.MaxTxBytes = 64 << 20
	}
	if cfg.MaxSpillBytes <= 0 {
		cfg.MaxSpillBytes = 1 << 30
	}
	return &PGOutputParser{
		maxBufferBytes: cfg.MaxBufferBytes, maxTxBytes: cfg.MaxTxBytes, maxSpillBytes: cfg.MaxSpillBytes, spillDir: cfg.SpillDir,
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
	p.outputBudget = budget.New(p.maxBufferBytes)
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
				if msg.Reset {
					p.cleanupTx()
					p.relations = make(map[uint32]relationInfo)
					continue
				}
				if msg.Plugin != PluginPGOutput && msg.Plugin != "" {
					continue
				}
				logical, err := parseLogical(msg.Data)
				if msg.ReleaseBytes != nil {
					msg.ReleaseBytes()
				}
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
		keys := make([]bool, 0, len(m.Columns))
		for _, c := range m.Columns {
			cols = append(cols, c.Name)
			types = append(types, c.DataType)
			keys = append(keys, c.Flags&1 != 0)
		}
		p.relations[m.RelationID] = relationInfo{
			ID:          m.RelationID,
			Schema:      m.Namespace,
			Table:       m.RelationName,
			Columns:     cols,
			ColumnTypes: types,
			KeyColumns:  keys,
		}
		p.logger.Debug("pgoutput relation", zap.Uint32("rel_id", m.RelationID), zap.String("schema", m.Namespace), zap.String("table", m.RelationName))
	case *pglogrepl.BeginMessage:
		if p.tx != nil {
			return fmt.Errorf("begin before previous transaction completed")
		}
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
			return fmt.Errorf("commit without begin")
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
			var seq uint32
			for i, evt := range p.tx.events {
				if evt == nil {
					continue
				}
				p.enrichEventForCommit(evt, checkpointPos, seq)
				if err := p.reserveEvent(ctx, evt); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- evt:
					p.tx.events[i] = nil
					seq++
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
		metrics.Pilot.SpillBytes.Set(0)
		metrics.Pilot.TxBytes.Set(0)

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
	case *pglogrepl.InsertMessage, *pglogrepl.UpdateMessage, *pglogrepl.DeleteMessage, *pglogrepl.TruncateMessage:
		if p.tx == nil {
			return fmt.Errorf("row change outside transaction")
		}
		events, err := p.buildEventsForReplay(logical)
		if err != nil {
			return err
		}
		p.attachRecovery(events, rawData, logical)
		return p.bufferOrSpillEvents(ctx, rawData, events)
	case *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
		// Type OIDs not registered in pgx retain their PostgreSQL text representation.
		// Origin carries provenance; it does not contain a row change.
	default:
		return fmt.Errorf("unsupported pgoutput message %T", logical)
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
	metrics.Pilot.SpillBytes.Set(0)
	metrics.Pilot.TxBytes.Set(0)
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
	metrics.Pilot.SpillBytes.Set(0)
	metrics.Pilot.TxBytes.Set(0)
	p.tx = nil
	return nil
}

func (p *PGOutputParser) enrichEventForCommit(evt *model.WALEvent, checkpointPos model.WALPosition, seq uint32) {
	evt.CommitTime = p.tx.commitTime
	evt.Position = checkpointPos
	evt.LSN = p.tx.commitLSN.String()
	evt.TxID = uint64(p.tx.xid)
	evt.Timestamp = p.tx.commitTime
	evt.TransactionID = fmt.Sprintf("%d", p.tx.xid)
	// seq is the event's deterministic WAL-order position within the
	// transaction. All events in a tx share the same commit LSN and xid, so
	// this ordinal is what keeps their EventIDs unique (and stable on replay).
	evt.SeqInTx = seq
	if evt.Recovery != nil {
		evt.Recovery.SeqInTx = seq
		evt.Recovery.LSN = evt.LSN
		evt.Recovery.Position = checkpointPos
		evt.Recovery.TxID = evt.TxID
		evt.Recovery.CommitTime = evt.CommitTime
	}
}

func (p *PGOutputParser) emitSpilledEvents(ctx context.Context, checkpointPos model.WALPosition, out chan<- *model.WALEvent) error {
	// seq lives outside the Replay closure so it keeps counting across messages,
	// yielding the same WAL-order ordinals as the in-memory commit path.
	var seq uint32
	return p.tx.spill.Replay(func(raw []byte) error {
		var capsule model.RecoveryChange
		if err := decodeExactJSON(raw, &capsule); err != nil {
			return err
		}
		evt, err := RestoreChange(&capsule)
		if err != nil {
			return err
		}
		p.enrichEventForCommit(evt, checkpointPos, seq)
		if err := p.reserveEvent(ctx, evt); err != nil {
			model.ReleaseWALEvent(evt)
			return err
		}
		select {
		case <-ctx.Done():
			model.ReleaseWALEvent(evt)
			return ctx.Err()
		case out <- evt:
			seq++
		}

		return nil
	})
}

func (p *PGOutputParser) buildEventsForReplay(logical pglogrepl.Message) ([]*model.WALEvent, error) {
	var evt *model.WALEvent
	var err error
	switch m := logical.(type) {
	case *pglogrepl.InsertMessage:
		if m.Tuple == nil {
			return nil, fmt.Errorf("insert missing tuple")
		}
		evt, err = p.buildEventFromTuple(m.RelationID, m.Tuple.Columns, model.OperationInsert)
	case *pglogrepl.UpdateMessage:
		if m.NewTuple == nil {
			return nil, fmt.Errorf("update missing tuple")
		}
		evt, err = p.buildEventFromTuple(m.RelationID, m.NewTuple.Columns, model.OperationUpdate)
		if err == nil && evt != nil && m.OldTuple != nil {
			evt.UnavailableBefore = unavailableColumns(p.lookupRelation(m.RelationID), m.OldTuple.Columns)
			_, err = p.populateTupleColumnMap(evt.OldValues, p.lookupRelation(m.RelationID), m.OldTuple.Columns)
			if m.OldTupleType == 'K' {
				p.markKeyOnlyBefore(evt, p.lookupRelation(m.RelationID))
			}
		}
	case *pglogrepl.DeleteMessage:
		if m.OldTuple == nil {
			return nil, fmt.Errorf("delete missing tuple")
		}
		evt, err = p.buildEventFromTuple(m.RelationID, m.OldTuple.Columns, model.OperationDelete)
		if evt != nil && err == nil && m.OldTupleType == 'K' {
			p.markKeyOnlyBefore(evt, p.lookupRelation(m.RelationID))
		}
	case *pglogrepl.TruncateMessage:
		var events []*model.WALEvent
		for _, id := range m.RelationIDs {
			e, err := p.buildRelationEvent(p.lookupRelation(id), model.OperationDDL)
			if err != nil {
				for _, prior := range events {
					model.ReleaseWALEvent(prior)
				}
				return nil, err
			}
			if e != nil {
				events = append(events, e)
			}
		}
		return events, nil
	default:
		return nil, fmt.Errorf("unexpected row message %T", logical)
	}
	if err != nil {
		model.ReleaseWALEvent(evt)
		return nil, err
	}
	if evt == nil {
		return nil, nil
	}
	return []*model.WALEvent{evt}, nil
}

// bufferOrSpillEvents either buffers transaction events in memory or spills them for replay on commit.
func (p *PGOutputParser) bufferOrSpillEvents(ctx context.Context, _ []byte, events []*model.WALEvent) error {
	if len(events) == 0 {
		return nil
	}
	if p.tx == nil {
		return fmt.Errorf("buffer outside transaction")
	}
	retained := false
	defer func() {
		if !retained {
			for _, evt := range events {
				model.ReleaseWALEvent(evt)
			}
		}
	}()
	var records [][]byte
	var bytes int64
	for _, evt := range events {
		raw, err := json.Marshal(evt.Recovery)
		if err != nil {
			return err
		}
		records = append(records, raw)
		bytes += int64(len(raw))*4 + 512
	}
	if p.tx.spill == nil && ((p.maxTxBufferSize > 0 && len(p.tx.events)+len(events) > p.maxTxBufferSize) || p.tx.memoryBytes+bytes > p.maxTxBytes) {
		spill, err := newTxSpill(p.spillDir, p.maxSpillBytes)
		if err != nil {
			return err
		}
		p.tx.spill = spill
		p.promMetrics.TxBufferOverflows.Inc()
		for _, raw := range p.tx.rawMsgs {
			if err := spill.Write(raw); err != nil {
				return err
			}
		}
		for _, evt := range p.tx.events {
			model.ReleaseWALEvent(evt)
		}
		p.tx.events = nil
		p.tx.rawMsgs = nil
		p.tx.memoryBytes = 0
	}
	if p.tx.spill != nil {
		for _, raw := range records {
			if err := p.tx.spill.Write(raw); err != nil {
				return err
			}
		}
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.tx.events = append(p.tx.events, events...)
	p.tx.rawMsgs = append(p.tx.rawMsgs, records...)
	p.tx.memoryBytes += bytes
	metrics.Pilot.TxBytes.Set(p.tx.memoryBytes)
	retained = true
	p.promMetrics.TxBufferSize.Set(int64(len(p.tx.events)))
	return nil
}

func (p *PGOutputParser) lookupRelation(relID uint32) relationInfo {
	if rel, ok := p.relations[relID]; ok {
		return rel
	}
	return relationInfo{}
}

func (p *PGOutputParser) buildEventFromTuple(relID uint32, cols []*pglogrepl.TupleDataColumn, op model.OperationType) (*model.WALEvent, error) {
	rel := p.lookupRelation(relID)
	evt, err := p.buildRelationEvent(rel, op)
	if evt == nil || err != nil {
		return evt, err
	}

	switch op {
	case model.OperationInsert, model.OperationUpdate:
		evt.UnavailableAfter = unavailableColumns(rel, cols)
		_, err = p.populateTupleColumnMap(evt.NewValues, rel, cols)
	case model.OperationDelete:
		evt.UnavailableBefore = unavailableColumns(rel, cols)
		_, err = p.populateTupleColumnMap(evt.OldValues, rel, cols)
	}
	return evt, err
}

func (p *PGOutputParser) buildRelationEvent(rel relationInfo, op model.OperationType) (*model.WALEvent, error) {
	if rel.ID == 0 {
		return nil, fmt.Errorf("unknown relation")
	}
	if p.tx == nil {
		return nil, fmt.Errorf("row outside transaction")
	}
	tableKey := rel.Schema + "." + rel.Table
	if len(p.tableFilter) > 0 {
		if _, ok := p.tableFilter[tableKey]; !ok {
			return nil, nil
		}
	}

	evt := model.AcquireWALEvent()
	evt.Schema = rel.Schema
	evt.Table = rel.Table
	evt.Operation = op
	evt.TxID = uint64(p.tx.xid)
	evt.LSN = p.tx.beginLSN.String()
	evt.TransactionID = fmt.Sprintf("%d", p.tx.xid)
	return evt, nil
}

// populateTupleColumnMap populates the given map with tuple column data.
// Returns the map for convenience, or nil if relation has no columns.
func (p *PGOutputParser) populateTupleColumnMap(out map[string]interface{}, rel relationInfo, cols []*pglogrepl.TupleDataColumn) (map[string]interface{}, error) {
	if len(cols) != len(rel.Columns) {
		return nil, fmt.Errorf("tuple column count %d differs from relation %d", len(cols), len(rel.Columns))
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
			v, err := p.decodeColumn(oid, col.Data)
			if err != nil {
				return nil, err
			}
			out[rel.Columns[i]] = v
		case 'u': // unchanged toast
			// skip unchanged column
		default:
			return nil, fmt.Errorf("unsupported tuple format %q", col.DataType)
		}
	}
	return out, nil
}

func (p *PGOutputParser) decodeColumn(oid uint32, data []byte) (interface{}, error) {
	if oid == pgtype.JSONOID || oid == pgtype.JSONBOID {
		var value any
		err := decodeExactJSON(data, &value)
		return value, err
	}
	if p.typeMap != nil {
		if dt, ok := p.typeMap.TypeForOID(oid); ok {
			value, err := dt.Codec.DecodeValue(p.typeMap, oid, pgtype.TextFormatCode, data)
			if err != nil {
				return nil, fmt.Errorf("decode column oid %d: %w", oid, err)
			}
			return value, nil
		}
	}
	// Unknown PostgreSQL types have an explicit text fallback, never a failed known codec.
	return string(data), nil
}

func unavailableColumns(rel relationInfo, cols []*pglogrepl.TupleDataColumn) []string {
	var names []string
	for i, c := range cols {
		if i < len(rel.Columns) && c.DataType == 'u' {
			names = append(names, rel.Columns[i])
		}
	}
	return names
}

func (p *PGOutputParser) reserveEvent(ctx context.Context, evt *model.WALEvent) error {
	if p.outputBudget == nil {
		return nil
	}
	raw, err := json.Marshal(evt.Recovery)
	if err != nil {
		return err
	}
	release, err := p.outputBudget.Acquire(ctx, int64(len(raw))*4+512)
	if err == nil {
		evt.ReleaseBytes = release
	}
	return err
}

func (p *PGOutputParser) attachRecovery(events []*model.WALEvent, raw []byte, logical pglogrepl.Message) {
	if len(raw) == 0 {
		return
	}
	var ids []uint32
	switch m := logical.(type) {
	case *pglogrepl.InsertMessage:
		ids = []uint32{m.RelationID}
	case *pglogrepl.UpdateMessage:
		ids = []uint32{m.RelationID}
	case *pglogrepl.DeleteMessage:
		ids = []uint32{m.RelationID}
	case *pglogrepl.TruncateMessage:
		ids = m.RelationIDs
	}
	rels := make(map[uint32]relationInfo, len(ids))
	for _, id := range ids {
		rels[id] = p.relations[id]
	}
	metadata, _ := json.Marshal(rels)
	for _, evt := range events {
		index := 0
		for i, id := range ids {
			rel := rels[id]
			if rel.Schema == evt.Schema && rel.Table == evt.Table {
				index = i
				break
			}
		}
		evt.Recovery = &model.RecoveryChange{Version: 1, Plugin: string(PluginPGOutput), Data: append([]byte(nil), raw...), Relations: metadata, Index: index, LSN: evt.LSN, TxID: evt.TxID}
	}
}

// RestoreChange reconstructs one change independently of current database schema.
func RestoreChange(c *model.RecoveryChange) (*model.WALEvent, error) {
	if c == nil || c.Version != 1 {
		return nil, fmt.Errorf("unsupported recovery capsule")
	}
	var events []*model.WALEvent
	var err error
	switch Plugin(c.Plugin) {
	case PluginWal2JSON:
		events, err = decodeWal2JSON(c.WALStart, c.Data, nil)
	case PluginPGOutput:
		p := NewPGOutputParser(PGOutputConfig{})
		if err = json.Unmarshal(c.Relations, &p.relations); err != nil {
			return nil, err
		}
		p.tx = &txBuffer{xid: uint32(c.TxID)}
		var m pglogrepl.Message
		m, err = parseLogical(c.Data)
		if err == nil {
			events, err = p.buildEventsForReplay(m)
		}
	default:
		return nil, fmt.Errorf("unsupported recovery plugin %q", c.Plugin)
	}
	if err != nil {
		return nil, err
	}
	if c.Index < 0 || c.Index >= len(events) {
		for _, e := range events {
			model.ReleaseWALEvent(e)
		}
		return nil, fmt.Errorf("invalid recovery event index")
	}
	evt := events[c.Index]
	for i, e := range events {
		if i != c.Index {
			model.ReleaseWALEvent(e)
		}
	}
	evt.Recovery = c
	evt.LSN = c.LSN
	evt.Position = c.Position
	evt.TxID = c.TxID
	evt.TransactionID = fmt.Sprint(c.TxID)
	evt.CommitTime = c.CommitTime
	evt.Timestamp = c.CommitTime
	evt.SeqInTx = c.SeqInTx
	return evt, nil
}

func parseLogical(data []byte) (msg pglogrepl.Message, err error) {
	defer func() {
		if v := recover(); v != nil {
			msg = nil
			err = fmt.Errorf("malformed pgoutput message: %v", v)
		}
	}()
	if len(data) == 0 {
		return nil, fmt.Errorf("empty pgoutput message")
	}
	return pglogrepl.Parse(data)
}

// Non-key placeholders in a K tuple are unavailable values, not SQL NULLs.
func (p *PGOutputParser) markKeyOnlyBefore(evt *model.WALEvent, rel relationInfo) {
	for i, name := range rel.Columns {
		if i < len(rel.KeyColumns) && !rel.KeyColumns[i] {
			delete(evt.OldValues, name)
			found := false
			for _, prior := range evt.UnavailableBefore {
				if prior == name {
					found = true
					break
				}
			}
			if !found {
				evt.UnavailableBefore = append(evt.UnavailableBefore, name)
			}
		}
	}
}
