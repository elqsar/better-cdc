package parser

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
	"go.uber.org/zap"

	"better-cdc/internal/metrics"
	"better-cdc/internal/model"
)

// Wal2JSONConfig configures wal2json parsing.
type Wal2JSONConfig struct {
	TableFilter map[string]struct{} // schema.table allowlist; empty means all
	Logger      *zap.Logger
	BufferSize  int // Output channel buffer size for throughput optimization
}

// Wal2JSONParser decodes wal2json plugin output into WALEvents.
type Wal2JSONParser struct {
	tableFilter map[string]struct{}
	logger      *zap.Logger
	bufferSize  int
	promMetrics *metrics.Metrics
}

func NewWal2JSONParser(cfg Wal2JSONConfig) *Wal2JSONParser {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Wal2JSONParser{
		tableFilter: cfg.TableFilter,
		logger:      logger,
		bufferSize:  cfg.BufferSize,
		promMetrics: metrics.GlobalMetrics,
	}
}

func (p *Wal2JSONParser) Parse(ctx context.Context, stream <-chan *RawMessage) (<-chan *model.WALEvent, error) {
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
				if msg.Plugin != PluginWal2JSON && msg.Plugin != "" {
					continue
				}
				events, err := decodeWal2JSON(uint64(msg.WALStart), msg.Data, p.tableFilter)
				if err != nil {
					p.promMetrics.DecodeErrors.Inc()
					p.logger.Warn("decode wal2json failed", zap.Error(err))
					continue
				}
				for _, evt := range events {
					if !evt.CommitTime.IsZero() {
						lag := time.Since(evt.CommitTime).Milliseconds()
						p.promMetrics.ReplicationLag.Set(lag)
					}
					if p.logger != nil {
						p.logger.Debug("wal2json event", zap.String("lsn", evt.LSN), zap.Uint64("txid", evt.TxID), zap.String("table", evt.Table), zap.String("op", string(evt.Operation)))
					}
					select {
					case <-ctx.Done():
						return
					case out <- evt:
					}
				}
			}
		}
	}()
	return out, nil
}

// decodeWal2JSON converts wal2json format-version 2 message into a WALEvent.
// Format v2 sends separate messages per action (B=Begin, C=Commit, I=Insert, U=Update, D=Delete).
func decodeWal2JSON(walStart uint64, data []byte, tableFilter map[string]struct{}) ([]*model.WALEvent, error) {
	var msg wal2JSONMessageV2
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal wal2json: %w", err)
	}

	position := model.WALPosition{LSN: pglogrepl.LSN(walStart).String()}
	txID := strconv.FormatInt(msg.XID, 10)

	evt := &model.WALEvent{
		Position:      position,
		LSN:           position.LSN,
		Timestamp:     msg.Timestamp.Time,
		CommitTime:    msg.Timestamp.Time,
		TransactionID: txID,
		TxID:          uint64(msg.XID),
		Schema:        msg.Schema,
		Table:         msg.Table,
	}

	switch msg.Action {
	case "B": // Begin
		evt.Begin = true
	case "C": // Commit
		evt.Commit = true
	case "I": // Insert
		if isTableFiltered(msg.Schema, msg.Table, tableFilter) {
			return nil, nil
		}
		evt.Operation = model.OperationInsert
		evt.NewValues = columnsToMap(msg.Columns)
	case "U": // Update
		if isTableFiltered(msg.Schema, msg.Table, tableFilter) {
			return nil, nil
		}
		evt.Operation = model.OperationUpdate
		evt.NewValues = columnsToMap(msg.Columns)
		evt.OldValues = columnsToMap(msg.Identity)
	case "D": // Delete
		if isTableFiltered(msg.Schema, msg.Table, tableFilter) {
			return nil, nil
		}
		evt.Operation = model.OperationDelete
		evt.OldValues = columnsToMap(msg.Identity)
	case "T": // Truncate
		if isTableFiltered(msg.Schema, msg.Table, tableFilter) {
			return nil, nil
		}
		evt.Operation = model.OperationDDL
	default:
		return nil, nil // Unknown action, skip
	}

	return []*model.WALEvent{evt}, nil
}

// isTableFiltered returns true if the table should be filtered out.
func isTableFiltered(schema, table string, filter map[string]struct{}) bool {
	if len(filter) == 0 {
		return false
	}
	_, ok := filter[schema+"."+table]
	return !ok
}

// pgTime handles PostgreSQL timestamp format (space separator instead of T).
type pgTime struct {
	time.Time
}

func (t *pgTime) UnmarshalJSON(data []byte) error {
	// Remove quotes
	s := string(data)
	if len(s) < 2 {
		return nil
	}
	s = s[1 : len(s)-1]
	if s == "" || s == "null" {
		return nil
	}

	// Try RFC3339 first, then PostgreSQL format
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999-07:00",
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999Z07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05-07",
		"2006-01-02 15:04:05Z07:00",
	}

	var err error
	for _, format := range formats {
		t.Time, err = time.Parse(format, s)
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("cannot parse timestamp %q", s)
}

// wal2JSONMessageV2 matches wal2json format-version 2 output.
// Each message is a separate JSON object with an action field.
type wal2JSONMessageV2 struct {
	Action    string           `json:"action"` // B=Begin, C=Commit, I=Insert, U=Update, D=Delete, T=Truncate
	XID       int64            `json:"xid"`
	Timestamp pgTime           `json:"timestamp"`
	Schema    string           `json:"schema,omitempty"`
	Table     string           `json:"table,omitempty"`
	Columns   []wal2JSONColumn `json:"columns,omitempty"`  // New values for I/U
	Identity  []wal2JSONColumn `json:"identity,omitempty"` // Old key values for U/D
}

// wal2JSONColumn represents a column in format-version 2.
type wal2JSONColumn struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// columnsToMap converts format-version 2 columns array to a map.
func columnsToMap(cols []wal2JSONColumn) map[string]interface{} {
	if len(cols) == 0 {
		return nil
	}
	m := make(map[string]interface{}, len(cols))
	for _, col := range cols {
		m[col.Name] = col.Value
	}
	return m
}
