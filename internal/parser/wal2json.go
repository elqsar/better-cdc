package parser

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"go.uber.org/zap"

	"better-cdc/internal/metrics"
	"better-cdc/internal/model"
)

// Wal2JSONParser decodes wal2json plugin output into WALEvents.
type Wal2JSONParser struct {
	logger   *zap.Logger
	lagGauge *metrics.Gauge
	errs     *metrics.Counter
}

func NewWal2JSONParser(logger *zap.Logger) *Wal2JSONParser {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Wal2JSONParser{
		logger:   logger,
		lagGauge: metrics.NewGauge("replication_lag_ms"),
		errs:     metrics.NewCounter("decode_errors"),
	}
}

func (p *Wal2JSONParser) Parse(ctx context.Context, stream <-chan *RawMessage) (<-chan *model.WALEvent, error) {
	out := make(chan *model.WALEvent)
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
				events, err := decodeWal2JSON(uint64(msg.WALStart), msg.Data)
				if err != nil {
					p.errs.Inc()
					p.logger.Warn("decode wal2json failed", zap.Error(err))
					continue
				}
				for _, evt := range events {
					if !evt.CommitTime.IsZero() {
						p.lagGauge.Set(time.Since(evt.CommitTime).Milliseconds())
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
