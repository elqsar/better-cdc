package transformer

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"better-cdc/internal/model"
)

// Transformer converts WALEvent into CDCEvent including enrichment and filtering.
type Transformer interface {
	Transform(ctx context.Context, evt *model.WALEvent) (*model.CDCEvent, error)
}

// SimpleTransformer constructs a CDCEvent with deterministic EventID using lsn:txid:table:pk.
type SimpleTransformer struct {
	source string
}

func NewSimpleTransformer(source string) *SimpleTransformer {
	return &SimpleTransformer{source: source}
}

func (t *SimpleTransformer) Transform(ctx context.Context, evt *model.WALEvent) (*model.CDCEvent, error) {
	_ = ctx
	if evt == nil {
		return nil, fmt.Errorf("nil event")
	}

	eventID := strings.Join([]string{
		evt.LSN,
		fmt.Sprintf("%d", evt.TxID),
		fmt.Sprintf("%s.%s", evt.Schema, evt.Table),
		primaryKeyFragment(evt),
	}, ":")

	return &model.CDCEvent{
		EventID:    eventID,
		EventType:  fmt.Sprintf("cdc.%s", lowerOp(evt.Operation)),
		Source:     t.source,
		Timestamp:  evt.Timestamp,
		CommitTime: evt.CommitTime,
		LSN:        evt.LSN,
		TxID:       evt.TxID,
		Schema:     evt.Schema,
		Table:      evt.Table,
		Operation:  string(evt.Operation),
		Before:     evt.OldValues,
		After:      evt.NewValues,
		Metadata:   map[string]interface{}{"txid": evt.TransactionID},
	}, nil
}

func lowerOp(op model.OperationType) string {
	switch op {
	case model.OperationInsert:
		return "insert"
	case model.OperationUpdate:
		return "update"
	case model.OperationDelete:
		return "delete"
	case model.OperationDDL:
		return "ddl"
	default:
		return "unknown"
	}
}

// primaryKeyFragment builds a deterministic, sorted key=value list from the best-known keyset.
// Prefers OldValues (oldkeys from wal2json / delete tuples) and falls back to NewValues.
func primaryKeyFragment(evt *model.WALEvent) string {
	var keyset map[string]interface{}
	if len(evt.OldValues) > 0 {
		keyset = evt.OldValues
	} else {
		keyset = evt.NewValues
	}
	if len(keyset) == 0 {
		return "nopk"
	}

	keys := make([]string, 0, len(keyset))
	for k := range keyset {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, keyset[k]))
	}
	return strings.Join(parts, ",")
}
