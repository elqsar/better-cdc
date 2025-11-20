package transformer

import (
	"context"
	"fmt"

	"better-cdc/internal/model"
)

// Transformer converts WALEvent into CDCEvent including enrichment and filtering.
type Transformer interface {
	Transform(ctx context.Context, evt *model.WALEvent) (*model.CDCEvent, error)
}

// SimpleTransformer constructs a CDCEvent with deterministic EventID using lsn:txid:table.
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

	eventID := fmt.Sprintf("%s:%d:%s:%s", evt.LSN, evt.TxID, evt.Table, evt.TransactionID)

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
