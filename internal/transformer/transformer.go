package transformer

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"better-cdc/internal/model"
)

var stringBuilderPool = sync.Pool{
	New: func() interface{} {
		return &strings.Builder{}
	},
}

// Transformer converts WALEvent into CDCEvent including enrichment and filtering.
type Transformer interface {
	Transform(ctx context.Context, evt *model.WALEvent) (*model.CDCEvent, error)
}

// SimpleTransformer constructs a CDCEvent with deterministic EventID using lsn:txid:op:table:seq.
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

	// Use pooled CDCEvent
	cdcEvt := model.AcquireCDCEvent()

	// Build EventID with strings.Builder for efficiency
	cdcEvt.EventID = buildEventID(evt)
	cdcEvt.EventType = eventType(evt.Operation)
	cdcEvt.Source = t.source
	cdcEvt.Timestamp = evt.Timestamp
	cdcEvt.CommitTime = evt.CommitTime
	cdcEvt.LSN = evt.LSN
	cdcEvt.TxID = evt.TxID
	cdcEvt.Schema = evt.Schema
	cdcEvt.Table = evt.Table
	cdcEvt.Operation = string(evt.Operation)
	cdcEvt.Before = evt.OldValues
	cdcEvt.After = evt.NewValues
	cdcEvt.Metadata["txid"] = evt.TransactionID

	return cdcEvt, nil
}

// Pre-computed event type strings to avoid allocations
var eventTypeStrings = map[model.OperationType]string{
	model.OperationInsert: "cdc.insert",
	model.OperationUpdate: "cdc.update",
	model.OperationDelete: "cdc.delete",
	model.OperationDDL:    "cdc.ddl",
}

func eventType(op model.OperationType) string {
	if s, ok := eventTypeStrings[op]; ok {
		return s
	}
	return "cdc.unknown"
}

// buildEventID constructs EventID using pooled strings.Builder for efficiency.
// Format: lsn:txid:op:schema.table:seq
//
// The operation and per-transaction sequence (evt.SeqInTx) are included because
// every event in a transaction shares the same commit LSN and txid; without them
// two events touching the same table in one transaction could produce identical
// IDs and the second would be silently deduped by JetStream. seq is a
// deterministic WAL-order ordinal, so genuine replayed duplicates still collapse
// to the same ID. Row values are intentionally excluded so large payload fields
// never become NATS message-id headers.
func buildEventID(evt *model.WALEvent) string {
	sb := stringBuilderPool.Get().(*strings.Builder)
	sb.Reset()
	defer stringBuilderPool.Put(sb)

	// Pre-grow to avoid reallocation (estimate: ~128 bytes typical)
	sb.Grow(128)

	sb.WriteString(evt.LSN)
	sb.WriteByte(':')
	sb.WriteString(strconv.FormatUint(evt.TxID, 10))
	sb.WriteByte(':')
	sb.WriteString(string(evt.Operation))
	sb.WriteByte(':')
	sb.WriteString(evt.Schema)
	sb.WriteByte('.')
	sb.WriteString(evt.Table)
	sb.WriteByte(':')
	sb.WriteString(strconv.FormatUint(uint64(evt.SeqInTx), 10))

	return sb.String()
}
