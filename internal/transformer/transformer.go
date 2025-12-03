package transformer

import (
	"context"
	"fmt"
	"sort"
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
// Format: lsn:txid:schema.table:pk_fragment
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
	sb.WriteString(evt.Schema)
	sb.WriteByte('.')
	sb.WriteString(evt.Table)
	sb.WriteByte(':')
	writePrimaryKeyFragment(sb, evt)

	return sb.String()
}

// writePrimaryKeyFragment writes sorted key=value pairs to the builder.
func writePrimaryKeyFragment(sb *strings.Builder, evt *model.WALEvent) {
	var keyset map[string]interface{}
	if len(evt.OldValues) > 0 {
		keyset = evt.OldValues
	} else {
		keyset = evt.NewValues
	}
	if len(keyset) == 0 {
		sb.WriteString("nopk")
		return
	}

	keys := make([]string, 0, len(keyset))
	for k := range keyset {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		writeValue(sb, keyset[k])
	}
}

// writeValue writes a value without fmt.Sprintf allocation.
func writeValue(sb *strings.Builder, v interface{}) {
	switch val := v.(type) {
	case string:
		sb.WriteString(val)
	case int:
		sb.WriteString(strconv.Itoa(val))
	case int64:
		sb.WriteString(strconv.FormatInt(val, 10))
	case uint64:
		sb.WriteString(strconv.FormatUint(val, 10))
	case float64:
		sb.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
	case bool:
		sb.WriteString(strconv.FormatBool(val))
	case nil:
		sb.WriteString("null")
	default:
		// Fallback for complex types
		sb.WriteString(fmt.Sprintf("%v", val))
	}
}
