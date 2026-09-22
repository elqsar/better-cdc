package transformer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/elqsar/better-cdc/internal/model"
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
	source   string
	identity model.Identity
}

func NewSimpleTransformer(source string, identity ...model.Identity) *SimpleTransformer {
	t := &SimpleTransformer{source: source}
	if len(identity) > 0 {
		t.identity = identity[0]
	}
	return t
}

func (t *SimpleTransformer) Transform(ctx context.Context, evt *model.WALEvent) (*model.CDCEvent, error) {
	_ = ctx
	if evt == nil {
		return nil, fmt.Errorf("nil event")
	}

	// Use pooled CDCEvent
	cdcEvt := model.AcquireCDCEvent()

	// Build EventID with strings.Builder for efficiency
	identity := evt.Identity
	if identity.SourceID == "" {
		identity = t.identity
	}
	cdcEvt.EventID = EventIDFor(identity, evt)
	cdcEvt.SchemaVersion = 1
	if identity.SourceID != "" {
		cdcEvt.SchemaVersion = 2
		cdcEvt.SourceID = identity.SourceID
	}
	cdcEvt.SeqInTx = evt.SeqInTx
	if len(evt.UnavailableBefore) > 0 {
		cdcEvt.Metadata["unavailable_before"] = evt.UnavailableBefore
	}
	if len(evt.UnavailableAfter) > 0 {
		cdcEvt.Metadata["unavailable_after"] = evt.UnavailableAfter
	}
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

// EventID returns the identity used for publication and recovery.
func EventID(evt *model.WALEvent) string { return EventIDFor(evt.Identity, evt) }

// EventIDFor preserves legacy IDs only when reconstructing legacy records.
func EventIDFor(identity model.Identity, evt *model.WALEvent) string {
	if identity.SourceID == "" {
		return buildEventID(evt)
	}
	data, _ := json.Marshal([]any{identity.SourceID, identity.Slot, identity.Decoder, evt.LSN, evt.TxID, string(evt.Operation), evt.Schema, evt.Table, evt.SeqInTx})
	sum := sha256.Sum256(data)
	return "v2:" + hex.EncodeToString(sum[:])
}
