package publisher

import (
	"context"
	"fmt"
	"strings"
	"time"

	"better-cdc/internal/model"
	"better-cdc/internal/subject"
)

// DeadLetterRecord retains a complete event or a value-independent replay capsule.
// Payload and Recovery live in Object Store, never inside the small index message.
type DeadLetterRecord struct {
	EventID       string                `json:"event_id"`
	Subject       string                `json:"subject"`
	Database      string                `json:"database"`
	Schema        string                `json:"schema"`
	Table         string                `json:"table"`
	Operation     string                `json:"operation"`
	LSN           string                `json:"lsn"`
	TxID          uint64                `json:"txid"`
	Error         string                `json:"error"`
	PayloadSize   int                   `json:"payload_size"`
	Payload       []byte                `json:"payload,omitempty"`
	Recovery      *model.RecoveryChange `json:"recovery,omitempty"`
	QuarantinedAt time.Time             `json:"quarantined_at"`
	Object        string                `json:"object"`
	SHA256        string                `json:"sha256"`
}

func (r *DeadLetterRecord) SetPayload(payload []byte) {
	r.PayloadSize = len(payload)
	r.Payload = append([]byte(nil), payload...)
}
func DeadLetterSubject(prefix, database, schema, table string) string {
	return strings.Join([]string{prefix, subject.Token(database), subject.Token(schema), subject.Token(table)}, ".")
}

type Quarantiner interface {
	Quarantine(context.Context, string, *DeadLetterRecord) error
}

// PublishDeadLetter must never fall back to a diagnostic-only publish.
func PublishDeadLetter(ctx context.Context, pub Publisher, prefix string, rec *DeadLetterRecord) error {
	q, ok := pub.(Quarantiner)
	if !ok {
		return fmt.Errorf("publisher does not provide durable quarantine")
	}
	if rec.EventID == "" || (len(rec.Payload) == 0 && rec.Recovery == nil) {
		return fmt.Errorf("event has no complete recovery representation")
	}
	return q.Quarantine(ctx, prefix, rec)
}
