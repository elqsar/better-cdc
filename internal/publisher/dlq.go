package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// DLQPayloadPrefixLimit caps how much of the original payload is embedded in a
// dead-letter record. An oversized payload is usually what sent the event to
// the DLQ in the first place, so the record must stay well under any broker
// payload limit while keeping enough data to diagnose and repair.
const DLQPayloadPrefixLimit = 64 * 1024

// DeadLetterRecord describes an event that permanently failed to publish and
// was quarantined instead of crashing the engine.
type DeadLetterRecord struct {
	EventID       string    `json:"event_id"`
	Subject       string    `json:"subject"`
	Database      string    `json:"database"`
	Schema        string    `json:"schema"`
	Table         string    `json:"table"`
	Operation     string    `json:"operation"`
	LSN           string    `json:"lsn"`
	TxID          uint64    `json:"txid"`
	Error         string    `json:"error"`
	PayloadSize   int       `json:"payload_size"`
	Truncated     bool      `json:"truncated"`
	PayloadPrefix string    `json:"payload_prefix,omitempty"`
	QuarantinedAt time.Time `json:"quarantined_at"`
}

// SetPayload stores the original payload on the record, truncating it to
// DLQPayloadPrefixLimit bytes.
func (r *DeadLetterRecord) SetPayload(payload []byte) {
	r.PayloadSize = len(payload)
	if len(payload) > DLQPayloadPrefixLimit {
		r.Truncated = true
		payload = payload[:DLQPayloadPrefixLimit]
	}
	r.PayloadPrefix = string(payload)
}

// DeadLetterSubject builds the subject a quarantined event is published to:
// {prefix}.{database}.{schema}.{table}. Empty segments are replaced with "_"
// so the subject stays valid even when the failure happened before the event
// was fully transformed.
func DeadLetterSubject(prefix, database, schema, table string) string {
	return strings.Join([]string{
		subjectToken(prefix), subjectToken(database), subjectToken(schema), subjectToken(table),
	}, ".")
}

func subjectToken(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "_"
	}
	// Spaces and wildcard tokens are invalid inside a NATS subject token.
	return strings.NewReplacer(" ", "_", "*", "_", ">", "_").Replace(s)
}

// PublishDeadLetter marshals and synchronously publishes a dead-letter record.
// The MsgId is derived from the original event ID so a replayed batch does not
// duplicate quarantine records within the stream's duplicate window.
func PublishDeadLetter(ctx context.Context, pub Publisher, prefix string, rec *DeadLetterRecord) error {
	subject := DeadLetterSubject(prefix, rec.Database, rec.Schema, rec.Table)
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal dead-letter record: %w", err)
	}
	msgID := ""
	if rec.EventID != "" {
		msgID = "dlq-" + rec.EventID
	}
	if err := pub.Publish(ctx, subject, data, msgID); err != nil {
		return fmt.Errorf("publish dead-letter record to %s: %w", subject, err)
	}
	return nil
}
