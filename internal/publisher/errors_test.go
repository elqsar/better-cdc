package publisher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestIsPermanentPublishError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		permanent bool
	}{
		{"nil", nil, false},
		{"max payload", nats.ErrMaxPayload, true},
		{"bad subject", nats.ErrBadSubject, true},
		{"wrapped max payload", fmt.Errorf("publish async: %w", nats.ErrMaxPayload), true},
		{"js message size exceeds maximum", &nats.APIError{ErrorCode: jsErrCodeMessageSizeExceedsMaximum, Code: 400}, true},
		{"js other api error", &nats.APIError{ErrorCode: nats.JSErrCodeStreamNotFound, Code: 404}, false},
		{"timeout", errors.New("ack timeout"), false},
		{"no responders", nats.ErrNoResponders, false},
		{"connection closed", nats.ErrConnectionClosed, false},
		{"authorization", nats.ErrAuthorization, false},
		{"permissions violation", nats.ErrPermissionViolation, false},
		{"generic", errors.New("boom"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPermanentPublishError(tt.err); got != tt.permanent {
				t.Errorf("IsPermanentPublishError(%v) = %v, want %v", tt.err, got, tt.permanent)
			}
		})
	}
}

func TestDeadLetterRecordSetPayloadTruncates(t *testing.T) {
	rec := &DeadLetterRecord{}
	big := make([]byte, DLQPayloadPrefixLimit+100)
	for i := range big {
		big[i] = 'a'
	}
	rec.SetPayload(big)
	if !rec.Truncated {
		t.Error("expected Truncated=true for oversized payload")
	}
	if rec.PayloadSize != len(big) {
		t.Errorf("PayloadSize = %d, want %d", rec.PayloadSize, len(big))
	}
	if len(rec.PayloadPrefix) != DLQPayloadPrefixLimit {
		t.Errorf("PayloadPrefix length = %d, want %d", len(rec.PayloadPrefix), DLQPayloadPrefixLimit)
	}

	rec = &DeadLetterRecord{}
	rec.SetPayload([]byte("small"))
	if rec.Truncated {
		t.Error("expected Truncated=false for small payload")
	}
	if rec.PayloadPrefix != "small" || rec.PayloadSize != 5 {
		t.Errorf("unexpected record: %+v", rec)
	}
}

func TestDeadLetterSubject(t *testing.T) {
	tests := []struct {
		prefix, db, schema, table, want string
	}{
		{"cdc.dlq", "appdb", "public", "users", "cdc.dlq.appdb.public.users"},
		{"cdc.dlq", "appdb", "", "", "cdc.dlq.appdb._._"},
		{"", "db", "s", "bad table*", "_.db.s.bad_table_"},
	}
	for _, tt := range tests {
		if got := DeadLetterSubject(tt.prefix, tt.db, tt.schema, tt.table); got != tt.want {
			t.Errorf("DeadLetterSubject(%q,%q,%q,%q) = %q, want %q", tt.prefix, tt.db, tt.schema, tt.table, got, tt.want)
		}
	}
}

type capturingPublisher struct {
	NoopPublisher
	subjects []string
	payloads [][]byte
	msgIDs   []string
	err      error
}

func (c *capturingPublisher) Publish(_ context.Context, subject string, data []byte, eventID string) error {
	if c.err != nil {
		return c.err
	}
	c.subjects = append(c.subjects, subject)
	c.payloads = append(c.payloads, append([]byte(nil), data...))
	c.msgIDs = append(c.msgIDs, eventID)
	return nil
}

func TestPublishDeadLetter(t *testing.T) {
	pub := &capturingPublisher{}
	rec := &DeadLetterRecord{
		EventID:  "evt-1",
		Database: "appdb",
		Schema:   "public",
		Table:    "users",
		Error:    "nats: maximum payload exceeded",
	}
	if err := PublishDeadLetter(context.Background(), pub, "cdc.dlq", rec); err != nil {
		t.Fatalf("PublishDeadLetter: %v", err)
	}
	if len(pub.subjects) != 1 || pub.subjects[0] != "cdc.dlq.appdb.public.users" {
		t.Errorf("unexpected subjects: %v", pub.subjects)
	}
	if pub.msgIDs[0] != "dlq-evt-1" {
		t.Errorf("msgID = %q, want dlq-evt-1", pub.msgIDs[0])
	}

	pub = &capturingPublisher{err: errors.New("down")}
	if err := PublishDeadLetter(context.Background(), pub, "cdc.dlq", rec); err == nil {
		t.Error("expected error when DLQ publish fails")
	}
}
