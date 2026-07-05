//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamDedup(t *testing.T) {
	connString, slotName := startPostgres(t, "pgoutput")
	natsURL := startNATS(t)
	streamName := fmt.Sprintf("CDC_dedup_%d", rand.Int64N(100000))

	cancel, _ := startEngine(t, engineConfig{
		ConnString: connString,
		SlotName:   slotName,
		Plugin:     "pgoutput",
		NATSURLs:   []string{natsURL},
		StreamName: streamName,
	})
	defer cancel()

	waitForEngineReady(t, connString, natsURL, streamName, "cdc.>", 15*time.Second)

	// Insert a row and capture the event
	dedupEmail := fmt.Sprintf("dedup_%d@test.com", rand.Int64N(1_000_000))
	execSQL(t, connString, fmt.Sprintf(
		"INSERT INTO accounts (email, status) VALUES ('%s', 'active')", dedupEmail,
	))

	events := consumeEvents(t, natsURL, streamName, "cdc.>", 1, 10*time.Second)
	if len(events) == 0 {
		t.Fatal("no events captured for dedup test")
	}

	var targetEvent *struct {
		eventID string
		subject string
		data    []byte
	}
	for _, evt := range events {
		if email, ok := evt.After["email"].(string); ok && email == dedupEmail {
			data, err := json.Marshal(evt)
			if err != nil {
				t.Fatalf("marshal event: %v", err)
			}
			targetEvent = &struct {
				eventID string
				subject string
				data    []byte
			}{
				eventID: evt.EventID,
				subject: fmt.Sprintf("cdc.postgres.%s.%s", evt.Schema, evt.Table),
				data:    data,
			}
			break
		}
	}
	if targetEvent == nil {
		t.Fatalf("target event for email %s not found", dedupEmail)
	}
	t.Logf("Captured event with EventID: %s", targetEvent.eventID)

	// Get stream message count before duplicate publish
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect for dedup check: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream for dedup check: %v", err)
	}

	info1, err := js.StreamInfo(streamName)
	if err != nil {
		t.Fatalf("stream info before dedup: %v", err)
	}
	countBefore := info1.State.Msgs
	t.Logf("Stream message count before duplicate: %d", countBefore)

	// Re-publish the exact same message with same MsgId
	_, err = js.Publish(targetEvent.subject, targetEvent.data, nats.MsgId(targetEvent.eventID))
	if err != nil {
		t.Fatalf("publish duplicate: %v", err)
	}

	// Small delay for JetStream to process
	time.Sleep(500 * time.Millisecond)

	info2, err := js.StreamInfo(streamName)
	if err != nil {
		t.Fatalf("stream info after dedup: %v", err)
	}
	countAfter := info2.State.Msgs
	t.Logf("Stream message count after duplicate: %d", countAfter)

	if countAfter != countBefore {
		t.Errorf("dedup failed: message count increased from %d to %d (expected no change)", countBefore, countAfter)
	} else {
		t.Log("Dedup verified: duplicate message was rejected by JetStream")
	}
}

// TestJetStreamNoFalseDedupWithinTransaction guards the EventID-collision fix:
// two distinct changes to the same row inside one transaction share the same
// commit LSN and txid, and (values unchanged) the same tuple fragment. Before the
// per-transaction sequence was added to the EventID, both events produced an
// identical Nats-Msg-Id and JetStream silently dropped the second one. Both must
// now survive.
func TestJetStreamNoFalseDedupWithinTransaction(t *testing.T) {
	connString, slotName := startPostgres(t, "pgoutput")
	natsURL := startNATS(t)
	streamName := fmt.Sprintf("CDC_nofalsededup_%d", rand.Int64N(100000))

	cancel, _ := startEngine(t, engineConfig{
		ConnString: connString,
		SlotName:   slotName,
		Plugin:     "pgoutput",
		NATSURLs:   []string{natsURL},
		StreamName: streamName,
	})
	defer cancel()

	waitForEngineReady(t, connString, natsURL, streamName, "cdc.>", 15*time.Second)

	// Two identical updates to the same row in ONE transaction. The values are
	// unchanged between them, so their tuple fragments are identical; only the
	// per-transaction sequence distinguishes their EventIDs.
	probe := fmt.Sprintf("probe_%d", rand.Int64N(1_000_000))
	execSQL(t, connString, fmt.Sprintf(
		"BEGIN; UPDATE accounts SET status = '%s' WHERE id = 1; UPDATE accounts SET status = '%s' WHERE id = 1; COMMIT;",
		probe, probe,
	))

	// Both update events for this probe value must land in the stream.
	events := consumeEvents(t, natsURL, streamName, "cdc.>", 2, 10*time.Second)

	matched := 0
	var ids []string
	for _, evt := range events {
		if status, ok := evt.After["status"].(string); ok && status == probe {
			matched++
			ids = append(ids, evt.EventID)
		}
	}

	if matched != 2 {
		t.Fatalf("expected 2 events for probe %q (one per in-transaction update), got %d; EventIDs=%v",
			probe, matched, ids)
	}
	if ids[0] == ids[1] {
		t.Fatalf("the two in-transaction updates produced identical EventIDs %q — collision not fixed", ids[0])
	}
	t.Logf("Both in-transaction updates survived with distinct EventIDs: %v", ids)
}
