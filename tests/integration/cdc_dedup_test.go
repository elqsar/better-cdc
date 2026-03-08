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
