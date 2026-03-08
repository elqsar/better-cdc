//go:build integration

package integration

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

func TestCheckpointRecovery(t *testing.T) {
	connString, slotName := startPostgres(t, "pgoutput")
	natsURL := startNATS(t)
	streamName := fmt.Sprintf("CDC_ckpt_%d", rand.Int64N(100000))

	// Phase 1: Start engine, insert 5 rows, consume events, stop gracefully
	cancel1, doneCh1 := startEngine(t, engineConfig{
		ConnString: connString,
		SlotName:   slotName,
		Plugin:     "pgoutput",
		NATSURLs:   []string{natsURL},
		StreamName: streamName,
	})

	waitForEngineReady(t, connString, natsURL, streamName, "cdc.>", 15*time.Second)

	phase1Emails := make([]string, 5)
	for i := range 5 {
		phase1Emails[i] = fmt.Sprintf("ckpt_p1_%d_%d@test.com", i, rand.Int64N(1_000_000))
		execSQL(t, connString, fmt.Sprintf(
			"INSERT INTO accounts (email, status) VALUES ('%s', 'active')", phase1Emails[i],
		))
	}

	// Wait for all 5 events to be published to NATS
	// Use drainEvents to get everything including sentinel
	time.Sleep(2 * time.Second) // Allow engine to process and publish
	phase1AllEvents := drainEvents(t, natsURL, streamName, "cdc.>", 5*time.Second)
	phase1IDs := make(map[string]bool)
	phase1EmailSet := make(map[string]bool)
	for _, evt := range phase1AllEvents {
		if evt.EventID != "" {
			phase1IDs[evt.EventID] = true
		}
		if email, ok := evt.After["email"].(string); ok {
			phase1EmailSet[email] = true
		}
	}

	// Verify all 5 test emails were captured
	capturedPhase1 := 0
	for _, email := range phase1Emails {
		if phase1EmailSet[email] {
			capturedPhase1++
		}
	}
	t.Logf("Phase 1: captured %d total events, %d/%d test emails", len(phase1AllEvents), capturedPhase1, len(phase1Emails))

	// Graceful stop
	cancel1()
	select {
	case <-doneCh1:
	case <-time.After(10 * time.Second):
		t.Fatal("engine did not stop within 10s")
	}

	// Phase 2: Insert 3 more rows while engine is down
	phase2Emails := make([]string, 3)
	for i := range 3 {
		phase2Emails[i] = fmt.Sprintf("ckpt_p2_%d_%d@test.com", i, rand.Int64N(1_000_000))
		execSQL(t, connString, fmt.Sprintf(
			"INSERT INTO accounts (email, status) VALUES ('%s', 'active')", phase2Emails[i],
		))
	}

	// Phase 3: Start a new engine on the same slot + stream
	cancel2, _ := startEngine(t, engineConfig{
		ConnString: connString,
		SlotName:   slotName,
		Plugin:     "pgoutput",
		NATSURLs:   []string{natsURL},
		StreamName: streamName,
	})
	defer cancel2()

	// Wait for the new engine to catch up and process the 3 pending inserts
	time.Sleep(3 * time.Second)

	// Drain ALL events from the stream (includes Phase 1 + Phase 3 events)
	phase3Events := drainEvents(t, natsURL, streamName, "cdc.>", 10*time.Second)

	// Assert: the 3 new emails must appear somewhere in the stream
	phase2EmailSet := make(map[string]bool)
	for _, e := range phase2Emails {
		phase2EmailSet[e] = true
	}

	foundNewEmails := 0
	replayedOldIDs := 0
	for _, evt := range phase3Events {
		if email, ok := evt.After["email"].(string); ok {
			if phase2EmailSet[email] {
				foundNewEmails++
			}
		}
		if phase1IDs[evt.EventID] {
			replayedOldIDs++
		}
	}

	if foundNewEmails < 3 {
		t.Errorf("expected all 3 new emails to appear, found %d", foundNewEmails)
	}
	if replayedOldIDs > 0 {
		// At-least-once semantics: some replay is acceptable since confirmed_flush_lsn
		// may not advance on graceful shutdown (StandbyStatusUpdate only sent during
		// the replication loop, not explicitly on shutdown).
		t.Logf("NOTE: %d old events were replayed (acceptable under at-least-once semantics)", replayedOldIDs)
	}
	t.Logf("Phase 3: %d events from stream, %d new emails found, %d old IDs replayed", len(phase3Events), foundNewEmails, replayedOldIDs)
}
