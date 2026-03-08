//go:build integration

package integration

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

func TestRecoveryAtLeastOnce(t *testing.T) {
	connString, slotName := startPostgres(t, "pgoutput")
	natsURL := startNATS(t)
	streamName := fmt.Sprintf("CDC_recovery_%d", rand.Int64N(100000))

	// Phase 1: Start engine, insert 20 rows concurrently, hard-kill after ~10
	cancel1, doneCh1 := startEngine(t, engineConfig{
		ConnString: connString,
		SlotName:   slotName,
		Plugin:     "pgoutput",
		NATSURLs:   []string{natsURL},
		StreamName: streamName,
	})

	waitForEngineReady(t, connString, natsURL, streamName, "cdc.>", 15*time.Second)

	const totalRows = 20
	emails := make([]string, totalRows)
	for i := range totalRows {
		emails[i] = fmt.Sprintf("recovery_%d_%d@test.com", i, rand.Int64N(1_000_000))
	}

	// Insert rows in a goroutine with delay
	insertDone := make(chan struct{})
	go func() {
		defer close(insertDone)
		for i, email := range emails {
			execSQL(t, connString, fmt.Sprintf(
				"INSERT INTO accounts (email, status) VALUES ('%s', 'active')", email,
			))
			if i < totalRows-1 {
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	// Hard-cancel after ~500ms (roughly 10 rows)
	time.Sleep(500 * time.Millisecond)
	cancel1()

	select {
	case <-doneCh1:
	case <-time.After(10 * time.Second):
		t.Fatal("engine did not stop within 10s after cancel")
	}

	// Wait for all inserts to complete
	<-insertDone

	// Drain all events from first run
	firstRunEvents := drainEvents(t, natsURL, streamName, "cdc.>", 5*time.Second)
	firstRunEmails := make(map[string]bool)
	for _, evt := range firstRunEvents {
		if email, ok := evt.After["email"].(string); ok {
			firstRunEmails[email] = true
		}
	}
	t.Logf("Phase 1: captured %d events, %d unique emails", len(firstRunEvents), len(firstRunEmails))

	// Phase 2: Start new engine on same slot, let it catch up
	cancel2, _ := startEngine(t, engineConfig{
		ConnString: connString,
		SlotName:   slotName,
		Plugin:     "pgoutput",
		NATSURLs:   []string{natsURL},
		StreamName: streamName,
	})
	defer cancel2()

	// Give it time to catch up
	time.Sleep(5 * time.Second)

	secondRunEvents := drainEvents(t, natsURL, streamName, "cdc.>", 5*time.Second)
	secondRunEmails := make(map[string]bool)
	for _, evt := range secondRunEvents {
		if email, ok := evt.After["email"].(string); ok {
			secondRunEmails[email] = true
		}
	}
	t.Logf("Phase 2: captured %d events, %d unique emails", len(secondRunEvents), len(secondRunEmails))

	// Assert: union of both runs covers all 20 inserted emails
	allEmails := make(map[string]bool)
	for k, v := range firstRunEmails {
		allEmails[k] = v
	}
	for k, v := range secondRunEmails {
		allEmails[k] = v
	}

	missing := 0
	for _, email := range emails {
		if !allEmails[email] {
			t.Errorf("missing email in combined runs: %s", email)
			missing++
		}
	}

	if missing > 0 {
		t.Fatalf("at-least-once violated: %d/%d emails missing", missing, totalRows)
	}

	// Count duplicates (expected and acceptable)
	duplicates := 0
	for email := range firstRunEmails {
		if secondRunEmails[email] {
			duplicates++
		}
	}
	t.Logf("At-least-once verified: all %d emails captured. %d duplicates between runs (acceptable).", totalRows, duplicates)
}
