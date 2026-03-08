//go:build integration

package integration

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestCheckpointRecovery(t *testing.T) {
	connString, slotName := startPostgres(t, "pgoutput")
	natsURL := startNATS(t)
	streamNamePhase1 := fmt.Sprintf("CDC_ckpt_p1_%d", rand.Int64N(100000))

	cancel1, doneCh1 := startEngine(t, engineConfig{
		ConnString: connString,
		SlotName:   slotName,
		Plugin:     "pgoutput",
		NATSURLs:   []string{natsURL},
		StreamName: streamNamePhase1,
	})

	waitForEngineReady(t, connString, natsURL, streamNamePhase1, "cdc.>", 15*time.Second)

	initialLSN := getConfirmedFlushLSN(t, connString, slotName)

	flushedEmail := fmt.Sprintf("ckpt_flushed_%d@test.com", rand.Int64N(1_000_000))
	execSQL(t, connString, fmt.Sprintf(
		"INSERT INTO accounts (email, status) VALUES ('%s', 'active')", flushedEmail,
	))
	consumeEvents(t, natsURL, streamNamePhase1, "cdc.>", 1, 10*time.Second)

	pendingEmail := fmt.Sprintf("ckpt_pending_%d@test.com", rand.Int64N(1_000_000))
	execSQL(t, connString, fmt.Sprintf(
		"INSERT INTO accounts (email, status) VALUES ('%s', 'active')", pendingEmail,
	))
	consumeEvents(t, natsURL, streamNamePhase1, "cdc.>", 1, 10*time.Second)

	beforeStopLSN := getConfirmedFlushLSN(t, connString, slotName)
	cancel1()
	select {
	case <-doneCh1:
	case <-time.After(10 * time.Second):
		t.Fatal("engine did not stop within 10s")
	}

	afterStopLSN := pollConfirmedFlushLSN(t, connString, slotName, initialLSN, 5*time.Second)
	if afterStopLSN == initialLSN {
		t.Fatalf("expected graceful shutdown to advance confirmed_flush_lsn beyond initial %q", initialLSN)
	}

	purgeStream(t, natsURL, streamNamePhase1)

	catchupEmails := []string{
		fmt.Sprintf("ckpt_catchup_0_%d@test.com", rand.Int64N(1_000_000)),
		fmt.Sprintf("ckpt_catchup_1_%d@test.com", rand.Int64N(1_000_000)),
	}
	for _, email := range catchupEmails {
		execSQL(t, connString, fmt.Sprintf(
			"INSERT INTO accounts (email, status) VALUES ('%s', 'active')", email,
		))
	}

	cancel2, _ := startEngine(t, engineConfig{
		ConnString: connString,
		SlotName:   slotName,
		Plugin:     "pgoutput",
		NATSURLs:   []string{natsURL},
		StreamName: streamNamePhase1,
	})
	defer cancel2()

	waitForEngineReady(t, connString, natsURL, streamNamePhase1, "cdc.>", 15*time.Second)
	time.Sleep(2 * time.Second)

	restartEvents := drainEvents(t, natsURL, streamNamePhase1, "cdc.>", 10*time.Second)

	foundCatchup := make(map[string]bool, len(catchupEmails))
	for _, evt := range restartEvents {
		email, ok := evt.After["email"].(string)
		if !ok {
			continue
		}
		if email == flushedEmail || email == pendingEmail {
			t.Fatalf("graceful restart replayed pre-shutdown event for %s", email)
		}
		for _, want := range catchupEmails {
			if email == want {
				foundCatchup[email] = true
			}
		}
	}

	for _, email := range catchupEmails {
		if !foundCatchup[email] {
			t.Fatalf("expected catch-up event for %s after restart", email)
		}
	}

	t.Logf("Graceful restart resumed from flushed checkpoint %s without replaying pre-shutdown work (slot was %s before stop).", afterStopLSN, beforeStopLSN)
}

func purgeStream(t *testing.T, natsURL, streamName string) {
	t.Helper()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("purge stream connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("purge stream jetstream: %v", err)
	}

	if err := js.PurgeStream(streamName); err != nil {
		t.Fatalf("purge stream %q: %v", streamName, err)
	}
}
