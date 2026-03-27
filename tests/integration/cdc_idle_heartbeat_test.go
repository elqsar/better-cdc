//go:build integration

package integration

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

func TestCheckpointAdvancesDuringIdleHeartbeat(t *testing.T) {
	connString, slotName := startPostgres(t, "pgoutput")
	natsURL := startNATS(t)
	streamName := fmt.Sprintf("CDC_idle_%d", rand.Int64N(100000))

	cancel, _ := startEngine(t, engineConfig{
		ConnString:     connString,
		SlotName:       slotName,
		Plugin:         "pgoutput",
		NATSURLs:       []string{natsURL},
		StreamName:     streamName,
		StandbyTimeout: 300 * time.Millisecond,
	})
	defer cancel()

	waitForEngineReady(t, connString, natsURL, streamName, "cdc.>", 15*time.Second)

	initialLSN := getConfirmedFlushLSN(t, connString, slotName)

	email := fmt.Sprintf("idle_heartbeat_%d@test.com", rand.Int64N(1_000_000))
	execSQL(t, connString, fmt.Sprintf(
		"INSERT INTO accounts (email, status) VALUES ('%s', 'active')", email,
	))
	consumeEvents(t, natsURL, streamName, "cdc.>", 1, 10*time.Second)

	advancedLSN := pollConfirmedFlushLSN(t, connString, slotName, initialLSN, 5*time.Second)
	if advancedLSN == initialLSN {
		t.Fatalf("expected idle standby heartbeat to advance confirmed_flush_lsn beyond %q", initialLSN)
	}
}
