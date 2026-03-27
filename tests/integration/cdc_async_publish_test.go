//go:build integration

package integration

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

func TestAsyncPublishSupportsProductionSizedBatch(t *testing.T) {
	connString, slotName := startPostgres(t, "pgoutput")
	natsURL := startNATS(t)
	streamName := fmt.Sprintf("CDC_async_%d", rand.Int64N(100000))

	cancel, _ := startEngine(t, engineConfig{
		ConnString: connString,
		SlotName:   slotName,
		Plugin:     "pgoutput",
		NATSURLs:   []string{natsURL},
		StreamName: streamName,
		BatchSize:  500,
	})
	defer cancel()

	waitForEngineReady(t, connString, natsURL, streamName, "cdc.>", 15*time.Second)

	const rowCount = 300
	bulkPrefix := fmt.Sprintf("bulk_async_%d", rand.Int64N(1_000_000))
	execSQL(t, connString, buildBulkAccountInsertSQL(bulkPrefix, rowCount))

	events := consumeEvents(t, natsURL, streamName, "cdc.>", rowCount, 20*time.Second)

	found := make(map[string]struct{}, rowCount)
	for _, evt := range events {
		email, ok := evt.After["email"].(string)
		if !ok || !strings.HasPrefix(email, bulkPrefix+"_") {
			continue
		}
		found[email] = struct{}{}
	}

	if len(found) != rowCount {
		t.Fatalf("expected %d bulk insert events, got %d", rowCount, len(found))
	}
}

func buildBulkAccountInsertSQL(prefix string, rowCount int) string {
	var sb strings.Builder
	sb.Grow(rowCount * 48)
	sb.WriteString("INSERT INTO accounts (email, status) VALUES ")
	for i := 0; i < rowCount; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("('%s_%03d@test.com', 'active')", prefix, i))
	}
	return sb.String()
}
