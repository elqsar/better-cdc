//go:build integration

package integration

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

func TestTruncateCDC(t *testing.T) {
	for _, plugin := range []string{"wal2json", "pgoutput"} {
		t.Run(plugin, func(t *testing.T) {
			connString, slotName := startPostgres(t, plugin)
			natsURL := startNATS(t)
			streamName := fmt.Sprintf("CDC_truncate_%s_%d", plugin, rand.Int64N(100000))

			cancel, _ := startEngine(t, engineConfig{
				ConnString: connString,
				SlotName:   slotName,
				Plugin:     plugin,
				NATSURLs:   []string{natsURL},
				StreamName: streamName,
			})
			defer cancel()

			waitForEngineReady(t, connString, natsURL, streamName, "cdc.>", 15*time.Second)

			execSQL(t, connString, "TRUNCATE TABLE public.orders")

			events := consumeEvents(t, natsURL, streamName, "cdc.>", 1, 10*time.Second)
			found := false
			for _, evt := range events {
				if evt.Schema == "public" && evt.Table == "orders" && evt.EventType == "cdc.ddl" && evt.Operation == "DDL" {
					found = true
					if len(evt.Before) != 0 {
						t.Fatalf("expected empty before image for truncate, got %+v", evt.Before)
					}
					if len(evt.After) != 0 {
						t.Fatalf("expected empty after image for truncate, got %+v", evt.After)
					}
					if evt.EventID == "" {
						t.Fatal("expected non-empty event ID for truncate event")
					}
					break
				}
			}
			if !found {
				t.Fatalf("truncate event not found for plugin %s", plugin)
			}
		})
	}
}
