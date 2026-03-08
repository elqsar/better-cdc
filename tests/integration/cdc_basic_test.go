//go:build integration

package integration

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

func TestBasicCDC(t *testing.T) {
	for _, plugin := range []string{"wal2json", "pgoutput"} {
		t.Run(plugin, func(t *testing.T) {
			connString, slotName := startPostgres(t, plugin)
			natsURL := startNATS(t)
			streamName := fmt.Sprintf("CDC_%s_%d", plugin, rand.Int64N(100000))

			cancel, _ := startEngine(t, engineConfig{
				ConnString: connString,
				SlotName:   slotName,
				Plugin:     plugin,
				NATSURLs:   []string{natsURL},
				StreamName: streamName,
			})
			defer cancel()

			waitForEngineReady(t, connString, natsURL, streamName, "cdc.>", 15*time.Second)

			// INSERT
			email := fmt.Sprintf("test_insert_%d@example.com", rand.Int64N(1_000_000))
			execSQL(t, connString, fmt.Sprintf(
				"INSERT INTO accounts (email, status) VALUES ('%s', 'active')", email,
			))

			events := consumeEvents(t, natsURL, streamName, "cdc.>", 1, 10*time.Second)
			found := false
			for _, evt := range events {
				if afterEmail, ok := evt.After["email"].(string); ok && afterEmail == email {
					found = true
					if evt.EventType != "cdc.insert" {
						t.Errorf("expected event_type cdc.insert, got %s", evt.EventType)
					}
					if evt.Schema != "public" {
						t.Errorf("expected schema public, got %s", evt.Schema)
					}
					if evt.Table != "accounts" {
						t.Errorf("expected table accounts, got %s", evt.Table)
					}
					if evt.EventID == "" {
						t.Error("expected non-empty EventID")
					}
					break
				}
			}
			if !found {
				t.Fatalf("INSERT event for %s not found in %d events", email, len(events))
			}

			// UPDATE
			execSQL(t, connString, fmt.Sprintf(
				"UPDATE accounts SET status = 'disabled' WHERE email = '%s'", email,
			))
			events = consumeEvents(t, natsURL, streamName, "cdc.>", 1, 10*time.Second)
			found = false
			for _, evt := range events {
				if evt.EventType == "cdc.update" {
					if afterStatus, ok := evt.After["status"].(string); ok && afterStatus == "disabled" {
						found = true
						break
					}
				}
			}
			if !found {
				t.Fatal("UPDATE event not found")
			}

			// DELETE
			execSQL(t, connString, fmt.Sprintf(
				"DELETE FROM accounts WHERE email = '%s'", email,
			))
			events = consumeEvents(t, natsURL, streamName, "cdc.>", 1, 10*time.Second)
			found = false
			for _, evt := range events {
				if evt.EventType == "cdc.delete" {
					found = true
					break
				}
			}
			if !found {
				t.Fatal("DELETE event not found")
			}

			// Cross-table: insert account then order
			crossEmail := fmt.Sprintf("cross_%d@example.com", rand.Int64N(1_000_000))
			execSQL(t, connString,
				fmt.Sprintf("INSERT INTO accounts (email, status) VALUES ('%s', 'active')", crossEmail),
			)
			// Wait for the account insert event
			consumeEvents(t, natsURL, streamName, "cdc.>", 1, 10*time.Second)

			// Get the account ID
			execSQL(t, connString,
				fmt.Sprintf(
					"INSERT INTO orders (account_id, total_cents, status) SELECT id, 4299, 'pending' FROM accounts WHERE email = '%s'",
					crossEmail,
				),
			)
			events = consumeEvents(t, natsURL, streamName, "cdc.>", 1, 10*time.Second)
			found = false
			for _, evt := range events {
				if evt.Table == "orders" && evt.EventType == "cdc.insert" {
					if tc, ok := evt.After["total_cents"]; ok {
						// total_cents can be float64 or int depending on parser
						switch v := tc.(type) {
						case float64:
							if v == 4299 {
								found = true
							}
						case int:
							if v == 4299 {
								found = true
							}
						}
					}
					break
				}
			}
			if !found {
				t.Fatal("cross-table orders INSERT event not found or total_cents mismatch")
			}
		})
	}
}
