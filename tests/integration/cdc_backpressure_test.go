//go:build integration

package integration

import (
	"context"
	"github.com/elqsar/better-cdc/internal/checkpoint"
	"github.com/elqsar/better-cdc/internal/wal"
	"github.com/jackc/pgx/v5"
	"testing"
	"time"
)

func TestBackpressureKeepsReplicationSession(t *testing.T) {
	db, slot := startPostgres(t, "pgoutput")
	execSQL(t, db, "ALTER SYSTEM SET wal_sender_timeout = '2s'", "SELECT pg_reload_conf()")
	conn, err := pgx.Connect(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	for _, mode := range []string{"channel", "budget"} {
		t.Run(mode, func(t *testing.T) {
			capacity, bytes := 0, int64(1<<20)
			if mode == "budget" {
				capacity, bytes = 8, 256
			}
			reader := wal.NewPGReader(wal.SlotConfig{DatabaseURL: db, SlotName: slot, Plugin: "pgoutput", Publications: []string{"better_cdc_pub"}, FeedbackInterval: 100 * time.Millisecond, MaxBufferBytes: bytes}, capacity, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			pos, err := checkpoint.NewSlotStore(db, slot).Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := reader.Start(ctx); err != nil {
				t.Fatal(err)
			}
			defer func() {
				stopCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
				defer done()
				if err := reader.Stop(stopCtx); err != nil {
					t.Error(err)
				}
			}()
			raw, err := reader.ReadWAL(ctx, pos)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case msg := <-raw:
				if msg == nil || !msg.Reset {
					t.Fatal("missing initial reset")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var before int
			if err := conn.QueryRow(ctx, "SELECT active_pid FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&before); err != nil {
				t.Fatal(err)
			}
			execSQL(t, db, "INSERT INTO accounts(email,status) VALUES ('blocked-"+mode+"@test','active')")
			// Deliberately do not drain the reader for longer than wal_sender_timeout.
			time.Sleep(3 * time.Second)
			var after int
			if err := conn.QueryRow(ctx, "SELECT active_pid FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&after); err != nil {
				t.Fatalf("replication disconnected under backpressure: %v", err)
			}
			if before != after {
				t.Fatal("session reconnected under backpressure")
			}
			if got := getConfirmedFlushLSN(t, db, slot); got != pos.LSN {
				t.Fatalf("unhandled transaction acknowledged: %s -> %s", pos.LSN, got)
			}
			// Draining releases the pending delivery/budget and capture resumes.
			for i := 0; i < 3; i++ {
				select {
				case msg, ok := <-raw:
					if !ok {
						t.Fatalf("reader stopped: %v", reader.Err())
					}
					if msg.Reset {
						t.Fatal("unexpected reconnect")
					}
					if msg.ReleaseBytes != nil {
						msg.ReleaseBytes()
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			cancel()
		})
	}
}
