//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cdc "github.com/elqsar/better-cdc"
	"github.com/elqsar/better-cdc/internal/model"
	"github.com/elqsar/better-cdc/internal/parser"
	"github.com/elqsar/better-cdc/internal/publisher"
	"github.com/elqsar/better-cdc/internal/transformer"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
)

func TestSourceIdentityAndRedriveRestart(t *testing.T) {
	url := startNATS(t)
	p := publisher.NewJetStreamPublisher(publisher.JetStreamOptions{URLs: []string{url}, StreamName: "CDC_identity", EnableDLQ: true}, nil)
	if err := p.Connect(); err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()
	js := pilotJS(t, url)
	capsule := &model.RecoveryChange{Version: 1, Plugin: "wal2json", WALStart: 42, LSN: "0/2A", TxID: 7,
		Data: []byte(`{"action":"I","xid":7,"schema":"public","table":"accounts","columns":[{"name":"id","value":9007199254740993}]}`)}
	evt, err := parser.RestoreChange(capsule)
	if err != nil {
		t.Fatal(err)
	}
	defer model.ReleaseWALEvent(evt)
	identities := []model.Identity{{SourceID: "a", Slot: "same_slot", Decoder: "wal2json"}, {SourceID: "b", Slot: "same_slot", Decoder: "wal2json"}}
	var records []publisher.DeadLetterRecord
	for _, identity := range identities {
		normalized, err := transformer.NewSimpleTransformer("postgres", identity).Transform(ctx, evt)
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(normalized)
		if err != nil {
			t.Fatal(err)
		}
		id := normalized.EventID
		model.ReleaseCDCEvent(normalized)
		for attempt := 0; attempt < 2; attempt++ {
			if err := p.Publish(ctx, "cdc.postgres.public.accounts", data, id); err != nil {
				t.Fatal(err)
			}
		}
		rec := publisher.DeadLetterRecord{Identity: identity, EventID: id, Database: "postgres", Schema: "public", Table: "accounts", Recovery: capsule}
		if err := p.Quarantine(ctx, "cdc_dlq", &rec); err != nil {
			t.Fatal(err)
		}
	}
	info, err := js.StreamInfo("CDC_identity")
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 2 {
		t.Fatalf("distinct sources or replay dedup failed: %d messages", info.State.Msgs)
	}
	// Persist a v1 capsule as well; the new executable must reconstruct its old ID.
	legacy := publisher.DeadLetterRecord{EventID: transformer.EventID(evt), Database: "postgres", Schema: "public", Table: "accounts", Recovery: capsule}
	if err := p.Quarantine(ctx, "cdc_dlq", &legacy); err != nil {
		t.Fatal(err)
	}
	if err := p.WalkDLQ(ctx, func(_ uint64, r publisher.DeadLetterRecord) error { records = append(records, r); return nil }); err != nil {
		t.Fatal(err)
	}
	for _, rec := range records {
		obj, err := p.LoadRecovery(ctx, rec)
		if err != nil {
			t.Fatal(err)
		}
		if obj.EventID != rec.EventID {
			t.Fatal("changed ID")
		}
	}
	tampered := records[0]
	tampered.Identity.SourceID = "wrong"
	if _, err := p.LoadRecovery(ctx, tampered); err == nil {
		t.Fatal("identity mismatch accepted")
	}
	// Shorten only the fixture's redelivery delay; production remains five minutes.
	if _, err := js.AddConsumer("CDC_identity_DLQ", &nats.ConsumerConfig{Durable: "redrive", FilterSubject: "cdc_dlq.>", AckPolicy: nats.AckExplicitPolicy, MaxAckPending: 1, AckWait: 4 * time.Second}); err != nil {
		t.Fatal(err)
	}
	binary := pilotBinary(t)
	env := pilotEnvironment(t, "postgres://postgres:postgres@localhost/postgres", "same_slot", url, "CDC_identity", "wal2json")
	// Match the fixture's default dedup window.
	env = append(env, "DUPLICATE_WINDOW=2m")
	stage := filepath.Join(t.TempDir(), "delivered")
	crashed := startPilot(t, binary, append(append([]string{}, env...), "CDC_TEST_PAUSE_STAGE=redrive_delivered", "CDC_TEST_STAGE_FILE="+stage), "dlq", "redrive")
	eventuallyPilot(t, 10*time.Second, func() bool { _, err := os.Stat(stage); return err == nil })
	if err := crashed.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-crashed.done
	restarted := startPilot(t, binary, env, "dlq", "redrive")
	select {
	case err := <-restarted.done:
		t.Fatalf("redrive exited before redelivery: %v", err)
	case <-time.After(1200 * time.Millisecond):
	}
	select {
	case err := <-restarted.done:
		if err != nil {
			body, _ := os.ReadFile(restarted.log)
			t.Fatalf("redrive: %v\n%s", err, body)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("redrive did not finish")
	}
	consumer, err := js.ConsumerInfo("CDC_identity_DLQ", "redrive")
	if err != nil {
		t.Fatal(err)
	}
	if consumer.NumPending != 0 || consumer.NumAckPending != 0 {
		t.Fatalf("unfinished recovery: %+v", consumer)
	}
	last, err := js.GetLastMsg("CDC_identity", "cdc.postgres.public.accounts")
	if err != nil {
		t.Fatal(err)
	}
	if last.Header.Get("Nats-Msg-Id") != legacy.EventID || !strings.Contains(string(last.Data), "9007199254740993") {
		t.Fatalf("legacy recovery changed: %s", last.Data)
	}
	// Cancellation with an outstanding delivery must never be reported as success.
	evt.SeqInTx++
	pending := publisher.DeadLetterRecord{EventID: transformer.EventID(evt), Database: "postgres", Schema: "public", Table: "accounts", Recovery: capsule}
	if err := p.Quarantine(ctx, "cdc_dlq", &pending); err != nil {
		t.Fatal(err)
	}
	sub, err := js.PullSubscribe("cdc_dlq.>", "redrive", nats.Bind("CDC_identity_DLQ", "redrive"))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	messages, err := sub.Fetch(1, nats.MaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	cancelCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	err = p.RedriveDLQ(cancelCtx, func(context.Context, *publisher.RecoveryObject) error {
		t.Fatal("pending delivery was not held")
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation returned %v", err)
	}
	if err := messages[0].AckSync(); err != nil {
		t.Fatal(err)
	}
	consumer.Config.MaxAckPending = 2
	if _, err := js.UpdateConsumer("CDC_identity_DLQ", &consumer.Config); err != nil {
		t.Fatal(err)
	}
	if err := p.RedriveDLQ(ctx, func(context.Context, *publisher.RecoveryObject) error { return nil }); err == nil {
		t.Fatal("incompatible consumer accepted")
	}

}

func TestIndependentLibraryInstances(t *testing.T) {
	db, slotA := startPostgres(t, "pgoutput")
	slotB := randomSlotName()
	createSlot(t, db, slotB, "pgoutput")
	url := startNATS(t)
	base := t.TempDir()
	runners := make([]*cdc.Runner, 2)
	cancels := make([]context.CancelFunc, 2)
	done := make([]chan error, 2)
	for i, slot := range []string{slotA, slotB} {
		cfg := cdc.DefaultConfig()
		cfg.Source.SourceID = fmt.Sprintf("source-%d", i)
		cfg.Source.SlotName = slot
		cfg.Source.DatabaseURL = db
		cfg.JetStream.NATSURLs = []string{url}
		cfg.JetStream.StreamName = "CDC_instances"
		cfg.Pipeline.SpillDir = base
		cfg.Pipeline.MaxTxBufferSize = 1
		cfg.Recovery.PublishFailurePolicy = "crash"
		r, err := cdc.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		runners[i] = r
		cfg.Source.Publications[0] = "mutation_after_new"
		cfg.JetStream.NATSURLs[0] = "nats://127.0.0.1:1"
		ctx, cancel := context.WithCancel(context.Background())
		cancels[i] = cancel
		done[i] = make(chan error, 1)
		go func() { done[i] <- r.Run(ctx) }()
		t.Cleanup(cancel)
		// Sequential startup avoids racing automatic stream creation, which is not
		// part of this test's independent lifecycle assertion.
		eventuallyPilot(t, 15*time.Second, func() bool { return r.Ready(context.Background()) == nil })
	}
	execSQL(t, db, "INSERT INTO accounts(email,status) VALUES ('instance-1@test','active'),('instance-2@test','active')")
	js := pilotJS(t, url)
	eventuallyPilot(t, 10*time.Second, func() bool { info, err := js.StreamInfo("CDC_instances"); return err == nil && info.State.Msgs == 4 })
	body := func(r *cdc.Runner) string {
		w := httptest.NewRecorder()
		r.MetricsHandler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
		return w.Body.String()
	}
	for _, r := range runners {
		if !strings.Contains(body(r), "cdc_engine_events_total 2\n") {
			t.Fatal("metrics shared between runners")
		}
	}
	cancels[0]()
	select {
	case err := <-done[0]:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("shutdown timed out")
	}
	if runners[0].Ready(context.Background()) == nil || runners[1].Ready(context.Background()) != nil {
		t.Fatal("stopping one affected other")
	}
	execSQL(t, db, "INSERT INTO accounts(email,status) VALUES ('instance-3@test','active')")
	eventuallyPilot(t, 10*time.Second, func() bool { return strings.Contains(body(runners[1]), "cdc_engine_events_total 3\n") })
	if !strings.Contains(body(runners[0]), "cdc_engine_events_total 2\n") {
		t.Fatal("stopped metrics changed")
	}
	cancels[1]()
	select {
	case err := <-done[1]:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("shutdown timed out")
	}
}

func TestLibraryStartupFailureReleasesSlotAndSpill(t *testing.T) {
	db, slot := startPostgres(t, "pgoutput")
	cfg := cdc.DefaultConfig()
	cfg.Source.SourceID = "failed-start"
	cfg.Source.DatabaseURL = db
	cfg.Source.SlotName = slot
	cfg.Pipeline.SpillDir = t.TempDir()
	cfg.JetStream.NATSURLs = []string{"nats://127.0.0.1:1"}
	runner, err := cdc.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := runner.Run(ctx); err == nil || ctx.Err() != nil {
		t.Fatalf("expected startup failure before deadline: %v", err)
	}
	if runner.Ready(ctx) == nil {
		t.Fatal("failed producer ready")
	}
	_, release, err := parser.PrepareSpillDir(cfg.Pipeline.SpillDir, cfg.Source.SourceID, slot)
	if err != nil {
		t.Fatal(err)
	}
	release()
	conn, err := pgx.Connect(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	eventuallyPilot(t, 3*time.Second, func() bool {
		var active bool
		err := conn.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active)
		return err == nil && !active
	})
}
