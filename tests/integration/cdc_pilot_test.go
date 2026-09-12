//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"better-cdc/internal/publisher"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
)

func pilotBinary(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cdc-handler")
	cmd := exec.Command("go", "build", "-tags=integration", "-o", path, "./cmd/cdc-handler")
	cmd.Dir = projectRoot()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build executable: %v\n%s", err, out)
	}
	return path
}
func pilotEnvironment(t *testing.T, db, slot, url, stream, plugin string) []string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return append(os.Environ(), "DATABASE_URL="+db, "CDC_SLOT_NAME="+slot, "CDC_PLUGIN="+plugin, "NATS_URL="+url, "STREAM_NAME="+stream,
		"HEALTH_ADDR="+addr, "SPILL_DIR="+t.TempDir(), "DEBUG=false", "PUBLISH_FAILURE_POLICY=dlq", "CDC_DATABASE_NAME=postgres",
		"DLQ_STREAM_NAME="+stream+"_DLQ", "DLQ_BUCKET="+stream+"_RECOVERY", "DLQ_SUBJECT_PREFIX=cdc_dlq", "STREAM_SUBJECTS=cdc.>",
		"CDC_PUBLICATIONS=better_cdc_pub", "DUPLICATE_WINDOW=1s", "STREAM_STORAGE=file", "STREAM_REPLICAS=1", "STREAM_MAX_AGE=72h",
		"CHECKPOINT_INTERVAL=30s", "TABLE_FILTERS=", "NATS_CREDENTIALS_FILE=", "NATS_TLS_CA=", "NATS_TLS_CERT=", "NATS_TLS_KEY=")
}

type pilotProcess struct {
	cmd    *exec.Cmd
	done   chan error
	exited chan struct{}
	log    string
}

func startPilot(t *testing.T, binary string, env []string) *pilotProcess {
	t.Helper()
	log, err := os.CreateTemp(t.TempDir(), "process-*.log")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary)
	cmd.Env = env
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &pilotProcess{cmd: cmd, done: make(chan error, 1), exited: make(chan struct{}), log: log.Name()}
	go func() { p.done <- cmd.Wait(); _ = log.Close(); close(p.exited) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-p.exited:
		case <-time.After(10 * time.Second):
		}
	})
	return p
}
func waitPilotReady(t *testing.T, p *pilotProcess, env []string) {
	t.Helper()
	var addr string
	for _, v := range env {
		if strings.HasPrefix(v, "HEALTH_ADDR=") {
			addr = strings.TrimPrefix(v, "HEALTH_ADDR=")
		}
	}
	client := http.Client{Timeout: time.Second}
	eventuallyPilot(t, 15*time.Second, func() bool {
		select {
		case err := <-p.done:
			body, _ := os.ReadFile(p.log)
			t.Fatalf("pilot exited: %v\n%s", err, body)
		default:
		}
		resp, err := client.Get("http://" + addr + "/ready")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == 200
	})
}
func eventuallyPilot(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		if fn() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func pilotJS(t *testing.T, url string) nats.JetStreamContext {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	return js
}
func slotFlush(t *testing.T, db, slot string) string {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var lsn string
	if err = conn.QueryRow(context.Background(), "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&lsn); err != nil {
		t.Fatal(err)
	}
	return lsn
}

func TestPilotExecutableKillReplayAndLosslessDLQ(t *testing.T) {
	binary := pilotBinary(t)
	for _, plugin := range []string{"pgoutput", "wal2json"} {
		t.Run(plugin, func(t *testing.T) {
			db, slot := startPostgres(t, plugin)
			url := startNATS(t)
			js := pilotJS(t, url)
			stream := "PILOT"
			env := pilotEnvironment(t, db, slot, url, stream, plugin)
			p := startPilot(t, binary, env)
			waitPilotReady(t, p, env)
			baseline := slotFlush(t, db, slot)
			execSQL(t, db, `INSERT INTO public.accounts(email,status) SELECT 'pilot-'||n,'active' FROM generate_series(1,5) AS n`)
			eventuallyPilot(t, 10*time.Second, func() bool { info, err := js.StreamInfo(stream); return err == nil && info.State.Msgs == 5 })
			if current := slotFlush(t, db, slot); current != baseline {
				t.Fatalf("test missed pre-feedback crash window: %s -> %s", baseline, current)
			}
			if err := p.cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-p.done:
				if err == nil {
					t.Fatal("SIGKILL reported clean exit")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("kill did not terminate")
			}
			// Recovery outside the one-second dedup window must remain complete; duplicates are expected.
			time.Sleep(1100 * time.Millisecond)
			p2 := startPilot(t, binary, env)
			waitPilotReady(t, p2, env)
			eventuallyPilot(t, 10*time.Second, func() bool { info, err := js.StreamInfo(stream); return err == nil && info.State.Msgs >= 10 })
			ids := map[string]int{}
			for seq := uint64(1); seq <= 10; seq++ {
				msg, err := js.GetMsg(stream, seq)
				if err != nil {
					t.Fatal(err)
				}
				var evt struct {
					EventID string `json:"event_id"`
				}
				if err = json.Unmarshal(msg.Data, &evt); err != nil {
					t.Fatal(err)
				}
				ids[evt.EventID]++
			}
			if len(ids) != 5 {
				t.Fatalf("replay changed identities: %v", ids)
			}
			for id, count := range ids {
				if count != 2 {
					t.Fatalf("event %s count %d", id, count)
				}
			}
			// Force a permanent rejection while Object Store can still chunk the full row.
			info, err := js.StreamInfo(stream)
			if err != nil {
				t.Fatal(err)
			}
			info.Config.MaxMsgSize = 64 << 10
			if _, err = js.UpdateStream(&info.Config); err != nil {
				t.Fatal(err)
			}
			execSQL(t, db, `INSERT INTO public.accounts(email,status) VALUES ('oversized',repeat('x',200000))`)
			eventuallyPilot(t, 10*time.Second, func() bool { info, err := js.StreamInfo(stream + "_DLQ"); return err == nil && info.State.Msgs == 1 })
			indexMsg, err := js.GetMsg(stream+"_DLQ", 1)
			if err != nil {
				t.Fatal(err)
			}
			var index publisher.DeadLetterRecord
			if err = json.Unmarshal(indexMsg.Data, &index); err != nil {
				t.Fatal(err)
			}
			if len(indexMsg.Data) > 8<<10 {
				t.Fatal("index embeds the oversized payload")
			}
			inspect := exec.Command(binary, "dlq", "inspect", index.EventID)
			inspect.Env = env
			out, err := inspect.Output()
			if err != nil {
				t.Fatalf("inspect: %v", err)
			}
			var object publisher.RecoveryObject
			if err = json.Unmarshal(out, &object); err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(object.Payload, bytes.Repeat([]byte("x"), 200000)) {
				t.Fatal("quarantine payload truncated")
			}
			// Redrive failure must remain pending. It succeeds only after repairing the destination.
			redrive := exec.Command(binary, "dlq", "redrive")
			redrive.Env = env
			if out, err := redrive.CombinedOutput(); err == nil {
				t.Fatalf("oversized redrive unexpectedly succeeded: %s", out)
			}
			info, err = js.StreamInfo(stream)
			if err != nil {
				t.Fatal(err)
			}
			info.Config.MaxMsgSize = -1
			if _, err = js.UpdateStream(&info.Config); err != nil {
				t.Fatal(err)
			}
			redrive = exec.Command(binary, "dlq", "redrive")
			redrive.Env = env
			if out, err := redrive.CombinedOutput(); err != nil {
				t.Fatalf("redrive: %v\n%s", err, out)
			}
			eventuallyPilot(t, 5*time.Second, func() bool {
				info, err := js.ConsumerInfo(stream+"_DLQ", "redrive")
				return err == nil && info.NumPending == 0 && info.NumAckPending == 0
			})
			last, err := js.GetLastMsg(stream, "cdc.postgres.public.accounts")
			if err != nil {
				t.Fatal(err)
			}
			if last.Header.Get("Nats-Msg-Id") != index.EventID || !bytes.Equal(last.Data, object.Payload) {
				t.Fatal("redrive changed identity or payload")
			}
			if err = p2.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-p2.done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("graceful stop timed out")
			}
		})
	}
}

func TestPilotQuarantineCrashBoundaries(t *testing.T) {
	binary := pilotBinary(t)
	db, slot := startPostgres(t, "pgoutput")
	url := startNATS(t)
	js := pilotJS(t, url)
	for i, stage := range []string{"before_object", "object_verified", "index_acked"} {
		t.Run(stage, func(t *testing.T) {
			stream := fmt.Sprintf("CRASH_%d", i)
			// A separate subject prefix avoids overlap with another subtest's retained DLQ stream.
			env := pilotEnvironment(t, db, slot, url, stream, "pgoutput")
			env = append(env, "DLQ_SUBJECT_PREFIX="+strings.ToLower(stream)+"_dlq")
			marker := filepath.Join(t.TempDir(), "stage")
			pausedEnv := append(append([]string(nil), env...), "CDC_TEST_PAUSE_STAGE="+stage, "CDC_TEST_STAGE_FILE="+marker)
			p := startPilot(t, binary, pausedEnv)
			waitPilotReady(t, p, pausedEnv)
			info, err := js.StreamInfo(stream)
			if err != nil {
				t.Fatal(err)
			}
			info.Config.MaxMsgSize = 1024
			if _, err = js.UpdateStream(&info.Config); err != nil {
				t.Fatal(err)
			}
			baseline := slotFlush(t, db, slot)
			execSQL(t, db, fmt.Sprintf("INSERT INTO public.accounts(email,status) VALUES ('stage-%d',repeat('x',4000))", i))
			eventuallyPilot(t, 10*time.Second, func() bool { _, err := os.Stat(marker); return err == nil })
			if slotFlush(t, db, slot) != baseline {
				t.Fatal("checkpoint advanced before quarantine finished")
			}
			_ = p.cmd.Process.Kill()
			<-p.done
			restarted := startPilot(t, binary, env)
			waitPilotReady(t, restarted, env)
			eventuallyPilot(t, 10*time.Second, func() bool { info, err := js.StreamInfo(stream + "_DLQ"); return err == nil && info.State.Msgs >= 1 })
			indexMsg, err := js.GetMsg(stream+"_DLQ", 1)
			if err != nil {
				t.Fatal(err)
			}
			var index publisher.DeadLetterRecord
			if err = json.Unmarshal(indexMsg.Data, &index); err != nil {
				t.Fatal(err)
			}
			inspect := exec.Command(binary, "dlq", "inspect", index.EventID)
			inspect.Env = env
			out, err := inspect.Output()
			if err != nil {
				t.Fatal(err)
			}
			var object publisher.RecoveryObject
			if err = json.Unmarshal(out, &object); err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(object.Payload, bytes.Repeat([]byte("x"), 4000)) {
				t.Fatal("crash recovery lost payload")
			}
			_ = restarted.cmd.Process.Signal(syscall.SIGTERM)
			if err := <-restarted.done; err != nil {
				t.Fatal(err)
			}
			// The source slot is now current; remove only this test's live stream so
			// the next subtest can use the same source subject without overlap.
			if err := js.DeleteStream(stream); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPilotFullQuarantineDoesNotAcknowledge(t *testing.T) {
	binary := pilotBinary(t)
	db, slot := startPostgres(t, "pgoutput")
	url := startNATS(t)
	js := pilotJS(t, url)
	env := pilotEnvironment(t, db, slot, url, "FULL", "pgoutput")
	env = append(env, "DLQ_MAX_BYTES=1024")
	p := startPilot(t, binary, env)
	waitPilotReady(t, p, env)
	baseline := slotFlush(t, db, slot)
	info, err := js.StreamInfo("FULL")
	if err != nil {
		t.Fatal(err)
	}
	info.Config.MaxMsgSize = 1024
	if _, err = js.UpdateStream(&info.Config); err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, "INSERT INTO public.accounts(email,status) VALUES ('full',repeat('x',200000))")
	select {
	case err := <-p.done:
		if err == nil {
			t.Fatal("full quarantine did not fail")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("full quarantine did not stop producer")
	}
	if slotFlush(t, db, slot) != baseline {
		t.Fatal("full quarantine advanced checkpoint")
	}
	info, err = js.StreamInfo("FULL_DLQ")
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 0 {
		t.Fatal("partial object acquired an index record")
	}
}

func TestPilotRestartsAndSlotOwnership(t *testing.T) {
	binary := pilotBinary(t)
	db, slot, postgres := startPostgresContainer(t, "pgoutput")
	url, broker := startNATSContainer(t)
	js := pilotJS(t, url)
	env := pilotEnvironment(t, db, slot, url, "RESTART", "pgoutput")
	p := startPilot(t, binary, env)
	waitPilotReady(t, p, env)
	// A competing instance has healthy dependencies but must never report ready.
	otherEnv := pilotEnvironment(t, db, slot, url, "RESTART", "pgoutput")
	other := startPilot(t, binary, otherEnv)
	var addr string
	for _, v := range otherEnv {
		if strings.HasPrefix(v, "HEALTH_ADDR=") {
			addr = strings.TrimPrefix(v, "HEALTH_ADDR=")
		}
	}
	eventuallyPilot(t, 5*time.Second, func() bool {
		response, err := http.Get("http://" + addr + "/ready")
		if err != nil {
			return false
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode == 200 {
			t.Fatal("competing slot owner reported ready")
		}
		return response.StatusCode == 503
	})
	_ = other.cmd.Process.Kill()
	<-other.done
	execSQL(t, db, "INSERT INTO public.accounts(email,status) VALUES ('pre-restart','active')")
	eventuallyPilot(t, 5*time.Second, func() bool { info, err := js.StreamInfo("RESTART"); return err == nil && info.State.Msgs == 1 })
	if err := postgres.Stop(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := postgres.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitPilotReady(t, p, env)
	if err := broker.Stop(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := broker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitPilotReady(t, p, env)
	execSQL(t, db, "INSERT INTO public.accounts(email,status) VALUES ('post-restart','active')")
	eventuallyPilot(t, 10*time.Second, func() bool {
		msg, err := js.GetLastMsg("RESTART", "cdc.postgres.public.accounts")
		return err == nil && bytes.Contains(msg.Data, []byte("post-restart"))
	})
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	if err := <-p.done; err != nil {
		t.Fatal(err)
	}
}

// This is a reproducible local capacity observation, not a production throughput SLO.
func TestPilotOrderedCapacity(t *testing.T) {
	binary := pilotBinary(t)
	db, slot := startPostgres(t, "pgoutput")
	url := startNATS(t)
	js := pilotJS(t, url)
	env := pilotEnvironment(t, db, slot, url, "CAPACITY", "pgoutput")
	p := startPilot(t, binary, env)
	waitPilotReady(t, p, env)
	const count = 1000
	started := time.Now()
	execSQL(t, db, "INSERT INTO public.accounts(email,status) SELECT 'capacity-'||n,repeat('x',256) FROM generate_series(1,1000) n")
	eventuallyPilot(t, 30*time.Second, func() bool { info, err := js.StreamInfo("CAPACITY"); return err == nil && info.State.Msgs == count })
	elapsed := time.Since(started)
	var latency []time.Duration
	var totalBytes int
	for seq := uint64(1); seq <= count; seq++ {
		msg, err := js.GetMsg("CAPACITY", seq)
		if err != nil {
			t.Fatal(err)
		}
		var evt struct {
			CommitTime time.Time `json:"commit_time"`
		}
		if err = json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatal(err)
		}
		latency = append(latency, msg.Time.Sub(evt.CommitTime))
		totalBytes += len(msg.Data)
	}
	slices.Sort(latency)
	t.Logf("ordered pgoutput -> file JetStream, one replica, localhost Podman: rows=%d avg_event_bytes=%d duration=%s rows_per_second=%.0f commit_to_publish_p50=%s p95=%s", count, totalBytes/count, elapsed, float64(count)/elapsed.Seconds(), latency[count/2], latency[count*95/100])
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	if err := <-p.done; err != nil {
		t.Fatal(err)
	}
}
