//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"better-cdc/internal/checkpoint"
	"better-cdc/internal/engine"
	"better-cdc/internal/model"
	"better-cdc/internal/parser"
	"better-cdc/internal/publisher"
	"better-cdc/internal/transformer"
	"better-cdc/internal/wal"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
)

// projectRoot returns the absolute path to the project root directory.
func projectRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}

// randomSlotName generates a unique slot name for test isolation.
func randomSlotName() string {
	return fmt.Sprintf("test_slot_%d", rand.Int64N(1_000_000))
}

// startPostgres boots a Postgres 17 container with wal2json, logical replication,
// init SQL, and creates a test-specific replication slot.
func startPostgres(t *testing.T, plugin string) (connString string, slotName string) {
	t.Helper()
	ctx := context.Background()
	root := projectRoot()

	dockerfilePath := filepath.Join(root, "docker", "postgres")
	initSQLPath := filepath.Join(root, "docker", "postgres", "init", "001_init.sql")

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    dockerfilePath,
			Dockerfile: "Dockerfile",
		},
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":      "postgres",
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_wal_senders=10",
			"-c", "max_replication_slots=10",
		},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      initSQLPath,
				ContainerFilePath: "/docker-entrypoint-initdb.d/001_init.sql",
				FileMode:          0644,
			},
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		).WithDeadline(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		// Drop slot before terminating to prevent WAL bloat
		if cs, err := buildConnString(context.Background(), container); err == nil {
			dropSlot(context.Background(), cs, slotName)
		}
		_ = container.Terminate(context.Background())
	})

	connString, err = buildConnString(ctx, container)
	if err != nil {
		t.Fatalf("build conn string: %v", err)
	}

	// Create a unique replication slot for this test
	slotName = randomSlotName()
	createSlot(t, connString, slotName, plugin)

	return connString, slotName
}

func buildConnString(ctx context.Context, container testcontainers.Container) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", err
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("postgres://postgres:postgres@%s:%s/postgres", host, port.Port()), nil
}

func createSlot(t *testing.T, connString, slotName, plugin string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		t.Fatalf("connect to create slot: %v", err)
	}
	defer conn.Close(ctx)

	query := fmt.Sprintf(
		"SELECT pg_create_logical_replication_slot('%s', '%s')",
		slotName, plugin,
	)
	_, err = conn.Exec(ctx, query)
	if err != nil {
		t.Fatalf("create replication slot %s: %v", slotName, err)
	}
}

func dropSlot(ctx context.Context, connString, slotName string) {
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	_, _ = conn.Exec(ctx, fmt.Sprintf(
		"SELECT pg_drop_replication_slot('%s')", slotName,
	))
}

// startNATS boots a NATS container with JetStream enabled.
func startNATS(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "nats:2.10-alpine",
		ExposedPorts: []string{"4222/tcp"},
		Cmd:          []string{"-js"},
		WaitingFor:   wait.ForListeningPort("4222/tcp").WithStartupTimeout(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start nats container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("nats host: %v", err)
	}
	port, err := container.MappedPort(ctx, "4222")
	if err != nil {
		t.Fatalf("nats port: %v", err)
	}
	return fmt.Sprintf("nats://%s:%s", host, port.Port())
}

// execSQL runs SQL statements against Postgres using a standard (non-replication) connection.
func execSQL(t *testing.T, connString string, stmts ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		t.Fatalf("execSQL connect: %v", err)
	}
	defer conn.Close(ctx)

	for _, stmt := range stmts {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("execSQL %q: %v", stmt, err)
		}
	}
}

// consumeEvents subscribes to a JetStream stream and collects CDC events.
func consumeEvents(t *testing.T, natsURL, streamName, filterSubject string, minCount int, timeout time.Duration) []model.CDCEvent {
	t.Helper()

	events := drainEvents(t, natsURL, streamName, filterSubject, timeout)
	if len(events) < minCount {
		t.Fatalf("consumeEvents: timed out waiting for %d events, got %d", minCount, len(events))
	}
	if minCount <= 0 || len(events) == minCount {
		return events
	}
	// These tests care about the most recent CDC activity after each SQL statement,
	// but each helper call uses a fresh consumer against a retained stream.
	return events[len(events)-minCount:]
}

// drainEvents collects all available events from a JetStream stream without a minimum count requirement.
func drainEvents(t *testing.T, natsURL, streamName, filterSubject string, timeout time.Duration) []model.CDCEvent {
	t.Helper()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("drainEvents connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("drainEvents jetstream: %v", err)
	}

	consumerName := fmt.Sprintf("test_drain_%d", rand.Int64N(1_000_000))
	deadline := time.After(timeout)
	var sub *nats.Subscription
	for sub == nil {
		select {
		case <-deadline:
			t.Fatalf("drainEvents: timed out waiting for stream %q", streamName)
		default:
		}
		sub, err = js.PullSubscribe(filterSubject, consumerName, nats.BindStream(streamName))
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
	}
	defer func() { _ = sub.Unsubscribe() }()

	var events []model.CDCEvent
	emptyFetches := 0

	for {
		select {
		case <-deadline:
			return events
		default:
		}

		msgs, err := sub.Fetch(100, nats.MaxWait(500*time.Millisecond))
		if err == nats.ErrTimeout || len(msgs) == 0 {
			emptyFetches++
			if emptyFetches >= 3 {
				return events
			}
			continue
		}
		if err != nil {
			t.Fatalf("drainEvents fetch: %v", err)
		}
		emptyFetches = 0
		for _, msg := range msgs {
			var evt model.CDCEvent
			if err := json.Unmarshal(msg.Data, &evt); err != nil {
				t.Fatalf("drainEvents unmarshal: %v", err)
			}
			events = append(events, evt)
			_ = msg.Ack()
		}
	}
}

// engineConfig holds configuration for starting a test engine instance.
type engineConfig struct {
	ConnString string
	SlotName   string
	Plugin     string
	NATSURLs   []string
	StreamName string
}

// startEngine boots the full CDC pipeline and returns a cancel function and error channel.
func startEngine(t *testing.T, cfg engineConfig) (context.CancelFunc, <-chan error) {
	t.Helper()
	logger, _ := zap.NewDevelopment()

	ctx, cancel := context.WithCancel(context.Background())

	reader := wal.NewPGReader(wal.SlotConfig{
		SlotName:     cfg.SlotName,
		Plugin:       cfg.Plugin,
		DatabaseURL:  cfg.ConnString,
		Publications: []string{"better_cdc_pub"},
		TableFilter:  nil,
	}, 5000, logger)

	var parse parser.Parser
	switch cfg.Plugin {
	case "pgoutput":
		parse = parser.NewPGOutputParser(parser.PGOutputConfig{
			TableFilter:     nil,
			Logger:          logger,
			BufferSize:      5000,
			MaxTxBufferSize: 100000,
		})
	default:
		parse = parser.NewWal2JSONParser(parser.Wal2JSONConfig{
			TableFilter: nil,
			Logger:      logger,
			BufferSize:  5000,
		})
	}

	trans := transformer.NewSimpleTransformer("postgres")

	streamName := cfg.StreamName
	if streamName == "" {
		streamName = "CDC"
	}
	pub := publisher.NewJetStreamPublisher(publisher.JetStreamOptions{
		URLs:            cfg.NATSURLs,
		ConnectTimeout:  5 * time.Second,
		PublishTimeout:  5 * time.Second,
		StreamName:      streamName,
		StreamSubjects:  []string{"cdc.>"},
		StreamStorage:   "memory",
		StreamReplicas:  1,
		StreamMaxAge:    1 * time.Hour,
		DuplicateWindow: 30 * time.Second,
	}, logger)

	store := checkpoint.NewSlotStore(cfg.ConnString, cfg.SlotName)
	ckpt := checkpoint.NewManager(store, 1*time.Second, logger)

	eng := engine.NewEngine(reader, parse, trans, pub, ckpt, "postgres", 100, 100*time.Millisecond, logger)

	startPos, err := store.Load(ctx)
	if err != nil {
		logger.Warn("failed to load checkpoint", zap.Error(err))
	}
	ckpt.Init(startPos, time.Now())

	doneCh := make(chan error, 1)
	go func() {
		defer close(doneCh)
		doneCh <- eng.Run(ctx, startPos)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-doneCh:
		case <-time.After(10 * time.Second):
			t.Log("engine did not stop within 10s")
		}
	})

	return cancel, doneCh
}

// waitForEngineReady inserts a sentinel row and polls NATS until it appears,
// confirming the engine is connected and capturing changes.
func waitForEngineReady(t *testing.T, connString, natsURL, streamName, subject string, timeout time.Duration) {
	t.Helper()
	sentinel := fmt.Sprintf("_sentinel_%d@test.com", rand.Int64N(1_000_000))

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("waitForEngineReady connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("waitForEngineReady jetstream: %v", err)
	}

	// Insert sentinel after connecting to NATS (but before subscribing)
	execSQL(t, connString, fmt.Sprintf(
		"INSERT INTO accounts (email, status) VALUES ('%s', 'sentinel')", sentinel,
	))

	deadline := time.After(timeout)

	// Retry subscribing until the stream exists (engine creates it on Connect)
	consumerName := fmt.Sprintf("test_ready_%d", rand.Int64N(1_000_000))
	var sub *nats.Subscription
	for sub == nil {
		select {
		case <-deadline:
			t.Fatalf("waitForEngineReady: timed out waiting for stream %q to be created", streamName)
		default:
		}
		sub, err = js.PullSubscribe(subject, consumerName, nats.BindStream(streamName))
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
	}
	defer func() { _ = sub.Unsubscribe() }()

	for {
		select {
		case <-deadline:
			t.Fatalf("waitForEngineReady: timed out after %v waiting for sentinel %q", timeout, sentinel)
		default:
		}

		msgs, err := sub.Fetch(10, nats.MaxWait(500*time.Millisecond))
		if err != nil && err != nats.ErrTimeout {
			continue
		}
		for _, msg := range msgs {
			_ = msg.Ack()
			var evt model.CDCEvent
			if err := json.Unmarshal(msg.Data, &evt); err != nil {
				continue
			}
			if email, ok := evt.After["email"].(string); ok {
				if email == sentinel {
					return
				}
			}
		}
	}
}

// getConfirmedFlushLSN queries the confirmed_flush_lsn for a replication slot.
func getConfirmedFlushLSN(t *testing.T, connString, slotName string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		t.Fatalf("getConfirmedFlushLSN connect: %v", err)
	}
	defer conn.Close(ctx)

	var lsn *string
	err = conn.QueryRow(ctx,
		"SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
		slotName,
	).Scan(&lsn)
	if err != nil {
		t.Fatalf("getConfirmedFlushLSN query: %v", err)
	}
	if lsn == nil {
		return ""
	}
	return *lsn
}

// pollConfirmedFlushLSN polls until confirmed_flush_lsn changes from initialLSN or timeout.
func pollConfirmedFlushLSN(t *testing.T, connString, slotName, initialLSN string, timeout time.Duration) string {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			current := getConfirmedFlushLSN(t, connString, slotName)
			if current != initialLSN {
				return current
			}
			t.Logf("confirmed_flush_lsn did not advance from %s within %v", initialLSN, timeout)
			return current
		default:
		}

		current := getConfirmedFlushLSN(t, connString, slotName)
		if current != initialLSN {
			return current
		}
		time.Sleep(200 * time.Millisecond)
	}
}
