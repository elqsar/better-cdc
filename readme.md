# Better CDC

A lightweight Go CDC handler that reads PostgreSQL logical replication changes and publishes normalized events to NATS JetStream. Comes with Docker-based local stack (Postgres + NATS JetStream) for quick end-to-end testing.

## Features
- PostgreSQL logical replication via `pgoutput` or `wal2json` (`wal2json` is the config default; `pgoutput` is the recommended production plugin).
- Deterministic event IDs, before/after images, commit timestamps.
- NATS JetStream publisher with automatic stream creation and publish retries.
- **High throughput pipeline** with configurable buffered channels and batch async publishing.
- Checkpoint recovery from PostgreSQL replication slot (`confirmed_flush_lsn`).
- Table allowlisting and publication support.
- Simple health endpoint and lightweight metrics logger.

## Quickstart (Local E2E)
1) Start infra (Postgres with logical replication, NATS):
   ```bash
   docker compose up -d
   ```

2) Run the CDC app:
   ```bash
   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
   export CDC_SLOT_NAME=better_cdc_slot
   export CDC_PLUGIN=pgoutput
   export CDC_PUBLICATIONS=better_cdc_pub
   go run ./cmd/cdc-handler
   ```

3) Generate changes:
   ```bash
   psql -h localhost -U postgres -d postgres \
     -c "insert into public.orders(account_id,total_cents,status) values (1,2999,'pending');"
   ```

4) Inspect NATS (JetStream) messages on the `CDC` stream (subjects `cdc.>`). For example using `nats` CLI:
   ```bash
   nats --server localhost:4222 sub 'cdc.>'
   ```
5) Debug logging: set `DEBUG=true` to enable verbose zap logging of WAL events, publishes, and checkpoints.

## Configuration
Environment variables (defaults in `internal/config`):

**Database & Replication:**
- `DATABASE_URL` (default `postgres://postgres:postgres@localhost:5432/postgres`)
- `CDC_SLOT_NAME` (default `better_cdc_slot`)
- `CDC_PLUGIN` (`pgoutput` | `wal2json`)
- `CDC_PUBLICATIONS` (comma-separated; default `better_cdc_pub`)
- `TABLE_FILTERS` (comma-separated `schema.table` allowlist)

**Batching & Throughput:**
- `BATCH_SIZE` (default `500`) - events per batch before flush
- `BATCH_TIMEOUT` (default `100ms`) - max time before flush
- `RAW_MESSAGE_BUFFER_SIZE` (default `5000`) - buffer between WAL reader and parser
- `PARSED_EVENT_BUFFER_SIZE` (default `5000`) - buffer between parser and engine

**Checkpoint:**
- `CHECKPOINT_INTERVAL` (default `1s`) - how often to advance replication slot feedback

**NATS:**
- `NATS_URL` (comma-separated; default `nats://localhost:4222`)
- `NATS_USERNAME`, `NATS_PASSWORD`
- `NATS_TIMEOUT` (default `5s`)
- `ALLOW_NOOP_PUBLISHER` (default `false`) - allows startup without NATS and drops publishes intentionally; use only for local testing

**JetStream Stream:**
- `STREAM_NAME` (default `CDC`)
- `STREAM_SUBJECTS` (comma-separated; default `cdc.>`)
- `STREAM_STORAGE` (`file` or `memory`; default `file`)
- `STREAM_REPLICAS` (default `1`)
- `STREAM_MAX_AGE` (default `72h`) - max retention age for messages
- `DUPLICATE_WINDOW` (default `2m`) - JetStream de-duplication window; publishes with the same `event_id` within this window are idempotent

**Other:**
- `HEALTH_ADDR` (default `:8080`)
- `DEBUG` (set `true` for verbose logging)
- `ENABLE_PPROF` (default `false`) - exposes `/debug/pprof/*` endpoints on the health server

## Architecture (high level)

```
PostgreSQL WAL ──► [buffer] ──► Parser ──► [buffer] ──► Engine ──► JetStream
                                                          │
                                                          ▼
                                                     Checkpoint
```

- **WAL Reader** (`internal/wal`): replication connection, `pgoutput`/`wal2json`, emits begin/commit markers and row changes; configurable output buffer for backpressure handling.
- **Parser** (`internal/parser`): decodes WAL messages into structured events; buffered output channel.
- **Transformer** (`internal/transformer`): builds normalized CDC events with deterministic IDs.
- **Publisher** (`internal/publisher`): connects to NATS JetStream, ensures stream exists (defaults: name `CDC`, subjects `cdc.>`). Supports batch async publishing for high throughput.
- **Engine** (`internal/engine`): orchestrates the pipeline; batches events by size/timeout/commit boundaries; uses async batch publishing when available.
- **Checkpoint Manager** (`internal/checkpoint`): on startup, reads `confirmed_flush_lsn` from the PostgreSQL replication slot. During operation, the `StandbyStatusUpdate` heartbeat advances the slot position in Postgres; the checkpoint `Save` is a no-op since Postgres already tracks the position durably.
- **Health/Metrics**: `/health` for liveness, `/ready` for dependency readiness, `/metrics` for Prometheus.

## Delivery semantics
- The engine checkpoints only on commit boundaries after publish acks succeed. PostgreSQL replication feedback is advanced only to that durable checkpoint, so the base guarantee is **at-least-once**.
- On a graceful shutdown, the engine flushes any pending durable checkpoint and sends a final `StandbyStatusUpdate` before closing the replication connection. If the slot was already current, `confirmed_flush_lsn` may not visibly move during shutdown; either way, restarting the same slot resumes from the last flushed checkpoint so already-acknowledged commits should not be replayed on a clean stop/start cycle.
- Unclean exits can still replay already-published commits: process crashes, host loss, network failures before feedback reaches PostgreSQL, and partial batch failures all fall back to **at-least-once** behavior.
- JetStream de-duplication is enabled by default: each message is published with `Nats-Msg-Id` set to the deterministic `event_id`. Duplicate publishes within the configured `DUPLICATE_WINDOW` (default 2 minutes) are silently discarded by JetStream, providing **effectively-once** delivery only within that window.
- Consumers that need exactly-once processing beyond the de-dup window must de-duplicate using the deterministic `event_id` (or an equivalent idempotency key).

## Known limitations
- Schema evolution is not tracked as first-class CDC. `ALTER TABLE` and most other DDL are not emitted as structured events, so consumers must tolerate shape changes during deploys.
- `TRUNCATE` is supported by both plugins and is emitted as `cdc.ddl` with empty `before` / `after` images. Other DDL is still unsupported.
- Large `pgoutput` transactions are buffered until commit. If `MAX_TX_BUFFER_SIZE` is exceeded, the parser switches to streaming mode to avoid unbounded memory growth; row events from that overflowed transaction can be published before commit metadata is available.
- Duplicate delivery remains possible after crashes, after publish retries, or after recovery outside the JetStream de-dup window.

## Throughput Tuning

The pipeline uses buffered channels and batch async publishing to achieve high throughput (target: 1-2k TPS).

**Buffer sizes** control backpressure between pipeline stages:
```bash
RAW_MESSAGE_BUFFER_SIZE=5000    # Increase for bursty workloads
PARSED_EVENT_BUFFER_SIZE=5000   # Increase if parser is faster than publisher
```

**Batch settings** control how events are grouped before publishing:
```bash
BATCH_SIZE=500       # Larger = higher throughput, more latency
BATCH_TIMEOUT=100ms  # Lower = less latency, more flushes
```

Set buffer sizes to `0` to revert to unbuffered (sequential) behavior for debugging.

## Notes / Tips
- Postgres in compose is configured with `wal_level=logical`, `max_wal_senders=10`, `max_replication_slots=10` and initializes schema, publication, and replication slot via `docker/postgres/init/001_init.sql`.
- If you change the publication/slot names, update both the init SQL and env vars.
- JetStream stream can be customized via `JetStreamOptions` (stream name, subjects); defaults target `cdc.*` topics.
- wal2json path now emits begin/commit markers so checkpoints persist; `pgoutput` remains the recommended plugin for RDS-like environments.

## Integration Tests

End-to-end tests validate the full CDC pipeline (Postgres WAL -> Parser -> Engine -> JetStream -> Checkpoint) using testcontainers-go. Requires Docker running.

```bash
go test -tags=integration -v -timeout=3m ./tests/integration/
```

Tests cover:
- **Basic CDC** (`TestBasicCDC`): INSERT/UPDATE/DELETE capture with both `wal2json` and `pgoutput` parsers, plus cross-table validation.
- **Checkpoint Recovery** (`TestCheckpointRecovery`): Graceful shutdown preserves the flushed checkpoint, and restart on the same slot resumes with only post-shutdown work.
- **At-Least-Once Recovery** (`TestRecoveryAtLeastOnce`): Hard-kill mid-stream, verify all events are captured across two engine runs.
- **Truncate Parity** (`TestTruncateCDC`): `TRUNCATE` is emitted as `cdc.ddl` for both `wal2json` and `pgoutput`.
- **JetStream Dedup** (`TestJetStreamDedup`): Duplicate messages with the same `event_id` are rejected by JetStream.

## Development
- Requires Go 1.23+
- Run `go test ./...` for unit tests.
- Run `go test -tags=integration -v -timeout=3m ./tests/integration/` for integration tests (requires Docker).
- Code lives under `internal/`; entrypoint `cmd/cdc-handler/main.go`.
