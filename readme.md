# Better CDC

A lightweight Go CDC handler that reads PostgreSQL logical replication changes and publishes normalized events to NATS JetStream. Comes with Docker-based local stack (Postgres + NATS JetStream + Redis) for quick end-to-end testing.

## Features
- PostgreSQL logical replication via `pgoutput` (default) or `wal2json`.
- Deterministic event IDs, before/after images, commit timestamps.
- NATS JetStream publisher with automatic stream creation and publish retries.
- **High throughput pipeline** with configurable buffered channels and batch async publishing.
- Checkpoint persistence (Redis by default, in-memory fallback).
- Table allowlisting and publication support.
- Simple health endpoint and lightweight metrics logger.

## Quickstart (Local E2E)
1) Start infra (Postgres with logical replication, NATS, Redis):
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
- `CHECKPOINT_INTERVAL` (default `1s`)
- `CHECKPOINT_KEY` (default `better-cdc:checkpoint`)
- `CHECKPOINT_TTL` (default `24h`)
- `REDIS_URL` (default `redis://localhost:6379`)

**NATS:**
- `NATS_URL` (comma-separated; default `nats://localhost:4222`)
- `NATS_USERNAME`, `NATS_PASSWORD`
- `NATS_TIMEOUT` (default `5s`)

**Other:**
- `HEALTH_ADDR` (default `:8080`)
- `DEBUG` (set `true` for verbose logging)

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
- **Checkpoint Manager** (`internal/checkpoint`): saves LSNs (Redis/in-memory) on commit boundaries only after all events are acknowledged.
- **Health/Metrics**: `/health` endpoint; periodic counters/gauges logger.

## Delivery semantics
- Postgres replication feedback is only advanced to the last *durably persisted* checkpoint, so a crash/restart can replay already-published events (**at-least-once**).
- Consumers that need exactly-once processing should de-duplicate using the deterministic `event_id` (or an equivalent idempotency key).

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

## Development
- Requires Go 1.23+
- Run `go test ./...` (unit-level only; no integration harness yet).
- Code lives under `internal/`; entrypoint `cmd/cdc-handler/main.go`.
