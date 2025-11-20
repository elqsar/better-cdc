# Better CDC

A lightweight Go CDC handler that reads PostgreSQL logical replication changes and publishes normalized events to NATS JetStream. Comes with Docker-based local stack (Postgres + NATS JetStream + Redis) for quick end-to-end testing.

## Features
- PostgreSQL logical replication via `pgoutput` (default) or `wal2json`.
- Deterministic event IDs, before/after images, commit timestamps.
- NATS JetStream publisher with automatic stream creation and publish retries.
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
Environment variables (defaults in `internal/config/defaults`):
- `DATABASE_URL` (default `postgres://postgres:postgres@localhost:5432/postgres`)
- `CDC_SLOT_NAME` (default `better_cdc_slot`)
- `CDC_PLUGIN` (`pgoutput` | `wal2json`)
- `CDC_PUBLICATIONS` (comma-separated; default `better_cdc_pub`)
- `TABLE_FILTERS` (comma-separated `schema.table` allowlist)
- `BATCH_SIZE`, `BATCH_TIMEOUT`
- `CHECKPOINT_INTERVAL`, `CHECKPOINT_KEY`, `CHECKPOINT_TTL`, `REDIS_URL`
- `NATS_URL` (comma-separated), `NATS_USERNAME`, `NATS_PASSWORD`, `NATS_TIMEOUT`
- `HEALTH_ADDR` (default `:8080`)

## Architecture (high level)
- WAL Reader (`internal/wal`): replication connection, `pgoutput`/`wal2json`, emits begin/commit markers and row changes; filters by table allowlist.
- Parser (`internal/parser`): pass-through placeholder.
- Transformer (`internal/transformer`): builds normalized CDC events with deterministic IDs.
- Publisher (`internal/publisher`): connects to NATS JetStream, ensures stream exists (defaults: name `CDC`, subjects `cdc.>`), publishes with retries/acks.
- Checkpoint Manager (`internal/checkpoint`): saves LSNs (Redis/in-memory) on commit boundaries.
- Health/Metrics: `/health` endpoint; simple periodic counters/gauges logger.

## Notes / Tips
- Postgres in compose is configured with `wal_level=logical`, `max_wal_senders=10`, `max_replication_slots=10` and initializes schema, publication, and replication slot via `docker/postgres/init/001_init.sql`.
- If you change the publication/slot names, update both the init SQL and env vars.
- JetStream stream can be customized via `JetStreamOptions` (stream name, subjects); defaults target `cdc.*` topics.
- wal2json path now emits begin/commit markers so checkpoints persist; `pgoutput` remains the recommended plugin for RDS-like environments.

## Development
- Requires Go 1.21+
- Run `go test ./...` (unit-level only; no integration harness yet).
- Code lives under `internal/`; entrypoint `cmd/cdc-handler/main.go`.
