# Better CDC

A focused PostgreSQL logical-replication producer for NATS JetStream. One process
owns one existing replication slot. `pgoutput` is the default; `wal2json` remains
supported. This project does not implement other source or destination connectors.

## Local quickstart

Requires Go 1.26.8+, Task, and Podman with `podman-compose` (set
`COMPOSE="docker compose"` to use Docker instead). Integration tests need a
Docker-compatible socket, such as Podman's.

```sh
task up
task run
psql -h localhost -U postgres -d postgres -c \
  "insert into public.orders(account_id,total_cents,status) values (1,2999,'pending');"
nats --server localhost:4222 sub 'cdc.>'
```

The local stack provisions `better_cdc_pub` and a `pgoutput` slot. Existing slots
are never replaced automatically. A pre-existing `wal2json` slot must keep
`CDC_PLUGIN=wal2json`, or be migrated using an explicit bootstrap plan. Existing
rows are **not snapshotted**; only changes available from the slot are emitted.
The `nats sub` example observes live traffic; durable consumers should use JetStream.

## Delivery and recovery

The guarantee is **at least once**. Only a fully handled source transaction can
advance PostgreSQL feedback. A row is handled after JetStream acknowledges its
normal publication or its complete durable quarantine record. Malformed protocol,
invalid relation state, unknown actions, and unrecoverable storage errors stop CDC.

`Nats-Msg-Id` is the deterministic `event_id`. JetStream deduplicates within
`DUPLICATE_WINDOW`, not indefinitely. Consumers must deduplicate for recovery beyond
that window. Event IDs are stable for the same source, decoder, slot and table
selection. Changing decoder or capture configuration requires a migration decision.

Live publication waits for each acknowledgement before submitting the next event.
Replay can revisit older events. Source transactions are not applied atomically to
downstream consumers. `UNSAFE_UNORDERED_ASYNC_PUBLISH=true` explicitly gives up the
ordered live path and is not part of the pilot configuration.

The default `PUBLISH_FAILURE_POLICY=crash` stops on permanent event failures and
replays them after restart, so per-row order is never broken. Opt-in
`PUBLISH_FAILURE_POLICY=dlq` retains the full event in NATS Object Store,
verifies its checksum, then acknowledges a small record in a separate DLQ stream.
Only then may PostgreSQL advance. Objects and indexes have no automatic expiry and
reject new writes when their configured capacity is exhausted. They need durable
broker storage, monitoring, and an operator retention policy. A full/unavailable DLQ
stops CDC; it never authorizes dropping a change. Each quarantine write, including
the object upload, must finish within `DLQ_TIMEOUT`.

```sh
# Uses the same source/NATS environment as the producer.
go run ./cmd/cdc-handler dlq list
go run ./cmd/cdc-handler dlq inspect '<event_id>'
go run ./cmd/cdc-handler dlq redrive
```

Redrive uses a persistent consumer and the original event identity. It stops on the
first unsuccessful replay, naming that event, and acknowledges work only after a
publish acknowledgement. `dlq redrive --skip <event_id>` (repeatable) removes an
unrecoverable record from the redrive backlog so later records can proceed; it stays
in the index for `dlq list`/`inspect`.
Fix destination payload limits before replaying oversized events. Input capsules
for serialization/transform failures can be replayed after the underlying code fix.
Redrive arrives **after newer changes**; consumers must handle stale changes as well
as duplicates. Retained index records include successfully redriven records; inspect
the `redrive` consumer's pending and ack-pending counts for outstanding work.

`PUBLISH_FAILURE_POLICY=skip` intentionally
loses events and is outside the delivery guarantee. Transient failures stop the
engine after retries under every policy. `ALLOW_NOOP_PUBLISHER` is only for local
testing and always reports unready when the noop publisher is actually selected.

## Consumer contract

Envelope version 1 adds `schema_version` and `seq_in_tx` to the existing fields:
`event_id`, `event_type`, `source`, `timestamp`, `commit_time`, `lsn`, `txid`,
`schema`, `table`, `operation`, `before`, `after`, and `metadata`.

- Subjects are `cdc.{database}.{schema}.{table}`. Each identifier is encoded as UTF-8
  bytes with `%HH` escapes; letters, digits, `_` and `-` are unchanged. Decode each
  token once. Names containing dots, spaces, percent signs, Unicode, or wildcard
  characters cannot change the subject hierarchy. Empty identifiers use `%00`.
- Integers, decimals and nested JSON numbers retain their exact numeric values.
  Consumers must use a JSON decoder that preserves integer/decimal precision.
  Other known PostgreSQL types use pgx's JSON representation; unknown type OIDs
  retain PostgreSQL text. Unrepresentable values go to lossless quarantine.
- SQL NULL is JSON `null`; an empty string remains `""`. Before-images depend on
  replica identity and may contain only keys or be absent. Unchanged TOAST fields and non-key placeholders in key-only before-images
  are omitted and listed in `metadata.unavailable_before` / `unavailable_after`.
- `TRUNCATE` emits `operation=DDL`, `event_type=cdc.ddl`, and no row image. Other DDL
  is not emitted. Relation changes update decoding metadata; spilled records keep
  the relation revision under which they were received.
- Ordinals identify emitted row order within a transaction. `pgoutput` uses the
  commit LSN in event identity and transaction-end LSN for feedback. `wal2json`
  uses its message WAL position. No cross-decoder identity equivalence is promised.

## Configuration

All settings are environment variables. Defaults favor a local stack; use the
[production pilot example](deploy/pilot.env.example) for durable deployment.

| Setting | Default / meaning |
|---|---|
| `DATABASE_URL` | Local postgres/postgres connection; production should verify TLS |
| `CDC_DATABASE_NAME` | Derived from URL; stable source name and subject token |
| `CDC_SLOT_NAME` | `better_cdc_slot`; must already exist |
| `CDC_PLUGIN` | `pgoutput`; compatibility option `wal2json` |
| `CDC_PUBLICATIONS` | `better_cdc_pub`, comma-separated |
| `TABLE_FILTERS` | Empty means all; comma-separated `schema.table` allowlist |
| `BATCH_SIZE`, `BATCH_TIMEOUT` | `500`, `100ms`; zero size disables size-based flushing |
| `CHECKPOINT_INTERVAL` | `1s`; feedback interval for acknowledged commits |
| `RAW_MESSAGE_BUFFER_SIZE`, `PARSED_EVENT_BUFFER_SIZE` | `5000`, `5000`; count limits |
| `RAW_MESSAGE_BUFFER_BYTES`, `PARSED_EVENT_BUFFER_BYTES` | `67108864` each; byte backpressure |
| `MAX_TX_BUFFER_SIZE`, `MAX_TX_BUFFER_BYTES` | `100000` events, `67108864` accounted bytes; either triggers spill |
| `MAX_SPILL_BYTES` | `1073741824`; stop when a transaction exceeds this disk budget |
| `SPILL_DIR` | System temp `better-cdc-spill`; container uses `/var/lib/better-cdc/spill` |
| `NATS_URL` | `nats://localhost:4222`; comma-separated servers |
| `NATS_USERNAME`, `NATS_PASSWORD` | Optional basic credentials |
| `NATS_CREDENTIALS_FILE` | Optional `.creds` file; exclusive with basic credentials |
| `NATS_TLS_CA`, `NATS_TLS_CERT`, `NATS_TLS_KEY` | CA and optional client certificate/key; certificate and key must be paired |
| `NATS_TIMEOUT`, `MAX_PUBLISH_RETRIES` | `5s` per acknowledgement, `3` retries |
| `STREAM_NAME`, `STREAM_SUBJECTS` | `CDC`, `cdc.>` |
| `STREAM_STORAGE`, `STREAM_REPLICAS`, `STREAM_MAX_AGE` | `file`, `1`, `72h`; production example uses three replicas |
| `DUPLICATE_WINDOW` | `10m`; must match an existing stream; consumers still need replay deduplication |
| `PUBLISH_FAILURE_POLICY` | `crash`; alternatives `dlq` (reorders redriven rows), lossy `skip` |
| `DLQ_STREAM_NAME`, `DLQ_BUCKET` | `<STREAM_NAME>_DLQ`, `<STREAM_NAME>_RECOVERY` |
| `DLQ_SUBJECT_PREFIX` | `cdc_dlq`; must not overlap normal stream subjects |
| `DLQ_MAX_BYTES`, `DLQ_INDEX_MAX_BYTES` | `1073741824`, `67108864`; no expiry or eviction |
| `DLQ_TIMEOUT` | `1m`; deadline for one quarantine write, including the object upload |
| `PUBLISH_ASYNC_MAX_PENDING` | `max(256,BATCH_SIZE)`; primarily relevant to unsafe unordered mode |
| `HEALTH_ADDR` | `:8080`: `/health`, `/ready`, `/metrics` |
| `DEBUG`, `ENABLE_PPROF`, `ENABLE_PROFILING` | `false`; profiling is opt-in |

Byte budgets account conservatively for payloads and their decoded representations;
they are not a hard RSS limit. Keep container headroom for codecs, maps, transient
serialization copies, broker buffers and the Go runtime. A record larger than a
whole pipeline budget is admitted alone once that budget drains, logged, and counted
in `cdc_pipeline_oversized_records_total`; size memory headroom for it. Spill files
are private, source-specific and locked; a later process removes orphaned spill
files only after acquiring that directory lock, then replays from PostgreSQL.

For quoted identifiers containing commas or dots, use PostgreSQL publication
selection instead of the simple CSV table-filter syntax. A slot and its plugin
cannot be switched transparently. No automatic slot creation, deletion, skipping,
snapshotting or leader election is implemented.

## Development and pilot

```sh
task check
go test -race ./...
task test:integration    # isolated containers; five-minute suite timeout
task bench
podman build -t better-cdc:pilot .
```

Tests cover CRUD/truncate, checkpoint boundaries, exact numbers, schema-aware spill,
malformed input, capacity limits, ordered retries, real executable SIGKILL recovery,
and oversized-event quarantine/redrive. See [operations](docs/operations.md) for
provisioning, capacity, rollout and incident procedures. The supported pilot baseline
is PostgreSQL 17 with NATS 2.10; other versions need their own compatibility run.
