# Current project assessment

The project now exposes a focused producer library and a CLI wrapper. Both
pgoutput and wal2json remain supported; durable quarantine remains the default.
Unsafe unordered publishing, intentional skip, and local noop modes remain explicit
exceptions to the normal delivery/ordering guarantees.

## Review findings addressed

- Event IDs include an explicit stable source identity, slot, and decoder. Envelope
  version 2 exposes source_id; old recovery records preserve their original IDs.
- DLQ redrive waits for pending and acknowledgement-pending work instead of treating
  a one-second fetch timeout as completion. A terminated worker's delivery is retried.
- The replication connection owner sends feedback while waiting for raw-byte budget
  or channel capacity, and reconnects on feedback transport errors. Feedback never
  advances beyond handled transactions.
- The public API provides configuration, lifecycle, readiness, metrics, and event
  types. Signals, environment loading, HTTP serving, and profiling stay in the CLI.
- Runners own isolated metrics registries and source/slot spill locks. Shutdown joins
  pipeline workers, releases buffered reservations, and closes broker connections.

## Validation and remaining work

Regression coverage includes source separation in a shared stream, stable IDs
through spill/recovery, actual redrive process termination and restart, blocked WAL
feedback with a shortened sender timeout, and concurrent library instance isolation.
The existing CRUD, recovery, quarantine, TLS, and decoder suites remain release gates.
Unit/race checks, the full container suite, affected integration race tests, vet,
formatting, module tidiness, and CI-pinned lint/vulnerability checks passed. See
[validation](validation.md) for recorded checks and the historical throughput observation.

Before selecting production capacity limits, run a sustained workload with realistic
row sizes, transaction sizes, broker latency, and three-replica JetStream durability.
Track process memory, retained WAL, spill usage, and quarantine capacity. A local
single-broker benchmark does not establish those limits.

Snapshotting, automatic slot creation/deletion/advancement, leader election, alternate
connectors, and public pipeline extension interfaces remain outside this release.
The repository's older untracked findings.md is historical and has not been modified.
