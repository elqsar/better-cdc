# Operating the PostgreSQL → JetStream pilot

## Provision and validate

Run one producer per source slot and use a dedicated live stream, DLQ index stream,
and Object Store bucket. PostgreSQL enforces slot ownership; this service does not
provide leader election. A second instance is unready while waiting for the slot.

Provision PostgreSQL logical replication (`wal_level=logical`, sufficient senders
and slots), a publication listing captured tables, and a logical `pgoutput` slot.
Use a dedicated login with REPLICATION and the required database/schema privileges.
A DBA should create publications and slots; normal CDC startup never changes them.
Choose replica identity deliberately: default primary-key identity provides partial
before-images; `REPLICA IDENTITY FULL` provides more old data at additional WAL cost.

For this streaming-only pilot, existing rows are outside capture. Establish the
consumer's baseline before relying on changes. A newly created slot after lost WAL
cannot fill the gap; restore/reconcile the consumer explicitly.

Configure NATS file storage on persistent broker volumes. Three stream replicas
require a healthy cluster of at least three servers. Local Compose uses one broker
and a persistent volume for development; it is not a replicated production cluster.
Retain normal events longer than maximum downstream downtime. Quarantine resources
have no TTL and `DiscardNew`: capacity exhaustion stops capture rather than evicting
unrecovered events. Preserve these policies when managing streams externally.

The initial service account needs stream/bucket provisioning API permissions, live
and DLQ publish permissions, Object Store chunk/metadata read/write permissions, and
reply inbox access. The operator account additionally needs consumer management and
redrive permissions. Scope subjects to this source; never share recovery buckets
between independently configured sources. NATS URLs are redacted in logs; prefer
mounted credentials and CA/client certificate files, never secrets in images.

Copy `deploy/pilot.env.example` to an untracked `deploy/pilot.env`, supply real
endpoints and mounted secrets, then use `deploy/compose.pilot.yml`. The image runs
as UID/GID 65532 with a read-only root. Its persistent spill directory must be
writable by that user and private (0700); the image initializes this for a new
named volume. One process holds an exclusive advisory lock for its slot directory.

The sample container has 768 MiB memory headroom and a 1 GiB spill cap. Set broker
storage and PostgreSQL WAL limits against measured event sizes, transaction sizes,
outage duration and retained DLQ volume. Pipeline budgets bound accounted bytes,
not exact heap allocation. A record larger than a whole budget is admitted alone
after that budget drains, so peak memory is roughly the budget plus the largest
record; watch `cdc_pipeline_oversized_records_total`. Increase budgets only with
corresponding process headroom.

## Readiness and alerts

`/health` is process liveness. `/ready` requires this process's active replication
session, a recent successful slot poll, the live publisher and configured recovery
resources. Idle publications are healthy while replication keepalives continue;
no receive or acknowledgement progress for 90 seconds is stale. Do not restart a
healthy process just because no captured table has changed.

Monitor retained WAL, safe WAL capacity, slot state, last receive/ack times, spill
usage, quarantine events and recovery storage. Numeric LSN gauges are diagnostic
and may lose low bits in Prometheus floating-point samples; never use them as
recovery positions. Use PostgreSQL's exact LSN values for recovery decisions.

`monitoring/prometheus/alerts.yml` contains starter alerts. Adjust byte thresholds to
your configured budgets and route them to an operator. The recovery record count
and oldest timestamp include redriven records. For unresolved work, inspect the
persistent `redrive` consumer's pending/ack-pending counts (before its first use,
all index records are outstanding).

Inactive/quiet captured tables can still retain WAL generated elsewhere. Monitor
that growth; do not call `pg_replication_slot_advance` or acknowledge the server's
latest WAL position as a substitute for decoded commit progress. WAL retention
limits can invalidate a slot instead of protecting capture continuity.

## Quarantine and redrive

1. Inspect `cdc-handler dlq list` and `dlq inspect <event-id>` using the producer's
   environment. Treat the output and stored objects as source data, including any
   sensitive fields. Index entries contain checksums and deterministic object names.
2. Repair the root cause. An oversized event requires larger destination limits;
   a transform/serialization bug requires a corrected binary. Do not modify stored
   recovery bytes or identities to hide the failure.
3. Run one `cdc-handler dlq redrive` per source. Failed replay remains pending and
   the error names the blocking `event_id`. Redrive publishes original payload bytes
   when present; otherwise it reconstructs the event from its original plugin bytes
   and relation revision. If a record cannot be repaired, record the decision and run
   `dlq redrive --skip <event-id>`: it leaves the redrive backlog, stays in the index
   for audit, and downstream reconciliation for that change becomes manual.
4. Consumers deduplicate by `event_id` and reject/apply stale events according to
   their own business rules. Redrive does not restore source ordering or atomicity.
5. Verify the durable consumer has no pending work and reconcile downstream state.
   The command retains objects and index records after success for audit/recovery.

A crash between object upload and index acknowledgement leaves WAL unacknowledged;
replay reuses and verifies the object. A crash between index acknowledgement and
PostgreSQL feedback may duplicate the index after the dedup window. Redrive is
therefore also at least once. A crash after downstream acknowledgement but before
redrive acknowledgement may publish the same identity again.

There is deliberately no automatic deletion or unsafe garbage collection. Before
manual retention cleanup, stop producer and redrive, verify all targeted index
records are resolved and downstream reconciliation is complete, and retain a tested
backup. Only then remove the corresponding index records and objects. Increase
capacity first if this cannot be established. Orphaned objects and partial chunks
from abrupt uploads also consume capacity; never delete them while recovery or
publication is active. Never apply a TTL to the underlying bucket stream.

## Failure procedures

- **Database/broker outage:** transient reader errors reconnect with backoff; exhausted
  publish retries exit for the container supervisor to restart. Feedback remains at
  handled commit boundaries. Watch PostgreSQL disk while the destination is unavailable.
- **DLQ full/unavailable:** fix storage availability or increase both configured and
  actual resource capacity, then restart. Do not select `skip` to silence the alert.
- **Malformed input/unknown relation:** keep the slot; preserve logs and input fixtures,
  fix decoder support, then replay. A diagnostic log is not a substitute for the WAL.
- **Slot/plugin mismatch:** restart with the slot's existing decoder, or perform an
  explicit migration with a fresh consumer baseline. Do not drop the slot on startup.
- **Lost/invalidated slot:** stop and reconcile/resynchronize downstream data using an
  operator-controlled process. Provision a new slot only with an explicit baseline.
- **Spill limit:** free/expand private storage or increase the transaction budget with
  memory headroom. Restart from the slot. Orphan files are removed after exclusive
  lock acquisition and are never used as checkpoints.

## Rollout, rollback and capacity

Complete unit/race and integration checks before rollout. Start against a dedicated
pilot source/stream, check readiness, publish a known transaction, inspect exact
values and identities, then exercise graceful restart and quarantine/redrive.

Measure ordered publishing using representative row and transaction sizes and broker
RTT. Record events/second, p95 latency, memory, spill and retained WAL during bursts
and outages. Select a workload ceiling with headroom; do not claim high throughput
from internal batching or enable unordered async mode to meet an unmeasured target.

Contract version 1, percent-encoded subjects and lossless recovery replace the old
unversioned/diagnostic behavior. No deployed-consumer compatibility migration is
provided for that old contract. Existing `wal2json` slots need an explicit decoder
override. Before rollback, stop the producer and preserve exact slot and stream
state; an old binary cannot read the new recovery format and must not resume with
its lossy DLQ default. Prefer a forward fix or keep capture stopped with WAL retained.
