# Pilot validation

Validated locally on macOS/arm64 with Go 1.26.8 and a Podman Linux VM.
The source fixture uses PostgreSQL 17; the broker fixture uses NATS 2.10.
This is a reproducible development baseline, not a production workload limit.

The test suites exercise unit/race checks, real executable termination before
feedback and around quarantine durable writes, replay outside the dedup window,
oversized-event inspect/redrive, full recovery storage, both decoder paths,
PostgreSQL and broker restarts, competing slot ownership, and mutual TLS with
rejected incorrect credentials and untrusted certificates.

The pinned vulnerability checker reports **zero reachable vulnerabilities** after
updating pgx to 5.9.2, x/text to 0.39.0 and the Go toolchain to 1.26.8. Its module
inventory still contains advisories in uncalled transitive code; the release gate
uses its standard symbol-reachability result.

## Library and identity-v2 validation — 2026-09-22

Verified on macOS/arm64 with Go 1.26.8 and the running Podman VM:

- `go test -race ./...` passed; affected constructor/configuration tests were
  rerun after the final default-resolution changes.
- `go vet ./...`, formatting, whitespace checks, and module tidiness passed.
- The full PostgreSQL 17 / NATS 2.10 integration suite passed (about 97 seconds).
- Affected integration tests also passed under the race detector: source identity,
  SIGKILL/redrive recovery, cancellation, incompatible consumers, concurrent
  runners, configuration snapshots, startup cleanup, and heartbeat/backpressure.
- The CI-pinned `golangci-lint` v2.12.2 reported zero issues.
- The CI-pinned `govulncheck` v1.1.4 found zero reachable vulnerabilities. It listed
  21 module-level advisories without affected imported packages/called symbols.

These checks establish regression coverage, not a production capacity ceiling.
Sustained representative workloads with a three-replica broker remain a deployment
validation step. No new throughput ceiling is claimed for this release.

## Historical ordered pipeline observation

The following numbers were recorded before the envelope-version-2/library change;
they are not a capacity claim for the current release. Reproduce with
`go test -tags=integration -run TestPilotOrderedCapacity -count=1 -v ./tests/integration`.
This uses the actual executable, `pgoutput`, one source transaction and a
file-backed, single-replica JetStream stream over the local Podman connection.

| Measurement | Observed |
|---|---:|
| Source rows | 1,000 |
| Added status value per row | 256 bytes |
| Average serialized event | 738 bytes |
| Insert start to all JetStream acknowledgements observed | 218 ms |
| Approximate throughput | 4,577 events/second |
| Commit-to-publish p50 | 121 ms |
| Commit-to-publish p95 | 194 ms |

The completion measurement includes a polling interval. Per-event latency uses
PostgreSQL commit timestamps and JetStream storage timestamps. Both containers
share the VM clock. It excludes consumer processing. Results will change with
broker RTT, three-replica durability, source data types, transaction size and
host contention. Use the same harness with representative data and collect
process memory and WAL/spill gauges during a sustained pilot before choosing
its workload ceiling.
