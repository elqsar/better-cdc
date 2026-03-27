# Production Readiness Issues

## Verdict

Not production-ready as-is.

The core CDC flow works and the current automated coverage is solid for the happy path, but there are still implementation and operational issues that should be addressed before a production rollout.

## Findings

### 1. High: async publish defaults are internally inconsistent

- The publisher hard-codes `PublishAsyncMaxPending(256)` in `internal/publisher/jetstream.go`.
- The shipped default `BATCH_SIZE` is `500` in `internal/config/config.go`, and the README documents the same default.
- Under slower JetStream acknowledgements or higher round-trip latency, batches larger than 256 can hit async stall errors instead of being a safe default.
- The integration suite does not exercise this path because tests start the engine with batch size `100`.

Code references:
- `internal/publisher/jetstream.go:83`
- `internal/config/config.go:61`
- `readme.md:53`
- `readme.md:122`
- `tests/integration/helpers_test.go:345`

### 2. Medium: slot feedback is not sent on idle receive timeouts

- The replication loops reset the deadline and continue on `pgconn.Timeout(err)`.
- Standby status updates are only sent when processing data messages or server keepalives.
- This means the implementation does not actually send a periodic heartbeat during idle periods.
- As a result, `confirmed_flush_lsn` can remain stale while the service is quiet, which is weaker than the operational behavior described in the README.

Code references:
- `internal/wal/reader.go:281`
- `internal/wal/reader.go:321`
- `internal/wal/reader.go:375`
- `internal/wal/reader.go:428`
- `readme.md:94`

### 3. Medium: health server startup failures are non-fatal

- `health.Start` launches `ListenAndServe` in a goroutine and only logs a warning if the bind fails.
- `main` immediately logs the health, readiness, and metrics endpoints as if they are available.
- In production, the service can therefore continue running without its health endpoints, which breaks orchestration and observability assumptions.

Code references:
- `internal/health/server.go:112`
- `cmd/cdc-handler/main.go:79`
- `cmd/cdc-handler/main.go:105`

### 4. Medium: `ALLOW_NOOP_PUBLISHER` is a production foot-gun

- When enabled, the service can start without NATS and silently drop all publishes.
- The readiness check treats the noop publisher as healthy because `NoopPublisher.Ready` always returns success.
- That is acceptable for local testing, but unsafe if accidentally enabled in a real deployment.

Code references:
- `cmd/cdc-handler/main.go:137`
- `cmd/cdc-handler/main.go:92`
- `internal/publisher/publisher.go:177`

## Additional Gaps

### Large transaction overflow path is intentionally weaker and not deeply tested

- When `MAX_TX_BUFFER_SIZE` is exceeded, the `pgoutput` parser switches to streaming mode and emits row events before commit metadata is available.
- This behavior is documented, but it weakens commit-boundary guarantees for oversized transactions.
- I did not find integration coverage specifically targeting this overflow mode.

Code references:
- `internal/parser/pgoutput.go:183`
- `internal/parser/pgoutput.go:264`
- `readme.md:107`

### Deployment hardening is out of scope of the current repo

- The repository does not include production deployment manifests, TLS termination, secret-management wiring, or CI/CD release gates.
- Those areas were not validated by this review and remain open for production readiness.

## Validation Performed

- `go test ./...`
- `go test -tags=integration -v -timeout=6m ./tests/integration/...`

Both test suites passed during the review, including checkpoint recovery, at-least-once recovery, truncate handling, and JetStream de-duplication scenarios.
