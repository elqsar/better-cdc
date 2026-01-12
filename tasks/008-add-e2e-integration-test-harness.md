# 008 - Add end-to-end integration test harness (docker compose)

## Problem / Why
The repo has unit tests, but there is no automated validation of the end-to-end CDC contract across Postgres → WAL → parser → engine → JetStream → checkpoint.

High-risk correctness issues (resume, duplicates, crash recovery) won’t be caught by unit tests alone.

## Goal
Add an integration test suite that validates behavior against real Postgres + NATS JetStream (and Redis for checkpoints).

## Scope
- Add `go test` integration tests under a build tag (e.g., `-tags=integration`) that:
  - boots dependencies via `docker compose` (or assumes they’re running and checks readiness)
  - runs the CDC handler as a subprocess
  - performs controlled SQL writes (insert/update/delete)
  - consumes from JetStream and validates received events and ordering constraints
  - restarts/kills the handler to validate resume semantics
- Keep runtime bounded (e.g., <2–3 minutes).

## Definition of done
- `go test -tags=integration ./...` passes locally with docker running.
- Tests cover:
  - basic change capture for `orders` and `accounts`
  - restart with checkpoints (no missed events)
  - failure injection (kill handler mid-stream) and recovery
  - duplicate behavior matches documented semantics (at-least-once; effectively-once if de-dup enabled)
- `readme.md` documents how to run integration tests.


