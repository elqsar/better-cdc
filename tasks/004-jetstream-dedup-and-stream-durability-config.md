# 004 - Add JetStream de-dup and explicit durability configuration

## Problem / Why
Retries and restarts can produce duplicate publishes. Today the publisher does not use JetStream de-duplication (Msg-Id) and the stream is created with minimal config.

This makes it hard to reason about delivery semantics and increases downstream burden.

## Goal
Enable practical effectively once behavior for a configured time window by:
- setting per-message de-duplication ids, and
- making stream durability settings explicit and configurable.

## Scope
- Set JetStream Msg-Id (e.g., `Nats-Msg-Id`) to `CDCEvent.EventID` for every publish (including batch path).
- Extend stream creation to configure, at minimum:
  - `Storage` (file/memory)
  - `Replicas`
  - `MaxAge` / limits as needed
  - `DuplicateWindow` (must be >= expected retry window)
- Add config/env vars for these options with safe defaults for local dev.

## Definition of done
- Publishing the same `EventID` twice within the configured duplicate window results in a single stored message in JetStream (verified via consumer/stream info).
- Stream creation is idempotent and does not fail if a stream exists but differs; define expected behavior (fail fast vs reconcile).
- Update `readme.md` with the new delivery semantics: “at-least-once without de-dup; effectively-once within duplicate window with de-dup enabled”.


