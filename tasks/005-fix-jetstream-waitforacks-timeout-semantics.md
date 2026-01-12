# 005 - Fix `WaitForAcks` timeout semantics to avoid spurious failures/duplicates

## Problem / Why
The current `WaitForAcks` implementation can return a timeout error without correctly accounting for already-acked items (and the error message uses `result.Succeeded` before tallying).

This can trigger unnecessary retries and amplify duplicates.

## Goal
Make ack waiting:
- accurate (reflects what actually acked),
- predictable (timeouts clearly identify which items didn’t resolve),
- safe under cancellation.

## Scope
- On timeout, compute final `Succeeded` / `Failed` counts before building the error.
- Ensure per-item ack goroutines can’t leak indefinitely (bounded by context/timeout).
- Add unit tests for:
  - all-acked within timeout
  - partial ack then timeout
  - timeout with none acked
  - context cancellation

## Definition of done
- Timeout errors include correct resolved counts and do not misclassify already-acked items as failed.
- Engine retry behavior only retries items that truly did not ack (or had definite ack errors).
- Unit tests cover the scenarios above and are stable (no flakes).


