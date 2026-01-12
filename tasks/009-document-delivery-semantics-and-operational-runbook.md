# 009 - Document delivery semantics and an operational runbook

## Problem / Why
Production readiness depends on clear guarantees and operational guidance:
- at-least-once vs exactly-once/effectively-once
- expected duplicates and how consumers should handle them
- checkpointing and restart behavior
- monitoring and alerting

## Goal
Make the system operable and its guarantees explicit.

## Scope
- Add a “Delivery semantics” section: what is guaranteed, and under which configurations (e.g., with JetStream de-dup window).
- Add a minimal runbook:
  - required Postgres settings and permissions
  - slot/publication management
  - how to do safe deploys/restarts
  - metrics to watch and suggested alerts (lag, decode errors, ack failures, retries)
- Include a “Known limitations” section (e.g., large transactions, DDL handling, schema evolution expectations).

## Definition of done
- `readme.md` (or `docs/`) clearly states semantics and operational steps.
- Every config knob has a short explanation and default behavior.
- Includes example Prometheus queries for key SLOs (throughput, lag, error rates).

