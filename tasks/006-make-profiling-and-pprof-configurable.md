# 006 - Make profiling and pprof configurable (safe defaults)

## Problem / Why
The binary enables mutex/block profiling globally at startup, which can add overhead in production. The health server also exposes pprof endpoints unconditionally when enabled.

## Goal
Make profiling behavior explicit and safe by default.

## Scope
- Gate `runtime.SetBlockProfileRate` and `runtime.SetMutexProfileFraction` behind config (e.g., `DEBUG=true` or dedicated env vars).
- Consider gating pprof endpoints behind config (or bind to localhost by default for production).
- Document recommended settings for local debugging vs production.

## Definition of done
- Default run (no env vars) does not enable mutex/block profiling.
- Setting the chosen env var(s) enables profiling and pprof endpoints as expected.
- `readme.md` contains a short “Profiling” section with example env vars.


