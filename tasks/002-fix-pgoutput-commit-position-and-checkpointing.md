# 002 - Fix `pgoutput` commit position propagation and checkpointing

## Problem / Why
In the `pgoutput` parser, COMMIT markers currently do not populate `WALEvent.Position` (and BEGIN/COMMIT events are not pooled consistently). The checkpointer ignores empty LSNs, so `pgoutput` runs may not checkpoint at all.

This breaks restart correctness (always starting from "earliest" or from stale checkpoints).

## Goal
Make `pgoutput` mode checkpoint correctly at commit boundaries, using the commit LSN.

## Scope
- Ensure `Commit` WALEvents include `Position.LSN` (commit LSN).
- Ensure commit/begin marker allocation and lifecycle are consistent (avoid leaks / inconsistent pooling).
- Ensure the Engine checkpoints using commit positions (and doesn’t accidentally use begin LSNs).

## Definition of done
- In `pgoutput` mode, after processing commits, Redis checkpoint key advances to the latest commit LSN.
- After restart, the reader resumes from the checkpointed commit LSN (no “start from earliest” unless no checkpoint exists).
- Add/extend unit tests that feed a sequence of `pgoutput` messages and validate emitted events include `Position.LSN` on COMMIT and on buffered row events.

## Notes
If `pgoutput` "streaming mode" (buffer overflow) is kept, define explicitly how commit metadata/LSN is assigned to streamed events.


