# 001 - Tie Postgres replication feedback to durable checkpoints

## Problem / Why
The WAL reader currently sends `StandbyStatusUpdate` (write/flush/apply positions) based on the latest received `WALStart` / keepalive position, not on the last *durably processed* commit.

This can cause **permanent data loss**:
- The replication slot can advance past changes that were read but not yet published durably to JetStream.
- A crash/restart can resume from a position that has already been acknowledged to Postgres, making those changes unrecoverable.

## Goal
Only acknowledge replication progress to Postgres when:
1) all events for a commit boundary have been durably handled (published + acked), and
2) the checkpoint for that commit LSN has been persisted.

## Scope
- Introduce a clear “acked LSN” concept: the highest commit LSN considered safe to acknowledge to Postgres.
- Ensure `sendStandbyStatus` uses the acked LSN, not the last received LSN.
- Keep the system responsive to server `ReplyRequested` keepalives.

## Proposed approach (one viable design)
- Add a small interface to the WAL reader, e.g. `Acknowledger` / `SetAckedPosition(pos model.WALPosition)` or an ack channel.
- Engine calls this after successful commit publish + checkpoint save.
- WAL reader keeps the most recent acked commit LSN and uses it for `StandbyStatusUpdate` positions.

## Definition of done
- The WAL reader **never** sends `StandbyStatusUpdate` with an LSN greater than the latest durably checkpointed commit LSN.
- Under a forced crash between “read WAL” and “publish/ack”, the system replays data after restart (at-least-once), and does not permanently lose events.
- Add unit tests around the “acked LSN” handling (e.g., it never regresses; it never exceeds last checkpoint).
- Update `readme.md` to state the expected delivery semantics post-change (at-least-once unless combined with de-dup).

## Verification
- Local run with `docker compose up -d`, generate changes, kill the process mid-stream, restart; verify all changes eventually appear on `cdc.>` subjects.


