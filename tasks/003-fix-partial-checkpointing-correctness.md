# 003 - Fix partial-checkpoint correctness (avoid skipping failed events)

## Problem / Why
The engine currently checkpoints on *partial batch success* using the last successful item position.

This is unsafe if:
- an earlier item failed but a later item succeeded, and
- the checkpoint advances beyond the failed item’s LSN.

On restart, the system can skip the failed item entirely (data loss).

## Goal
If any item in a batch fails, the engine must not checkpoint past any failed event.

## Scope
Choose one of:
1) **Disable partial checkpointing** entirely (simplest and safest until de-dup exists).
2) Only checkpoint a **contiguous-success prefix**: checkpoint position of the last consecutively-acked item starting from the beginning of the batch.
3) Only checkpoint **on commit boundaries** and treat any failure in a transaction as “no checkpoint for that transaction”.

## Definition of done
- When there is any failure in a batch, the persisted checkpoint LSN is **<=** the earliest failed event’s position (never beyond).
- Add unit tests to cover:
  - success-failure-success pattern in a batch
  - failure at index 0
  - multiple failures
  - retry eventually succeeds
- Update metrics/logs to reflect batch partial failure without implying safe progress when it is not safe.


