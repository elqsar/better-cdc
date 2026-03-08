# 011 - Fix replication correctness and plugin parity

## Problem / Why
The CDC pipeline is functional, but some behavioral guarantees are still weaker or less consistent than the docs suggest:
- graceful restart semantics need to be documented and tested precisely
- `pgoutput` and `wal2json` are not fully feature-equivalent today
- truncate / DDL-like handling is only partially implemented
- documentation still overstates some guarantees relative to current behavior

## Goal
Align runtime behavior, tests, and docs around the actual replication guarantees, and close obvious parser parity gaps.

## Scope
- Re-review graceful shutdown and restart semantics, then tighten docs and tests to match the real guarantee.
- Decide and implement how truncate events should be handled under `pgoutput`, or explicitly document that they are unsupported.
- Add parser / integration coverage for any newly supported parity behavior.
- Add a “known limitations” section covering schema evolution, large transactions, truncate / DDL behavior, and duplicate expectations.
- Update delivery-semantics documentation so at-least-once and effectively-once claims are precise and not overstated.

## Definition of done
- Graceful restart behavior is covered by tests and described accurately in docs.
- `pgoutput` truncate behavior is either implemented and tested or explicitly documented as unsupported.
- Known limitations are documented in `readme.md` or `docs/`.
- The project’s stated guarantees match what the code and tests actually prove.

