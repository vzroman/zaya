# Transaction Refactoring Review

Spec: `docs/superpowers/specs/2026-04-16-transaction-refactoring-design.md`

This file tracks review comments as they are evaluated. Code changes are not made from this document directly; each item is first discussed and recorded here once agreed.

## Agreed Points

1. `zaya_transaction:single_node_commit/2` should only create a new monitored worker process when the single participating node is the local node (`N =:= node()`). For a remote single-node commit, `ecall:call/4` already provides the remote spawned execution context, so the remote handler should run the single-node worker logic directly instead of spawning a second worker process.
2. Keep commit scenario implementation blocks cohesive in `zaya_transaction`. Methods relevant only to single-node commit should remain under the `SINGLE NODE COMMIT` section; methods relevant only to multi-node commit should remain under `MULTI NODE COMMIT`; the same locality rule applies to other commit scenarios. Helpers shared by multiple scenarios may live in a separate shared/helper section after the scenario sections.
3. `zaya_transaction:multi_node_commit/2` must not use `?dbRefMod(DB)` when computing whether any participating DB is persistent. `?dbRefMod/1` reads the local opened DB ref from `persistent_term` and is valid only for local DBs; multi-node commit data may include DBs that are remote on the coordinator node. Use `?dbModule(DB)` from schema for this cross-node module lookup, then call `Module:is_persistent()`.

## Open Discussion

_None yet._

## Rejected / Deferred

_None yet._
