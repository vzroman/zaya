# Transaction Review

This file tracks review comments after they are checked against the code.
Accepted points are recorded here for later implementation. Source code is not
changed during review intake.

## Accepted Points

1. Rename the multi-node transaction participation value from
   `all_participating_dbs_nodes` / `AllParticipatingDBsNodes` to `scope` /
   `Scope`. The current name is repeated through `zaya_transaction` worker
   request records and helper signatures, making an already argument-heavy
   protocol harder to scan. In this protocol, `scope` is a concise enough name
   for the DB-to-nodes participation list when used consistently.
2. Adjust `zaya_transaction:single_node_commit/2` so the extra monitored
   worker process is created only for the local single-node path
   (`N =:= node()`). For a remote single-node commit, `ecall:call/4` already
   provides the remote execution boundary and error transport, so the remote
   handler should run the single-node worker logic directly instead of calling
   `single_node_commit/1` and spawning a second process on the remote node.
3. Keep commit scenario implementation blocks cohesive in
   `zaya_transaction`. Helpers used only by one scenario should live under that
   scenario's section, for example single-node-only log helpers under
   `SINGLE NODE COMMIT` and multi-node-only worker/protocol helpers under
   `MULTI NODE COMMIT`. Helpers shared by multiple commit scenarios may live in
   a separate shared/helper section after the scenario sections.
4. In `zaya_transaction:multi_node_commit/2`, compute `AnyPersistent` with
   `?dbModule(DB)` instead of `?dbRefMod(DB)`. The persistence check only needs
   the DB module, and `?dbRefMod/1` reads the local opened DB ref/module pair
   from `persistent_term`, which may be undefined for DBs that participate in
   the transaction but are remote-only on the coordinator node. `?dbModule/1`
   reads schema metadata and is the correct cross-node module lookup before
   calling `Module:is_persistent()`.
5. Rename the multi-node worker request record from `#worker_request{}` to
   `#commit_request{}`. The record is used only as the argument shape for
   `zaya_transaction:commit_request/1` and its test mirror, so the new name
   better matches the exported commit request protocol without conflicting with
   an existing record.
6. Rename `any_persistent` / `AnyPersistent` to `is_persistent` /
   `IsPersistent` throughout the multi-node commit request protocol. Although
   the value is computed with `lists:any/2`, its protocol meaning is whether the
   commit request needs persistent recovery handling at all. `is_persistent`
   is shorter, reads as a boolean property of the commit request, and aligns
   with the storage module API `Module:is_persistent/0`.
7. Introduce a private `#ops{write, delete}` record and use it inside
   `#local_commit{}` for both the forward commit operation and the rollback
   operation. The current `#local_commit{}` shape has parallel fields
   `write` / `delete` and `rollback_write` / `rollback_delete`; grouping each
   pair into an `#ops{}` value makes the paired invariant explicit and reduces
   the chance of commit and rollback fields drifting apart.
8. Add a local-node fast path to `zaya_transaction:single_db_node_commit/2`:
   when the only participating node is the current node (`Node =:= node()`),
   call `single_db_node_commit(Data)` directly. The existing
   `single_db_node_commit/1` function is already the local commit handler, so
   routing the local case through `ecall:call/4` adds unnecessary call
   machinery while the remote case still needs `ecall`.
9. Rename `run_single_node_worker/2` to `do_single_node_commit/2`. With the
   accepted remote single-node change, the function may be used as the direct
   single-node commit implementation rather than only as a spawned worker body.
   The new name describes the operation and matches the module's private
   `do_*` helper style.
10. Replace the long `run_multi_node_coordinator/6` argument list with a
    private record that carries the multi-node coordinator context. The
    function currently receives `TRef`, commit data, participating nodes,
    per-node DB mapping, transaction scope, and persistence flag, then passes
    most of that state onward to worker spawning and rollback logging. This
    exceeds the local style guide's long-argument warning and represents one
    stable protocol context, so a record is clearer and less fragile.

## Rejected / Not Accepted

_None yet._

## Needs Clarification

_None yet._
