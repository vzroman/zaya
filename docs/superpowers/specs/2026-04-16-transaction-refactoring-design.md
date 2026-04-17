# Zaya Transaction Refactoring Design

## Goal

Centralize the transaction log at the zaya level and simplify backend modules. Currently each persistent backend (zaya_rocksdb, zaya_ets_rocksdb) maintains its own transaction log via `commit1/commit2/rollback`. This refactoring moves logging to a dedicated `zaya_transaction_log` process and reduces the backend API to three functions: `commit`, `prepare_rollback`, and `is_persistent`.

Single DB commits skip logging entirely — the backend guarantees atomicity on its own.

## Backend Module API

Each backend exports:

```erlang
-spec commit(Ref, Write :: [{Key, Value}], Delete :: [Key]) -> ok.
-spec prepare_rollback(Ref, Write :: [{Key, Value}], Delete :: [Key]) -> {RollbackWrite :: [{Key, Value}], RollbackDelete :: [Key]}.
-spec is_persistent() -> boolean().
```

Removed from backends: `commit1/3`, `commit2/2`, `rollback/2`, `/LOG` directories, `log` field in `#ref` records.

Rollback is performed by calling `Module:commit(Ref, RollbackWrite, RollbackDelete)` — same commit function with rollback data as input.

## Transaction Log (`zaya_transaction_log`)

### Process

A gen_server started as part of `zaya_sup`, one per node.

### Storage

A zaya_rocksdb instance with pool. Directory configurable via application env:

```erlang
{transaction_log, #{dir => "...", pool => #{...}}}
```

Defaults to `zaya:schema_dir() ++ "/TLOG"`.

### Log Entries

Two types of entries coexist in the same rocksdb instance:

**Rollback entries:** `{ {DB, TRef}, {RollbackWrite, RollbackDelete} }` — inverse operations to undo a committed DB within a transaction. Key is sext-encoded so entries for a DB are contiguous, enabling efficient prefix scans. All rollback entries for a transaction are written in a single atomic batch.

**Rollbacked entries:** `{ {rollbacked, TRef}, AllParticipatingDBsNodes }` — evidence that a transaction was rolled back. `AllParticipatingDBsNodes` has format `[{DB, [Node, ...]}, ...]` listing every DB and the nodes that hold a replica of it for this transaction. Used by `is_rollbacked/1` queries during recovery and as state for the GC sweep.

There is **no committed entry**. Absence of `{rollbacked, TRef}` on every reachable node implies the transaction was committed (see DB Open Flow recovery rules). The coordinator logs `{rollbacked}` on its own node before broadcasting `{rollback}`; each worker also logs `{rollbacked}` on its own node when it learns of a rollback decision.

### Rollbacked Entry Cleanup

Performed periodically by `zaya_transaction_log` (no synchronous mutation during `is_rollbacked` queries):

- **Skip `{rollbacked, TRef}` entirely while any `{{DB, TRef}, _}` rollback entry still exists locally.** Rollback-data entries are the live in-flight signal for a transaction; as long as they are present, the `{rollbacked}` marker is still load-bearing and must not be GC'd.
- Otherwise, for each `{rollbacked, TRef}` entry with `AllParticipatingDBsNodes = [{DB, [Node, ...]}, ...]`:
  - For each `(DB, Node)` pair, remove `Node` from the list when **either**:
    - `Node ∈ zaya:db_available_nodes(DB)` — DB is open and finished its log scan on `Node` (recovery resolved this TRef there), **or**
    - `Node ∉ zaya:all_nodes()` — the node has been decommissioned.
  - When a DB's node list becomes empty, drop the `(DB, _)` tuple.
  - When the entry's DB list becomes empty, delete the `{rollbacked, TRef}` entry.
- `zaya_db_srv` only adds a node to `db_available_nodes(DB)` **after** its log scan completes, so "available" implies "recovery for this DB has resolved every TRef it cared about on that node".

### API

```erlang
%% Direct batch write/delete to the log. Bypasses gen_server — calls
%% zaya_rocksdb:commit(Ref, Write, Delete) directly using Ref from persistent_term.
%% Keys are sext-encoded internally. Callers build semantic batches:
%%   write rollbacks:        commit([{{DB, TRef}, {RW, RD}}, ...], [])
%%   delete rollbacks:       commit([], [{DB1, TRef}, {DB2, TRef}, ...])
%%   log rollbacked:         commit([{{rollbacked, TRef}, AllParticipatingDBsNodes}], [])
%%   log rollbacked + delete rollbacks:
%%                           commit([{{rollbacked, TRef}, AllParticipatingDBsNodes}],
%%                                  [{DB1, TRef}, {DB2, TRef}, ...])
-spec commit(Write :: [{Key, Value}], Delete :: [Key]) -> ok.

%% Check if a transaction was rolled back. Used during DB open recovery.
%% Returns true iff a {rollbacked, TRef} entry exists locally (regardless of
%% the DB-list contents — the entry's mere presence is conclusive).
%% Reads the log Ref from persistent_term. If the gen_server has not yet
%% installed the Ref (boot still in progress), waits briefly and retries
%% until it can answer from disk; never returns "still scanning" or similar.
-spec is_rollbacked(TRef :: reference()) -> boolean().

%% Scan rollback entries for a specific DB during open.
%% For each {{DB, TRef}, {RW, RD}}: queries is_rollbacked(TRef) cluster-wide
%% via ecall:call_all_wait(zaya:all_nodes(), ...) and resolves per the
%% recovery rule order in DB Open Flow. Applies Callback when rolling back,
%% deletes the rollback entry afterward.
-spec rollback(DB :: atom(), Callback :: fun(({RW, RD}) -> ok)) -> ok.

%% Purge all entries (rollback + rollbacked) referencing a DB without
%% applying rollback. Used for recovered (copied) DBs and DB removal.
-spec purge(DB :: atom()) -> ok.

%% Enumerate transactions stuck in recovery (coordinator unreachable, no
%% peer evidence). Used together with the PENDING_TRANSACTIONS env override
%% for manual operator resolution. Returns the rollback record's TRef along
%% with the participation map and the action that PENDING_TRANSACTIONS would
%% trigger.
-spec list_pending_transactions() ->
        [{TRef :: reference(),
          AllParticipatingDBsNodes :: [{atom(), [node()]}],
          PendingAction :: rollback | commit | undefined}].
```

The gen_server manages: open/close the rocksdb instance, store Ref in `persistent_term`, periodic rollbacked-entry cleanup.

### Startup/Shutdown

- Startup: opens the rocksdb instance. Does NOT replay the log proactively — rollback happens lazily when each DB opens.
- Shutdown: closes the rocksdb instance and pool.

## Commit Flow in `zaya_transaction`

### Decision Tree (after `prepare_data` filters)

```
CommitData has 1 DB -> single DB commit
  1 node  -> ecall to that node, Module:commit(Ref, Write, Delete)
  N nodes -> ecall to all nodes, Module:commit(Ref, Write, Delete)

CommitData has N DBs -> multi DB commit
  1 node  -> single_node_commit (isolated worker process:
               spawn_monitor if local, ecall:call if remote;
               rollback + {rollbacked} written/deleted in atomic batches)
  N nodes -> multi_node_commit (distributed coordination)
```

### Single DB Commit

```erlang
Module:commit(Ref, Write, Delete)
```

No prepare_rollback, no log. Backend guarantees atomicity.

### Single Node Commit (N DBs, 1 Node)

The commit runs inside a dedicated, isolated process — not in the transaction owner. The owner dispatches as follows:

- Participating node is **local** (= owner's node): `spawn_monitor/3` a worker process on the local node, send it the full commit request, and wait for its `'DOWN'`.
- Participating node is **remote**: `ecall:call` to the remote node (as in the existing spec); the remotely-spawned handler plays the same role as the local worker.

**Why the spawned process must be isolated.** The worker is created with `spawn_monitor` (or `ecall:call`, which gives remote-spawn semantics), not `spawn_link`. It has no links, no registered name, and its PID is known only to the owner. External supervisors, upstream links, caller timeouts, and stray exit signals that tear down the owner cannot reach the worker — it sees no `EXIT` signals from anyone. This guarantees that the worker's `try/catch` completes its rollback-and-cleanup even when the owner is terminated mid-transaction.

**Worker flow** (runs inside the spawned process):

```
1. Classify: Persistent = [DB || is_persistent(DB)], NeedLog = length(Persistent) > 0.
2. prepare_rollback for each DB -> collect [{DB, {RW, RD}}, ...].
3. If NeedLog -> single-batch write of rollback entries AND the rollbacked marker:
     transaction_log:commit(
         [{{DB, TRef}, {RW, RD}} || ...]
         ++ [{{rollbacked, TRef}, [{DB, [node()]} || DB <- Persistent]}],
         []).
4. Commit each DB sequentially:
     try Module:commit(Ref, Write, Delete)
     catch ->
       Rollback all previously committed: Module:commit(Ref, RW, RD).
       If NeedLog -> single-batch delete of both:
         transaction_log:commit(
             [],
             [{DB, TRef} || ...] ++ [{rollbacked, TRef}]).
       exit(Error).
5. If NeedLog -> single-batch delete of both:
     transaction_log:commit(
         [],
         [{DB, TRef} || ...] ++ [{rollbacked, TRef}]).
6. exit(normal).
```

The owner waits for the worker's `'DOWN'`:

- `'DOWN' normal` → return `ok` to the caller.
- `'DOWN' other Reason` → re-raise `Reason` to the caller.

**Crash safety.** Writing both the rollback-data entries and the `{rollbacked}` marker in one atomic rocksdb batch at step 3, and deleting both in one atomic batch at step 4-catch or step 5, means every kill-9 (worker process or whole VM) during steps 3–5 lands on one of two consistent states:

| Kill timing | On-disk state | Recovery | Outcome |
|---|---|---|---|
| Before step 3 | nothing | nothing | DBs untouched, nothing to do |
| Between step 3 write and step 5 (any point during step 4) | both entries present | R1 → apply rollback | DBs returned to pre-commit state, consistent with "txn failed" from the owner's view (owner saw `'DOWN' killed` and re-raised error) |
| Mid step 5 (atomic batch) | both present or both absent | R1 or nothing | consistent either way |
| After step 5 | nothing | nothing | commit is durable; if kill happened before `exit(normal)` the owner sees an abnormal DOWN but the data is committed — this is the standard "commit durable / result-reporting racy" window every system has |

The rollback entries carry inverse operations for all persistent DBs; applying them to pre-commit data is an idempotent no-op, and applying them to partially-committed data reverses only the committed DBs — so R1 is safe at every intermediate point.

**GC safety.** While the worker is between step 3 and step 5, both the rollback entries and the `{rollbacked}` marker are on disk. Per §Rollbacked Entry Cleanup, GC is inhibited for any `{rollbacked, TRef}` entry while *any* `{{DB, TRef}, _}` rollback entry still exists locally — so the marker cannot be swept while the commit is in flight.

### Multi Node Commit (N DBs, N Nodes)

#### Data Passed to Workers

Each `commit_request` contains:

- `TRef` — transaction reference (created on coordinator via `make_ref/0`; `node(TRef)` returns the coordinator's node and is used as durable coordinator identity during recovery).
- `DBs` — this node's `[{DB, Module, Ref, Write, Delete}]`.
- `AllParticipatingDBsNodes` — `[{DB, [Node, ...]}, ...]` listing every DB and its replica nodes for this transaction. Embedded verbatim into any `{rollbacked, TRef}` entry the worker writes.
- `Coordinator` — coordinator PID (workers `erlang:monitor` it).

#### Coordinator (spawned process, monitored by owner)

```
1. Spawn a worker on every participating node, sending commit_request
   (TRef, this node's DBs, AllParticipatingDBsNodes, self()).

2. Wait for {commit1, confirm, Worker} from every worker, or 'DOWN'/error reply.

3. All workers confirmed (commit path):
   3.1. Broadcast {commit2, AllWorkers} to all workers.
   3.2. Wait until each worker has either replied {commit2, confirmed}
        or sent 'DOWN'. (DOWN here means the worker finished and exited;
        for the coordinator's purpose it is equivalent to confirmation.)
   3.3. Exit normal.

4. Any worker failed phase 1 (DOWN or error reply) — rollback path:
   4.1. transaction_log:commit(
            [{{rollbacked, TRef}, AllParticipatingDBsNodes}], []).
        — log {rollbacked} on the coordinator's node BEFORE broadcasting,
          so the rollback decision is durable and is_rollbacked/1 on this
          node will see it from the moment the broadcast leaves.
   4.2. Broadcast {rollback, AllWorkers} to all surviving workers.
   4.3. Wait for 'DOWN' from every worker.
   4.4. Exit with the original error (owner releases locks and re-raises).
```

Key properties:

- `{commit2, AllWorkers}` and `{rollback, AllWorkers}` carry the full list of participating worker PIDs so that any worker can broadcast peer-to-peer if the coordinator dies.
- The coordinator does **not** log on the commit path — committed is the default state in the absence of `{rollbacked, TRef}`.
- The coordinator logs `{rollbacked}` even when its own node has no participating DBs (coordinator-non-participant case). This is intentional: the coordinator is the source of truth and `is_rollbacked/1` on `node(TRef)` is the canonical check during recovery. One fsync per rollback only; the happy path never logs on the coordinator.

#### Worker

Single state machine. The worker monitors the coordinator from the start.

```
1. Start phase 1 (prepare_rollback + log rollbacks + per-DB Module:commit):
   1.1. prepare_rollback for each local DB -> collect [{DB, {RW, RD}}, ...].
   1.2. If any local DB is persistent ->
        transaction_log:commit([{{DB, TRef}, {RW, RD}} || ...], []).
   1.3. Commit each local DB sequentially: Module:commit(Ref, Write, Delete).

2. Phase 1 error (catch branch in step 1.3 or anywhere in step 1):
   2.1. Local rollback for any DB already committed in 1.3:
        Module:commit(Ref, RW, RD).
   2.2. If any persistent ->
        transaction_log:commit(
            [{{rollbacked, TRef}, AllParticipatingDBsNodes}],
            [{DB, TRef} || ...]).
        — single batch: log {rollbacked} + delete pending rollback entries.
   2.3. Exit with error (the coordinator sees 'DOWN' and goes to step 4).

3. Phase 1 ok:
   3.1. Reply {commit1, confirm, self()} to coordinator.
   3.2. Wait for {commit2, AllWorkers} | {rollback, AllWorkers} | coordinator 'DOWN'.

4. On {commit2, AllWorkers}:
   4.1. Reply {commit2, confirmed, self()} to coordinator.
   4.2. Wait for coordinator 'DOWN':
        4.2.1. 'DOWN' normal:
               * transaction_log:commit([], [{DB, TRef} || ...])  -- delete rollbacks
               * Exit normal.
        4.2.2. 'DOWN' other Reason (coordinator crashed after we confirmed):
               * Broadcast {committed, AllWorkers -- [self()]} to all peer workers
                 via direct PID send.
               * transaction_log:commit([], [{DB, TRef} || ...])  -- delete rollbacks
               * Exit normal.
               (No {committed} log entry — committed is the default.)

5. On {rollback, AllWorkers}:
   5.1. Broadcast {rollbacked, AllWorkers -- [self()]} to all peer workers.
   5.2. transaction_log:commit(
            [{{rollbacked, TRef}, AllParticipatingDBsNodes}], []).
        — log {rollbacked} BEFORE applying the rollback so recovery on this
          node will always see evidence even if the apply step crashes.
   5.3. Apply rollback: Module:commit(Ref, RW, RD) for each local DB.
   5.4. transaction_log:commit([], [{DB, TRef} || ...])  -- delete rollback entries.
   5.5. Exit normal. (Coordinator is also waiting for our 'DOWN' in 4.3.)

6. Coordinator 'DOWN' before {commit2} or {rollback} arrived — worker
   doesn't know the decision. Resolve cooperatively with peers:
   6.1. pg:join(zaya_transaction, TRef); pg:monitor(zaya_transaction, TRef).
   6.2. erlang:monitor_node(Node, true) for every Node in
        AllParticipatingDBsNodes (deduped).
   6.3. Wait for one of:
        6.3.1. {committed, _} from a peer:
               * Broadcast {committed, AllWorkers -- [self()]}.
               * transaction_log:commit([], [{DB, TRef} || ...])  -- delete rollbacks.
               * Exit normal.
        6.3.2. {rollbacked, _} from a peer:
               * Broadcast {rollbacked, AllWorkers -- [self()]}.
               * transaction_log:commit(
                     [{{rollbacked, TRef}, AllParticipatingDBsNodes}], []).
               * Apply rollback: Module:commit(Ref, RW, RD) for each local DB.
               * transaction_log:commit([], [{DB, TRef} || ...]).
               * Exit normal.
        6.3.3. pg {join} event OR {nodedown, Node}:
               Reclassify every participating node into one of three buckets
               (see "pg semantics" below):
                 * "don't know" — node is alive and its worker is in the pg group.
                 * "knows (will broadcast)" — node is alive and its worker is NOT
                   in the pg group; the worker has either decided and broadcast
                   already (we just haven't received it yet) or is about to.
                 * "down" — erlang:monitor_node reported nodedown for this node.
               6.3.3.1. If every participating node is "don't know" OR "down":
                        — consensus reached, no living peer can broadcast a
                          decision; the worker rolls back independently.
                        * transaction_log:commit(
                              [{{rollbacked, TRef}, AllParticipatingDBsNodes}], []).
                        * Apply rollback.
                        * transaction_log:commit([], [{DB, TRef} || ...]).
                        * Exit normal.
               6.3.3.2. Otherwise — at least one peer is "knows (will broadcast)":
                        wait for the next event (its broadcast, its nodedown,
                        or its pg join).
```

**Guard asymmetry (implementation note):** step 1.2 is guarded by "if any local DB is persistent" because `{{DB, TRef}, {RW, RD}}` entries only exist to replay inverse operations on *this* node's persistent data during recovery. The `{rollbacked, TRef}` writes in 5.2 / 6.3.2 / 6.3.3.1 have no such guard — the marker is cluster-wide evidence for peer `is_rollbacked/1` queries, and a worker with only non-persistent local DBs still contributes to that N-way redundancy. Mirror this reasoning as an inline comment at each unguarded write site in the code.

**pg semantics (per the consensus rule):** A worker joins `pg:join(zaya_transaction, TRef)` exactly when it enters step 6 — i.e. when it does not know the outcome. A worker that has decided (steps 4 or 5) is **not** in the group; it either already broadcast `{committed}` / `{rollbacked}` or is about to. So:

- **In group + alive** = "don't know" (still searching).
- **Not in group + alive** = "knows; broadcast pending or in flight" — wait for it.
- **Down** = node will not contribute; treat as if it had nothing to say.

This is why 6.3.3.1's check is "every node is don't-know-or-down": as long as one alive peer is *not* in the group, a broadcast is coming.

### Log vs. No-Log Decision

- All DBs non-persistent: prepare_rollback for all, keep rollback in memory only (no log). If node crashes, non-persistent data is lost anyway.
- Any persistent DB: prepare_rollback for all, log persistent rollbacks.

## DB Open Flow in `zaya_db_srv`

```
1. Module:open(Params) -> Ref.
2. If recovered (copied) DB -> zaya_transaction_log:purge(DB).
   Else -> zaya_transaction_log:rollback(
               DB,
               fun({RW, RD}) -> Module:commit(Ref, RW, RD) end).
         For each {{DB, TRef}, {RW, RD}} rollback entry:
           a. ecall:call_all_wait(zaya:all_nodes(),
                                  zaya_transaction_log, is_rollbacked, [TRef]).
              — all known cluster nodes, not just DB replicas, so that any
                node with evidence is reached. Non-participants harmlessly
                answer false.
           b. Decide per the rule order below; on a final decision either
              call Callback({RW, RD}) (rollback) or skip Callback (commit),
              then delete the rollback entry from the log.

   Recovery rule order (first match wins):
     R1. Any node (including the coordinator) replied true (rollbacked):
         -> apply Callback, delete entry.
     R2. The coordinator (node(TRef)) replied false AND no node replied true:
         -> delete entry without Callback (transaction was committed).
     R3. The coordinator was unreachable AND no node replied true:
         -> consult os:getenv("PENDING_TRANSACTIONS"):
              "COMMIT"   -> delete entry without Callback.
              "ROLLBACK" -> apply Callback, delete entry.
              unset/other -> wait (sleep) and retry the call_all_wait.

   (R1 deliberately wins over R2: a peer with {rollbacked, TRef} is
   conclusive, even if the coordinator's own log was GC'd or the coordinator
   restarted between logging and the query — see concern C2.)

3. If Callback throws -> retry loop (DB stays unavailable until rollback succeeds).
4. Add node() to zaya:db_available_nodes(DB) — only after the entire log
   scan completes. This is what allows the rollbacked-entry GC sweep to use
   "Node ∈ db_available_nodes(DB)" as proof that recovery on Node finished.
```

`is_rollbacked/1` on a freshly booted target may need to wait for that target's `zaya_transaction_log` gen_server to install the log Ref in `persistent_term`; the API blocks briefly and retries until it can read disk, then answers definitively.

DB remove calls `zaya_transaction_log:purge(DB)` to clean up orphaned entries.

`zaya:list_pending_transactions/0` enumerates TRefs currently stuck in R3's retry loop, so an operator can review and set `PENDING_TRANSACTIONS` accordingly.

## Changes by Repository

| Repo | Branch | Changes |
|------|--------|---------|
| zaya | `pool` (exists) | Refactor `zaya_transaction.erl` (new coordinator/worker FSMs, `{rollbacked}`-only logging, peer-to-peer broadcast on coordinator DOWN). Add `zaya_transaction_log` gen_server with `commit/2`, `is_rollbacked/1`, `rollback/2`, `purge/1`, `list_pending_transactions/0`, periodic rollbacked-entry GC. Update `zaya_sup` to start log process. Update `zaya_db_srv` open/remove flow with the recovery rule order and the "available only after scan" guarantee. Add `zaya:list_pending_transactions/0` and honour `PENDING_TRANSACTIONS` env. |
| zaya_rocksdb | `pool` (exists) | Remove `commit1/commit2/rollback/rollback_log`, `/LOG` dir, `log` from `#ref`. Add `prepare_rollback/3`, `is_persistent/0`. |
| zaya_ets_rocksdb | `pool` (exists) | Remove `commit1/commit2/rollback`, `/TLOG` dir, `log` from `#ref`. Add `prepare_rollback/3` (delegates to `zaya_ets`), `is_persistent/0`. |
| zaya_ets | `pool` (create) | Remove `commit1/commit2/rollback`. Add `prepare_rollback/3`, `is_persistent/0`. |
| zaya_ets_leveldb | `pool` (create) | Remove `commit1/commit2/rollback`. Add `prepare_rollback/3` (delegates to `zaya_ets`), `is_persistent/0`. |
| zaya_pterm_leveldb | `pool` (create) | Remove `commit1/commit2/rollback`. Add `prepare_rollback/3` (delegates to `zaya_pterm`), `is_persistent/0`. |
| zaya_leveldb | `pool` (create) | Remove `commit1/commit2/rollback/rollback_log`, `/LOG` dir, `log` from `#ref`. Add `prepare_rollback/3`, `is_persistent/0`. |
| zaya_pterm | `pool` (create) | Remove `commit1/commit2/rollback`. Add `prepare_rollback/3`, `is_persistent/0`. |
| zaya_pool | no changes | — |

## Accepted Trade-offs

- **Idempotent rollback on uncommitted data:** If a worker crashes between logging rollback entries (step 1.2) and the per-DB `Module:commit` (step 1.3), rollback entries exist but no data was committed. On recovery the rollback is applied — writing back the same values that are already there (idempotent, no harm).
- **Non-persistent in-memory rollback:** If a worker process errors (not crashes — try/catch prevents crashes) between committing two non-persistent DBs, rollback data is in memory and available for in-process recovery. Node crash loses non-persistent data regardless.
- **`prepare_rollback` overhead for non-persistent backends:** adds a read cost to multi-DB transactions involving ETS/pterm. Acceptable — multi-DB transactions are the uncommon path.
- **`{rollbacked}`-only logging; commit is the default:** The log records rollbacks, never commits. Absence of `{rollbacked, TRef}` on every reachable node implies committed (R2). This eliminates the `{committed}` log-entry GC complexity at the cost of one assumption: cluster-wide reachability of *some* node with evidence. The coordinator logs `{rollbacked}` on its own node before broadcasting `{rollback}`, and every rolling-back worker also logs `{rollbacked}` on its own node — so even with cascading failures the evidence is N-way redundant.
- **R1 wins over R2 (peer-evidence beats coordinator-not-found):** If a phase-1-failing worker logs `{rollbacked}` and exits before the coordinator finishes its own `{rollbacked}` write, recovery on a third node could see "coordinator not_found" + "peer rollbacked" simultaneously. The rule order `R1 > R2` accepts the rollback in this case (per concern C2). The reverse ordering would risk treating a rolled-back transaction as committed.
- **Coordinator's `not_found` overload (concern C4):** `not_found` from the coordinator can in principle mean (a) committed, (b) coordinator restarted and the entry was GC'd, (c) decision still in flight, or (d) — impossible by construction since `node(TRef)` is always the creator. (a) is the intended meaning. (b) is bounded by the GC scheme: a node is only removed from a `{rollbacked}` entry's per-DB list once it appears in `db_available_nodes(DB)` (recovery there finished) or is decommissioned, so any DB still scanning will see the entry. (c) — the in-flight race — is theoretically possible but practically negligible: a peer's reboot (minutes) is far longer than the coordinator's decide-and-fsync window (seconds). The risk is accepted; an in-memory active-TRef tracker can be added later if observed.
- **Coordinator-non-participant overhead:** When the coordinator's node has no participating DBs, its `{rollbacked}` log entry references DBs that don't live there. Intentional: the coordinator is the source of truth and `node(TRef)` is the canonical lookup. Cost is one fsync per rollback only; the happy path never logs on the coordinator.
- **Cluster-wide recovery query:** `is_rollbacked` is invoked on every known node, not just DB replicas, so any node with evidence is reached. Non-participants harmlessly answer false. One `ecall:call_all_wait` per stuck TRef during a DB's open scan (no batching of TRefs into a single call — per F15 resolution).
- **Stuck recovery via env override:** When the coordinator is unreachable AND no peer has `{rollbacked}` evidence, recovery loops on R3 until either evidence arrives or `PENDING_TRANSACTIONS` is set. The env is global per boot; operators inspect candidates via `zaya:list_pending_transactions/0`. Env stays set across restarts until the operator clears it.
- **O(N²) peer broadcasts:** Workers broadcast `{committed}` / `{rollbacked}` to all peers in steps 4.2.2, 5.1, 6.3.1, and 6.3.2. Acceptable for typical cluster sizes; revisit if N grows large. Erlang messages are cheap and rollback is the unhappy path.
- **Kill-9 safety:** Single-node multi-DB commits are fully kill-9-safe. Rollback entries and the `{rollbacked}` marker are written in one atomic batch before step 4, and deleted in one atomic batch on success — so any hard kill during the commit loop leaves both entries on disk, and R1 rolls back consistently. No extra fsync: the existing pre-commit batch just carries one more key. Multi-node transactions survive kill-9 of individual participants the same way (coordinator and each rolling-back worker write `{rollbacked}` on their own nodes, giving N-way redundancy). The one remaining gap is simultaneous kill-9 of every VM that holds in-flight state for a multi-node transaction — in that case no `{rollbacked}` reaches disk on any node and R2 treats the transaction as committed. Accepted; closing it would require the pre-commit-marker-plus-post-commit-delete pattern that single-node now uses on every participating node for every multi-node transaction.
- **Worker FSM test surface:** Six top-level worker states with nested sub-states yield a non-trivial test matrix (phase1-fail, commit2-then-DOWN-normal, commit2-then-DOWN-error, rollback-received, coordinator-DOWN-then-pg-consensus-rollback, coordinator-DOWN-then-peer-tells-committed, coordinator-DOWN-then-peer-tells-rollbacked). A dedicated FSM test suite is planned during implementation; not a design issue.
