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

**Committed entries:** `{ {committed, TRef}, FailedPersistentNodesDBs }` — evidence that a transaction was committed. `FailedPersistentNodesDBs` has format `[{Node, [DB, ...]}, ...]` listing nodes/DBs that may have orphaned rollback entries needing cleanup.

### Committed Entry Cleanup

Performed two ways:

- **On request:** `is_committed(TRef, Node, DB)` — returns `true` if a `{committed, TRef}` entry exists containing `{Node, DB}`. On match, removes `{Node, DB}` from the entry (removes the entry entirely if no nodes/DBs remain).
- **Periodically:** For each `{committed, TRef}` entry, check every `{Node, DB}` pair — query the remote node for a corresponding rollback entry. If no rollback entry exists, remove `{Node, DB}` from the committed entry. Remove the entire entry when empty.

### API

```erlang
%% Write rollback entries for all persistent DBs in a transaction (single atomic batch)
-spec write(TRef, [{DB, {RollbackWrite, RollbackDelete}}]) -> ok.

%% Delete rollback entries for a transaction (single atomic batch)
-spec delete(TRef, [DB]) -> ok.

%% Delete rollback entries and log committed evidence in a single atomic batch.
%% Used by workers during crash recovery broadcasting.
-spec delete_and_log_committed(TRef, [DB], FailedPersistentNodesDBs) -> ok.

%% Check if a transaction was committed. Used during DB open recovery.
%% Returns true if {committed, TRef} entry exists for the given Node/DB.
%% Cleans up the Node/DB from the entry on match.
-spec is_committed(TRef, Node, DB) -> boolean().

%% Scan rollback entries for a specific DB during open.
%% For each {TRef, {RW, RD}}: queries is_committed on participating nodes.
%% If committed -> deletes rollback entry. Otherwise -> calls Callback, then deletes.
-spec rollback(DB, Callback) -> ok.

%% Purge all entries for a DB without applying rollback.
%% Used for recovered (copied) DBs and DB removal.
-spec purge(DB) -> ok.
```

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
  1 node  -> single_node_commit (Phase 1+2 in one call)
  N nodes -> multi_node_commit (distributed coordination)
```

### Single DB Commit

```erlang
Module:commit(Ref, Write, Delete)
```

No prepare_rollback, no log. Backend guarantees atomicity.

### Single Node Commit (N DBs, 1 Node)

Runs entirely in one call (local or remote via ecall). No spawned coordinator — the transaction owner or remote process does everything directly.

```
1. Classify: Persistent = [DB || is_persistent(DB)], NeedLog = length(Persistent) > 0
2. prepare_rollback for each DB -> collect {DB, {RW, RD}}
3. If NeedLog -> transaction_log:write(TRef, PersistentRollbacks)
4. Commit each DB sequentially:
     try Module:commit(Ref, Write, Delete)
     catch ->
       rollback all previously committed: Module:commit(Ref, RW, RD)
       if NeedLog -> transaction_log:delete(TRef, DBs)
       throw error
5. If NeedLog -> transaction_log:delete(TRef, DBs)
```

### Multi Node Commit (N DBs, N Nodes)

#### Data Passed to Workers

Each `commit_request` contains:

- `TRef` — transaction reference
- `DBs` — this node's `[{DB, Module, Ref, Write, Delete}]`
- `AllNodesDBs` — `#{ Node => #{ DB => IsPersistent } }` for all participating nodes
- `Coordinator` — coordinator PID

#### Coordinator (spawned process, monitored by owner)

```
Phase 1:
  Spawn worker on each node with that node's DBs
  Wait for {confirm, Worker} from all workers
  If any worker fails (DOWN) ->
    send {rollback, Workers} to all confirmed workers
    wait for {rollback_done} from all (with DOWN handling)
    return error (owner releases locks)

Phase 2:
  Send {commit2, Workers} to all workers (Workers = all confirmed worker PIDs)
  Wait for {commit2_done} from all
  If any worker goes DOWN -> record in FailedPersistentNodesDBs
  Exit with {committed, FailedPersistentNodesDBs}
```

Key properties:
- `{commit2, Workers}` and `{rollback, Workers}` include the full list of participating worker PIDs.
- Coordinator exits with `{committed, FailedPersistentNodesDBs}` — workers that went DOWN during phase 2. Empty list means all succeeded.
- Coordinator sends `{rollback}` and waits for `{rollback_done}` before returning error. Locks are held throughout.

#### Worker — Phase 1

```
1. prepare_rollback for each local DB -> collect {DB, {RW, RD}}
2. If any persistent -> transaction_log:write(TRef, PersistentRollbacks)
3. Commit each local DB sequentially:
     try Module:commit(Ref, Write, Delete)
     catch ->
       rollback all previously committed: Module:commit(Ref, RW, RD)
       transaction_log:delete(TRef, DBs)
       exit with error
4. Send {confirm, self()} to coordinator
```

On worker failure (error exit), steps 1-3 handle their own rollback before exiting. The coordinator sees the `'DOWN'` and triggers rollback on other workers.

#### Worker — Phase 2: Case 5 (receives `{commit2, Workers}`)

Worker accepts the decision and waits for coordinator `'DOWN'`:

```
5.1. DOWN with {committed, []} ->
       transaction_log:delete(TRef, DBs)
       normal exit

5.2. DOWN with {committed, FailedPersistentNodesDBs} when non-empty ->
       transaction_log:delete_and_log_committed(TRef, DBs, FailedPersistentNodesDBs)
       normal exit

5.3. DOWN with other Reason (coordinator crash) ->
       broadcast {committed, Workers -- [self()]} to all Workers via PID !
       transaction_log:delete_and_log_committed(TRef, DBs, AllPersistentNodesDBs)
       exit
```

**5.3 ordering:** broadcast first, then batch. If crash between broadcast and batch — other workers log `{committed}`, and this worker recovers via `is_committed` on restart.

#### Worker — Phase 2: Case 6 (receives `{rollback, Workers}`)

Worker applies rollback and waits for coordinator `'DOWN'`:

```
6.1. DOWN with normal ->
       normal exit

6.2. DOWN with other Reason (coordinator crash) ->
       broadcast {rollbacked, Workers -- [self()]} to all Workers via PID !
       exit
```

Rollback is applied immediately on receiving `{rollback}`: `Module:commit(Ref, RW, RD)` for each DB, then `transaction_log:delete(TRef, DBs)`.

#### Worker — Phase 2: Case 7 (receives `'DOWN'` before `{commit2}` or `{rollback}`)

Worker doesn't know the coordinator's decision. Uses `pg` consensus to resolve:

```
7.0. Join pg:join(zaya_transaction, TRef)
     Monitor pg group via pg:monitor(zaya_transaction, TRef)
     Monitor each participating node via erlang:monitor_node(Node, true)
     Wait for messages:

7.1. {committed, Workers} ->
       broadcast {committed, Workers -- [self()]} to Workers via PID !
       transaction_log:delete_and_log_committed(TRef, DBs, AllPersistentNodesDBs)
       exit

7.2. {rollbacked, Workers} ->
       broadcast {rollbacked, Workers -- [self()]} to Workers via PID !
       apply rollback: Module:commit(Ref, RW, RD) for each DB
       transaction_log:delete(TRef, DBs)
       exit

7.3. pg {join} event ->
       add node to "don't know" set.
       if all participating nodes are "don't know" or DOWN ->
         apply rollback, delete log, exit

7.4. {nodedown, Node} ->
       add node to DOWN set.
       if all participating nodes are "don't know" or DOWN ->
         apply rollback, delete log, exit
```

**Consensus rule:** "all don't know" = all participating nodes are either in the pg group (undecided) or DOWN. When this condition is met, all undecided workers independently reach the same conclusion and rollback.

### Log vs. No-Log Decision

- All DBs non-persistent: prepare_rollback for all, keep rollback in memory only (no log). If node crashes, non-persistent data is lost anyway.
- Any persistent DB: prepare_rollback for all, log persistent rollbacks.

## DB Open Flow in `zaya_db_srv`

```
1. Module:open(Params) -> Ref
2. If recovered (copied) DB -> zaya_transaction_log:purge(DB)
   Else -> zaya_transaction_log:rollback(DB, fun({RW, RD}) -> Module:commit(Ref, RW, RD) end)
         The rollback function, for each {TRef, {RW, RD}} entry:
           a. Query is_committed(TRef, node(), DB) on all participating nodes
           b. If any returns true -> delete rollback entry (data is correct from phase 1)
           c. Otherwise -> call Callback({RW, RD}), then delete rollback entry
3. If rollback callback throws -> retry loop (DB stays unavailable until rollback succeeds)
4. Mark DB available
```

DB remove calls `zaya_transaction_log:purge(DB)` to clean up orphaned entries.

## Changes by Repository

| Repo | Branch | Changes |
|------|--------|---------|
| zaya | `pool` (exists) | Refactor `zaya_transaction.erl` (new commit flow, explicit rollback protocol). Add `zaya_transaction_log` gen_server. Update `zaya_sup` to start log process. Update `zaya_db_srv` open/remove flow. |
| zaya_rocksdb | `pool` (exists) | Remove `commit1/commit2/rollback/rollback_log`, `/LOG` dir, `log` from `#ref`. Add `prepare_rollback/3`, `is_persistent/0`. |
| zaya_ets_rocksdb | `pool` (exists) | Remove `commit1/commit2/rollback`, `/TLOG` dir, `log` from `#ref`. Add `prepare_rollback/3` (delegates to `zaya_ets`), `is_persistent/0`. |
| zaya_ets | `pool` (create) | Remove `commit1/commit2/rollback`. Add `prepare_rollback/3`, `is_persistent/0`. |
| zaya_ets_leveldb | `pool` (create) | Remove `commit1/commit2/rollback`. Add `prepare_rollback/3` (delegates to `zaya_ets`), `is_persistent/0`. |
| zaya_pterm_leveldb | `pool` (create) | Remove `commit1/commit2/rollback`. Add `prepare_rollback/3` (delegates to `zaya_pterm`), `is_persistent/0`. |
| zaya_leveldb | `pool` (create) | Remove `commit1/commit2/rollback/rollback_log`, `/LOG` dir, `log` from `#ref`. Add `prepare_rollback/3`, `is_persistent/0`. |
| zaya_pterm | `pool` (create) | Remove `commit1/commit2/rollback`. Add `prepare_rollback/3`, `is_persistent/0`. |
| zaya_pool | no changes | — |

## Accepted Trade-offs

- **Race window in Phase 1:** If a node crashes after `Module:commit` succeeds but before `transaction_log:write` was called for that DB's rollback, the committed changes have no rollback record. This window is extremely narrow and no worse than the current design where the backend's own log write could similarly fail after data write.
- **Non-persistent in-memory rollback:** If the worker process errors (not crashes — try/catch prevents crashes) between committing two non-persistent DBs, rollback data is in memory and available for recovery. Node crash loses non-persistent data regardless.
- **prepare_rollback overhead for non-persistent backends:** Adds read cost to multi-DB transactions involving ETS/pterm. Acceptable — multi-DB transactions are the less common path.
- **No coordinator decision log:** The coordinator does not persist its commit decision. Durable commit evidence relies on workers logging `{committed}` entries. If ALL workers crash after broadcast but before batch-writing `{committed}`, no evidence survives and all nodes rollback on restart. This requires simultaneous multi-node crashes with specific timing — extremely unlikely.
- **Case 7 in-flight rollback:** A case-7 worker may rollback via pg consensus while a `{committed}` entry exists on a DOWN node. This requires: (1) a worker logged `{committed}` then its node went DOWN, (2) the broadcast didn't reach the case-7 worker, (3) all other participating nodes are also undecided or DOWN. For a worker to miss ALL redundant broadcasts from multiple independent workers requires cascading failures — acceptable.
- **Committed entry cleanup cost:** Periodic cleanup of `{committed}` entries requires cross-node RPC to check for corresponding rollback entries. Operational overhead, not a correctness concern.
