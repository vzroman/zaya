# Pool vs Main Review - Transaction Refactoring

Review date: 2026-04-21

This file records agreed review comments for the transaction refactoring work. Items are added after verification against the codebase.

## Agreed Findings

### 1. Move `transaction_log` runtime params out of `zaya.app.src`

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya.app.src:25`
- `/home/roman/PROJECTS/SOURCES/zaya/config/apps/zaya.config:2`

The new `transaction_log` app env is currently defined in `src/zaya.app.src`:

```erlang
{env,[
  {transaction_log, #{}}
]},
```

This is runtime configuration and should live with the rest of the `zaya` runtime application settings in `config/apps/zaya.config`, not in the application resource file.

Recommendation: remove the `transaction_log` entry from `src/zaya.app.src` and add the corresponding configuration under the existing `{zaya, [...]}` block in `config/apps/zaya.config`.

### 2. Avoid redundant `DATA` directory under `TLOG`

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:106`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:114`
- `/home/roman/PROJECTS/SOURCES/zaya_rocksdb/src/zaya_rocksdb.erl:202`
- `/home/roman/PROJECTS/SOURCES/zaya_rocksdb/src/zaya_rocksdb.erl:231`

`zaya_transaction_log` defaults its storage directory to `schema_dir/TLOG`, but then checks for and opens `TLOG/DATA` through the `zaya_rocksdb` backend layout:

```erlang
dir => filename:join(zaya:schema_dir(), "TLOG")
```

```erlang
case filelib:is_dir(filename:join(Dir, "DATA")) of
```

The nested `DATA` directory is inherited from the legacy database layout used by `zaya_rocksdb:create/1` and `zaya_rocksdb:open/1`, where the backend always maps `Dir` to `Dir ++ "/DATA"`. For the centralized transaction log there is no sibling legacy `LOG` directory, so `TLOG/DATA` is redundant and preserves legacy formatting where it is no longer needed.

Recommendation: store the transaction log directly under `TLOG`, or add/open it through a storage path that does not force the legacy `DATA` subdirectory.

### 3. Enable the transaction log RocksDB pool by default

Severity: Medium

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:107`
- `/home/roman/PROJECTS/SOURCES/zaya_rocksdb/src/zaya_rocksdb.erl:761`
- `/home/roman/PROJECTS/SOURCES/zaya_rocksdb/src/zaya_rocksdb.erl:763`

`zaya_transaction_log:init/1` currently hard-codes the transaction log RocksDB backend to direct writes:

```erlang
pool => disabled
```

In `zaya_rocksdb`, this exact value is the special case that disables the worker pool. If the `pool` key is absent or contains pool parameters, `zaya_rocksdb` starts a `zaya_pool` worker pool instead.

The transaction log is a shared persistence path used by transaction workers and recovery logic. It should not default to the direct RocksDB write path; the pool must be enabled for normal operation and configured through the transaction log application config.

Recommendation: remove the hard-coded `pool => disabled` default from `zaya_transaction_log:init/1` and configure an enabled pool under `transaction_log` in `config/apps/zaya.config`. Tests can still override this to `pool => disabled` where direct mode is intentional.

### 4. Separate transaction-log runtime options from RocksDB options

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:102`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:108`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:115`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:116`
- `/home/roman/PROJECTS/SOURCES/zaya_rocksdb/src/zaya_rocksdb.erl:193`
- `/home/roman/PROJECTS/SOURCES/zaya_rocksdb/src/zaya_rocksdb.erl:222`

`zaya_transaction_log:init/1` builds one `Config` map that contains both RocksDB parameters and transaction-log server parameters:

```erlang
cleanup_interval_ms => 60000
```

The same `Config` map is then passed directly to `zaya_rocksdb:open/1` or `zaya_rocksdb:create/1`, even though `cleanup_interval_ms` is consumed only by `zaya_transaction_log` for cleanup scheduling and state. It is not a `zaya_rocksdb` option.

Recommendation: split the configuration into transaction-log options and RocksDB storage options before opening the backend. For example, read `cleanup_interval_ms` from the transaction-log config, but pass only storage-relevant keys such as `dir`, `pool`, and `rocksdb` into `zaya_rocksdb`.

### 5. Define shared records for transaction log keys

Severity: Medium

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:58`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:155`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:170`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction.erl:1005`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction.erl:1065`
- `/home/roman/PROJECTS/SOURCES/zaya/include`

The transaction log storage keys are currently repeated as raw tuples across modules:

```erlang
{rollback, DB, Seq, TRef}
{rollbacked, TRef}
```

These tuple formats are part of the transaction log storage schema, but the shape is currently duplicated in `zaya_transaction`, `zaya_transaction_log`, and tests. This makes future schema changes brittle and easy to apply inconsistently.

Recommendation: add `include/zaya_transaction.hrl` with shared record definitions for the rollback entry key and rollbacked marker key, then reuse those records everywhere the transaction log schema is read or written.

### 6. Simplify recovery reply classification without widening accepted success replies

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:220`

The current classifier spells out the two accepted successful replies separately:

```erlang
{ok, true}
{ok, false}
```

This can be simplified, but the simplification must preserve protocol validation. The recovery flow should treat only boolean `{ok, Result}` replies as valid reachable answers and keep every other reply in the error bucket.

Recommendation: replace the duplicated success branches with a guarded clause such as `case Reply of {ok, Result} when is_boolean(Result) -> ...; _ -> ... end`. This keeps the code smaller without accepting malformed success payloads like `{ok, unexpected}`.

### 7. Simplify `classify_recovery_responses/2` contract and accumulator shape

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:216`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:234`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:239`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:268`
- `/home/roman/PROJECTS/SOURCES/zaya/test/zaya_transaction_log_SUITE.erl:123`

`classify_recovery_responses/2` currently does more work than its callers need:

- it preserves successful replies as `{Node, {ok, Result}}` instead of `{Node, Result}`
- it accepts the separate `Errors` list and returns full error payloads
- it reverses accumulators to preserve input order even though callers do not depend on response order

The current decision logic only needs three pieces of information:

- whether any reachable node returned `true`
- whether the coordinator node returned `false`
- whether any reply or RPC error occurred

Recommendation: simplify the classifier contract so it returns reachable boolean results in a compact shape such as `{Node, Result}` and an error indicator (`HasErrors` or `ErrorCount`) instead of full error payloads. With that contract, there is no need to preserve fold order with `lists:reverse/1`; the `lists:foldl/3` result can be used as accumulated.

### 8. Extract the shared recovery decision logic from `decide_recovery/1` and `pending_transaction_decision_loop/1`

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:236`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:265`

`decide_recovery/1` and `pending_transaction_decision_loop/1` are not fully identical, but they currently duplicate the same core recovery flow:

- call `ecall:call_all_wait/4`
- classify replies with `classify_recovery_responses/2`
- return `rollback` if any reachable node reports `true`
- return `commit` if the coordinator reports `false`
- otherwise fall through to pending handling

The only meaningful difference is what happens in the pending case: `decide_recovery/1` enters pending-transaction handling, while `pending_transaction_decision_loop/1` waits for a manual override and retries. Keeping the probe-and-decide logic duplicated in both places makes later changes to recovery behavior easy to apply inconsistently.

Recommendation: extract the shared recovery probe into a helper that returns a compact decision such as `rollback`, `commit`, or `pending`, and let `decide_recovery/1` and `pending_transaction_decision_loop/1` handle only their different pending-path behavior.

### 9. Remove the transient `?PENDING_TABLE` shadow state and derive pending transactions from the log

Severity: Medium

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:90`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:256`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:327`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:378`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya.erl:153`

`list_pending_transactions/0` currently does not read the transaction log at all. It reports only the contents of the process-local `?PENDING_TABLE` ETS table:

- entries are inserted only while a local rollback is sitting in `pending_transaction_decision/1`
- entries are removed when that local wait loop exits
- `ensure_pending_table/0` clears the ETS table on startup

That means the current API exposes transient local wait-state, not the durable set of unresolved transactions stored in the log. After restart, or when no local process is currently blocked in the decision loop, `list_pending_transactions/0` can return `[]` even though rollback entries are still present in RocksDB.

Recommendation: remove the extra `?PENDING_TABLE` shadow state and build pending-transaction listing directly from the transaction log. Provide:

- `list_pending_transactions/0` to return all pending transactions visible in the log
- `list_pending_transactions(DB)` to return only pending transactions for a specific DB

Participation data can still be read from the persisted rollback markers, and the current manual action can still be derived from `pending_env_action/0` if it should remain part of the listing output.

### 10. Store the transaction-log runtime handles under one persistent-term key

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:32`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:47`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:52`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:132`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:133`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:188`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:189`

`zaya_transaction_log` currently stores the RocksDB handle and the sequence counter atomics handle in two separate persistent-term entries:

```erlang
persistent_term:put(?REF_KEY, Ref),
persistent_term:put(?ATOMICS_REF_KEY, AtomicsRef),
```

These two values form one runtime context for the transaction log, but they are initialized, fetched, and erased independently. That spreads one logical piece of state across multiple keys and makes the module API more coupled to storage details than it needs to be.

Recommendation: keep the runtime handles under one persistent-term key and access them through a single container, preferably a module-specific record such as `#runtime_refs{db_ref, atomics_ref}` or similar. This keeps the runtime state grouped and avoids splitting one logical handle set across separate persistent-term entries.

### 11. Split `init/1` into dedicated initialization helpers

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:101`

`zaya_transaction_log:init/1` currently combines several distinct responsibilities in one function body:

- read and merge transaction-log configuration
- open or create the log database
- scan the log to seed the transaction sequence counter
- register runtime handles and schedule background work

Those steps are conceptually separate, and some of them are already the subject of independent review comments about configuration shape and runtime handle management. Keeping them all inline makes the startup path harder to read and harder to evolve safely.

Recommendation: split `init/1` into dedicated helpers such as `read_config`, `open_log_db`, and `init_seq`, then keep `init/1` as a short orchestration function that wires the startup steps together.

### 12. Keep `handle_info(cleanup, ...)` as loop orchestration and move cleanup work into dedicated helpers

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:148`

`handle_info(cleanup, State)` currently contains the whole cleanup implementation inline:

- scans rollback entries to find stale per-DB rollback keys
- deletes stale rollback entries
- scans rollback markers and rewrites or deletes stale `rollbacked` entries
- reschedules the next cleanup timer

That mixes two concerns in one callback:

- gen_server loop handling
- cleanup procedure internals

Because the callback performs the full cleanup inline, any exception in the fold/delete/rewrite path will also abort the loop callback before the next timer is scheduled. For this kind of periodic maintenance path, the loop should stay small and the cleanup work should be isolated.

Recommendation: keep `handle_info(cleanup, ...)` as a thin orchestration callback that calls a dedicated `cleanup(Ref)` helper and then reschedules the timer. Inside `cleanup/1`, split the work into dedicated helpers such as `clean_rollbacks` and `clean_rollbacked`, wrapped in `try ... catch` with logging so cleanup failures are reported without silently killing the transaction-log server or skipping future cleanup runs.

### 13. Validate `cleanup_interval_ms` up front and trigger the first cleanup immediately

Severity: Medium

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:108`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:135`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:148`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:372`

The cleanup interval is currently merged into config as a defaulted value, but it is not validated before use:

```erlang
cleanup_interval_ms => 60000
```

If configuration overrides `cleanup_interval_ms` with an invalid value, that invalid value survives the merge and reaches `schedule_cleanup/1`. The current fallback branch in `schedule_cleanup/1` just returns `ok`, which silently disables periodic cleanup instead of reporting configuration damage and falling back to a safe interval.

Startup timing is also delayed unnecessarily: on init, the server schedules the first cleanup only after the interval elapses, even though stale rollback and rollback-marker entries may already exist when the log starts.

Recommendation:

- validate `cleanup_interval_ms` while reading config
- require the runtime state to always hold a valid interval
- if config provides an invalid value, log the error and fall back to the default interval
- trigger the first cleanup immediately on startup, for example by sending `cleanup` to `self()`
- after that, use the validated interval for periodic re-arming without a silent no-op scheduling branch

### 14. Add targeted inline comments at non-obvious decision points

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:236`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:265`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:335`

`zaya_transaction_log` contains a few branches where the code is making distributed-state decisions that are not obvious from control flow alone. The most important examples are:

- the recovery decision tree that chooses between `rollback`, `commit`, and pending/manual resolution
- the pending decision loop that keeps retrying until a safe outcome is known or a manual override is supplied
- the cleanup logic that decides when a persisted `rollbacked` marker should be rewritten or deleted

These areas are understandable after careful reading, but the intent of the branches is not self-evident at first pass. Short inline comments explaining why each branch means “safe to commit”, “must rollback”, or “keep waiting / keep marker” would make future maintenance less error-prone.

Recommendation: add a few targeted inline comments at the decision-making points, especially in the recovery and cleanup paths. Keep them intent-focused and local to the branches whose behavior is non-obvious, rather than adding broad explanatory comments everywhere.
