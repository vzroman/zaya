# Transaction Log Review

Review date: 2026-04-24

Agreed review comments for the transaction refactoring work. Items are added after verification against the codebase.

## Agreed Findings

### 1. Document all supported `transaction_log` keys in the default config

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/config/apps/zaya.config:19`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:131`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:409`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:419`

The `transaction_log` application env supports more keys than the default `zaya.config` shows. `zaya_transaction_log` currently reads:

- `dir` (default `schema_dir/TLOG`)
- `pool` (forwarded to `zaya_rocksdb`)
- `rocksdb` (forwarded to `zaya_rocksdb`)
- `cleanup_interval_ms` (default 60000)
- `ref_wait_timeout_ms` (default 600000)
- `ref_wait_poll_ms` (default 1000)

But the default config only lists `pool` and `cleanup_interval_ms`:

```erlang
{transaction_log,#{
    pool => #{},                    % zaya_pool defaults
    cleanup_interval_ms => 60000    % 1 min
}}
```

A user reading `zaya.config` has no way to discover that `ref_wait_timeout_ms`, `ref_wait_poll_ms`, `dir`, and `rocksdb` are tunable without reading the source.

Recommendation: list every supported `transaction_log` key in `config/apps/zaya.config` with its default value and a short inline comment. Keys that are intentionally not user-tunable should be kept out of the config and documented accordingly in the module.

### 2. Drop `Errors` argument and `ErrorCount` return from `classify_recovery_responses/2`

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:234`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:256`
- `/home/roman/PROJECTS/SOURCES/zaya/test/zaya_transaction_log_SUITE.erl:118`

`classify_recovery_responses/2` takes `{Replies, Errors}` and returns `{Reachable, ErrorCount}`, but neither the error input nor the count output influences any decision:

```erlang
classify_recovery_responses(Replies, Errors) ->
  lists:foldl(
    fun({Node, Reply}, {ReachableAcc, ErrorCount}) ->
      case Reply of
        {ok, Result} when is_boolean(Result) ->
          {[{Node, Result} | ReachableAcc], ErrorCount};
        _ ->
          {ReachableAcc, ErrorCount + 1}
      end
    end,
    {[], length(Errors)},
    Replies
  ).
```

The sole caller in `probe_recovery/1` discards the count:

```erlang
{Reachable, _ErrorCount} = classify_recovery_responses(Replies, Errors),
```

The recovery decision tree only inspects `Reachable`; unreachable peers and classifier-rejected replies are already handled correctly by falling through to `pending`. There is no behavior downstream that needs to know *how many* errors occurred.

Recommendation: reduce the function to `classify_recovery_responses(Replies) -> [{Node, Result}]`, drop the `Errors` argument, and update `probe_recovery/1` and the test in `zaya_transaction_log_SUITE` to match the simpler contract.

### 3. Fold `pending_transaction_decision_loop/1` into `decide_recovery/1`

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:270`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:276`

`decide_recovery/1` and `pending_transaction_decision_loop/1` both call `probe_recovery/1` and branch on `pending`. The outer function exists only to perform the first probe; the loop repeats the same probe with an env-override check and a sleep. That split serves no purpose beyond duplicating the dispatch shape:

```erlang
decide_recovery(TRef) ->
  case probe_recovery(TRef) of
    pending  -> pending_transaction_decision_loop(TRef);
    Decision -> Decision
  end.

pending_transaction_decision_loop(TRef) ->
  case probe_recovery(TRef) of
    pending ->
      case pending_env_action() of
        undefined ->
          timer:sleep(1000),
          pending_transaction_decision_loop(TRef);
        Action ->
          ?LOGWARNING("~p ~p by PENDING_TRANSACTIONS", [TRef, Action]),
          Action
      end;
    Decision ->
      Decision
  end.
```

The env-override check and the sleep belong directly in `decide_recovery/1`'s `pending` branch; the function can recurse into itself instead of into a sibling.

Recommendation: remove `pending_transaction_decision_loop/1` and move `pending_env_action/0`, the warning log, and the sleep/retry directly into `decide_recovery/1`'s `pending` branch. The single caller at `zaya_transaction_log.erl:72` is unaffected.

### 4. Hoist the shared `commit/2` out of `rollback/2`'s case

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:72`

Both branches of the `decide_recovery/1` case in `rollback/2` end with the same call:

```erlang
case decide_recovery(Key#rollback.tref) of
  rollback ->
    ok = Callback({RollbackWrite, RollbackDelete}),
    ok = commit([], [Key]);
  commit ->
    ok = commit([], [Key])
end
```

The rollback-entry removal (`commit([], [Key])`) happens unconditionally; only the callback invocation is branch-specific.

Recommendation: keep only the callback dispatch inside the case and move the shared `ok = commit([], [Key])` after it. For example:

```erlang
case decide_recovery(Key#rollback.tref) of
  rollback -> ok = Callback({RollbackWrite, RollbackDelete});
  commit   -> ok
end,
ok = commit([], [Key])
```

### 5. Use `start`/`stop` bounds in `rollback_entries/1` instead of full-table scan + Erlang filter

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:306`

`rollback_entries/1` iterates the entire transaction log with an empty query and filters in Erlang by comparing `EntryDB =:= DB`:

```erlang
rollback_entries(DB) ->
  zaya_rocksdb:foldl(
    db_ref(),
    #{},
    fun
      ({#rollback{db = EntryDB} = Key, Value}, Acc) when EntryDB =:= DB ->
        [{Key, Value} | Acc];
      (_, Acc) ->
        Acc
    end,
    []
  ).
```

`zaya_rocksdb:foldl/4` supports `start` and `stop` keys in the query map. Bounding the scan to exactly this DB's entries removes the Erlang-side guard and the catch-all `_` clause.

Erlang compares tuples by arity first, then element-wise, so the bounds must stay at arity 4 (matching the `#rollback{}` record shape) and differ only in element values:

```erlang
#{
  start => {rollback, DB, 0, 0},
  stop  => {rollback, DB, '$end', '$end'}
}
```

- `{rollback, DB, 0, 0}`: integer `0` at `seq` is the minimum, and integer `0` at `tref` is below any real reference (number < reference in Erlang term order), so this is ≤ any real `{rollback, DB, Seq, TRef}` and > any `{rollback, SmallerDB, …}`.
- `{rollback, DB, '$end', '$end'}`: atom `'$end'` at `seq` sorts above any integer (number < atom), so this is > every real entry for `DB` and still < any `{rollback, NextDB, …}` or `{rollbacked, …}`.

Note: the design plan at `docs/superpowers/plans/2026-04-20-transaction-refactoring.md:391` proposes `#{start => {rollback, DB}, stop => {rollback, DB, '$end'}}`. Those are arity 2 and arity 3 respectively and therefore both sort *before* any arity-4 real entry — `start` is too loose (scans entries for all DBs) and `stop` terminates the iterator before yielding anything. The plan text should be corrected along with this fix.

Recommendation: pass the arity-4 bounds shown above to `zaya_rocksdb:foldl/4` in `rollback_entries/1`, remove the DB-matching guard and the catch-all clause, and update the plan document accordingly.

Same scan-and-filter pattern appears at:
- `zaya_transaction_log.erl:103` `pending_trefs/1` — at minimum can bound to `#rollback{}` keys (skip `#rollbacked{}`); the DB-specific caller at `:97` can use the full per-DB bound.
- `zaya_transaction_log.erl:158` `init_seq/1` — bound to `#rollback{}` range; could also use `foldr` to find the max in a single step.
- `zaya_transaction_log.erl:324` `clean_rollbacks/2` — bound to `#rollback{}` range.
- `zaya_transaction_log.erl:343` `clean_rollbacked/2` — bound to `#rollbacked{}` range.

### 6. Replace `sets` with native maps for TRef membership accumulators

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:111`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:117`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:119`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:332`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:338`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:351`

Three sites use `sets` purely as a membership accumulator for `TRef` values — `sets:new/0`, `sets:add_element/2`, `sets:is_element/2`, `sets:to_list/1`. No set-algebra operations (union, intersection, difference) are used:

```erlang
pending_trefs(Filter) ->
  Set =
    zaya_rocksdb:foldl(
      db_ref(),
      #{},
      fun
        ({#rollback{db = DB, tref = TRef}, _Value}, Acc) ->
          case Filter(DB) of
            true -> sets:add_element(TRef, Acc);
            false -> Acc
          end;
        ...
      end,
      sets:new()
    ),
  sets:to_list(Set).
```

```erlang
clean_rollbacks(Ref, LocalDBs) ->
  {ActiveTRefs, StaleKeys} =
    zaya_rocksdb:foldl(Ref, #{},
      fun
        ({#rollback{db = DB, tref = TRef} = Key, _Value}, {ActiveAcc, DeleteAcc}) ->
          case lists:member(DB, LocalDBs) of
            true  -> {sets:add_element(TRef, ActiveAcc), DeleteAcc};
            ...
          end;
        ...
      end,
      {sets:new(), []}),
  ...
```

```erlang
clean_rollbacked(Ref, ActiveTRefs) ->
  zaya_rocksdb:foldl(Ref, #{},
    fun
      ({#rollbacked{tref = TRef}, NodesByDB}, ok) ->
        case sets:is_element(TRef, ActiveTRefs) of
          true  -> ok;
          false -> cleanup_rollbacked_entry(Ref, TRef, NodesByDB)
        end;
      ...
    end,
    ok).
```

Two reasons to drop `sets` here:

1. `sets:new/0` returns the legacy v1 representation (an internal hashed-bucket record) unless `{version, 2}` is explicitly passed. The code as written does not opt in, so it pays the cost of the older implementation on every `add_element`/`is_element` call.
2. Native maps (`#{}`, `Acc#{TRef => []}`, `is_map_key/2`, `maps:keys/1`) have direct language support, avoid the module-boundary call per fold iteration, and match the idiom already used elsewhere in this module (the log DB is addressed via map literals, record updates are maps-first, etc.).

Recommendation: replace all three uses with native maps:

- `pending_trefs/1`: accumulator `#{}`, add with `Acc#{TRef => []}`, return `maps:keys(Set)`.
- `clean_rollbacks/2`: accumulator `{#{}, []}`, add with `ActiveAcc#{TRef => []}`; `ActiveTRefs` becomes a map and is passed through to `clean_rollbacked/2`.
- `clean_rollbacked/2`: test membership via `is_map_key(TRef, ActiveTRefs)`.

No call sites outside these functions rely on `ActiveTRefs` being a `sets` value, so the signature change is local.

### 7. Rebuild `list_pending_transactions/{0,1}` around a single bounded foldl over `#rollback{}` entries

Severity: Medium

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:93`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:96`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:99`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:103`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:300`
- `/home/roman/PROJECTS/SOURCES/zaya/test/zaya_transaction_log_SUITE.erl:211`
- `/home/roman/PROJECTS/SOURCES/zaya/test/zaya_transaction_log_SUITE.erl:217`
- `/home/roman/PROJECTS/SOURCES/zaya/test/zaya_transaction_log_SUITE.erl:235`
- `/home/roman/PROJECTS/SOURCES/zaya/test/zaya_transaction_SUITE.erl:68`
- `/home/roman/PROJECTS/SOURCES/zaya/test/zaya_transaction_SUITE.erl:104`
- `/home/roman/PROJECTS/SOURCES/zaya/test/zaya_transaction_SUITE.erl:170`

Current flow joins two unrelated data sources:

```erlang
list_pending_transactions() ->
  describe_pending(pending_trefs(fun(_DB) -> true end)).

list_pending_transactions(DB) ->
  describe_pending(pending_trefs(fun(EntryDB) -> EntryDB =:= DB end)).

describe_pending(TRefs) ->
  Action = pending_env_action(),
  [{TRef, rollback_participants(TRef), Action} || TRef <- TRefs].

pending_trefs(Filter) ->
  Set =
    zaya_rocksdb:foldl(
      db_ref(),
      #{},
      fun
        ({#rollback{db = DB, tref = TRef}, _Value}, Acc) ->
          case Filter(DB) of
            true -> sets:add_element(TRef, Acc);
            false -> Acc
          end;
        (_, Acc) -> Acc
      end,
      sets:new()
    ),
  sets:to_list(Set).

rollback_participants(TRef) ->
  case zaya_rocksdb:read(db_ref(), [#rollbacked{tref = TRef}]) of
    [{_, NodesByDB}] -> NodesByDB;
    [] -> []
  end.
```

Problems:

1. `pending_trefs/1` scans `#rollback{}` entries (the local per-DB rollback queue) with no range bounds, filtering in Erlang.
2. `describe_pending/1` then issues N separate point reads against `#rollbacked{}` markers to recover per-TRef `NodesByDB`. That's a join of two different sources (local per-DB work queue vs. cluster-wide decision marker) and the marker may be absent for a locally pending TRef — in which case `rollback_participants/1` returns `[]` and the per-DB information present in the `#rollback{}` entries themselves is discarded.
3. `#rollbacked{}` participants are frozen at rollback-decision time; `zaya:db_all_nodes(DB)` is the current cluster topology, which is what an admin looking at this list actually needs.

Correct shape: a single bounded `foldl` over `#rollback{}` entries. The query differs per arity:

- `list_pending_transactions/0`: bound to the `#rollback{}` prefix only. Minimum at arity 4 with a value below any atom at position 2 (e.g. `{rollback, 0, 0, 0}` — integer `0` < any atom); maximum at arity 4 with a value above any atom at position 2 (e.g. `{rollback, [], 0, 0}` — list `[]` > any atom in Erlang term order).
- `list_pending_transactions/1(DB)`: use the per-DB bounds from item 5 — `{rollback, DB, 0, 0}` .. `{rollback, DB, '$end', '$end'}`.

Accumulate into `#{TRef => #{DB => [Node]}}` directly inside the fold callback:

```erlang
fun({#rollback{db = DB, tref = TRef}, _Value}, Acc) ->
  Nodes = zaya:db_all_nodes(DB),
  DBMap = maps:get(TRef, Acc, #{}),
  Acc#{TRef => DBMap#{DB => Nodes}}
end
```

(`zaya:db_all_nodes/1` can be hoisted to a per-DB cache inside the accumulator if the fold visits many entries for the same DB.)

Consequences of the new shape:

- `describe_pending/1`, `pending_trefs/1`, and `rollback_participants/1` all disappear. Their only callers are the two `list_pending_transactions` entry points.
- The `Action` field (derived from `pending_env_action/0`) drops from the return value. `pending_env_action/0` itself stays — it is still used by `decide_recovery/1` (item 3).
- The three `?assertEqual([], zaya:list_pending_transactions())` checks in `zaya_transaction_SUITE.erl` and `zaya_transaction_log_SUITE.erl:235` keep working (empty map `#{}` is the new "nothing pending" value — the test needs to be switched from `[]` to `#{}`).
- The two `?assertEqual([{TRef, Participation, Action}], …)` checks in `zaya_transaction_log_SUITE.erl:211-213,217-219` need to be rewritten against the new map shape `#{TRef => #{DB => Nodes}}`. The `Participation` fixture already carries the DB→Nodes map, so the conversion is mechanical; the `Action` assertion becomes a separate check against `pending_env_action/0` if the test still wants to cover the env override path.

Recommendation: replace `list_pending_transactions/{0,1}` with a single `zaya_rocksdb:foldl/4` call that builds `#{TRef => #{DB => zaya:db_all_nodes(DB)}}`, using arity-4 `start`/`stop` bounds tailored to the all-DBs vs. single-DB case. Delete `describe_pending/1`, `pending_trefs/1`, and `rollback_participants/1`. Update the affected test assertions accordingly.

### 8. Group functions into banner-commented sections (API / OTP / Internals)

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl` (whole file)

Reference style: `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_db_srv.erl:205-526`.

The file has no section separators. OTP callbacks are interleaved with non-OTP helpers in three separate zones rather than living under a single "OTP callbacks" banner:

- Public API: `:48-89`
- Pending-transaction helpers: `:91-119`
- OTP `init/1`: `:121-128`
- Config / open_log_db / init_seq (helpers used by init): `:130-169`
- OTP `handle_call/handle_cast/handle_info`: `:171-189`
- OTP `terminate/code_change`: `:192-202`
- Runtime accessors (`runtime/0`, `db_ref/0`, `wait_for_runtime/1`): `:205-221`
- Recovery decision helpers: `:231-297`
- Rollback readers (`rollback_participants/1`, `rollback_entries/1`): `:300-317`
- Cleanup helpers: `:319-395`
- Config accessors (`ref_wait_timeout_ms/0`, `ref_wait_poll_ms/0`): `:408-423`

The rest of the zaya codebase follows the convention in `zaya_db_srv.erl`:

```erlang
%%---------------------------------------------------------------------------
%%   CREATE
%%---------------------------------------------------------------------------
handle_event(state_timeout, run, {create, ...}, ...) -> ...

%%------------------------------------------------------------------------------------
%%  Internals
%%------------------------------------------------------------------------------------
default_params(DB, Params) -> ...
```

Recommendation: reorganise `zaya_transaction_log.erl` into banner-separated sections. One workable layout:

1. `%% API` — the exported non-OTP functions (`seq/0`, `commit/2`, `is_rollbacked/1`, `rollback/2`, `purge/1`, `list_pending_transactions/0,1`).
2. `%% OTP` — the `gen_server` callbacks only, contiguous (`start_link/0`, `init/1`, `handle_call/3`, `handle_cast/2`, `handle_info/2`, `terminate/2`, `code_change/3`).
3. `%% Internals` (or finer-grained sub-banners):
   - `%% Runtime` — `runtime/0`, `db_ref/0`, `wait_for_runtime/1`.
   - `%% Config` — `read_config/0`, `validate_cleanup_interval/1`, `ref_wait_timeout_ms/0`, `ref_wait_poll_ms/0`.
   - `%% Log storage` — `open_log_db/1`, `init_seq/1`.
   - `%% Recovery decision` — `classify_recovery_responses/…`, `probe_recovery/1`, `decide_recovery/1`, `pending_env_action/0`.
   - `%% Cleanup` — `cleanup/1`, `clean_rollbacks/2`, `clean_rollbacked/2`, `cleanup_rollbacked_entry/3`, `maybe_delete/2`, `schedule_cleanup/1`.
   - `%% Rollback entries` — `rollback_entries/1` (and, until item 7 lands, `rollback_participants/1`).

The exact sub-sectioning is a judgement call; the hard rule is "OTP callbacks stay together in one contiguous section, helpers live below banners, no interleaving."

### 9. Inline `schedule_cleanup/1` into `handle_info(cleanup, ...)`

Severity: Low

Status: agreed

Location:
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:187`
- `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl:402`

```erlang
schedule_cleanup(Interval) when is_integer(Interval), Interval > 0 ->
  erlang:send_after(Interval, self(), cleanup),
  ok;
schedule_cleanup(_Interval) ->
  ok.
```

The wrapper adds no value over a direct call:

- The sole caller is `handle_info(cleanup, ...)` at `:187` and it discards the return.
- `Interval` lives in `State#state.cleanup_interval_ms`, set once in `init/1` (`:128`) from a value that already passed `validate_cleanup_interval/1` (`:138-145`), so it is always a positive integer.
- Consequently the guard is unreachable and the silent-skip fallback clause is dead — an invalid interval cannot reach `schedule_cleanup/1`.

Recommendation: replace the call at `:187` with `erlang:send_after(Interval, self(), cleanup)` and delete `schedule_cleanup/1`.

