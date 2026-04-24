# Transaction Refactoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace backend-owned transaction logs with a central `zaya_transaction_log` service and migrate all transaction-aware backends to the reduced `commit/3`, `prepare_rollback/3`, and `is_persistent/0` contract without changing user-facing transaction semantics.

**Architecture:** `zaya` becomes the only owner of transaction durability and recovery. `zaya_transaction_log` stores rollback data and `{rollbacked, TRef}` evidence in a dedicated RocksDB instance, `zaya_db_srv` replays unresolved rollback data while opening a DB, and `zaya_transaction` switches from backend-managed `commit1/commit2/rollback` to coordinator/worker FSMs that write directly to the central log. Backends shrink to storage adapters that can compute inverse operations and apply a batch.

**Tech Stack:** Erlang/OTP (`gen_server`, `gen_statem`, `pg`, `persistent_term`, `atomics`), Common Test, `ecall`, `zaya_rocksdb`, `sext`

---

## Preconditions

- Execute this plan from a dedicated worktree rooted at `/home/roman/PROJECTS/SOURCES/zaya`.
- Repos already present locally: `/home/roman/PROJECTS/SOURCES/zaya`, `/home/roman/PROJECTS/SOURCES/zaya_rocksdb`, `/home/roman/PROJECTS/SOURCES/zaya_ets_rocksdb`, `/home/roman/PROJECTS/SOURCES/zaya_ets`, `/home/roman/PROJECTS/SOURCES/zaya_leveldb`.
- Repos declared in `/home/roman/PROJECTS/SOURCES/zaya/rebar.config` but missing locally: `/home/roman/PROJECTS/SOURCES/zaya_ets_leveldb`, `/home/roman/PROJECTS/SOURCES/zaya_pterm`, `/home/roman/PROJECTS/SOURCES/zaya_pterm_leveldb`.
- The reviewed spec for this work is `/home/roman/PROJECTS/SOURCES/zaya/docs/superpowers/specs/2026-04-16-transaction-refactoring-design.md`; do not improvise protocol changes beyond that document.

### Task 1: Central Transaction Log And DB-Open Recovery

**Files:**
- Create: `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction_log.erl`
- Create: `/home/roman/PROJECTS/SOURCES/zaya/test/zaya_transaction_log_SUITE.erl`
- Create: `/home/roman/PROJECTS/SOURCES/zaya/test/support/zaya_tx_test_backend.erl`
- Modify: `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_sup.erl`
- Modify: `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_db_srv.erl`
- Modify: `/home/roman/PROJECTS/SOURCES/zaya/src/zaya.erl`
- Modify: `/home/roman/PROJECTS/SOURCES/zaya/src/zaya.app.src`

- [ ] **Step 1: Write the failing Common Test suite for the new log API and DB-open integration**

```erlang
-module(zaya_transaction_log_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  seq_and_marker_roundtrip_test/1,
  rollback_replays_newest_first_test/1,
  purge_deletes_only_db_entries_test/1
]).

all()->
  [
    seq_and_marker_roundtrip_test,
    rollback_replays_newest_first_test,
    purge_deletes_only_db_entries_test
  ].

init_per_suite(Config)->
  application:set_env(
    zaya,
    transaction_log,
    #{
      dir => filename:join(?config(priv_dir, Config), "tlog"),
      pool => disabled,
      cleanup_interval_ms => 1000
    }
  ),
  {ok, _Apps} = application:ensure_all_started(zaya),
  Config.

end_per_suite(_Config)->
  ok = application:stop(zaya),
  ok.

init_per_testcase(_Name, Config)->
  erase(applied_rollbacks),
  Config.

end_per_testcase(_Name, _Config)->
  ok.

seq_and_marker_roundtrip_test(_Config)->
  TRef = make_ref(),
  ?assertEqual({ok, false}, zaya_transaction_log:is_rollbacked(TRef)),
  Seq0 = zaya_transaction_log:seq(),
  Seq1 = zaya_transaction_log:seq(),
  ?assertEqual(Seq0 + 1, Seq1),
  ok = zaya_transaction_log:commit(
    [{{rollbacked, TRef}, [{orders, [node()]}]}],
    []
  ),
  ?assertEqual({ok, true}, zaya_transaction_log:is_rollbacked(TRef)).

rollback_replays_newest_first_test(_Config)->
  Older = make_ref(),
  Newer = make_ref(),
  Seq0 = zaya_transaction_log:seq(),
  Seq1 = zaya_transaction_log:seq(),
  ok = zaya_transaction_log:commit(
    [
      {{rollbacked, Older}, [{orders, [node()]}]},
      {{rollback, orders, Seq0, Older}, {[{item, old_value}], []}},
      {{rollbacked, Newer}, [{orders, [node()]}]},
      {{rollback, orders, Seq1, Newer}, {[{item, newer_value}], []}}
    ],
    []
  ),
  ok = zaya_transaction_log:rollback(
    orders,
    fun(RollbackOps)->
      put(applied_rollbacks, [RollbackOps | get(applied_rollbacks)])
    end
  ),
  ?assertEqual(
    [
      {[{item, old_value}], []},
      {[{item, newer_value}], []}
    ],
    lists:reverse(get(applied_rollbacks))
  ).

purge_deletes_only_db_entries_test(_Config)->
  TRef = make_ref(),
  Seq = zaya_transaction_log:seq(),
  ok = zaya_transaction_log:commit(
    [
      {{rollbacked, TRef}, [{orders, [node()]}]},
      {{rollback, orders, Seq, TRef}, {[{item, old_value}], []}}
    ],
    []
  ),
  ok = zaya_transaction_log:purge(orders),
  ?assertEqual({ok, true}, zaya_transaction_log:is_rollbacked(TRef)).
```

- [ ] **Step 2: Run the new suite and verify it fails because the log service does not exist yet**

Run:

```bash
cd /home/roman/PROJECTS/SOURCES/zaya && rebar3 ct --suite test/zaya_transaction_log_SUITE.erl
```

Expected: Common Test fails with `undef` for `zaya_transaction_log:*` calls and missing supervisor wiring.

- [ ] **Step 3: Create the minimal log module skeleton and supervisor/public API wiring**

```erlang
-module(zaya_transaction_log).

-behaviour(gen_server).

-export([
  start_link/0,
  seq/0,
  commit/2,
  is_rollbacked/1,
  rollback/2,
  purge/1,
  list_pending_transactions/0
]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-record(state, {
  ref,
  atomics_ref,
  cleanup_interval_ms = 60000
}).

start_link()->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

seq()->
  AtomicsRef = persistent_term:get({?MODULE, atomics_ref}),
  atomics:add_get(AtomicsRef, 1, 1) - 1.

commit(Write, Delete)->
  Ref = persistent_term:get({?MODULE, ref}),
  EncodedWrite = [{sext:encode(Key), term_to_binary(Value)} || {Key, Value} <- Write],
  EncodedDelete = [sext:encode(Key) || Key <- Delete],
  zaya_rocksdb:commit(Ref, EncodedWrite, EncodedDelete).

is_rollbacked(TRef)->
  Ref = persistent_term:get({?MODULE, ref}),
  case zaya_rocksdb:read(Ref, [sext:encode({rollbacked, TRef})]) of
    [{_, _}] -> {ok, true};
    [] -> {ok, false}
  end.
```

```erlang
%% zaya_sup.erl
TransactionLogServer = #{
  id => zaya_transaction_log,
  start => {zaya_transaction_log, start_link, []},
  restart => permanent,
  shutdown => ?env(stop_timeout, ?DEFAULT_STOP_TIMEOUT),
  type => worker,
  modules => [zaya_transaction_log]
}.
```

```erlang
%% zaya.erl
-export([
  list_pending_transactions/0
]).

list_pending_transactions()->
  zaya_transaction_log:list_pending_transactions().
```

```erlang
%% test/support/zaya_tx_test_backend.erl
-module(zaya_tx_test_backend).

-export([
  open/1,
  close/1,
  commit/3,
  prepare_rollback/3,
  is_persistent/0,
  fail_once/1,
  fail_after_confirm/1,
  last_tref/0
]).

open(_Params)->
  ets:new(?MODULE, [public, named_table, ordered_set]).

close(Table)->
  catch ets:delete(Table),
  ok.

commit(Table, Write, Delete)->
  persistent_term:put({?MODULE, last_tref}, make_ref()),
  maybe_fail(),
  ets:insert(Table, Write),
  [ets:delete(Table, Key) || Key <- Delete],
  ok.

prepare_rollback(Table, Write, Delete)->
  RollbackWrite =
    lists:foldl(
      fun({Key, _Value}, Acc)->
        case ets:lookup(Table, Key) of
          [{Key, Existing}] -> [{Key, Existing} | Acc];
          [] -> Acc
        end
      end,
      [],
      Write
    ),
  RollbackDelete =
    lists:foldl(
      fun({Key, _Value}, Acc)->
        case ets:lookup(Table, Key) of
          [] -> [Key | Acc];
          [_] -> Acc
        end
      end,
      Delete,
      Write
    ),
  {lists:reverse(RollbackWrite), lists:reverse(RollbackDelete)}.

is_persistent()->
  true.

fail_once(DB)->
  persistent_term:put({?MODULE, fail_once, DB}, true),
  ok.

fail_after_confirm(DB)->
  persistent_term:put({?MODULE, fail_after_confirm, DB}, true),
  ok.

last_tref()->
  persistent_term:get({?MODULE, last_tref}, undefined).

maybe_fail()->
  ok.
```

- [ ] **Step 4: Implement the real log lifecycle, direct RocksDB access, rollback scan, purge, and cleanup sweep**

```erlang
init([])->
  Config0 = application:get_env(zaya, transaction_log, #{}),
  Config = maps:merge(
    #{
      dir => filename:join(zaya:schema_dir(), "TLOG"),
      pool => disabled,
      cleanup_interval_ms => 60000
    },
    Config0
  ),
  Ref =
    case filelib:is_dir(filename:join(maps:get(dir, Config), "DATA")) of
      true -> zaya_rocksdb:open(Config);
      false -> zaya_rocksdb:create(Config)
    end,
  AtomicsRef = atomics:new(1, [{signed, false}]),
  Seed =
    zaya_rocksdb:foldl(
      Ref,
      #{},
      fun({EncodedKey, _EncodedValue}, Acc)->
        case sext:decode(EncodedKey) of
          {rollback, _DB, Seq, _TRef} when Seq >= Acc -> Seq + 1;
          _ -> Acc
        end
      end,
      0
    ),
  ok = atomics:put(AtomicsRef, 1, Seed),
  persistent_term:put({?MODULE, ref}, Ref),
  persistent_term:put({?MODULE, atomics_ref}, AtomicsRef),
  erlang:send_after(maps:get(cleanup_interval_ms, Config), self(), cleanup),
  {ok, #state{
    ref = Ref,
    atomics_ref = AtomicsRef,
    cleanup_interval_ms = maps:get(cleanup_interval_ms, Config)
  }}.

rollback(DB, Callback)->
  Ref = persistent_term:get({?MODULE, ref}),
  Prefix = {rollback, DB},
  zaya_rocksdb:foldr(
    Ref,
    #{start => Prefix, stop => {rollback, DB, '$end'}},
    fun({EncodedKey, EncodedValue}, ok)->
      {rollback, DB, Seq, TRef} = sext:decode(EncodedKey),
      {RollbackWrite, RollbackDelete} = binary_to_term(EncodedValue),
      case decide_recovery(TRef) of
        rollback ->
          ok = Callback({RollbackWrite, RollbackDelete}),
          commit([], [{rollback, DB, Seq, TRef}]);
        commit ->
          commit([], [{rollback, DB, Seq, TRef}])
      end
    end,
    ok
  ).

decide_recovery(TRef)->
  {Replies, Errors} =
    ecall:call_all_wait(zaya:all_nodes(), ?MODULE, is_rollbacked, [TRef]),
  case lists:any(fun({_Node, Reply}) -> Reply =:= {ok, true} end, Replies) of
    true ->
      rollback;
    false ->
      CoordinatorNode = node(TRef),
      case lists:keyfind(CoordinatorNode, 1, Replies) of
        {CoordinatorNode, {ok, false}} ->
          commit;
        _ when Errors =/= [] ->
          pending_transaction_decision(TRef);
        _ ->
          pending_transaction_decision(TRef)
      end
  end.

pending_transaction_decision(TRef)->
  case os:getenv("PENDING_TRANSACTIONS") of
    "COMMIT" ->
      ?LOGWARNING("~p committed by PENDING_TRANSACTIONS", [TRef]),
      commit;
    "ROLLBACK" ->
      ?LOGWARNING("~p rolled back by PENDING_TRANSACTIONS", [TRef]),
      rollback;
    _ ->
      timer:sleep(1000),
      decide_recovery(TRef)
  end.

purge(DB)->
  Ref = persistent_term:get({?MODULE, ref}),
  zaya_rocksdb:foldl(
    Ref,
    #{start => {rollback, DB}, stop => {rollback, DB, '$end'}},
    fun({EncodedKey, _Value}, ok)->
      zaya_rocksdb:delete(Ref, [EncodedKey]),
      ok
    end,
    ok
  ).

handle_info(cleanup, State = #state{cleanup_interval_ms = Interval})->
  Ref = persistent_term:get({?MODULE, ref}),
  Active =
    zaya_rocksdb:foldl(
      Ref,
      #{},
      fun({EncodedKey, _}, Acc)->
        case sext:decode(EncodedKey) of
          {rollback, DB, _Seq, TRef} ->
            case lists:member(DB, zaya:node_dbs(node())) of
              true -> sets:add_element(TRef, Acc);
              false -> Acc
            end;
          _ ->
            Acc
        end
      end,
      sets:new()
    ),
  ok =
    zaya_rocksdb:foldl(
      Ref,
      #{},
      fun({EncodedKey, EncodedValue}, ok)->
        case sext:decode(EncodedKey) of
          {rollbacked, TRef} ->
            case sets:is_element(TRef, Active) of
              true ->
                ok;
              false ->
                Remaining =
                  [
                    {DB, [Node || Node <- Nodes, not lists:member(Node, zaya:db_available_nodes(DB))]}
                    || {DB, Nodes} <- binary_to_term(EncodedValue),
                       lists:member(DB, zaya:all_dbs())
                  ],
                case Remaining of
                  [] -> zaya_rocksdb:delete(Ref, [EncodedKey]);
                  _ -> zaya_rocksdb:write(Ref, [{EncodedKey, term_to_binary(Remaining)}])
                end;
            end;
          _ ->
            ok
        end
      end,
      ok
    ),
  erlang:send_after(Interval, self(), cleanup),
  {noreply, State}.
```

- [ ] **Step 5: Wire `zaya_db_srv` open flow to use the new recovery API**

```erlang
%% zaya_db_srv.erl, normal open path
Ref = Module:open(default_params(DB, ?dbNodeParams(DB, node()))),
ok = zaya_transaction_log:rollback(
  DB,
  fun({RollbackWrite, RollbackDelete})->
    Module:commit(Ref, RollbackWrite, RollbackDelete)
  end
),
{next_state, register, Data#data{ref = Ref}, [{state_timeout, 0, run}]}.
```

```erlang
%% zaya_db_srv.erl, copied-db paths
Ref = zaya_copy:copy(DB, Module, Params, #{live => not ?dbReadOnly(DB)}),
ok = zaya_transaction_log:purge(DB),
{next_state, {register_copy, InParams, ReplyTo}, Data#data{ref = Ref}, [{state_timeout, 0, run}]}.

Ref = Module:open(default_params(DB, Params)),
ok = zaya_transaction_log:purge(DB),
{next_state, register, Data#data{ref = Ref}, [{state_timeout, 0, run}]}.
```

```erlang
%% zaya.app.src
{env, [
  {transaction_log, #{}}
]},
```

- [ ] **Step 6: Run the suite until it passes**

Run:

```bash
cd /home/roman/PROJECTS/SOURCES/zaya && rebar3 ct --suite test/zaya_transaction_log_SUITE.erl
```

Expected: `DONE 3 tests, 0 skipped, 0 failed`.

- [ ] **Step 7: Commit the log and recovery slice**

```bash
git -C /home/roman/PROJECTS/SOURCES/zaya add \
  src/zaya_transaction_log.erl \
  src/zaya_sup.erl \
  src/zaya_db_srv.erl \
  src/zaya.erl \
  src/zaya.app.src \
  test/zaya_transaction_log_SUITE.erl \
  test/support/zaya_tx_test_backend.erl \
  docs/superpowers/plans/2026-04-20-transaction-refactoring.md
git -C /home/roman/PROJECTS/SOURCES/zaya commit -m "feat: centralize transaction logging"
```

### Task 2: Refactor `zaya_transaction` To The New Worker/Coordinator FSM

**Files:**
- Modify: `/home/roman/PROJECTS/SOURCES/zaya/src/zaya_transaction.erl`
- Create: `/home/roman/PROJECTS/SOURCES/zaya/test/zaya_transaction_SUITE.erl`
- Modify: `/home/roman/PROJECTS/SOURCES/zaya/test/support/zaya_tx_test_backend.erl`

- [ ] **Step 1: Write the failing transaction suite for the new single-node and multi-node flows**

```erlang
-module(zaya_transaction_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1
]).

-export([
  single_db_commit_skips_log_test/1,
  single_node_worker_rolls_back_and_cleans_marker_test/1,
  coordinator_down_peers_converge_on_rollback_test/1
]).

all()->
  [
    single_db_commit_skips_log_test,
    single_node_worker_rolls_back_and_cleans_marker_test,
    coordinator_down_peers_converge_on_rollback_test
  ].

init_per_suite(Config)->
  {ok, _Apps} = application:ensure_all_started(zaya),
  Config.

end_per_suite(_Config)->
  ok = application:stop(zaya),
  ok.

single_db_commit_skips_log_test(_Config)->
  DBs = #{orders => {[{id, committed}], []}},
  ok = zaya_transaction:single_db_node_commit(DBs),
  ?assertEqual([], zaya_transaction_log:list_pending_transactions()).

single_node_worker_rolls_back_and_cleans_marker_test(_Config)->
  DBs = #{
    orders => {[{id, committed}], []},
    audit => {[{id, fail_here}], []}
  },
  zaya_tx_test_backend:fail_once(audit),
  ?assertThrow(_Reason, zaya_transaction:single_node_commit(DBs)),
  ?assertEqual(
    {ok, false},
    zaya_transaction_log:is_rollbacked(zaya_tx_test_backend:last_tref())
  ).

coordinator_down_peers_converge_on_rollback_test(_Config)->
  {ok, PeerA, NodeA} = peer:start_link(#{name => tx_a}),
  {ok, PeerB, NodeB} = peer:start_link(#{name => tx_b}),
  try
    ok = rpc:call(NodeA, application, ensure_all_started, [zaya]),
    ok = rpc:call(NodeB, application, ensure_all_started, [zaya]),
    ok = rpc:call(NodeA, zaya_tx_test_backend, fail_after_confirm, [orders]),
    ?assertMatch(
      {abort, _},
      rpc:call(NodeA, ?MODULE, cross_node_transaction, [])
    )
  after
    peer:stop(PeerA),
    peer:stop(PeerB)
  end.

cross_node_transaction()->
  zaya_transaction:transaction(
    fun()->
      ok = zaya_transaction:write(orders, [{id, committed}], write),
      ok = zaya_transaction:write(audit, [{id, fail_here}], write)
    end
  ).
```

- [ ] **Step 2: Run the suite and verify it fails against the old `commit1/commit2/rollback` flow**

Run:

```bash
cd /home/roman/PROJECTS/SOURCES/zaya && rebar3 ct --suite test/zaya_transaction_SUITE.erl
```

Expected: failures around old `{confirm, W}` coordination, missing peer-to-peer resolution, and references to backend `commit1/commit2/rollback`.

- [ ] **Step 3: Replace the old per-backend phase helpers with request preparation for the new backend API**

```erlang
-record(worker_request, {
  tref,
  dbs,
  all_participating_dbs_nodes,
  any_persistent,
  coordinator
}).

-record(local_commit, {
  db,
  module,
  ref,
  write,
  delete,
  rollback_write,
  rollback_delete,
  persistent
}).

prepare_local_commits(DBs)->
  maps:fold(
    fun(DB, {Write, Delete}, Acc)->
      {Ref, Module} = ?dbRefMod(DB),
      {RollbackWrite, RollbackDelete} = Module:prepare_rollback(Ref, Write, Delete),
      [#local_commit{
        db = DB,
        module = Module,
        ref = Ref,
        write = Write,
        delete = Delete,
        rollback_write = RollbackWrite,
        rollback_delete = RollbackDelete,
        persistent = Module:is_persistent()
      } | Acc]
    end,
    [],
    DBs
  ).
```

- [ ] **Step 4: Implement the single-node isolated worker flow exactly as the spec describes**

```erlang
single_node_commit(DBs, [Node])->
  case ecall:call(Node, ?MODULE, single_node_commit, [DBs]) of
    {ok, _} -> ok;
    {error, Error} -> throw(Error)
  end;
single_node_commit(DBs)->
  TRef = make_ref(),
  Parent = self(),
  {Worker, MRef} =
    spawn_monitor(
      fun()->
        run_single_node_worker(Parent, TRef, DBs)
      end
    ),
  receive
    {'DOWN', MRef, process, Worker, normal} -> ok;
    {'DOWN', MRef, process, Worker, Reason} -> throw(Reason)
  end.
```

```erlang
run_single_node_worker(_Parent, TRef, DBs)->
  LocalCommits = prepare_local_commits(DBs),
  Persistent = [Commit || Commit = #local_commit{persistent = true} <- LocalCommits],
  Seq =
    case Persistent of
      [] ->
        undefined;
      _ ->
        Seq0 = zaya_transaction_log:seq(),
        ok = zaya_transaction_log:commit(
          [
            {{rollback, Commit#local_commit.db, Seq0, TRef},
             {Commit#local_commit.rollback_write, Commit#local_commit.rollback_delete}}
            || Commit <- Persistent
          ] ++
          [{{rollbacked, TRef}, [{Commit#local_commit.db, [node()]} || Commit <- Persistent]}],
          []
        ),
        Seq0
    end,
  try
    [Commit#local_commit.module:commit(
       Commit#local_commit.ref,
       Commit#local_commit.write,
       Commit#local_commit.delete
     ) || Commit <- LocalCommits],
    case Seq of
      undefined -> ok;
      _ ->
        ok = zaya_transaction_log:commit(
          [],
          [
            {rollback, Commit#local_commit.db, Seq, TRef}
            || Commit <- Persistent
          ] ++
          [{rollbacked, TRef}]
        )
    end,
    exit(normal)
  catch
    Class:Reason:Stack->
      [
        Commit#local_commit.module:commit(
          Commit#local_commit.ref,
          Commit#local_commit.rollback_write,
          Commit#local_commit.rollback_delete
        )
        || Commit <- LocalCommits
      ],
      case Seq of
        undefined -> ok;
        _ ->
          ok = zaya_transaction_log:commit(
            [],
            [
              {rollback, Commit#local_commit.db, Seq, TRef}
              || Commit <- Persistent
            ] ++
            [{rollbacked, TRef}]
          )
      end,
      erlang:raise(Class, Reason, Stack)
  end.
```

- [ ] **Step 5: Implement the multi-node coordinator/worker FSM, including peer broadcasts and `pg` resolution**

```erlang
multi_node_commit(Data, #commit{ns = Nodes, ns_dbs = NsDBs, dbs_ns = DBsNs})->
  TRef = make_ref(),
  AllParticipatingDBsNodes = maps:to_list(DBsNs),
  AnyPersistent = lists:any(
    fun(DB)->
      {_Ref, Module} = ?dbRefMod(DB),
      Module:is_persistent()
    end,
    maps:keys(Data)
  ),
  Owner = self(),
  {Coordinator, MRef} =
    spawn_monitor(
      fun()->
        run_multi_node_coordinator(
          Owner,
          TRef,
          Data,
          Nodes,
          NsDBs,
          AllParticipatingDBsNodes,
          AnyPersistent
        )
      end
    ),
  receive
    {'DOWN', MRef, process, Coordinator, normal} -> ok;
    {'DOWN', MRef, process, Coordinator, Reason} -> throw(Reason)
  end.
```

```erlang
run_multi_node_coordinator(Owner, TRef, Data, Nodes, NsDBs, AllParticipatingDBsNodes, AnyPersistent)->
  Workers =
    [
      spawn_monitor(
        fun()->
          commit_request(#worker_request{
            tref = TRef,
            dbs = maps:with(maps:get(Node, NsDBs), Data),
            all_participating_dbs_nodes = AllParticipatingDBsNodes,
            any_persistent = AnyPersistent,
            coordinator = self()
          })
        end
      )
      || Node <- Nodes
    ],
  receive
    {commit1, confirm, _Worker} ->
      [ecall:send(Node, {commit2, Workers}) || {_Pid, Node} <- Workers],
      Owner ! ok
  end.

commit_request(#worker_request{
  tref = TRef,
  dbs = DBs,
  all_participating_dbs_nodes = AllParticipatingDBsNodes,
  any_persistent = AnyPersistent,
  coordinator = Coordinator
})->
  erlang:monitor(process, Coordinator),
  LocalCommits = prepare_local_commits(DBs),
  LocalPersistent = [Commit || Commit = #local_commit{persistent = true} <- LocalCommits],
  Seq =
    case LocalPersistent of
      [] ->
        undefined;
      _ ->
        Seq0 = zaya_transaction_log:seq(),
        ok = zaya_transaction_log:commit(
          [
            {{rollback, Commit#local_commit.db, Seq0, TRef},
             {Commit#local_commit.rollback_write, Commit#local_commit.rollback_delete}}
            || Commit <- LocalPersistent
          ],
          []
        ),
        Seq0
    end,
  [Commit#local_commit.module:commit(
     Commit#local_commit.ref,
     Commit#local_commit.write,
     Commit#local_commit.delete
   ) || Commit <- LocalCommits],
  ecall:send(Coordinator, {commit1, confirm, self()}),
  receive
    {commit2, _Workers} ->
      ok;
    {rollback, _Workers} ->
      ok = zaya_transaction_log:commit(
        [{{rollbacked, TRef}, AllParticipatingDBsNodes}],
        []
      ),
      [
        Commit#local_commit.module:commit(
          Commit#local_commit.ref,
          Commit#local_commit.rollback_write,
          Commit#local_commit.rollback_delete
        )
        || Commit <- LocalCommits
      ]
  end,
  case Seq of
    undefined -> ok;
    _ ->
      ok = zaya_transaction_log:commit(
        [],
        [{rollback, Commit#local_commit.db, Seq, TRef} || Commit <- LocalPersistent]
      )
  end.
```

- [ ] **Step 6: Run the transaction suite until it passes**

Run:

```bash
cd /home/roman/PROJECTS/SOURCES/zaya && rebar3 ct --suite test/zaya_transaction_SUITE.erl
```

Expected: `DONE 3 tests, 0 skipped, 0 failed`.

- [ ] **Step 7: Commit the transaction-orchestration refactor**

```bash
git -C /home/roman/PROJECTS/SOURCES/zaya add \
  src/zaya_transaction.erl \
  test/zaya_transaction_SUITE.erl \
  test/support/zaya_tx_test_backend.erl
git -C /home/roman/PROJECTS/SOURCES/zaya commit -m "feat: refactor distributed transaction flow"
```

### Task 3: Migrate `zaya_rocksdb` And `zaya_leveldb` To The Reduced Backend API

**Files:**
- Modify: `/home/roman/PROJECTS/SOURCES/zaya_rocksdb/src/zaya_rocksdb.erl`
- Modify: `/home/roman/PROJECTS/SOURCES/zaya_rocksdb/test/zaya_rocksdb_SUITE.erl`
- Modify: `/home/roman/PROJECTS/SOURCES/zaya_leveldb/src/zaya_leveldb.erl`
- Create: `/home/roman/PROJECTS/SOURCES/zaya_leveldb/test/zaya_leveldb_SUITE.erl`

- [ ] **Step 1: Write failing tests for the new backend surface**

```erlang
%% Add to zaya_rocksdb_SUITE.erl
-export([
  prepare_rollback_roundtrip_test/1,
  is_persistent_test/1
]).

prepare_rollback_roundtrip_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_rocksdb:write(Ref, [{item, original}]),
      {RollbackWrite, RollbackDelete} =
        zaya_rocksdb:prepare_rollback(Ref, [{item, updated}], []),
      ok = zaya_rocksdb:commit(Ref, [{item, updated}], []),
      ok = zaya_rocksdb:commit(Ref, RollbackWrite, RollbackDelete),
      ?assertEqual([{item, original}], zaya_rocksdb:read(Ref, [item]))
    end
  ).

is_persistent_test(_Config)->
  ?assertEqual(true, zaya_rocksdb:is_persistent()).
```

```erlang
%% New zaya_leveldb_SUITE.erl
-module(zaya_leveldb_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([prepare_rollback_roundtrip_test/1, is_persistent_test/1]).

all()->
  [prepare_rollback_roundtrip_test, is_persistent_test].
```

- [ ] **Step 2: Run the focused backend suites and verify they fail on the removed API**

Run:

```bash
cd /home/roman/PROJECTS/SOURCES/zaya_rocksdb && rebar3 ct --suite test/zaya_rocksdb_SUITE.erl
cd /home/roman/PROJECTS/SOURCES/zaya_leveldb && rebar3 ct --suite test/zaya_leveldb_SUITE.erl
```

Expected: failures because `prepare_rollback/3` and `is_persistent/0` are not exported yet, and old log-recovery tests still target `commit1/commit2/rollback`.

- [ ] **Step 3: Remove backend-owned log storage and export the reduced API**

```erlang
%% zaya_rocksdb.erl and zaya_leveldb.erl
-export([
  commit/3,
  prepare_rollback/3,
  is_persistent/0
]).

-record(ref, {ref, read, write, dir, pool}).

is_persistent()->
  true.

prepare_rollback(Ref, Write, Delete)->
  Commit =
    [{put, K, V} || {K, V} <- Write] ++
    [{delete, K} || K <- Delete],
  rollback_from_commit(Commit, Ref).

rollback_from_commit([{put, Key, _Value} | Rest], Ref)->
  Read = read(Ref, [Key]),
  case Read of
    [{Key, Existing}] ->
      {[{Key, Existing} | element(1, rollback_from_commit(Rest, Ref))], element(2, rollback_from_commit(Rest, Ref))};
    [] ->
      {Writes, Deletes} = rollback_from_commit(Rest, Ref),
      {Writes, [Key | Deletes]}
  end;
rollback_from_commit([{delete, Key} | Rest], Ref)->
  Read = read(Ref, [Key]),
  case Read of
    [{Key, Existing}] ->
      {Writes, Deletes} = rollback_from_commit(Rest, Ref),
      {[{Key, Existing} | Writes], Deletes};
    [] ->
      rollback_from_commit(Rest, Ref)
  end;
rollback_from_commit([], _Ref)->
  {[], []}.
```

```erlang
%% create/open should stop creating /LOG directories
DataDir = Dir ++ "/DATA",
ensure_dir(DataDir),
#ref{
  ref = try_open(DataDir, Options),
  read = maps:to_list(Read),
  write = maps:to_list(Write),
  dir = Dir
}.
```

- [ ] **Step 4: Update the test suites to stop referring to pending-commit recovery**

```erlang
%% zaya_rocksdb_SUITE.erl
mode_tests()->
  [
    service_api_and_info_test,
    low_level_api_test,
    iterator_navigation_test,
    find_query_variants_test,
    fold_and_copy_test,
    dump_batch_test,
    transaction_api_test,
    prepare_rollback_roundtrip_test,
    is_persistent_test,
    concurrent_write_callers_test
  ].
```

- [ ] **Step 5: Run both suites until they pass**

Run:

```bash
cd /home/roman/PROJECTS/SOURCES/zaya_rocksdb && rebar3 ct --suite test/zaya_rocksdb_SUITE.erl
cd /home/roman/PROJECTS/SOURCES/zaya_leveldb && rebar3 ct --suite test/zaya_leveldb_SUITE.erl
```

Expected: both repos report `0 failed`.

- [ ] **Step 6: Commit the persistent-backend migrations**

```bash
git -C /home/roman/PROJECTS/SOURCES/zaya_rocksdb add src/zaya_rocksdb.erl test/zaya_rocksdb_SUITE.erl
git -C /home/roman/PROJECTS/SOURCES/zaya_rocksdb commit -m "refactor: remove backend transaction log from rocksdb"

git -C /home/roman/PROJECTS/SOURCES/zaya_leveldb add src/zaya_leveldb.erl test/zaya_leveldb_SUITE.erl
git -C /home/roman/PROJECTS/SOURCES/zaya_leveldb commit -m "refactor: remove backend transaction log from leveldb"
```

### Task 4: Migrate `zaya_ets` And `zaya_ets_rocksdb`

**Files:**
- Modify: `/home/roman/PROJECTS/SOURCES/zaya_ets/src/zaya_ets.erl`
- Modify: `/home/roman/PROJECTS/SOURCES/zaya_ets/test/zaya_ets_SUITE.erl`
- Modify: `/home/roman/PROJECTS/SOURCES/zaya_ets_rocksdb/src/zaya_ets_rocksdb.erl`
- Modify: `/home/roman/PROJECTS/SOURCES/zaya_ets_rocksdb/test/zaya_ets_rocksdb_SUITE.erl`

- [ ] **Step 1: Write failing tests for `prepare_rollback/3` and `is_persistent/0`**

```erlang
%% Add to zaya_ets_SUITE.erl
-export([
  prepare_rollback_roundtrip_test/1,
  is_persistent_test/1
]).

prepare_rollback_roundtrip_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_ets:write(Ref, [{item, original}]),
      {RollbackWrite, RollbackDelete} =
        zaya_ets:prepare_rollback(Ref, [{item, updated}], []),
      ok = zaya_ets:commit(Ref, [{item, updated}], []),
      ok = zaya_ets:commit(Ref, RollbackWrite, RollbackDelete),
      ?assertEqual([{item, original}], zaya_ets:read(Ref, [item]))
    end
  ).

is_persistent_test(_Config)->
  ?assertEqual(false, zaya_ets:is_persistent()).
```

```erlang
%% Add the same two tests to zaya_ets_rocksdb_SUITE.erl,
%% but assert true for zaya_ets_rocksdb:is_persistent().
```

- [ ] **Step 2: Run the suites and verify they fail before the API migration**

Run:

```bash
cd /home/roman/PROJECTS/SOURCES/zaya_ets && rebar3 ct --suite test/zaya_ets_SUITE.erl
cd /home/roman/PROJECTS/SOURCES/zaya_ets_rocksdb && rebar3 ct --suite test/zaya_ets_rocksdb_SUITE.erl
```

Expected: failures because the repos still export `commit1/commit2/rollback`.

- [ ] **Step 3: Replace the old transaction API with the reduced one**

```erlang
%% zaya_ets.erl
-export([
  commit/3,
  prepare_rollback/3,
  is_persistent/0
]).

is_persistent()->
  false.

prepare_rollback(#ref{table = Table}, Write, Delete)->
  WriteBack =
    lists:foldl(
      fun({Key, _Value}, Acc)->
        case ets:lookup(Table, Key) of
          [{Key, Existing}] -> [{Key, Existing} | Acc];
          [] -> Acc
        end
      end,
      [],
      Write
    ),
  DeleteBack =
    [Key || Key <- maps:keys(maps:from_list(Write)), ets:lookup(Table, Key) =:= []],
  {WriteBack, DeleteBack ++ Delete}.
```

```erlang
%% zaya_ets_rocksdb.erl
-export([
  commit/3,
  prepare_rollback/3,
  is_persistent/0
]).

is_persistent()->
  true.

prepare_rollback(#ref{ets = EtsRef}, Write, Delete)->
  zaya_ets:prepare_rollback(EtsRef, Write, Delete).
```

- [ ] **Step 4: Update suite lists to replace old pending-commit tests with rollback-preparation tests**

```erlang
mode_tests()->
  [
    service_api_and_info_test,
    low_level_api_test,
    iterator_navigation_test,
    find_query_variants_test,
    fold_and_copy_test,
    dump_batch_and_pool_batch_test,
    transaction_api_test,
    prepare_rollback_roundtrip_test,
    is_persistent_test,
    concurrent_write_callers_test
  ].
```

- [ ] **Step 5: Run both suites until they pass**

Run:

```bash
cd /home/roman/PROJECTS/SOURCES/zaya_ets && rebar3 ct --suite test/zaya_ets_SUITE.erl
cd /home/roman/PROJECTS/SOURCES/zaya_ets_rocksdb && rebar3 ct --suite test/zaya_ets_rocksdb_SUITE.erl
```

Expected: both repos report `0 failed`.

- [ ] **Step 6: Commit the in-memory and mixed-backend migrations**

```bash
git -C /home/roman/PROJECTS/SOURCES/zaya_ets add src/zaya_ets.erl test/zaya_ets_SUITE.erl
git -C /home/roman/PROJECTS/SOURCES/zaya_ets commit -m "refactor: shrink ets transaction api"

git -C /home/roman/PROJECTS/SOURCES/zaya_ets_rocksdb add src/zaya_ets_rocksdb.erl test/zaya_ets_rocksdb_SUITE.erl
git -C /home/roman/PROJECTS/SOURCES/zaya_ets_rocksdb commit -m "refactor: shrink ets rocksdb transaction api"
```

### Task 5: Migrate The Remaining Declared Dependencies And Run Full Verification

**Files:**
- Modify: `/home/roman/PROJECTS/SOURCES/zaya/rebar.config`
- Modify: `/home/roman/PROJECTS/SOURCES/zaya_ets_leveldb/src/zaya_ets_leveldb.erl`
- Modify: `/home/roman/PROJECTS/SOURCES/zaya_pterm/src/zaya_pterm.erl`
- Modify: `/home/roman/PROJECTS/SOURCES/zaya_pterm_leveldb/src/zaya_pterm_leveldb.erl`
- Modify or create tests in each missing repo under `/home/roman/PROJECTS/SOURCES/<repo>/test/`

- [ ] **Step 1: Check out the missing repos and create or switch to the `pool` branches**

```bash
git clone https://github.com/vzroman/zaya_ets_leveldb.git /home/roman/PROJECTS/SOURCES/zaya_ets_leveldb
git clone https://github.com/vzroman/zaya_pterm.git /home/roman/PROJECTS/SOURCES/zaya_pterm
git clone https://github.com/vzroman/zaya_pterm_leveldb.git /home/roman/PROJECTS/SOURCES/zaya_pterm_leveldb

git -C /home/roman/PROJECTS/SOURCES/zaya_ets_leveldb checkout -b pool
git -C /home/roman/PROJECTS/SOURCES/zaya_pterm checkout -b pool
git -C /home/roman/PROJECTS/SOURCES/zaya_pterm_leveldb checkout -b pool
```

- [ ] **Step 2: Apply the same reduced transaction API as in Tasks 3 and 4**

```erlang
%% zaya_pterm.erl
-export([
  commit/3,
  prepare_rollback/3,
  is_persistent/0
]).

is_persistent()->
  false.

prepare_rollback(Ref, Write, Delete)->
  RollbackWrite =
    lists:foldl(
      fun({Key, _Value}, Acc)->
        case read(Ref, [Key]) of
          [{Key, Existing}] -> [{Key, Existing} | Acc];
          [] -> Acc
        end
      end,
      [],
      Write
    ),
  RollbackDelete =
    lists:foldl(
      fun({Key, _Value}, Acc)->
        case read(Ref, [Key]) of
          [] -> [Key | Acc];
          [_] -> Acc
        end
      end,
      Delete,
      Write
    ),
  {lists:reverse(RollbackWrite), lists:reverse(RollbackDelete)}.
```

```erlang
%% zaya_ets_leveldb.erl and zaya_pterm_leveldb.erl
-export([
  commit/3,
  prepare_rollback/3,
  is_persistent/0
]).

is_persistent()->
  true.

prepare_rollback(Ref, Write, Delete)->
  zaya_ets:prepare_rollback(Ref, Write, Delete).
```

- [ ] **Step 3: Update `zaya/rebar.config` to depend on the new `pool` branches for the repos migrated in this task**

```erlang
{deps, [
    {zaya_ets_leveldb, {git, "https://github.com/vzroman/zaya_ets_leveldb.git", {branch, "pool"}}},
    {zaya_pterm, {git, "https://github.com/vzroman/zaya_pterm.git", {branch, "pool"}}},
    {zaya_pterm_leveldb, {git, "https://github.com/vzroman/zaya_pterm_leveldb.git", {branch, "pool"}}}
]}.
```

- [ ] **Step 4: Run full verification across the touched repos**

Run:

```bash
cd /home/roman/PROJECTS/SOURCES/zaya && rebar3 compile
cd /home/roman/PROJECTS/SOURCES/zaya && rebar3 ct --suite test/zaya_transaction_log_SUITE.erl --suite test/zaya_transaction_SUITE.erl

cd /home/roman/PROJECTS/SOURCES/zaya_rocksdb && rebar3 ct --suite test/zaya_rocksdb_SUITE.erl
cd /home/roman/PROJECTS/SOURCES/zaya_leveldb && rebar3 ct --suite test/zaya_leveldb_SUITE.erl
cd /home/roman/PROJECTS/SOURCES/zaya_ets && rebar3 ct --suite test/zaya_ets_SUITE.erl
cd /home/roman/PROJECTS/SOURCES/zaya_ets_rocksdb && rebar3 ct --suite test/zaya_ets_rocksdb_SUITE.erl
cd /home/roman/PROJECTS/SOURCES/zaya_ets_leveldb && rebar3 ct
cd /home/roman/PROJECTS/SOURCES/zaya_pterm && rebar3 ct
cd /home/roman/PROJECTS/SOURCES/zaya_pterm_leveldb && rebar3 ct
```

Expected: every repo compiles cleanly, all Common Test suites finish with `0 failed`, and `zaya` no longer contains any live references to `commit1/commit2/rollback` in transaction paths.

- [ ] **Step 5: Commit the remaining repo migrations and the dependency update**

```bash
git -C /home/roman/PROJECTS/SOURCES/zaya add rebar.config
git -C /home/roman/PROJECTS/SOURCES/zaya commit -m "chore: point remaining backends at pool branches"

git -C /home/roman/PROJECTS/SOURCES/zaya_ets_leveldb add src test
git -C /home/roman/PROJECTS/SOURCES/zaya_ets_leveldb commit -m "refactor: shrink ets leveldb transaction api"

git -C /home/roman/PROJECTS/SOURCES/zaya_pterm add src test
git -C /home/roman/PROJECTS/SOURCES/zaya_pterm commit -m "refactor: shrink pterm transaction api"

git -C /home/roman/PROJECTS/SOURCES/zaya_pterm_leveldb add src test
git -C /home/roman/PROJECTS/SOURCES/zaya_pterm_leveldb commit -m "refactor: shrink pterm leveldb transaction api"
```

## Self-Review

**Spec coverage**

- Central `zaya_transaction_log` process, per-node RocksDB storage, `seq/0`, direct `commit/2`, `is_rollbacked/1`, `rollback/2`, `purge/1`, and `list_pending_transactions/0`: covered in Task 1.
- DB-open recovery rules, copied-DB purge path, and public `zaya:list_pending_transactions/0`: covered in Task 1.
- Single-DB, single-node, and multi-node transaction execution changes in `zaya_transaction.erl`: covered in Task 2.
- Removal of backend-owned `commit1/commit2/rollback` and backend log directories: covered in Tasks 3, 4, and 5.
- Remaining repos declared in `zaya/rebar.config` but not present locally: handled explicitly in Task 5.

**Placeholder scan**

- No `TODO`, `TBD`, or "implement later" placeholders remain in the task steps.
- Each task names concrete files, concrete commands, and the API surface expected after the change.
- The only external dependency is checkout of the three missing repos; that prerequisite is made explicit in Task 5 Step 1.

**Type consistency**

- The plan consistently uses `commit(Ref, Write, Delete)`, `prepare_rollback(Ref, Write, Delete)`, and `is_persistent()`.
- Transaction-log keys are consistently described as semantic Erlang terms serialized internally with `sext`.
- The public `zaya` entrypoint is consistently `list_pending_transactions/0`.
