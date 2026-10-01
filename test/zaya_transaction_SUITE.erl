-module(zaya_transaction_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("zaya_atoms.hrl").

-export([
  all/0,
  groups/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_group/2,
  end_per_group/2
]).

-export([
  single_db_node_commit_uses_local_fast_path_test/1,
  single_db_commit_skips_log_test/1,
  single_node_worker_rolls_back_and_cleans_marker_test/1,
  multi_node_worker_rolls_back_after_coordinator_decision_test/1
]).

%% locks
-export([
  read_lock_test/1,
  write_lock_test/1,
  no_lock_test/1,
  empty_keys_request_test/1,
  locks_are_released_on_abort_test/1,
  locks_are_released_when_commit_fails_test/1,
  locks_are_released_when_read_fails_test/1,
  locks_are_released_when_request_is_malformed_test/1,
  locks_are_released_when_caller_dies_test/1,
  transaction_waits_for_db_lock_test/1,
  locks_of_several_dbs_test/1,
  key_equal_to_db_lock_name_test/1,

  upgrade_test/1,
  upgrade_is_released_on_abort_test/1,
  upgrade_by_any_write_locked_request_test/1,
  repeated_requests_take_no_new_locks_test/1,
  write_then_read_keeps_write_lock_test/1,
  mixed_keys_request_test/1,
  upgrade_waits_for_other_readers_test/1,

  repeated_key_in_request_test/1,
  repeated_key_upgrade_test/1,

  nested_commit_keeps_locks_test/1,
  nested_abort_releases_inner_locks_test/1,
  nested_abort_downgrades_upgraded_lock_test/1,
  nested_abort_then_upgrade_again_test/1,
  nested_abort_keeps_parent_write_lock_test/1,
  nested_abort_releases_inner_db_lock_test/1,
  deep_nested_abort_test/1,
  nested_lock_error_restarts_whole_transaction_test/1,

  failed_request_releases_its_locks_test/1,
  failed_request_keeps_held_locks_test/1,
  failed_upgrade_request_keeps_read_lock_test/1,
  failed_first_request_releases_db_lock_test/1,
  lock_error_restarts_transaction_test/1,
  upgrade_lock_error_restarts_transaction_test/1,
  lock_error_aborts_after_attempts_test/1,
  unavailable_db_aborts_and_releases_test/1,
  invalid_lock_type_aborts_and_releases_test/1,

  readers_share_writer_waits_test/1,
  writer_blocks_reader_test/1,
  concurrent_upgrades_keep_updates_test/1,
  many_concurrent_upgrades_keep_updates_test/1,
  deadlocked_transactions_restart_and_commit_test/1
]).

%% cluster_locks
-export([
  cluster_read_lock_is_local_test/1,
  cluster_write_lock_is_set_on_all_copies_test/1,
  cluster_upgrade_test/1,
  cluster_nested_abort_downgrades_upgraded_lock_test/1,
  cluster_failed_upgrade_request_test/1,
  cluster_remote_reader_waits_for_writer_test/1,
  cluster_locks_are_released_when_caller_dies_test/1,
  cluster_no_local_copy_test/1
]).

%% The peer node loads the suite to run the transactions of the cluster cases
-export([
  read_transaction/2
]).

-record(commit_request, {
  tref,
  dbs,
  scope,
  is_persistent,
  coordinator
}).

-record(msg, {
  tref,
  type,
  data
}).

%% The state of a lock as the probes see it: {Shared, Exclusive}, each is
%% free if the probe gets the lock and busy if it doesn't (see lock_state/2)
-define(FREE, {free, free}).
-define(READ_LOCKED, {free, busy}).
-define(WRITE_LOCKED, {busy, busy}).

%% How long a probe waits for the lock before it reports it busy
-define(PROBE_TIMEOUT, 200).
%% How long a helper process waits for its lock if the case goes wrong
-define(HELPER_TIMEOUT, 30000).
%% The locks a trap holds to win a deadlock (see start_trap/2)
-define(TRAP_WEIGHT, 50).

all() ->
  [
    {group, commit},
    {group, locks},
    {group, cluster_locks}
  ].

groups() ->
  [
    {commit, [], [
      single_db_node_commit_uses_local_fast_path_test,
      single_db_commit_skips_log_test,
      single_node_worker_rolls_back_and_cleans_marker_test,
      multi_node_worker_rolls_back_after_coordinator_decision_test
    ]},
    {locks, [], [
      read_lock_test,
      write_lock_test,
      no_lock_test,
      empty_keys_request_test,
      locks_are_released_on_abort_test,
      locks_are_released_when_commit_fails_test,
      locks_are_released_when_read_fails_test,
      locks_are_released_when_request_is_malformed_test,
      locks_are_released_when_caller_dies_test,
      transaction_waits_for_db_lock_test,
      locks_of_several_dbs_test,
      key_equal_to_db_lock_name_test,

      upgrade_test,
      upgrade_is_released_on_abort_test,
      upgrade_by_any_write_locked_request_test,
      repeated_requests_take_no_new_locks_test,
      write_then_read_keeps_write_lock_test,
      mixed_keys_request_test,
      upgrade_waits_for_other_readers_test,

      repeated_key_in_request_test,
      repeated_key_upgrade_test,

      nested_commit_keeps_locks_test,
      nested_abort_releases_inner_locks_test,
      nested_abort_downgrades_upgraded_lock_test,
      nested_abort_then_upgrade_again_test,
      nested_abort_keeps_parent_write_lock_test,
      nested_abort_releases_inner_db_lock_test,
      deep_nested_abort_test,
      nested_lock_error_restarts_whole_transaction_test,

      failed_request_releases_its_locks_test,
      failed_request_keeps_held_locks_test,
      failed_upgrade_request_keeps_read_lock_test,
      failed_first_request_releases_db_lock_test,
      lock_error_restarts_transaction_test,
      upgrade_lock_error_restarts_transaction_test,
      lock_error_aborts_after_attempts_test,
      unavailable_db_aborts_and_releases_test,
      invalid_lock_type_aborts_and_releases_test,

      readers_share_writer_waits_test,
      writer_blocks_reader_test,
      concurrent_upgrades_keep_updates_test,
      many_concurrent_upgrades_keep_updates_test,
      deadlocked_transactions_restart_and_commit_test
    ]},
    {cluster_locks, [], [
      cluster_read_lock_is_local_test,
      cluster_write_lock_is_set_on_all_copies_test,
      cluster_upgrade_test,
      cluster_nested_abort_downgrades_upgraded_lock_test,
      cluster_failed_upgrade_request_test,
      cluster_remote_reader_waits_for_writer_test,
      cluster_locks_are_released_when_caller_dies_test,
      cluster_no_local_copy_test
    ]}
  ].

%% The cluster cases run on two nodes: this one and a peer attached to it
init_per_group(cluster_locks, Config) ->
  PrivDir = ?config(priv_dir, Config),
  CodePath =
    lists:append([["-pa", Dir] || Dir <- code:get_path(), string:find(Dir, "_build") =/= nomatch]),
  {ok, Peer, PeerNode} =
    peer:start(#{
      name => peer:random_name(),
      args => CodePath,
      env => [{"ATTACH_TO", atom_to_list(node())}]
    }),
  ok = rpc:call(PeerNode, application, set_env, [zaya, schema_dir, filename:join(PrivDir, "peer-schema")]),
  ok =
    rpc:call(PeerNode, application, set_env, [
      zaya,
      transaction_log,
      #{
        dir => filename:join(PrivDir, "peer-tlog"),
        pool => disabled,
        cleanup_interval_ms => 1000
      }
    ]),
  ok = rpc:call(PeerNode, zaya_ct, start_zaya, []),
  ok = wait_until(fun() -> lists:sort(zaya:ready_nodes()) =:= lists:sort([node(), PeerNode]) end),
  [{peer, Peer}, {peer_node, PeerNode} | Config];
init_per_group(_Group, Config) ->
  Config.

end_per_group(cluster_locks, Config) ->
  PeerNode = ?config(peer_node, Config),
  catch peer:stop(?config(peer, Config)),
  ok = wait_until(fun() -> not zaya:is_node_ready(PeerNode) end),
  % The node must leave the schema: the next start of zaya would wait for it
  _ = zaya:remove_node(PeerNode),
  ok = wait_until(fun() -> zaya:all_nodes() =:= [node()] end),
  ok;
end_per_group(_Group, _Config) ->
  ok.

init_per_suite(Config) ->
  PrivDir = ?config(priv_dir, Config),
  ok = ensure_distributed(),
  ok = load_support_backend(PrivDir),
  ok = zaya_ct:stop_zaya(),
  application:set_env(zaya, schema_dir, filename:join(PrivDir, "schema")),
  application:set_env(
    zaya,
    transaction_log,
    #{
      dir => filename:join(PrivDir, "tlog"),
      pool => disabled,
      cleanup_interval_ms => 1000
    }
  ),
  ok = zaya_ct:start_zaya(),
  Config.

end_per_suite(_Config) ->
  ok = zaya_ct:stop_zaya(),
  ok.

single_db_node_commit_uses_local_fast_path_test(Config) ->
  DB = test_db(single_db_node_commit_uses_local_fast_path_test),
  Params = db_params(Config, "single-db-local-node"),
  FullParams = zaya_db_srv:default_params(DB, Params),
  try
    ok = setup_local_db(DB, Params, [{item, old_value}]),
    ok = zaya_transaction:single_db_node_commit(
      #{DB => {[{item, committed_value}], []}},
      [node()]
    ),
    ?assertEqual([{item, committed_value}], zaya:read(DB, [item])),
    ?assertEqual(self(), zaya_tx_test_backend:last_commit_pid(FullParams))
  after
    cleanup_db(DB, FullParams)
  end.

single_db_commit_skips_log_test(Config) ->
  DB = test_db(single_db_commit_skips_log_test),
  Params = db_params(Config, "single-db"),
  FullParams = zaya_db_srv:default_params(DB, Params),
  try
    ok = setup_local_db(DB, Params, [{item, old_value}]),
    _ = zaya_transaction:single_db_node_commit(
      #{DB => {[{item, committed_value}], []}}
    ),
    ?assertEqual([{item, committed_value}], zaya:read(DB, [item])),
    ?assertEqual(1, zaya_tx_test_backend:commit_count(FullParams)),
    ok = restart_db(DB),
    ?assertEqual([{item, committed_value}], zaya:read(DB, [item])),
    ?assertEqual(1, zaya_tx_test_backend:commit_count(FullParams)),
    ?assertEqual(#{}, zaya:list_pending_transactions())
  after
    cleanup_db(DB, FullParams)
  end.

single_node_worker_rolls_back_and_cleans_marker_test(Config) ->
  Orders = test_db(single_node_orders),
  Audit = test_db(single_node_audit),
  OrdersParams = db_params(Config, "single-node-orders"),
  AuditParams = db_params(Config, "single-node-audit"),
  OrdersFullParams = zaya_db_srv:default_params(Orders, OrdersParams),
  AuditFullParams = zaya_db_srv:default_params(Audit, AuditParams),
  try
    ok = setup_local_db(Orders, OrdersParams, [{item, old_orders}]),
    ok = setup_local_db(Audit, AuditParams, [{item, old_audit}]),
    ok = zaya_tx_test_backend:fail_after_confirm(AuditFullParams),
    ?assertException(
      error,
      _,
      zaya_transaction:single_node_commit(
        #{
          Orders => {[{item, new_orders}], []},
          Audit => {[{item, new_audit}], []}
        }
      )
    ),
    ?assertEqual([{item, old_orders}], zaya:read(Orders, [item])),
    ?assertEqual([{item, old_audit}], zaya:read(Audit, [item])),
    ?assertEqual(2, zaya_tx_test_backend:commit_count(AuditFullParams)),
    OrdersCommits = zaya_tx_test_backend:commit_count(OrdersFullParams),
    ok = restart_db(Orders),
    ok = restart_db(Audit),
    ?assertEqual([{item, old_orders}], zaya:read(Orders, [item])),
    ?assertEqual([{item, old_audit}], zaya:read(Audit, [item])),
    ?assertEqual(OrdersCommits, zaya_tx_test_backend:commit_count(OrdersFullParams)),
    ?assertEqual(2, zaya_tx_test_backend:commit_count(AuditFullParams)),
    ?assertEqual(#{}, zaya:list_pending_transactions())
  after
    cleanup_db(Orders, OrdersFullParams),
    cleanup_db(Audit, AuditFullParams)
  end.

multi_node_worker_rolls_back_after_coordinator_decision_test(Config) ->
  Orders = test_db(multi_node_worker_orders),
  Audit = test_db(multi_node_worker_audit),
  OrdersParams = db_params(Config, "multi-node-orders"),
  AuditParams = db_params(Config, "multi-node-audit"),
  OrdersFullParams = zaya_db_srv:default_params(Orders, OrdersParams),
  AuditFullParams = zaya_db_srv:default_params(Audit, AuditParams),
  try
    ok = setup_local_db(Orders, OrdersParams, [{item, old_orders}]),
    ok = setup_local_db(Audit, AuditParams, [{item, old_audit}]),
    Coordinator = self(),
    TRef = make_ref(),
    {Worker, MRef} =
      spawn_monitor(
        fun() ->
          zaya_transaction:commit_request(
            #commit_request{
              tref = TRef,
              dbs = #{
                Orders => {[{item, new_orders}], []},
                Audit => {[{item, new_audit}], []}
              },
              scope = [
                {Orders, [node()]},
                {Audit, [node()]}
              ],
              is_persistent = true,
              coordinator = Coordinator
            }
          )
        end
      ),
    receive
      #msg{tref = TRef, type = commit1, data = Worker} ->
        ok
    after 1000 ->
      ct:fail(commit1_confirm_timeout)
    end,
    Worker ! #msg{tref = TRef, type = abort, data = [Worker]},
    receive
      {'DOWN', MRef, process, Worker, normal} ->
        ok;
      {'DOWN', MRef, process, Worker, Reason} ->
        ct:fail({unexpected_worker_exit, Reason})
    after 1000 ->
      ct:fail(worker_exit_timeout)
    end,
    ?assertEqual([{item, old_orders}], zaya:read(Orders, [item])),
    ?assertEqual([{item, old_audit}], zaya:read(Audit, [item])),
    ?assertEqual(2, zaya_tx_test_backend:commit_count(OrdersFullParams)),
    ?assertEqual(2, zaya_tx_test_backend:commit_count(AuditFullParams)),
    OrdersCommits = zaya_tx_test_backend:commit_count(OrdersFullParams),
    ok = restart_db(Orders),
    ok = restart_db(Audit),
    ?assertEqual([{item, old_orders}], zaya:read(Orders, [item])),
    ?assertEqual([{item, old_audit}], zaya:read(Audit, [item])),
    ?assertEqual(
      OrdersCommits,
      zaya_tx_test_backend:commit_count(OrdersFullParams)
    ),
    ?assertEqual(2, zaya_tx_test_backend:commit_count(AuditFullParams)),
    ?assertEqual(#{}, zaya:list_pending_transactions())
  after
    cleanup_db(Orders, OrdersFullParams),
    cleanup_db(Audit, AuditFullParams)
  end.

%%=================================================================
%%  LOCKS
%%
%%  A case asks the transaction for locks and checks what is locked
%%  inside of the transaction and that nothing is locked after it.
%%  The transaction runs in the process of the case, which stays alive
%%  while the locks are checked: a lock that is not released by the
%%  transaction itself stays held and is seen
%%=================================================================
%%-----------------------------------------------------------------
%%  A read lock is shared, whatever request takes it. The first lock
%%  of a transaction in a DB also locks the DB itself for read
%%-----------------------------------------------------------------
read_lock_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [r], read),
        ok = zaya:write(DB, [{w, v}], read),
        ok = zaya:delete(DB, [d], read),
        held_locks(DB, [r, w, d])
      end),
    ?assertEqual(
      {ok, [
        {node(), db, ?READ_LOCKED},
        {node(), {key, r}, ?READ_LOCKED},
        {node(), {key, w}, ?READ_LOCKED},
        {node(), {key, d}, ?READ_LOCKED}
      ]},
      Result
    ),
    ?assertEqual([], held_locks(DB, [r, w, d])),
    ?assertNot(zaya:is_transaction())
  end).

%%-----------------------------------------------------------------
%%  A write lock is exclusive, whatever request takes it. The DB is
%%  still locked for read
%%-----------------------------------------------------------------
write_lock_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [r], write),
        ok = zaya:write(DB, [{w, v}], write),
        ok = zaya:delete(DB, [d], write),
        held_locks(DB, [r, w, d])
      end),
    ?assertEqual(
      {ok, [
        {node(), db, ?READ_LOCKED},
        {node(), {key, r}, ?WRITE_LOCKED},
        {node(), {key, w}, ?WRITE_LOCKED},
        {node(), {key, d}, ?WRITE_LOCKED}
      ]},
      Result
    ),
    ?assertEqual([], held_locks(DB, [r, w, d])),
    ?assertEqual([{w, v}], zaya:read(DB, [r, w, d]))
  end).

%%-----------------------------------------------------------------
%%  The requests without a lock lock nothing, the DB as well
%%-----------------------------------------------------------------
no_lock_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [r], none),
        ok = zaya:write(DB, [{w, v}], none),
        ok = zaya:delete(DB, [d], none),
        held_locks(DB, [r, w, d])
      end),
    ?assertEqual({ok, []}, Result),
    ?assertEqual([], held_locks(DB, [r, w, d])),
    ?assertEqual([{w, v}], zaya:read(DB, [r, w, d]))
  end).

%%-----------------------------------------------------------------
%%  A request without keys locks only the DB
%%-----------------------------------------------------------------
empty_keys_request_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [], read),
        ok = zaya:write(DB, [], write),
        held_locks(DB, [])
      end),
    ?assertEqual({ok, [{node(), db, ?READ_LOCKED}]}, Result),
    ?assertEqual([], held_locks(DB, []))
  end).

%%-----------------------------------------------------------------
%%  An aborted transaction releases all its locks: a read lock, a
%%  write lock and a read lock upgraded to write. Whatever the class
%%  of the exception is
%%-----------------------------------------------------------------
locks_are_released_on_abort_test(Config) ->
  with_db(Config, fun(DB) ->
    lists:foreach(
      fun(Abort) ->
        Result =
          zaya:transaction(fun() ->
            [] = zaya:read(DB, [r, u], read),
            ok = zaya:write(DB, [{w, v}, {u, v}], write),
            Abort()
          end),
        ?assertEqual({abort, test_abort}, Result),
        ?assertEqual([], held_locks(DB, [r, w, u])),
        ?assertEqual([], zaya:read(DB, [r, w, u])),
        ?assertNot(zaya:is_transaction())
      end,
      [
        fun() -> throw(test_abort) end,
        fun() -> erlang:error(test_abort) end,
        fun() -> exit(test_abort) end
      ]
    )
  end).

%%-----------------------------------------------------------------
%%  The locks are held until the commit and are released if it fails
%%-----------------------------------------------------------------
locks_are_released_when_commit_fails_test(Config) ->
  with_db(Config, fun(DB) ->
    ok = zaya_tx_test_backend:fail_once(full_params(Config, DB)),
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [r, u], read),
        ok = zaya:write(DB, [{w, v}, {u, v}], write)
      end),
    ?assertMatch({abort, _}, Result),
    ?assertEqual([], held_locks(DB, [r, w, u])),
    ?assertEqual([], zaya:read(DB, [r, w, u]))
  end).

%%-----------------------------------------------------------------
%%  A request locks the keys and then reads them. If the read fails
%%  the transaction is aborted and the locks of the failed request
%%  are released as the others are
%%-----------------------------------------------------------------
locks_are_released_when_read_fails_test(Config) ->
  with_db(Config, fun(DB) ->
    lists:foreach(
      fun(Lock) ->
        ok = zaya_tx_test_backend:fail_read_once(full_params(Config, DB)),
        Result =
          zaya:transaction(fun() ->
            ok = zaya:write(DB, [{w, v}], write),
            zaya:read(DB, [k], Lock)
          end),
        ?assertMatch({Lock, {abort, _}}, {Lock, Result}),
        ?assertEqual({Lock, []}, {Lock, held_locks(DB, [w, k])})
      end,
      [read, write]
    )
  end).

%%-----------------------------------------------------------------
%%  A request locks the keys and then fails on a malformed item
%%-----------------------------------------------------------------
locks_are_released_when_request_is_malformed_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [r], read),
        zaya:write(DB, [{k, v}, malformed], write)
      end),
    ?assertMatch({abort, _}, Result),
    ?assertEqual([], held_locks(DB, [r, k]))
  end).

%%-----------------------------------------------------------------
%%  The locks of a transaction are released when its process dies
%%-----------------------------------------------------------------
locks_are_released_when_caller_dies_test(Config) ->
  with_db(Config, fun(DB) ->
    Parent = self(),
    {Pid, MRef} =
      spawn_monitor(fun() ->
        zaya:transaction(fun() ->
          [] = zaya:read(DB, [r, u], read),
          ok = zaya:write(DB, [{w, v}, {u, v}], write),
          pause(Parent)
        end)
      end),
    ok = wait_paused(Pid),
    ?assertEqual(
      [
        {node(), db, ?READ_LOCKED},
        {node(), {key, r}, ?READ_LOCKED},
        {node(), {key, w}, ?WRITE_LOCKED},
        {node(), {key, u}, ?WRITE_LOCKED}
      ],
      held_locks(DB, [r, w, u])
    ),
    exit(Pid, kill),
    receive
      {'DOWN', MRef, process, Pid, killed} -> ok
    after 5000 ->
      ct:fail(caller_exit_timeout)
    end,
    ?assertEqual(ok, wait_until(fun() -> held_locks(DB, [r, w, u]) =:= [] end))
  end).

%%-----------------------------------------------------------------
%%  The DB is locked exclusively while its copies are transformed:
%%  the first lock of a transaction in the DB waits for it
%%-----------------------------------------------------------------
transaction_waits_for_db_lock_test(Config) ->
  with_db(Config, fun(DB) ->
    Parent = self(),
    Holder =
      async(fun() ->
        {ok, Ref} = elock:lock(?locks, DB, [node()], #{}),
        pause(Parent),
        elock:unlock(Ref)
      end),
    ok = wait_paused(Holder),
    Reader = async(fun() -> read_transaction(DB, [k]) end),
    ?assertNot(is_done(Reader, 500)),
    ok = resume(Holder),
    ?assertEqual(ok, await(Holder)),
    ?assertEqual({ok, []}, await(Reader)),
    ?assertEqual([], held_locks(DB, [k]))
  end).

%%-----------------------------------------------------------------
%%  The same key in two DBs is two locks. Each DB is locked by its
%%  first lock and all of them are released by the commit
%%-----------------------------------------------------------------
locks_of_several_dbs_test(Config) ->
  with_db(Config, fun(DB1) ->
    with_db(Config, fun(DB2) ->
      Result =
        zaya:transaction(fun() ->
          [] = zaya:read(DB1, [k], read),
          Locks1 = held_locks(DB2, [k]),
          ok = zaya:write(DB2, [{k, v}], write),
          Locks2 = {held_locks(DB1, [k]), held_locks(DB2, [k])},
          ok = zaya:write(DB1, [{k, v}], write),
          {Locks1, Locks2, held_locks(DB1, [k])}
        end),
      ?assertEqual(
        {ok, {
          [],
          {
            [{node(), db, ?READ_LOCKED}, {node(), {key, k}, ?READ_LOCKED}],
            [{node(), db, ?READ_LOCKED}, {node(), {key, k}, ?WRITE_LOCKED}]
          },
          [{node(), db, ?READ_LOCKED}, {node(), {key, k}, ?WRITE_LOCKED}]
        }},
        Result
      ),
      ?assertEqual([], held_locks(DB1, [k])),
      ?assertEqual([], held_locks(DB2, [k])),
      ?assertEqual([{k, v}], zaya:read(DB1, [k])),
      ?assertEqual([{k, v}], zaya:read(DB2, [k]))
    end)
  end).

%%-----------------------------------------------------------------
%%  The lock of the DB is kept among the locks of its keys under the
%%  name {zaya_transaction, DB}. A key with the same name is still a
%%  key and must be locked
%%-----------------------------------------------------------------
key_equal_to_db_lock_name_test(Config) ->
  with_db(Config, fun(DB) ->
    Key = {zaya_transaction, DB},
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [Key], read),
        Locks1 = held_locks(DB, [Key]),
        ok = zaya:write(DB, [{Key, v}], write),
        {Locks1, held_locks(DB, [Key])}
      end),
    ?assertEqual(
      {ok, {
        [{node(), db, ?READ_LOCKED}, {node(), {key, Key}, ?READ_LOCKED}],
        [{node(), db, ?READ_LOCKED}, {node(), {key, Key}, ?WRITE_LOCKED}]
      }},
      Result
    ),
    ?assertEqual([], held_locks(DB, [Key]))
  end).

%%=================================================================
%%  UPGRADE
%%
%%  A key locked for read and then for write holds two locks of elock,
%%  both of them must be released
%%=================================================================
upgrade_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [k], read),
        Lock1 = key_lock(DB, k),
        ok = zaya:write(DB, [{k, v}], write),
        {Lock1, key_lock(DB, k)}
      end),
    ?assertEqual({ok, {?READ_LOCKED, ?WRITE_LOCKED}}, Result),
    ?assertEqual([], held_locks(DB, [k])),
    ?assertEqual([{k, v}], zaya:read(DB, [k]))
  end).

upgrade_is_released_on_abort_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [k], read),
        ok = zaya:write(DB, [{k, v}], write),
        throw({test_abort, key_lock(DB, k)})
      end),
    ?assertEqual({abort, {test_abort, ?WRITE_LOCKED}}, Result),
    ?assertEqual([], held_locks(DB, [k])),
    ?assertEqual([], zaya:read(DB, [k]))
  end).

%%-----------------------------------------------------------------
%%  Any request with the write lock upgrades the read lock
%%-----------------------------------------------------------------
upgrade_by_any_write_locked_request_test(Config) ->
  with_db(Config, fun(DB) ->
    lists:foreach(
      fun({Key, Upgrade}) ->
        Result =
          zaya:transaction(fun() ->
            _ = zaya:read(DB, [Key], read),
            _ = Upgrade(Key),
            key_lock(DB, Key)
          end),
        ?assertEqual({Key, {ok, ?WRITE_LOCKED}}, {Key, Result}),
        ?assertEqual([], held_locks(DB, [Key]))
      end,
      [
        {by_read, fun(K) -> zaya:read(DB, [K], write) end},
        {by_write, fun(K) -> zaya:write(DB, [{K, v}], write) end},
        {by_delete, fun(K) -> zaya:delete(DB, [K], write) end}
      ]
    )
  end).

%%-----------------------------------------------------------------
%%  The key is locked once for read and once for write, the further
%%  requests take no new locks
%%-----------------------------------------------------------------
repeated_requests_take_no_new_locks_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [k], read),
        [] = zaya:read(DB, [k], read),
        ok = zaya:write(DB, [{k, v1}], write),
        [{k, v1}] = zaya:read(DB, [k], read),
        ok = zaya:write(DB, [{k, v2}], write),
        ok = zaya:delete(DB, [k], write),
        [] = zaya:read(DB, [k], write),
        ok = zaya:write(DB, [{k, v3}], read),
        key_lock(DB, k)
      end),
    ?assertEqual({ok, ?WRITE_LOCKED}, Result),
    ?assertEqual([], held_locks(DB, [k])),
    ?assertEqual([{k, v3}], zaya:read(DB, [k]))
  end).

%%-----------------------------------------------------------------
%%  The write lock covers the read: no read lock is taken after it
%%-----------------------------------------------------------------
write_then_read_keeps_write_lock_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        ok = zaya:write(DB, [{k, v}], write),
        [{k, v}] = zaya:read(DB, [k], read),
        key_lock(DB, k)
      end),
    ?assertEqual({ok, ?WRITE_LOCKED}, Result),
    ?assertEqual([], held_locks(DB, [k]))
  end).

%%-----------------------------------------------------------------
%%  One request with a key locked for read, a key locked for write
%%  and a new key
%%-----------------------------------------------------------------
mixed_keys_request_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [r], read),
        ok = zaya:write(DB, [{w, v}], write),
        ok = zaya:write(DB, [{r, v}, {w, v}, {n, v}], write),
        Locks1 = [key_lock(DB, K) || K <- [r, w, n]],
        _ = zaya:read(DB, [r, w, n, x], read),
        {Locks1, [key_lock(DB, K) || K <- [r, w, n, x]]}
      end),
    ?assertEqual(
      {ok, {
        [?WRITE_LOCKED, ?WRITE_LOCKED, ?WRITE_LOCKED],
        [?WRITE_LOCKED, ?WRITE_LOCKED, ?WRITE_LOCKED, ?READ_LOCKED]
      }},
      Result
    ),
    ?assertEqual([], held_locks(DB, [r, w, n, x]))
  end).

%%-----------------------------------------------------------------
%%  The upgrade waits for the other readers to leave and keeps its
%%  read lock meanwhile
%%-----------------------------------------------------------------
upgrade_waits_for_other_readers_test(Config) ->
  with_db(Config, fun(DB) ->
    Parent = self(),
    Reader =
      async(fun() ->
        zaya:transaction(fun() ->
          [] = zaya:read(DB, [k], read),
          pause(Parent)
        end)
      end),
    ok = wait_paused(Reader),
    Upgrader =
      async(fun() ->
        zaya:transaction(fun() ->
          [] = zaya:read(DB, [k], read),
          ok = zaya:write(DB, [{k, v}], write),
          key_lock(DB, k)
        end)
      end),
    ?assertNot(is_done(Upgrader, 500)),
    ok = resume(Reader),
    ?assertEqual({ok, ok}, await(Reader)),
    ?assertEqual({ok, ?WRITE_LOCKED}, await(Upgrader)),
    ?assertEqual([], held_locks(DB, [k])),
    ?assertEqual([{k, v}], zaya:read(DB, [k]))
  end).

%%=================================================================
%%  REPEATED KEYS
%%
%%  A key repeated in a request is locked once
%%=================================================================
repeated_key_in_request_test(Config) ->
  with_db(Config, fun(DB) ->
    lists:foreach(
      fun({Key, Request, Expected}) ->
        Result =
          zaya:transaction(fun() ->
            _ = Request(Key),
            key_lock(DB, Key)
          end),
        ?assertEqual({Key, {ok, Expected}}, {Key, Result}),
        ?assertEqual({Key, []}, {Key, held_locks(DB, [Key])})
      end,
      [
        {read_read, fun(K) -> zaya:read(DB, [K, K, K], read) end, ?READ_LOCKED},
        {read_write, fun(K) -> zaya:read(DB, [K, K, K], write) end, ?WRITE_LOCKED},
        {write_read, fun(K) -> zaya:write(DB, [{K, v1}, {K, v2}], read) end, ?READ_LOCKED},
        {write_write, fun(K) -> zaya:write(DB, [{K, v1}, {K, v2}], write) end, ?WRITE_LOCKED},
        {delete_write, fun(K) -> zaya:delete(DB, [K, K], write) end, ?WRITE_LOCKED}
      ]
    )
  end).

repeated_key_upgrade_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [k, k], read),
        Lock1 = key_lock(DB, k),
        ok = zaya:write(DB, [{k, v1}, {k, v2}], write),
        {Lock1, key_lock(DB, k)}
      end),
    ?assertEqual({ok, {?READ_LOCKED, ?WRITE_LOCKED}}, Result),
    ?assertEqual([], held_locks(DB, [k])),
    ?assertEqual([{k, v2}], zaya:read(DB, [k]))
  end).

%%=================================================================
%%  NESTED TRANSACTIONS
%%
%%  An internal transaction that commits leaves its locks to the
%%  parent. The one that aborts releases what it has locked and
%%  nothing of what the parent has locked before it
%%=================================================================
nested_commit_keeps_locks_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [r], read),
        Inner =
          zaya:transaction(fun() ->
            [] = zaya:read(DB, [ir], read),
            ok = zaya:write(DB, [{iw, v}], write),
            % The lock of the parent is upgraded
            ok = zaya:write(DB, [{r, v}], write)
          end),
        {Inner, [key_lock(DB, K) || K <- [r, ir, iw]]}
      end),
    ?assertEqual({ok, {{ok, ok}, [?WRITE_LOCKED, ?READ_LOCKED, ?WRITE_LOCKED]}}, Result),
    ?assertEqual([], held_locks(DB, [r, ir, iw])),
    ?assertEqual([{iw, v}, {r, v}], lists:sort(zaya:read(DB, [r, iw])))
  end).

nested_abort_releases_inner_locks_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [r], read),
        ok = zaya:write(DB, [{w, v}], write),
        Inner =
          zaya:transaction(fun() ->
            [] = zaya:read(DB, [ir, iu], read),
            ok = zaya:write(DB, [{iw, v}, {iu, v}], write),
            throw(inner_abort)
          end),
        {Inner, held_locks(DB, [r, w, ir, iw, iu])}
      end),
    ?assertEqual(
      {ok, {
        {abort, inner_abort},
        [
          {node(), db, ?READ_LOCKED},
          {node(), {key, r}, ?READ_LOCKED},
          {node(), {key, w}, ?WRITE_LOCKED}
        ]
      }},
      Result
    ),
    ?assertEqual([], held_locks(DB, [r, w, ir, iw, iu])),
    ?assertEqual([{w, v}], zaya:read(DB, [w, iw, iu]))
  end).

%%-----------------------------------------------------------------
%%  The internal transaction upgrades a read lock of the parent and
%%  aborts: the write lock is released, the read lock of the parent
%%  stays
%%-----------------------------------------------------------------
nested_abort_downgrades_upgraded_lock_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [k], read),
        Inner =
          zaya:transaction(fun() ->
            ok = zaya:write(DB, [{k, v}], write),
            throw({inner_abort, key_lock(DB, k)})
          end),
        {Inner, key_lock(DB, k)}
      end),
    ?assertEqual({ok, {{abort, {inner_abort, ?WRITE_LOCKED}}, ?READ_LOCKED}}, Result),
    ?assertEqual([], held_locks(DB, [k])),
    ?assertEqual([], zaya:read(DB, [k]))
  end).

%%-----------------------------------------------------------------
%%  After the downgrade the parent upgrades the lock itself
%%-----------------------------------------------------------------
nested_abort_then_upgrade_again_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [k], read),
        {abort, inner_abort} =
          zaya:transaction(fun() ->
            ok = zaya:write(DB, [{k, inner}], write),
            throw(inner_abort)
          end),
        Lock1 = key_lock(DB, k),
        ok = zaya:write(DB, [{k, outer}], write),
        {Lock1, key_lock(DB, k)}
      end),
    ?assertEqual({ok, {?READ_LOCKED, ?WRITE_LOCKED}}, Result),
    ?assertEqual([], held_locks(DB, [k])),
    ?assertEqual([{k, outer}], zaya:read(DB, [k]))
  end).

%%-----------------------------------------------------------------
%%  The internal transaction asks for a key locked by the parent for
%%  write: it takes no locks and its abort releases nothing
%%-----------------------------------------------------------------
nested_abort_keeps_parent_write_lock_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        ok = zaya:write(DB, [{k, outer}], write),
        {abort, inner_abort} =
          zaya:transaction(fun() ->
            [{k, outer}] = zaya:read(DB, [k], read),
            ok = zaya:write(DB, [{k, inner}], write),
            throw(inner_abort)
          end),
        key_lock(DB, k)
      end),
    ?assertEqual({ok, ?WRITE_LOCKED}, Result),
    ?assertEqual([], held_locks(DB, [k])),
    ?assertEqual([{k, outer}], zaya:read(DB, [k]))
  end).

%%-----------------------------------------------------------------
%%  The internal transaction is the first to lock a DB and aborts:
%%  the lock of the DB is released as well. The parent locks the DB
%%  again when it needs it
%%-----------------------------------------------------------------
nested_abort_releases_inner_db_lock_test(Config) ->
  with_db(Config, fun(DB1) ->
    with_db(Config, fun(DB2) ->
      Result =
        zaya:transaction(fun() ->
          [] = zaya:read(DB1, [k], read),
          {abort, inner_abort} =
            zaya:transaction(fun() ->
              ok = zaya:write(DB2, [{k, inner}], write),
              throw(inner_abort)
            end),
          Locks1 = {held_locks(DB1, [k]), held_locks(DB2, [k])},
          ok = zaya:write(DB2, [{k, outer}], write),
          {Locks1, held_locks(DB2, [k])}
        end),
      ?assertEqual(
        {ok, {
          {[{node(), db, ?READ_LOCKED}, {node(), {key, k}, ?READ_LOCKED}], []},
          [{node(), db, ?READ_LOCKED}, {node(), {key, k}, ?WRITE_LOCKED}]
        }},
        Result
      ),
      ?assertEqual([], held_locks(DB1, [k])),
      ?assertEqual([], held_locks(DB2, [k])),
      ?assertEqual([{k, outer}], zaya:read(DB2, [k]))
    end)
  end).

%%-----------------------------------------------------------------
%%  Three levels. Each abort releases the locks of its own level:
%%  the innermost one keeps the upgrade of the middle one, the middle
%%  one keeps the read lock of the outer one
%%-----------------------------------------------------------------
deep_nested_abort_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [k], read),
        {abort, {middle_abort, Locks1}} =
          zaya:transaction(fun() ->
            ok = zaya:write(DB, [{k, middle}], write),
            {abort, inner_abort} =
              zaya:transaction(fun() ->
                [{k, middle}] = zaya:read(DB, [k], read),
                ok = zaya:write(DB, [{k, inner}, {n, inner}], write),
                throw(inner_abort)
              end),
            throw({middle_abort, [key_lock(DB, K) || K <- [k, n]]})
          end),
        {Locks1, [key_lock(DB, K) || K <- [k, n]]}
      end),
    ?assertEqual({ok, {[?WRITE_LOCKED, ?FREE], [?READ_LOCKED, ?FREE]}}, Result),
    ?assertEqual([], held_locks(DB, [k, n])),
    ?assertEqual([], zaya:read(DB, [k, n]))
  end).

%%-----------------------------------------------------------------
%%  A lock error in an internal transaction is not its abort: the
%%  whole transaction is restarted from the external one
%%-----------------------------------------------------------------
nested_lock_error_restarts_whole_transaction_test(Config) ->
  with_db(Config, fun(DB) ->
    Attempts = counters:new(1, []),
    Result =
      zaya:transaction(fun() ->
        ok = counters:add(Attempts, 1, 1),
        ok = zaya:write(DB, [{a, v}], write),
        zaya:transaction(fun() ->
          case counters:get(Attempts, 1) of
            1 -> start_trap(key_term(DB, a), key_term(DB, b));
            _ -> ok
          end,
          zaya:write(DB, [{b, v}], write)
        end)
      end),
    ?assertEqual({ok, {ok, ok}}, Result),
    ?assertEqual(2, counters:get(Attempts, 1)),
    ok = wait_helpers(),
    ?assertEqual([], held_locks(DB, [a, b])),
    ?assertEqual([{a, v}, {b, v}], lists:sort(zaya:read(DB, [a, b])))
  end).

%%=================================================================
%%  FAILED REQUESTS
%%
%%  A request that fails to lock a key releases the locks it has
%%  taken itself and leaves the locks held before it. The cases catch
%%  the error to see the locks, though it is not what a user does
%%=================================================================
failed_request_releases_its_locks_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        ok = zaya:write(DB, [{a, v}], write),
        start_trap(key_term(DB, a), key_term(DB, b)),
        Error = (catch zaya:write(DB, [{n1, v}, {n2, v}, {b, v}], write)),
        Locks1 = [key_lock(DB, K) || K <- [a, n1, n2]],
        % The released key is locked again by the next request
        ok = zaya:write(DB, [{n1, v}], write),
        {Error, Locks1, key_lock(DB, n1)}
      end),
    ?assertEqual({ok, {{lock, deadlock}, [?WRITE_LOCKED, ?FREE, ?FREE], ?WRITE_LOCKED}}, Result),
    ok = wait_helpers(),
    ?assertEqual([], held_locks(DB, [a, b, n1, n2])),
    ?assertEqual([{a, v}, {n1, v}], lists:sort(zaya:read(DB, [a, b, n1, n2])))
  end).

failed_request_keeps_held_locks_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [r], read),
        ok = zaya:write(DB, [{w, v}, {a, v}], write),
        start_trap(key_term(DB, a), key_term(DB, b)),
        Error1 = (catch zaya:write(DB, [{w, v}, {b, v}], write)),
        Locks1 = [key_lock(DB, K) || K <- [r, w]],
        Error2 = (catch zaya:read(DB, [r, w, b], read)),
        {Error1, Locks1, Error2, [key_lock(DB, K) || K <- [r, w]]}
      end),
    ?assertEqual(
      {ok, {
        {lock, deadlock}, [?READ_LOCKED, ?WRITE_LOCKED],
        {lock, deadlock}, [?READ_LOCKED, ?WRITE_LOCKED]
      }},
      Result
    ),
    ok = wait_helpers(),
    ?assertEqual([], held_locks(DB, [r, w, a, b]))
  end).

%%-----------------------------------------------------------------
%%  The request upgrades one key and fails on the other: the write
%%  lock is released, the read lock stays and is upgraded later
%%-----------------------------------------------------------------
failed_upgrade_request_keeps_read_lock_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [k1, k2], read),
        start_upgrader(key_term(DB, k2)),
        Error = (catch zaya:write(DB, [{k1, v}, {k2, v}], write)),
        Lock1 = key_lock(DB, k1),
        ok = zaya:write(DB, [{k1, v}], write),
        {Error, Lock1, key_lock(DB, k1)}
      end),
    ?assertEqual({ok, {{lock, deadlock}, ?READ_LOCKED, ?WRITE_LOCKED}}, Result),
    ok = wait_helpers(),
    ?assertEqual([], held_locks(DB, [k1, k2])),
    ?assertEqual([{k1, v}], zaya:read(DB, [k1, k2]))
  end).

%%-----------------------------------------------------------------
%%  The first request to a DB locks the DB and fails on the key: the
%%  lock of the DB is released as well
%%-----------------------------------------------------------------
failed_first_request_releases_db_lock_test(Config) ->
  with_db(Config, fun(DB1) ->
    with_db(Config, fun(DB2) ->
      Result =
        zaya:transaction(fun() ->
          ok = zaya:write(DB1, [{a, v}], write),
          start_trap(key_term(DB1, a), key_term(DB2, b)),
          Error = (catch zaya:write(DB2, [{n, v}, {b, v}], write)),
          {Error, held_locks(DB1, [a]), db_lock(DB2), key_lock(DB2, n)}
        end),
      ?assertEqual(
        {ok, {
          {lock, deadlock},
          [{node(), db, ?READ_LOCKED}, {node(), {key, a}, ?WRITE_LOCKED}],
          ?FREE,
          ?FREE
        }},
        Result
      ),
      ok = wait_helpers(),
      ?assertEqual([], held_locks(DB1, [a])),
      ?assertEqual([], held_locks(DB2, [n, b]))
    end)
  end).

%%-----------------------------------------------------------------
%%  A lock error releases all the locks and the transaction starts
%%  from scratch
%%-----------------------------------------------------------------
lock_error_restarts_transaction_test(Config) ->
  with_db(Config, fun(DB) ->
    Attempts = counters:new(1, []),
    Result =
      zaya:transaction(fun() ->
        ok = counters:add(Attempts, 1, 1),
        [] = zaya:read(DB, [r], read),
        ok = zaya:write(DB, [{a, v}], write),
        case counters:get(Attempts, 1) of
          1 -> start_trap(key_term(DB, a), key_term(DB, b));
          _ -> ok
        end,
        zaya:write(DB, [{n, v}, {b, v}], write)
      end),
    ?assertEqual({ok, ok}, Result),
    ?assertEqual(2, counters:get(Attempts, 1)),
    ok = wait_helpers(),
    ?assertEqual([], held_locks(DB, [r, a, n, b])),
    ?assertEqual([{a, v}, {b, v}, {n, v}], lists:sort(zaya:read(DB, [a, n, b])))
  end).

%%-----------------------------------------------------------------
%%  The same with the error of an upgrade, after another key of the
%%  request is upgraded
%%-----------------------------------------------------------------
upgrade_lock_error_restarts_transaction_test(Config) ->
  with_db(Config, fun(DB) ->
    Attempts = counters:new(1, []),
    Result =
      zaya:transaction(fun() ->
        ok = counters:add(Attempts, 1, 1),
        [] = zaya:read(DB, [k1, k2], read),
        case counters:get(Attempts, 1) of
          1 -> start_upgrader(key_term(DB, k2));
          _ -> ok
        end,
        zaya:write(DB, [{k1, v}, {k2, v}], write)
      end),
    ?assertEqual({ok, ok}, Result),
    ?assertEqual(2, counters:get(Attempts, 1)),
    ok = wait_helpers(),
    ?assertEqual([], held_locks(DB, [k1, k2])),
    ?assertEqual([{k1, v}, {k2, v}], lists:sort(zaya:read(DB, [k1, k2])))
  end).

%%-----------------------------------------------------------------
%%  The transaction that gets a lock error at every attempt is
%%  aborted and holds nothing
%%-----------------------------------------------------------------
lock_error_aborts_after_attempts_test(Config) ->
  with_db(Config, fun(DB) ->
    Attempts = counters:new(1, []),
    Result =
      zaya:transaction(fun() ->
        ok = counters:add(Attempts, 1, 1),
        [] = zaya:read(DB, [r], read),
        ok = zaya:write(DB, [{a, v}], write),
        start_trap(key_term(DB, a), key_term(DB, b)),
        zaya:write(DB, [{n, v}, {b, v}], write)
      end),
    ?assertEqual({abort, {lock, deadlock}}, Result),
    ?assertEqual(5, counters:get(Attempts, 1)),
    ok = wait_helpers(),
    ?assertEqual([], held_locks(DB, [r, a, n, b])),
    ?assertEqual([], zaya:read(DB, [a, n, b])),
    ?assertNot(zaya:is_transaction())
  end).

%%-----------------------------------------------------------------
%%  A request to a DB that is not available aborts the transaction
%%-----------------------------------------------------------------
unavailable_db_aborts_and_releases_test(Config) ->
  with_db(Config, fun(DB) ->
    NoDB = test_db(no_such_db),
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [r, u], read),
        ok = zaya:write(DB, [{w, v}, {u, v}], write),
        zaya:read(NoDB, [k], read)
      end),
    ?assertEqual({abort, {unavailable, NoDB}}, Result),
    ?assertEqual([], held_locks(DB, [r, w, u])),
    ?assertEqual([], held_locks(NoDB, [k]))
  end).

invalid_lock_type_aborts_and_releases_test(Config) ->
  with_db(Config, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [r, u], read),
        ok = zaya:write(DB, [{w, v}, {u, v}], write),
        zaya:read(DB, [k], exclusive)
      end),
    ?assertMatch({abort, _}, Result),
    ?assertEqual([], held_locks(DB, [r, w, u, k]))
  end).

%%=================================================================
%%  CONCURRENT TRANSACTIONS
%%=================================================================
readers_share_writer_waits_test(Config) ->
  with_db(Config, fun(DB) ->
    Parent = self(),
    Read =
      fun() ->
        zaya:transaction(fun() ->
          [] = zaya:read(DB, [k], read),
          pause(Parent)
        end)
      end,
    Reader1 = async(Read),
    ok = wait_paused(Reader1),
    Reader2 = async(Read),
    ok = wait_paused(Reader2),
    Writer = async(fun() -> zaya:transaction(fun() -> zaya:write(DB, [{k, v}], write) end) end),
    ?assertNot(is_done(Writer, 500)),
    ok = resume(Reader1),
    ?assertEqual({ok, ok}, await(Reader1)),
    ?assertNot(is_done(Writer, 500)),
    ok = resume(Reader2),
    ?assertEqual({ok, ok}, await(Reader2)),
    ?assertEqual({ok, ok}, await(Writer)),
    ?assertEqual([], held_locks(DB, [k])),
    ?assertEqual([{k, v}], zaya:read(DB, [k]))
  end).

writer_blocks_reader_test(Config) ->
  with_db(Config, fun(DB) ->
    Parent = self(),
    Writer =
      async(fun() ->
        zaya:transaction(fun() ->
          ok = zaya:write(DB, [{k, v}], write),
          pause(Parent)
        end)
      end),
    ok = wait_paused(Writer),
    Reader = async(fun() -> read_transaction(DB, [k]) end),
    ?assertNot(is_done(Reader, 500)),
    ok = resume(Writer),
    ?assertEqual({ok, ok}, await(Writer)),
    % The reader sees the committed value
    ?assertEqual({ok, [{k, v}]}, await(Reader)),
    ?assertEqual([], held_locks(DB, [k]))
  end).

%%-----------------------------------------------------------------
%%  Two transactions read the counter and write it incremented. They
%%  upgrade the same lock: one of them gets a lock error and restarts
%%  with the committed value, no update is lost
%%-----------------------------------------------------------------
concurrent_upgrades_keep_updates_test(Config) ->
  with_db(Config, fun(DB) ->
    ok = zaya:write(DB, [{counter, 0}]),
    Pids = [async(fun() -> increment(DB, 200) end) || _ <- lists:seq(1, 2)],
    ?assertEqual([{ok, ok}, {ok, ok}], [await(Pid) || Pid <- Pids]),
    ?assertEqual([{counter, 2}], zaya:read(DB, [counter])),
    ?assertEqual([], held_locks(DB, [counter]))
  end).

%%-----------------------------------------------------------------
%%  The same under contention. A transaction may run out of attempts
%%  and abort, but the counter is equal to the number of the committed
%%  ones and nothing stays locked
%%-----------------------------------------------------------------
many_concurrent_upgrades_keep_updates_test(Config) ->
  with_db(Config, fun(DB) ->
    ok = zaya:write(DB, [{counter, 0}]),
    Pids = [async(fun() -> increment(DB, 50) end) || _ <- lists:seq(1, 8)],
    Results = [await(Pid) || Pid <- Pids],
    Committed = length([ok || {ok, ok} <- Results]),
    ct:pal("committed ~p of ~p: ~p", [Committed, length(Results), Results]),
    ?assertEqual([], [R || R <- Results, R =/= {ok, ok}, R =/= {abort, {lock, deadlock}}]),
    ?assert(Committed > 0),
    ?assertEqual([{counter, Committed}], zaya:read(DB, [counter])),
    ?assertEqual([], held_locks(DB, [counter]))
  end).

%%-----------------------------------------------------------------
%%  Two transactions lock two keys in the opposite order. One of them
%%  loses the deadlock, releases its locks and restarts, both commit
%%-----------------------------------------------------------------
deadlocked_transactions_restart_and_commit_test(Config) ->
  with_db(Config, fun(DB) ->
    Transaction =
      fun(Key1, Key2, Delay) ->
        zaya:transaction(fun() ->
          ok = zaya:write(DB, [{Key1, self()}], write),
          timer:sleep(Delay),
          ok = zaya:write(DB, [{Key2, self()}], write)
        end)
      end,
    % The delays differ for the requests of the cycle to come one after another
    Pid1 = async(fun() -> Transaction(a, b, 100) end),
    Pid2 = async(fun() -> Transaction(b, a, 300) end),
    ?assertEqual({ok, ok}, await(Pid1)),
    ?assertEqual({ok, ok}, await(Pid2)),
    ?assertEqual([], held_locks(DB, [a, b])),
    % Both keys are written by the transaction that has committed the last
    [{a, Writer}, {b, Writer}] = lists:sort(zaya:read(DB, [a, b])),
    ?assert(lists:member(Writer, [Pid1, Pid2]))
  end).

%%=================================================================
%%  CLUSTER LOCKS
%%
%%  The DB has copies on two nodes. A read lock is set on the local
%%  copy only, a write lock is set on each copy
%%=================================================================
cluster_read_lock_is_local_test(Config) ->
  Nodes = [Node, PeerNode] = cluster_nodes(Config),
  with_cluster_db(Nodes, Nodes, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [k], read),
        held_locks(Nodes, DB, [k])
      end),
    % The DB itself is locked on each copy
    ?assertEqual(
      {ok, [
        {Node, db, ?READ_LOCKED},
        {Node, {key, k}, ?READ_LOCKED},
        {PeerNode, db, ?READ_LOCKED}
      ]},
      Result
    ),
    ?assertEqual([], held_locks(Nodes, DB, [k]))
  end).

cluster_write_lock_is_set_on_all_copies_test(Config) ->
  Nodes = [Node, PeerNode] = cluster_nodes(Config),
  with_cluster_db(Nodes, Nodes, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        ok = zaya:write(DB, [{k, v}], write),
        held_locks(Nodes, DB, [k])
      end),
    ?assertEqual(
      {ok, [
        {Node, db, ?READ_LOCKED},
        {Node, {key, k}, ?WRITE_LOCKED},
        {PeerNode, db, ?READ_LOCKED},
        {PeerNode, {key, k}, ?WRITE_LOCKED}
      ]},
      Result
    ),
    ?assertEqual([], held_locks(Nodes, DB, [k])),
    ?assertEqual([{k, v}], zaya:read(DB, [k])),
    ?assertEqual([{k, v}], rpc:call(PeerNode, zaya, read, [DB, [k]]))
  end).

%%-----------------------------------------------------------------
%%  The read lock is held on the local copy, the write lock on both:
%%  the locks of the upgraded key are on different sets of nodes
%%-----------------------------------------------------------------
cluster_upgrade_test(Config) ->
  Nodes = [_Node, PeerNode] = cluster_nodes(Config),
  with_cluster_db(Nodes, Nodes, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [k], read),
        Locks1 = [key_lock(N, DB, k) || N <- Nodes],
        ok = zaya:write(DB, [{k, v}], write),
        {Locks1, [key_lock(N, DB, k) || N <- Nodes]}
      end),
    ?assertEqual({ok, {[?READ_LOCKED, ?FREE], [?WRITE_LOCKED, ?WRITE_LOCKED]}}, Result),
    ?assertEqual([], held_locks(Nodes, DB, [k])),
    ?assertEqual([{k, v}], zaya:read(DB, [k])),
    ?assertEqual([{k, v}], rpc:call(PeerNode, zaya, read, [DB, [k]]))
  end).

cluster_nested_abort_downgrades_upgraded_lock_test(Config) ->
  Nodes = cluster_nodes(Config),
  with_cluster_db(Nodes, Nodes, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [k], read),
        {abort, inner_abort} =
          zaya:transaction(fun() ->
            ok = zaya:write(DB, [{k, v}], write),
            throw(inner_abort)
          end),
        [key_lock(N, DB, k) || N <- Nodes]
      end),
    ?assertEqual({ok, [?READ_LOCKED, ?FREE]}, Result),
    ?assertEqual([], held_locks(Nodes, DB, [k]))
  end).

%%-----------------------------------------------------------------
%%  The write lock of k2 is refused by the local node and granted by
%%  the peer: nothing of the failed request stays locked on the peer
%%-----------------------------------------------------------------
cluster_failed_upgrade_request_test(Config) ->
  Nodes = [_Node, PeerNode] = cluster_nodes(Config),
  with_cluster_db(Nodes, Nodes, fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [k1, k2], read),
        start_upgrader(key_term(DB, k2)),
        Error = (catch zaya:write(DB, [{k1, v}, {k2, v}], write)),
        {Error, [key_lock(N, DB, k1) || N <- Nodes], key_lock(PeerNode, DB, k2)}
      end),
    ?assertEqual({ok, {{lock, deadlock}, [?READ_LOCKED, ?FREE], ?FREE}}, Result),
    ok = wait_helpers(),
    ?assertEqual([], held_locks(Nodes, DB, [k1, k2]))
  end).

%%-----------------------------------------------------------------
%%  The read lock of a transaction on the peer is local to the peer,
%%  it waits for the write lock of a transaction on this node
%%-----------------------------------------------------------------
cluster_remote_reader_waits_for_writer_test(Config) ->
  Nodes = [_Node, PeerNode] = cluster_nodes(Config),
  with_cluster_db(Nodes, Nodes, fun(DB) ->
    Parent = self(),
    Writer =
      async(fun() ->
        zaya:transaction(fun() ->
          ok = zaya:write(DB, [{k, v}], write),
          pause(Parent)
        end)
      end),
    ok = wait_paused(Writer),
    Reader = async(fun() -> rpc:call(PeerNode, ?MODULE, read_transaction, [DB, [k]]) end),
    ?assertNot(is_done(Reader, 500)),
    ok = resume(Writer),
    ?assertEqual({ok, ok}, await(Writer)),
    ?assertEqual({ok, [{k, v}]}, await(Reader)),
    ?assertEqual([], held_locks(Nodes, DB, [k]))
  end).

cluster_locks_are_released_when_caller_dies_test(Config) ->
  Nodes = [Node, PeerNode] = cluster_nodes(Config),
  with_cluster_db(Nodes, Nodes, fun(DB) ->
    Parent = self(),
    {Pid, MRef} =
      spawn_monitor(fun() ->
        zaya:transaction(fun() ->
          [] = zaya:read(DB, [r, u], read),
          ok = zaya:write(DB, [{u, v}], write),
          pause(Parent)
        end)
      end),
    ok = wait_paused(Pid),
    ?assertEqual(
      [
        {Node, db, ?READ_LOCKED},
        {Node, {key, r}, ?READ_LOCKED},
        {Node, {key, u}, ?WRITE_LOCKED},
        {PeerNode, db, ?READ_LOCKED},
        {PeerNode, {key, u}, ?WRITE_LOCKED}
      ],
      held_locks(Nodes, DB, [r, u])
    ),
    exit(Pid, kill),
    receive
      {'DOWN', MRef, process, Pid, killed} -> ok
    after 5000 ->
      ct:fail(caller_exit_timeout)
    end,
    ?assertEqual(ok, wait_until(fun() -> held_locks(Nodes, DB, [r, u]) =:= [] end))
  end).

%%-----------------------------------------------------------------
%%  The node of the transaction has no copy of the DB: the read lock
%%  is set on each copy as the write lock is
%%-----------------------------------------------------------------
cluster_no_local_copy_test(Config) ->
  Nodes = [_Node, PeerNode] = cluster_nodes(Config),
  with_cluster_db(Nodes, [PeerNode], fun(DB) ->
    Result =
      zaya:transaction(fun() ->
        [] = zaya:read(DB, [k], read),
        Locks1 = held_locks(Nodes, DB, [k]),
        ok = zaya:write(DB, [{k, v}], write),
        {Locks1, held_locks(Nodes, DB, [k])}
      end),
    ?assertEqual(
      {ok, {
        [{PeerNode, db, ?READ_LOCKED}, {PeerNode, {key, k}, ?READ_LOCKED}],
        [{PeerNode, db, ?READ_LOCKED}, {PeerNode, {key, k}, ?WRITE_LOCKED}]
      }},
      Result
    ),
    ?assertEqual([], held_locks(Nodes, DB, [k])),
    ?assertEqual([{k, v}], zaya:read(DB, [k]))
  end).

%%=================================================================
%%  Lock helpers
%%=================================================================
%%-----------------------------------------------------------------
%%  The transactions lock the terms in elock: a key is
%%  {zaya_transaction, DB, Key} and the DB is DB. A probe is a process
%%  that asks elock for the same term on the Node and reports if it
%%  has got it. The shared probe goes first: it would wait behind
%%  a waiting exclusive one
%%-----------------------------------------------------------------
key_term(DB, Key) ->
  {zaya_transaction, DB, Key}.

key_lock(DB, Key) ->
  key_lock(node(), DB, Key).

key_lock(Node, DB, Key) ->
  lock_state(Node, key_term(DB, Key)).

db_lock(DB) ->
  lock_state(node(), DB).

lock_state(Node, Term) ->
  Shared = probe(Node, Term, true),
  {Shared, probe(Node, Term, false)}.

probe(Node, Term, IsShared) ->
  {Pid, MRef} =
    spawn_monitor(fun() ->
      Options = #{is_shared => IsShared, timeout => ?PROBE_TIMEOUT},
      case elock:lock(?locks, Term, [Node], Options) of
        {ok, Ref} ->
          elock:unlock(Ref),
          exit({probe, free});
        {error, timeout} ->
          exit({probe, busy});
        Error ->
          exit(Error)
      end
    end),
  receive
    {'DOWN', MRef, process, Pid, {probe, Result}} ->
      Result;
    {'DOWN', MRef, process, Pid, Reason} ->
      ct:fail({probe_failed, Node, Term, Reason})
  end.

%% The locks of the DB and of its Keys that are not free, node by node
held_locks(DB, Keys) ->
  held_locks([node()], DB, Keys).

held_locks(Nodes, DB, Keys) ->
  [
    {Node, Name, State}
    || Node <- Nodes,
       {Name, Term} <- [{db, DB} | [{{key, Key}, key_term(DB, Key)} || Key <- Keys]],
       State <- [lock_state(Node, Term)],
       State =/= ?FREE
  ].

%%-----------------------------------------------------------------
%%  The helpers make a lock request of the caller fail at once, with
%%  a deadlock. They are started from the transaction and wait for
%%  its locks, so they are waited for after it (see wait_helpers/0)
%%
%%  A trap holds the Wanted term and waits for the Held term, that is
%%  locked by the caller. A request of the caller for Wanted closes
%%  the cycle and loses: the trap holds more locks
%%-----------------------------------------------------------------
start_trap(Held, Wanted) ->
  Caller = self(),
  Weight = [{?MODULE, trap, make_ref(), I} || I <- lists:seq(1, ?TRAP_WEIGHT)],
  start_helper(fun() ->
    Refs =
      [
        begin {ok, Ref} = elock:lock(?locks, Term, [node()], #{}), Ref end
        || Term <- [Wanted | Weight]
      ],
    Caller ! {helper_ready, self()},
    case elock:lock(?locks, Held, [node()], #{timeout => ?HELPER_TIMEOUT}) of
      {ok, HeldRef} -> elock:unlock(HeldRef);
      _ -> ok
    end,
    [elock:unlock(Ref) || Ref <- Refs]
  end).

%%-----------------------------------------------------------------
%%  An upgrader holds the Term shared, as the caller does, and waits
%%  for its upgrade. The second upgrade of a lock is a deadlock: the
%%  upgrade of the caller fails
%%-----------------------------------------------------------------
start_upgrader(Term) ->
  Caller = self(),
  start_helper(fun() ->
    {ok, Shared} = elock:lock(?locks, Term, [node()], #{is_shared => true}),
    Caller ! {helper_ready, self()},
    case elock:lock(?locks, Term, [node()], #{timeout => ?HELPER_TIMEOUT}) of
      {ok, Exclusive} -> elock:unlock(Exclusive);
      _ -> ok
    end,
    elock:unlock(Shared)
  end),
  % A shared request waits behind a pending upgrade
  ok = wait_until(fun() -> probe(node(), Term, true) =:= busy end).

start_helper(Fun) ->
  {Pid, MRef} = spawn_monitor(Fun),
  receive
    {helper_ready, Pid} ->
      ok;
    {'DOWN', MRef, process, Pid, Reason} ->
      ct:fail({helper_failed, Reason})
  after 5000 ->
    ct:fail(helper_start_timeout)
  end,
  put(lock_helpers, [{Pid, MRef} | lock_helpers()]),
  ok.

lock_helpers() ->
  case get(lock_helpers) of
    Helpers when is_list(Helpers) -> Helpers;
    _ -> []
  end.

%% Must be called by the process that has started the helpers
wait_helpers() ->
  Helpers = lock_helpers(),
  erase(lock_helpers),
  lists:foreach(
    fun({Pid, MRef}) ->
      receive
        {'DOWN', MRef, process, Pid, normal} ->
          ok;
        {'DOWN', MRef, process, Pid, Reason} ->
          ct:fail({helper_failed, Reason})
      after 5000 ->
        ct:fail({helper_is_stuck, Pid})
      end
    end,
    Helpers
  ).

%%-----------------------------------------------------------------
%%  Transactions in other processes. The process outlives its Fun and
%%  stays until the case is over: the locks of a process are released
%%  when it exits, that would hide the locks its transaction has left
%%-----------------------------------------------------------------
async(Fun) ->
  Parent = self(),
  spawn_link(fun() ->
    Parent ! {async, self(), Fun()},
    MRef = erlang:monitor(process, Parent),
    receive
      {'DOWN', MRef, process, Parent, _Reason} -> ok
    end
  end).

await(Pid) ->
  receive
    {async, Pid, Result} -> Result
  after 30000 ->
    ct:fail({await_timeout, Pid})
  end.

is_done(Pid, Timeout) ->
  receive
    {async, Pid, _Result} = Message ->
      self() ! Message,
      true
  after Timeout ->
    false
  end.

%% Stops the transaction of the caller until the Parent resumes it
pause(Parent) ->
  Parent ! {paused, self()},
  receive
    resume -> ok
  end.

wait_paused(Pid) ->
  receive
    {paused, Pid} -> ok
  after 5000 ->
    ct:fail({pause_timeout, Pid})
  end.

resume(Pid) ->
  Pid ! resume,
  ok.

read_transaction(DB, Keys) ->
  zaya:transaction(fun() -> zaya:read(DB, Keys, read) end).

increment(DB, Delay) ->
  zaya:transaction(fun() ->
    [{counter, Value}] = zaya:read(DB, [counter], read),
    timer:sleep(Delay),
    zaya:write(DB, [{counter, Value + 1}], write)
  end).

%%-----------------------------------------------------------------
%%  DBs of the lock cases
%%-----------------------------------------------------------------
with_db(Config, Fun) ->
  DB = test_db(lock_db),
  try
    ok = setup_local_db(DB, db_params(Config, atom_to_list(DB)), []),
    Fun(DB)
  after
    cleanup_db(DB, full_params(Config, DB))
  end.

full_params(Config, DB) ->
  zaya_db_srv:default_params(DB, db_params(Config, atom_to_list(DB))).

cluster_nodes(Config) ->
  [node(), ?config(peer_node, Config)].

%% Nodes are the nodes of the cluster, the DB has its copies on CopyNodes
with_cluster_db(Nodes, CopyNodes, Fun) ->
  DB = test_db(cluster_lock_db),
  IsAvailable =
    fun(Node) ->
      lists:sort(rpc:call(Node, zaya, db_available_nodes, [DB])) =:= lists:sort(CopyNodes)
    end,
  try
    {_, []} = zaya:db_create(DB, zaya_ets, maps:from_keys(CopyNodes, #{})),
    ok = wait_until(fun() -> lists:all(IsAvailable, Nodes) end),
    Fun(DB)
  after
    catch zaya:db_close(DB),
    wait_until(fun() -> zaya:db_available_nodes(DB) =:= [] end),
    catch zaya:db_remove(DB)
  end.

load_support_backend(PrivDir) ->
  SupportDir = filename:join(PrivDir, "support-ebin"),
  ok = filelib:ensure_dir(filename:join(SupportDir, "dummy")),
  SupportRoot = filename:join([code:lib_dir(zaya), "test", "support"]),
  SupportModules = [cthr, zaya_ct, zaya_tx_test_backend],
  ok =
    lists:foreach(
      fun(Module) ->
        SupportSrc = filename:join(SupportRoot, atom_to_list(Module) ++ ".erl"),
        {ok, Module} =
          compile:file(
            SupportSrc,
            [{outdir, SupportDir}, report_errors, report_warnings]
          )
      end,
      SupportModules
    ),
  true = code:add_patha(SupportDir),
  {module, cthr} = code:load_file(cthr),
  {module, zaya_tx_test_backend} = code:load_file(zaya_tx_test_backend),
  ok.

setup_local_db(DB, Params, SeedData) ->
  FullParams = zaya_db_srv:default_params(DB, Params),
  ok = zaya_schema_srv:add_db(DB, zaya_tx_test_backend),
  ok = zaya_schema_srv:add_db_copy(DB, node(), Params),
  ok = zaya_tx_test_backend:seed(FullParams, SeedData),
  ok = zaya_db_srv:open(DB),
  ok = wait_until(fun() -> zaya:is_db_available(DB) end),
  ok.

ensure_distributed() ->
  case node() of
    nonode@nohost ->
      Name =
        list_to_atom(
          "zaya_tx_suite_" ++ integer_to_list(erlang:unique_integer([positive]))
        ),
      {ok, _PID} = net_kernel:start([Name, shortnames]),
      ok;
    _ ->
      ok
  end.

db_params(Config, Name) ->
  #{dir => filename:join(?config(priv_dir, Config), Name)}.

test_db(BaseName) ->
  list_to_atom(atom_to_list(BaseName) ++ "_" ++ integer_to_list(erlang:unique_integer([positive]))).

restart_db(DB) ->
  _ = zaya:db_close(DB),
  ok = wait_until(fun() -> whereis(DB) =:= undefined end),
  _ = zaya:db_open(DB),
  ok = wait_until(fun() -> zaya:is_db_available(DB) end),
  ok.

cleanup_db(DB, FullParams) ->
  catch zaya:db_close(DB),
  ensure_stopped(DB),
  catch zaya_schema_srv:close_db(DB, node()),
  catch zaya_schema_srv:remove_db(DB),
  ok = zaya_tx_test_backend:reset(FullParams),
  ok.

wait_until(Fun) ->
  wait_until(Fun, 50).

wait_until(Fun, 0) ->
  case Fun() of
    true -> ok;
    _ -> {error, timeout}
  end;
wait_until(Fun, AttemptsLeft) ->
  case Fun() of
    true ->
      ok;
    _ ->
      timer:sleep(100),
      wait_until(Fun, AttemptsLeft - 1)
  end.

ensure_stopped(DB) ->
  case wait_until(fun() -> whereis(DB) =:= undefined end, 5) of
    ok ->
      ok;
    {error, timeout} ->
      case whereis(DB) of
        PID when is_pid(PID) ->
          exit(PID, shutdown),
          wait_until(fun() -> whereis(DB) =:= undefined end);
        _ ->
          ok
      end
  end.
