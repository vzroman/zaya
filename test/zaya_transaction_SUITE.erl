-module(zaya_transaction_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1
]).

-export([
  single_db_node_commit_uses_local_fast_path_test/1,
  single_db_commit_skips_log_test/1,
  single_node_worker_rolls_back_and_cleans_marker_test/1,
  multi_node_worker_rolls_back_after_coordinator_decision_test/1
]).

-record(commit_request, {
  tref,
  dbs,
  scope,
  is_persistent,
  coordinator
}).

all() ->
  [
    single_db_node_commit_uses_local_fast_path_test,
    single_db_commit_skips_log_test,
    single_node_worker_rolls_back_and_cleans_marker_test,
    multi_node_worker_rolls_back_after_coordinator_decision_test
  ].

init_per_suite(Config) ->
  PrivDir = ?config(priv_dir, Config),
  ok = ensure_distributed(),
  ok = load_support_backend(PrivDir),
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
  {ok, _Apps} = application:ensure_all_started(zaya),
  Config.

end_per_suite(_Config) ->
  ok = application:stop(zaya),
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
      throw,
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
    {Worker, MRef} =
      spawn_monitor(
        fun() ->
          zaya_transaction:commit_request(
            #commit_request{
              tref = make_ref(),
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
      {commit1, confirm, Worker} ->
        ok
    after 1000 ->
      ct:fail(commit1_confirm_timeout)
    end,
    Worker ! {rollback, [Worker]},
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

load_support_backend(PrivDir) ->
  SupportDir = filename:join(PrivDir, "support-ebin"),
  ok = filelib:ensure_dir(filename:join(SupportDir, "dummy")),
  SupportRoot = filename:join([code:lib_dir(zaya), "test", "support"]),
  SupportModules = [cthr, zaya_tx_test_backend],
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
