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
  purge_deletes_only_db_entries_test/1,
  db_open_replays_pending_rollback_test/1,
  attach_copy_purges_stale_rollbacks_test/1
]).

all() ->
  [
    seq_and_marker_roundtrip_test,
    rollback_replays_newest_first_test,
    purge_deletes_only_db_entries_test,
    db_open_replays_pending_rollback_test,
    attach_copy_purges_stale_rollbacks_test
  ].

init_per_suite(Config) ->
  ok = load_support_backend(?config(priv_dir, Config)),
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

end_per_suite(_Config) ->
  ok = application:stop(zaya),
  ok.

init_per_testcase(_Name, Config) ->
  put(applied_rollbacks, []),
  Config.

end_per_testcase(_Name, _Config) ->
  ok.

seq_and_marker_roundtrip_test(_Config) ->
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

rollback_replays_newest_first_test(_Config) ->
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
    fun(RollbackOps) ->
      put(applied_rollbacks, [RollbackOps | get(applied_rollbacks)])
    end
  ),
  ?assertEqual(
    [
      {[{item, newer_value}], []},
      {[{item, old_value}], []}
    ],
    lists:reverse(get(applied_rollbacks))
  ).

purge_deletes_only_db_entries_test(_Config) ->
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

db_open_replays_pending_rollback_test(Config) ->
  DB = test_db(db_open_replays_pending_rollback_test),
  Params = db_params(Config, "db-open"),
  FullParams = zaya_db_srv:default_params(DB, Params),
  try
    ok = zaya_schema_srv:add_db(DB, zaya_tx_test_backend),
    ok = zaya_schema_srv:add_db_copy(DB, node(), Params),
    ok = zaya_tx_test_backend:seed(FullParams, [{item, newer_value}]),
    TRef = make_ref(),
    Seq = zaya_transaction_log:seq(),
    ok = zaya_transaction_log:commit(
      [
        {{rollbacked, TRef}, [{DB, [node()]}]},
        {{rollback, DB, Seq, TRef}, {[{item, old_value}], []}}
      ],
      []
    ),
    ok = zaya_db_srv:open(DB),
    ok = wait_until(fun() -> zaya:is_db_available(DB) end),
    ?assertEqual([{item, old_value}], zaya:read(DB, [item])),
    ?assertEqual(1, zaya_tx_test_backend:commit_count(FullParams)),
    ok = zaya_db_srv:close(DB),
    ok = wait_until(fun() -> whereis(DB) =:= undefined end),
    ok = zaya_db_srv:open(DB),
    ok = wait_until(fun() -> zaya:is_db_available(DB) end),
    ?assertEqual([{item, old_value}], zaya:read(DB, [item])),
    ?assertEqual(1, zaya_tx_test_backend:commit_count(FullParams))
  after
    cleanup_db(DB, FullParams)
  end.

attach_copy_purges_stale_rollbacks_test(Config) ->
  DB = test_db(attach_copy_purges_stale_rollbacks_test),
  Params = db_params(Config, "attach-copy"),
  FullParams = zaya_db_srv:default_params(DB, Params),
  try
    ok = zaya_schema_srv:add_db(DB, zaya_tx_test_backend),
    ok = zaya_tx_test_backend:seed(FullParams, [{item, copied_value}]),
    TRef = make_ref(),
    Seq = zaya_transaction_log:seq(),
    ok = zaya_transaction_log:commit(
      [
        {{rollbacked, TRef}, [{DB, [node()]}]},
        {{rollback, DB, Seq, TRef}, {[{item, old_value}], []}}
      ],
      []
    ),
    ok = zaya_db_srv:attach_copy(DB, Params),
    ok = wait_until(fun() -> zaya:is_db_available(DB) end),
    ?assertEqual([{item, copied_value}], zaya:read(DB, [item])),
    ?assertEqual(0, zaya_tx_test_backend:commit_count(FullParams)),
    ok = zaya_db_srv:close(DB),
    ok = wait_until(fun() -> whereis(DB) =:= undefined end),
    ok = zaya_db_srv:open(DB),
    ok = wait_until(fun() -> zaya:is_db_available(DB) end),
    ?assertEqual([{item, copied_value}], zaya:read(DB, [item])),
    ?assertEqual(0, zaya_tx_test_backend:commit_count(FullParams))
  after
    cleanup_db(DB, FullParams)
  end.

load_support_backend(PrivDir) ->
  SupportDir = filename:join(PrivDir, "support-ebin"),
  ok = filelib:ensure_dir(filename:join(SupportDir, "dummy")),
  SupportSrc = filename:join([code:lib_dir(zaya), "test", "support", "zaya_tx_test_backend.erl"]),
  {ok, zaya_tx_test_backend} = compile:file(SupportSrc, [{outdir, SupportDir}, report_errors, report_warnings]),
  true = code:add_patha(SupportDir),
  {module, zaya_tx_test_backend} = code:load_file(zaya_tx_test_backend),
  ok.

db_params(Config, Name) ->
  #{dir => filename:join(?config(priv_dir, Config), Name)}.

test_db(BaseName) ->
  list_to_atom(atom_to_list(BaseName) ++ "_" ++ integer_to_list(erlang:unique_integer([positive]))).

cleanup_db(DB, FullParams) ->
  catch zaya_db_srv:close(DB),
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
