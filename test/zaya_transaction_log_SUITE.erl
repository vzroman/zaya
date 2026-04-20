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
  recovery_classifier_treats_unexpected_replies_as_errors_test/1,
  rollback_callback_failure_preserves_entries_across_retry_test/1,
  purge_deletes_only_db_entries_test/1,
  pending_transaction_visibility_tracks_manual_decision_test/1,
  db_open_replays_pending_rollback_test/1,
  db_open_retries_after_rollback_commit_failure_test/1,
  db_open_retries_after_post_commit_failure_without_leaking_ref_test/1,
  attach_copy_purges_stale_rollbacks_test/1
]).

all() ->
  [
    seq_and_marker_roundtrip_test,
    rollback_replays_newest_first_test,
    recovery_classifier_treats_unexpected_replies_as_errors_test,
    rollback_callback_failure_preserves_entries_across_retry_test,
    purge_deletes_only_db_entries_test,
    pending_transaction_visibility_tracks_manual_decision_test,
    db_open_replays_pending_rollback_test,
    db_open_retries_after_rollback_commit_failure_test,
    db_open_retries_after_post_commit_failure_without_leaking_ref_test,
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
  os:unsetenv("PENDING_TRANSACTIONS"),
  Config.

end_per_testcase(_Name, _Config) ->
  os:unsetenv("PENDING_TRANSACTIONS"),
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
      put(applied_rollbacks, [RollbackOps | get(applied_rollbacks)]),
      ok
    end
  ),
  ?assertEqual(
    [
      {[{item, newer_value}], []},
      {[{item, old_value}], []}
    ],
    lists:reverse(get(applied_rollbacks))
  ).

recovery_classifier_treats_unexpected_replies_as_errors_test(_Config) ->
  Node1 = node(),
  Node2 = list_to_atom("other1@nohost"),
  Node3 = list_to_atom("other2@nohost"),
  Node4 = list_to_atom("other3@nohost"),
  ?assertEqual(
    {
      [{Node1, {ok, false}}],
      [
        {Node2, {error, timeout}},
        {Node3, {badrpc, nodedown}},
        {Node4, noconnection}
      ]
    },
    zaya_transaction_log:classify_recovery_responses(
      [
        {Node1, {ok, false}},
        {Node2, {error, timeout}},
        {Node3, {badrpc, nodedown}},
        {Node4, noconnection}
      ],
      []
    )
  ).

rollback_callback_failure_preserves_entries_across_retry_test(_Config) ->
  TRef = make_ref(),
  Seq = zaya_transaction_log:seq(),
  RollbackOps = {[{item, old_value}], []},
  ok = zaya_transaction_log:commit(
    [
      {{rollbacked, TRef}, [{orders, [node()]}]},
      {{rollback, orders, Seq, TRef}, RollbackOps}
    ],
    []
  ),
  ?assertException(
    error,
    {badmatch, callback_failed},
    zaya_transaction_log:rollback(
      orders,
      fun(_Ops) ->
        callback_failed
      end
    )
  ),
  put(applied_rollbacks, []),
  ok = zaya_transaction_log:rollback(
    orders,
    fun(Ops) ->
      put(applied_rollbacks, [Ops | get(applied_rollbacks)]),
      ok
    end
  ),
  ?assertEqual([RollbackOps], lists:reverse(get(applied_rollbacks))),
  put(applied_rollbacks, []),
  ok = zaya_transaction_log:rollback(
    orders,
    fun(Ops) ->
      put(applied_rollbacks, [Ops | get(applied_rollbacks)]),
      ok
    end
  ),
  ?assertEqual([], get(applied_rollbacks)).

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

pending_transaction_visibility_tracks_manual_decision_test(_Config) ->
  DB = test_db(pending_transaction_visibility_tracks_manual_decision_test),
  Participation = [],
  {CoordinatorNode, TRef} = make_remote_ref(),
  try
    ok = zaya_schema_srv:node_up(CoordinatorNode, pending_transaction_test),
    Seq = zaya_transaction_log:seq(),
    ok = zaya_transaction_log:commit(
      [
        {{rollback, DB, Seq, TRef}, {[{item, old_value}], []}}
      ],
      []
    ),
    Parent = self(),
    RollbackPid =
      spawn(fun() ->
        Result =
          catch zaya_transaction_log:rollback(
            DB,
            fun(Ops) ->
              Parent ! {rollback_callback, self(), Ops},
              ok
            end
          ),
        Parent ! {rollback_finished, self(), Result}
      end),
    ok = wait_until(fun() -> pending_transaction_visible(TRef, Participation, undefined) end, 100),
    ?assertEqual(
      [{TRef, Participation, undefined}],
      zaya:list_pending_transactions()
    ),
    os:putenv("PENDING_TRANSACTIONS", "ROLLBACK"),
    ok = wait_until(fun() -> pending_transaction_visible(TRef, Participation, rollback) end, 20),
    ?assertEqual(
      [{TRef, Participation, rollback}],
      zaya:list_pending_transactions()
    ),
    receive
      {rollback_callback, RollbackPid, {[{item, old_value}], []}} ->
        ok
    after 7000 ->
      ct:fail(rollback_callback_timeout)
    end,
    receive
      {rollback_finished, RollbackPid, ok} ->
        ok;
      {rollback_finished, RollbackPid, Result} ->
        ct:fail({unexpected_rollback_result, Result})
    after 7000 ->
      ct:fail(rollback_finish_timeout)
    end,
    ?assertEqual([], zaya:list_pending_transactions())
  after
    cleanup_schema_node(CoordinatorNode),
    os:unsetenv("PENDING_TRANSACTIONS")
  end.

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

db_open_retries_after_rollback_commit_failure_test(Config) ->
  DB = test_db(db_open_retries_after_rollback_commit_failure_test),
  Params = db_params(Config, "db-open-fail-once"),
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
    ok = zaya_tx_test_backend:fail_once(FullParams),
    ok = zaya_db_srv:open(DB),
    timer:sleep(300),
    ?assertEqual(false, zaya:is_db_available(DB)),
    ?assertEqual(0, zaya_tx_test_backend:commit_count(FullParams)),
    ok = wait_until(fun() -> zaya:is_db_available(DB) end, 80),
    ?assertEqual([{item, old_value}], zaya:read(DB, [item])),
    ?assertEqual(1, zaya_tx_test_backend:commit_count(FullParams))
  after
    cleanup_db(DB, FullParams)
  end.

db_open_retries_after_post_commit_failure_without_leaking_ref_test(Config) ->
  DB = test_db(db_open_retries_after_post_commit_failure_without_leaking_ref_test),
  Params = db_params(Config, "db-open-fail-after-confirm"),
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
    ok = zaya_tx_test_backend:fail_after_confirm(FullParams),
    ok = zaya_db_srv:open(DB),
    timer:sleep(300),
    ?assertEqual(false, zaya:is_db_available(DB)),
    ?assertEqual(1, zaya_tx_test_backend:commit_count(FullParams)),
    ok = wait_until(fun() -> zaya:is_db_available(DB) end, 80),
    ?assertEqual([{item, old_value}], zaya:read(DB, [item])),
    ?assertEqual(2, zaya_tx_test_backend:commit_count(FullParams))
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

pending_transaction_visible(TRef, Participation, Action) ->
  case zaya:list_pending_transactions() of
    [{ListedTRef, ListedParticipation, ListedAction}] ->
      ListedTRef =:= TRef andalso
        ListedParticipation =:= Participation andalso
        ListedAction =:= Action;
    _ ->
      false
  end.

make_remote_ref() ->
  Node = list_to_atom("other0@nohost"),
  Ref = make_ref(),
  OldNode = atom_to_binary(node(), latin1),
  NewNode = atom_to_binary(Node, latin1),
  {Node, binary_to_term(binary:replace(term_to_binary(Ref), OldNode, NewNode, [global]))}.

cleanup_schema_node(Node) ->
  catch zaya_schema_srv:node_down(Node, pending_transaction_test),
  catch zaya_schema_srv:remove_node(Node),
  ok.
