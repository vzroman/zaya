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

all() ->
  [
    seq_and_marker_roundtrip_test,
    rollback_replays_newest_first_test,
    purge_deletes_only_db_entries_test
  ].

init_per_suite(Config) ->
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
