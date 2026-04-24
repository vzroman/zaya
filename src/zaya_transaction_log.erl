-module(zaya_transaction_log).

-include("zaya.hrl").

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

-ifdef(TEST).
-export([
  classify_recovery_responses/2
]).
-endif.

-define(REF_KEY, {?MODULE, ref}).
-define(ATOMICS_REF_KEY, {?MODULE, atomics_ref}).
-define(PENDING_TABLE, zaya_transaction_log_pending_transactions).
-define(DEFAULT_REF_WAIT_TIMEOUT_MS, 600000).
-define(DEFAULT_REF_WAIT_POLL_MS, 1000).

-record(state, {
  ref,
  atomics_ref,
  cleanup_interval_ms = 60000
}).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

seq() ->
  AtomicsRef = persistent_term:get(?ATOMICS_REF_KEY),
  atomics:add_get(AtomicsRef, 1, 1) - 1.

commit(Write, Delete) ->
  Ref = persistent_term:get(?REF_KEY),
  zaya_rocksdb:commit(Ref, Write, Delete).

is_rollbacked(TRef) ->
  case wait_for_ref(ref_wait_timeout_ms()) of
    {ok, Ref} ->
      case zaya_rocksdb:read(Ref, [{rollbacked, TRef}]) of
        [{{rollbacked, TRef}, _}] -> {ok, true};
        [] -> {ok, false}
      end;
    {error, timeout} ->
      {error, timeout}
  end.

rollback(DB, Callback) ->
  Entries = rollback_entries(DB),
  lists:foreach(
    fun({{rollback, RollbackDB, Seq, TRef}, {RollbackWrite, RollbackDelete}}) ->
      case decide_recovery(TRef) of
        rollback ->
          ok = Callback({RollbackWrite, RollbackDelete}),
          ok = commit([], [{rollback, RollbackDB, Seq, TRef}]);
        commit ->
          ok = commit([], [{rollback, RollbackDB, Seq, TRef}])
      end
    end,
    Entries
  ),
  ok.

purge(DB) ->
  Ref = persistent_term:get(?REF_KEY),
  Keys = [Key || {Key, _} <- rollback_entries(DB)],
  case Keys of
    [] -> ok;
    _ -> zaya_rocksdb:delete(Ref, Keys)
  end.

list_pending_transactions() ->
  case ets:info(?PENDING_TABLE) of
    undefined ->
      [];
    _ ->
      [
        {TRef, Participation, pending_env_action()}
        || {TRef, Participation} <- ets:tab2list(?PENDING_TABLE)
      ]
  end.

init([]) ->
  Config0 = application:get_env(zaya, transaction_log, #{}),
  Config =
    maps:merge(
      #{
        dir => filename:join(zaya:schema_dir(), "TLOG"),
        pool => disabled,
        cleanup_interval_ms => 60000
      },
      Config0
    ),
  Dir = maps:get(dir, Config),
  Ref =
    case filelib:is_dir(filename:join(Dir, "DATA")) of
      true -> zaya_rocksdb:open(Config);
      false -> zaya_rocksdb:create(Config)
    end,
  AtomicsRef = atomics:new(1, [{signed, false}]),
  Seed =
    zaya_rocksdb:foldl(
      Ref,
      #{},
      fun
        ({{rollback, _DB, Seq, _TRef}, _Value}, Acc) ->
          erlang:max(Seq + 1, Acc);
        (_, Acc) ->
          Acc
      end,
      0
    ),
  ok = atomics:put(AtomicsRef, 1, Seed),
  persistent_term:put(?REF_KEY, Ref),
  persistent_term:put(?ATOMICS_REF_KEY, AtomicsRef),
  ensure_pending_table(),
  schedule_cleanup(maps:get(cleanup_interval_ms, Config)),
  {ok, #state{
    ref = Ref,
    atomics_ref = AtomicsRef,
    cleanup_interval_ms = maps:get(cleanup_interval_ms, Config)
  }}.

handle_call(_Request, _From, State) ->
  {reply, {error, unsupported}, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(cleanup, State = #state{ref = Ref, cleanup_interval_ms = Interval}) ->
  LocalDBs = zaya:node_dbs(node()),
  {ActiveTRefs, StaleRollbackKeys} =
    zaya_rocksdb:foldl(
      Ref,
      #{},
      fun
        ({{rollback, DB, _Seq, TRef}, _Value}, {ActiveAcc, DeleteAcc}) ->
          case lists:member(DB, LocalDBs) of
            true -> {sets:add_element(TRef, ActiveAcc), DeleteAcc};
            false -> {ActiveAcc, [{rollback, DB, _Seq, TRef} | DeleteAcc]}
          end;
        (_, Acc) ->
          Acc
      end,
      {sets:new(), []}
    ),
  maybe_delete(Ref, StaleRollbackKeys),
  zaya_rocksdb:foldl(
    Ref,
    #{},
    fun
      ({{rollbacked, TRef}, NodesByDB}, ok) ->
        case sets:is_element(TRef, ActiveTRefs) of
          true ->
            ok;
          false ->
            cleanup_rollbacked_entry(Ref, TRef, NodesByDB)
        end;
      (_, ok) ->
        ok
    end,
    ok
  ),
  schedule_cleanup(Interval),
  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{ref = Ref}) ->
  persistent_term:erase(?REF_KEY),
  persistent_term:erase(?ATOMICS_REF_KEY),
  catch zaya_rocksdb:close(Ref),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

wait_for_ref(infinity) ->
  case persistent_term:get(?REF_KEY, undefined) of
    undefined ->
      timer:sleep(ref_wait_poll_ms()),
      wait_for_ref(infinity);
    Ref ->
      {ok, Ref}
  end;
wait_for_ref(RemainingMs) when RemainingMs =< 0 ->
  {error, timeout};
wait_for_ref(RemainingMs) ->
  case persistent_term:get(?REF_KEY, undefined) of
    undefined ->
      SleepMs = erlang:min(ref_wait_poll_ms(), RemainingMs),
      timer:sleep(SleepMs),
      wait_for_ref(RemainingMs - SleepMs);
    Ref ->
      {ok, Ref}
  end.

classify_recovery_responses(Replies0, Errors) ->
  {ReachableReplies, ReplyErrors} =
    lists:foldl(
      fun({Node, Reply}, {ReachableAcc, ErrorAcc}) ->
        case Reply of
          {ok, true} ->
            {[{Node, Reply} | ReachableAcc], ErrorAcc};
          {ok, false} ->
            {[{Node, Reply} | ReachableAcc], ErrorAcc};
          {error, timeout} ->
            {ReachableAcc, [{Node, Reply} | ErrorAcc]};
          _ ->
            {ReachableAcc, [{Node, Reply} | ErrorAcc]}
        end
      end,
      {[], []},
      Replies0
    ),
  {lists:reverse(ReachableReplies), lists:reverse(ReplyErrors) ++ Errors}.

decide_recovery(TRef) ->
  {Replies0, Errors} =
    ecall:call_all_wait(zaya:all_nodes(), ?MODULE, is_rollbacked, [TRef]),
  {ReachableReplies, CombinedErrors} =
    classify_recovery_responses(Replies0, Errors),
  case lists:any(fun({_Node, Reply}) -> Reply =:= {ok, true} end, ReachableReplies) of
    true ->
      rollback;
    false ->
      CoordinatorNode = node(TRef),
      case lists:keyfind(CoordinatorNode, 1, ReachableReplies) of
        {CoordinatorNode, {ok, false}} ->
          commit;
        _ when CombinedErrors =/= [] ->
          pending_transaction_decision(TRef);
        _ ->
          pending_transaction_decision(TRef)
      end
  end.

pending_transaction_decision(TRef) ->
  Participation = rollback_participants(TRef),
  put_pending_transaction(TRef, Participation),
  try
    pending_transaction_decision_loop(TRef)
  after
    remove_pending_transaction(TRef)
  end.

pending_transaction_decision_loop(TRef) ->
  {Replies0, Errors} =
    ecall:call_all_wait(zaya:all_nodes(), ?MODULE, is_rollbacked, [TRef]),
  {ReachableReplies, CombinedErrors} =
    classify_recovery_responses(Replies0, Errors),
  case lists:any(fun({_Node, Reply}) -> Reply =:= {ok, true} end, ReachableReplies) of
    true ->
      rollback;
    false ->
      CoordinatorNode = node(TRef),
      case lists:keyfind(CoordinatorNode, 1, ReachableReplies) of
        {CoordinatorNode, {ok, false}} ->
          commit;
        _ when CombinedErrors =/= [] ->
          case pending_env_action() of
            undefined ->
              timer:sleep(1000),
              pending_transaction_decision_loop(TRef);
            Action ->
              ?LOGWARNING("~p ~p by PENDING_TRANSACTIONS", [TRef, Action]),
              Action
          end;
        _ ->
          case pending_env_action() of
            undefined ->
              timer:sleep(1000),
              pending_transaction_decision_loop(TRef);
            Action ->
              ?LOGWARNING("~p ~p by PENDING_TRANSACTIONS", [TRef, Action]),
              Action
          end
      end
  end.

pending_env_action() ->
  case os:getenv("PENDING_TRANSACTIONS") of
    "COMMIT" -> commit;
    "ROLLBACK" -> rollback;
    _ -> undefined
  end.

rollback_participants(TRef) ->
  Ref = persistent_term:get(?REF_KEY),
  case zaya_rocksdb:read(Ref, [{rollbacked, TRef}]) of
    [{{rollbacked, TRef}, NodesByDB}] -> NodesByDB;
    [] -> []
  end.

rollback_entries(DB) ->
  Ref = persistent_term:get(?REF_KEY),
  zaya_rocksdb:foldl(
    Ref,
    #{},
    fun
      ({{rollback, EntryDB, Seq, TRef}, Value}, Acc) when EntryDB =:= DB ->
        [{{rollback, EntryDB, Seq, TRef}, Value} | Acc];
      (_, Acc) ->
        Acc
    end,
    []
  ).

put_pending_transaction(TRef, Participation) ->
  true = ets:insert(?PENDING_TABLE, {TRef, Participation}),
  ok.

remove_pending_transaction(TRef) ->
  true = ets:delete(?PENDING_TABLE, TRef),
  ok.

cleanup_rollbacked_entry(Ref, TRef, NodesByDB) ->
  AllDBs = zaya:all_dbs(),
  AllNodes = zaya:all_nodes(),
  Remaining =
    lists:filtermap(
      fun({DB, Nodes}) ->
        case lists:member(DB, AllDBs) of
          false ->
            false;
          true ->
            FilteredNodes =
              [
                Node
                || Node <- Nodes,
                   lists:member(Node, AllNodes),
                   not lists:member(Node, zaya:db_available_nodes(DB))
              ],
            case FilteredNodes of
              [] -> false;
              _ -> {true, {DB, FilteredNodes}}
            end
        end
      end,
      NodesByDB
    ),
  case Remaining of
    [] ->
      zaya_rocksdb:delete(Ref, [{rollbacked, TRef}]);
    _ ->
      zaya_rocksdb:write(Ref, [{{rollbacked, TRef}, Remaining}])
  end.

maybe_delete(_Ref, []) ->
  ok;
maybe_delete(Ref, Keys) ->
  zaya_rocksdb:delete(Ref, Keys).

schedule_cleanup(Interval) when is_integer(Interval), Interval > 0 ->
  erlang:send_after(Interval, self(), cleanup),
  ok;
schedule_cleanup(_Interval) ->
  ok.

ensure_pending_table() ->
  case ets:info(?PENDING_TABLE) of
    undefined ->
      _ =
        ets:new(
          ?PENDING_TABLE,
          [named_table, public, set, {read_concurrency, true}, {write_concurrency, true}]
        ),
      ok;
    _ ->
      true = ets:delete_all_objects(?PENDING_TABLE),
      ok
  end.

ref_wait_timeout_ms() ->
  case maps:get(ref_wait_timeout_ms, application:get_env(zaya, transaction_log, #{}), ?DEFAULT_REF_WAIT_TIMEOUT_MS) of
    infinity ->
      infinity;
    TimeoutMs when is_integer(TimeoutMs), TimeoutMs > 0 ->
      TimeoutMs;
    _ ->
      ?DEFAULT_REF_WAIT_TIMEOUT_MS
  end.

ref_wait_poll_ms() ->
  case maps:get(ref_wait_poll_ms, application:get_env(zaya, transaction_log, #{}), ?DEFAULT_REF_WAIT_POLL_MS) of
    PollMs when is_integer(PollMs), PollMs > 0 ->
      PollMs;
    _ ->
      ?DEFAULT_REF_WAIT_POLL_MS
  end.
