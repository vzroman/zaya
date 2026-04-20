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

-define(REF_KEY, {?MODULE, ref}).
-define(ATOMICS_REF_KEY, {?MODULE, atomics_ref}).
-define(PENDING_KEY, {?MODULE, pending_transactions}).
-define(REF_WAIT_ATTEMPTS, 10).
-define(REF_WAIT_MS, 100).

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
  case wait_for_ref(?REF_WAIT_ATTEMPTS) of
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
          _ = Callback({RollbackWrite, RollbackDelete}),
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
  maps:values(persistent_term:get(?PENDING_KEY, #{})).

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
  persistent_term:put(?PENDING_KEY, #{}),
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
  persistent_term:erase(?PENDING_KEY),
  catch zaya_rocksdb:close(Ref),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

wait_for_ref(0) ->
  {error, timeout};
wait_for_ref(Attempts) ->
  case persistent_term:get(?REF_KEY, undefined) of
    undefined ->
      timer:sleep(?REF_WAIT_MS),
      wait_for_ref(Attempts - 1);
    Ref ->
      {ok, Ref}
  end.

decide_recovery(TRef) ->
  {Replies0, Errors} =
    ecall:call_all_wait(zaya:all_nodes(), ?MODULE, is_rollbacked, [TRef]),
  ReachableReplies =
    [{Node, Reply} || {Node, Reply} <- Replies0, Reply =/= {error, timeout}],
  TimeoutReplies =
    [{Node, timeout} || {Node, {error, timeout}} <- Replies0],
  CombinedErrors = TimeoutReplies ++ Errors,
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
  ReachableReplies =
    [{Node, Reply} || {Node, Reply} <- Replies0, Reply =/= {error, timeout}],
  TimeoutReplies =
    [{Node, timeout} || {Node, {error, timeout}} <- Replies0],
  CombinedErrors = TimeoutReplies ++ Errors,
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
  Pending = persistent_term:get(?PENDING_KEY, #{}),
  persistent_term:put(
    ?PENDING_KEY,
    Pending#{
      TRef => {TRef, Participation, pending_env_action()}
    }
  ).

remove_pending_transaction(TRef) ->
  Pending = persistent_term:get(?PENDING_KEY, #{}),
  persistent_term:put(?PENDING_KEY, maps:remove(TRef, Pending)).

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
