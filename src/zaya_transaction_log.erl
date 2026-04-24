-module(zaya_transaction_log).

-include("zaya.hrl").
-include("zaya_transaction.hrl").

-behaviour(gen_server).

-export([
  start_link/0,
  seq/0,
  commit/2,
  is_rollbacked/1,
  rollback/2,
  purge/1,
  list_pending_transactions/0,
  list_pending_transactions/1
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

-define(RUNTIME_KEY, {?MODULE, runtime}).
-define(DEFAULT_REF_WAIT_TIMEOUT_MS, 600000).
-define(DEFAULT_REF_WAIT_POLL_MS, 1000).
-define(DEFAULT_CLEANUP_INTERVAL_MS, 60000).

-record(runtime, {
  db_ref,
  atomics_ref
}).

-record(state, {
  cleanup_interval_ms
}).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

seq() ->
  atomics:add_get((runtime())#runtime.atomics_ref, 1, 1) - 1.

commit(Write, Delete) ->
  zaya_rocksdb:commit(db_ref(), Write, Delete).

is_rollbacked(TRef) ->
  case wait_for_runtime(ref_wait_timeout_ms()) of
    {ok, #runtime{db_ref = Ref}} ->
      case zaya_rocksdb:read(Ref, [#rollbacked{tref = TRef}]) of
        [_] -> {ok, true};
        [] -> {ok, false}
      end;
    {error, timeout} ->
      {error, timeout}
  end.

rollback(DB, Callback) ->
  Entries = rollback_entries(DB),
  lists:foreach(
    fun({#rollback{} = Key, {RollbackWrite, RollbackDelete}}) ->
      case decide_recovery(Key#rollback.tref) of
        rollback ->
          ok = Callback({RollbackWrite, RollbackDelete}),
          ok = commit([], [Key]);
        commit ->
          ok = commit([], [Key])
      end
    end,
    Entries
  ),
  ok.

purge(DB) ->
  Keys = [Key || {Key, _} <- rollback_entries(DB)],
  case Keys of
    [] -> ok;
    _ -> zaya_rocksdb:delete(db_ref(), Keys)
  end.

%% Enumerate every transaction with a live rollback entry in the log.
%% An entry is "pending" until its rollback is applied or purged.
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
        (_, Acc) ->
          Acc
      end,
      sets:new()
    ),
  sets:to_list(Set).

init([]) ->
  #{cleanup_interval_ms := Interval} = Settings = read_config(),
  Runtime = open_log_db(Settings),
  persistent_term:put(?RUNTIME_KEY, Runtime),
  %% Run the first cleanup immediately so startup-time stale entries
  %% are not kept around for a full interval.
  self() ! cleanup,
  {ok, #state{cleanup_interval_ms = Interval}}.

read_config() ->
  Raw = application:get_env(zaya, transaction_log, #{}),
  #{
    dir => maps:get(dir, Raw, filename:join(zaya:schema_dir(), "TLOG")),
    storage_options => maps:with([pool, rocksdb], Raw),
    cleanup_interval_ms => validate_cleanup_interval(maps:get(cleanup_interval_ms, Raw, ?DEFAULT_CLEANUP_INTERVAL_MS))
  }.

validate_cleanup_interval(Value) when is_integer(Value), Value > 0 ->
  Value;
validate_cleanup_interval(Invalid) ->
  ?LOGERROR(
    "invalid cleanup_interval_ms=~p; falling back to default ~p",
    [Invalid, ?DEFAULT_CLEANUP_INTERVAL_MS]
  ),
  ?DEFAULT_CLEANUP_INTERVAL_MS.

open_log_db(#{dir := Dir, storage_options := Storage}) ->
  StorageConfig = Storage#{dir => Dir},
  DbRef =
    case filelib:is_dir(Dir) of
      true -> zaya_rocksdb:open(StorageConfig);
      false -> zaya_rocksdb:create(StorageConfig)
    end,
  AtomicsRef = atomics:new(1, [{signed, false}]),
  ok = atomics:put(AtomicsRef, 1, init_seq(DbRef)),
  #runtime{db_ref = DbRef, atomics_ref = AtomicsRef}.

init_seq(DbRef) ->
  zaya_rocksdb:foldl(
    DbRef,
    #{},
    fun
      ({#rollback{seq = Seq}, _Value}, Acc) ->
        erlang:max(Seq + 1, Acc);
      (_, Acc) ->
        Acc
    end,
    0
  ).

handle_call(_Request, _From, State) ->
  {reply, {error, unsupported}, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(cleanup, State = #state{cleanup_interval_ms = Interval}) ->
  try
    cleanup(db_ref())
  catch
    Class:Reason:Stack ->
      ?LOGERROR(
        "transaction_log cleanup failed: ~p:~p ~p",
        [Class, Reason, Stack]
      )
  end,
  schedule_cleanup(Interval),
  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  case persistent_term:get(?RUNTIME_KEY, undefined) of
    undefined ->
      ok;
    #runtime{db_ref = Ref} ->
      persistent_term:erase(?RUNTIME_KEY),
      catch zaya_rocksdb:close(Ref),
      ok
  end.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

runtime() ->
  persistent_term:get(?RUNTIME_KEY).

db_ref() ->
  (runtime())#runtime.db_ref.

wait_for_runtime(infinity) ->
  case persistent_term:get(?RUNTIME_KEY, undefined) of
    undefined ->
      timer:sleep(ref_wait_poll_ms()),
      wait_for_runtime(infinity);
    Runtime ->
      {ok, Runtime}
  end;
wait_for_runtime(RemainingMs) when RemainingMs =< 0 ->
  {error, timeout};
wait_for_runtime(RemainingMs) ->
  case persistent_term:get(?RUNTIME_KEY, undefined) of
    undefined ->
      SleepMs = erlang:min(ref_wait_poll_ms(), RemainingMs),
      timer:sleep(SleepMs),
      wait_for_runtime(RemainingMs - SleepMs);
    Runtime ->
      {ok, Runtime}
  end.

%% Classify ecall:call_all_wait/4 output into a compact decision shape.
%% Reachable nodes are those that returned a boolean {ok, true|false};
%% everything else (bad tuples, timeouts, RPC errors) counts as an error.
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

%% Single source of truth for the recovery decision tree.
%% Returns rollback | commit | pending.
%%   - rollback: any reachable node has a {rollbacked} marker.
%%   - commit:   the coordinator replied "no marker" and no peer said otherwise.
%%   - pending:  neither conclusive (coordinator unreachable, no peer evidence).
probe_recovery(TRef) ->
  {Replies, Errors} =
    ecall:call_all_wait(zaya:all_nodes(), ?MODULE, is_rollbacked, [TRef]),
  {Reachable, _ErrorCount} = classify_recovery_responses(Replies, Errors),
  case lists:any(fun({_, R}) -> R end, Reachable) of
    true ->
      %% R1: any peer marker wins, even over a coordinator "false".
      rollback;
    false ->
      case lists:keyfind(node(TRef), 1, Reachable) of
        %% R2: coordinator is reachable and has no marker -> committed.
        {_, false} -> commit;
        %% R3: coordinator unreachable and no peer evidence; wait for either.
        _          -> pending
      end
  end.

decide_recovery(TRef) ->
  case probe_recovery(TRef) of
    pending  -> pending_transaction_decision_loop(TRef);
    Decision -> Decision
  end.

pending_transaction_decision_loop(TRef) ->
  case probe_recovery(TRef) of
    pending ->
      %% Still no conclusive answer. Either an operator has set the
      %% env override, or we keep retrying until peer evidence shows up.
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

pending_env_action() ->
  case os:getenv("PENDING_TRANSACTIONS") of
    "COMMIT" -> commit;
    "ROLLBACK" -> rollback;
    _ -> undefined
  end.

rollback_participants(TRef) ->
  case zaya_rocksdb:read(db_ref(), [#rollbacked{tref = TRef}]) of
    [{_, NodesByDB}] -> NodesByDB;
    [] -> []
  end.

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

cleanup(Ref) ->
  ActiveTRefs = clean_rollbacks(Ref, zaya:node_dbs(node())),
  clean_rollbacked(Ref, ActiveTRefs),
  ok.

clean_rollbacks(Ref, LocalDBs) ->
  {ActiveTRefs, StaleKeys} =
    zaya_rocksdb:foldl(
      Ref,
      #{},
      fun
        ({#rollback{db = DB, tref = TRef} = Key, _Value}, {ActiveAcc, DeleteAcc}) ->
          case lists:member(DB, LocalDBs) of
            true  -> {sets:add_element(TRef, ActiveAcc), DeleteAcc};
            false -> {ActiveAcc, [Key | DeleteAcc]}
          end;
        (_, Acc) ->
          Acc
      end,
      {sets:new(), []}
    ),
  maybe_delete(Ref, StaleKeys),
  ActiveTRefs.

clean_rollbacked(Ref, ActiveTRefs) ->
  zaya_rocksdb:foldl(
    Ref,
    #{},
    fun
      ({#rollbacked{tref = TRef}, NodesByDB}, ok) ->
        %% A {rollbacked} marker is load-bearing while any of its
        %% transaction's #rollback{} entries still live locally.
        case sets:is_element(TRef, ActiveTRefs) of
          true  -> ok;
          false -> cleanup_rollbacked_entry(Ref, TRef, NodesByDB)
        end;
      (_, ok) ->
        ok
    end,
    ok
  ).

cleanup_rollbacked_entry(Ref, TRef, NodesByDB) ->
  %% Shrink (DB, [Node]) participation: drop removed DBs, drop
  %% decommissioned nodes, and drop nodes that already resolved
  %% recovery for this DB (proven by membership in db_available_nodes/1).
  %% When nothing is left to witness, the marker can be deleted.
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
      zaya_rocksdb:delete(Ref, [#rollbacked{tref = TRef}]);
    _ ->
      zaya_rocksdb:write(Ref, [{#rollbacked{tref = TRef}, Remaining}])
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
