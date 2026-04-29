
-module(zaya_transaction).

-include("zaya.hrl").
-include("zaya_schema.hrl").
-include("zaya_transaction.hrl").

-define(transaction,{?MODULE,'$transaction$'}).
-define(pg_group(TRef),{?MODULE,TRef}).

-record(transaction,{
  data,
  changes,
  locks
}).

-record(ops,{
  write,
  delete
}).

-record(local_commit,{
  db,
  module,
  ref,
  is_persistent,
  commit,
  rollback
}).

-record(commit_request,{
  tref,
  dbs,
  scope,
  is_persistent,
  coordinator
}).

-record(commit_state,{
  request,
  commits,
  log
}).

-record(context,{
  tref,
  data,
  nodes,
  ns_dbs,
  scope,
  is_persistent
}).

-record(msg,{
  tref,
  type,
  data
}).

-define(LOCK_TIMEOUT, 60000).
-define(ATTEMPTS,5).
-define(WORKER_RESOLUTION_POLL_MS, 100).
-define(none,{?MODULE,?undefined}).

%%=================================================================
%%	API
%%=================================================================
-export([
  read/3,
  write/3,
  delete/3,
  is_transaction/0,
  transaction/1
]).

%%=================================================================
%%	Internal API
%%=================================================================
-export([
  single_db_node_commit/2,
  single_db_node_commit/1,
  single_db_commit/1,
  single_node_commit/1,
  commit_request/1
]).

%%-----------------------------------------------------------
%%  READ
%%-----------------------------------------------------------
read(DB, Keys, _Lock = none)->
  %--------Dirty read (no lock)---------------------------
  % If the key is already in the transaction data we return it's transaction value.
  % Otherwise we don't add any new keys in the transaction data but read them in dirty mode
  #transaction{
    changes = Changes0,
    data = Data0
  } = get(?transaction),

  Changes = maps:get( DB, Changes0, ?undefined),
  Data = maps:get( DB, Data0, ?undefined ),

  { Ready, ToRead } = t_read(Keys, Changes, Data ),

  if
    length(Ready) =:= 0->
      zaya_db:read(DB, Keys);
    length(ToRead) =:=0->
      Ready;
    true ->
      Ready ++ zaya_db:read(DB, ToRead)
  end;

read(DB, Keys, Lock)->

  Transaction = #transaction{
    changes = Changes0,
    data = Data0,
    locks = Locks0
  } = get(?transaction),

  % Obtain locks on all keys before the action starts
  Locks = lock(DB, Keys, Lock, maps:get(DB, Locks0, #{}) ),

  Changes = maps:get( DB, Changes0, ?undefined),
  Data = maps:get( DB, Data0, ?undefined ),

  {Ready, ToRead} = t_read(Keys, Changes, Data ),

  if
    length( ToRead ) =:= 0 ->
      put( ?transaction, Transaction#transaction{
        locks = Locks0#{ DB => Locks }
      }),
      Ready;
    true ->

      ReadResult = zaya_db:read(DB, ToRead),
      ReadResultMap = maps:from_list( ReadResult ),

      NewData =
        if
          is_map( Data ) -> maps:merge( Data, ReadResultMap );
          true -> ReadResultMap
        end,

      put( ?transaction, Transaction#transaction{
        data = Data0#{ DB => NewData },
        locks = Locks0#{ DB => Locks }
      }),

      if
        length( Ready ) =:= 0 -> ReadResult;
        true -> Ready ++ ReadResult
      end
  end.

t_read(Keys, Changes, Data)->
  if
    Changes =:= ?undefined, Data =:= ?undefined ->
      {[], Keys};
    Data =:= ?undefined->
      t_data(Keys, Changes, _TData= [], _ToRead = [] );
    Changes =:= ?undefined->
      t_data(Keys, Data, _TData= [], _ToRead = [] );
    true ->
      t_data(Keys, Changes, Data, _TData= [], _ToRead = [] )
  end.

t_data([K|Rest], Data, TData, ToRead )->
  case Data of
    #{ K:= V } when V=/=?none->
      t_data( Rest, Data, [{K,V} | TData], ToRead );
    _->
      t_data(Rest, Data, TData, [K|ToRead])
  end;
t_data([], _Data, TData, ToRead )->
  { TData, ToRead }.

t_data([K|Rest], Changes, Data, TData, ToRead )->
  case Changes of
    #{ K := V } when V=/=?none ->
      t_data( Rest, Changes, Data, [{K,V} | TData], ToRead );
    #{ K:=_ }->
      t_data(Rest, Changes ,Data, TData, ToRead);
    _->
      case Data of
        #{ K:= V } when V=/=?none->
          t_data( Rest, Changes, Data, [{K,V} | TData], ToRead );
        _->
          t_data(Rest, Changes, Data, TData, [K|ToRead])
      end
  end;
t_data([], _Changes, _Data, TData, ToRead )->
  { TData, ToRead }.

%%-----------------------------------------------------------
%%  WRITE
%%-----------------------------------------------------------
write(DB, KVs, _Lock = none)->

  Transaction = #transaction{
    changes = Changes0
  } = get(?transaction),

  Changes = do_write(KVs, maps:get( DB, Changes0, #{}) ),

  put( ?transaction, Transaction#transaction{
    changes = Changes0#{ DB => Changes }
  }),

  ok;

write(DB, KVs, Lock)->

  Transaction = #transaction{
    changes = Changes0,
    locks = Locks0
  } = get(?transaction),

  % Obtain locks on all keys before the action starts
  Locks = lock(DB, [K || {K,_} <- KVs], Lock, maps:get(DB, Locks0, #{}) ),

  Changes = do_write(KVs, maps:get( DB, Changes0, #{}) ),

  put( ?transaction, Transaction#transaction{
    changes = Changes0#{ DB => Changes },
    locks = Locks0#{ DB => Locks }
  }),

  ok.

do_write([{K,V}|Rest], Changes )->
  do_write(Rest, Changes#{K=>V});
do_write([], Changes )->
  Changes.

%%-----------------------------------------------------------
%%  DELETE
%%-----------------------------------------------------------
delete(DB, Keys, _Lock = none)->

  Transaction = #transaction{
    changes = Changes0
  } = get(?transaction),

  Changes = do_delete(Keys, maps:get( DB, Changes0, #{}) ),

  put( ?transaction, Transaction#transaction{
    changes = Changes0#{ DB => Changes }
  }),

  ok;

delete(DB, Keys, Lock)->

  Transaction = #transaction{
    changes = Changes0,
    locks = Locks0
  } = get(?transaction),

  % Obtain locks on all keys before the action starts
  Locks = lock(DB, Keys, Lock, maps:get(DB, Locks0, #{}) ),

  Changes = do_delete(Keys, maps:get( DB, Changes0, #{}) ),

  put( ?transaction, Transaction#transaction{
    changes = Changes0#{ DB => Changes },
    locks = Locks0#{ DB => Locks }
  }),

  ok.

do_delete([K|Rest], Data)->
  do_delete( Rest, Data#{ K => ?none });
do_delete([], Data)->
  Data.

ensure_editable( DB )->
  case ?dbReadOnly(DB) of
    true ->
      throw({read_only, DB});
    _->
      case ?dbMasters(DB) of
        []->
          ?dbAvailableNodes(DB);
        Masters->
          Nodes = ?dbAvailableNodes(DB),
          case Masters -- (Nodes -- Masters) of
            []-> throw({unavailable, DB});
            _-> Nodes
          end
      end
  end.

%%-----------------------------------------------------------
%%  TRANSACTION
%%-----------------------------------------------------------
is_transaction()->
  case get(?transaction) of
    #transaction{} -> true;
    _-> false
  end.

transaction(Fun) when is_function(Fun,0)->
  case get(?transaction) of
    #transaction{locks = PLocks}=Parent->
      % Internal transaction
      try { ok, Fun() }
      catch
        _:{lock,Error}->
          % Lock errors restart the whole transaction starting from external
          throw({lock,Error});
        _:Error->
          % Other errors rollback just internal transaction changes and release only it's locks
          #transaction{locks = Locks} = get(?transaction),
          release_locks(Locks, PLocks),
          put(?transaction, Parent),
          % The user should decide to abort the whole transaction or not
          {abort,Error}
      end;
    _->
      % The transaction entry point
      run_transaction( Fun, ?ATTEMPTS )
  end;
transaction(_Fun)->
  throw(bad_argument).

run_transaction(Fun, Attempts)->
  % Create the transaction storage
  put(?transaction,#transaction{ data = #{}, changes = #{}, locks = #{} }),
  try
    Result = Fun(),

    % The transaction is ok, try to commit the changes.
    % The locks are held until committed or aborted
    #transaction{changes = Changes, data = Data} = get(?transaction),
    commit( Changes, Data ),

    erase_transaction(),
    % Committed
    {ok,Result}
  catch
    _:{lock,_} when Attempts > 1->
      % In the case of lock errors all held locks are released and the transaction starts from scratch
      erase_transaction(),
      run_transaction(Fun, Attempts-1 );
    _:Error->
      erase_transaction(),
      % Other errors lead to transaction abort
      {abort,Error}
  end.

erase_transaction()->
  #transaction{locks = Locks} = erase( ?transaction ),
  release_locks( Locks, #{} ).

%%-----------------------------------------------------------
%%  LOCKS
%%-----------------------------------------------------------
-record(locks,{ db, nodes, type, held, acc }).
lock(DB, Keys, Type, Locks) when Type=:=read; Type=:=write->

  LockNodes = ?dbAvailableNodes(DB),
  if
    length( LockNodes ) =:= 0 -> throw({unavailable,DB});
    true -> ok
  end,

  % Shared lock is set on the whole DB to prevent it's copies transformations
  % during transactions
  InitAcc =
    case maps:is_key({?MODULE,DB}, Locks) of
      true->
        #{};
      _->
        #{
          {?MODULE,DB} => {read, lock_key( DB, _IsShared=true, _Timeout=?infinity, LockNodes )}
        }
    end,

  lock(Keys, #locks{
    db = DB,
    nodes = LockNodes,
    type = Type,
    held = Locks,
    acc = InitAcc
  });
lock(_DB, _Keys, none, Locks)->
  % The lock is not needed
  Locks.

lock([K|Rest], #locks{
  acc = Acc
} = State)->
  KeyLock =
    try lock_key( K, State )
    catch
      _:E->
        [Unlock() || {_Type, Unlock} <- maps:values( Acc) ],
        throw(E)
    end,
  lock(Rest, State#locks{
    acc = Acc#{ K => KeyLock }
  });
lock([], #locks{
  held = Held,
  acc = Acc
})->
  maps:merge( Held, Acc).

lock_key(K, #locks{
  db = DB,
  nodes = Nodes,
  type = Type,
  held = Held
})->
  case Held of
    #{K := {HeldType,_} = Lock} when Type=:= read; HeldType =:= write ->
      Lock;
    _->
      % Write locks must be set on each DB copy.
      % Read locks. If the DB has local copy it's
      % enough to obtain only local lock because if the transaction
      % origin node fails the whole transaction will be aborted. Otherwise
      % The lock must be set at each node holding DB copy.
      % Because if we set lock only at one (or some copies) they can fall down
      % while the transaction is not finished then the lock will be lost and the
      % transaction can become not isolated
      IsLocal = lists:member(node(), Nodes),
      LockNodes =
        if
          Type =:= read, IsLocal->
            [node()];
          true->
            Nodes
        end,
      {Type, lock_key( {?MODULE,DB,K}, _IsShared = Type=:=read, ?LOCK_TIMEOUT, LockNodes )}
  end.

lock_key( Key, IsShared, Timeout, Nodes )->
  case elock:lock( ?locks, Key, IsShared, Timeout, Nodes) of
    {ok, Unlock}->
      Unlock;
    {error,Error}->
      throw({lock,Error})
  end.

release_locks(Locks, Parent )->
  maps:fold(fun(DB,Keys,_)->
    ParentKeys = maps:get(DB,Parent,#{}),
    maps:fold(fun(K,{_Type,Unlock},_)->
      case maps:is_key(K,ParentKeys) of
        true -> ignore;
        _->Unlock()
      end
    end,?undefined,Keys)
  end,?undefined,Locks).

%%-----------------------------------------------------------
%%  COMMIT
%%-----------------------------------------------------------
-record(commit,{ ns, ns_dbs, dbs_ns }).
commit( Changes, Data )->

  case prepare_data( Changes, Data ) of
    CommitData when map_size(CommitData) > 0->

      Commit = #commit{ns = Ns} = prepare_commit( CommitData ),

      if
        map_size(CommitData) =:= 1, length(Ns) =:= 1->
          single_db_node_commit( CommitData, Ns );
        map_size(CommitData) =:= 1->
          single_db_commit( CommitData, Ns );
        length(Ns) =:= 1 ->
          single_node_commit( CommitData, Ns );
        true ->
          multi_node_commit(CommitData, Commit)
      end;
    _->
      % Nothing to commit
      ok
  end.

prepare_data(Changes, Data)->

  % Changes and Data has structure:
  % #{
  %   DB1 => #{
  %     Key1 => Value,
  %     ...
  %   }
  %   .....
  % }

  maps:fold(fun(DB, DBChanges, Acc)->

    DBData = maps:get( DB, Data, #{} ),
    DBOps =
      maps:fold(fun(K, V, {Write, Delete} = OpsAcc) ->
        case DBData of
          #{ K := V0 } when V =:= V0 -> OpsAcc;
          _ when V =/= ?none -> {[{K,V}|Write], Delete };
          _-> {Write, [K|Delete] }
        end
      end, {[],[]}, DBChanges ),

    case DBOps of
      {[],[]} -> Acc;
      _-> Acc#{ DB => DBOps }
    end

  end, #{}, Changes).

prepare_commit( Data )->

  % Databases that has changes to be committed
  DBs = ordsets:from_list( maps:keys(Data) ),

  % The nodes hosting the database
  DBsNs =
    [ {DB,
        case ensure_editable(DB) of
          [] -> throw({unavailable, DB});
          Nodes -> Nodes
        end
      } || DB <- DBs ],

  % All participating nodes
  Ns =
    ordsets:from_list( lists:append([ Ns || {_DB,Ns} <- DBsNs ])),

  % Databases that each node has to commit
  NsDBs =
    [ {N, [DB || {DB, DBNs} <- DBsNs, lists:member(N, DBNs) ]} || N <- Ns ],

  #commit{
    ns=Ns,
    ns_dbs = maps:from_list(NsDBs),
    dbs_ns = maps:from_list(DBsNs)
  }.

%%-----------------------------------------------------------
%%  SINGLE DB & NODE COMMIT
%%-----------------------------------------------------------
single_db_node_commit(Data, [N]) when N=:=node()->
  single_db_node_commit(Data);
single_db_node_commit(Data, [N])->
  case ecall:call(N, ?MODULE, ?FUNCTION_NAME, [ Data ]) of
    {ok,_} -> ok;
    {error, Error}-> throw( Error )
  end.

single_db_node_commit( Data )->
  [{DB, {Write, Delete}}] = maps:to_list( Data ),
  {Ref, Module} = ?dbRefMod( DB ),

  Module:commit( Ref, Write, Delete ),

  on_commit( Data ),
  ok.

%%-----------------------------------------------------------
%%  SINGLE DB COMMIT
%%-----------------------------------------------------------
single_db_commit(Data, Ns)->
  case ecall:call_all_wait( Ns, ?MODULE, ?FUNCTION_NAME, [ Data ]) of
    {Replies,_} when length(Replies) > 0 -> ok;
    {_, Errors}-> throw( Errors )
  end.

single_db_commit( Data )->
  [{DB, {Write, Delete}}] = maps:to_list( Data ),
  {Ref, Module} = ?dbRefMod( DB ),

  Module:commit( Ref, Write, Delete ),

  on_commit( Data ),
  ok.

%%-----------------------------------------------------------
%%  SINGLE NODE COMMIT
%%-----------------------------------------------------------
single_node_commit(DBs, [N]) when N=:=node()->
  {Worker, Ref} =
    spawn_monitor(
      fun()->
        single_node_commit(DBs)
      end
    ),
  receive
    {'DOWN', Ref, process, Worker, normal}->
      ok;
    {'DOWN', Ref, process, Worker, Error}->
      throw( Error )
  end;
single_node_commit( DBs, [N])->
  case ecall:call(N, ?MODULE, ?FUNCTION_NAME, [DBs]) of
    {ok,_}-> ok;
    {error, Error}-> throw( Error )
  end.

single_node_commit(DBs) ->
  Commits = prepare_local_commits(DBs),
  IsPersistent = persistent_commits(Commits) =/= [],
  do_single_node_commit(IsPersistent, DBs, Commits).

do_single_node_commit(_IsPersistent = false, DBs, Commits)->
  try
    apply_local_commits(Commits),
    on_commit(DBs)
  catch
    C:E:S->
      rollback_local_commits(Commits),
      erlang:raise(C,E,S)
  end;

do_single_node_commit(_IsPersistent = true, DBs, Commits)->

  TRef = make_ref(),
  Log = prepare_log(Commits, TRef),

  % Write the decision marker (#aborted) in an atomic batch with the rollback log
  Aborted = {
    #aborted{tref = TRef},
    [{DB,[node()]} || DB <- maps:keys(DBs)]
  },

  AtomicLogBatch = [Aborted | Log],
  commit_log( AtomicLogBatch ),

  try
    apply_local_commits(Commits),
    on_commit(DBs),
    ok
  catch
    C:E:S->
      rollback_local_commits(Commits),
      erlang:raise(C,E,S)
  after
    purge_log(AtomicLogBatch)
  end.

%%-----------------------------------------------------------
%%  MULTI NODE COMMIT
%%-----------------------------------------------------------
multi_node_commit(Data, #commit{ns = Nodes, ns_dbs = NsDBs, dbs_ns = DBsNs})->
  TRef = make_ref(),
  Scope = maps:to_list(DBsNs),
  IsPersistent =
    lists:any(
      fun(DB)->
        Module = ?dbModule(DB),
        Module:is_persistent()
      end,
      maps:keys(Data)
    ),
  Context = #context{
    tref = TRef,
    data = Data,
    nodes = Nodes,
    ns_dbs = NsDBs,
    scope = Scope,
    is_persistent = IsPersistent
  },
  {Coordinator, Ref} =
    spawn_monitor(
      fun()->
        coordinator(Context)
      end
    ),
  receive
    {'DOWN', Ref, process, Coordinator, normal}->
      ok;
    {'DOWN', Ref, process, Coordinator, Error}->
      throw( Error )
  end.

%%-----------------------------------------------------------
%%  COORDINATOR
%%-----------------------------------------------------------
coordinator(#context{
  tref = TRef
}=Context) ->
%-----------------PHASE 1------------------------------------------
  Workers = spawn_workers( Context ),
  case wait_commit1(Workers, TRef) of
    ok ->
%-----------------PHASE 2------------------------------------------
      broadcast_workers(
        Workers,
        #msg{
          tref = TRef,
          type = commit,
          data = Workers
        }
      ),
      wait_commit2(Workers, TRef),
      exit(normal);
    {abort, Worker, Reason} ->
%---------------ABORT----------------------------------------------
      coordinator_log_aborted( Context ),
      RollbackWorkers = Workers -- [Worker],
      broadcast_workers(
        RollbackWorkers,
        #msg{
          tref = TRef,
          type = abort,
          data = RollbackWorkers
        }
      ),
      wait_worker_downs(RollbackWorkers),
      exit( Reason )
  end.

spawn_workers(#context{
  tref = TRef,
  scope = Scope,
  is_persistent = IsPersistent,
  nodes = Nodes,
  ns_dbs = NsDBs,
  data = Data
}) ->
  Request = #commit_request{
    tref = TRef,
    scope = Scope,
    is_persistent = IsPersistent,
    coordinator = self()
  },
  [ begin
      {Worker, _Ref} = spawn_monitor(N, ?MODULE, commit_request, [Request#commit_request{
        dbs = maps:with(maps:get(N, NsDBs), Data)
      }]),
      Worker
    end || N <- Nodes ].

wait_commit1(PendingWorkers, TRef) when length(PendingWorkers) > 0->
  receive
    #msg{ tref = TRef, type = commit1, data = W }->
      wait_commit1(PendingWorkers -- [W], TRef);
    {'DOWN', _Ref, process, W, Reason}->
      case lists:member(W, PendingWorkers) of
        true ->
          ?LOGWARNING("transaction abort: node ~p reason ~p",[node(W),Reason]),
          {abort, W, Reason};
        _->
          ?LOGWARNING("unexpected DOWN from ~p, reason: ~p",[W, Reason]),
          wait_commit1(PendingWorkers, TRef)
      end;
    Unexpected->
      ?LOGWARNING("unexpected message: ~p",[Unexpected]),
      wait_commit1(PendingWorkers, TRef)
  end;
wait_commit1(_PendingWorkers, _TRef) ->
  ok.

wait_commit2(PendingWorkers, TRef) when length(PendingWorkers) > 0 ->
  receive
    #msg{ tref = TRef, type = commit2, data = W }->
      wait_commit2(PendingWorkers -- [W], TRef);
    {'DOWN', _Ref, process, W, Reason}->
      case lists:member(W, PendingWorkers) of
        true ->
          ?LOGWARNING("~p node commit2 error: ~p",[ node(W), Reason ]),
          wait_commit2(PendingWorkers -- [W], TRef);
        _->
          ?LOGWARNING("unexpected DOWN from ~p, reason: ~p",[W, Reason]),
          wait_commit2(PendingWorkers, TRef)
      end;
    Unexpected->
      ?LOGWARNING("unexpected message: ~p",[Unexpected]),
      wait_commit2(PendingWorkers, TRef)
  end;
wait_commit2(_PendingWorkers, _TRef)->
  ok.

broadcast_workers(Workers, Message) ->
  [ecall:send(Worker, Message) || Worker <- Workers],
  ok.

coordinator_log_aborted(#context{
  tref = TRef,
  scope = Scope,
  nodes = Nodes,
  is_persistent = IsPersistent
}) ->
  % If the coordinator's node participates in the transaction then #aborted is going to be logged by the local worker
  NeedsLog = IsPersistent andalso (not lists:member(node(), Nodes)),
  if
    NeedsLog ->
      commit_log([{#aborted{tref = TRef}, Scope}]);
    true ->
      ignore
  end.

wait_worker_downs(PendingWorkers) when length(PendingWorkers) > 0 ->
  receive
    {'DOWN', _Ref, process, W, Reason}->
      case lists:member(W, PendingWorkers) of
        true ->
          wait_worker_downs(PendingWorkers -- [W]);
        _->
          ?LOGWARNING("unexpected DOWN from ~p, reason: ~p",[W, Reason]),
          wait_worker_downs(PendingWorkers)
      end;
    Unexpected->
      ?LOGWARNING("unexpected message: ~p",[Unexpected]),
      wait_worker_downs(PendingWorkers)
  end;
wait_worker_downs(_PendingWorkers) ->
  ok.

%%-----------------------------------------------------------
%%  WORKER
%%-----------------------------------------------------------
commit_request(#commit_request{
  tref = TRef,
  coordinator = Coordinator
} = Request)->

%----------------PHASE 1--------------------------
  State =
    maybe
      {ok, State1} ?= phase_1_prepare( Request ),
      {ok, State2} ?= phase_1_log( State1 ),
      {ok, State3} ?= phase_1_commit( State2 ),
      State3
    else
      {abort, Error, RollbackState}->
        rollback( RollbackState ),
        exit(Error)
    end,
  ecall:send(Coordinator, #msg{
    tref = TRef,
    type = commit1,
    data = self()
  }),

%----------------PHASE 2--------------------------
  receive
    #msg{tref = TRef, type = commit, data = Workers}->
%--------------COMMITTED--------------------------
      ecall:send(Coordinator, #msg{
        tref = TRef,
        type = commit2,
        data = self()
      }),
      maybe_broadcast_committed(Request, Workers),
      committed( State );
    #msg{tref = TRef, type = abort, data = Workers}->
%--------------ABORTED-----------------------------
      ok = broadcast_decision( Workers, TRef, abort ),
      rollback( State );
    {'DOWN', _Ref, process, Coordinator, _Reason} ->
%--------------THE DECISION IS NOT KNOWN-------------
      case resolve_decision( State ) of
        commit ->
          committed( State );
        abort->
          rollback( State )
      end
  end.

phase_1_prepare(#commit_request{
  coordinator = Coordinator,
  dbs = DBs
} =Request)->

  erlang:monitor(process, Coordinator),
  Commits = prepare_local_commits(DBs),

  {ok, #commit_state{
    request = Request,
    commits = Commits
  }}.

phase_1_log(#commit_state{
  request = #commit_request{
    tref = TRef
  },
  commits = Commits
} =State0)->
  case prepare_log(Commits, TRef) of
    []->
      {ok, State0};
    Log ->
      try
        commit_log( Log ),
        State = State0#commit_state{
          log = Log
        },
        {ok, State}
      catch
        _:E->
          {abort, E, State0}
      end
  end.

phase_1_commit(#commit_state{
  commits = Commits
} = State0)->
  try
    apply_local_commits( Commits )
  catch
    _:E->
      {abort, E, State0}
  end.

rollback(#commit_state{
  request = Request,
  commits = Commits,
  log = Log
})->
  log_aborted( Request ),
  rollback_local_commits( Commits ),
  purge_log( Log ),
  ok.

log_aborted(#commit_request{
  is_persistent = true,
  tref = TRef,
  scope = Scope
})->
  Aborted = {
    #aborted{ tref = TRef },
    Scope
  },
  try commit_log([Aborted])
  catch
    _:E:S->
      ?LOGERROR("unable to log aborted transaction ~p, error: ~p, stack: ~p",[TRef,E,S])
  end;
log_aborted(_Request)->
  % Transactions touching no persistent DBs don't need logging
  ignore.

maybe_broadcast_committed(
    #commit_request{
      coordinator = Coordinator,
      tref = TRef
    },
    Workers
)->
  receive
    {'DOWN', _Ref, process, Coordinator, normal}->
      ignore;
    {'DOWN', _Ref, process, Coordinator, Reason}->
      BroadcastTo = Workers -- [self()],
      broadcast_decision(Workers, TRef, commit),
      ?LOGINFO("transaction ~p coordinator down, reason: ~p, broadcast 'committed' to ~p",[
        TRef, Reason, [ node(W) || W <- BroadcastTo]
      ])
  after
    60000->
      ?LOGERROR("transaction ~p timeout on waiting for coordinator down",[TRef])
  end.

broadcast_decision(Workers, TRef, Decision)->
  BroadcastTo = Workers -- [self()],
  Msg = #msg{
    tref = TRef,
    type = Decision,
    data = BroadcastTo
  },
  [ecall:send(W, Msg) || W <- BroadcastTo],
  ok.

committed(#commit_state{
  log = Log,
  request = #commit_request{
    dbs = DBs
  }
})->
  purge_log( Log ),
  on_commit( DBs ),
  ok.

%--------------------------------------------------------------
% Coordinator is down, the decision is not known
%--------------------------------------------------------------
resolve_decision(#commit_state{
  request = #commit_request{
    tref = TRef,
    scope = Scope
  }
})->

  % Claim yourself as 'don't know' node
  PG = ?pg_group(TRef),
  ok = pg:join(?transaction_pg, PG, self()),

  % Monitor who else doesn't know
  {_Ref, DontKnowWorkers} = pg:monitor(?transaction_pg, PG),

  % Monitor the nodes, that haven't 'raised a hand' yet
  AllNodes = ordsets:from_list(
    lists:append([Nodes || {_DB, Nodes} <- Scope])
  ),
  PendingNodes = AllNodes -- [node(W) || W <- DontKnowWorkers],
  [erlang:monitor_node(N, true) || N <- PendingNodes],

  wait_for_decision(PendingNodes, TRef).

wait_for_decision(_PendingNodes = [], _TRef)->
  abort;
wait_for_decision(PendingNodes, TRef)->
  receive
    #msg{tref = TRef, type = Decision, data = Workers}->
      broadcast_decision(Workers, TRef, Decision),
      Decision;
    {_Ref, join, ?pg_group(TRef), DontKnowWorkers}->
      RestNodes = PendingNodes -- [node(W) || W <- DontKnowWorkers],
      wait_for_decision(RestNodes, TRef);
    {nodedown, Node}->
      RestNodes = PendingNodes -- [Node],
      wait_for_decision(RestNodes, TRef);
    _Other->
      wait_for_decision(PendingNodes, TRef)
  after ?WORKER_RESOLUTION_POLL_MS ->
    case request_is_aborted(PendingNodes, TRef) of
      true ->
        abort;
      false->
        wait_for_decision(PendingNodes, TRef)
    end
  end.

request_is_aborted(Nodes, TRef)->
  {Replies, _} = ecall:call_all_wait(Nodes, zaya_transaction_log, is_aborted, [TRef]),
  case [N || {N,{ok, true}} <- Replies] of
    [] -> false;
    _-> true
  end.

%%=============================================================
%% Internal commit helpers
%%=============================================================
prepare_log( Commits, TRef )->
  Seq = zaya_transaction_log:seq(),
  [prepare_log(Seq, TRef, C) || C <- persistent_commits(Commits)].
prepare_log(
    Seq,
    TRef,
    #local_commit{
      db = DB,
      rollback = #ops{
        write = Write,
        delete = Delete
      }
    }
)->
  Key = #rollback{
    db = DB,
    seq = Seq,
    tref = TRef
  },
  Value = {Write, Delete},
  {Key, Value}.

commit_log([_|_]=Log)->
  zaya_transaction_log:commit(
    Log,
    _Deletes = []
  );
commit_log(_Log)->
  ignore.

purge_log([_|_] =Log)->
  try
    zaya_transaction_log:commit(
      _Writes = [],
      [K || {K,_} <- Log]
    )
  catch
    _:E:S->
      ?LOGERROR("unable to purge transaction log ~p, error: ~p, stack: ~p",[Log,E,S])
  end;
purge_log(_Log)->
  % No local persistent DBs
  ignore.

%%-----------------------------------------------------------
%%  LOCAL COMMIT HELPERS
%%-----------------------------------------------------------
prepare_local_commits(DBs) ->
  [ prepare_local_commit(DB, Ops) || {DB, Ops} <- maps:to_list(DBs) ].

prepare_local_commit(DB, {Write, Delete}) ->
  {Ref, Module} = ?dbRefMod(DB),
  {RollbackWrite, RollbackDelete} = Module:prepare_rollback(Ref, Write, Delete),
  IsPersistent = Module:is_persistent(),
  #local_commit{
    db = DB,
    module = Module,
    ref = Ref,
    is_persistent = IsPersistent,
    commit = #ops{
      write = Write,
      delete = Delete
    },
    rollback = #ops{
      write = RollbackWrite,
      delete = RollbackDelete
    }
  }.

persistent_commits(LocalCommits) ->
  [Commit || Commit = #local_commit{is_persistent = true} <- LocalCommits].

apply_local_commits(LocalCommits) ->
  [apply_local_commit(Commit) || Commit <- LocalCommits],
  ok.

apply_local_commit(#local_commit{
  module = Module,
  ref = Ref,
  commit = #ops{
    write = Write,
    delete = Delete
  }
}) ->
  Module:commit(Ref, Write, Delete).

rollback_local_commits(LocalCommits) ->
  [rollback_local_commit(Commit) || Commit <- LocalCommits],
  ok.

rollback_local_commit(#local_commit{
  db = DB,
  module = Module,
  ref = Ref,
  rollback = #ops{
    write = RollbackWrite,
    delete = RollbackDelete
  }
}) ->
  try Module:commit(Ref, RollbackWrite, RollbackDelete)
  catch
    _:E->
      ?LOGERROR("~p rollback error: ~p",[DB, E])
  end.

on_commit(DBs)->
  maps:fold(fun(DB, {Write, Delete},_)->
    if
      length(Write) > 0-> catch zaya_db:on_update( DB, write, Write );
      true->ignore
    end,
    if
      length(Delete) > 0-> catch zaya_db:on_update( DB, delete, Delete );
      true->ignore
    end
  end,?undefined,DBs).
