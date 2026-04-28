
-module(zaya_transaction).

-include("zaya.hrl").
-include("zaya_schema.hrl").
-include("zaya_transaction.hrl").

-define(transaction,{?MODULE,'$transaction$'}).
-record(transaction,{data, changes ,locks}).
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
-record(context,{
  tref,
  data,
  nodes,
  ns_dbs,
  scope,
  is_persistent
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
  Aborted = #aborted{tref = TRef},
  zaya_transaction_log:commit(
    [{Aborted, [node()]} | Log],
    _Deletes = []
  ),

  try
    apply_local_commits(Commits),
    on_commit(DBs),
    ok
  catch
    C:E:S->
      rollback_local_commits(Commits),
      erlang:raise(C,E,S)
  after
    zaya_transaction_log:commit(
      _Writes = [],
      [Aborted | [R || {R,_} <- Log]]
    )
  end.

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

coordinator( Context ) ->
%-----------------PHASE 1------------------------------------------
  Workers = spawn_workers(Context, self()),
  case wait_commit1( Workers ) of
    ok ->
%-----------------PHASE 2------------------------------------------
      broadcast_workers(Workers, {commit2, Workers}),
      wait_commit2(Workers),
      exit(normal);
    {abort, Worker, Reason } ->
%---------------ABORT----------------------------------------------
      log_aborted( Context ),
      broadcast_workers(Workers, {rollback, Workers}),
      wait_worker_downs(Workers -- [Worker]),
      exit( Reason )
  end.

spawn_workers(#context{
  tref = TRef,
  data = Data,
  nodes = Nodes,
  ns_dbs = NsDBs,
  scope = Scope,
  is_persistent = IsPersistent
}, Coordinator) ->
  [ begin
      WorkerDBs = maps:with(maps:get(N, NsDBs), Data),
      {Worker, _Ref} =
        spawn_monitor(
          N,
          ?MODULE,
          commit_request,
          [#commit_request{
            tref = TRef,
            dbs = WorkerDBs,
            scope = Scope,
            is_persistent = IsPersistent,
            coordinator = Coordinator
          }]
        ),
      Worker
    end || N <- Nodes ].

wait_commit1(Workers) when length(Workers) > 0->
  receive
    {commit1, confirm, W}->
      wait_commit1(Workers -- [W]);
    {'DOWN', _Ref, process, W, Reason}->
      case lists:member(W, Workers) of
        true ->
          ?LOGWARNING("transaction abort: node ~p reason ~p",[node(W),Reason]),
          {abort, W, Reason};
        _->
          ?LOGWARNING("unexpected DOWN from ~p, reason: ~p",[W, Reason]),
          wait_commit1(Workers)
      end;
    Unexpected->
      ?LOGWARNING("unexpected message: ~p",[Unexpected]),
      wait_commit1(Workers)
  end;
wait_commit1(_Workers) ->
  ok.

wait_commit2( Workers ) when length( Workers )>0 ->
  receive
    {commit2, confirmed, W}->
      wait_commit2( Workers -- [W] );
    {'DOWN', _Ref, process, W, Reason}->
      case lists:member(W, Workers) of
        true ->
          ?LOGWARNING("~p node commit2 error: ~p",[ node(W), Reason ]),
          wait_commit2( Workers -- [W] );
        _->
          ?LOGWARNING("unexpected DOWN from ~p, reason: ~p",[W, Reason]),
          wait_commit2(Workers)
      end;
    Unexpected->
      ?LOGWARNING("unexpected message: ~p",[Unexpected]),
      wait_commit1(Workers)
  end;
wait_commit2( _Workers )->
  ok.

log_aborted(#context{
  tref = TRef,
  scope = Scope,
  nodes = Nodes,
  is_persistent = IsPersistent
}) ->
  % If the coordinator's node participates in the transaction then #aborted is going to be logged by the local worker
  NeedsLog = IsPersistent andalso (not lists:member(node(), Nodes)),
  if
    NeedsLog ->
      zaya_transaction_log:commit([{#aborted{tref = TRef}, Scope}], _Deletes = []);
    true ->
      ignore
  end.

wait_worker_downs(Workers) when length(Workers) > 0 ->
  receive
    {'DOWN', _Ref, process, W, Reason}->
      case lists:member(W, Workers) of
        true ->
          wait_worker_downs(Workers -- [W]);
        _->
          ?LOGWARNING("unexpected DOWN from ~p, reason: ~p",[W, Reason]),
          wait_worker_downs(Workers)
      end;
    Unexpected->
      ?LOGWARNING("unexpected message: ~p",[Unexpected]),
      wait_worker_downs(Workers)
  end;
wait_worker_downs(_Workers) ->
  ok.

commit_request(#commit_request{
  tref = TRef,
  dbs = DBs,
  scope = Scope,
  is_persistent = IsPersistent,
  coordinator = Coordinator
})->
  erlang:monitor(process, Coordinator),
  case prepare_worker_state(TRef, DBs) of
    {ok, LocalCommits, LocalPersistent, Seq} ->
      case try_apply_local_commits(LocalCommits) of
        ok ->
          ecall:send(Coordinator, {commit1, confirm, self()}),
          wait_worker_decision(
            TRef,
            DBs,
            Coordinator,
            LocalCommits,
            LocalPersistent,
            Seq,
            Scope,
            IsPersistent
          );
        {error, Class, Reason, Stack} ->
          ok = rollback_local_commits(LocalCommits),
          ok = maybe_log_worker_failure(
            TRef,
            Scope,
            IsPersistent,
            LocalPersistent,
            Seq
          ),
          erlang:raise(Class, Reason, Stack)
      end;
    {error, Class, Reason, Stack} ->
      ok = maybe_log_worker_failure(
        TRef,
        Scope,
        IsPersistent,
        [],
        undefined
      ),
      erlang:raise(Class, Reason, Stack)
  end.

prepare_worker_state(TRef, DBs) ->
  try
    LocalCommits = prepare_local_commits(DBs),
    LocalPersistent = persistent_commits(LocalCommits),
    Seq = maybe_write_worker_rollbacks(TRef, LocalPersistent),
    {ok, LocalCommits, LocalPersistent, Seq}
  catch
    Class:Reason:Stack->
      {error, Class, Reason, Stack}
  end.

try_apply_local_commits(LocalCommits) ->
  try apply_local_commits(LocalCommits)
  catch
    Class:Reason:Stack->
      {error, Class, Reason, Stack}
  end.

wait_worker_decision(
  TRef,
  DBs,
  Coordinator,
  LocalCommits,
  LocalPersistent,
  Seq,
  Scope,
  IsPersistent
) ->
  receive
    {commit2, Workers}->
      ecall:send(Coordinator, {commit2, confirmed, self()}),
      wait_after_commit2(Coordinator, Workers, DBs, TRef, LocalPersistent, Seq);
    {rollback, Workers}->
      ok = broadcast_workers(Workers, {aborted, self()}),
      ok = maybe_write_aborted(TRef, Scope, IsPersistent),
      ok = rollback_local_commits(LocalCommits),
      ok = maybe_delete_worker_rollbacks(TRef, LocalPersistent, Seq),
      exit(normal);
    {'DOWN', _Ref, process, Coordinator, _Reason} ->
      resolve_unknown_worker_decision(
        TRef,
        DBs,
        LocalCommits,
        LocalPersistent,
        Seq,
        Scope,
        IsPersistent
      )
  end.

wait_after_commit2(Coordinator, Workers, DBs, TRef, LocalPersistent, Seq) ->
  receive
    {'DOWN', _Ref, process, Coordinator, normal}->
      ok = maybe_delete_worker_rollbacks(TRef, LocalPersistent, Seq),
      on_commit(DBs),
      exit(normal);
    {'DOWN', _Ref, process, Coordinator, _Reason}->
      ok = broadcast_workers(Workers, {committed, self()}),
      ok = maybe_delete_worker_rollbacks(TRef, LocalPersistent, Seq),
      on_commit(DBs),
      exit(normal);
    {committed, _From}->
      ok = maybe_delete_worker_rollbacks(TRef, LocalPersistent, Seq),
      on_commit(DBs),
      exit(normal)
  end.

resolve_unknown_worker_decision(
  TRef,
  DBs,
  LocalCommits,
  LocalPersistent,
  Seq,
  Scope,
  IsPersistent
) ->
  ok = ensure_pg_scope(),
  Group = worker_group(TRef),
  ok = pg:join(Group, self()),
  {PgRef, _Members} = pg:monitor(Group),
  [
    erlang:monitor_node(Node, true)
    || Node <- all_participating_nodes(Scope),
       Node =/= node()
  ],
  wait_unknown_worker_decision(
    Group,
    PgRef,
    sets:new(),
    TRef,
    DBs,
    LocalCommits,
    LocalPersistent,
    Seq,
    Scope,
    IsPersistent
  ).

wait_unknown_worker_decision(
  Group,
  PgRef,
  DownNodes,
  TRef,
  DBs,
  LocalCommits,
  LocalPersistent,
  Seq,
  Scope,
  IsPersistent
) ->
  case rollback_consensus_reached(Group, Scope, DownNodes) of
    true ->
      ok = broadcast_group_decision(Group, {aborted, self()}),
      ok = maybe_write_aborted(TRef, Scope, IsPersistent),
      ok = rollback_local_commits(LocalCommits),
      ok = maybe_delete_worker_rollbacks(TRef, LocalPersistent, Seq),
      exit(normal);
    false ->
      receive
        {committed, _From}->
          ok = broadcast_group_decision(Group, {committed, self()}),
          ok = maybe_delete_worker_rollbacks(TRef, LocalPersistent, Seq),
          on_commit(DBs),
          exit(normal);
        {aborted, _From}->
          ok = broadcast_group_decision(Group, {aborted, self()}),
          ok = maybe_write_aborted(TRef, Scope, IsPersistent),
          ok = rollback_local_commits(LocalCommits),
          ok = maybe_delete_worker_rollbacks(TRef, LocalPersistent, Seq),
          exit(normal);
        {PgRef, join, Group, _Pids}->
          wait_unknown_worker_decision(
            Group,
            PgRef,
            DownNodes,
            TRef,
            DBs,
            LocalCommits,
            LocalPersistent,
            Seq,
            Scope,
            IsPersistent
          );
        {PgRef, leave, Group, _Pids}->
          wait_unknown_worker_decision(
            Group,
            PgRef,
            DownNodes,
            TRef,
            DBs,
            LocalCommits,
            LocalPersistent,
            Seq,
            Scope,
            IsPersistent
          );
        {nodedown, Node}->
          wait_unknown_worker_decision(
            Group,
            PgRef,
            sets:add_element(Node, DownNodes),
            TRef,
            DBs,
            LocalCommits,
            LocalPersistent,
            Seq,
            Scope,
            IsPersistent
          )
      after ?WORKER_RESOLUTION_POLL_MS ->
        case any_silent_node_aborted(TRef, Group, Scope, DownNodes) of
          true ->
            ok = broadcast_group_decision(Group, {aborted, self()}),
            ok = maybe_write_aborted(TRef, Scope, IsPersistent),
            ok = rollback_local_commits(LocalCommits),
            ok = maybe_delete_worker_rollbacks(TRef, LocalPersistent, Seq),
            exit(normal);
          false ->
            wait_unknown_worker_decision(
              Group,
              PgRef,
              DownNodes,
              TRef,
              DBs,
              LocalCommits,
              LocalPersistent,
              Seq,
              Scope,
              IsPersistent
            )
        end
      end
  end.



broadcast_workers(Workers, Message) ->
  [ecall:send(Worker, Message) || Worker <- Workers, Worker =/= self()],
  ok.

maybe_write_worker_rollbacks(_TRef, []) ->
  undefined;
maybe_write_worker_rollbacks(TRef, LocalPersistent) ->
  Seq = zaya_transaction_log:seq(),
  ok = maybe_log_batch(rollback_entries(LocalPersistent, Seq, TRef), []),
  Seq.

maybe_delete_worker_rollbacks(_TRef, _LocalPersistent, undefined) ->
  ok;
maybe_delete_worker_rollbacks(TRef, LocalPersistent, Seq) ->
  maybe_log_batch([], rollback_keys(LocalPersistent, Seq, TRef)).

maybe_write_aborted(_TRef, _Scope, false) ->
  ok;
maybe_write_aborted(TRef, Scope, true) ->
  maybe_log_batch([{#aborted{tref = TRef}, Scope}], []).


maybe_log_worker_failure(TRef, Scope, IsPersistent, LocalPersistent, Seq) ->
  Writes =
    case IsPersistent of
      true -> [{#aborted{tref = TRef}, Scope}];
      false -> []
    end,
  Deletes =
    case Seq of
      undefined -> [];
      _ -> rollback_keys(LocalPersistent, Seq, TRef)
    end,
  maybe_log_batch(Writes, Deletes).

maybe_log_batch([], []) ->
  ok;
maybe_log_batch(Writes, Deletes) ->
  zaya_transaction_log:commit(Writes, Deletes).

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

%%-----------------------------------------------------------
%%  MULTI NODE LOG HELPERS
%%-----------------------------------------------------------
rollback_entries(LocalCommits, Seq, TRef) ->
  [
    {#rollback{db = Commit#local_commit.db, seq = Seq, tref = TRef},
     rollback_ops(Commit)}
    || Commit <- LocalCommits
  ].

rollback_ops(#local_commit{rollback = #ops{write = Write, delete = Delete}}) ->
  {Write, Delete}.

rollback_keys(LocalCommits, Seq, TRef) ->
  [
    #rollback{db = Commit#local_commit.db, seq = Seq, tref = TRef}
    || Commit <- LocalCommits
  ].

%%-----------------------------------------------------------
%%  MULTI NODE RECOVERY HELPERS
%%-----------------------------------------------------------
ensure_pg_scope() ->
  case whereis(pg) of
    undefined ->
      case pg:start_link() of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok
      end;
    _ ->
      ok
  end.

worker_group(TRef) ->
  {?MODULE, TRef}.

broadcast_group_decision(Group, Message) ->
  broadcast_workers(pg:get_members(Group), Message).

all_participating_nodes(Scope) ->
  ordsets:from_list(
    lists:append([Nodes || {_DB, Nodes} <- Scope])
  ).

rollback_consensus_reached(Group, Scope, DownNodes) ->
  MemberNodes = ordsets:from_list([node(Pid) || Pid <- pg:get_members(Group)]),
  lists:all(
    fun(Node) ->
      sets:is_element(Node, DownNodes) orelse lists:member(Node, MemberNodes)
    end,
    all_participating_nodes(Scope)
  ).

any_silent_node_aborted(TRef, Group, Scope, DownNodes) ->
  MemberNodes = ordsets:from_list([node(Pid) || Pid <- pg:get_members(Group)]),
  SilentNodes =
    [
      Node
      || Node <- all_participating_nodes(Scope),
         not sets:is_element(Node, DownNodes),
         not lists:member(Node, MemberNodes)
    ],
  lists:any(
    fun(Node) ->
      case ecall:call(Node, zaya_transaction_log, is_aborted, [TRef]) of
        {ok, {ok, true}} -> true;
        _ -> false
      end
    end,
    SilentNodes
  ).

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
