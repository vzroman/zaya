
-module(zaya_transaction).

-include("zaya.hrl").
-include("zaya_schema.hrl").
-include("zaya_transaction.hrl").

-define(transaction,{?MODULE,'$transaction$'}).
-record(transaction,{data, changes ,locks}).
-record(local_commit,{
  db,
  module,
  ref,
  write,
  delete,
  rollback_write,
  rollback_delete,
  persistent
}).
-record(worker_request,{
  tref,
  dbs,
  all_participating_dbs_nodes,
  any_persistent,
  coordinator
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
single_db_node_commit(Data, [Node])->
  case ecall:call(Node, ?MODULE, ?FUNCTION_NAME, [ Data ]) of
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
single_node_commit( DBs, [N])->
  case ecall:call(N,?MODULE, ?FUNCTION_NAME, [ DBs ]) of
    {ok,_}-> ok;
    {error, Error}-> throw( Error )
  end.
single_node_commit( DBs )->
  TRef = make_ref(),
  {Worker, Ref} =
    spawn_monitor(
      fun()->
        run_single_node_worker(TRef, DBs)
      end
    ),
  receive
    {'DOWN', Ref, process, Worker, normal}->
      ok;
    {'DOWN', Ref, process, Worker, Error}->
      throw( Error )
  end.

%%-----------------------------------------------------------
%%  MULTI NODE COMMIT
%%-----------------------------------------------------------
multi_node_commit(Data, #commit{ns = Nodes, ns_dbs = NsDBs, dbs_ns = DBsNs})->
  TRef = make_ref(),
  AllParticipatingDBsNodes = maps:to_list(DBsNs),
  AnyPersistent =
    lists:any(
      fun(DB)->
        {_Ref, Module} = ?dbRefMod(DB),
        Module:is_persistent()
      end,
      maps:keys(Data)
    ),
  {Coordinator, Ref} =
    spawn_monitor(
      fun()->
        run_multi_node_coordinator(
          TRef,
          Data,
          Nodes,
          NsDBs,
          AllParticipatingDBsNodes,
          AnyPersistent
        )
      end
    ),
  receive
    {'DOWN', Ref, process, Coordinator, normal}->
      ok;
    {'DOWN', Ref, process, Coordinator, Error}->
      throw( Error )
  end.

run_single_node_worker(TRef, DBs) ->
  LocalCommits = prepare_local_commits(DBs),
  PersistentCommits = persistent_commits(LocalCommits),
  Seq = maybe_write_single_node_log(TRef, PersistentCommits),
  case apply_local_commits(LocalCommits) of
    ok ->
      ok = maybe_delete_single_node_log(TRef, PersistentCommits, Seq),
      on_commit(DBs),
      exit(normal);
    {error, Class, Reason, Stack} ->
      ok = rollback_local_commits(LocalCommits),
      ok = maybe_delete_single_node_log(TRef, PersistentCommits, Seq),
      erlang:raise(Class, Reason, Stack)
  end.

run_multi_node_coordinator(TRef, Data, Nodes, NsDBs, AllParticipatingDBsNodes, AnyPersistent) ->
  Workers =
    spawn_workers(
      Nodes,
      NsDBs,
      Data,
      TRef,
      AllParticipatingDBsNodes,
      AnyPersistent,
      self()
    ),
  case wait_commit1_confirms(Workers, Workers) of
    ok ->
      WorkerPids = maps:keys(Workers),
      ok = broadcast_workers(WorkerPids, {commit2, WorkerPids}),
      wait_commit2(Workers),
      exit(normal);
    {error, Error, SurvivingWorkers} ->
      ok = maybe_write_coordinator_rollbacked(TRef, AllParticipatingDBsNodes, AnyPersistent, Nodes),
      WorkerPids = maps:keys(SurvivingWorkers),
      ok = broadcast_workers(WorkerPids, {rollback, WorkerPids}),
      wait_worker_downs(SurvivingWorkers),
      exit(Error)
  end.

wait_commit1_confirms(_Workers, PendingWorkers) when map_size(PendingWorkers) =:= 0 ->
  ok;
wait_commit1_confirms(Workers, PendingWorkers) ->
  receive
    {commit1, confirm, Worker}->
      wait_commit1_confirms(Workers, maps:remove(Worker, PendingWorkers));
    {'DOWN', _Ref, process, W, Reason}->
      case maps:is_key(W, Workers) of
        true ->
          ?LOGWARNING("commit node ~p down ~p",[node(W),Reason]),
          {error, Reason, maps:remove(W, Workers)};
        _->
          wait_commit1_confirms(Workers, PendingWorkers)
      end
  end.

wait_commit2( Workers ) when map_size( Workers )>0 ->
  receive
    {commit2, confirmed, W}->
      wait_commit2( maps:remove( W, Workers ) );
    {'DOWN', _Ref, process, W, normal}->
      wait_commit2( maps:remove( W, Workers ) );
    {'DOWN', _Ref, process, W, Reason}->
      ?LOGWARNING("~p node commit2 error: ~p",[ node(W), Reason ]),
      wait_commit2( maps:remove( W, Workers ) )
  end;
wait_commit2( _Workers )->
  ok.

wait_worker_downs(Workers) when map_size(Workers) > 0 ->
  receive
    {'DOWN', _Ref, process, W, _Reason}->
      wait_worker_downs(maps:remove(W, Workers));
    _Message->
      wait_worker_downs(Workers)
  end;
wait_worker_downs(_Workers) ->
  ok.

commit_request(#worker_request{
  tref = TRef,
  dbs = DBs,
  all_participating_dbs_nodes = AllParticipatingDBsNodes,
  any_persistent = AnyPersistent,
  coordinator = Coordinator
})->
  erlang:monitor(process, Coordinator),
  case prepare_worker_state(TRef, DBs) of
    {ok, LocalCommits, LocalPersistent, Seq} ->
      case apply_local_commits(LocalCommits) of
        ok ->
          ecall:send(Coordinator, {commit1, confirm, self()}),
          wait_worker_decision(
            TRef,
            DBs,
            Coordinator,
            LocalCommits,
            LocalPersistent,
            Seq,
            AllParticipatingDBsNodes,
            AnyPersistent
          );
        {error, Class, Reason, Stack} ->
          ok = rollback_local_commits(LocalCommits),
          ok = maybe_log_worker_failure(
            TRef,
            AllParticipatingDBsNodes,
            AnyPersistent,
            LocalPersistent,
            Seq
          ),
          erlang:raise(Class, Reason, Stack)
      end;
    {error, Class, Reason, Stack} ->
      ok = maybe_log_worker_failure(
        TRef,
        AllParticipatingDBsNodes,
        AnyPersistent,
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

wait_worker_decision(
  TRef,
  DBs,
  Coordinator,
  LocalCommits,
  LocalPersistent,
  Seq,
  AllParticipatingDBsNodes,
  AnyPersistent
) ->
  receive
    {commit2, Workers}->
      ecall:send(Coordinator, {commit2, confirmed, self()}),
      wait_after_commit2(Coordinator, Workers, DBs, TRef, LocalPersistent, Seq);
    {rollback, Workers}->
      ok = broadcast_workers(Workers, {rollbacked, self()}),
      ok = maybe_write_rollbacked(TRef, AllParticipatingDBsNodes, AnyPersistent),
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
        AllParticipatingDBsNodes,
        AnyPersistent
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
  AllParticipatingDBsNodes,
  AnyPersistent
) ->
  ok = ensure_pg_scope(),
  Group = worker_group(TRef),
  ok = pg:join(Group, self()),
  {PgRef, _Members} = pg:monitor(Group),
  [
    erlang:monitor_node(Node, true)
    || Node <- all_participating_nodes(AllParticipatingDBsNodes),
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
    AllParticipatingDBsNodes,
    AnyPersistent
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
  AllParticipatingDBsNodes,
  AnyPersistent
) ->
  case rollback_consensus_reached(Group, AllParticipatingDBsNodes, DownNodes) of
    true ->
      ok = broadcast_group_decision(Group, {rollbacked, self()}),
      ok = maybe_write_rollbacked(TRef, AllParticipatingDBsNodes, AnyPersistent),
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
        {rollbacked, _From}->
          ok = broadcast_group_decision(Group, {rollbacked, self()}),
          ok = maybe_write_rollbacked(TRef, AllParticipatingDBsNodes, AnyPersistent),
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
            AllParticipatingDBsNodes,
            AnyPersistent
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
            AllParticipatingDBsNodes,
            AnyPersistent
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
            AllParticipatingDBsNodes,
            AnyPersistent
          )
      after ?WORKER_RESOLUTION_POLL_MS ->
        case any_silent_node_rollbacked(TRef, Group, AllParticipatingDBsNodes, DownNodes) of
          true ->
            ok = broadcast_group_decision(Group, {rollbacked, self()}),
            ok = maybe_write_rollbacked(TRef, AllParticipatingDBsNodes, AnyPersistent),
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
              AllParticipatingDBsNodes,
              AnyPersistent
            )
        end
      end
  end.

prepare_local_commits(DBs) ->
  [
    prepare_local_commit(DB, Ops)
    || {DB, Ops} <- lists:sort(maps:to_list(DBs))
  ].

prepare_local_commit(DB, {Write, Delete}) ->
  {Ref, Module} = ?dbRefMod(DB),
  {RollbackWrite, RollbackDelete} = Module:prepare_rollback(Ref, Write, Delete),
  #local_commit{
    db = DB,
    module = Module,
    ref = Ref,
    write = Write,
    delete = Delete,
    rollback_write = RollbackWrite,
    rollback_delete = RollbackDelete,
    persistent = Module:is_persistent()
  }.

persistent_commits(LocalCommits) ->
  [Commit || Commit = #local_commit{persistent = true} <- LocalCommits].

apply_local_commits(LocalCommits) ->
  try
    [apply_local_commit(Commit) || Commit <- LocalCommits],
    ok
  catch
    Class:Reason:Stack->
      {error, Class, Reason, Stack}
  end.

apply_local_commit(#local_commit{
  module = Module,
  ref = Ref,
  write = Write,
  delete = Delete
}) ->
  Module:commit(Ref, Write, Delete).

rollback_local_commits(LocalCommits) ->
  [rollback_local_commit(Commit) || Commit <- LocalCommits],
  ok.

rollback_local_commit(#local_commit{
  db = DB,
  module = Module,
  ref = Ref,
  rollback_write = RollbackWrite,
  rollback_delete = RollbackDelete
}) ->
  try Module:commit(Ref, RollbackWrite, RollbackDelete)
  catch
    _:E->
      ?LOGERROR("~p rollback error: ~p",[DB, E])
  end.

spawn_workers(Nodes, NsDBs, Data, TRef, AllParticipatingDBsNodes, AnyPersistent, Coordinator) ->
  maps:from_list([
    begin
      WorkerDBs = maps:with(maps:get(Node, NsDBs), Data),
      Worker =
        spawn(
          Node,
          ?MODULE,
          commit_request,
          [#worker_request{
             tref = TRef,
             dbs = WorkerDBs,
             all_participating_dbs_nodes = AllParticipatingDBsNodes,
             any_persistent = AnyPersistent,
             coordinator = Coordinator
           }]
        ),
      erlang:monitor(process, Worker),
      {Worker, Node}
    end
    || Node <- Nodes
  ]).

broadcast_workers(Workers, Message) ->
  [ecall:send(Worker, Message) || Worker <- Workers, Worker =/= self()],
  ok.

maybe_write_single_node_log(_TRef, []) ->
  undefined;
maybe_write_single_node_log(TRef, PersistentCommits) ->
  Seq = zaya_transaction_log:seq(),
  ok =
    maybe_log_batch(
      rollback_entries(PersistentCommits, Seq, TRef) ++
      [{#rollbacked{tref = TRef},
        [{Commit#local_commit.db, [node()]} || Commit <- PersistentCommits]}],
      []
    ),
  Seq.

maybe_delete_single_node_log(_TRef, _PersistentCommits, undefined) ->
  ok;
maybe_delete_single_node_log(TRef, PersistentCommits, Seq) ->
  maybe_log_batch(
    [],
    rollback_keys(PersistentCommits, Seq, TRef) ++ [#rollbacked{tref = TRef}]
  ).

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

maybe_write_rollbacked(_TRef, _AllParticipatingDBsNodes, false) ->
  ok;
maybe_write_rollbacked(TRef, AllParticipatingDBsNodes, true) ->
  maybe_log_batch([{#rollbacked{tref = TRef}, AllParticipatingDBsNodes}], []).

maybe_write_coordinator_rollbacked(_TRef, _AllParticipatingDBsNodes, false, _Nodes) ->
  ok;
maybe_write_coordinator_rollbacked(TRef, AllParticipatingDBsNodes, true, Nodes) ->
  case lists:member(node(), Nodes) of
    true ->
      ok;
    false ->
      maybe_log_batch([{#rollbacked{tref = TRef}, AllParticipatingDBsNodes}], [])
  end.

maybe_log_worker_failure(TRef, AllParticipatingDBsNodes, AnyPersistent, LocalPersistent, Seq) ->
  Writes =
    case AnyPersistent of
      true -> [{#rollbacked{tref = TRef}, AllParticipatingDBsNodes}];
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

rollback_entries(LocalCommits, Seq, TRef) ->
  [
    {#rollback{db = Commit#local_commit.db, seq = Seq, tref = TRef},
     {Commit#local_commit.rollback_write, Commit#local_commit.rollback_delete}}
    || Commit <- LocalCommits
  ].

rollback_keys(LocalCommits, Seq, TRef) ->
  [
    #rollback{db = Commit#local_commit.db, seq = Seq, tref = TRef}
    || Commit <- LocalCommits
  ].

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

all_participating_nodes(AllParticipatingDBsNodes) ->
  ordsets:from_list(
    lists:append([Nodes || {_DB, Nodes} <- AllParticipatingDBsNodes])
  ).

rollback_consensus_reached(Group, AllParticipatingDBsNodes, DownNodes) ->
  MemberNodes = ordsets:from_list([node(Pid) || Pid <- pg:get_members(Group)]),
  lists:all(
    fun(Node) ->
      sets:is_element(Node, DownNodes) orelse lists:member(Node, MemberNodes)
    end,
    all_participating_nodes(AllParticipatingDBsNodes)
  ).

any_silent_node_rollbacked(TRef, Group, AllParticipatingDBsNodes, DownNodes) ->
  MemberNodes = ordsets:from_list([node(Pid) || Pid <- pg:get_members(Group)]),
  SilentNodes =
    [
      Node
      || Node <- all_participating_nodes(AllParticipatingDBsNodes),
         not sets:is_element(Node, DownNodes),
         not lists:member(Node, MemberNodes)
    ],
  lists:any(
    fun(Node) ->
      case ecall:call(Node, zaya_transaction_log, is_rollbacked, [TRef]) of
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


