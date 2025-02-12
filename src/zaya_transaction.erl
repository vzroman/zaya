
-module(zaya_transaction).

-include("zaya.hrl").
-include("zaya_schema.hrl").

-define(transaction,{?MODULE,'$transaction$'}).
-record(transaction,{data, changes ,locks}).

-record(db_commit,{ db, module, ref, t_ref }).

-define(LOCK_TIMEOUT, 60000).
-define(ATTEMPTS,5).

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
  single_db_commit/1,
  single_node_commit/1,
  commit_request/2
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
  case ?dbMasters(DB) of
    []->
      ?dbAvailableNodes(DB);
    Masters->
      Nodes = ?dbAvailableNodes(DB),
      case Masters -- (Nodes -- Masters) of
        []-> throw({unavailable, DB});
        _-> Nodes
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
lock(DB, Keys, Type, Locks) when Type=:=read; Type=:=write->
  case maps:is_key({?MODULE,DB}, Locks) of
    true->
      do_lock( Keys, DB, ?dbAvailableNodes(DB), Type, Locks );
    _->
      lock( DB, Keys, Type, Locks#{
        {?MODULE,DB} => {read,lock_key( DB, _IsShared=true, _Timeout=?infinity, ?dbAvailableNodes(DB) )}
      })
  end;
lock(_DB, _Keys, none, Locks)->
  % The lock is not needed
  Locks.

do_lock(_Keys, DB, []= _Nodes, _Type, _Locks)->
  throw({unavailable,DB});
do_lock([K|Rest], DB, Nodes, Type, Locks)->
  KeyLock =
    case Locks of
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
    end,

  do_lock( Rest, DB, Nodes, Type, Locks#{ K => KeyLock } );

do_lock([], _DB, _Nodes, _Type, Locks )->
  Locks.

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
%%  SINGLE DB COMMIT
%%-----------------------------------------------------------
single_db_commit(Data, Ns)->
  case ecall:call_any( Ns, ?MODULE, ?FUNCTION_NAME, [ Data ]) of
    {ok,_}-> ok;
    {error, Error}-> throw( Error )
  end.

single_db_commit( Data )->
  [{DB, {Write, Delete}}] = maps:to_list( Data ),
  {Ref, Module} = ?dbRefMod( DB ),

  Module:commit( Ref, Write, Delete ),

  on_commit( Data ).

%%-----------------------------------------------------------
%%  SINGLE NODE COMMIT
%%-----------------------------------------------------------
single_node_commit( DBs, [N])->
  case ecall:call(N,?MODULE, ?FUNCTION_NAME, [ DBs ]) of
    {ok,_}-> ok;
    {error, Error}-> throw( Error )
  end.
single_node_commit( DBs )->
  Commits = commit1( DBs ),
  commit2( Commits ).

%%-----------------------------------------------------------
%%  MULTI NODE COMMIT
%%-----------------------------------------------------------
multi_node_commit(Data, #commit{ns = Nodes, ns_dbs = NsDBs, dbs_ns = DBsNs})->

  {_,Ref} = spawn_monitor(fun()->

    Master = self(),

%-----------phase 1------------------------------------------
    Workers =
      maps:from_list([ begin
        W = spawn(N, ?MODULE, commit_request, [Master, maps:with(maps:get(N,NsDBs), Data)]),
        erlang:monitor(process, W),
        {W,N}
      end || N <- Nodes]),

    RestWorkers = wait_confirm(maps:keys(DBsNs), DBsNs, NsDBs, Workers, _Ns = [] ),
%-----------phase 2------------------------------------------
    [ W ! {commit2, Master} || W <- maps:keys(RestWorkers) ],
    wait_commit2( Workers ),
    ok
  end),

  receive
    {'DOWN', Ref, process, _, normal}->
      ok;
    {'DOWN', Ref, process, _, Error}->
      throw( Error )
  end.

wait_confirm([], _DBsNs, _NsDBs, Workers, _Ns )->
  ?LOGDEBUG("transaction confirmed"),
  Workers;
wait_confirm(DBs, DBsNs, NsDBs, Workers, Ns )->
  receive
    {confirm, W}->
      case Workers of
        #{W := N}->
          Ns1 = [N|Ns],
          % The DB is ready when all participating ready nodes have confirmed
          ReadyDBs =
            [DB || DB <- maps:get(N,NsDBs), length(maps:get(DB,DBsNs) -- Ns1) =:= 0],

          wait_confirm(DBs -- ReadyDBs, DBsNs, NsDBs, Workers, Ns1 );
        _->
          % Who was it?
          wait_confirm(DBs, DBsNs, NsDBs, Workers, Ns )
      end;
    {'DOWN', _Ref, process, W, Reason}->
      % One node went down
      case maps:take(W, Workers ) of
        {N, RestWorkers}->

          ?LOGDEBUG("commit node ~p down ~p",[N,Reason]),

          % Remove the node from the databases active nodes
          DBsNs1 =
            maps:map(fun(DB,DBNodes)->
              case DBNodes -- [N] of
                []->
                  ?LOGDEBUG("~p database is unavailable, reason ~p, abort transaction",[DB,Reason]),
                  throw({unavailable, DB});
                RestNodes->
                  RestNodes
              end
            end, DBsNs),

          DBs1 =  [ DB || DB <- DBs, [] =/= (maps:get(DB, DBsNs1) -- Ns)],

          wait_confirm(DBs1, DBsNs1, maps:remove(N,NsDBs),  RestWorkers, Ns );
        _->
          % Who was it?
          wait_confirm(DBs, DBsNs, NsDBs, Workers, Ns )
      end
  end.

wait_commit2( Workers ) when map_size( Workers )>0 ->
  receive
    {'DOWN', _Ref, process, W, normal}->
      wait_commit2( maps:remove( W, Workers ) );
    {'DOWN', _Ref, process, W, Reason}->
      ?LOGWARNING("~p node commit2 error: ~p",[ node(W), Reason ]),
      wait_commit2( maps:remove( W, Workers ) )
  end;
wait_commit2( _Workers )->
  ok.

commit_request( Master, DBs )->

  erlang:monitor(process, Master),

%-----------phase 1------------------------------------------
  Commits = commit1( DBs ),

  Master ! { confirm, self() },

  receive
%-----------phase 2------------------------------------------
    {commit2, Master}->
      commit2( Commits ),
      on_commit(DBs);
%-----------rollback------------------------------------------
    {'DOWN', _Ref, process, Master, Reason} ->
      ?LOGDEBUG("rollback commit master down ~p",[Reason]),
      rollback( Commits )
  end.

db_commit( DB, Write, Delete )->
  {Ref, Module} = ?dbRefMod( DB ),
  #db_commit{
    db = DB,
    module = Module,
    ref = Ref,
    t_ref = Module:commit1( Ref, Write, Delete )
  }.

commit1( DBs )->
  maps:fold( fun(DB, {Write, Delete}, Acc)->
    Commit =
    try db_commit( DB, Write, Delete )
    catch
      _:E->
        ?LOGERROR("~p commit phase 1 error: ~p",[ DB, E ]),
        rollback( Acc ),
        throw( E )
    end,
    [Commit|Acc]
  end, [], DBs ).

commit2( Commits )->
  [ Module:commit2( Ref, TRef) ||#db_commit{ ref = Ref, module = Module, t_ref = TRef } <- Commits],
  ok.

rollback( Commits )->
  [ try Module:rollback( Ref, TRef )
    catch
      _:E  ->
        ?LOGERROR("~p rollback commit phase 1 error: ~p",[ DB, E ])
    end ||#db_commit{ db = DB, ref = Ref, module = Module, t_ref = TRef } <- Commits],
  ok.

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




