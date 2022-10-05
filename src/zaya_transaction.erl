
-module(zaya_transaction).

-include("zaya.hrl").
-include("zaya_schema.hrl").

-define(logModule, zaya_leveldb).

-define(logParams,
  #{
    eleveldb => #{
      %compression_algorithm => todo,
      open_options=>#{
        create_if_missing => false,
        error_if_exists => false,
        %write_buffer_size => todo
        %sst_block_size => todo,
        %block_restart_interval = todo,
        %block_size_steps => todo,
        paranoid_checks => false,
        verify_compactions => false,
        compression => false
        %use_bloomfilter => todo,
        %total_memory => todo,
        %total_leveldb_mem => todo,
        %total_leveldb_mem_percent => todo,
        %is_internal_db => todo,
        %limited_developer_mem => todo,
        %eleveldb_threads => TODO pos_integer()
        %fadvise_willneed => TODO boolean()
        %block_cache_threshold => TODO pos_integer()
        %delete_threshold => pos_integer()
        %tiered_slow_level => pos_integer()
        %tiered_fast_prefix => TODO string()
        %tiered_slow_prefix => TODO string()
        %cache_object_warming => TODO
        %expiry_enabled => TODO boolean()
        %expiry_minutes => TODO pos_integer()
        %whole_file_expiry => boolean()
      },
      read => #{
        verify_checksums => false
        %fill_cache => todo,
        %iterator_refresh =todo
      },
      write => #{
        sync => true
      }
    }
  }
).

-define(log,{?MODULE,'$log$'}).
-define(logRef,persistent_term:get(?log)).

-define(transaction,{?MODULE,'$transaction$'}).
-record(transaction,{data,locks,parent}).

-define(index,{?MODULE,index}).
-define(t_id, atomics:add_get(persistent_term:get(?index), 1, 1)).

-define(LOCK_TIMEOUT, 60000).
-define(ATTEMPTS,5).

-define(none,{?MODULE,?undefined}).

%%=================================================================
%%	Environment API
%%=================================================================
-export([
  create_log/1,
  open_log/1
]).

%%=================================================================
%%	API
%%=================================================================
-export([
  read/3,
  write/3,
  delete/3,

  transaction/1,
  rollback_log/2,
  drop_log/1
]).

%%=================================================================
%%	Internal API
%%=================================================================
-export([
  single_node_commit/1,
  commit_request/2
]).

%%=================================================================
%%	Environment
%%=================================================================
create_log( Path )->
  ?logModule:create( ?logParams#{ dir=> Path } ),
  open_log( Path ).

open_log( Path )->
  persistent_term:put(?log, ?logModule:open( ?logParams#{ dir=> Path } )),
  persistent_term:put(?index, atomics:new(1,[{signed, false}])),
  ok.

%%-----------------------------------------------------------
%%  READ
%%-----------------------------------------------------------
read(DB, Keys, Lock)->
  DBData = in_context(DB, Keys, Lock, fun(Data)->
    do_read(DB, Keys, Data)
  end),
  data_keys(Keys, DBData).

do_read(DB, Keys, Data)->
  ToRead =
    [K || K <- Keys, maps:is_key(K, Data)],
  Values =
    maps:from_list( zaya_db:read( DB, ToRead ) ),
  lists:foldl(fun(K,Acc)->
    case Values of
      #{K:=V}->
        Acc#{ K => {V,V} };
      _->
        Acc#{ K=> {?none,?none} }
    end
  end, Data, ToRead).

data_keys([K|Rest], Data)->
  case Data of
    #{ K:= {_,V} } when V=/=?none->
      [{K,V} | data_keys(Rest,Data)];
    _->
      data_keys(Rest, Data)
  end;
data_keys([],_Data)->
  [].

%%-----------------------------------------------------------
%%  WRITE
%%-----------------------------------------------------------
write(DB, KVs, Lock)->
  in_context(DB, [K || {K,_}<-KVs], Lock, fun(Data)->
    do_write( KVs, Data )
  end),
  ok.

do_write([{K,V}|Rest], Data )->
  case Data of
    #{K := {V0,_}}->
      do_write(Rest, Data#{K => {V0,V} });
    _->
      do_write(Rest, Data#{K=>{{?none}, V}})
  end;
do_write([], Data )->
  Data.

%%-----------------------------------------------------------
%%  DELETE
%%-----------------------------------------------------------
delete(DB, Keys, Lock)->
  in_context(DB, Keys, Lock, fun(Data)->
    do_delete(Keys, Data)
  end),
  ok.

do_delete([K|Rest], Data)->
  case Data of
    #{K := {V0,_}}->
      do_delete( Rest, Data#{ K=> {V0,?none} });
    _->
      do_delete( Rest, Data#{ K => {{?none}, ?none}})
  end,
  do_delete(Rest, Data#{K=>delete});
do_delete([], Data)->
  Data.

%%-----------------------------------------------------------
%%  TRANSACTION
%%-----------------------------------------------------------
transaction(Fun)->
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
          {error,Error}
      end;
    _->
      % The transaction entry point
      run_transaction( Fun, ?ATTEMPTS )
  end.

rollback_log(Ref, DB)->
  todo.

drop_log( DB )->
  todo.

run_transaction(Fun, Attempts) when Attempts>0->
  % Create the transaction storage
  put(?transaction,#transaction{ data = #{}, locks = #{} }),
  try
    Result = Fun(),

    % The transaction is ok, try to commit the changes.
    % The locks are held until committed or aborted
    #transaction{data = Data} = get(?transaction),
    commit( Data ),

    % Committed
    Result
  catch
    _:{lock,_} when Attempts > 1->
      % In the case of lock errors all held locks are released and the transaction starts from scratch
      ?LOGDEBUG("lock error ~p"),
      run_transaction(Fun, Attempts-1 );
    _:Error->
      % Other errors lead to transaction abort
      {abort,Error}
  after
    % Always release locks
    #transaction{locks = Locks} = erase( ?transaction ),
    release_locks( Locks, #{} )
  end.

in_context(DB, Keys, Lock, Fun)->
  % read / write / delete actions transaction context
  case get(?transaction) of
    #transaction{data = Data,locks = Locks}=T->

      % Obtain locks on all keys before the action starts
      DBLocks = lock(DB, Keys, Lock, maps:get(DB,Locks) ),

      % Perform the action
      DBData = Fun( maps:get(DB,Data,#{}) ),

      % Update the transaction data
      put( ?transaction, T#transaction{
        data = Data#{ DB => DBData },
        locks = Locks#{ DB => DBLocks }
      }),

      % Return new database context
      DBData;
    _ ->
      throw(no_transaction)
  end.

%%-----------------------------------------------------------
%%  LOCKS
%%-----------------------------------------------------------
lock(DB, Keys, Type, Locks) when Type=:=read; Type=:=write->
  case maps:is_key({?MODULE,DB}, Locks) of
    true->
      do_lock( Keys, DB, ?dbAvailableNodes(DB), Type, Locks );
    _->
      lock( DB, Keys, Type, Locks#{
        {?MODULE,DB} => lock_key( DB, _IsShared=true, _Timeout=?infinity, ?dbAvailableNodes(DB) )
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
      #{K := {HeldType,_}} = Lock when Type=:= read; HeldType =:= write ->
        Lock;
      _->
        {Type, lock_key( {?MODULE,DB,K}, _IsShared = Type=:=read, ?LOCK_TIMEOUT, Nodes )}
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
commit( Data0 )->

  case prepare_data( Data0 ) of
    Data when map_size(Data) > 0->
      Commit = prepare_commit( Data ),
      case prepare_commit( Data ) of
        #commit{ns = [Node],ns_dbs = NsDBs} = Commit->
          % Single node commit
          case rpc:call(Node,?MODULE, single_node_commit, [maps:get(Node,NsDBs)]) of
            {badrpc, Reason}->
              throw( Reason );
            _->
              ok
          end;
        Commit->
          multi_node_commit( Commit )
      end;
    _->
      % Nothing to commit
      ok
  end.

prepare_data( Data )->

  % Data has structure:
  % #{
  %   DB1 => #{
  %     Key1 => {PreviousValue, NewValue},
  %     ...
  %   }
  %   .....
  % }
  % If the PreviousValue is {?none} then it is not known yet
  % If the PreviousValue is ?none the Key doesn't exists in the DB yet
  % If the New is ?none then the Key has to be deleted.

  % The result has the form:
  % #{
  %   DB1 => { Commit, Rollback },
  %   ...
  % }
  % The Commit and the Rollback has the same form: {ToWrite, ToDelete }
  % ToWrite is [{Key1,Value1},{Key2,Value2}|...]
  % ToDelete is [K1,K2|...]

  maps:fold(fun(DB, Changes, Acc)->

    case maps:filter(fun(_K,{V0,V1})-> V0=/=V1 end, Changes) of
      ToCommit when map_size( ToCommit ) > 0->

        % Read the keys that are not read yet to be able to rollback them
        ToReadKeys =
          [K || {K,{?none},_} <- maps:to_list( ToCommit )],
        Values =
          maps:from_list( zaya_db:read(DB, ToReadKeys) ),

        % Prepare the DB's Commit and Rollback
        CommitRollback =
          maps:fold(fun(K,{V0,V1},{{CW,CD},{RW,RD}})->
            % Commits
            Commits=
              if
                V1 =:= ?none->
                  {CW, [K|CD]}; % The Key has to be deleted
                true->
                  {[{K,V1}|CW], CD} % The Key has to be written with a V1 value
              end,

            % Rollbacks
            ActualV0 =
              if
                V0=:={?none}-> % The Key doesn't exist in the transaction data, check it in the read results
                  case Values of
                    #{K:=V}->V; % The key has the value
                    _-> ?none   % The Key does not exist
                  end;
                true-> % The Key is in the transaction data already, accept it's value
                  V0
              end,

            Rollbacks =
              if
                ActualV0 =:= ?none->
                  {RW,[K|RD]}; % The Key doesn't exist, it has to be deleted in the case of rollback
                true->
                  {[{K,ActualV0}|RW],RD} % The Key has the value, it has to be written back in the case of rollback
              end,

            {Commits, Rollbacks}

          end,{{[],[]}, {[],[]}},ToCommit),

        Acc#{DB => CommitRollback};
      _->
        % DB has no changes to commit
        Acc
    end

  end, #{}, Data).

prepare_commit( Data )->

  % Databases that has changes to be committed
  DBs = ordsets:from_list( maps:keys(Data) ),

  % The nodes hosting the database
  DBsNs =
    [ {DB,
        case ?dbAvailableNodes(DB) of
          [] -> throw({unavailable, DB});
          Nodes -> Nodes
        end
      } || DB <- maps:keys(Data) ],

  % All participating nodes
  Ns =
    ordsets:from_list( lists:append([ Ns || {_DB,Ns} <- DBs ])),

  % Databases that each node has to commit
  NsDBs =
    [ {N, ordsets:intersection( DBs, ordsets:from_list(?nodeDBs(N)) )} || N <- Ns ],

  #commit{
    ns=Ns,
    ns_dbs = maps:from_list(NsDBs),
    dbs_ns = maps:from_list(DBsNs)
  }.

%%-----------------------------------------------------------
%%  SINGLE NODE COMMIT
%%-----------------------------------------------------------
single_node_commit( DBs )->
  LogID = commit1( DBs ),
  try
    commit2( LogID ),
    spawn(fun()-> on_commit( DBs ) end)
  catch
    _:E->
      ?LOGDEBUG("phase 2 single node commit error ~p",[E]),
      commit_rollback(LogID, DBs),
      throw(E)
  end.

%%-----------------------------------------------------------
%%  MULTI NODE COMMIT
%%-----------------------------------------------------------
multi_node_commit(#commit{ns = Nodes, ns_dbs = NsDBs, dbs_ns = DBsNs} = Commit1)->

  Self = self(),

  Master = spawn(fun()->

    MasterSelf = self(),

%-----------phase 1------------------------------------------
    Workers =
      [ {N, spawn_opt(N, ?MODULE, commit_request, [MasterSelf, maps:get(N,NsDBs)],[ monitor ])} || N <- Nodes],

    Workers1 =
      maps:from_list([{W,N} || {N,{W,_}} <- Workers]),

    try
      {Workers2,Commit2} = wait_phase(commit1, maps:keys(DBsNs), Workers1, Commit1 ),
      Workers2List = maps:to_list( Workers2 ),
%-----------phase 2------------------------------------------
      [ catch W ! {commit, MasterSelf} || {W,_N} <- Workers2List ],

      wait_phase(commit2,maps:keys(DBsNs), Workers2, Commit2 ),

      catch Self ! {committed, MasterSelf},
%-----------confirm------------------------------------------
      [ catch W ! {committed, MasterSelf} || {W,_N} <- Workers2List ]
    catch
      _:Error->
%-----------rollback------------------------------------------
        [ catch W ! {rollback, MasterSelf } || {W,_N} <- Workers1],
        catch Self ! {abort, MasterSelf, Error}
    end

  end),

  receive
    {committed, Master}->
      ok;
    {abort, Master, Error}->
      throw( Error )
  end.

wait_phase(Phase, [], Workers, Commit )->
  ?LOGDEBUG("~p phase ready",[Phase]),
  {Workers, Commit};
wait_phase(Phase, DBs, Workers, #commit{ns = Ns, ns_dbs = NsDBs, dbs_ns = DBsNs} = Commit )->
  receive
    {Phase, W}->
      case Workers of
        #{W := N}->
          % At least one node is ready to commit it's DBs. It's enough for these DBs to move to the phase2
          ReadyDBs = maps:get(N,NsDBs),
          wait_phase(Phase, DBs -- ReadyDBs, Workers, Commit );
        _->
          % Who was it?
          wait_phase(Phase,DBs, Workers, Commit )
      end;
    {'DOWN', _Ref, process, W, Reason}->
      % One node down
      case maps:take(W, Workers ) of
        {N, RestWorkers}->

          % Rest nodes
          Ns1 = Ns -- [N],
          NodeDBs = maps:get(N, NsDBs),
          ?LOGDEBUG("~p phase commit node ~p worker down ~p",[Phase,N,Reason,NodeDBs]),

          % Remove the node from the databases active nodes
          DBsNs1 =
            maps:map(fun(DB,DBNodes)->
              case DBNodes -- [N] of
                []->
                  ?LOGDEBUG("~p phase commit database ~p is unavailable, abort transaction",[Phase,DB]),
                  throw({unavailable, DB});
                RestNodes->
                  RestNodes
              end
            end, DBsNs),

          % Remove the node from actors
          NsDBs1 = maps:remove(N,NsDBs),

          wait_phase(Phase, DBs, RestWorkers, Commit#commit{ ns = Ns1, ns_dbs = NsDBs1, dbs_ns = DBsNs1 } );
        _->
          % Who was it?
          wait_phase(Phase, DBs, Workers, Commit )
      end
  end.

commit_request( Master, DBs )->

  erlang:monitor(process, Master),

%-----------phase 1------------------------------------------
  LogID = commit1( DBs ),
  Master ! { commit1, self() },

  receive
    {commit, Master}->
%-----------phase 2------------------------------------------
      commit2( LogID ),
      Master ! {commit2, self()},
%-----------confirm------------------------------------------
      receive
        {committed,Master}->
          on_commit(DBs);
        {rollback, Master}->
          commit_rollback( LogID, DBs );
        {'DOWN', _Ref, process, Master, Reason}->
          ?LOGDEBUG("not confirmed commit master down ~p",[Reason]),
          on_commit(DBs)
      end;
%-----------rollback------------------------------------------
    {rollback, Master}->
      commit_rollback( LogID, DBs );
    {'DOWN', _Ref, process, Master, Reason} ->
      ?LOGDEBUG("rollback commit master down ~p",[Reason]),
      commit_rollback( LogID, DBs )
  end.

commit1( DBs )->
  LogID = ?t_id,
  ok = ?logModule:write( ?logRef, [{LogID,DBs}] ),
  try dump_dbs( DBs )
  catch
    _:Error->
      commit_rollback( LogID, DBs ),
      throw(Error)
  end.

commit2( LogID )->
  try ?logModule:delete( ?logRef,[ LogID ])
  catch
    _:E->
      ?LOGERROR("commit log ~p delete error ~p",[ LogID, E ])
  end.

on_commit(DBs)->
  [ begin
      if
        length(Write) > 0-> catch zaya_db:on_update( DB, write, Write );
        true->ignore
      end,
      if
        length(Delete) > 0-> catch zaya_db:on_update( DB, delete, Delete );
        true->ignore
      end
    end || {DB,{{Write,Delete},_Rollback}} <- DBs].

dump_dbs( DBs )->
  maps:fold(fun(DB,{{Write,Delete},_Rollback},_)->
    Module = ?dbModule(DB),
    Ref = ?dbRef(DB,node()),
    if
      length(Write)> 0 -> ok = Module:write( Ref, Write );
      true-> ignore
    end,
    if
      length(Delete) > 0-> ok = Module:delete( Ref, Delete );
      true-> ignore
    end
  end, ?undefined, DBs).



commit_rollback( LogID, DBs )->
  rollback_dbs( DBs ),
  try ?logModule:delete( ?logRef,[ LogID ])
  catch
    _:E->
      ?LOGERROR("rollback commit log ~p error ~p",[ LogID, E ])
  end.

rollback_dbs( DBs )->
  maps:fold(fun(DB,{_Commit,{Write,Delete}},_)->
    Module = ?dbModule(DB),
    Ref = ?dbRef(DB,node()),
    if
      length(Write)> 0 ->
        try Module:write( Ref, Write )
        catch
          _:WE->
            ?LOGERROR("unable to rollback write commit: database ~p, error ~p, data ~p",[DB, WE, Write ])
        end;
      true-> ignore
    end,
    if
      length(Delete) > 0->
        try Module:delete( Ref, Delete )
        catch
          _:DE->
            ?LOGERROR("unable to rollback delete commit: database ~p, error ~p, data ~p",[DB, DE, Delete ])
        end;
      true-> ignore
    end
  end, ?undefined, DBs).




