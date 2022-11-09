
-module(zaya_transaction).

-include("zaya.hrl").
-include("zaya_schema.hrl").

-define(logModule, zaya_rocksdb).

-define(logParams,
  #{
    rocksdb => #{
      %compression_algorithm => todo,
      open_options=>#{
        total_threads => 1,
        create_if_missing => false,
        %create_missing_column_families => todo,
        %error_if_exists => false,
        paranoid_checks => false,
        % compression => todo,
        % max_open_files => todo,
        % max_total_wal_size => todo,
        % use_fsync => todo,
        % db_paths => todo,
        % db_log_dir => todo,
        % wal_dir => todo,
        % delete_obsolete_files_period_micros => todo,
        max_background_jobs => 1,
        % max_background_compactions => todo,
        % max_background_flushes => todo,
        % max_log_file_size => todo,
        % log_file_time_to_roll => todo,
        % keep_log_file_num => todo,
        % max_manifest_file_size => todo,
        % table_cache_numshardbits => todo,
        % wal_ttl_seconds => todo,
        % manual_wal_flush => todo,
        % wal_size_limit_mb => todo,
        % manifest_preallocation_size => todo,
        % allow_mmap_reads => todo,
        % allow_mmap_writes => todo,
        % is_fd_close_on_exec => todo,
        % skip_log_error_on_recovery => todo,
        % stats_dump_period_sec => todo,
        % advise_random_on_open => todo,
        % access_hint => todo,
        %compaction_readahead_size => 2 * 1024 * 1024,
        % new_table_reader_for_compaction_inputs => todo,
        % use_adaptive_mutex => todo,
        % bytes_per_sync => todo,
        % skip_stats_update_on_db_open => todo,
        % wal_recovery_mode => todo,
        %allow_concurrent_memtable_write => true,
        % enable_write_thread_adaptive_yield => todo,
        db_write_buffer_size => 512 * 1024 * 1024,
        % in_memory => todo,
        % rate_limiter => todo,
        % sst_file_manager => todo,
        % write_buffer_manager => todo,
        % max_subcompactions => todo,
        % atomic_flush => todo,
        % use_direct_reads => todo,
        % use_direct_io_for_flush_and_compaction => todo,
        enable_pipelined_write => true,
        unordered_write => true
        %two_write_queues => true
        % statistics => todo
      },
      read => #{
        % read_tier => todo,
        verify_checksums => false,
        fill_cache => false
        % iterate_upper_bound => todo,
        % iterate_lower_bound => todo,
        % tailing => todo,
        % total_order_seek => todo,
        % prefix_same_as_start => todo,
        % snapshot => todo
      },
      write => #{
        sync => false
        % disable_wal => true,
        % ignore_missing_column_families => todo,
        % no_slowdown => todo,
        % low_pri => todo
      }
    }
  }
).

-define(log,{?MODULE,'$log$'}).
-define(logRef,persistent_term:get(?log)).

-define(transaction,{?MODULE,'$transaction$'}).
-record(transaction,{data,locks}).

-define(index,{?MODULE,index}).
-define(t_id, atomics:add_get(persistent_term:get(?index), 1, 1)).
-define(initIndex,persistent_term:get({?MODULE,init_index})).

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
  on_abort/2,
  changes/2,

  transaction/1
]).

%%=================================================================
%%  Database server API
%%=================================================================
-export([
  rollback_log/3,
  drop_log/1
]).

%%=================================================================
%%	Internal API
%%=================================================================
-export([
  single_node_commit/1,
  commit_request/2
]).

%%TODO. Remove me
-export([
  debug/0
]).

%%=================================================================
%%	Environment
%%=================================================================
create_log( Path )->
  Ref = ?logModule:create( ?logParams#{ dir=> Path } ),
  on_init( Ref ).

open_log( Path )->
  Ref = ?logModule:open( ?logParams#{ dir=> Path } ),
  on_init( Ref ).

on_init( Ref )->
  persistent_term:put(?log, Ref),
  persistent_term:put(?index, atomics:new(1,[{signed, false}])),
  try
    {InitLogId,_} = ?logModule:last(?logRef),
    atomics:put(persistent_term:get(?index),1,InitLogId),
    persistent_term:put({?MODULE,init_index}, InitLogId)
  catch
    _:_->
      % no entries
      persistent_term:put({?MODULE,init_index}, 0)
  end,
  ok.

%%-----------------------------------------------------------
%%  READ
%%-----------------------------------------------------------
read(DB, Keys, Lock) when Lock=:=read; Lock=:=write->
  DBData = in_context(DB, Keys, Lock, fun(Data)->
    do_read(DB, Keys, Data)
  end),
  data_keys(Keys, DBData);

read(DB, Keys, Lock = none)->
  %--------Dirty read (no lock)---------------------------
  % If the key is already in the transaction data we return it's transaction value.
  % Otherwise we don't add any new keys in the transaction data but read them in dirty mode
  TData = in_context(DB, Keys, Lock, fun(Data)->
    % We don't add anything in transaction data if the keys are not locked
    Data
  end),

  Data =
    case [K || K <- Keys, not maps:is_key(K, TData)] of
      []-> TData;
      DirtyKeys->
        % Add dirty keys
        lists:foldl(fun({K,V},Acc)->
          Acc#{K => {V,V}}
        end, TData, read_keys(DB,DirtyKeys))
    end,
  data_keys(Keys, Data).

do_read(DB, Keys, Data)->
  case [K || K <- Keys, not maps:is_key(K, Data)] of
    [] ->
      Data;
    ToRead->
      Values =
        maps:from_list( read_keys(DB, ToRead ) ),
      lists:foldl(fun(K,Acc)->
        case Values of
          #{K:=V}->
            Acc#{ K => {V,V} };
          _->
            Acc#{ K=> {?none,?none} }
        end
      end, Data, ToRead)
  end.

read_keys( DB, Keys )->
  case ?dbRef( DB, node() ) of
    ?undefined ->
      zaya_db:read( DB, Keys );
    _->
      % Check if the DB is in the recovery state
      AvailableNodes = ?dbAvailableNodes(DB),
      case lists:member(node(),AvailableNodes) of
        true ->
          % The local copy is ready
          zaya_db:read( DB, Keys );
        _->
          % The local copy is in the recovery state read from valid copies
          case ecall:call_one(AvailableNodes, zaya_db, read,[ DB, Keys ]) of
            {ok,{_,Result}}-> Result;
            _->[]
          end
      end
  end.

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
  ensure_editable(DB),
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
  ensure_editable(DB),
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
  end;
do_delete([], Data)->
  Data.

%%-----------------------------------------------------------
%%  ROLLBACK VALUES
%%-----------------------------------------------------------
on_abort(DB, KVs)->
  in_context(DB, [K || {K,_}<-KVs], _Lock = none, fun(Data)->
    do_on_abort( KVs, Data )
  end),
  ok.

do_on_abort([{K,V}|Rest], Data )->
  case Data of
    #{K := {{?none},V1}}->
      V0 =
        if
          V=:=delete-> ?none;
          true-> V
        end,
      do_on_abort(Rest, Data#{K => {V0,V1} });
    _->
      do_on_abort(Rest, Data)
  end;
do_on_abort([], Data )->
  Data.

changes(DB, Keys)->
  case get(?transaction) of
    #transaction{data = Data}->
      case Data of
        #{DB := DBChanges}->
          lists:foldl(fun(K, Acc)->
            case DBChanges of
              #{K:= {V,V} }->
                Acc;
              #{K:={{?none},_V1}}->
                V1 =
                  if
                    _V1 =:= ?none-> delete;
                    true->_V1
                  end,
                Acc#{ K=> {V1} };
              #{K:={_V0,_V1}}->
                V0 =
                  if
                    _V0 =:= ?none-> delete;
                    true->_V0
                  end,
                V1 =
                  if
                    _V1 =:= ?none-> delete;
                    true->_V1
                  end,
                Acc#{K => {V0,V1}};
              _->
                Acc
            end
          end,#{},Keys);
        _->
          #{}
      end;
    _ ->
      throw(no_transaction)
  end.

ensure_editable( DB )->
  case ?dbMasters(DB) of
    []-> ok;
    Nodes->
      case Nodes -- (?dbAvailableNodes(DB) -- Nodes) of
        []-> throw({unavailable, DB});
        _-> ok
      end
  end.

%%-----------------------------------------------------------
%%  TRANSACTION
%%-----------------------------------------------------------
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
  put(?transaction,#transaction{ data = #{}, locks = #{} }),
  try
    Result = Fun(),

    % The transaction is ok, try to commit the changes.
    % The locks are held until committed or aborted
    #transaction{data = Data} = get(?transaction),
    commit( Data ),

    % Committed
    {ok,Result}
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
      DBLocks = lock(DB, Keys, Lock, maps:get(DB,Locks,#{}) ),

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
commit( Data0 )->

  case prepare_data( Data0 ) of
    Data when map_size(Data) > 0->
      case prepare_commit( Data ) of
        #commit{ns = [Node]}->
          % Single node commit
          case rpc:call(Node,?MODULE, single_node_commit, [ Data ]) of
            {badrpc, Reason}->
              throw( Reason );
            _->
              ok
          end;
        Commit->
          multi_node_commit(Data, Commit)
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
          [K || {K,{{?none},_}} <- maps:to_list( ToCommit )],
        Values =
          if
            length(ToReadKeys) >0->
              maps:from_list( zaya_db:read(DB, ToReadKeys) );
            true->
              #{}
          end,

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
    ordsets:from_list( lists:append([ Ns || {_DB,Ns} <- DBsNs ])),

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

    wait_confirm(maps:keys(DBsNs), DBsNs, NsDBs, Workers, _Ns = [] )
%-----------phase 2------------------------------------------
    % Every participant is waiting {'DOWN', _, process, Master, normal}
    % It's accepted as the phase2 confirmation
  end),

  receive
    {'DOWN', Ref, process, _, normal}->
      ok;
    {'DOWN', Ref, process, _, Error}->
      throw( Error )
  end.

wait_confirm([], _DBsNs, _NsDBs, _Workers, _Ns )->
  ?LOGDEBUG("transaction confirmed");
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

          wait_confirm(DBs, DBsNs1, maps:remove(N,NsDBs),  RestWorkers, Ns );
        _->
          % Who was it?
          wait_confirm(DBs, DBsNs, NsDBs, Workers, Ns )
      end
  end.

commit_request( Master, DBs )->

  erlang:monitor(process, Master),

%-----------phase 1------------------------------------------
  LogID = commit1( DBs ),
  Master ! { confirm, self() },

  receive
    {'DOWN', _Ref, process, Master, normal} ->
%-----------phase 2------------------------------------------
      commit2( LogID ),
      on_commit(DBs);
%-----------rollback------------------------------------------
    {'DOWN', _Ref, process, Master, Reason} ->
      ?LOGDEBUG("rollback commit master down ~p",[Reason]),
      commit_rollback( LogID, DBs )
  end.

commit1( DBs )->
  LogID = ?t_id,
  ok = ?logModule:write( ?logRef, [{LogID,DBs}] ),
  try
    dump_dbs( DBs ),
    LogID
  catch
    _:Error->
      commit_rollback( LogID, DBs ),
      throw(Error)
  end.

commit2( LogID )->
  try ok = ?logModule:delete( ?logRef,[ LogID ])
  catch
    _:E->
      ?LOGERROR("commit log ~p delete error ~p",[ LogID, E ])
  end.

on_commit(DBs)->
  maps:fold(fun(DB,{{Write,Delete},_Rollback},_)->
    if
      length(Write) > 0-> catch zaya_db:on_update( DB, write, Write );
      true->ignore
    end,
    if
      length(Delete) > 0-> catch zaya_db:on_update( DB, delete, Delete );
      true->ignore
    end
  end,?undefined,DBs).

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
  try ok = ?logModule:delete( ?logRef,[ LogID ])
  catch
    _:E->?LOGWARNING("rollback log ~p delete error ~p, data: ~p",[ LogID, E, DBs ])
  end.

rollback_dbs( DBs )->
  maps:fold(fun(DB,{_Commit,{Write,Delete}},_)->
    Module = ?dbModule(DB),
    Ref = ?dbRef(DB,node()),
    if
      length(Write)> 0 ->
        try ok = Module:write( Ref, Write )
        catch
          _:WE->
            ?LOGERROR("unable to rollback write commit: database ~p, error ~p, data ~p",[DB, WE, Write ])
        end;
      true-> ignore
    end,
    if
      length(Delete) > 0->
        try ok =  Module:delete( Ref, Delete )
        catch
          _:DE->
            ?LOGERROR("unable to rollback delete commit: database ~p, error ~p, data ~p",[DB, DE, Delete ])
        end;
      true-> ignore
    end
  end, ?undefined, DBs).

%%-----------------------------------------------------------
%%  Database server
%%-----------------------------------------------------------
rollback_log(Module, Ref, DB)->
  fold_log(DB, ?initIndex, fun({Commit,{Write,Delete}})->
    if
      length(Write) >0->
        try ok = Module:write(Ref,Write)
        catch
          _:WE->
            ?LOGERROR("~p database rollback write error: ~p\r\ncommit: ~p\r\nrollback write: ~p",[DB,WE,Commit,Write])
        end;
      true->ignore
    end,
    if
      length(Delete) >0->
        try ok = Module:delete(Ref,Delete)
        catch
          _:DE->
            ?LOGERROR("~p database rollback delete error: ~p\r\ncommit: ~p\r\nrollback delete: ~p",[DB,DE,Commit,Delete])
        end;
      true->ignore
    end
  end).

drop_log( DB )->
  fold_log(DB, ?t_id, fun(_)-> ignore end).


fold_log( DB, Stop, Fun )->
  LogRef = ?logRef,
  ?logModule:foldl(LogRef,#{stop => Stop},fun({LogID,DBs},_)->
    {ok,Unlock} = elock:lock(?locks,{?MODULE,log,LogID},_IsShared=false,?infinity),
    try
      case maps:take(DB,DBs) of
        {Data, Rest} ->
          Fun( Data ),
          if
            map_size(Rest) > 0 ->
              ok = ?logModule:write(LogRef,[{LogID,Rest}]);
            true->
              ok = ?logModule:delete(LogRef,[LogID])
          end;
        _->
          ignore
      end
    catch
      _:E->
        ?LOGERROR("~p database drop transaction log ~p error ~p",[DB,LogID,E])
    after
      Unlock()
    end
  end,?undefined).

debug()->
  zaya_transaction:transaction(fun()->

    zaya_transaction:write(test,[{x1,y1}],none),

    zaya_transaction:read(test,[x1],read),

    zaya_transaction:write(test,[{x1,y11},{x2,y2}],write)

  end).


