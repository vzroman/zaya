
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
-define(transaction,{?MODULE,'$transaction$'}).
-record(transaction,{data,locks,parent}).

-define(LOCK_TIMEOUT, 60000).
-define(ATTEMPTS,5).

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
  rollback/1
]).

%%=================================================================
%%	Environment
%%=================================================================
create_log( Path )->
  ?logModule:create( ?logParams#{ dir=> Path } ),
  open_log( Path ).

open_log( Path )->
  persistent_term:put(?log, ?logModule:open( ?logParams#{ dir=> Path } )),
  ok.

%%-----------------------------------------------------------
%%  READ
%%-----------------------------------------------------------
read(DB, Keys, Lock)->
  #{DB:=DBData} = in_context(DB, Keys, Lock, fun(Data)->
    do_read(DB, Keys, Data)
  end),
  data_keys(Keys, DBData).

do_read(DB, Keys, Data)->
  ToRead =
    [K || K <- Keys, not maps:is_key(K,Data)],
  Values =
    maps:from_list( zaya_db:read( DB, ToRead ) ),
  lists:foldl(fun(K,Acc)->
    Value =
      case Values of
        #{K:=V}-> {V,V};
        _->?undefined
      end,
    Acc#{K => Value}
  end, Data, ToRead).

data_keys([K|Rest], Data)->
  case Data of
    #{ K:= {_,V} }->
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
      do_write(Rest, Data#{K=>{{V}, V}})
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
  do_delete(Rest, Data#{K=>delete});
do_delete([], Data)->
  Data.

%%-----------------------------------------------------------
%%  TRANSACTION
%%-----------------------------------------------------------
transaction(Fun)->
  case get(?transaction) of
    #transaction{locks = PLocks}=Parent->
      try { ok, Fun() }
      catch
        _:{lock,Error}->
          throw({lock,Error});
        _:Error->
          #transaction{locks = Locks} = get(?transaction),
          release_locks(Locks, PLocks),
          put(?transaction, Parent),
          {error,Error}
      end;
    _->
      run_transaction( Fun, ?ATTEMPTS )
  end.

rollback( Data )->
  todo.

run_transaction(Fun, Attempts) when Attempts>0->
  put(?transaction,#transaction{ data = #{}, locks = #{} }),
  try
    Result = Fun(),
    #transaction{data = Data} = get(?transaction),
    commit( Data ),
    Result
  catch
    _:{lock,_} when Attempts>1->
      ?LOGDEBUG("lock error ~p"),
      run_transaction(Fun, Attempts-1 );
    _:Error->
      {abort,Error}
  after
    #transaction{locks = Locks} = erase( ?transaction ),
    release_locks( Locks, #{} )
  end.

in_context(DB, Keys, Lock, Fun)->
  case get(?transaction) of
    #transaction{data = Data,locks = Locks}=T->
      DBLocks = lock(DB, Keys, Lock, maps:get(DB,Locks) ),
      DBData = Fun( maps:get(DB,Data,#{}) ),
      put( ?transaction, T#transaction{
        data = Data#{ DB => DBData },
        locks = Locks#{ DB => DBLocks }
      }),
      ok;
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
  Locks.

do_lock([K|Rest], DB, Nodes, Type, Locks)->
  Unlock =
    case Locks of
      #{K := #{Type:=_Unlock}}->
        _Unlock;
      _->
        lock_key( {?MODULE,DB,K}, _IsShared = Type=:=read, ?LOCK_TIMEOUT, Nodes )
    end,

  KeyLocks=
    if
      Type =:= write->
        #{ read => Unlock, write => Unlock };
      true->
        #{ read => Unlock }
    end,

  do_lock( Rest, DB, Nodes, Type, Locks#{ K => KeyLocks } );

do_lock([], _DB, _Nodes, _Type, Locks )->
  Locks.

lock_key( Key, IsShared, Timeout, Nodes )->
  case elock:lock( ?locks, Key, IsShared, Timeout, Nodes) of
    {ok, Unlock}->
      Unlock;
    {error,Error}->
      throw({lock,Error})
  end.

release_locks( Locks, Parent )->
  maps:fold(fun(DB,Keys,_)->
    PDBs = maps:get(DB,Parent,#{}),
    maps:fold(fun(K,Types,_)->
      PTypes = maps:get(K,PDBs),
      maps:fold(fun(Type,Unlock,_)->
        case maps:is_key(Type,PTypes) of
          true->ignore;
          _->Unlock()
        end
      end,?undefined,Types)
    end,?undefined,Keys)
  end,?undefined,Locks).

%%-----------------------------------------------------------
%%  COMMIT
%%-----------------------------------------------------------
commit( Data )->
  todo.