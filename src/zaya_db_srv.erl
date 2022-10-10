
-module(zaya_db_srv).

-include("zaya.hrl").
-include("zaya_schema.hrl").

-behaviour(gen_statem).

%%=================================================================
%%	API
%%=================================================================
-export([
  open/1,
  add_copy/2,
  force_load/1,
  close/1,
  split_brain/1
]).
%%=================================================================
%%	OTP API
%%=================================================================
-export([
  callback_mode/0,
  start_link/2,
  init/1,
  code_change/3,
  handle_event/4,
  terminate/3
]).

%%=================================================================
%%	API
%%=================================================================
open( DB )->
  case supervisor:start_child(zaya_db_sup,[DB, open]) of
    {ok, _PID}->
      ok;
    Error->
      Error
  end.

force_load( DB )->
  gen_statem:cast(DB, force_load).

add_copy( DB, InParams )->
  Params = zaya_db:params(DB,InParams),
  case supervisor:start_child(zaya_db_sup,[DB, {add_copy,Params}]) of
    {ok, PID} when is_pid( PID )->
      ok;
    Error->
      Error
  end.

close( DB )->
  case whereis( DB ) of
    PID when is_pid( PID )->
      gen_statem:cast(DB, close);
    _->
      {error, not_registered}
  end.

split_brain(DB)->
  spawn(fun()->merge_brain( DB ) end),
  ok.

%%=================================================================
%%	OTP
%%=================================================================
callback_mode() ->
  [
    handle_event_function
  ].

start_link( DB, Action )->
  gen_statem:start_link({local,DB},?MODULE, [DB, Action], []).

-record(data,{db, module, params, ref}).
init([DB, State])->

  process_flag(trap_exit,true),

  ?LOGINFO("~p starting database server ~p, state ~p",[DB ,self(),State]),

  Module = ?dbModule( DB ),

  {ok, State, #data{db = DB, module = Module }, [ {state_timeout, 0, run } ]}.

%%---------------------------------------------------------------------------
%%   OPEN
%%---------------------------------------------------------------------------
handle_event(state_timeout, run, open, #data{db = DB, module = Module} = Data) ->

  update_masters( DB ),

  case update_nodes( DB ) of
    recover ->
      {next_state, recover, Data, [ {state_timeout, 0, run } ] };
    ok->
      Params = ?dbNodeParams(DB,node()),
      try
        Ref = Module:open( Params ),
        ?LOGINFO("~p database open",[DB]),
        {next_state, rollback_transactions, Data#data{ ref = Ref }, [ {state_timeout, 0, run } ] }
      catch
        _:E->
          ?LOGERROR("~p open error ~p",[DB,E]),
          { keep_state_and_data, [ {state_timeout, 5000, run } ] }
      end
  end;

handle_event(state_timeout, run, rollback_transactions, #data{ ref = Ref, db = DB, module = Module } = Data) ->

  zaya_transaction:rollback_log(Module, Ref, DB),

  {next_state, register, Data, [ {state_timeout, 0, run } ] };

handle_event(state_timeout, run, register, #data{db = DB, ref = Ref} = Data) ->

  case elock:lock( ?locks, DB, _IsShared=false, 5000, ?dbAvailableNodes(DB)) of
    {ok, Unlock}->
      try
        {OKs, Errs} = ecall:call_all_wait( ?readyNodes, zaya_schema_srv, open_db, [ DB, node(), Ref ] ),
        ?LOGINFO("~p registered at ~p nodes, errors at ~p nodes",[ DB, [N || {N,_} <- OKs], [N || {N,_} <- Errs] ]),

        {next_state, ready, Data}
      after
        Unlock()
      end;
    {error,LockError}->
      ?LOGINFO("~p register lock error ~p, retry",[DB, LockError]),
      { keep_state_and_data, [ {state_timeout, 0, run } ] }
  end;

%%---------------------------------------------------------------------------
%%   ADD COPY
%%---------------------------------------------------------------------------
handle_event(state_timeout, run, {add_copy, Params}, #data{db = DB, module = Module} = Data) ->
  try
    Ref = zaya_copy:copy( DB, Module, Params, #{ live => true}),
    {next_state, {register_copy,Params}, Data#data{ ref = Ref }, [ {state_timeout, 0, run } ] }
  catch
    _:E->
      ?LOGERROR("~p copy error ~p",[DB,E]),
      { keep_state_and_data, [ {state_timeout, 5000, run } ] }
  end;

handle_event(state_timeout, run, {register_copy,Params}, #data{db = DB} = Data) ->
  case ecall:call_all(?readyNodes, zaya_schema_srv, add_db_copy, [DB, node(), Params] ) of
    {ok,_}->
      ?LOGINFO("~p copy registered",[DB]),
      {next_state, register, Data, [ {state_timeout, 0, run } ] };
    {error,Error}->
      ?LOGERROR("~p register copy error ~p",[ DB, Error ]),
      { keep_state_and_data, [ {state_timeout, 5000, run } ] }
  end;

%%---------------------------------------------------------------------------
%%   RECOVER
%%---------------------------------------------------------------------------
handle_event(state_timeout, run, recover, #data{db = DB}=Data ) ->
  case ?dbAvailableNodes(DB) of
    []->
      case os:getenv("FORCE_START") of
        "true"->
          ?LOGWARNING("~p force open",[DB]),
          {next_state, open, Data, [ {state_timeout, 0, run } ] };
        _->
          ?LOGERROR("~p database recover error: database is unavailable.\r\n"++
            "If you sure that the local copy is the latest you can try to load it with:\r\n"++
            "  zaya:db_force_open(~p, ~p).\r\n" ++
            "Execute this command from erlang console at any attached node.\r\n"++
            "WARNING!!! All the data changes made in other nodes copies will be LOST!",[DB,DB,node()]),
          { keep_state_and_data, [ {state_timeout, 5000, run } ] }
      end;
    _->
      {next_state, recovery, Data, [ {state_timeout, 0, run } ] }
  end;
handle_event(cast, force_load, recover, #data{db = DB} = Data ) ->
  ?LOGWARNING("~p FORCE LOAD. ALL THE DATA CHANGES MADE IN OTHER NODES COPIES WILL BE LOST!",[DB]),
  {next_state, open, Data, [ {state_timeout, 0, run } ] };

%%---------------------------------------------------------------------------
%%   DB IS READY
%%---------------------------------------------------------------------------
handle_event(cast, recover, ready, Data) ->
  {next_state, unregister, Data, [ {state_timeout, 0, recovery } ]};

handle_event(cast, close, ready, #data{db = DB}=Data) ->

  ?LOGINFO("~p close",[DB]),
  {next_state, unregister, Data, [ {state_timeout, 0, close } ]};

%%---------------------------------------------------------------------------
%%   RECOVERY
%%---------------------------------------------------------------------------
handle_event(state_timeout, run, recovery, #data{db = DB, module = Module, ref = Ref}=Data ) ->

  case ?dbAvailableNodes(DB) of
    []->
      ?LOGERROR("~p recovery is impossible: no other copies are available"),
      { keep_state_and_data, [ {state_timeout, 5000, run } ] };
    _->
      ?LOGWARNING("~p recovery",[DB]),
      % TODO. Hash tree
      Params = ?dbNodeParams(DB,node()),
      try
        if
          Ref =/=?undefined -> Module:close( Ref );
          true->ignore
        end,
        Module:remove( Params ),
        zaya_transaction:drop_log( DB ),
        {next_state, {add_copy, Params}, Data, [ {state_timeout, 0, run } ] }
      catch
        _:E:S->
          ?LOGERROR("~p recovery error ~p, stack ~p",[DB,E,S]),
          { keep_state_and_data, [ {state_timeout, 5000, run } ] }
      end
  end;
handle_event(cast, force_load, recovery, #data{db = DB} = Data ) ->
  ?LOGWARNING("~p FORCE LOAD. ALL THE DATA CHANGES MADE IN OTHER NODES COPIES WILL BE LOST!",[DB]),
  {next_state, register, Data, [ {state_timeout, 0, run } ] };

%%---------------------------------------------------------------------------
%%   CLOSE
%%---------------------------------------------------------------------------
handle_event(state_timeout, NextState, unregister, #data{db = DB} = Data) ->

  case elock:lock( ?locks, DB, _IsShared=false, 30000, ?dbAvailableNodes(DB)) of
    {ok, Unlock}->
      try
        ecall:call_all_wait(?readyNodes, zaya_schema_srv, close_db, [DB, node()]),
        {next_state, NextState, Data, [ {state_timeout, 0, run } ]}
      after
        Unlock()
      end;
    {error,LockError}->
      ?LOGINFO("~p unregister lock error ~p, retry",[DB, LockError]),
      { keep_state_and_data, [ {state_timeout, 5000, NextState } ] }
  end;

handle_event(state_timeout, run, close, #data{db = DB, module = Module, ref = Ref}) ->
  try  Module:close(Ref)
  catch
    _:E->?LOGERROR("~p close error ~p",[DB,E])
  end,
  {stop, shutdown};

handle_event(EventType, EventContent, _AnyState, #data{db = DB}) ->
  ?LOGWARNING("~p database server received unexpected event type ~p, content ~p",[
    DB,
    EventType, EventContent
  ]),
  keep_state_and_data.

terminate(Reason, _AnyState, #data{ db = DB, module = Module, ref = Ref })->

  ?LOGWARNING("~p terminating database server reason ~p",[DB,Reason]),
  ecall:call_all_wait(?readyNodes, zaya_schema_srv, close_db, [DB, node()]),

  if
    Ref =/= ?undefined->
      catch Module:close( Ref );
    true->
      ignore
  end,

  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%------------------------------------------------------------------------------------
%%  SPLIT BRAIN:
%%
%%------------------------------------------------------------------------------------
merge_brain( DB )->

  update_masters( DB ),

  case update_nodes( DB ) of
    ok->
      % No need to recover local copy
      ok;
    recover->
      case whereis( DB ) of
        PID when is_pid( PID )->
          gen_statem:cast(DB, recover);
        _->
          db_is_not_opened
      end
  end.

update_masters( DB )->
  case update_masters( ?dbMasters(DB), DB ) of
    error->
      update_masters( ordsets:from_list(?allNodes), DB );
    ok->
      ok
  end.

update_masters([N|_Rest], _DB ) when N=:=node()->
  ok;
update_masters([N|Rest], DB )->
  case rpc:call( N, zaya_db, masters, [DB] ) of
    Masters when is_list(Masters)->
      zaya_schema_srv:set_db_masters( DB, Masters ),
      ok;
    _->
      update_masters( Rest, DB )
  end;
update_masters( [], _DB )->
  error.

update_nodes( DB )->
  case update_nodes( ?dbMasters(DB), DB ) of
    error ->
      update_nodes( ordsets:from_list(?dbAllNodes(DB)), DB );
    OkOrRecover->
      OkOrRecover
  end.

update_nodes( [N|_Rest], _DB ) when N=:=node()->
  ok;
update_nodes( [N|Rest], DB )->
  case rpc:call( N, zaya_db, available_nodes, [DB] ) of
    Nodes  when is_list(Nodes)->
      case {ordsets:from_list( Nodes ), ordsets:from_list(?dbAvailableNodes(DB))} of
        {Same,Same}->
          ok;
        _->
          zaya_schema_srv:set_db_nodes( DB, Nodes ),
          recover
      end;
    _->
      update_nodes( Rest, DB )
  end;
update_nodes([], _DB )->
  error.

