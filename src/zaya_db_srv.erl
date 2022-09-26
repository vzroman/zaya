
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
  recover/1,
  force_load/1,
  close/1
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

recover( DB )->
  case supervisor:start_child(zaya_db_sup,[DB, recover]) of
    {ok, PID} when is_pid( PID )->
      ok;
    Error->
      Error
  end.

force_load( DB )->
  gen_statem:cast(DB, force_load).

add_copy( DB, Params )->
  case supervisor:start_child(zaya_db_sup,[DB, {add_copy,Params}]) of
    {ok, PID} when is_pid( PID )->
      ok;
    Error->
      Error
  end.

close( DB )->
  case whereis( DB ) of
    PID when is_pid( PID )->
      supervisor:terminate_child(zaya_db_sup, PID);
    _->
      {error, not_registered}
  end.

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
init([DB, Action])->

  process_flag(trap_exit,true),

  ?LOGINFO("~p starting database server ~p",[DB ,self()]),

  Module = ?dbModule( DB ),

  {ok, init, #data{db = DB, module = Module }, [ {state_timeout, 0, Action } ]}.

handle_event(state_timeout, open, init, #data{db = DB, module = Module} = Data) ->
  Params = ?dbNodeParams(DB,node()),
  try
    Ref = Module:open( Params ),
    ?LOGINFO("~p database open",[DB]),
    {next_state, register, Data#data{ ref = Ref }, [ {state_timeout, 0, register } ] }
  catch
    _:E->
      ?LOGERROR("~p open error ~p",[DB,E]),
      { keep_state_and_data, [ {state_timeout, 5000, open } ] }
  end;

handle_event(state_timeout, {add_copy, Params}, init, #data{db = DB, module = Module} = Data) ->
  try
    Ref = zaya_copy:copy( DB, Module, Params, #{ live => true}),
    {next_state, register_copy, Data#data{ ref = Ref }, [ {state_timeout, 0, {register_copy,Params} } ] }
  catch
    _:E->
      ?LOGERROR("~p copy error ~p",[DB,E]),
      { keep_state_and_data, [ {state_timeout, 5000, {add_copy, Params} } ] }
  end;

handle_event(state_timeout, recover, init, Data ) ->
  {next_state, recovery, Data, [ {state_timeout, 0, recover } ] };

handle_event(state_timeout, {register_copy,Params}, register_copy, #data{db = DB} = Data) ->
  case ecall:call_all(?readyNodes, zaya_schema_srv, add_db_copy, [DB, node(), Params] ) of
    {ok,_}->
      ?LOGINFO("~p copy registered",[DB]),
      {next_state, register, Data, [ {state_timeout, 0, register } ] };
    {error,Error}->
      ?LOGERROR("~p register copy error ~p",[ DB, Error ]),
      { keep_state_and_data, [ {state_timeout, 5000, {register_copy,Params} } ] }
  end;

handle_event(state_timeout, register, register, #data{db = DB, ref = Ref} = Data) ->

  {OKs, Errs} = ecall:call_all_wait( ?readyNodes, zaya_schema_srv, open_db, [ DB, node(), Ref ] ),
  ?LOGINFO("~p registered at ~p nodes, errors at ~p nodes",[ DB, [N || {N,_} <- OKs], [N || {N,_} <- Errs] ]),

  {next_state, ready, Data};

handle_event(state_timeout, recover, recovery, #data{db = DB, module = Module}=Data ) ->
  case ?dbAvailableNodes(DB) of
    []->
      ?LOGERROR("~p database recover error: database is unavailable.\r\n"++
        "If you sure that the local copy is the latest you can try to load it with:\r\n"++
        "  zaya:force_load_db_copy(~p, ~p).\r\n" ++
        "Execute this command from erlang console at any attached node.\r\n"++
        "WARNING!!! All the data changes made in other nodes copies will be LOST!",[DB,DB,node()]),
      { keep_state_and_data, [ {state_timeout, 5000, recover } ] };
    _->
      % TODO. Hash tree
      Params = ?dbNodeParams(DB,node()),
      try
        Module:remove( Params ),
        {next_state, init, Data, [ {state_timeout, 0, {add_copy, Params} } ] }
      catch
        _:E->
          ?LOGERROR("~p remove copy error ~p",[DB,E]),
          { keep_state_and_data, [ {state_timeout, 5000, recover } ] }
      end
  end;
handle_event(cast, force_load, recovery, #data{db = DB} = Data ) ->
  ?LOGWARNING("~p FORCE LOAD. ALL THE DATA CHANGES MADE IN OTHER NODES COPIES WILL BE LOST!",[DB]),
  {next_state, init, Data, [ {state_timeout, 0, open } ] };

handle_event(cast, recover, ready, #data{db = DB, module = Module, ref = Ref}=Data) ->
  ?LOGWARNING("~p recovery",[DB]),

  ecall:call_all_wait(?readyNodes, zaya_schema_srv, close_db, [DB, node()]),
  Module:close(Ref),

  {next_state, recovery, Data, [ {state_timeout, 0, recover } ]};

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












