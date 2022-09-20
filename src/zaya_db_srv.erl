
-module(zaya_db_srv).

-include("zaya.hrl").
-include("zaya_schema.hrl").

-behaviour(gen_server).

%%=================================================================
%%	API
%%=================================================================
-export([
  open/1,
  close/1,

  add_copy/2,
  recover/1
]).
%%=================================================================
%%	OTP API
%%=================================================================
-export([
  start_link/2,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
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

close( DB )->
  case whereis( DB ) of
    PID when is_pid( PID )->
      supervisor:terminate_child(zaya_db_sup, PID);
    _->
      {error, not_registered}
  end.

add_copy( DB, Params )->
  case supervisor:start_child(zaya_db_sup,[DB, {add_copy,Params}]) of
    {ok, PID} when is_pid( PID )->
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

%%=================================================================
%%	OTP
%%=================================================================
start_link( DB, Action )->
  gen_server:start_link({local,?MODULE},?MODULE, [ DB, Action ], []).

-record(state,{db, module, params, ref, register}).
init([DB, Action])->

  process_flag(trap_exit,true),

  ?LOGINFO("~p starting database server ~p",[DB ,self()]),

  Module = ?dbModule( DB ),

  Ref = do_init(Action, DB, Module),

  register(DB, self()),

  timer:send_after(0, register ),

  {ok,#state{db = DB, module = Module, ref = Ref, register = ?readyNodes }}.

do_init(open, DB, Module)->
  Params = ?dbNodeParams(DB,node()),
  Module:open( Params );

do_init({add_copy, Params}, DB, Module)->
  Ref = zaya_copy:copy( DB, Module, Params, #{ live => true}),
  case zaya_schema_srv:add_db_copy( DB, node(),Params ) of
    ok->
      ecall:cast_all( ?readyNodes, zaya_schema_srv, add_db_copy, [DB, node(), Params] ),
      Ref;
    SchemaError->
      Module:close( Ref ),
      Module:remove( Params ),
      throw( SchemaError )
  end;

do_init(recover, DB, Module)->
  % TODO. Hash tree
  Params = ?dbNodeParams(DB,node()),
  Module:remove( Params ),
  do_init( {add_copy, Params}, DB, Module ).

handle_call(Request, From, #state{db = DB} = State) ->
  ?LOGWARNING("~p database server got an unexpected call resquest ~p from ~p",[DB, Request,From]),
  {noreply,State}.

handle_cast(Request,#state{db = DB} = State)->
  ?LOGWARNING("~p database server got an unexpected cast resquest ~p",[DB,Request]),
  {noreply,State}.

handle_info(register, #state{db = DB, ref = Ref, register = Nodes} = State)->

  case ecall:call_all_wait( Nodes, zaya_schema_srv, open_db, [ DB, node(), Ref ] ) of
    {_,[]}->
      ?LOGINFO("~p registered at ~p nodes",[ DB, Nodes ]),
      {noreply,State};
    {_, Errors}->
      ErrNodes = [N || {N,_} <- Errors],
      ?LOGWARNING("~p registered at ~p nodes, error nodes ~p",[ DB, Nodes -- ErrNodes, Errors ]),
      timer:send_after(5000, register ),
      {noreply,State#state{register = ErrNodes}}
  end;

handle_info(Message,#state{db = DB} =State)->
  ?LOGWARNING("~p database server got an unexpected message ~p",[DB,Message]),
  {noreply,State}.

terminate(Reason,#state{db = DB, module = Module, ref = Ref})->
  ?LOGWARNING("~p terminating database server reason ~p",[DB,Reason]),

  Module:close( Ref ),

  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.












