
-module(zaya_db_srv).

-include("zaya.hrl").
-include("zaya_schema.hrl").

-behaviour(gen_server).

%%=================================================================
%%	API
%%=================================================================
-export([
  open/1,
  close/1
]).
%%=================================================================
%%	OTP API
%%=================================================================
-export([
  start_link/1,
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
  case supervisor:start_child(zaya_db_sup,[ DB ]) of
    {ok, PID} when is_pid( PID )->
      ok;
    {ok, PID, _Info} when is_pid(PID)->
      ok;
    {ok, undefined}->
      error;
    {ok, undefined, _Info}->
      error;
    {error,{already_started,PID}}->
      ok;
    {error,already_present}->
      ok;
    {error,Error}->
      ?LOGERROR("~p unable to start database server, error ~p",[DB,Error]),
      error
  end.

close( DB )->
  case whereis( DB ) of
    PID when is_pid( PID )->
      supervisor:terminate_child(zaya_db_sup,PID);
    _->
      {error, not_registered}
  end.

%%=================================================================
%%	OTP
%%=================================================================
start_link( DB )->
  gen_server:start_link({local,?MODULE},?MODULE, [ DB ], []).

-record(state,{db, module, params, ref, register}).
init([ DB ])->

  process_flag(trap_exit,true),

  ?LOGINFO("~p starting database server ~p",[DB ,self()]),

  Module = ?dbModule( DB ),
  Params = ?dbNodeParams(DB,node()),

  Ref = Module:open( Params ),

  register(DB, self()),

  timer:send_after(1, register ),

  {ok,#state{db = DB, module = Module, params = Params, ref = Ref, register = ?allNodes }}.

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












