
-module(zaya_node_srv).

-include("zaya.hrl").
-include("zaya_schema.hrl").

-behaviour(gen_server).

-define(CYCLE, 1000).

%%=================================================================
%%	OTP API
%%=================================================================
-export([
  start_link/0,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

%%=================================================================
%%	OTP
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE},?MODULE, [], []).

-record(state,{}).
init([])->

  process_flag(trap_exit,true),

  % Monitor nodes state
  net_kernel:monitor_nodes(true, [nodedown_reason]),

  ?LOGINFO("start node server ~p",[self()]),

  timer:send_after(0, loop),

  {ok,#state{ }}.

handle_call(Request, From, State) ->
  ?LOGWARNING("node server got an unexpected call resquest ~p from ~p",[Request,From]),
  {noreply,State}.

handle_cast(Request,State)->
  ?LOGWARNING("node server got an unexpected cast resquest ~p",[Request]),
  {noreply,State}.

handle_info(loop, State)->

  timer:send_after(?CYCLE, loop),

  [ net_adm:ping(N) || N <- ?allNodes ],

  {noreply,State};

handle_info({nodeup, Node, InfoList}, State)->

  zaya_schema_srv:node_up( Node, InfoList ),

  {noreply,State};

handle_info({nodedown, Node, InfoList}, State)->

  zaya_schema_srv:node_down( Node, InfoList ),

  {noreply,State};

handle_info(Message,State)->
  ?LOGWARNING("node server got an unexpected message ~p",[Message]),
  {noreply,State}.

terminate(Reason,_State)->
  ?LOGWARNING("terminating node server reason ~p",[Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.













