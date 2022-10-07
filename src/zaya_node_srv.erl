
-module(zaya_node_srv).

-include("zaya.hrl").
-include("zaya_schema.hrl").

-behaviour(gen_server).

-define(CYCLE, 1000).

%%=================================================================
%%	API
%%=================================================================
-export([
  handshake/2,
  heartbeat/2
]).

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

handshake(Node, PID)->
  case whereis(?MODULE) of
    LocalPID->
      gen_server:cast(?MODULE, {handshake, Node, PID}),
      LocalPID;
    _->
      throw( not_ready )
  end.

heartbeat(Node, PID)->
  gen_server:cast(?MODULE, {heartbeat, Node, PID}).

%%=================================================================
%%	OTP
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE},?MODULE, [], []).

-record(state,{ nodes }).
init([])->

  process_flag(trap_exit,true),

  ?LOGINFO("start node server ~p",[self()]),

  zaya_schema_srv:node_up(node(), init),
  {Replies,_} = ecall:call_all_wait( ?allNodes--[node()], ?MODULE, handshake, [node(),self()] ),
  Nodes =
    [begin
       erlang:monitor(process, PID),
       {PID,Node}
     end || {Node,PID} <- Replies],

  timer:send_after(?CYCLE, heartbeat),

  {ok,#state{ nodes = maps:from_list(Nodes)}}.

handle_call(Request, From, State) ->
  ?LOGWARNING("node server got an unexpected call resquest ~p from ~p",[Request,From]),
  {noreply,State}.

handle_cast({handshake, Node, PID},#state{ nodes = Nodes }=State)->

  erlang:monitor(process, PID),
  gen_server:cast({?MODULE, Node}, {handshake, node(), self()}),

  zaya_schema_srv:node_up(Node, init),

  {noreply,State#state{ nodes = Nodes#{ PID => Node }}};

handle_cast({heartbeat, Node, PID},#state{ nodes = Nodes }=State)->

  NewNodes =
    case Nodes of
      #{ PID:=_ }->
        Nodes;
      _->
        zaya_schema_srv:split_brain( Node ),
        erlang:monitor(process, PID),
        Nodes#{ PID => Node }
    end,

  {noreply,State#state{ nodes = NewNodes }};

handle_cast(Request,State)->
  ?LOGWARNING("node server got an unexpected cast resquest ~p",[Request]),
  {noreply,State}.

handle_info(heartbeat, State)->

  timer:send_after(?CYCLE, heartbeat),

  ecall:cast_all( ?allNodes--[node()], ?MODULE, heartbeat, [node(),self()] ),

  {noreply,State};

handle_info({'DOWN', _Ref, process, PID, Reason}, #state{nodes = Nodes} = State)->

  NewNodes =
    case maps:take( PID, Nodes ) of
      {Node, RestNodes}->
        zaya_schema_srv:node_down( Node, Reason ),
        RestNodes;
      _->
        Nodes
    end,

  {noreply,State#state{ nodes = NewNodes }};

handle_info(Message,State)->
  ?LOGWARNING("node server got an unexpected message ~p",[Message]),
  {noreply,State}.

terminate(Reason,_State)->
  ?LOGWARNING("terminating node server reason ~p",[Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.













