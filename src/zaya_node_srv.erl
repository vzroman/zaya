
-module(zaya_node_srv).

-include("zaya.hrl").
-include("zaya_schema.hrl").

-behaviour(gen_server).

-define(CYCLE, 1000).

%%=================================================================
%%	API
%%=================================================================
-export([
  handshake/3,
  heartbeat/3
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

handshake(Node, PID, SchemaID)->
  case whereis(?MODULE) of
    LocalPID when is_pid(LocalPID)->
      gen_server:cast(?MODULE, {handshake, Node, PID, SchemaID}),
      LocalPID;
    _->
      throw( not_ready )
  end.

heartbeat(Node, PID, SchemaID)->
  gen_server:cast(?MODULE, {heartbeat, Node, PID, SchemaID}).

%%=================================================================
%%	OTP
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE},?MODULE, [], []).

-record(state,{ nodes, schema_id, diff_schema_nodes }).
init([])->

  process_flag(trap_exit,true),

  ?LOGINFO("start node server ~p",[self()]),

  SchemaID = ?schemaId,

  zaya_schema_srv:node_up(node(), init),
  {Replies,_} = ecall:call_all_wait( ?allNodes--[node()], ?MODULE, handshake, [node(), self(), SchemaID] ),
  Nodes =
    [begin
       erlang:monitor(process, PID),
       {PID,Node}
     end || {Node,PID} <- Replies],

  timer:send_after(?CYCLE, heartbeat),

  {ok,#state{
    nodes = maps:from_list(Nodes),
    schema_id = SchemaID,
    diff_schema_nodes = #{}
  }}.

handle_call(Request, From, State) ->
  ?LOGWARNING("node server got an unexpected call resquest ~p from ~p",[Request,From]),
  {noreply,State}.

%------------------------------------------------------------------
% handshaking
%------------------------------------------------------------------
handle_cast({handshake, Node, PID, SchemaID},#state{
  nodes = Nodes,
  schema_id = SchemaID,
  diff_schema_nodes = DiffSchemaNodes
}=State)->

  erlang:monitor(process, PID),

  zaya_schema_srv:node_up(Node, init),

  {noreply,State#state{
    nodes = Nodes#{ PID => Node },
    diff_schema_nodes = maps:remove( Node, DiffSchemaNodes )
  }};

handle_cast({handshake, Node, _PID, RemoteSchemaID},#state{ schema_id = SchemaID, diff_schema_nodes = DiffSchemaNodes }=State)->

  ?LOGWARNING("~p node started with a different schema ID, local: ~p, remote: ~p",[
    Node,
    SchemaID,
    RemoteSchemaID
  ]),

  ecall:cast(Node, ?MODULE, heartbeat, [node(), self(), SchemaID] ),

  {noreply,State#state{ diff_schema_nodes = DiffSchemaNodes#{ Node => true } }};

%---------------------------------------------------------------------
% heartbeat
%---------------------------------------------------------------------
handle_cast({heartbeat, Node, PID, SchemaID},#state{
  nodes = Nodes,
  schema_id = SchemaID,
  diff_schema_nodes = DiffSchemaNodes
}=State)->

  NewNodes =
    case Nodes of
      #{ PID:=_ }->
        Nodes;
      _->
        zaya_schema_srv:split_brain( Node ),
        erlang:monitor(process, PID),
        Nodes#{ PID => Node }
    end,

  {noreply,State#state{
    nodes = NewNodes,
    diff_schema_nodes = maps:remove( Node, DiffSchemaNodes )
  }};
handle_cast({heartbeat, Node, _PID, RemoteSchemaID},#state{
  schema_id = SchemaID,
  diff_schema_nodes = DiffSchemaNodes
}=State)->

  ?LOGWARNING("~p node is running with a different schema ID, local ~p, remote ~p",[
    Node,
    SchemaID,
    RemoteSchemaID
  ]),

  ecall:cast(Node, ?MODULE, heartbeat, [node(), self(), SchemaID] ),

  {noreply, State#state{ diff_schema_nodes = DiffSchemaNodes#{ Node=>true } }};

handle_cast(Request,State)->
  ?LOGWARNING("node server got an unexpected cast resquest ~p",[Request]),
  {noreply,State}.

handle_info(heartbeat, #state{ schema_id = SchemaID, diff_schema_nodes = DiffSchemaNodes } = State)->

  timer:send_after(?CYCLE, heartbeat),

  ecall:cast_all( ?allNodes--[node()|maps:keys( DiffSchemaNodes )], ?MODULE, heartbeat, [node(),self(), SchemaID] ),

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













