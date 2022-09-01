
-module(zaya_node_srv).

-include("zaya.hrl").
-include("zaya_schema.hrl").

%%=================================================================
%%	TRANSFORMATION API
%%=================================================================
-export([
  remove/1
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  all_nodes/0,

  is_node_ready/1,
  is_node_not_ready/1,
  node_status/1,

  ready_nodes/0,
  not_ready_nodes/0,

  node_dbs/1,
  ready_nodes_dbs/0,
  not_ready_nodes_dbs/0,
  all_nodes_dbs/0,

  node_db_params/2,

  ready_nodes_db_params/1,
  not_ready_nodes_db_params/1,

  node_dbs_params/1,
  all_nodes_dbs_params/0

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

%%=================================================================
%%	TRANSFORMATION
%%=================================================================
remove( Node )->
  case ?isNodeReady( Node ) of
    true ->
      throw(node_is_still_ready);
    _->
      ok
  end,

  case [DB || DB <-?nodeDBs( Node ), ?isDBNotReady( DB ) ] of
    []->
      ok;
    NotReadyDBs->
      throw({not_ready_dbs,NotReadyDBs})
  end,

  ecall:cast_all(?readyNodes, gen_server, cast,[?MODULE,{remove_node, Node}] ),
  ok.

%%=================================================================
%%	INFO
%%=================================================================
all_nodes()->
  ?allNodes.
%----------------------------

is_node_ready( Node )->
  ?isNodeReady( Node ).

is_node_not_ready( Node )->
  ?isNodeNotReady( Node ).

node_status( Node )->
  ?nodeStatus( Node ).
%-------------------------------

ready_nodes()->
  ?readyNodes.

not_ready_nodes()->
  ?notReadyNodes.
%-------------------------------

node_dbs( Node )->
  ?nodeDBs( Node ).

ready_nodes_dbs()->
  ?readyNodesDBs.

not_ready_nodes_dbs()->
  ?notReadyNodesDBs.

all_nodes_dbs()->
  ?allNodesDBs.
%------------------------------
node_db_params( Node, db )->
  ?dbNodeParams(db, Node).

ready_nodes_db_params(db)->
  ?readyNodesDBParams(db).

not_ready_nodes_db_params(db)->
  ?notReadyNodesDBParams(db).

%--------------------------------

node_dbs_params( Node )->
  ?nodeDBsParams( Node ).

all_nodes_dbs_params()->
  ?allNodesDBsParams.

%%=================================================================
%%	OTP
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE},?MODULE, [], []).

-record(state,{}).

init([])->

  process_flag(trap_exit,true),

  ?LOGINFO("starting node server ~p",[self()]),

  {ok,#state{}}.

handle_call(Request, From, State) ->
  ?LOGWARNING("node server got an unexpected call resquest ~p from ~p",[Request,From]),
  {noreply,State}.

handle_cast({remove_node, Node} ,State)->

  zaya_schema_srv:remove_node( Node ),

  {noreply,State};

handle_cast(Request,State)->
  ?LOGWARNING("node server got an unexpected cast resquest ~p",[Request]),
  {noreply,State}.

handle_info(Message,State)->
  ?LOGWARNING("node server got an unexpected message ~p",[Message]),
  {noreply,State}.

terminate(Reason,_State)->
  ?LOGWARNING("terminating node server reason ~p",[Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


