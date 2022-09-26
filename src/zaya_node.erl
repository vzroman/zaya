
-module(zaya_node).

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
%%	TRANSFORMATION
%%=================================================================
remove( Node )->
  case ?isNodeReady( Node ) of
    true ->
      throw(node_is_still_ready);
    _->
      ok
  end,

  case [DB || DB <-?nodeDBs( Node ), ?isDBNotAvailable( DB ) ] of
    []->
      ok;
    NotAvailableDBs->
      throw({not_available_dbs,NotAvailableDBs})
  end,

  ecall:call_all_wait(?readyNodes, zaya_schema_srv, remove_node, [Node] ).

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



