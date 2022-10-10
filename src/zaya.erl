
-module(zaya).

%%=================================================================
%%	Nodes Service API
%%=================================================================
-export([
  remove_node/1
]).

%%=================================================================
%%	Nodes Info API
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
%%	DBs Service API
%%=================================================================
-export([
  db_create/3,
  db_open/1, db_open/2,
  db_force_open/2,
  db_close/1, db_close/2,
  db_remove/1,

  db_add_copy/3,
  db_remove_copy/2,

  db_masters/1, db_masters/2
]).

%%=================================================================
%%	DBs Info API
%%=================================================================
-export([
  db_module/1,
  db_available_nodes/1,
  db_source_node/1,
  db_all_nodes/1,
  db_node_params/2,
  db_nodes_params/1,
  all_dbs/0,
  all_dbs_nodes_params/0,
  db_ready_nodes/1,
  db_not_ready_nodes/1,
  dbs_ready_nodes/0,
  dbs_not_ready_nodes/0,
  is_db_available/1,
  is_db_not_available/1,
  available_dbs/0,
  not_available_dbs/0,
  db_available_copies/1,
  db_not_available_copies/1,
  all_dbs_available_copies/0,
  all_dbs_not_available_copies/0
]).

%%=================================================================
%%	Data API
%%=================================================================
-export([
  read/2,
  write/2,
  delete/2,

  first/1,
  last/1,
  next/2,
  prev/2,

  find/2,
  foldl/4,
  foldr/4,
  update/2
]).

%%=================================================================
%%	Transaction API
%%=================================================================
-export([
  read/3,
  write/3,
  delete/3,

  transaction/1
]).

%%%%---------------------------------------------------------------
%%%%	Nodes Service API
%%%%---------------------------------------------------------------
remove_node(Node)->
  zaya_node:remove (Node ).

%%%%---------------------------------------------------------------
%%%%	Nodes Info API
%%%%---------------------------------------------------------------
all_nodes()->
  zaya_node:all_nodes().

is_node_ready(Node)->
  zaya_node:is_node_ready(Node).

is_node_not_ready(Node)->
  zaya_node:is_node_not_ready(Node).

node_status(Node)->
  zaya_node:node_status( Node ).

ready_nodes()->
  zaya_node:ready_nodes().

not_ready_nodes()->
  zaya_node:not_ready_nodes().

node_dbs(Node)->
  zaya_node:node_dbs( Node ).

ready_nodes_dbs()->
  zaya_node:ready_nodes_dbs().

not_ready_nodes_dbs()->
  zaya_node:not_ready_nodes_dbs().

all_nodes_dbs()->
  zaya_node:all_nodes_dbs().

node_db_params(Node, DB)->
  zaya_node:node_db_params(Node, DB).

ready_nodes_db_params(DB)->
  zaya_node:ready_nodes_db_params( DB ).

not_ready_nodes_db_params(DB)->
  zaya_node:not_ready_nodes_db_params( DB ).

node_dbs_params(Node)->
  zaya_node:node_dbs_params(Node).

all_nodes_dbs_params()->
  zaya_node:all_nodes_dbs_params().

%%%%---------------------------------------------------------------
%%%%	DBs Service API
%%%%---------------------------------------------------------------
db_create(DB, Module, Params)->
  zaya_db:create(DB, Module, Params).

db_open(DB)->
  zaya_db:open( DB ).

db_open(DB, Node)->
  zaya_db:open( DB, Node ).

db_force_open( DB, Node )->
  zaya_db:force_open( DB, Node ).

db_close(DB)->
  zaya_db:close( DB ).

db_close(DB, Node)->
  zaya_db:close( DB, Node ).

db_remove( DB )->
  zaya_db:remove( DB ).

db_add_copy(DB,Node,Params)->
  zaya_db:add_copy(DB,Node,Params).

db_remove_copy(DB, Node)->
  zaya_db:remove_copy( DB, Node ).

db_masters(DB)->
  zaya_db:masters( DB ).

db_masters(DB, Masters)->
  zaya_db:masters( DB, Masters ).

%%%%---------------------------------------------------------------
%%%%	DBs Info API
%%%%---------------------------------------------------------------
db_module(DB)->
  zaya_db:module( DB ).

db_available_nodes(DB)->
  zaya_db:available_nodes( DB ).

db_source_node( DB )->
  zaya_db:source_node( DB ).

db_all_nodes( DB )->
  zaya_db:all_nodes( DB ).

db_node_params(DB, Node)->
  zaya_db:node_params( DB, Node ).

db_nodes_params( DB )->
  zaya_db:nodes_params( DB ).

all_dbs()->
  zaya_db:all_dbs().

all_dbs_nodes_params()->
  zaya_db:all_dbs_nodes_params().

db_ready_nodes(DB)->
  zaya_db:ready_nodes( DB ).


db_not_ready_nodes( DB )->
  zaya_db:not_ready_nodes( DB ).

dbs_ready_nodes()->
  zaya_db:all_dbs_ready_nodes().

dbs_not_ready_nodes()->
  zaya_db:all_dbs_not_ready_nodes().

is_db_available(DB)->
  zaya_db:is_available( DB ).

is_db_not_available( DB )->
  zaya_db:is_not_available( DB ).

available_dbs()->
  zaya_db:available_dbs().

not_available_dbs()->
  zaya_db:not_available_dbs().

db_available_copies(DB)->
  zaya_db:available_copies( DB ).

db_not_available_copies(DB)->
  zaya_db:not_available_copies( DB ).

all_dbs_available_copies()->
  zaya_db:all_dbs_available_copies().

all_dbs_not_available_copies()->
  zaya_db:all_dbs_not_available_copies().

%%%%---------------------------------------------------------------
%%%%	DATA API
%%%%---------------------------------------------------------------
read(DB,Keys)->
  zaya_db:read( DB, Keys ).

write(DB, KVs)->
  zaya_db:write( DB, KVs ).

delete(DB, Keys)->
  zaya_db:delete( DB, Keys ).

first(DB)->
  zaya_db:first( DB ).

last(DB)->
  zaya_db:last( DB ).

next(DB, Key)->
  zaya_db:next( DB, Key ).

prev(DB, Key)->
  zaya_db:prev( DB, Key ).

find(DB, Query)->
  zaya_db:find( DB, Query ).

foldl(DB, Query, Fun, InAcc)->
  zaya_db:foldl( DB, Query, Fun, InAcc ).

foldr( DB, Query, Fun, InAcc)->
  zaya_db:foldr(DB, Query, Fun, InAcc).

update(DB, Query)->
  zaya_db:update( DB, Query ).

%%%%---------------------------------------------------------------
%%%%	TRANSACTION API
%%%%---------------------------------------------------------------
read(DB, Keys, Lock)->
  zaya_transaction:read( DB, Keys, Lock ).

write(DB, KVs, Lock)->
  zaya_transaction:write( DB, KVs, Lock ).

delete(DB, Keys, Lock)->
  zaya_transaction:delete( DB, Keys, Lock ).

transaction( Fun )->
  zaya_transaction:transaction( Fun ).