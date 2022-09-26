
-module(zaya_db).

-include("zaya.hrl").
-include("zaya_schema.hrl").

%%=================================================================
%%	LOW_LEVEL API
%%=================================================================
-export([
  read/2,
  write/2,
  delete/2
]).

%%=================================================================
%%	ITERATOR API
%%=================================================================
-export([
  first/1,
  last/1,
  next/2,
  prev/2
]).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
-export([
  find/2,
  foldl/4,
  foldr/4,
  update/2
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  module/1,
  available_nodes/1,
  source_node/1,
  all_nodes/1,
  node_params/2,
  nodes_params/1,
  all_dbs/0,
  all_dbs_nodes_params/0,
  ready_nodes/1,
  not_ready_nodes/1,
  all_dbs_ready_nodes/0,
  all_dbs_not_ready_nodes/0,
  is_available/1,
  is_not_available/1,
  available_dbs/0,
  not_available_dbs/0,
  available_copies/1,
  not_available_copies/1,
  all_dbs_available_copies/0,
  all_dbs_not_available_copies/0
]).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  create/3, do_create/3,
  open/1, open/2,
  force_open/2,
  close/1, close/2,
  remove/1, do_remove/1,

  add_copy/3,
  remove_copy/2, do_remove_copy/1
]).

%%=================================================================
%%	ENGINE
%%=================================================================
not_available( F )->
  if
    F=:=write; F=:=update->
      ?not_available;
    F=:=next;F=:=prev;F=:=first;F=:=last->
      ?last;
    true->[]
  end.

-define(NOT_AVAILABLE,
  not_available( ?FUNCTION_NAME )
).

-define(LOCAL_CALL(Mod,Ref,Args),
  try apply( fun Mod:?FUNCTION_NAME/?FUNCTION_ARITY, [Ref|Args] )
  catch
    _:_->
    ?NOT_AVAILABLE
  end
).

remote_result(Type,Result) ->
  case Result of
    {ok,{_,_@Res}} when Type=:=call_one; Type=:= call_any->
      _@Res;
    ok ->
      ok;
    {ok,_@Res} when Type=:=call_all->
      _@Res;
    _@Res when Type=:=call_all_wait->
      _@Res;
    _->
      ?NOT_AVAILABLE
  end.

-define(REMOTE_CALL(Ns,Type,DB,Args),
  case Ns of
    _@Ns when is_list(_@Ns), length(_@Ns)>0 ->
      remote_result(Type, ecall:Type( Ns, ?MODULE, ?FUNCTION_NAME,[ DB|Args]) );
    _->
      ?NOT_AVAILABLE
  end
).

%------------entry points------------------------------------------
-define(read(DB, Args),
  case DB of
    _ when is_atom( DB )->
      _@Ref = ?dbRef( DB, node() ),
      if
        _@Ref =:= ?undefined->
          ?REMOTE_CALL( ?dbAvailableNodes(DB), call_one, {call,DB}, Args );
        true->
          _@M = ?dbModule(DB),
          ?LOCAL_CALL(_@M, _@Ref, Args)
      end;
    {call,_@DB}->
      _@M = ?dbModule(_@DB),
      _@Ref = ?dbRef(_@DB,node()),
      ?LOCAL_CALL( _@M, _@Ref, Args );
    _->
      ?NOT_AVAILABLE
  end
).

-define(write(DB, Args),
  case DB of
    _ when is_atom( DB ) ->
      _@Ref = ?dbRef( DB, node() ),
      _@Ns = ?dbAvailableNodes(DB)--[node()],
      if
        _@Ref =:= ?undefined->
          ?REMOTE_CALL( _@Ns, call_any, {call,DB}, Args );
        true->
          _@M = ?dbModule( DB ),
          case ?LOCAL_CALL(_@M,_@Ref, Args) of
            ?not_available->
              ?REMOTE_CALL( _@Ns, call_any, {call,DB}, Args );
            _@Res->
              ?REMOTE_CALL( _@Ns, cast_all, {call,DB}, Args ),
              _@Res
          end
      end;
    {call, _@DB}->
      _@M = ?dbModule(_@DB),
      _@Ref = ?dbRef(_@DB,node()),
      case ?LOCAL_CALL( _@M, _@Ref, Args) of
        ?not_available->
          ?not_available;
        _@Res->
          esubscribe:notify( _@DB, {?FUNCTION_NAME,Args} ),
          _@Res
      end;
    _->
      ?NOT_AVAILABLE
  end
).

-define(params(DB,Ps),
  maps:merge(#{
    dir => filename:absname(?schemaDir) ++"/"++atom_to_list(DB)
  },Ps)
).

%%=================================================================
%%	LOW-LEVEL (Required)
%%=================================================================
read( DB, Keys )->
  ?read(DB, [Keys] ).

write(DB,KVs)->
  ?write(DB, [KVs] ).

delete(DB, Keys)->
  ?write( DB, [Keys] ).

%%=================================================================
%%	ITERATOR (Optional)
%%=================================================================
first(DB)->
  ?read( DB, []).

last(DB)->
  ?read( DB, []).

next(DB,Key)->
  ?read( DB, [Key]).

prev(DB,Key)->
  ?read( DB, [Key]).

%%=================================================================
%%	HIGH-LEVEL (Optional)
%%=================================================================
find(DB, Query)->
  ?read( DB, [Query]).

foldl( DB, Query, Fun, InAcc )->
  ?read( DB, [Query, Fun, InAcc] ).

foldr( DB, Query, Fun, InAcc )->
  ?read( DB, [Query, Fun, InAcc] ).

update( DB, Query )->
  ?write( DB, [ Query ]).

%%=================================================================
%%	INFO
%%=================================================================
module( DB )->
  ?dbModule( DB ).

available_nodes( DB)->
  ?dbAvailableNodes( DB ).

source_node( DB )->
  ?dbSource( DB ).

all_nodes( DB )->
  ?dbAllNodes( DB ).

node_params(DB, Node)->
  ?dbNodeParams(DB, Node).

nodes_params( DB )->
  ?dbNodesParams(DB).

all_dbs()->
  ?allDBs.


all_dbs_nodes_params()->
  ?allDBsNodesParams.

ready_nodes(DB)->
  ?dbReadyNodes(DB).

not_ready_nodes(DB)->
  ?dbNotReadyNodes(DB).

all_dbs_ready_nodes()->
  ?dbsReadyNodes.

all_dbs_not_ready_nodes()->
  ?dbsNotReadyNodes.

is_available(DB)->
  ?isDBAvailable(DB).

is_not_available(DB)->
  ?isDBNotAvailable(DB).

available_dbs()->
  ?availableDBs.

not_available_dbs()->
  ?notAvailableDBs.

available_copies( DB )->
  ?dbAvailableNodes( DB ).

not_available_copies( DB )->
  ?dbAllNodes(DB) -- ?dbAvailableNodes(DB).

all_dbs_available_copies()->
  [{DB,?dbAvailableNodes( DB )} || DB <- ?allDBs ].

all_dbs_not_available_copies()->
  [{DB,not_available_copies( DB )} || DB <- ?allDBs ].

%%=================================================================
%%	SERVICE
%%=================================================================
create(DB, Module, Params)->
  if
    is_atom(DB)->
      ok;
    true->
      throw(invalid_name)
  end,

  case lists:member( Module, ?modules ) of
    false ->
      throw( invalid_module );
    _->
      ok
  end,

  if
    is_map( Params )->
      ok;
    true->
      throw( invalid_params )
  end,

  CreateNodes =
    case maps:keys( Params ) of
      []->
        throw(create_nodes_not_defined);
      Nodes->
        Nodes
    end,

  case CreateNodes -- (CreateNodes -- ?readyNodes) of
    [] -> throw(create_nodes_not_ready);
    _->ok
  end,

  {OKs, Errors} = ecall:call_all_wait( ?readyNodes ,?MODULE, do_create, [DB,Module,Params]),

  if
    length( OKs) =:= 0->
      ecall:cast_all( ?readyNodes, ?MODULE, do_remove, [DB] ),
      {error,Errors};
    true->
      {OKs,Errors}
  end.

do_create(DB, Module, InParams)->

  epipe:do([
    fun(_)->
      zaya_schema_srv:add_db(DB,Module),
      maps:get(node(), InParams, ?undefined)
    end,
    fun
      (_NodeParams = ?undefined)->
        {ok, added};
      (NodeParams)->
        Params = ?params(DB,NodeParams),
        epipe:do([
          fun(_) -> Module:create( Params ) end,
          fun(_)->
            ecall:call_all_wait(?readyNodes, zaya_schema_srv, add_db_copy,[ DB, node(), Params ]),
            {ok, created}
          end
        ],?undefined)
    end
  ], ?undefined ).

open( DB )->

  case ?dbModule( DB ) of
    ?undefined->
      throw( db_not_exists );
    _->
      ok
  end,

  ecall:call_all_wait( ?dbReadyNodes(DB), zaya_db_srv, open, [DB] ).

open( DB, Node )->
  rpc:call( Node, zaya_db_srv, open, [DB]).

force_open( DB, Node )->
  rpc:call( Node, zaya_db_srv, force_open, [DB]).

close(DB)->
  {ok,Unlock} = elock:lock(?locks, DB, _IsShared = false, _Timeout = ?infinity, ?readyNodes ),
  try ecall:call_all_wait( ?dbReadyNodes(DB), zaya_db_srv, close, [DB] )
  after
    Unlock()
  end.

close(DB, Node )->
  {ok,Unlock} = elock:lock(?locks, DB, _IsShared = false, _Timeout = ?infinity, [Node] ),
  try rpc:call( Node, zaya_db_srv, close, [DB])
  after
    Unlock()
  end.

remove( DB )->

  case ?dbModule(DB) of
    ?undefined->
      throw(db_not_exists);
    _->
      ok
  end,

  case ?dbAvailableNodes( DB ) of
    []-> ok;
    Nodes->
      throw({not_closed, Nodes})
  end,

  {ok,Unlock} = elock:lock(?locks, DB, _IsShared = false, _Timeout = ?infinity, ?readyNodes ),
  try ecall:call_all_wait(?readyNodes, ?MODULE, do_remove, [DB] )
  after
    Unlock()
  end.

do_remove( DB )->
  Module = ?dbModule( DB ),
  Params = ?dbNodeParams(DB,node()),
  epipe:do([
    fun(_) -> zaya_schema_srv:remove_db( DB ) end,
    fun
      (_) when Params =:= ?undefined->ok;
      (_)-> Module:remove( Params )
    end
  ], ?undefined).

add_copy(DB,Node,Params)->

  case ?dbModule(DB) of
    ?undefined->
      throw(db_not_exists);
    _->
      ok
  end,
  case lists:member( Node, ?allNodes ) of
    false->
      throw(node_not_attached);
    _->
      ok
  end,
  case ?isDBAvailable(DB) of
    false->
      throw(db_not_available);
    _->
      ok
  end,
  case lists:member(Node,?dbAllNodes(DB)) of
    true->
      throw(already_exists);
    _->
      ok
  end,
  case ?isNodeReady( Node ) of
    false->
      throw(node_not_ready);
    _->
      ok
  end,

  rpc:call( Node, zaya_db_srv, add_copy, [ DB, Params = ?params(DB,Params) ]).

remove_copy(DB, Node)->

  case ?dbModule(DB) of
    ?undefined->
      throw(db_not_exists);
    _->
      ok
  end,

  case lists:member( Node,?allNodes ) of
    false->
      throw(node_not_attached);
    _->
      ok
  end,

  case ?isNodeReady( Node ) of
    false->
      throw( node_not_ready );
    _->
      ok
  end,

  case ?dbNodeParams(DB,Node) of
    ?undefined ->
      throw(copy_not_exists);
    _->
      ok
  end,

  case ?dbRef( DB, Node ) of
    ?undefined -> ok;
    _->
      throw(db_not_closed)
  end,

  case ?dbAllNodes(DB) of
    [Node]->
      throw( last_copy );
    _->
      ok
  end,

  {ok,Unlock} = elock:lock(?locks, DB, _IsShared = false, [Node], _Timeout = ?infinity ),
  try rpc:call(Node, ?MODULE, do_remove_copy, [ DB ])
  after
    Unlock()
  end.

do_remove_copy( DB )->
  Module = ?dbModule( DB ),
  Params = ?dbNodeParams(DB,node()),
  epipe:do([
    fun(_) -> ecall:call_all(?readyNodes, zaya_schema_srv, remove_db_copy, [DB, node()] ) end,
    fun(_)-> Module:remove( Params ) end
  ],?undefined).






