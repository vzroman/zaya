
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
  foldr/4
]).

%%=================================================================
%%	SUBSCRIPTIONS API
%%=================================================================
-export([
  subscribe/1, subscribe/2,
  lookup/1,
  wait/2
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
  all_dbs_not_available_copies/0,
  get_size/1, get_size/2
]).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  create/3,
  open/1, open/2,
  force_open/2,
  close/1, close/2,
  remove/1,

  add_copy/3,
  remove_copy/2,
  set_copy_params/3,

  masters/1, masters/2,

  read_only/1, read_only/2,

  on_update/3
]).

%%=================================================================
%%	ENGINE
%%=================================================================
-define(NOT_AVAILABLE(DB), throw({not_available, DB})).

-define(LOCAL_CALL(Mod,Ref,Args),
  apply( fun Mod:?FUNCTION_NAME/?FUNCTION_ARITY, [Ref|Args] )
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
    {error,_@Err}->
      throw(_@Err)
  end.

-define(REMOTE_CALL(Ns,Type,DB,Args),
  case Ns of
    _@Ns when is_list(_@Ns), length(_@Ns)>0 ->
      remote_result(Type, ecall:Type( Ns, ?MODULE, ?FUNCTION_NAME,[ DB|Args]) );
    _->
      ?NOT_AVAILABLE(DB)
  end
).

%------------entry points------------------------------------------
-define(read(DB, Args),
  case DB of
    _ when is_atom( DB )->
      case ?dbRefMod( DB ) of
        {_@Ref, _@M}->
          ?LOCAL_CALL(_@M, _@Ref, Args);
        _->
          ?REMOTE_CALL( ?dbAvailableNodes(DB), call_one, {call,DB}, Args )
      end;
    {call,_@DB}->
      {_@Ref, _@M} = ?dbRefMod( _@DB ),
      ?LOCAL_CALL( _@M, _@Ref, Args );
    _->
      throw( badarg )
  end
).

-define(writeLocal(DB, Args),
  begin
    {_@Ref, _@M} = ?dbRefMod( DB ),
    _@Res = ?LOCAL_CALL( _@M, _@Ref, Args),
    on_update(DB, ?FUNCTION_NAME, hd(Args)),
    _@Res
  end
).

-define(write(DB, Args),
  case DB of
    _ when is_atom( DB ) ->
      case ?dbReadOnly(DB) of
        false ->
          case ?dbAvailableNodes(DB) of
            _@DBNs when is_list( _@DBNs )->
              case lists:member(node(), _@DBNs ) of
                true ->
                  ?REMOTE_CALL( _@DBNs--[node()], cast_all, {call,DB}, Args ),
                  ?writeLocal(DB, Args);
                _ ->
                  ?REMOTE_CALL( _@DBNs, call_any, {call,DB}, Args )
              end;
            _->
              ?NOT_AVAILABLE(DB)
          end;
        _->
          throw({read_only, DB})
      end;
    {call, _@DB}->
      ?writeLocal(_@DB, Args);
    _->
      throw( badarg )
  end
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

%%=================================================================
%%	SUBSCRIPTIONS
%%=================================================================
subscribe(DB)->
  subscribe(DB, self()).
subscribe(DB, PID)->
  Nodes = ?dbAvailableNodes(DB),
  case lists:member(node(), Nodes) of
    true -> esubscribe:subscribe(?subscriptions, DB, PID);
    _->
      case ecall:call_one(Nodes, ?MODULE, ?FUNCTION_NAME, [DB,PID]) of
        {ok,_}-> ok;
        Error -> Error
      end
  end.

lookup(DB)->
  esubscribe:lookup(?subscriptions,DB).
wait(DB, Timeout)->
  esubscribe:wait(?subscriptions, DB, Timeout).

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

get_size( DB )->
  Undefined = maps:from_list([{N,?undefined} || N <- ?dbAllNodes(DB)] ),
  {OKs,_} = ecall:call_all_wait( ?dbAvailableNodes(DB), zaya_db_srv, get_size,[DB] ),
  lists:foldl(fun({N,Size},Acc)->
    Acc#{ N => Size }
  end,Undefined, OKs).

get_size( DB, Node )->
  ecall:call(Node, zaya_db_srv, get_size, [DB]).


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

  if
    is_map( Params )->
      ok;
    true->
      throw( invalid_params )
  end,

  case lists:member( DB, ?allDBs ) of
    true ->
      throw( already_exists );
    _->
      ok
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

  {OKs, Errors} = ecall:call_all_wait( ?readyNodes ,zaya_db_srv, create, [DB,Module,Params]),

  case [OK || OK = {_N,{ok,created}} <- OKs ] of
    []->
      ecall:cast_all( ?readyNodes, ?MODULE, remove, [DB] ),
      throw(Errors);
    CreateOKs->
      ?LOGINFO("~p database created at ~p nodes",[DB,[N || {N,_} <- CreateOKs]]),
      {OKs,Errors}
  end.

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
  ecall:call_all_wait( ?dbReadyNodes(DB), zaya_db_srv, close, [DB] ).

close(DB, Node )->
  rpc:call( Node, zaya_db_srv, close, [DB]).

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

  ecall:call_all_wait(?readyNodes, zaya_db_srv, remove, [DB] ).

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

  case rpc:call( Node, zaya_db_srv, add_copy, [ DB, Params ]) of
    ok->ok;
    {error,Error}->throw(Error)
  end.

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

  rpc:call(Node, zaya_db_srv, remove_copy, [ DB ]).

set_copy_params(DB,Node,Params)->
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

  case ?dbNodeParams(DB,Node) of
    ?undefined ->
      throw(copy_not_exists);
    _->
      ok
  end,

  case ecall:call_all_wait(?readyNodes, zaya_schema_srv, set_copy_params, [DB, Node,Params] ) of
    {[],Errors}->
      throw(Errors);
    Result->
      Result
  end.


masters( DB )->
  ?dbMasters( DB ).

masters( DB, Masters )->
  case ?dbModule(DB) of
    ?undefined->
      throw(db_not_exists);
    _->
      ok
  end,

  if
    Masters =:= ?undefined->
      ok;
    is_list( Masters ) ->
      case Masters -- ?dbAllNodes( DB ) of
        []->
          ok;
        InvalidCopies->
          throw({invalid_copies, InvalidCopies})
      end;
    true->
      throw({badarg,Masters})
  end,

  ecall:call_all_wait(?readyNodes, zaya_schema_srv, set_db_masters, [DB,Masters] ).

read_only( DB )->
  ?dbReadOnly( DB ).

read_only( DB, IsReadOnly )->
  case ?dbModule(DB) of
    ?undefined->
      throw(db_not_exists);
    _->
      ok
  end,

  if
    is_boolean( IsReadOnly ) ->
      ok;
    true->
      throw({badarg,IsReadOnly})
  end,

  ecall:call_all_wait(?readyNodes, zaya_db_srv, set_readonly, [DB,IsReadOnly] ).

on_update( DB, Action, Args )->
  esubscribe:notify(?subscriptions, DB, {Action,Args} ),
  % TODO. Update hash tree
  ok.




