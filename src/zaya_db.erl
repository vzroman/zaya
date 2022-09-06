
-module(zaya_db).

-include("zaya.hrl").
-include("zaya_schema.hrl").

%%=================================================================
%%	LOW_LEVEL API
%%=================================================================
-export([
  get/2,
  put/2,
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

]).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  create/3, do_create/3,
  open/3,
  close/3,
  remove/1, do_remove/1,

  add_copy/3, do_add_copy/2,
  remove_copy/2, do_remove_copy/1
]).

%%=================================================================
%%	RECOVERY API
%%=================================================================
-export([
  try_recover/1
]).

%%=================================================================
%%	ENGINE
%%=================================================================
-define(NOT_AVAILABLE,
  if
    ?FUNCTION_NAME=:=write->
      ?not_available;
    ?FUNCTION_NAME=:=next;?FUNCTION_NAME=:=prev;?FUNCTION_NAME=:=first;?FUNCTION_NAME=:=last->
      ?last;
    true->[]
  end
).

-define(LOCAL_CALL(Mod,Ref,Args),
  try case Args of
    []-> Mod:?FUNCTION_NAME(Ref);
    [_@1]->Mod:?FUNCTION_NAME(Ref,_@1);
    [_@1,_@2]->Mod:?FUNCTION_NAME(Ref,_@1,_@2);
    [_@1,_@2,_@3]->Mod:?FUNCTION_NAME(Ref,_@1,_@2,_@3)
  end catch
    _:_-> ?NOT_AVAILABLE
  end
).

-define(REMOTE_CALL(Type,Ns,DB,Args),
  case ecall:Type( Ns, ?MODULE, ?FUNCTION_NAME,[ DB|Args]) of
    {ok,_@Res} -> _@Res;
    _-> ?NOT_AVAILABLE
  end
).

%------------entry points------------------------------------------
-define(REF(DB),
  case DB of
    _ when is_atom( DB)->
      {db, ?dbRef( DB ), ?dbModule( DB ) };
    {call,_@DB}->
      {call, ?dbRef( _@DB ), ?dbModule(_@DB) }
  end
).

-define(get(DB, Args),
  case ?REF(DB) of
    {db, _@Ref,_@Mod} when _@Ref =/= ?undefined, _@Mod =/= ?undefined ->
      ?LOCAL_CALL( _@Mod, _@Ref, Args);
    {call, _@Ref, _@Mod} when _@Ref =/= ?undefined, _@Mod =/= ?undefined ->
      ?LOCAL_CALL( _@Mod, _@Ref, Args);
    {db, _, _@Mod} when _@Mod =/= ?undefined->
      ?REMOTE_CALL( ?dbReadyNodes(_@DB), call_one, {call,DB}, Args );
    _->
      ?NOT_AVAILABLE
  end
).

-define(put(DB, Args),
  case ?REF(DB) of
    {db, _@Ref,_@Mod} when _@Ref =/= ?undefined, _@Mod =/= ?undefined ->
      ?REMOTE_CALL( ?dbReadyNodes(DB), call_any, {call,DB}, _@Args );
    [{call, _@Ref,_@Mod}|_@Args] when _@Ref =/= ?undefined, _@Mod =/= ?undefined ->
      ?LOCAL_CALL( _@Mod, _@Ref, _@Args);
    _->
      ?NOT_AVAILABLE
  end
).

%%=================================================================
%%	LOW-LEVEL (Required)
%%=================================================================
get( DB, Keys )->
  ?get(DB, [Keys] ).

put(DB,KVs)->
  ?put(DB, [KVs] ).

delete(DB, Keys)->
  ?put( DB, [Keys] ).

%%=================================================================
%%	ITERATOR (Optional)
%%=================================================================
first(DB)->
  ?get( DB, []).

last(DB)->
  ?get( DB, []).

next(DB,Key)->
  ?get( DB, [Key]).

prev(DB,Key)->
  ?get( DB, [Key]).

%%=================================================================
%%	HIGH-LEVEL (Optional)
%%=================================================================
find(DB, Query)->
  ?get( DB, [Query]).

foldl( DB, Query, Fun, InAcc )->
  ?get( DB, [Query, Fun, InAcc] ).

foldr( DB, Query, Fun, InAcc )->
  ?get( DB, [Query, Fun, InAcc] ).

update( DB, Query )->
  ?put( DB, [ Query ]).

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

  case [N || {N, created} <- OKs] of
    []->
      ecall:cast_all( ?readyNodes, ?MODULE, do_remove, [DB] ),
      {error,Errors};
    _->
      {OKs,Errors}
  end.

do_create(DB, Module, NodesParams)->

  epipe:do([
    fun(_)->
      zaya_schema_srv:add_db(DB,Module),
      maps:get(node(), NodesParams, ?undefined)
    end,
    fun
      (_Params = ?undefined)->
        {ok, added};
      (Params)->
        epipe:do([
          fun(_) -> Module:create( Params ) end,
          fun(_)-> Module:open( Params ) end,
          fun( Ref )-> zaya_schema_srv:open_db(DB,Ref) end,
          fun(_)->
            ecall:cast_all(?readyNodes, zaya_schema_srv, add_db_copy,[ DB, node(), Params ]),
            {ok, created}
          end
        ],?undefined)
    end
  ], ?undefined ).


open(DB, Module, Params )->
  try
    Ref=Module:open( Params ),
    {ok, Ref}
  catch
    _:E:S->
      ?LOGERROR("~p open with params ~p module ~p error ~p stack ~p",[DB,Params,Module,E,S]),
      {error, {module_error,E}}
  end.

close(DB, Module, Ref )->
  try
    Module:close( Ref )
  catch
    _:E:S->
      ?LOGERROR("~p db close module error ~p stack ~p",[DB,E,S]),
      {error,{module_error,E}}
  end.

remove( DB )->

  case ?dbModule(DB) of
    ?undefined->
      throw(db_not_exists);
    _->
      ok
  end,

  {ok,Unlock} = elock:lock(elock, DB, false =_IsShared, ?dbReadyNodes(DB), ?infinity= _Timeout ),
  try ecall:call_all_wait(?readyNodes, ?MODULE, do_remove, [DB] )
  after
    Unlock()
  end.

do_remove( DB )->
  Module = ?dbModule( DB ),
  Ref = ?dbRef(DB),
  Params = ?dbNodeParams(DB,node()),
  epipe:do([
    fun(_) -> zaya_schema_srv:remove_db( DB ) end,
    fun
      (_) when Ref =:= ?undefined-> ok;
      (_)-> Module:close( Ref )
    end,
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
  case ?dbReadyNodes(DB) of
    []->
      throw(db_not_ready);
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

  ecall:call_one([Node], ?MODULE, do_add_copy, [ DB, Params ]).

do_add_copy( DB, Params )->

  Module = ?dbModule(DB),
  epipe:do([
    fun(_)-> Module:create( Params ) end,
    fun(_)-> Module:open( Params ) end,
    fun( Ref )->
      epipe:do([
        fun(_)-> zaya_copy:copy(DB, Ref, Module, #{ live=>true }) end,
        fun(_)-> zaya_schema_srv:open_db(DB,Ref) end
      ],?undefined)
    end,
    fun(_)-> ecall:call_any(?readyNodes, zaya_schema_srv, add_db_copy,[ DB, node(), Params ]) end
  ],?undefined).

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

  case ?dbReadyNodes(DB) of
    [Node]->
      throw( last_ready_copy );
    []->
      throw( db_not_ready );
    _->
      ok
  end,

  {ok,Unlock} = elock:lock(elock, DB, false =_IsShared, [Node], ?infinity= _Timeout ),
  try ecall:call_one([Node], ?MODULE, do_remove_copy, [ DB ])
  after
    Unlock()
  end.

do_remove_copy( DB )->
  Module = ?dbModule( DB ),
  Ref = ?dbRef(DB),
  Params = ?dbNodeParams(DB,node()),
  epipe:do([
    fun(_) -> ecall:call_all(?readyNodes, zaya_schema_srv, remove_db_copy, [DB, node()] ) end,
    fun
      (_) when Ref =:= ?undefined-> ok;
      (Ref)-> Module:close( Ref )
    end,
    fun(_)-> Module:remove( Params ) end
  ],?undefined).


%%=================================================================
%%	SCHEMA SERVER
%%=================================================================
try_recover(DB)->
  Params = ?dbNodeParams(DB,node()),
  Module = ?dbModule( DB ),
  Ref = Module:open(Params),
  zaya_copy:copy(DB, Ref, Module, #{
    live=>true,
    attempts=>?env(db_recovery_attempts,?DEFAULT_DB_RECOVERY_ATTEMPTS)
  }),
  Module:close(Ref).





