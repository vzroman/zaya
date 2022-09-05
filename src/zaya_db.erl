
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
%%	RPC API
%%=================================================================
-export([
  create/3,
  open/3,
  close/3,
  remove/3
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
%%	DB SERVER
%%=================================================================
create(DB, Module, Params)->
  try
    Module:create( Params ),
    _Ref=Module:open( Params )
  catch
    _:E:S->
      ?LOGERROR("~p create with params ~p module ~p error ~p stack ~p",[DB,Params,Module,E,S]),
      throw({module_error,E})
  end.

open(DB, Module, Params )->
  try
    _Ref=Module:open( Params )
  catch
    _:E:S->
      ?LOGERROR("~p open with params ~p module ~p error ~p stack ~p",[DB,Params,Module,E,S]),
      throw({module_error,E})
  end.

close(DB, Module, Ref )->
  try
    Module:close( Ref )
  catch
    _:E:S->
      ?LOGERROR("~p db close module error ~p stack ~p",[DB,E,S]),
      throw({module_error,E})
  end.

remove( DB, Module, Params )->
  try
     Module:remove( Params )
  catch
    _:E:S->
      ?LOGERROR("~p remove module error ~p stack ~p",[DB,E,S])
  end.

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





