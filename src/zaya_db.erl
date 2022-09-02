
-module(zaya_db).

-include("zaya.hrl").
-include("zaya_schema.hrl").

%%=================================================================
%%	READ/WRITE API
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
%%	SEARCH API
%%=================================================================
-export([
  search/2
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

-define(LOCAL(M), fun M:?FUNCTION_NAME/?FUNCTION_ARITY).

-define(RPC(N,M,F,T),
  if
    ?FUNCTION_ARITY=:=0->fun()->T(N,M,F,[]) end;
    ?FUNCTION_ARITY=:=1->fun(A)->T(N,M,F,[A]) end;
    ?FUNCTION_ARITY=:=2->fun(A1,A2)->T(N,M,F,[A1,A2]) end
  end).

-define(IS_LOCAL(DB),
  case ?dbRef(DB) of ?undefined->false; _->true end
).

-define(REF(DB),
  case DB of
    {'$call$',_@DB}->
      ?dbRef(_@DB);
    _->
      case ?IS_LOCAL(DB) of
        true ->?dbRef(DB);
        false->{'$call$',DB}
      end
  end
).

%------------entry points------------------------------------------
-define(read(DB),
  case ?dbModule(DB) of ?undefined->?NOT_AVAILABLE;
    _@M->
      case ?IS_LOCAL(DB) of
        true->
          ?LOCAL(_@M);
        _->
          ?RPC(?dbReadyNodes(DB),_@M,?FUNCTION_NAME,fun ecall:call_one/4)
      end
  end).

-define(write(DB),
  case DB of
    {'$call$',_@DB}->
      ?LOCAL( ?dbModule(_@DB) );
    _->
      case ?dbModule(DB) of ?undefined->?NOT_AVAILABLE;
        _@M->
          case ?IS_LOCAL(DB) of
            true->
              ?RPC(?dbReadyNodes(DB) -- [node()],_@M,?FUNCTION_NAME,fun ecall:cast_all/4),
              ?LOCAL(_@M);
            _ ->
              ?RPC(?dbReadyNodes(DB),_@M,?FUNCTION_NAME,fun ecall:call_any/4)
          end
      end
  end
).

%%=================================================================
%%	READ/WRITE API
%%=================================================================
read( DB, Keys )->
  (?read(DB))(?REF(DB), Keys).

write(DB,KVs)->
  (?write(DB))(?REF(DB), KVs ).

delete(DB,Keys)->
  (?write(DB))(?REF(DB), Keys).


%%=================================================================
%%	ITERATOR
%%=================================================================
first(DB)->
  (?read(DB))(?REF(DB)).

last(DB)->
  (?read(DB))(?REF(DB)).

next(DB,Key)->
  (?read(DB))(?REF(DB), Key ).

prev(DB,Key)->
  (?read(DB))(?REF(DB), Key ).

%%=================================================================
%%	SEARCH
%%=================================================================
search(DB,Options)->
  (?read(DB))(?REF(DB), Options).


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





