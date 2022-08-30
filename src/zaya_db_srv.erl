
-module(zaya_db_srv).

-include("zaya.hrl").
-include("zaya_schema.hrl").

%%=================================================================
%%	TRANSFORMATION API
%%=================================================================
-export([
  create/2,
  add_copy/3,
  remove_copy/2,
  remove/2
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
create(db, Params)->
  if
    is_atom(db)->ok;
    true->throw(invalid_name)
  end,

  case Params of
    #{module:=Module}->
      case lists:member( Module, ?modules ) of
        false ->
          throw( invalid_module );
        _->
          Module:check_params( Params )
      end;
    _-> throw(module_not_defined)
  end,

  case Params of
    #{nodes:=NodesParams}->
      case maps:keys( NodesParams ) of
        []-> throw(empty_nodes_params);
        Nodes->
          case Nodes -- ?readyNodes of
            []->
              Module = maps:get(module,Params),
              [ Module:check_params( NPs) || {_N,NPs} <- maps:to_list(NodesParams) ];
            NotReadyNodes->
              throw({not_ready_nodes, NotReadyNodes})
          end
      end;
    _->
      throw(nodes_not_defined)
  end,
  CreateNodes = maps:get(nodes,Params),
  case ecall:call_all( maps:keys(CreateNodes) ,?MODULE,create,[db,Params]) of
    {ok,_}->
      ok;
    {error,NodesErrors}->
      throw(NodesErrors)
  end.

add_copy(db,Node,Params)->

  case ?dbModule(db) of
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
  case ?dbReadyNodes(db) of
    []->
      throw(not_ready_db);
    _->
      ok
  end,
  case lists:member(Node,?dbAllNodes(db)) of
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

  Module = ?dbModule(db),
  Module:check_params(Params),

  gen_server:call({?MODULE, Node},{add_copy,db,Params}).

remove_copy(db, Node)->

  case ?dbModule(db) of
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

  case ?dbNodeParams(db,Node) of
    ?undefined ->
      throw(copy_not_exists);
    _->
      ok
  end,

  case ?dbReadyNodes(S) of
    [Node]->
      throw( last_ready_copy );
    []->
      throw( db_not_ready );
    _->
      ok
  end,

  gen_server:call({?MODULE, Node},{remove_copy,db}).


remove( db, Params )->
  gen_server:cast(?MODULE, {remove,db, Params}).


%%=================================================================
%%	OTP
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE},?MODULE, [], []).

-record(state,{}).

init([])->

  process_flag(trap_exit,true),

  ?LOGINFO("starting db server ~p",[self()]),

  {ok,#state{}}.

%-----------------CALL------------------------------------------------
handle_call({create,db,#{module:=Module, nodes:=NodesParams}} = Params, From ,State)->

  try
    Ref = zaya_db:create(db, Module, Params),
    case zaya_schema_srv:add_db(db,Params,Ref) of
      ok->
        gen_server:reply(From,ok);
      {error,SchemaError}->
        gen_server:reply(From,{error,{schema_error,SchemaError}}),
        ecall:cast_all( maps:keys(NodesParams),?MODULE,remove,[db,Params] )
    end
  catch
    _:CreateError->
      gen_server:reply(From,{error,{create_error,CreateError}})
  end,

  {noreply,State};

handle_call({add_copy,db,Params}, From ,State)->
  Module = ?dbModule(db),
  try
    Ref = zaya_db:create(db, Module, Params),
    case zaya_schema_srv:add_copy(db,Params,Ref) of
      ok->
        gen_server:reply(From,ok);
      {error,SchemaError}->
        gen_server:reply(From,{error,{schema_error,SchemaError}}),
        zaya_db:remove(db,Module,Params)
    end
  catch
    _:CreateError->
      gen_server:reply(From,{error,{create_error,CreateError}})
  end,

  {noreply,State};

handle_call({remove_copy,db}, From ,State)->

  case zaya_schema_srv:remove_copy(db) of
    ok->
      Module = ?dbModule(db),
      Params = ?dbNodeParams( db, node() ),
      Ref = ?dbRef( db ),
      try
        zaya_db:close(db,Module,Ref),
        zaya_db:remove(db,Module, Params),
        gen_server:reply(From,ok)
      catch
        _:CreateError->
          gen_server:reply(From,{error,{create_error,CreateError}})
      end;
    {error,SchemaError}->
      gen_server:reply(From,{error,{schema_error,SchemaError}})
  end,

  {noreply,State};

handle_call(Request, From, State) ->
  ?LOGWARNING("node server got an unexpected call resquest ~p from ~p",[Request,From]),
  {noreply,State}.

handle_cast({remove, db,#{module:=Module,nodes:=NodesParams}},State)->
  Params = maps:get(node(),NodesParams),
  zaya_db:remove(db,Module,Params),
  zaya_schema_srv:remove_db(db),
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


