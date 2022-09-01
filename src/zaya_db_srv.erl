
-module(zaya_db_srv).

-include("zaya.hrl").
-include("zaya_schema.hrl").

%%=================================================================
%%	TRANSFORMATION API
%%=================================================================
-export([
  create/3,
  add_copy/3,
  remove_copy/2,
  remove/1
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
        case Nodes -- ?readyNodes of
          []->
            Nodes;
          NotReadyNodes->
            throw({not_ready_nodes, NotReadyNodes})
        end
    end,

  case ecall:call_all( CreateNodes ,gen_server,call,[?MODULE,{create,DB,Module,Params}]) of
    {ok,_}->
      case ecall:call_all( ?readyNodes, gen_server,call,[?MODULE,{add,DB,Module,Params}] ) of
        {ok,_}->
          ok;
        {error,AddErrors}->
          ecall:cast_all(?readyNodes, gen_server, call, [?MODULE,{remove,DB}] ),
          throw({add_error, AddErrors})
      end;
    {error,CreateErrors}->
      ecall:cast_all(CreateNodes, gen_server, call, [?MODULE,{remove,DB}] ),
      throw({create_error,CreateErrors})
  end.

remove( DB )->
  case ecall:call_all_wait(?readyNodes, gen_server,call,[?MODULE,{remove,DB}] ) of
    {_,_Errors = []}->
      ok;
    {_,Errors}->
      {error,Errors}
  end.


add_copy(DB,Node,Params)->

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

  case ecall:call_all( ?readyNodes, gen_server,call,[?MODULE,{add_copy,DB,Node,Params}] ) of
    {ok,_}->
      ok;
    {_, AddErrors }->
      ecall:cast_all(?readyNodes, gen_server, cast, [?MODULE,{remove_copy,DB,Node}] ),
      throw(AddErrors)
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

  case ?dbReadyNodes(DB) of
    [Node]->
      throw( last_ready_copy );
    []->
      throw( db_not_ready );
    _->
      ok
  end,

  gen_server:call({?MODULE, Node},{remove_copy,DB}).


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
handle_call({create,DB,Module,NodesParams}, From ,State)->

  Params = maps:get( node(), NodesParams ),
  try
    Ref = zaya_db:create(DB, Module, Params),
    case zaya_schema_srv:create_db(DB, Module, Ref ) of
      ok ->
        gen_server:reply(From,ok);
      {error,SchemaError}->
        gen_server:reply(From,{error,{schema_error,SchemaError}})
    end
  catch
    _:CreateError->
      gen_server:reply(From,{error,CreateError})
  end,

  {noreply,State};

handle_call({add,DB,Module,NodesParams}, From ,State)->

  case zaya_schema_srv:add_db( DB,Module,NodesParams ) of
    ok->
      gen_server:reply(From,ok);
    {error,SchemaError}->
      gen_server:reply(From,{error,{schema_error,SchemaError}})
  end,

  {noreply,State};

handle_call({remove,DB}, From ,State)->

  case ?dbModule(DB) of

    ?undefined->
      gen_server:reply(From,ok);

    Module->
      Params = ?dbNodeParams(DB,node()),
      Ref = ?dbRef(DB),

      case zaya_schema_srv:remove_db( DB ) of
        ok->
          if
            Params =/= ?undefined->
              try
                if
                  Ref =/= ?undefined->
                    zaya_db:close( DB, Module, Ref );
                  true-> ignore
                end,
                zaya_db:remove(DB,Module,Params),

                gen_server:reply(From,ok)
              catch
                _:RemoveError->
                  gen_server:reply(From,{error,{remove_error,RemoveError}})
              end;
            true->
              gen_server:reply(From,ok)
          end;
        {error,SchemaError}->
          gen_server:reply(From,{error,{schema_error,SchemaError}})
      end
  end,

  {noreply,State};

handle_call({add_copy,DB,Node,Params}, From ,State)->
  % TODO
  Module = ?dbModule(DB),
  try

    Ref = zaya_db:create(DB, Module, Params),

    zaya_copy:copy(DB, Ref, Module, #{
      live=>true
    }),

    case zaya_schema_srv:open_db(DB,Ref) of
      ok->
        gen_server:reply(From,ok);
      {error,SchemaError}->
        gen_server:reply(From,{error,{schema_error,SchemaError}}),
        try
          zaya_db:close( DB, Module, Ref ),
          zaya_db:remove(DB, Module,Params)
        catch
          _:_ -> already_logged
        end
    end
  catch
    _:CreateError->
      gen_server:reply(From,{error,{create_error,CreateError}})
  end,

  {noreply,State};

handle_call({remove_copy,DB}, From ,State)->
  % TODO
  case zaya_schema_srv:remove_copy(DB) of
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


