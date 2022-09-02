
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
        Nodes
    end,

  case CreateNodes -- (CreateNodes -- ?readyNodes) of
    [] -> throw(create_nodes_not_ready);
    _->ok
  end,

  {OKs, Errors} = ecall:call_all_wait( ?readyNodes ,gen_server,call,[?MODULE,{create,DB,Module,Params},?infinity]),

  case [N || {N, created} <- OKs] of
    []->
      ecall:cast_all( ?readyNodes, gen_server,call,[?MODULE,{remove,DB}] ),
      {error,Errors};
    _->
      {OKs,Errors}
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

  gen_server:call({?MODULE, Node}, {add_copy,DB,Params}, ?infinity ).


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

  gen_server:call({?MODULE, Node},{remove_copy,DB},?infinity).

remove( DB )->

  case ?dbModule(DB) of
    ?undefined->
      throw(db_not_exists);
    _->
      ok
  end,

  ecall:call_all_wait(?readyNodes, gen_server,call,[?MODULE,{remove,DB},?infinity] ).


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
handle_call({create,DB,Module,NodesParams}, _From ,State)->

  Result =
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
            fun(_) -> zaya_db:create(DB, Module, Params) end,
            fun( Ref )-> zaya_schema_srv:open_db(DB,Ref) end,
            fun(_)->
              ecall:cast_all(?readyNodes, zaya_schema_srv, add_db_copy,[ DB, node(), Params ]),
              {ok, created}
            end
          ],?undefined)
      end
    ], ?undefined ),

  {reply, Result ,State};

handle_call({remove,DB}, _From ,State)->

  Module = ?dbModule( DB ),
  Result = epipe:do([
    fun(_) -> zaya_schema_srv:remove_db( DB ) end,
    fun
      (_Ref = ?undefined)-> ok;
      ( Ref )-> zaya_db:close( DB, Module, Ref )
    end,
    fun(_)-> ?dbNodeParams(DB,node()) end,
    fun
      (_Params = ?undefined)->ok;
      (Params)-> zaya_db:remove(DB,Module,Params)
    end
  ], ?dbRef(DB)),

  {reply, Result ,State};

handle_call({add_copy,DB,Params}, _From ,State)->

  Module = ?dbModule(DB),
  Result = epipe:do([
    fun(_)-> zaya_db:create(DB, Module, Params) end,
    fun( Ref )->
      epipe:do([
        fun(_)-> zaya_copy:copy(DB, Ref, Module, #{ live=>true }) end,
        fun(_)-> zaya_schema_srv:open_db(DB,Ref) end
      ],?undefined)
    end,
    fun(_)-> ecall:cast_all(?readyNodes, zaya_schema_srv, add_db_copy,[ DB, node(), Params ]) end
  ],?undefined),

  {reply, Result ,State};

handle_call({remove_copy,DB}, _From ,State)->

  Module = ?dbModule(DB),
  Params = ?dbNodeParams(DB, node()),
  Result = epipe:do([
    fun(_) -> ecall:call_all(?readyNodes, zaya_schema_srv, remove_db_copy, [DB, node()] ) end,
    fun(_)-> ?dbRef(DB) end,
    fun
      (_Ref = ?undefined)-> ok;
      (Ref)-> zaya_db:close(DB, Module, Ref)
    end,
    fun(_)-> zaya_db:remove(DB, Module, Params) end
  ],?undefined),

  {reply, Result ,State};

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


