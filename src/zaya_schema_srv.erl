
-module(zaya_schema_srv).

-include("zaya.hrl").
-include("zaya_schema.hrl").

-behaviour(gen_server).

%%=================================================================
%%	ONLY ME DO SCHEMA WRITES!
%%=================================================================
%%=================================================================
%%	NODES API
%%=================================================================
-export([
  node_up/2,
  node_down/2,
  remove_node/1,
  split_brain/1
]).

%%=================================================================
%%	dbS API
%%=================================================================
-export([
  add_db/2,
  open_db/3,
  close_db/2,
  remove_db/1,

  add_db_copy/3,
  remove_db_copy/2,
  set_copy_params/3,

  set_db_masters/2,
  set_db_readonly/2,
  set_db_nodes/2
]).

%%=================================================================
%%	Info API
%%=================================================================
-export([
  schema_dir/0
]).

%%=================================================================
%%	Subscriptions API
%%=================================================================
-export([
  subscribe/0,subscribe/1
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
%%	NODES
%%=================================================================
node_up( Node, Info )->
  gen_server:call(?MODULE, {node_up, Node, Info}, ?infinity).

node_down( Node, Info )->
  gen_server:call(?MODULE, {node_down, Node, Info}, ?infinity).

remove_node( Node )->
  gen_server:call(?MODULE, {remove_node, Node}, ?infinity).

split_brain( Node )->
  gen_server:cast(?MODULE, {split_brain, Node}).

%%=================================================================
%%	DBs
%%=================================================================
add_db(DB,Module)->
  gen_server:call(?MODULE, {add_db, DB, Module}, ?infinity).

open_db(DB, Node, Ref)->
  gen_server:call(?MODULE, {open_db, DB, Node, Ref}, ?infinity).

close_db(DB, Node)->
  gen_server:call(?MODULE, {close_db, DB, Node}, ?infinity).

remove_db( DB )->
  gen_server:call(?MODULE, {remove_db, DB}, ?infinity).

add_db_copy(DB,Node,Params)->
  gen_server:call(?MODULE, {add_db_copy,DB,Node,Params}, ?infinity).

remove_db_copy( DB, Node )->
  gen_server:call(?MODULE, {remove_db_copy, DB, Node}, ?infinity).

set_copy_params(DB, Node,Params)->
  gen_server:call(?MODULE, {set_copy_params,DB,Node,Params}, ?infinity).

set_db_masters( DB, Masters )->
  gen_server:call(?MODULE, {set_db_masters, DB, Masters}, ?infinity).

set_db_readonly( DB, IsReadOnly )->
  gen_server:call(?MODULE, {set_db_readonly, DB, IsReadOnly}, ?infinity).

set_db_nodes( DB, Nodes )->
  gen_server:call(?MODULE, {set_db_nodes, DB, Nodes}, ?infinity).

%%=================================================================
%%	Info
%%=================================================================
schema_dir()->
  ?schemaDir.

%%=================================================================
%%	Subscriptions
%%=================================================================
subscribe( )->
  subscribe( self() ).

subscribe( PID )->
  ?schemaSubscribe( PID ).

%%=================================================================
%%	OTP
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE},?MODULE, [], []).

-record(state,{}).
init([])->

  process_flag(trap_exit,true),

  ?LOGINFO("starting schema server ~p",[self()]),

  try try_load()
  catch
    _:E:S->
      ?LOGERROR("LOAD ERROR! ~p stack ~p\r\n"
      ++" please check error logs, fix the problem and try to start again",[E,S]),
      timer:sleep( ?infinity )
  end,

  {ok,#state{ }}.

%%=================================================================
%%	CALL NODES
%%=================================================================
handle_call({attach_request, Node, ?MODULE}, _From, State) ->

  ?LOGINFO("attach request from node ~p",[Node]),

  {reply,{?schema, ?schemaId ,?getSchema},State};

handle_call({node_up, Node, Info}, From, State) ->

  try
    ?NODE_UP( Node ),
    gen_server:reply(From,ok),
    ?LOGINFO("~p node up, info ~p",[Node, Info])
  catch
    _:E:S->
      gen_server:reply(From, {error,E}),
      ?LOGERROR("~p node up schema error ~p stack ~p",[Node,E,S])
  end,

  {noreply,State};

handle_call({node_down, Node, Info}, From, State) ->

  try
    ?NODE_DOWN( Node ),
    gen_server:reply(From,ok),
    ?LOGINFO("~p node down, info ~p",[Node, Info])
  catch
    _:E:S->
      gen_server:reply(From, {error,E}),
      ?LOGERROR("~p node down schema error ~p stack ~p",[Node,E,S])
  end,

  {noreply,State};

handle_call({remove_node, Node}, From, State) ->

  try
    ?REMOVE_NODE( Node ),
    ?LOGINFO("~p node removed from schema",[Node]),
    gen_server:reply(From,ok)
  catch
    _:E:S->
      gen_server:reply(From, {error,E}),
      ?LOGERROR("~p node remove error ~p stack ~p",[Node,E,S])
  end,

  {noreply,State};

%%=================================================================
%%	CALL DBs
%%=================================================================
handle_call({add_db, DB, Module}, From, State) ->

  try
      ?ADD_DB(DB,Module),
      gen_server:reply(From,ok),
      ?LOGINFO("~p db added to schema, module ~p",[DB,Module])
  catch
    _:E:S->
      gen_server:reply(From, {error,E}),
      ?LOGERROR("~p add db schema error ~p stack ~p",[DB,E,S])
  end,

  {noreply,State};

handle_call({open_db, DB, Node, Ref}, From, State) ->

  try
    ?OPEN_DB(DB,Node,Ref),
    gen_server:reply(From,ok),
    ?LOGINFO("~p db opened at ~p",[DB, Node])
  catch
    _:E:S->
      gen_server:reply(From, {error,E}),
      ?LOGERROR("~p db open schema error ~p stack ~p",[DB,E,S])
  end,

  {noreply,State};

handle_call({close_db, DB, Node}, From, State) ->

  try
    case ?dbModule(DB) of
      ?undefined -> ignore;
      _-> ?CLOSE_DB(DB, Node)
    end,
    gen_server:reply(From,ok),
    ?LOGINFO("~p db closed at ~p",[DB,Node])
  catch
    _:E:S->
      gen_server:reply(From, {error,E}),
      ?LOGERROR("~p db close schema error ~p stack ~p",[DB,E,S])
  end,

  {noreply,State};


handle_call({remove_db, DB},From,State)->

  try
    ?REMOVE_DB(DB),
    gen_server:reply(From,ok),
    ?LOGINFO("~p removed from schema",[DB])
  catch
    _:E:S->
      gen_server:reply(From, {error,E}),
      ?LOGERROR("~p remove from schema error ~p stack ~p",[DB,E,S])
  end,

  {noreply,State};

handle_call({add_db_copy,DB,Node,Params}, From, State) ->
  try
    ?ADD_DB_COPY(DB,Node,Params),
    gen_server:reply(From,ok),
    ?LOGINFO("~p copy added to ~p, params: ~p",[DB,Node,Params])
  catch
    _:E:S->
      ?LOGERROR("~p add copy to ~p schema error ~p stack ~p",[DB,Node,E,S]),
      gen_server:reply(From, {error,E})
  end,

  {noreply,State};

handle_call({remove_db_copy, DB, Node}, From, State) ->

  try
    ?REMOVE_DB_COPY( DB, Node ),
    gen_server:reply(From,ok),
    ?LOGINFO("~p copy removed from ~p",[DB, Node])
  catch
    _:E:S->
      gen_server:reply(From, {error,E}),
      ?LOGERROR("~p remove copy from ~p schema error ~p stack ~p",[DB,Node,E,S])
  end,

  {noreply,State};

handle_call({set_copy_params,DB,Node,Params}, From, State) ->
  try
    ?UPDATE_DB_COPY(DB,Node,Params),
    gen_server:reply(From,ok),
    ?LOGINFO("~p copy update params at ~p, params: ~p",[DB,Node,Params])
  catch
    _:E:S->
      ?LOGERROR("~p update copy params at ~p schema error ~p stack ~p",[DB,Node,E,S]),
      gen_server:reply(From, {error,E})
  end,

  {noreply,State};

handle_call({set_db_masters, DB, Masters}, From, State) ->

  try
    ?SET_DB_MASTERS( DB, Masters ),
    gen_server:reply(From,ok),
    ?LOGINFO("~p set master nodes ~p",[DB, Masters])
  catch
    _:E:S->
      gen_server:reply(From, {error,E}),
      ?LOGERROR("~p set master nodes ~p schema error ~p stack ~p",[DB,Masters,E,S])
  end,

  {noreply,State};

handle_call({set_db_readonly, DB, IsReadOnly}, From, State) ->

  try
    case ?dbReadOnly( DB ) of
      IsReadOnly ->
        ignore;
      _->
        ?SET_DB_READONLY( DB, IsReadOnly ),
        ?LOGINFO("~p set read only mode ~p",[DB, IsReadOnly])
    end,
    gen_server:reply(From,ok)
  catch
    _:E:S->
      gen_server:reply(From, {error,E}),
      ?LOGERROR("~p set read only mode ~p schema error ~p stack ~p",[DB,IsReadOnly,E,S])
  end,

  {noreply,State};

handle_call({set_db_nodes, DB, Nodes}, From, State) ->

  try
    ?DB_AVAILABLE_NODES( DB, Nodes ),
    gen_server:reply(From,ok),
    ?LOGINFO("~p set available nodes ~p",[DB, Nodes])
  catch
    _:E:S->
      gen_server:reply(From, {error,E}),
      ?LOGERROR("~p set available nodes ~p schema error ~p stack ~p",[DB,Nodes,E,S])
  end,

  {noreply,State};

handle_call(Request, From, State) ->
  ?LOGWARNING("schema server got an unexpected call resquest ~p from ~p",[Request,From]),
  {noreply,State}.

handle_cast({split_brain, Node},State)->

  ?LOGWARNING("~p node split brain",[Node]),
  try ?NODE_UP( Node )
  catch
    _:E:S->
      ?LOGERROR("~p node up schema error ~p stack ~p",[Node,E,S])
  end,

  merge_brain( Node ),

  {noreply,State};

handle_cast(Request,State)->
  ?LOGWARNING("schema server got an unexpected cast resquest ~p",[Request]),
  {noreply,State}.

handle_info(Message,State)->
  ?LOGWARNING("schema server got an unexpected message ~p",[Message]),
  {noreply,State}.

terminate(Reason,_State)->
  ?LOGWARNING("terminating schema server reason ~p",[Reason]),
  ?SCHEMA_CLOSE,
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%=================================================================
%%	Schema initialization
%%=================================================================
try_load()->
  case ?schemaExists of
    true->
      ?LOGINFO("schema initialization"),
      schema_open(),
      close_dbs(),
      restart(?allNodes);
    _->
      schema_create(),
      case os:getenv("ATTACH_TO") of
        false->
          ?SCHEMA_ID({node(), erlang:system_time( microsecond ), erlang:unique_integer()}),
          ?LOGINFO("single node first start");
        Node->
          try_attach_to([list_to_atom( Node )])
      end
  end.

schema_create()->
  try ?SCHEMA_CREATE
  catch
    _:E:S->
      ?LOGERROR("CRITICAL ERROR! UNABLE TO CREATE SCHEMA ERROR ~p STACK ~p",[E,S]),
      ?LOGINFO("check schema path: \r\n"
      ++" ~p\r\n"
        ++" is available, check acces for writing and try to start again.\r\n"
        ++" if the schema moved to another path, close the application edit config file, and try to start again",
        [ ?schemaPath ]),
      timer:sleep( ?infinity )
  end.

schema_open()->
  try ?SCHEMA_OPEN
  catch
    _:E:S->
      ?LOGERROR("CRITICAL ERROR! UNABLE TO OPEN SCHEMA \r\n~p\r\n ERROR ~p STACK ~p",[?schemaPath,E,S]),
      ?LOGINFO("close the application, fix the problem and try start again"),
      timer:sleep( ?infinity )
  end.

close_dbs()->
  Node = node(),
  [ ?CLOSE_DB(DB, Node) || DB <- ?nodeDBs(node()) ],
  ok.

restart([Node]) when Node =:= node()->
  ?LOGINFO("single node application restart"),
  [ zaya_db_srv:open( DB ) || DB <- ?nodeDBs(node()) ],
  ok;
restart([])->
  ?LOGINFO("single application empty node restart"),
  ok;
restart(Nodes)->
  ?LOGINFO("multi-node application restart, nodes ~p",[Nodes]),
  try_attach_to( ordsets:from_list(Nodes -- [node()]) ).

try_attach_to([Node|Rest])->
  ?LOGINFO("trying to connect to ~p node",[Node]),
  case net_adm:ping( Node ) of
    pong->
      ?LOGINFO("~p node is available, trying to get the schema",[Node]),
      case get_schema_from( Node ) of
        {?schema, RemoteSchemaID, Schema}->
          LocalSchemaID = ?schemaId,
          if
            LocalSchemaID =/= ?undefined, LocalSchemaID =/= RemoteSchemaID->
              ?LOGWARNING("~p node has differenet schema ID, local ~p, remote ~p",[Node, LocalSchemaID, RemoteSchemaID]),
              try_attach_to( Rest );
            true->
              ?LOGINFO("try to recover by schema from ~p node",[Node]),
              SchemaBackup = ?getSchema,
              try recover_by_schema(Schema)
              catch
                _:E:S->
                  ?LOGERROR("error to recover from node ~p:\r\n"
                  ++"error ~p, stack ~p",[Node,E,S]),

                  % We need to interrupt already started DBs
                  [ zaya_db_srv:close( DB ) || DB <- ?nodeDBs(node()) ],

                  ?SCHEMA_CLEAR,
                  ?SCHEMA_LOAD(SchemaBackup),

                  CorruptedSchemaPath =
                    ?schemaPath++"/"++atom_to_list(Node)++".corrupted_schema",

                  file:write_file(CorruptedSchemaPath,term_to_binary(Schema)),

                  ?LOGINFO("please send file:\r\n ~p\r\n and logs to your support",[CorruptedSchemaPath]),
                  try_attach_to(Rest)
              end
          end;
        {error, Error}->
          ?LOGERROR("~p node schema request error ~p",[Node, Error]),
          try_attach_to(Rest)
      end;
    pang->
      ?LOGINFO("~p node is not available",[Node]),
      try_attach_to( Rest )
  end;
try_attach_to([])->
  case os:getenv("ATTACH_TO") of
    false->
      case ?readyNodes -- [node()] of
        []->
          ?LOGINFO("multi node application full restart"),
          [ zaya_db_srv:open( DB ) || DB <- ?nodeDBs(node()) ],
          ok;
        ReadyNodes->
          case os:getenv("FORCE_START") of
            "true"->
              ?LOGWARNING("FORCE RESTART"),
              [ ?NODE_DOWN(N) || N <- ?allNodes],
              [ zaya_db_srv:open( DB ) || DB <- ?nodeDBs(node()) ],
              ok;
            _->
              ?LOGWARNING("There were active nodes ~p, when the node went down. They might have more actual data.\r\n"
              ++"Try to restart them first. If those nodes are lost or this node has the latest data you can restart it with:\r\n"
                ++" env FORCE_START=true <your application>\r\n"
                ++"ATTENTION! All the latest data on other nodes will be lost.",[ReadyNodes]),
              timer:sleep(5000),
              try_attach_to(?allNodes)
          end
      end;
    Node->
      ?LOGERROR("unable to attach to ~p, close application, fix the problem and try start again",[Node]),
      timer:sleep(?infinity)
  end.

get_schema_from( Node )->
  try gen_server:call({?MODULE, Node}, {attach_request, node(),?MODULE}, 5000)
  catch
    _:Error->
      {error,Error}
  end.

recover_by_schema(Schema)->
  OldSchema =
    lists:foldl(fun(DB, Acc)->
      Acc#{DB =>#{
        module => ?dbModule( DB ),
        params => ?dbNodeParams(DB, node()),
        nodes => ?dbAllNodes( DB )
      }}
    end,#{}, ?allDBs ),

  ?SCHEMA_CLEAR,
  ?SCHEMA_LOAD(Schema),

  % Remove stale DBs
  [ case ?dbModule(DB) of
      ?undefined ->
        ?LOGINFO("~p database was removed, remove local copy",[ DB ]),
        try Module:remove( zaya_db_srv:default_params(DB,Params) )
        catch
          _:E->
            ?LOGERROR("~p remove stale database error ~p",[ DB, E ])
        end;
      _->
        not_stale
    end || {DB,#{ module:=Module, params:=Params }} <- maps:to_list( OldSchema )],

  merge_schema(?allDBs, OldSchema ),

  ok.

merge_schema([DB|Rest],OldSchema)->
  case {?dbNodeParams(DB,node()), maps:get(DB, OldSchema, ?undefined) } of
    {?undefined, #{module := Module, params:=Params } } when Params=/=?undefined->
      ?LOGINFO("~p local copy was removed, try remove",[DB]),
      try Module:remove( zaya_db_srv:default_params(DB, Params) )
      catch
        _:E->?LOGERROR("~p remove local copy error ~p",[DB,E])
      end;
    {?undefined,_}->
      no_local_copy;
    {Params, ?undefined}->
      ?LOGINFO("~p add local copy",[DB]),
      spawn(fun()->zaya_db_srv:add_copy(DB, Params) end);
    {Params, #{params:=?undefined}}->
      ?LOGINFO("~p add local copy",[DB]),
      spawn(fun()->zaya_db_srv:add_copy(DB, Params) end);
    _->
      zaya_db_srv:open( DB )
  end,
  merge_schema(Rest, OldSchema);
merge_schema([], _OldSchema)->
  ok.

%%===========================================================================
%%  Network recovery (split brain)
%%===========================================================================
% When the network is recovered each recovered node <--> node  connection will
% trigger the split brain procedure.
% There is no need to merge node's states because they are updated automatically when
% the connection recovered. The task is to merge databases.
merge_brain( Node )->
  case rpc:call( Node, zaya_node, node_dbs_params, [Node] ) of
    {badrpc,Error}->
      ?LOGERROR("~p node merge brain error ~p",[Node,Error]);
    NodeDBs->
      Remote = maps:from_list( NodeDBs ),
      Local = maps:from_list( ?nodeDBsParams( Node ) ),

      % Remove stale
      maps:map(fun(DB,_Ps)->
        case maps:is_key(DB, Remote ) of
          true -> ignore;
          _->
            try ?REMOVE_DB_COPY( DB,Node )
            catch
              E->?LOGERROR("~p remove copy ~p error ~p",[DB,Node,E])
            end
        end
      end, Local ),

      % Add/Update
      maps:map(fun(DB,Ps)->
        case Local of
          #{ DB := Ps }->
            ignore;
          #{DB:=_}->
            try ?SCHEMA_WRITE({db,DB,'@node@',Node,'@params@'},Ps)
            catch
              _:E->?LOGERROR("~p update ~p copy params error ~p",[DB,Node,E])
            end;
          _->
            try ?ADD_DB_COPY( DB,Node, Ps )
            catch
              E->?LOGERROR("~p add copy ~p error ~p",[DB,Node,E])
            end
        end
      end, Remote ),

      [ zaya_db_srv:split_brain( DB ) || DB <- ?nodeDBs(Node)]

  end.








