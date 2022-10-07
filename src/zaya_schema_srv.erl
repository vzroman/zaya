
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
  remove_db_copy/2
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
  gen_server:call(?MODULE, {node_up, Node, Info}).

node_down( Node, Info )->
  gen_server:call(?MODULE, {node_down, Node, Info}).

remove_node( Node )->
  gen_server:call(?MODULE, {remove_node, Node}).

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

  {reply,{?schema,?getSchema},State};

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
    ?CLOSE_DB(DB, Node),
    gen_server:reply(From,ok),
    ?LOGINFO("~p db closed",[DB])
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
      log_open(),
      restart(?allNodes);
    _->
      schema_create(),
      log_create(),
      case os:getenv("ATTACH_TO") of
        false->
          ?LOGINFO("single node first start");
        Node->
          try_attach_to([list_to_atom( Node )])
      end
  end.

schema_create()->
  try
    ?SCHEMA_CREATE,
    ?SCHEMA_OPEN
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

log_create()->
  try zaya_transaction:create_log( ?transactionLogPath )
  catch
    _:E:S->
      ?LOGERROR("CRITICAL ERROR! UNABLE TO CREATE TRANSACTION LOG ERROR ~p STACK ~p",[E,S]),
      ?LOGINFO("check transactiuon log path: \r\n"
      ++" ~p\r\n"
        ++" is available, check acces for writing and try to start again.\r\n"
        ++" if the transaction log moved return it back, close the application and try to start again",
        [ ?transactionLogPath ]),
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

log_open()->
  try zaya_transaction:open_log( ?transactionLogPath )
  catch
    _:E:S->
      ?LOGERROR("CRITICAL ERROR! UNABLE TO OPEN TRANSACTION \r\n~p\r\n ERROR ~p STACK ~p",[?transactionLogPath,E,S]),
      ?LOGINFO("close the application, fix the problem and try start again"),
      timer:sleep( ?infinity )
  end.

restart([Node]) when Node =:= node()->
  ?LOGINFO("single node application restart"),
  [ zaya_db_srv:open( DB ) || DB <- ?nodeDBs(node()) ],
  ok;
restart([])->
  ?LOGINFO("single application empty node restart"),
  ok;
restart(Nodes)->
  ?LOGINFO("multi-node application restart, nodes ~p",[Nodes]),
  try_attach_to( Nodes -- [node()] ).

try_attach_to([Node|Rest])->
  ?LOGINFO("trying to connect to ~p node",[Node]),
  case net_adm:ping( Node ) of
    pong->
      ?LOGINFO("~p node is available, trying to get the schema",[Node]),
      case get_schema_from( Node ) of
        {?schema,Schema}->
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
          end;
        {error,Error}->
          ?LOGERROR("~p node schema request error ~p",[Node,Error]),
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
        try Module:remove( Params )
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
      zaya_transaction:drop_log( DB ),
      try Module:remove( Params )
      catch
        _:E->?LOGERROR("~p remove local copy error ~p",[DB,E])
      end;
    {?undefined,_}->
      no_local_copy;
    {Params, ?undefined}->
      ?LOGINFO("~p add local copy",[DB]),
      zaya_db_srv:add_copy(DB, Params);
    {Params, #{params:=?undefined}}->
      ?LOGINFO("~p add local copy",[DB]),
      zaya_db_srv:add_copy(DB, Params);
    _->
      case ?dbAllNodes(DB) of
        [Node] when Node =:= node()->
          ?LOGINFO("~p has only local copy, open",[ DB ]),
          zaya_db_srv:open( DB );
        Nodes->
          ?LOGINFO("~p has copies at ~p nodes which can have more actual data, try to recover",[DB, Nodes--[node()]]),
          zaya_db_srv:recover( DB )
      end
  end,
  merge_schema(Rest, OldSchema);
merge_schema([], _OldSchema)->
  ok.

%%===========================================================================
%%  Network recovery (split brain)
%%===========================================================================
% When the network is recovered each recovered node <--> node  connection will
% trigger the split brain procedure.
% There is no need to merge node's states because them updated automatically when
% the connection recovered. Here we need just to remove 'stale' nodes.
%
% The task is to merge databases. On a node <--> node connection recovery
% the nodes exchange their schemas and do their databases merge.
%-----------Database merge-----------------------------
% Cases:
% * Database is not known (it was added on the other node) or has no local copy.
%   Add it's: '@module@', '@nodes@', '@masters@', '@ref@'s, '@params@'s
% * Database was removed.
% * Database has local copy. Recovery.
%---------Database recovery----------------------------
% Who is the master. :
% * '@masters@' is not defined nor locally either remotely. The master is the node with a smaller name.
% * '@masters@' is defined
% The master is the node with smaller index in the:
%   '@masters@' -- ('@nodes@' -- [Node]) ++ (('@nodes@' ++ [Node]) -- '@masters@')
%
% Cases are:
% * if node() < Node
%  * database was added or updated to the Node (update: '@module@', '@params@', '@ref@', '@nodes@','@masters@')
%  * databases was removed from the Node (update: '@params@', '@ref@', '@nodes@','@masters@', remove if no other copies)
% From this point node() is not in the database's '@nodes@'.
% If the database has no local copy then all reads and writes are switched to new '@nodes@'.
% If the database has local copy then it enters the 'recovery' state:
% * all database writes are switched to '@nodes@'
% * dirty reads go to the local copy as {'@ref@', node()} is alive
% * transactional reads go to the '@nodes@'
%  * database has no local copy - ignore
%  * node() is not in the '@nodes@' (local copy is not ready) - ignore
%  * node() is in the '@nodes@' (local copy copy is ready):
%   - set '@nodes@' [Node]. It will stop all the writes to the local copy
merge_brain( Node )->
  case get_schema_from( Node ) of
    {?schema,Schema}->
      NodeKnownDBs =
        lists:foldl(fun({Key, Value}, Acc)->
          case Key of
            {db,DB,'@module@'} ->
              DBSchema = maps:get(DB,Acc,#{}),
              Acc#{ DB => DBSchema#{ '@module@' => Value } };
            {db,DB,'@nodes@'}->
              DBSchema = maps:get(DB,Acc,#{}),
              Acc#{ DB => DBSchema#{ '@nodes@' => Value } };
            {db,DB,'@masters@'}->
              DBSchema = maps:get(DB,Acc,#{}),
              Acc#{ DB => DBSchema#{ '@masters@' => Value } };
            {db,DB,'@ref@',N} when N=:=Node->
              DBSchema = maps:get(DB,Acc,#{}),
              Acc#{ DB => DBSchema#{ '@ref@' => Value } };
            {db,DB,'@node@',N,'@params@'} when N=:=Node->
              DBSchema = maps:get(DB,Acc,#{}),
              Acc#{ DB => DBSchema#{ '@params@' => Value } };
            _->
              Acc
          end
        end,#{},Schema),

      NodeDBs =
        maps:filter(fun(_DB,DBSchema)->
          maps:is_key('@params@', DBSchema )
        end, NodeKnownDBs),

      merge_databases(maps:to_list(NodeDBs), Node ),

      % TODO. Remove stale nodes
      ok;
    {error,Error}->
      ?LOGERROR("~p node schema request error ~p",[Node,Error])
  end.

merge_databases([{DB,Config}|Rest], Node )->
  case ?dbNodeParams(DB,node()) of
    ?undefined->
      % Database has no local copy
      update_db_config(DB, Node, Config);
    _->
      case lists:member(node(),?dbAvailableNodes(DB)) of
        true ->
          % The database is in the conflict state
          merge_db_config(DB,Node,Config);
        _->
          % The local copy is not ready yet
          update_db_config(DB, Node, Config)
      end
  end,
  merge_databases(Rest, Node);
merge_databases([],_Node)->
  ok.

update_db_config(DB, Node, Config)->
  maps:map(fun(Key, Value)->
    try
      case Key of
        '@module@' -> ?SCHEMA_WRITE({db,DB,'@module@'}, Value );
        '@nodes@' -> ?SCHEMA_WRITE({db,DB,'@nodes@'}, Value );
        '@masters@' -> ?SCHEMA_WRITE({db,DB,'@masters@'}, Value );
        '@ref@'-> ?SCHEMA_WRITE({db,DB,'@ref@',Node}, Value );
        '@params@' -> ?SCHEMA_WRITE({db,DB,'@node@',Node,'@params@'}, Value );
        _->ignore
      end
    catch
      _:E->
        ?LOGERROR("~p database merge config ~p with node ~p error ~p",[DB,Key,Node,E])
    end
  end, Config).

merge_db_config(DB,Node,Config)->

  % Node private configs
  update_db_config(DB,Node, maps:with(['@ref@','@params@'], Config)),

  % If DB '@masters@' are also in the conflict state the node with a smaller name has priority
  if
    Node < node()->
      update_db_config(DB,Node, maps:with(['@masters@'], maps:merge(#{'@masters@'=>[]},Config)) );
    _->
      accept_local
  end,

  case ?dbMasters(DB) of
    []->
      % By default the node with a smaller name is the master
      if
        Node < node()->
          recover_db(DB, maps:get('@nodes',Config,[]));
        true ->
          ?LOGINFO("~p database conflict with ~p node, accept local copy",[DB,Node])
      end;
    Masters->
      case {lists:member(node(),Masters),lists:member(Node,Masters)} of
        {true,false}->
          ?LOGINFO("~p database conflict with ~p node, accept local copy",[DB,Node]);
        {false,true}->
          recover_db(DB, Node);
        {true,true}->
          if
            Node < node()->
              recover_db(DB, Node);
            true ->
              ?LOGINFO("~p database conflict with ~p node, accept local copy",[DB,Node])
          end;
        {false,false}->
          todo
      end
  end.


recover_db( DB, Nodes )->
  case lists:member(node(),?dbAvailableNodes(DB)) of
    true->
      try
        ?SCHEMA_WRITE({db,DB,'@nodes@'}, Nodes ),
        zaya_db_srv:recover(DB)
      catch
        _:E->
          ?LOGERROR("~p database recovery error, unable to update schema with available copies ~p, error ~p",[DB,Nodes,E])
      end;
    _->
      % The database is either closed and will be recovered during on open
      % or is under recovery already
      ok
  end.







