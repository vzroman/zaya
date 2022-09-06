
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
  remove_node/1
]).

%%=================================================================
%%	dbS API
%%=================================================================
-export([
  add_db/2,
  open_db/2,
  close_db/1,
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

remove_node( Node )->
  gen_server:call(?MODULE, {remove_node, Node}).

%%=================================================================
%%	DBs
%%=================================================================
add_db(DB,Module)->
  gen_server:call(?MODULE, {add_db, DB, Module}, ?infinity).

open_db(DB,Ref)->
  gen_server:call(?MODULE, {open_db, DB, Ref}, ?infinity).

close_db(DB)->
  gen_server:call(?MODULE, {close_db, DB}, ?infinity).

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

-record(state,{ nodes }).
init([])->

  process_flag(trap_exit,true),

  ?LOGINFO("starting schema server ~p",[self()]),

  try try_load()
  catch
    _:E:S->
      ?LOGERROR("LOAD ERROR! ~p stack ~p\r\n"
      ++" please check error logs, fix the problem and start again",[E,S]),
      timer:sleep( ?infinity )
  end,

  case ecall:call_all(?readyNodes,gen_server,call,[{add_node, ?MODULE, node(), self()}] ) of
    {ok,_}->
      ?NODE_UP( node() ),
      ?LOGINFO("node is ready")
  end,

  {ok,#state{ nodes = #{} }}.

%%=================================================================
%%	CALL NODES
%%=================================================================
handle_call({attach_request, Node, ?MODULE}, _From, State) ->

  ?LOGINFO("attach request from node ~p",[Node]),

  {reply,{?schema,?getSchema},State};

handle_call({add_node, ?MODULE, Node, NodeSchemaServer}, From, #state{nodes = LinkedNodes} =State) ->

  NewState =
  try
    ?NODE_UP( Node ),
    ?LOGINFO("~p node added to schema",[Node]),
    link(NodeSchemaServer),
    gen_server:reply(From,ok),
    State#state{ nodes = LinkedNodes#{NodeSchemaServer=>Node}}
  catch _:E:S->
    gen_server:reply(From, {error,E}),
    ?LOGERROR("~p node add to schema error ~p stack ~p",[Node,E,S]),
    State
  end,

  {noreply,NewState};


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

handle_call({open_db, DB, Ref}, From, State) ->

  try
    ?OPEN_DB(DB,Ref),
    gen_server:reply(From,ok),
    ?LOGINFO("~p db opened, ref ~p",[DB, Ref])
  catch
    _:E:S->
      gen_server:reply(From, {error,E}),
      ?LOGERROR("~p db open schema error ~p stack ~p",[DB,E,S])
  end,

  {noreply,State};

handle_call({close_db, DB}, From, State) ->

  try
    ?CLOSE_DB(DB),
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

handle_cast(Request,State)->
  ?LOGWARNING("schema server got an unexpected cast resquest ~p",[Request]),
  {noreply,State}.

handle_info({'EXIT',NodeServer, Reason},#state{nodes = Nodes}= State)->
  case maps:take(NodeServer,Nodes) of
    {Node, RestNodes}->
      ?LOGWARNING("~p node down, reason ~p",[Node,Reason]),
      try ?NODE_DOWN(Node)
      catch
        _:E:S->
          ?LOGERROR("schema update error ~p, stack ~p",[E,S])
      end,
      {noreply,State#state{nodes = RestNodes}};
    _->
      ?LOGWARNING("unexpected trap exit process ~p, reason ~p",[NodeServer,Reason]),
      {noreply,State}
  end;

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
      try ?SCHEMA_OPEN
      catch
        _:E:S->
          ?LOGERROR("CRITICAL ERROR! UNABLE TO OPEN SCHMA \r\n~p\r\n ERROR ~p STACK ~p",[?schemaPath,E,S]),
          ?LOGINFO("close the application, fix the problem and try to recover from backup"),
          timer:sleep( ?infinity )
      end,

      make_schema_backup(),

      case ?readyNodes -- [node()] of
        []->
          case ?allNodes -- [node()] of
            []->
              ?LOGINFO("SINGLE NODE RESTART"),
              ok;
            OtherNodes->
              ?LOGINFO("FULL RESTART: nodes to reattach",[OtherNodes]),
              ok
          end;
        _OtherReadyNodes->
          OldSchema = ?getSchema,
          SchemaBackupPath =
            filename:absname(?schemaDir)++"/"++list_to_atom(node())++".schema_backup",
          file:write_file(SchemaBackupPath,term_to_binary(OldSchema)),
          attach_from_node()
      end;
    _->
      try
        ?SCHEMA_CREATE,
        ?SCHEMA_OPEN
      catch
        _:E:S->
          ?LOGERROR("CRITICAL ERROR! UNABLE TO CREATE SCHMA ERROR ~p STACK ~p",[E,S]),
          ?LOGINFO("check schema path: \r\n"
            ++" ~p\r\n"
            ++" is available, check acces for writing and try to start again.\r\n"
            ++" if the schema moved to another path, close the application edit config file, and try to start again",
            [ ?schemaPath ]),
          timer:sleep( ?infinity )
      end,
      case load_from_backup() of
        {ok, Backup}->
          ?LOGINFO("try to recover from backup:\r\n "++Backup),
          ?LOAD_SCHEMA_FROM_BACKUP(Backup),
          attach_from_node();
        no->
          first_start_dialog();
        reattach->
          attach_from_node()
      end,
      ok
  end.

first_start_dialog()->
  ?LOGINFO("clear start"),
  case yes_or_no("Do you want to attach this node to existing application?") of
    yes->
      attach_from_node();
    no->
      case yes_or_no("is it a single node first start?") of
        yes ->
          ?LOGINFO("single node first start"),
          _NewSchema = ?undefined;
        no->
          attach_from_node()
      end
  end.

attach_from_node()->
  OldSchema = ?getSchema,
  try try_attach_to( ?allNodes --[node()], OldSchema )
  catch
    _:E:S->
      ?LOGERROR("recovery unexpected error ~p, stack ~p",[E,S]),
      ?SCHEMA_CLEAR,
      ?SCHEMA_LOAD(OldSchema),
      attach_from_node()
  end.

try_attach_to([Node|Rest], OldSchema)->
  ?LOGINFO("trying to get schema from ~p node",[Node]),
  case net_adm:ping( Node ) of
    pong->
      ?LOGINFO("~p node is available, trying to get the schema",[Node]),
      case get_schema_from( Node ) of
        {?schema,Schema}->
          ?LOGINFO("try to recover by schema from ~p node",[Node]),
          try recover_by_schema(Schema, OldSchema)
          catch
            _:E:S->
              ?LOGERROR("error to recover from node ~p schema ~p\r\n"
              ++"error ~p, stack ~p",[Node,Schema,E,S]),

              ?SCHEMA_CLEAR,

              CorruptedSchemaPath =
                filename:absname(?schemaDir)++"/"++list_to_atom(Node)++".corrapted_schema",

              file:write_file(CorruptedSchemaPath,term_to_binary(Schema)),

              ?LOGINFO("please send file:\r\n ~p\r\n and logs to your support",[CorruptedSchemaPath]),
              try_attach_to(Rest, OldSchema)
          end;
        {error,Error}->
          ?LOGERROR("~p node schema request error ~p",[Node,Error]),
          try_attach_to(Rest, OldSchema)
      end;
    pang->
      ?LOGINFO("~p node is not available",[Node]),
      try_attach_to( Rest, OldSchema )
  end;
try_attach_to([], OldSchema)->
  ?LOGINFO("there are no known nodes to get schema from"),
  Node = attach_node_dialog(),
  try_attach_to([Node], OldSchema).

attach_node_dialog()->
  case io:get_line("type the node name to attach to >") of
    NodeName when is_list(NodeName)->
      list_to_atom( NodeName );
    {error,Error}->
      ?LOGERROR("io error ~p",[Error]),
      attach_node_dialog();
    Other->
      ?LOGERROR("unexpected io result ~p",[Other]),
      attach_node_dialog()
  end.

get_schema_from( Node )->
  try gen_server:call({?MODULE, Node}, {attach_request, node(),?MODULE}, 5000)
  catch
    _:Error->
      {error,Error}
  end.

recover_by_schema({?schema, Schema}, OldSchema)->
  OldSchema = ?getSchema,
  ?LOGINFO("node recovery by schema ~p\r\n old schema ~p",[Schema,OldSchema]),
  ?SCHEMA_CLEAR,
  ?SCHEMA_LOAD(Schema),
  localDBs =
    [S || {{sgm,S,'@node@',N,'@params@'},_} <- OldSchema, N=:=node()],
  merge_schema(localDBs),
  ok.

merge_schema([DB|Rest])->
  case ?dbAllNodes( DB ) of
    []->
      ?LOGINFO("~p db was removed, remove local copy",[DB]),
      try
        Module = ?dbModule( DB ),
        LocalParams = ?dbNodeParams(DB, node() ),
        Module:remove( DB, LocalParams ),
        ?LOGINFO("~p local copy removed",[DB])
      catch
        _:E:S->
          ?LOGERROR("~p remove local copy error ~p stack ~p",[DB,E,S])
      end;
    _dbNodes->
      case ?dbReadyNodes(DB) of
        []->
          case ?dbNotReadyNodes( DB )--[node()] of
            []->
              ?LOGINFO("~p has only local copy, continue");
            OtherNotReadyNodes->
              ?LOGWARNING("~p HAS COPIES at ~p NODES THAT ARE NOT READY NOW! This nodes can have more fresh data", [DB,OtherNotReadyNodes]),
              case confirm_copy_dialog( DB, OtherNotReadyNodes ) of
                yes->
                  ?LOGWARNING("~p local copy accepted as the latest, continue");
                no->
                  ?LOGINFO("close the application, start the nodes with the latest data first and then try to start this node again"),
                  timer:sleep(?infinity)
              end
          end;
        dbReadyNodes->
          ?LOGINFO("~p db has copies at ~p nodes, try to recover",[DB,?dbReadyNodes(DB)]),
          try
            zaya_db:try_recover( DB ),
            ?LOGINFO("~p db recovered")
          catch
            _:E:S->
              ?LOGERROR("~p RECOVERY ERROR ~p stack ~p",[DB,E,S])
          end
      end
  end,
  merge_schema(Rest);
merge_schema([])->
  ok.

confirm_copy_dialog( DB, Nodes )->
  io:format("~p db copy nodes:",[Nodes]),
  [ io:format("\r\n  "++atom_to_list(N)) || N <- Nodes],
  yes_or_no("is the local copy of "++atom_to_list(DB)++" the latest:").

make_schema_backup()->
  DT = unicode:characters_to_binary(calendar:system_time_to_rfc3339(erlang:system_time(millisecond),[{unit,millisecond},{offset,"Z"}])),
  ?makeSchemaBackup(?schemaDir++".zaya.SCHEMA.BACKUP."++DT).

load_from_backup()->
  case yes_or_no("schema not found, load from backup?") of
    no-> no;
    yes->
      case find_backup() of
        []->
          ?LOGWARNING("backup files not found"),
          case yes_or_no("is the schema dirrectory:\r\n "++?schemaDir++"\r\ncorrect?") of
            no->
              ?LOGINFO("fix schema directory on the config file, and try start again"),
              timer:sleep( ?infinity);
            yes->
              ?LOGWARNING("close the application, try to find lost backup files, copy them into schema dirrectory and start again\r\n"
              ++"or you can try to reattach the node of there are any other ready nodes"),
              case yes_or_no("try to reattach?") of
                yes->
                  reattach;
                no->
                  ?LOGINFO("close the application"),
                  timer:sleep(?infinity)
              end
          end;
       [Backup]->
          case yes_or_no("only backup found "++Backup++" try to load from it?") of
            yes->
              {ok,?schemaDir++"/"++Backup};
            no->
              io:format("if there are other ready nodes you can try to reattach this node\r\n"),
              case yes_or_no("try to reattach?") of
                yes->
                  reattach;
                no->
                  no
              end
          end;
       Backups ->
          choose_backup_dialog(Backups)
      end
  end.

find_backup()->
  case file:list_dir(?schemaDir) of
    {error,_}->
      [];
    {ok,Backups}->
      lists:reverse( lists:usort(Backups) )
  end.

choose_backup_dialog([Latest|_]=Backups)->
  io:format("found backups:"),
  [ io:format(integer_to_list(I)++". ~s",[B]) || {I,B} <- lists:zip(lists:seq(1,length(Backups)), Backups) ],
  io:format("it's strictly recomended to load from the latest"),
  case yes_or_no("load from the "++Latest) of
    yes->
      Backup = ?schemaDir++"/"++Latest,
      ?LOGINFO("loading from backup ~p",[Backup]),
      {ok,Backup};
    no->
      case io:get_line( "type the number of backup and press enter >" ) of
        Chosen when is_list(Chosen) ->
          N =
          try list_to_integer(string:replace(Chosen,"\n",""))
          catch
            _:_->
              io:format("invalid input ~p, try again",[Chosen]),
              choose_backup_dialog(Backups)
          end,
          if
            length(Backups) >= N->
              Backup = lists:nth(N,Backups),
              {ok,Backup}
          end;
        Unexpected->
          ?LOGWARNING("unexpected result ~p, try again",[Unexpected]),
          choose_backup_dialog(Backups)
      end
  end.


yes_or_no(Question)->
  Yes ="yes\n",
  No = "no\n",
  ?LOGINFO("type yes or no and press Enter"),
  case io:get_line( Question ++" >" ) of
    Yes ->
      yes;
    No->
      no;
    _Other->
      ?LOGINFO("unable to understand you reply, please type yes or no again and press Enter"),
      yes_or_no( Question )
  end.











