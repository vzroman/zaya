
-module(zaya_db_srv).

-include("zaya.hrl").
-include("zaya_schema.hrl").

-behaviour(gen_statem).

%%=================================================================
%%	API
%%=================================================================
-export([
  create/3,
  open/1,
  add_copy/2,
  remove_copy/1,
  force_load/1,
  close/1,
  remove/1,
  set_readonly/2,
  split_brain/1,

  get_size/1,
  default_params/2
]).
%%=================================================================
%%	OTP API
%%=================================================================
-export([
  callback_mode/0,
  start_link/2,
  init/1,
  code_change/3,
  handle_event/4,
  terminate/3
]).

%%=================================================================
%%	API
%%=================================================================
create( DB, Module, InParams)->
  epipe:do([
    fun(_)->
      zaya_schema_srv:add_db(DB,Module),
      maps:get(node(), InParams, ?undefined)
    end,
    fun
      (_NodeParams = ?undefined)->
        added;
      (NodeParams)->
        case supervisor:start_child(zaya_db_sup,[DB, {create, NodeParams, _ReplyTo=self()}]) of
          {ok, PID}->
            receive
              {created, PID}->
                created;
              {error,PID,Error}->
                {error,Error}
            end;
          Error->
            Error
        end
    end
  ], ?undefined ).

open( DB )->
  case supervisor:start_child(zaya_db_sup,[DB, open]) of
    {ok, _PID}->
      ok;
    Error->
      Error
  end.

force_load( DB )->
  gen_statem:cast(DB, force_load).

add_copy( DB, Params )->
  case supervisor:start_child(zaya_db_sup,[DB, {add_copy,Params,_ReplyTo=self()}]) of
    {ok, PID} when is_pid( PID )->
      receive
        {added,PID}->
          ok;
        {error,PID,Error}->
          {error,Error}
      end;
    Error->
      Error
  end.

remove_copy( DB )->
  case whereis(DB) of
    PID when is_pid( PID ) ->
      timer:sleep( 100 ),
      remove_copy( DB );
    _->
      Module = ?dbModule( DB ),
      Params = ?dbNodeParams(DB,node()),
      epipe:do([
        fun(_) -> ecall:call_all(?readyNodes, zaya_schema_srv, remove_db_copy, [DB, node()] ) end,
        fun(_)-> Module:remove( default_params(DB,Params) ), {ok,ok} end
      ],?undefined)
  end.

close( DB )->
  case whereis( DB ) of
    PID when is_pid( PID )->
      gen_statem:cast(DB, close);
    _->
      {error, not_registered}
  end.

remove( DB )->
  case whereis(DB) of
    PID when is_pid( PID ) ->
      timer:sleep( 100 ),
      remove( DB );
    _->
      Module = ?dbModule( DB ),
      Params = ?dbNodeParams(DB,node()),
      epipe:do([
        fun(_) -> zaya_schema_srv:remove_db( DB ) end,
        fun
          (_) when Params =:= ?undefined->ok;
          (_)-> Module:remove( default_params(DB,Params) )
        end
      ], ?undefined)
  end.

set_readonly( DB, IsReadOnly )->
  {ok, Unlock} = elock:lock( ?locks, DB, _IsShared = false, _Timeout = infinity, [node()]),
  try zaya_schema_srv:set_db_readonly( DB, IsReadOnly )
  after
    Unlock
  end.


split_brain(DB)->
  spawn(fun()->merge_brain( DB ) end),
  ok.

get_size(DB)->
  Module = ?dbModule(DB),
  Ref = ?dbRef( DB, node() ),
  Module:get_size( Ref ).

%%=================================================================
%%	OTP
%%=================================================================
callback_mode() ->
  [
    handle_event_function
  ].

start_link( DB, Action )->
  gen_statem:start_link({local,DB},?MODULE, [DB, Action], []).

-record(data,{db, module, ref}).
init([DB, State])->

  process_flag(trap_exit,true),

  ?LOGINFO("~p starting database server ~p, state ~p",[DB ,self(),State]),

  Module = ?dbModule( DB ),

  {ok, State, #data{db = DB, module = Module }, [ {state_timeout, 0, run } ]}.

%%---------------------------------------------------------------------------
%%   CREATE
%%---------------------------------------------------------------------------
handle_event(state_timeout, run, {create, Params, ReplyTo}, #data{db = DB, module = Module} = Data) ->
  try
    Ref = Module:create( default_params(DB, Params) ),
    ecall:call_all_wait(?readyNodes, zaya_schema_srv, add_db_copy,[ DB, node(), Params ]),
    if
      is_pid( ReplyTo ) -> catch ReplyTo ! {created, self()};
      true -> ignore
    end,
    ?LOGINFO("~p database created",[DB]),
    {next_state, register, Data#data{ref = Ref}, [ {state_timeout, 0, run } ] }
  catch
    _:E->
      ?LOGERROR("~p database create error ~p",[DB,E]),
      ReplyTo ! {error,self(),E},
      {stop, shutdown}
  end;

%%---------------------------------------------------------------------------
%%   OPEN
%%---------------------------------------------------------------------------
handle_event(state_timeout, run, open, #data{db = DB} = Data) ->

  update_masters( DB ),

  update_nodes( ?dbMasters(DB), DB ),

  case ?dbAllNodes( DB ) of
    [Node] when Node =:= node()->
      ?LOGINFO("~p database has single copy, open",[DB]),
      {next_state, try_open, Data, [ {state_timeout, 0, run } ] };
    Nodes->
      case ?dbReadOnly( DB ) of
        true ->
          Version = ?dbReadOnlyVersion(DB),
          case ?dbReadOnlyVersion(DB, node()) of
            Version ->
              ?LOGINFO("~p database open in read only mode",[DB]),
              {next_state, try_open, Data, [ {state_timeout, 0, run } ] };
            _->
              ?LOGINFO("~p has stale read only copy, try to recover from ~p nodes",[DB,Nodes--[node()]]),
              {next_state, recover, Data, [ {state_timeout, 0, run } ] }
          end;
        _->
          ?LOGINFO("~p has copies on ~p nodes which can have more actual data, try to recover",[DB,Nodes--[node()]]),
          {next_state, recover, Data, [ {state_timeout, 0, run } ] }
      end
  end;

handle_event(state_timeout, run, try_open, #data{db = DB, module = Module} = Data) ->

  try
    Ref = Module:open( default_params(DB, ?dbNodeParams(DB,node())) ),
    {next_state, register, Data#data{ ref = Ref }, [ {state_timeout, 0, run } ] }
  catch
    _:E->
      ?LOGERROR("~p database open error ~p",[DB,E]),
      {next_state, open, Data, [ {state_timeout, 5000, run } ] }
  end;

handle_event(state_timeout, run, register, #data{db = DB, ref = Ref} = Data) ->

  case lock(DB, _IsShared=false, _Timeout = 30000) of
    {ok, Unlock}->
      try
        ok = zaya_schema_srv:open_db(DB, node(), Ref),
        {OKs, Errs} = ecall:call_all_wait( ?readyNodes -- [node()], zaya_schema_srv, open_db, [ DB, node(), Ref ] ),
        ?LOGINFO("~p database registered at ~p nodes, errors at ~p nodes",[ DB, [N || {N,_} <- OKs], [N || {N,_} <- Errs] ]),

        {next_state, ready, Data}
      after
        Unlock()
      end;
    {error,LockError}->
      ?LOGINFO("~p database register lock error ~p, retry",[DB, LockError]),
      { keep_state_and_data, [ {state_timeout, 0, run } ] }
  end;

%%---------------------------------------------------------------------------
%%   ADD COPY
%%---------------------------------------------------------------------------
handle_event(state_timeout, run, {add_copy, Params, ReplyTo}, #data{db = DB, module = Module} = Data) ->
  try
    Ref = zaya_copy:copy( DB, Module, default_params(DB,Params), #{ live => not ?dbReadOnly(DB)}),
    ?LOGINFO("~p database copy added",[DB]),
    {next_state, {register_copy,Params,ReplyTo}, Data#data{ ref = Ref }, [ {state_timeout, 0, run } ] }
  catch
    _:E->
      ?LOGERROR("~p database add copy error ~p",[DB,E]),
      if
        is_pid( ReplyTo ) -> catch ReplyTo ! {error,self(),E};
        true -> ignore
      end,
      {stop, shutdown}
  end;

handle_event(state_timeout, run, {register_copy,Params,ReplyTo}, #data{db = DB} = Data) ->
  case ecall:call_all(?readyNodes, zaya_schema_srv, add_db_copy, [DB, node(), Params] ) of
    {ok,_}->
      ?LOGINFO("~p database copy registered",[DB]),
      if
        is_pid( ReplyTo ) -> catch ReplyTo ! {added, self()};
        true -> ignore
      end,
      {next_state, register, Data, [ {state_timeout, 0, run } ] };
    {error,Error}->
      ?LOGERROR("~p database register copy error ~p",[ DB, Error ]),
      { keep_state_and_data, [ {state_timeout, 5000, run } ] }
  end;

%%---------------------------------------------------------------------------
%%   RECOVER
%%---------------------------------------------------------------------------
handle_event(state_timeout, run, recover, #data{db = DB}=Data ) ->
  case ?dbAvailableNodes(DB)--[node()] of
    []->
      case os:getenv("FORCE_START") of
        "true"->
          ?LOGWARNING("~p database force open",[DB]),
          {next_state, try_open, Data, [ {state_timeout, 0, run } ] };
        _->
          ?LOGERROR("~p database recover error: database is unavailable.\r\n"++
            "If you sure that the local copy is the latest you can try to load it with:\r\n"++
            "  zaya:db_force_open(~p, ~p).\r\n" ++
            "Execute this command from erlang console at any attached node.\r\n"++
            "WARNING!!! All the data changes made in other nodes copies will be LOST!",[DB,DB,node()]),
          { keep_state_and_data, [ {state_timeout, 5000, run } ] }
      end;
    _->
      {next_state, recovery, Data, [ {state_timeout, 0, run } ] }
  end;
handle_event(cast, force_load, recover, #data{db = DB} = Data ) ->
  ?LOGWARNING("~p database FORCE LOAD. ALL THE DATA CHANGES MADE IN OTHER NODES COPIES WILL BE LOST!",[DB]),
  {next_state, try_open, Data, [ {state_timeout, 0, run } ] };

%%---------------------------------------------------------------------------
%%   DB IS READY
%%---------------------------------------------------------------------------
handle_event(cast, recover, ready, Data) ->
  {next_state, unregister, Data, [ {state_timeout, 0, recovery } ]};

handle_event(cast, close, ready, #data{db = DB}=Data) ->

  ?LOGINFO("~p database close",[DB]),
  {next_state, unregister, Data, [ {state_timeout, 0, close } ]};

%%---------------------------------------------------------------------------
%%   RECOVERY
%%---------------------------------------------------------------------------
handle_event(state_timeout, run, recovery, #data{db = DB, module = Module, ref = Ref}=Data ) ->

  case ?dbAvailableNodes(DB)--[node()] of
    []->
      ?LOGERROR("~p database recovery is impossible: no other copies are available"),
      { keep_state_and_data, [ {state_timeout, 5000, run } ] };
    _->
      ?LOGWARNING("~p database recovery",[DB]),
      % TODO. Hash tree
      Params = ?dbNodeParams(DB,node()),
      try
        if
          Ref =/=?undefined -> catch Module:close( Ref );
          true->ignore
        end,
        Module:remove( default_params(DB,Params) ),
        {next_state, {add_copy, Params, ?undefined}, Data#data{ref = ?undefined}, [ {state_timeout, 0, run } ] }
      catch
        _:E:S->
          ?LOGERROR("~p database recovery error ~p, stack ~p",[DB,E,S]),
          { keep_state_and_data, [ {state_timeout, 5000, run } ] }
      end
  end;
handle_event(cast, force_load, recovery, #data{db = DB} = Data ) ->
  ?LOGWARNING("~p database FORCE LOAD. ALL THE DATA CHANGES MADE IN OTHER NODES COPIES WILL BE LOST!",[DB]),
  {next_state, register, Data, [ {state_timeout, 0, run } ] };

%%---------------------------------------------------------------------------
%%   CLOSE
%%---------------------------------------------------------------------------
handle_event(state_timeout, NextState, unregister, #data{db = DB} = Data) ->

  case lock( DB, _IsShared=false, _Timeout = infinity ) of
    {ok, Unlock}->
      try
        ecall:call_all_wait(?readyNodes, zaya_schema_srv, close_db, [DB, node()]),
        {next_state, NextState, Data, [ {state_timeout, 0, run } ]}
      after
        Unlock()
      end;
    {error,LockError}->
      ?LOGINFO("~p database unregister lock error ~p, retry",[DB, LockError]),
      { keep_state_and_data, [ {state_timeout, 5000, NextState } ] }
  end;

handle_event(state_timeout, run, close, #data{db = DB, module = Module, ref = Ref}) ->
  try  Module:close(Ref)
  catch
    _:E->?LOGERROR("~p database close error ~p",[DB,E])
  end,
  {stop, shutdown};

handle_event(EventType, EventContent, _AnyState, #data{db = DB}) ->
  ?LOGWARNING("~p database server received unexpected event type ~p, content ~p",[
    DB,
    EventType, EventContent
  ]),
  keep_state_and_data.

terminate(Reason, _AnyState, #data{ db = DB, module = Module, ref = Ref })->

  ?LOGWARNING("~p terminating database server reason ~p",[DB,Reason]),
  case ?dbModule(DB) of
    ?undefined ->
      % DB was deleted
      ignore;
    _->
      ecall:call_all_wait(?readyNodes, zaya_schema_srv, close_db, [DB, node()])
  end,

  if
    Ref =/= ?undefined->
      catch Module:close( Ref );
    true->
      ignore
  end,

  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%------------------------------------------------------------------------------------
%%  Internals
%%------------------------------------------------------------------------------------
default_params(DB, Params )->
  Dir =
    case Params of
      #{ dir := [$.] } ->
        filename:absname(?schemaDir);
      #{ dir := [$.|RelativePath] } ->
        filename:absname(?schemaDir) ++"/"++ RelativePath;
      #{dir := AbsPath}->
        AbsPath;
      _->
        filename:absname(?schemaDir)
    end,

  Params#{ dir => Dir ++"/"++atom_to_list(DB) }.

lock( DB, IsShared, Timeout )->
  Nodes =
    if
      IsShared -> [node()];
      true -> ?dbAvailableNodes( DB )
    end,
  case elock:lock( ?locks, DB, IsShared, Timeout, Nodes) of
    {ok, Unlock} ->
      {ok, Unlock};
    {error, deadlock}->
      lock( DB, IsShared, Timeout );
    Error ->
      Error
  end.

%%------------------------------------------------------------------------------------
%%  SPLIT BRAIN:
%%
%%------------------------------------------------------------------------------------
merge_brain( DB )->

  update_masters( DB ),

  case update_nodes( DB ) of
    ok->
      % No need to recover local copy
      ok;
    recover->
      case whereis( DB ) of
        PID when is_pid( PID )->
          gen_statem:cast(DB, recover);
        _->
          db_is_not_opened
      end
  end.

update_masters( DB )->
  case update_masters( ?dbMasters(DB), DB ) of
    error->
      update_masters( ordsets:from_list(?allNodes), DB );
    ok->
      ok
  end.

update_masters([N|_Rest], _DB ) when N=:=node()->
  ok;
update_masters([N|Rest], DB )->
  case rpc:call( N, zaya_db, masters, [DB] ) of
    Masters when is_list(Masters)->
      zaya_schema_srv:set_db_masters( DB, Masters ),
      ok;
    _->
      update_masters( Rest, DB )
  end;
update_masters( [], _DB )->
  error.

update_nodes( DB )->
  case update_nodes( ?dbMasters(DB), DB ) of
    error ->
      update_nodes( ordsets:from_list(?dbAllNodes(DB)), DB );
    OkOrRecover->
      OkOrRecover
  end.

update_nodes( [N|_Rest], _DB ) when N=:=node()->
  ok;
update_nodes( [N|Rest], DB )->
  case rpc:call( N, zaya_db, available_nodes, [DB] ) of
    Nodes  when is_list(Nodes)->
      case {ordsets:from_list( Nodes ), ordsets:from_list(?dbAvailableNodes(DB))} of
        {Same,Same}->
          ok;
        _->
          zaya_schema_srv:set_db_nodes( DB, Nodes ),
          recover
      end;
    _->
      update_nodes( Rest, DB )
  end;
update_nodes([], _DB )->
  error.

