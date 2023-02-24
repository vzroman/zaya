
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
  split_brain/1,

  get_size/1
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
  Module = ?dbModule( DB ),
  Params = ?dbNodeParams(DB,node()),
  epipe:do([
    fun(_) -> ecall:call_all(?readyNodes, zaya_schema_srv, remove_db_copy, [DB, node()] ) end,
    fun(_)-> Module:remove( default_params(DB,Params) ) end
  ],?undefined).

close( DB )->
  case whereis( DB ) of
    PID when is_pid( PID )->
      gen_statem:cast(DB, close);
    _->
      {error, not_registered}
  end.

remove( DB )->
  Module = ?dbModule( DB ),
  Params = ?dbNodeParams(DB,node()),
  epipe:do([
    fun(_) -> zaya_schema_srv:remove_db( DB ) end,
    fun
      (_) when Params =:= ?undefined->ok;
      (_)-> Module:remove( default_params(DB,Params) )
    end
  ], ?undefined).


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
    ReplyTo ! {created, self()},
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
handle_event(state_timeout, run, open, #data{db = DB, module = Module} = Data) ->

  update_masters( DB ),

  update_nodes( ?dbMasters(DB), DB ),


  case ?dbAllNodes( DB ) of
    [Node] when Node =:= node()->
      try
        Ref = Module:open( default_params(DB, ?dbNodeParams(DB,node())) ),
        ?LOGINFO("~p database has single copy, open",[DB]),
        {next_state, rollback_transactions, Data#data{ ref = Ref }, [ {state_timeout, 0, run } ] }
      catch
        _:E->
          ?LOGERROR("~p database open error ~p",[DB,E]),
          { keep_state_and_data, [ {state_timeout, 5000, run } ] }
      end;
    Nodes->
      ?LOGINFO("~p has copies on ~p nodes which can have more actual data, try to recover",[DB,Nodes--[node()]]),
      {next_state, recover, Data, [ {state_timeout, 0, run } ] }
  end;

handle_event(state_timeout, run, rollback_transactions, #data{ ref = Ref, db = DB, module = Module } = Data) ->

  zaya_transaction:rollback_log(Module, Ref, DB),

  {next_state, register, Data, [ {state_timeout, 0, run } ] };

handle_event(state_timeout, run, register, #data{db = DB, ref = Ref} = Data) ->

  case elock:lock( ?locks, DB, _IsShared=false, 5000, ?dbAvailableNodes(DB)) of
    {ok, Unlock}->
      try
        {OKs, Errs} = ecall:call_all_wait( ?readyNodes, zaya_schema_srv, open_db, [ DB, node(), Ref ] ),
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
    Ref = zaya_copy:copy( DB, Module, default_params(DB,Params), #{ live => true}),
    ecall:call_all_wait(?readyNodes, zaya_schema_srv, add_db_copy,[ DB, node(), Params ]),
    ReplyTo ! {added, self()},
    ?LOGINFO("~p database copy added",[DB]),
    {next_state, {register_copy,Params}, Data#data{ ref = Ref }, [ {state_timeout, 0, run } ] }
  catch
    _:E->
      ?LOGERROR("~p database add copy error ~p",[DB,E]),
      ReplyTo ! {error,self(),E},
      {stop, shutdown}
  end;

handle_event(state_timeout, run, {register_copy,Params}, #data{db = DB} = Data) ->
  case ecall:call_all(?readyNodes, zaya_schema_srv, add_db_copy, [DB, node(), Params] ) of
    {ok,_}->
      ?LOGINFO("~p database copy registered",[DB]),
      {next_state, register, Data, [ {state_timeout, 0, run } ] };
    {error,Error}->
      ?LOGERROR("~p database register copy error ~p",[ DB, Error ]),
      { keep_state_and_data, [ {state_timeout, 5000, run } ] }
  end;

%%---------------------------------------------------------------------------
%%   RECOVER
%%---------------------------------------------------------------------------
handle_event(state_timeout, run, recover, #data{db = DB}=Data ) ->
  case ?dbAvailableNodes(DB) of
    []->
      case os:getenv("FORCE_START") of
        "true"->
          ?LOGWARNING("~p database force open",[DB]),
          {next_state, open, Data, [ {state_timeout, 0, run } ] };
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
  {next_state, open, Data, [ {state_timeout, 0, run } ] };

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

  case ?dbAvailableNodes(DB) of
    []->
      ?LOGERROR("~p database recovery is impossible: no other copies are available"),
      { keep_state_and_data, [ {state_timeout, 5000, run } ] };
    _->
      ?LOGWARNING("~p database recovery",[DB]),
      % TODO. Hash tree
      Params = ?dbNodeParams(DB,node()),
      try
        if
          Ref =/=?undefined -> Module:close( Ref );
          true->ignore
        end,
        Module:remove( default_params(DB,Params) ),
        zaya_transaction:drop_log( DB ),
        {next_state, {add_copy, Params}, Data, [ {state_timeout, 0, run } ] }
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

  case elock:lock( ?locks, DB, _IsShared=false, 30000, ?dbAvailableNodes(DB)) of
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
  ecall:call_all_wait(?readyNodes, zaya_schema_srv, close_db, [DB, node()]),

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
  maps:merge(#{
    dir => filename:absname(?schemaDir) ++"/"++atom_to_list(DB)
  },Params).

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

