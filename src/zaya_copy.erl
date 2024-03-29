
-module(zaya_copy).

-include("zaya.hrl").
-include("zaya_schema.hrl").
-include("zaya_copy.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  copy/3, copy/4,
  local_copy/4
]).

%%=================================================================
%%	Remote API
%%=================================================================
-export([
  copy_request/1,
  remote_batch/3
]).

-export([
  debug/2,
  get_hash/1
]).

%%-----------------------------------------------------------------
%%  Internals
%%-----------------------------------------------------------------
-record(acc,{acc, source_ref, module, batch_size, batch, size, on_batch }).
fold(Module,SourceRef, OnBatch, InAcc)->

  Acc0 = #acc{
    source_ref = SourceRef,
    batch = [],
    acc = InAcc,
    batch_size = ?BATCH_SIZE,
    size = 0,
    on_batch = OnBatch
  },

  case try Module:copy(SourceRef, fun iterator/2, Acc0)
  catch
   _:{stop,Stop}-> Stop;
   _:{final,Final}->{final,Final}
  end of
    #acc{batch = [], acc = FinalAcc}-> FinalAcc;
    #acc{batch = Tail, size = Size, acc = TailAcc, on_batch = OnBatch}->
      OnBatch(Tail, Size, TailAcc);
    {final,FinalAcc}-> FinalAcc
  end.
%---------------------------------------------------
iterator(Record,#acc{
  batch_size = BatchSize,
  batch = Batch,
  size = Size0
} = Acc)
  when Size0 < BatchSize->

  Acc#acc{batch = [Record|Batch], size = Size0 + 1};

% Batch is ready
iterator(Record,#acc{
  batch = Batch,
  on_batch = OnBatch,
  size = Size0,
  acc = InAcc0
} = Acc) ->

  Acc#acc{batch = [Record], acc =OnBatch(Batch, Size0, InAcc0), size = 1}.

%%=================================================================
%%	API
%%=================================================================
%------------------types-------------------------------------------
-record(copy,{ send_node, source, params, copy_ref, module, options, attempts, log,error }).
-record(r_acc,{sender,module,source,copy_ref,live,log}).
-record(live,{ source, module, send_node, copy_ref, live_ets, owner, log }).

copy(Source, Module, Params )->
  copy(Source, Module, Params, #{}).
copy( Source, Module, Params, Options0 )->

  Options = #{attempts:=Attempts} = ?OPTIONS(Options0),
  Copy = #copy{
    send_node = ?dbSource( Source ),
    source = Source,
    params = Params,
    module = Module,
    options = Options,
    attempts = Attempts,
    error = ?undefined
  },

  try_copy( Copy ).

%---------------------------------------------------------------------
% REMOTE COPY
%---------------------------------------------------------------------
try_copy(#copy{
  send_node = ?undefined,
  source = Source
})->
  ?LOGERROR("~p is not available", [ Source ]),
  throw(?not_available);

try_copy(#copy{
  send_node = SendNode,
  source = Source,
  module = Module,
  params = Params,
  attempts = Attempts,
  options = Options
} = State) when Attempts > 0->

  Log = ?LOG_RECEIVE(SendNode,Source),
  ?LOGINFO("~s attempt ~p",[Log, Attempts - Attempts +1]),

  CopyRef = Module:create( Params ),

  Live = prepare_live_copy( Source, Module, SendNode, CopyRef, Log, Options ),

  ?LOGINFO("~s init sender",[Log]),
  SenderAgs = #{
    receiver => self(),
    source => Source,
    log => ?LOG_SEND(Source,node()),
    options => Options
  },
  Sender = spawn_link(SendNode, ?MODULE, copy_request,[SenderAgs]),

  try receive_loop(#r_acc{
    sender = Sender,
    source = Source,
    module = Module,
    copy_ref = CopyRef,
    live = Live,
    log = Log
  }) catch
    _:Error:Stack->
      ?LOGERROR("~s attempt failed ~p, left attempts ~p",[Log,Error,Attempts-1]),

      exit(Sender,rollback),
      drop_live_copy( Live ),
      Module:close( CopyRef ),
      Module:remove( Params ),
      try_copy(State#copy{send_node = ?dbSource(Source), attempts = Attempts - 1, error ={Error,Stack} })
  end,

  finish_live_copy( Live ),

  ?LOGINFO("~s finish", [Log]),

  CopyRef;

try_copy(#copy{source = Source,error = {Error,Stack}})->
  ?LOGERROR("~p copy failed, no attempts left, last error ~p, stack ~p",[
    Source, Error, Stack
  ]),
  throw( Error ).

%----------------------Receiver---------------------------------------
receive_loop(#r_acc{
  sender = Sender,
  module = Module,
  copy_ref =  CopyRef,
  live = Live,
  log = Log
} = Acc )->
  receive
    {write_batch, Sender, Batch }->

      Sender ! {confirmed, self()},

      ?LOGINFO("~s batch received",[
        Log
      ]),

      % Dump batch
      [ begin
          ?LOGDEBUG("~s write length ~p",[
            Log,
            ?PRETTY_COUNT(length(B))
          ]),

          Module:dump_batch(CopyRef, B)

        end || B <- Batch ],

      % Roll over stockpiled live updates
      roll_live_updates( Live ),

      receive_loop( Acc );

    {finish, Sender }->
      % Finish
      ?LOGINFO("~s sender finished",[Log]);
    {error,Sender,SenderError}->
      ?LOGERROR("~s sender error ~p",[Log,SenderError]),
      throw({sender_error,SenderError});
    {'EXIT',Sender,Reason}->
      throw({interrupted,Reason});
    {'EXIT',_Other,Reason}->
      throw({exit,Reason})
  end.

%----------------------Sender---------------------------------------
-record(s_acc,{receiver,source_ref,module,log,batch}).
copy_request(#{
  receiver := Receiver,
  source := Source,
  log := Log,
  options := Options
})->
  ?LOGINFO("~s request options ~p", [
    Log, Options
  ]),

  % Monitor
  spawn_link(fun()->
    process_flag(trap_exit,true),
    receive
      {'EXIT',_,Reason} when Reason=:=normal; Reason =:= shutdown->
        ok;
      {'EXIT',_,Reason}->
        ?LOGERROR("~s interrupted, reason ~p",[Log,Reason])
    end
  end),

  Module = ?dbModule( Source ),
  SourceRef = ?dbRef(Source,node()),

  InitState = #s_acc{
    receiver = Receiver,
    source_ref = SourceRef,
    module = Module,
    log = Log,
    batch = []
  },

  {ok, Unlock} = elock:lock(?locks, Source, _IsShared = true, _Timeout = ?infinity ),

  try
      #s_acc{ batch = TailBatch } = TailState =
        fold(Module, SourceRef, fun remote_batch/3, InitState ),

      % Send the tail batch if exists
      case TailBatch of [] -> ok; _->send_batch( TailState ) end,

      ?LOGINFO("~s finished",[Log ]),
      Receiver ! {finish, self() }

  catch
    _:Error:Stack->
      ?LOGERROR("~s error ~p, stack ~p",[Log,Error,Stack]),
      Receiver ! {error, self(), Error}
  after
    Unlock(),
    unlink(Receiver)
  end.

% Zip and stockpile local batches until they reach ?REMOTE_BATCH_SIZE
remote_batch(Batch, Size, #s_acc{
  batch = BatchAcc,
  log = Log
} = State) when length( BatchAcc ) < 10->


  ?LOGDEBUG("~s add batch: size ~s",[
    Log, ?PRETTY_COUNT(Size)
  ]),

  State#s_acc{
    batch = [lists:reverse( Batch )|BatchAcc]
  };

% The batch is ready, send it
remote_batch(Batch0, Size, #s_acc{
  receiver = Receiver,
  log = Log
}=State)->

  send_batch( State ),
  % First we have to receive a confirmation of the previous batch
  receive
    {confirmed, Receiver}->
      ?LOGINFO("~s confirmed",[ Log ]),
      remote_batch(Batch0, Size,State#s_acc{ batch = [] })
  end.

send_batch(#s_acc{
  batch = Batch,
  receiver = Receiver,
  log = Log
})->

  ?LOGINFO("~s send batch",[
    Log
  ]),
  Receiver ! {write_batch, self(), lists:reverse( Batch ) }.


%%===========================================================================
%% LIVE COPY
%%===========================================================================
prepare_live_copy( _Source, _Module, _SendNode, _CopyRef, Log, #{live:=false} )->
  ?LOGINFO("~s cold copy",[Log]),
  ?undefined;
prepare_live_copy( Source, Module, SendNode, CopyRef, Log, _Options )->
  Owner = self(),
  Live = spawn_link(fun()->
    ?LOGINFO("~s live copy, subscribe....",[Log]),

    esubscribe:subscribe(?subscriptions, Source, self(), [SendNode]),
    esubscribe:subscribe(?subscriptions, ?schema, self(), [SendNode]),

    Owner ! {ready, self()},

    wait_live_updates(#live{
      source = Source,
      module = Module,
      copy_ref = CopyRef,
      log = Log,
      send_node = SendNode,
      owner = Owner,
      % Prepare the buffer for live updates
      live_ets = ets:new(live,[private,ordered_set])
    })
  end),

  receive {ready, Live} -> Live end.

finish_live_copy( ?undefined )->
  ok;
finish_live_copy( Live )->
  Live ! {finish, self()},
  receive {finish, Live}->ok end.

drop_live_copy(?undefined)->
  ok;
drop_live_copy(Live)->
  catch Live ! {drop, self()}.

roll_live_updates(?undefined)->
  ok;
roll_live_updates( Live )->
  Live ! {roll_updates, self()}.

wait_live_updates(#live{ source = Source, live_ets = LiveEts, owner = Owner }=Live)->
  receive
    {?subscriptions, Source, {write,KVs}, _Node, _Actor}->
      ets:insert( LiveEts, [{K,{write,V}} || {K,V} <- KVs] ),
      wait_live_updates( Live );
    {?subscriptions, Source, {delete,Keys}, _Node, _Actor}->
      ets:insert( LiveEts, [{K,delete} || K <- Keys] ),
      wait_live_updates( Live );
    {roll_updates, Owner}->
      roll_updates(Live),
      wait_live_updates( Live );
    {finish, Owner}->
      roll_tail_updates( Live ),
      Owner ! {finish,self()},
      wait_ready( Live ),
      unlink(Owner);
    {drop,Owner}->
      unlink(Owner)
  end.

roll_updates(#live{ module = Module, copy_ref = CopyRef, live_ets = LiveEts, log = Log })->

  % First we flush subscriptions and roll them over already stockpiled actions,
  % Then we take only those actions that are in the copy keys range already
  % because the next batch may not contain the update yet
  % and so will overwrite came live update.
  % Timeout 0 because we must to receive the next remote batch as soon as possible

  try
    {TailKey,_} = Module:last( CopyRef ),
    % Take out the actions that are in the copy range already
    {Write,Delete} = take_head(ets:first(LiveEts), LiveEts, TailKey, {[],[]}),
    ?LOGINFO("~s actions to write to the copy ~p, delete ~p, stockpiled ~p, tail key ~p",[
      Log,
      ?PRETTY_COUNT(length(Write)),
      ?PRETTY_COUNT(length(Delete)),
      ?PRETTY_COUNT(ets:info(LiveEts,size)),
      TailKey
    ]),

    Module:delete(CopyRef, Delete),
    Module:write(CopyRef, Write)
  catch
    _:_-> ignore
  end.

take_head(K, Live, TailKey, {Write,Delete} ) when K =/= '$end_of_table', K =< TailKey->
  case ets:take(Live, K) of
    [{K,{write,V}}]->
      take_head( ets:next(Live,K), Live, TailKey, {[{K,V}|Write],Delete});
    [{K,delete}]->
      take_head( ets:next(Live,K), Live, TailKey, {Write,[K|Delete]});
    _->
      take_head( ets:next(Live,K), Live, TailKey, {Write,Delete})
  end;
take_head(_K, _Live, _TailKey, Acc)->
  Acc.

roll_tail_updates( #live{ module = Module, copy_ref = CopyRef, live_ets = LiveEts, log = Log })->

  % Take out the actions that are in the copy range already
  {Write,Delete} = take_all(ets:first(LiveEts), LiveEts, {[],[]}),
  ?LOGINFO("~s actions to write to the copy ~p, delete ~p",[
    Log,
    ?PRETTY_COUNT(length(Write)),
    ?PRETTY_COUNT(length(Delete))
  ]),

  Module:delete(CopyRef, Delete),
  Module:write(CopyRef, Write),

  ok.

take_all(K, Live, {Write,Delete} ) when K =/= '$end_of_table'->
  case ets:take(Live, K) of
    [{K,{write,V}}]->
      take_all( ets:next(Live,K), Live, {[{K,V}|Write],Delete});
    [{K,delete}]->
      take_all( ets:next(Live,K), Live, {Write,[K|Delete]});
    _->
      take_all( ets:next(Live,K), Live, {Write,Delete})
  end;
take_all(_K, _Live, Acc)->
  Acc.

wait_ready(#live{
  source = Source,
  module = Module,
  copy_ref = CopyRef,
  log = Log
} = Live)->
  receive
    {?subscriptions, Source, {write,KVs}, _Node, _Actor}->
      Module:write(CopyRef, KVs),
      wait_ready(Live);
    {?subscriptions, Source, {delete,Keys}, _Node, _Actor}->
      Module:delete(CopyRef, Keys),
      wait_ready(Live);
    {?subscriptions, ?schema, {'open_db',Source, Node}, _Node, _Actor} when Node =:= node()->
      ?LOGINFO("~s finsish copy",[Log]);
    _->
      wait_ready( Live )
  end.

%---------------------------------------------------------------------
% LOCAL COPY DB COPY TO DB
%---------------------------------------------------------------------
local_copy( Source, Target, Module, Options)->

  Log = ?LOG_LOCAL(Source,Target),
  SourceRef = ?dbRef(Source,node()),
  TargetRef = ?dbRef(Target,node()),

  Live = prepare_live_copy( Source, Module, node(), TargetRef, Log, Options ),

  OnBatch =
    fun(Batch, _Size, _)->
      ?LOGINFO("~s write batch, length ~s",[
        Log,
        ?PRETTY_COUNT(length(Batch))
      ]),
      % TODO, Roll over live updates
      Module:write_batch(Batch, TargetRef)
    end,

  ?LOGINFO("~s finish",[Log]),
  fold(Module, SourceRef , OnBatch, ?undefined),

  _LiveTail = finish_live_copy( Live ).


debug(DB, Count)->
  spawn(fun()->fill(DB, Count) end).

fill(S,C) when C>0 ->
  if C rem 100000 =:= 0-> ?LOGINFO("DEBUG: write ~p",[C]); true->ignore end,
  KV = {
    {x, erlang:phash2({C}, 200000000), erlang:phash2({C}, 200000000)},
    {y, binary:copy(integer_to_binary(C), 100)}
  },
  zaya:write(S,[KV]),
  %timer:sleep(10),
  fill(S,C-1);
fill(_S,_C)->
  ok.

% Test2 = zaya_copy:debug(test1, 200000000)
% exit(Test,shutdown).
% zaya_copy:get_hash(test)

get_hash(DB)->
  InitHash = crypto:hash_update(crypto:hash_init(sha256),<<>>),
  FinalHash = zaya:foldl(DB,#{},fun(Rec,Hash)->crypto:hash_update(Hash,term_to_binary(Rec)) end, InitHash),
  ?PRETTY_HASH(crypto:hash_final( FinalHash )).