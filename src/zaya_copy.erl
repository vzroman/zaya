
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
  debug/2
]).

%%-----------------------------------------------------------------
%%  Internals
%%-----------------------------------------------------------------
-record(acc,{acc, source_ref, module, batch_size, batch, size, on_batch }).
fold(#source{module = Module,ref = SourceRef } , OnBatch, InAcc)->

  Acc0 = #acc{
    source_ref = SourceRef,
    batch = [],
    acc = InAcc,
    batch_size = ?BATCH_SIZE,
    size = 0,
    on_batch = OnBatch
  },

  case try Module:foldl(SourceRef,#{}, fun iterator/2, Acc0)
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

  Size = size(term_to_binary( Record )),
  Acc#acc{batch = [Record|Batch], size = Size0 + Size};

% Batch is ready
iterator(Record,#acc{
  batch = Batch,
  on_batch = OnBatch,
  size = Size0,
  acc = InAcc0
} = Acc) ->

  Size = size(term_to_binary( Record )),
  Acc#acc{batch = [Record], acc =OnBatch(Batch, Size0, InAcc0), size = Size}.

%%=================================================================
%%	API
%%=================================================================
%------------------types-------------------------------------------
-record(copy,{ send_node, source, params, copy_ref, module, options, attempts, log,error }).
-record(r_acc,{sender,module,source,copy_ref,live,tail_key,hash,log}).
-record(live,{ source, module, send_node, copy_ref, live_ets, giver, taker, log }).

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
  options = Options,
  error = undefined
} = State) when Attempts > 0->

  Log = ?LOG_RECEIVE(SendNode,Source),
  ?LOGINFO("~p attempt ~p",[Log, Attempts - Attempts +1]),

  ok = Module:create( Params ),
  CopyRef = Module:open( Params ),

  Live = prepare_live_copy( Source, Module, SendNode, CopyRef, Log, Options ),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),<<>>),

  ?LOGINFO("~p init sender",[Log]),
  SenderAgs = #{
    receiver => self(),
    source => Source,
    log => ?LOG_SEND(Source,node()),
    options => Options
  },
  Sender = spawn_link(SendNode, ?MODULE, copy_request,[SenderAgs]),

  % The remote sender needs a confirmation of the previous batch before it sends the next one
  Sender ! {confirmed, self()},

  FinalHash=
    try receive_loop(#r_acc{
      sender = Sender,
      source = Source,
      module = Module,
      copy_ref = CopyRef,
      live = Live,
      hash = InitHash,
      log = Log
    }) catch
      _:Error:Stack->
        ?LOGERROR("~p attempt failed ~p, left attempts ~p",[Log,Error,Attempts-1]),

        exit(Sender,rollback),
        drop_live_copy( Live ),
        Module:close( CopyRef ),
        Module:remove( Params ),
        try_copy(State#copy{send_node = ?dbSource(Source), attempts = Attempts - 1, error ={Error,Stack} })
    end,

  finish_live_copy( Live ),

  ?LOGINFO("~p finish hash ~s", [Log, ?PRETTY_HASH( FinalHash )]),

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
  hash = Hash0,
  live = Live,
  log = Log
} = Acc )->
  receive
    {write_batch, Sender, ZipBatch, ZipSize, SenderHash }->

      ?LOGINFO("~p batch received size ~s, hash ~s",[
        Log,
        ?PRETTY_SIZE(ZipSize),
        ?PRETTY_HASH(SenderHash)
      ]),
      {BatchList,Hash} = unzip_batch( lists:reverse(ZipBatch), {[],Hash0}),

      % Check hash
      case crypto:hash_final(Hash) of
        SenderHash -> Sender ! {confirmed, self()};
        LocalHash->
          ?LOGERROR("~p invalid sender hash ~s, local hash ~s",[
            Log,
            ?PRETTY_HASH(SenderHash),
            ?PRETTY_HASH(LocalHash)
          ]),
          Sender ! {invalid_hash, self()},
          throw(invalid_hash)
      end,

      % Dump batch
      [TailKey|_] =
        [ begin
            [{BTailKey,_}|_] = Batch = binary_to_term( BatchBin ),
            ?LOGINFO("~p write batch size ~s, length ~p, last key ~p",[
              Log,
              ?PRETTY_SIZE(size( BatchBin )),
              ?PRETTY_COUNT(length(Batch)),
              BTailKey
            ]),

            Module:write(CopyRef, Batch),
            % Return batch tail key
            BTailKey
          end || BatchBin <- BatchList ],

      % Roll over stockpiled live updates
      roll_live_updates( Live, TailKey ),

      receive_loop( Acc#r_acc{hash = Hash, tail_key = TailKey});

    {finish, Sender, SenderFinalHash }->
      % Finish
      ?LOGINFO("~p sender finished, final hash ~s",[Log, ?PRETTY_HASH(SenderFinalHash)]),
      case crypto:hash_final(Hash0) of
        SenderFinalHash ->
          % Everything is fine!
          SenderFinalHash;
        LocalFinalHash->
          ?LOGERROR("~p invalid sender final hash ~s, local final hash ~s",[
            Log,
            ?PRETTY_HASH(SenderFinalHash),
            ?PRETTY_HASH(LocalFinalHash)
          ]),
          throw(invalid_hash)
      end;
    {error,Sender,SenderError}->
      ?LOGERROR("~p sender error ~p",[Log,SenderError]),
      throw({sender_error,SenderError});
    {'EXIT',Sender,Reason}->
      throw({interrupted,Reason});
    {'EXIT',_Other,Reason}->
      throw({exit,Reason})
  end.

unzip_batch( [Zip|Rest], {Acc0,Hash0})->
  Batch = zlib:unzip( Zip ),
  Hash = crypto:hash_update(Hash0, Batch),
  unzip_batch(Rest ,{[Batch|Acc0], Hash});
unzip_batch([], Acc)->
  Acc.

%----------------------Sender---------------------------------------
-record(s_acc,{receiver,source_ref,module,hash,log,batch_size,size,batch}).
copy_request(#{
  receiver := Receiver,
  source := Source,
  log := Log,
  options := Options
})->
  ?LOGINFO("~p request options ~p", [
    Log, Options
  ]),

  % Monitor
  spawn_link(fun()->
    process_flag(trap_exit,true),
    receive
      {'EXIT',_,Reason} when Reason=:=normal; Reason =:= shutdown->
        ok;
      {'EXIT',_,Reason}->
        ?LOGERROR("~p interrupted, reason ~p",[Log,Reason])
    end
  end),

  SourceRef = ?dbRef(Source,node()),
  InitHash = crypto:hash_update(crypto:hash_init(sha256),<<>>),

  InitState = #s_acc{
    receiver = Receiver,
    source_ref = SourceRef,
    module = ?dbModule( Source ),
    hash = InitHash,
    log = Log,
    batch_size = ?REMOTE_BATCH_SIZE,
    size = 0,
    batch = []
  },

  {ok, Unlock} = elock:lock(?locks, Source, _IsShared = true, [node()], _Timeout = ?infinity  ),

  try
      #s_acc{ batch = TailBatch, hash = TailHash } = TailState =
        fold(SourceRef, fun remote_batch/3, InitState ),

      % Send the tail batch if exists
      case TailBatch of [] -> ok; _->send_batch( TailState ) end,

      FinalHash = crypto:hash_final( TailHash ),

      ?LOGINFO("~p finished, final hash ~p",[Log, ?PRETTY_HASH(FinalHash) ]),
      Receiver ! {finish, self(), FinalHash}

  catch
    _:Error:Stack->
      ?LOGERROR("~p error ~p, stack ~p",[Log,Error,Stack]),
      Receiver ! {error, self(), Error}
  after
    Unlock()
  end.

% Zip and stockpile local batches until they reach ?REMOTE_BATCH_SIZE
remote_batch(Batch0, Size, #s_acc{
  size = TotalZipSize0,
  batch_size = BatchSize,
  batch = ZipBatch,
  hash = Hash0,
  log = Log
} = State) when TotalZipSize0 < BatchSize->

  Batch = term_to_binary( Batch0 ),
  Hash = crypto:hash_update(Hash0, Batch),
  Zip = zlib:zip( Batch ),

  ZipSize = size(Zip),
  TotalZipSize = TotalZipSize0 + ZipSize,

  ?LOGINFO("~p add zip: size ~s, zip size ~p, total zip size ~p",[
    Log, ?PRETTY_SIZE(Size), ?PRETTY_SIZE(ZipSize), ?PRETTY_SIZE(TotalZipSize)
  ]),

  State#{
    size => TotalZipSize,
    batch => [Zip|ZipBatch],
    hash => Hash
  };

% The batch is ready, send it
remote_batch(Batch0, Size, #s_acc{
  receiver = Receiver
}=State)->
  % First we have to receive a confirmation of the previous batch
  receive
    {confirmed, Receiver}->
      send_batch( State ),
      remote_batch(Batch0, Size,State#s_acc{ batch = [], size = 0 });
    {invalid_hash,Receiver}->
      throw(invalid_hash)
  end.

send_batch(#s_acc{
  size = ZipSize,
  batch = ZipBatch,
  hash = Hash,
  receiver = Receiver,
  log = Log
})->

  BatchHash = crypto:hash_final(Hash),
  ?LOGINFO("~p send batch: zip size ~s, length ~p, hash ~s",[
    Log,
    ?PRETTY_SIZE(ZipSize),
    length(ZipBatch),
    ?PRETTY_HASH(BatchHash)
  ]),
  Receiver ! {write_batch, self(), ZipBatch, ZipSize, BatchHash }.


%%===========================================================================
%% LIVE COPY
%%===========================================================================
prepare_live_copy( _Source, _Module, _SendNode, _CopyRef, Log, #{live:=false} )->
  ?LOGINFO("~p cold copy",[Log]),
  #live{live_ets = false};
prepare_live_copy( Source, Module, SendNode, CopyRef, Log, _Options )->
  ?LOGINFO("~p live copy, subscribe....",[Log]),
  % We need to subscribe to all nodes, every node can do updates,
  % timeout is infinity we do not start until everybody is ready
  esubscribe:subscribe(Source, [SendNode], self(), _Timeout = infinity),
  #live{
    source = Source,
    module = Module,
    copy_ref = CopyRef,
    log = Log,
    % Prepare the storage for live updates anyway to avoid excessive check during the copying
    live_ets = ets:new(live,[private,ordered_set])
  }.

finish_live_copy(#live{live_ets = false})->
  ok;
finish_live_copy( Live )->
% Give away live updates to another process until the copy is attached to the schema
  give_away_live_updates( Live ).

drop_live_copy(#live{live_ets = false})->
  ok;
drop_live_copy(#live{ source = Source, send_node = SendNode, live_ets = LiveEts})->

  esubscribe:unsubscribe(Source,[SendNode], self()),
  % Try to drop tail updates
  esubscribe:wait(Source, ?FLUSH_TAIL_TIMEOUT),

  ets:delete( LiveEts ).

roll_live_updates(#live{ live_ets = false },_TailKey)->
  ok;
roll_live_updates(#live{ source = Source, module = Module, copy_ref = CopyRef, live_ets = LiveEts, log = Log }, TailKey)->

  % First we flush subscriptions and roll them over already stockpiled actions,
  % Then we take only those actions that are in the copy keys range already
  % because the next batch may not contain the update yet
  % and so will overwrite came live update.
  % Timeout 0 because we must to receive the next remote batch as soon as possible

  Actions =
    [ Module:live_action(Action) || {Action,_Node,_Actor} <- esubscribe:lookup( Source )],
  ?LOGINFO("~p live updates count ~p",[
    Log,
    ?PRETTY_COUNT(length(Actions))
  ]),

  % Put them into the live ets to sort them and also overwrite old updates to avoid excessive
  % writes to the copy
  true = ets:insert(LiveEts, Actions),

  % Take out the actions that are in the copy range already
  Head = take_head(ets:first(LiveEts), LiveEts, TailKey),
  ?LOGINFO("~p actions to add to the copy ~p, stockpiled ~p",[
    Log,
    ?PRETTY_COUNT(length(Head)),
    ?PRETTY_COUNT(ets:info(LiveEts,size))
  ]),

  Module:write_batch(Head, CopyRef).

take_head(K, Live, TailKey ) when K =/= '$end_of_table', K =< TailKey->
  [{_,Action}] = ets:take(Live, K),
  [Action| take_head(ets:next(Live,K), Live, TailKey)];
take_head(_K, _Live, _TailKey)->
  [].

%---------------------------------------------------------------------
% The remote copy has finished, but there can be live updates
% in the queue that we must not lose. We cannot wait for them
% because the copier (storage server) have to add the table
% to the mnesia schema. Until it does it the updates will
% not go to the local copy. Therefore we start another process
% write tail updates to the copy
%---------------------------------------------------------------------
give_away_live_updates(#live{source = Source, send_node = SendNode, live_ets = LiveEts, log = Log }=Live)->

  Giver = self(),
  Taker =
    spawn_link(fun()->

      receive {'ETS-TRANSFER',LiveEts,Giver,start}->ok end,

      ?LOGINFO("~p live updates has taken by ~p from ~p",[Log,self(),Giver]),
      wait_ready(Live#live{ giver = Giver }, ?dbRef( Source,node() ))

    end),

  % Subscribe the Taker
  esubscribe:subscribe( Source, [SendNode], Taker, _Timeout=infinity ),

  % From now the Taker receives updates I can unsubscribe, and wait
  % for my tail updates
  esubscribe:unsubscribe( Source, [SendNode], self() ),

  ?LOGINFO("~p: giver ~p roll over tail live updates",[Log,Giver]),
  roll_tail_updates( Live ),

  ?LOGINFO("~p give away live updates from ~p to ~p",[Log,Giver,Taker]),
  ets:give_away(LiveEts, Taker, start),

  Live#live{taker = Taker}.

roll_tail_updates( #live{ source = Source, module = Module, live_ets = LiveEts, log = Log } )->

  % Timeout because I have already unsubscribed and it's a finite process
  Actions =
    [ Module:live_action(Action) || {Action,_Node,_Actor} <- esubscribe:wait( Source, ?FLUSH_TAIL_TIMEOUT )],

  ?LOGINFO("~p giver ~p tail updates count ~p",[
    Log,
    self(),
    ?PRETTY_COUNT(length(Actions))
  ]),

  % Put them into the live ets to sort them and also overwrite old updates to avoid excessive
  % writes to the copy
  ets:insert(LiveEts, Actions).

wait_ready(#live{
  source = Source,
  module=Module,
  copy_ref = CopyRef,
  live_ets = LiveEts,
  log = Log
} = Live,_Ref = ?undefined)->

  % The copy is not ready yet
  Actions0 =
    [ Module:live_action(Action) || {Action,_Node,_Actor} <- esubscribe:wait( Source, ?FLUSH_TAIL_TIMEOUT )],

  ?LOGINFO("~p taker ~p tail updates count ~p",[
    Log,
    self(),
    ?PRETTY_COUNT(length(Actions0))
  ]),

  ets:insert(LiveEts, Actions0),
  Actions = take_all(ets:first(LiveEts),LiveEts),

  ?LOGINFO("~p taker ~p actions to add to the copy ~p",[
    Log,
    ?PRETTY_COUNT(length(Actions))
  ]),

  Module:write_batch(Actions, CopyRef),

  wait_ready(Live, ?dbRef(Source,node()));

wait_ready(#live{
  source = Source,
  send_node = SendNode,
  module = Module,
  copy_ref = CopyRef,
  live_ets = LiveEts,
  log = Log
}, Ref) when Ref =/= ?undefined ->

  esubscribe:unsubscribe( Source, [SendNode], self() ),

  ?LOGINFO("~p ready, taker ~p flush tail subscriptions",[Log,self()]),

  Actions0 =
    [ Module:live_action(Action) || {Action,_Node,_Actor} <- esubscribe:wait( Source, ?FLUSH_TAIL_TIMEOUT )],

  ?LOGINFO("~p taker ~p tail updates count ~p",[
    Log,
    self(),
    ?PRETTY_COUNT(length(Actions0))
  ]),

  ets:insert(LiveEts, Actions0),
  Actions = take_all(ets:first(LiveEts),LiveEts),

  ?LOGINFO("~p taker ~p tail actions to add to the copy ~p",[
    Log,
    ?PRETTY_COUNT(length(Actions))
  ]),

  Module:write_batch(Actions, CopyRef),

  ets:delete( LiveEts ),

  ?LOGINFO("~p live copy finish",[Log]).

take_all(K, Live ) when K =/= '$end_of_table'->
  [{_,Action}] = ets:take(Live, K),
  [Action| take_all(ets:next(Live,K), Live)];
take_all(_K, _Live)->
  [].

%---------------------------------------------------------------------
% LOCAL COPY DB COPY TO DB
%---------------------------------------------------------------------
local_copy( Source, Target, Module, Options)->

  Log = ?LOG_LOCAL(Source,Target),
  SourceRef = ?dbRef(Source,node()),
  TargetRef = ?dbRef(Target,node()),

  Live = prepare_live_copy( Source, Module, node(), TargetRef, Log, Options ),

  OnBatch =
    fun(Batch, Size, _)->
      ?LOGINFO("~p write batch, size ~s, length ~s",[
        Log,
        ?PRETTY_SIZE(Size),
        ?PRETTY_COUNT(length(Batch))
      ]),
      Module:write_batch(Batch, TargetRef)
    end,

  ?LOGINFO("~p finish, hash ~s",[Log]),
  fold(#source{module = Module,ref = SourceRef } , OnBatch, ?undefined),

  _LiveTail = finish_live_copy( Live ).


debug(Storage, Count)->
  spawn(fun()->fill(Storage, Count) end).

fill(S,C) when C>0 ->
  if C rem 100000 =:= 0-> ?LOGINFO("DEBUG: write ~p",[C]); true->ignore end,
  KV = {
    {x, erlang:phash2({C}, 200000000), erlang:phash2({C}, 200000000)},
    {y, binary:copy(integer_to_binary(C), 100)}
  },
  zaya:write(S,[KV]),
  fill(S,C-1);
fill(_S,_C)->
  ok.


