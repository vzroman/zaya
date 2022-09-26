

-ifndef(zaya_COPY).
-define(zaya_COPY,1).

-include("zaya_util.hrl").

% constants
-define(DEFAULT_BATCH_SIZE, 31457280). % 30 MB
-define(DEFAULT_REMOTE_BATCH_SIZE, 4194304). % 4 MB
-define(DEFAULT_REMOTE_ATTEMPTS,3).
-define(DEFAULT_FLUSH_TAIL_TIMEOUT,10000). % 10 sec
-define(CHECK_LIVE_READY_TIMER, 5000).

-define(DEFAULT_LIVE_CONFIG, #{
  flush_tail_timeout => ?DEFAULT_FLUSH_TAIL_TIMEOUT			% 1 sec
}).

-define(DEFAULT_REMOTE_CONFIG,#{
  batch_size => ?DEFAULT_REMOTE_BATCH_SIZE,						% 4 MB
  attempts => ?DEFAULT_REMOTE_ATTEMPTS,
  live => ?DEFAULT_LIVE_CONFIG
}).
-define(DEFAULT_CONFIG,#{
  batch_size => ?DEFAULT_BATCH_SIZE,							% 30 MB
  remote => ?DEFAULT_REMOTE_CONFIG
}).

-define(CONFIG,?env(copy, ?DEFAULT_CONFIG)).
-define(REMOTE_CONFIG,maps:get(remote,?CONFIG)).
-define(LIVE_CONFIG,maps:get(live,?REMOTE_CONFIG)).

-define(BATCH_SIZE,maps:get(batch_size,?CONFIG)).
-define(REMOTE_BATCH_SIZE,maps:get(batch_size,?REMOTE_CONFIG,?DEFAULT_REMOTE_BATCH_SIZE)).
-define(FLUSH_TAIL_TIMEOUT,maps:get(flush_tail_timeout,?LIVE_CONFIG)).


-define(DEFAULT_OPTIONS,#{
  live => true,
  start_key =>undefined,
  end_key => undefined,
  hash => <<>>,
  sync => false,
  attempts => 3
}).
-define(OPTIONS(O),maps:merge(?DEFAULT_OPTIONS,O)).

% Log formats
-define(LOG_SEND(Source,Node),unicode:characters_to_list(io_lib:format("copy: ~p to ~p",[Source,Node]))).
-define(LOG_RECEIVE(Node,Source),unicode:characters_to_list(io_lib:format("copy: ~p from ~p",[Source,Node]))).

-define(LOG_LOCAL(Source,Target),unicode:characters_to_list(io_lib:format("local copy: ~p:~p",[Source,Target]))).

-endif.
