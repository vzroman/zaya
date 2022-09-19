
-module(zaya_db_sup).

-include("zaya.hrl").
-include("zaya_util.hrl").
-include("zaya_atoms.hrl").

-behaviour(supervisor).

-export([
  start_link/0,
  init/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->

  ChildConfig = #{
    id=> zaya_db_srv,
    start=>{zaya_db_srv,start_link,[]},
    restart=> permanent,
    shutdown=> ?env(stop_timeout, ?DEFAULT_STOP_TIMEOUT),
    type=>worker,
    modules=>[zaya_db_srv]
  },

  Supervisor=#{
    strategy=>simple_one_for_one,
    intensity=>?env(max_restarts, ?DEFAULT_MAX_RESTARTS),
    period=>?env(max_period, ?DEFAULT_MAX_PERIOD)
  },

  {ok,{Supervisor, [ ChildConfig ]}}.


