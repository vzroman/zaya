
-module(zaya_sup).

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

  SubscriptionsServer = #{
    id=>esubscribe,
    start=>{esubscribe,start_link,[]},
    restart=>permanent,
    shutdown=> ?DEFAULT_STOP_TIMEOUT,
    type=>worker,
    modules=>[esubscribe]
  },

  LockServer = #{
    id=>?locks,
    start=>{elock,start_link,[ ?locks ]},
    restart=>permanent,
    shutdown=> ?env(stop_timeout, ?DEFAULT_STOP_TIMEOUT),
    type=>worker,
    modules=>[elock]
  },

  SchemaServer=#{
    id=>zaya_schema_srv,
    start=>{zaya_schema_srv,start_link,[]},
    restart=>permanent,
    shutdown=>?env(stop_timeout, ?DEFAULT_STOP_TIMEOUT),
    type=>worker,
    modules=>[zaya_schema_srv]
  },

  Supervisor=#{
    strategy=>one_for_one,
    intensity=>?env(max_restarts, ?DEFAULT_MAX_RESTARTS),
    period=>?env(max_period, ?DEFAULT_MAX_PERIOD)
  },

  {ok, {Supervisor, [
    SubscriptionsServer,
    LockServer,
    SchemaServer
  ]}}.


