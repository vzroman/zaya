-module(zaya_db_srv_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
  all/0
]).

-export([
  default_params_includes_db_name_test/1
]).

all() ->
  [
    default_params_includes_db_name_test
  ].

default_params_includes_db_name_test(Config) ->
  DB = default_params_name_test,
  Params = #{dir => filename:join(?config(priv_dir, Config), "custom-dir")},
  FullParams = zaya_db_srv:default_params(DB, Params),
  ?assertEqual(DB, maps:get(name, FullParams)).
