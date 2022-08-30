
-ifndef(zaya_util).
-define(zaya_util,1).

-define(env(Key,Default), application:get_env(zaya,Key,Default)).
-define(env(OS,Config,Default),

  case os:getenv(OS) of false->?env(Config,Default); _@V->_@V end

).

-define(random(List),
  begin
    _@I = erlang:phash2(make_ref(),length(List)),
    lists:nth(_@I+1, List)
  end
).

-define(PRETTY_SIZE(S),zaya_util:pretty_size(S)).
-define(PRETTY_COUNT(C),zaya_util:pretty_count(C)).
-define(PRETTY_HASH(H),binary_to_list(base64:encode(H))).

-define(MS_COMPILE(MS),ets:match_spec_compile(MS)).
-define(MB,1048576).

-endif.
