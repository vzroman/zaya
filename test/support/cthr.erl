-module(cthr).

-export([
  pal/1,
  pal/2,
  pal/3,
  pal/4
]).

pal(_Message) ->
  ok.

pal(_Format, _Args) ->
  ok.

pal(_Level, _Message, _Meta) ->
  ok.

pal(_Level, _Format, _Args, _Meta) ->
  ok.
