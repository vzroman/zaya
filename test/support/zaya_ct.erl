-module(zaya_ct).

-export([
  start_zaya/0,
  stop_zaya/0,
  wait_until/1,
  wait_until/2
]).

-define(OWNER_KEY, {?MODULE, zaya_owner}).

start_zaya() ->
  ok = ensure_zaya_dependencies_started(),
  case whereis(zaya_sup) of
    Pid when is_pid(Pid) ->
      ok;
    undefined ->
      start_zaya_owner()
  end.

stop_zaya() ->
  ok = stop_zaya_owner(),
  _ = zaya:stop(),
  ok = wait_until(fun() -> whereis(zaya_sup) =:= undefined end),
  ok = wait_until(fun() -> whereis(zaya_transaction_log) =:= undefined end),
  cleanup_transaction_log_runtime().

wait_until(Fun) ->
  wait_until(Fun, 50).

wait_until(Fun, Attempts) when Attempts > 0 ->
  case Fun() of
    true ->
      ok;
    _ ->
      timer:sleep(100),
      wait_until(Fun, Attempts - 1)
  end;
wait_until(_Fun, 0) ->
  ct:fail(wait_until_timeout).

ensure_zaya_dependencies_started() ->
  ok = load_zaya_application(),
  case application:get_key(zaya, applications) of
    {ok, Apps} ->
      lists:foreach(fun ensure_application_started/1, Apps);
    undefined ->
      ct:fail(zaya_application_dependencies_not_found)
  end.

load_zaya_application() ->
  case application:load(zaya) of
    ok ->
      ok;
    {error, {already_loaded, zaya}} ->
      ok;
    {error, Reason} ->
      ct:fail({failed_to_load_zaya_application, Reason})
  end.

ensure_application_started(App) ->
  case application:ensure_all_started(App) of
    {ok, _Started} ->
      ok;
    {error, Reason} ->
      ct:fail({failed_to_start_dependency, App, Reason})
  end.

start_zaya_owner() ->
  Parent = self(),
  Owner = spawn(fun() -> zaya_owner(Parent) end),
  receive
    {?MODULE, Owner, started, {ok, _Pid}} ->
      persistent_term:put(?OWNER_KEY, Owner),
      ok;
    {?MODULE, Owner, started, {error, {already_started, _Pid}}} ->
      persistent_term:put(?OWNER_KEY, Owner),
      ok;
    {?MODULE, Owner, started, Error} ->
      ct:fail({failed_to_start_zaya, Error})
  after 5000 ->
    exit(Owner, kill),
    ct:fail(zaya_start_timeout)
  end.

zaya_owner(Parent) ->
  process_flag(trap_exit, true),
  Result = zaya:start(),
  Parent ! {?MODULE, self(), started, Result},
  case Result of
    {ok, _Pid} ->
      zaya_owner_loop();
    {error, {already_started, _Pid}} ->
      zaya_owner_loop();
    _ ->
      ok
  end.

zaya_owner_loop() ->
  receive
    {stop_zaya, From} ->
      _ = zaya:stop(),
      From ! {?MODULE, self(), stopped},
      ok;
    {'EXIT', _Pid, _Reason} ->
      zaya_owner_loop();
    _Unexpected ->
      zaya_owner_loop()
  end.

stop_zaya_owner() ->
  case persistent_term:get(?OWNER_KEY, undefined) of
    Owner when is_pid(Owner) ->
      persistent_term:erase(?OWNER_KEY),
      stop_zaya_owner(Owner);
    undefined ->
      ok
  end.

stop_zaya_owner(Owner) ->
  case erlang:is_process_alive(Owner) of
    true ->
      Owner ! {stop_zaya, self()},
      receive
        {?MODULE, Owner, stopped} ->
          ok
      after 5000 ->
        exit(Owner, kill),
        ok
      end;
    false ->
      ok
  end.

cleanup_transaction_log_runtime() ->
  RuntimeKey = {zaya_transaction_log, runtime},
  case persistent_term:get(RuntimeKey, undefined) of
    {runtime, Ref, _AtomicsRef} ->
      persistent_term:erase(RuntimeKey),
      catch zaya_rocksdb:close(Ref),
      ok;
    undefined ->
      ok;
    _Unexpected ->
      persistent_term:erase(RuntimeKey),
      ok
  end.
