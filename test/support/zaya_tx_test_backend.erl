-module(zaya_tx_test_backend).

-export([
  create/1,
  open/1,
  close/1,
  commit/3,
  prepare_rollback/3,
  read/2,
  write/2,
  delete/2,
  remove/1,
  is_persistent/0,
  fail_once/1,
  fail_after_confirm/1,
  last_tref/0,
  seed/2,
  snapshot/1,
  commit_count/1,
  last_commit_pid/1,
  reset/1
]).

create(Params) ->
  seed(Params, []),
  open(Params).

open(Params) ->
  Dir = dir_key(Params),
  case persistent_term:get({?MODULE, open_count, Dir}, 0) of
    0 ->
      persistent_term:put({?MODULE, open_count, Dir}, 1),
      Table = ets:new(?MODULE, [public, ordered_set]),
      ets:insert(Table, snapshot(Dir)),
      #{dir => Dir, table => Table};
    _ ->
      erlang:error({already_open, Dir})
  end.

close(#{dir := Dir, table := Table}) ->
  sync(Dir, Table),
  catch ets:delete(Table),
  persistent_term:erase({?MODULE, open_count, Dir}),
  ok.

commit(#{dir := Dir, table := Table}, Write, Delete) ->
  persistent_term:put({?MODULE, last_tref}, make_ref()),
  persistent_term:put({?MODULE, last_commit_pid, Dir}, self()),
  maybe_fail_once(Dir),
  ets:insert(Table, Write),
  [ets:delete(Table, Key) || Key <- Delete],
  persistent_term:put({?MODULE, commit_count, Dir}, commit_count(Dir) + 1),
  sync(Dir, Table),
  maybe_fail_after_confirm(Dir),
  ok.

prepare_rollback(#{table := Table}, Write, Delete) ->
  RollbackWrite =
    lists:foldl(
      fun({Key, _Value}, Acc) ->
        case ets:lookup(Table, Key) of
          [{Key, Existing}] -> [{Key, Existing} | Acc];
          [] -> Acc
        end
      end,
      [],
      Write
    ),
  RollbackDelete =
    lists:foldl(
      fun({Key, _Value}, Acc) ->
        case ets:lookup(Table, Key) of
          [] -> [Key | Acc];
          [_] -> Acc
        end
      end,
      Delete,
      Write
    ),
  {lists:reverse(RollbackWrite), lists:reverse(RollbackDelete)}.

read(#{table := Table}, Keys) ->
  lists:foldl(
    fun(Key, Acc) ->
      case ets:lookup(Table, Key) of
        [{Key, Value}] -> [{Key, Value} | Acc];
        [] -> Acc
      end
    end,
    [],
    Keys
  ).

write(Ref, KVs) ->
  commit(Ref, KVs, []).

delete(#{table := Table} = Ref, Keys) ->
  case Table of
    _ ->
      commit(Ref, [], Keys)
  end.

remove(Params) ->
  Dir = dir_key(Params),
  persistent_term:erase({?MODULE, store, Dir}),
  persistent_term:erase({?MODULE, commit_count, Dir}),
  persistent_term:erase({?MODULE, open_count, Dir}),
  ok.

is_persistent() ->
  true.

fail_once(DB) ->
  persistent_term:put({?MODULE, fail_once, dir_key(DB)}, true),
  ok.

fail_after_confirm(DB) ->
  persistent_term:put({?MODULE, fail_after_confirm, dir_key(DB)}, true),
  ok.

last_tref() ->
  persistent_term:get({?MODULE, last_tref}, undefined).

seed(ParamsOrDir, KVs) ->
  Dir = dir_key(ParamsOrDir),
  persistent_term:put({?MODULE, store, Dir}, lists:sort(KVs)),
  persistent_term:put({?MODULE, commit_count, Dir}, 0),
  ok.

snapshot(ParamsOrDir) ->
  Dir = dir_key(ParamsOrDir),
  persistent_term:get({?MODULE, store, Dir}, []).

commit_count(ParamsOrDir) ->
  Dir = dir_key(ParamsOrDir),
  persistent_term:get({?MODULE, commit_count, Dir}, 0).

last_commit_pid(ParamsOrDir) ->
  Dir = dir_key(ParamsOrDir),
  persistent_term:get({?MODULE, last_commit_pid, Dir}, undefined).

reset(ParamsOrDir) ->
  Dir = dir_key(ParamsOrDir),
  persistent_term:erase({?MODULE, store, Dir}),
  persistent_term:erase({?MODULE, commit_count, Dir}),
  persistent_term:erase({?MODULE, last_commit_pid, Dir}),
  persistent_term:erase({?MODULE, open_count, Dir}),
  persistent_term:erase({?MODULE, fail_once, Dir}),
  persistent_term:erase({?MODULE, fail_after_confirm, Dir}),
  ok.

maybe_fail_once(Dir) ->
  fail_if_requested({?MODULE, fail_once, Dir}, {forced_failure, Dir}).

maybe_fail_after_confirm(Dir) ->
  fail_if_requested(
    {?MODULE, fail_after_confirm, Dir},
    {forced_failure_after_confirm, Dir}
  ).

fail_if_requested(Key, Error) ->
  case persistent_term:get(Key, false) of
    true ->
      persistent_term:erase(Key),
      erlang:error(Error);
    false ->
      ok
  end.

sync(Dir, Table) ->
  persistent_term:put(
    {?MODULE, store, Dir},
    lists:sort(ets:tab2list(Table))
  ).

dir_key(#{dir := Dir}) ->
  Dir;
dir_key(Params) when is_map(Params) ->
  maps:get(dir, Params);
dir_key(Dir) ->
  Dir.
