-module(zaya_tx_test_backend).

-export([
  open/1,
  close/1,
  commit/3,
  prepare_rollback/3,
  is_persistent/0,
  fail_once/1,
  fail_after_confirm/1,
  last_tref/0
]).

open(_Params) ->
  ets:new(?MODULE, [public, named_table, ordered_set]).

close(Table) ->
  catch ets:delete(Table),
  ok.

commit(Table, Write, Delete) ->
  persistent_term:put({?MODULE, last_tref}, make_ref()),
  maybe_fail(),
  ets:insert(Table, Write),
  [ets:delete(Table, Key) || Key <- Delete],
  ok.

prepare_rollback(Table, Write, Delete) ->
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

is_persistent() ->
  true.

fail_once(DB) ->
  persistent_term:put({?MODULE, fail_once, DB}, true),
  ok.

fail_after_confirm(DB) ->
  persistent_term:put({?MODULE, fail_after_confirm, DB}, true),
  ok.

last_tref() ->
  persistent_term:get({?MODULE, last_tref}, undefined).

maybe_fail() ->
  ok.
