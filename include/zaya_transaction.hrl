-ifndef(ZAYA_TRANSACTION_HRL).
-define(ZAYA_TRANSACTION_HRL, true).

%% Per-DB rollback entry key.
%% On-disk tuple form: {rollback, DB, Seq, TRef}.
-record(rollback, {
  db   :: atom(),
  seq  :: non_neg_integer(),
  tref :: reference()
}).

%% Cluster-wide "transaction rolled back" marker key.
%% On-disk tuple form: {rollbacked, TRef}.
-record(aborted, {
  tref :: reference()
}).

-endif.
