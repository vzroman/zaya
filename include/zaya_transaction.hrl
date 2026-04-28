-ifndef(ZAYA_TRANSACTION_HRL).
-define(ZAYA_TRANSACTION_HRL, true).

%% Per-DB rollback entry key.
%% On-disk tuple form: {rollback, DB, Seq, TRef}.
-record(rollback, {
  db   :: atom(),
  seq  :: non_neg_integer(),
  tref :: reference()
}).

%% Cluster-wide "transaction aborted" marker key.
%% On-disk tuple form: {aborted, TRef}.
-record(aborted, {
  tref :: reference()
}).

-endif.
