# Holding locks until the commit decision — spec

Status: draft, 2026-10-01. Not implemented.
Depends on: the passive holder API of elock (`docs/add_passive_holder.md` in the elock repository).

## 1. Problem

zaya's multi-node commit is two-phase. In phase 1 each participating node writes the new data, before the commit or abort decision exists. The locks of the transaction belong to the client process, so they are released as soon as that process, or its node, goes down.

If that happens between phase 1 and the decision:

1. The lock managers on the surviving nodes release the locks.
2. The commit workers on those nodes are still resolving the decision, with uncommitted data in place.
3. Another transaction locks the same keys, reads the uncommitted values and commits its own writes.
4. If the decision turns out to be abort, the rollback rewrites the old values over the second transaction's committed write.

The result is a lost update. The same happens, more rarely, when only the client process is killed during the commit while its node stays up.

The rule this spec restores: a participant keeps the locks on its node until it has applied the decision.

## 2. Scope

The change covers `multi_node_commit` in `zaya_transaction.erl`, the only path with a coordinator and workers. A transaction takes it when it changes more than one DB and the changes involve more than one node.

It has two parts:

- The workers hold the locks (§3.1–3.4).
- Every worker is guaranteed to finish, so the locks are always released (§3.5–3.7).

Not changed (see §8): `single_db_node_commit`, `single_db_commit`, `single_node_commit`.

## 3. Design

Each commit worker becomes a passive holder of the transaction's locks on its node. elock then releases a lock on that node only when the client has released it **and** the worker has exited. The worker exits after it has committed or rolled back, so the locks cover the whole in-doubt window.

### 3.1 Client process: collect the locks per node

The lock context of elock lives in the client process, so this step must run there, before the coordinator is spawned.

- Take every lock reference of the transaction from `#transaction.locks`: key locks of both types and the DB locks.
- For each reference call `elock:get_managers(Ref)`, which returns `#{Node => Manager}`.
- Build `#{Node => [{Manager, Ref}]}`.

`commit/2` does not receive the locks today. Pass them from `run_transaction/2` down to `multi_node_commit`; the other commit paths ignore them.

All the locks are passed, not only the write locks. The write locks and the DB locks are the ones the correctness argument needs. The read locks are included to keep the rule simple: a participant keeps everything the transaction holds on its node. Narrowing it to write and DB locks is possible later and would make fewer keys wait behind an in-doubt worker.

### 3.2 Coordinator: pass each worker its node's locks

- `#context{}` gains `ns_locks :: #{node() => [{pid(), reference()}]}`.
- `#commit_request{}` gains `locks = [] :: [{pid(), reference()}]`.
- `spawn_workers/1` sets `locks = maps:get(N, NsLocks, [])` for the worker of node `N`.

A node can have a worker and no locks, for example when it became available for the DB after the locks were taken. Its list is empty and its worker holds nothing.

### 3.3 Worker: hold the locks first

A new first step of phase 1 in `commit_request/1`, before `phase_1_prepare`:

- For each `{Manager, Ref}` call `elock:add_passive_holder(Manager, Ref)`.
- If every call returns `ok`, continue with phase 1.
- If any call returns `{error, not_held}`, abort through the existing abort branch with the reason `{lock, not_held}`.

The step must come before `phase_1_prepare`. That function reads the values the rollback will restore, and those values are only trustworthy while the keys are locked.

On the abort branch the worker has written no data. `rollback/1` logs the `#aborted{}` marker for a persistent transaction and the worker exits with `{lock, not_held}`. The coordinator aborts the other workers as it does for any phase 1 failure.

The reason has the `{lock, _}` form on purpose. If the client is still alive, `run_transaction/2` treats it as a lock error and restarts the transaction, while attempts remain.

### 3.4 Release

No code. The passive holds end when the worker process exits, which is after `committed/1` or `rollback/1`. The lock on a node is released when both the client has unlocked and that node's worker has exited.

### 3.5 Coordinator: announce a worker lost in phase 2

Because the locks now live as long as the worker, every worker must finish. A worker that never learns the decision does not. This step closes the most likely way for that to happen; §3.6 and §3.7 cover the rest.

Today, when a worker goes down while the coordinator waits for the `commit2` confirmations, the coordinator drops it from the pending list and still exits `normal`. The surviving workers rebroadcast `commit` only after an abnormal coordinator exit, so nobody tells the lost worker. If it is alive and did not receive `commit`, it stays in doubt.

The change:

- `wait_commit2/2` remembers the workers that went down instead of only logging them, and keeps collecting the confirmations of the others.
- If no worker was lost, the coordinator exits `normal` as today.
- If any was lost, it exits with `{committed, LostWorkers}`.
- `multi_node_commit/2` treats `{committed, _}` as success, the same as `normal`.

The decision stays commit. It must not be turned into an abort at this point: `commit` has already been sent, and the lost worker may have received it and committed.

The workers need no change. The survivors are waiting in `maybe_broadcast_committed/2`, where any non-normal coordinator exit makes them send `commit` to every other worker. A lost worker that is alive and in doubt takes it in `wait_for_decision/2` and commits. If the lost worker's node is down, the message goes nowhere, and that node recovers from its log as today.

### 3.6 Worker: announce the commit on the timeout too

A worker that received `commit` waits in `maybe_broadcast_committed/2` for the coordinator's exit, for at most 60 seconds. On the timeout it logs an error and finishes without telling anyone.

The change: on the timeout it also sends `commit` to every other worker, as it does after an abnormal coordinator exit. The error is still logged.

With §3.5 this gives the rule the deadline of §3.7 relies on:

> A worker that has decided commit announces it to every other worker, unless the coordinator exited `normal`. The coordinator exits `normal` only when every worker has confirmed the commit, so then nobody is in doubt.

### 3.7 Worker: a deadline for the in-doubt wait

`wait_for_decision/2` has no overall timeout today. It gets a deadline, counted from the moment the worker enters `resolve_decision/1`.

- When the deadline passes without a decision, the worker decides **abort**. It rolls back through the existing path, which logs the `#aborted{}` marker for a persistent transaction, and exits. Its locks are released.
- It logs a warning with the transaction reference and the nodes it was still waiting for.
- The deadline is checked on every pass of the loop. The loop already wakes up every `?WORKER_RESOLUTION_POLL_MS`.
- The marker poll must not delay the deadline. `zaya_transaction_log:is_aborted/1` waits for the log to start on a node that is still booting, up to `ref_wait_timeout_ms` (600 seconds by default), so a poll of such a node can outlast the deadline unless the call is bounded.

Why abort is the right verdict: by the rule of §3.6, a commit would have been announced within the announce timeout. Silence for longer than that means no worker decided commit. Other in-doubt workers of the same transaction reach the same verdict, by their own deadline or by finding the marker.

The worker must not exit undecided, because that would release the locks over in-doubt data. It must not commit either, because in the cases where no announcement comes the coordinator died before deciding.

**The two timeouts are one setting.** The deadline is only safe if it is longer than the announce timeout of §3.6.

- The literal `60000` in `maybe_broadcast_committed/2` becomes a value read from the application environment, default 60000 ms.
- The deadline is twice that value, 120 seconds by default. It is derived, not configured separately, so the two cannot drift apart.
- Tests shorten the one setting.

## 4. How the failures play out

**The coordinator's node dies after phase 1.**
- The managers on the other nodes see the client down. The locks stay, held by the workers.
- The workers resolve the decision, apply it and exit. Only then are the locks released.
- A second transaction waits for the locks during this time and then reads resolved data.

**The client process is killed during the commit, its node stays up.**
- The coordinator is not linked to the client and carries on.
- On a node where the worker already holds the locks, they stay until the worker exits.
- On a node where the manager released the lock before the worker asked, the worker gets `{error, not_held}` and exits before writing anything. The coordinator aborts the transaction on the other nodes, whose locks are still held, so the rollback is safe.

**A lock is lost before the commit for another reason**, such as a crashed manager.
- Same as the previous case: the worker aborts before writing, and a live client restarts the transaction.

**A worker loses the coordinator in phase 2 before `commit` reaches it.**
- The worker is in doubt and keeps its locks.
- The coordinator sees it down, collects the other confirmations and exits with `{committed, _}`. The client gets success.
- The other workers announce `commit`. The in-doubt worker commits and exits, and its locks are released.

**No decision ever reaches an in-doubt worker.**
- It keeps its locks until the deadline, then rolls back and exits.

## 5. Behaviour visible outside

- **Locks may outlive `zaya:transaction/1` briefly.** After a multi-node commit the call can return before the workers have exited. A worker purges its log and notifies subscribers before it exits, and the locks on its node are held until then. A transaction that immediately locks the same keys waits for that.
- **A new abort reason.** `{lock, not_held}`, reported when a worker could not take over a lock.
- **An in-doubt transaction is always resolved.** A participant that cannot learn the outcome aborts on its node after the deadline, 120 seconds by default.
- **One new setting.** The announce timeout in the application environment, default 60000 ms. The deadline is twice its value.

## 6. Remaining limitations

The locks on a node live as long as that node's commit worker. The deadline of §3.7 bounds that life, so no lock is held forever. What remains:

- **A wait of up to the deadline.** Two cases leave an in-doubt worker with nobody to tell it the outcome. It then aborts at the deadline, which is the correct outcome in both, but until then the keys stay locked on its node (120 seconds by default).
  - A worker fails phase 1 of a transaction that touches no persistent DB, so it logs no `#aborted{}` marker, and the coordinator dies before it broadcasts the abort.
  - The coordinator's node dies while it is still spawning the workers, so a participant never gets one.
- **A lost announcement.** The verdict at the deadline is wrong if some worker decided commit and its announcement never reaches the in-doubt worker. That takes the announcing worker being killed before it sends, or the message being undeliverable while the in-doubt worker still sees that node as up. The in-doubt worker then rolls back while the others committed, and its copy differs from theirs. Before this change it would have waited forever with the committed data in place.
- **An isolated node**, unchanged. A worker that sees every other participant down aborts at once, as today.

These cases were found by reading the code and have not been reproduced.

Decisions of 2026-10-01: the coordinator's exit of §3.5, the announcement of §3.6 and the deadline of §3.7 are part of this change. The limitations above are accepted.

## 7. Performance

Figures are from a micro-benchmark on one machine (OTP 27) with stand-in manager processes. Treat them as order of magnitude.

- **Phase 1 on each node** gains one local call per lock: 2–3 µs per lock, so 2–3 ms for 1000 locks. The nodes do this in parallel. For comparison, taking a local lock costs 4–10 µs.
- **No extra network round trips.** The lock list travels in the commit request, which grows by one pid and one reference per lock.
- **Client side:** one map lookup per lock to build the per-node lists.
- **Lock hold time** grows by what the worker does between the decision and its exit: purging the transaction log and notifying subscribers. This matters for back-to-back transactions on the same keys.
- **Other commit paths** are not affected.

If the per-lock calls or the longer hold time become visible, elock can add a batch call and an explicit release. Neither is part of this spec.

## 8. Non-goals

- **The single-node and single-DB commit paths.** They also keep writing after the client's locks are released if the client dies during the commit. The window is much narrower because there is no in-doubt wait. They can adopt the same mechanism later.
- **Repairing a copy that diverged** through a lost announcement (§6).
- **Dirty reads.** A read without a lock can still see phase 1 data.

## 9. Rollout

- **elock.** zaya needs an elock version with `get_managers/1` and `add_passive_holder/2`. Update `rebar.lock`.
- **All nodes together.** `#commit_request{}` gains a field, so a commit request between an old and a new node fails and the transaction aborts. The announce rule of §3.6 also holds only when every participant runs the new code. Upgrade the nodes of a cluster together.

## 10. Tests

In `test/zaya_transaction_SUITE.erl`. The suite keeps its own copy of `#commit_request{}`, which must get the new field.

Worker driven directly, in the style of `multi_node_worker_rolls_back_after_coordinator_decision_test`. A helper process takes the elock locks the transaction would take and hands over `[{Manager, Ref}]`:

- The worker reaches `commit1`, the lock owner is killed: the keys are still locked. After `abort` and the worker's exit, the keys are free and hold the old values.
- The same with `commit`: the keys are free after the worker's exit and hold the new values.
- The coordinator process dies instead of deciding: the keys stay locked until the worker has resolved and exited. A second writer started meanwhile is granted after that, and its write is not overwritten.
- The locks are released before the worker starts: the worker exits with `{lock, not_held}`, sends no `commit1`, writes nothing to the backends and leaves no pending transaction.
- An empty lock list: the worker behaves as today.
- After `commit`, the coordinator process exits with `{committed, _}`: the worker sends `commit` to every other worker in the list it was given.
- After `commit`, the coordinator process stays alive past the announce timeout: the worker sends `commit` to every other worker and then finishes.
- An in-doubt worker waits for a participant that is up and silent: after the deadline it rolls back, logs the `#aborted{}` marker for a persistent transaction, exits, and its keys are free with the old values.
- An in-doubt worker receives `commit` from another worker before the deadline: it commits.

The cases that wait for the announce timeout or the deadline shorten the setting of §3.7.

Whole transaction:

- A transaction over two DBs on two nodes commits: every lock becomes free on every node.
- A worker is lost during phase 2: the call returns success, the surviving copies hold the new values, and a lost worker that is still alive commits and releases its locks.
- The same transaction aborted by a failing participant: every lock becomes free and no copy keeps the new values.
- The caller is killed during the commit: every lock becomes free, and the copies agree on either the old or the new values.

Regression: the existing cases pass. Any assertion that locks are free straight after a multi-node commit must wait for it, as `wait_until/1` does.
