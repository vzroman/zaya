# Epoch-Merged Asynchronous Replication — Architecture Draft

Status: draft, revision 2. Captures the design points agreed so far. Partition handling, rejoin, replay and anti-entropy are out of scope (see §9). DB rows carry no metadata (§3.3).

## 1. Goals and guarantees

- Multi-master: any node accepts writes for any key.
- Eventual consistency with last-writer-wins (LWW) over a total order of stamps. Concurrent writes to one key converge to the same winner on every node; losers are discarded.
- A write acknowledgment means: applied to the local backend. Replication to other copies is asynchronous.
- The write path never blocks on other nodes. Only epoch rotation (memory reclamation) waits on the slowest proxy in the cluster.
- Per-key ordering is preserved. Cross-key ordering from a single client is not: writes to different keys go through different proxies and may replicate in either order.
- Failure scope: a colleague that goes down takes with it any writes it had not yet delivered. Recovering them is deferred (§9). Within that scope, replicas that stay up never diverge.

## 2. Components (per node, per replicated DB)

| Component | Count | Role |
|---|---|---|
| Ticker | 1 | Agrees on epoch numbers with neighbor tickers; issues `switch` to local proxies |
| Proxy | P (identical on all nodes) | Owns key bucket `phash(Key) rem P`; merges, writes to the DB, replicates to colleagues |
| Merge table T(N) | per proxy, per live epoch | Private ETS `set`; entries `{Key, Stamp, Value}` |
| Backend DB | 1 | Durable store; plain `Key -> Value` |

**Colleagues.** Proxy `i` on node A and proxy `i` on node B are colleagues. Each proxy has exactly one colleague per neighbor. All replication traffic for bucket `i` flows only between the colleagues `i`.

**Transport.** `ecall:send` to the colleague pid. FIFO per (sender, receiver) pair, same guarantee as native `!`. Fire-and-forget. Note that `ecall` traffic and Erlang distribution (monitors) are separate paths with no ordering between them; §7 accounts for this.

## 3. Data model

### 3.1 Stamp

`{Epoch, Node, Seq}`, compared by Erlang term order.

- `Epoch`: cluster-agreed integer, incremented on each rotation.
- `Node`: origin node name; deterministic tiebreak within an epoch. Optionally rotate the priority per epoch (`Epoch rem NodeCount`) so no node always wins.
- `Seq`: per-proxy monotonic counter. Required by the merge rule itself: two same-origin writes to one key in one epoch must compare strictly, or `Stamp > Best` rejects the second. Two proxies on one node may emit equal stamps; they own disjoint keys, so the stamps never compete.

Rules: higher epoch wins; equal epoch → higher `Node`; equal → higher `Seq`.

### 3.2 Entry

`{Key, Stamp, Value}` where `Value` is the payload or `'$deleted'` (tombstone). Tombstones are ordinary entries and go through the same comparison. They exist only in merge tables.

### 3.3 DB row

`Key -> Value`. Deletes are physical. The DB stores no stamp and no tombstone.

Rationale: on the hot path the stamp is never read and a tombstone is never consulted. Both would serve only a conditional path for writes whose epoch table is gone, and every source of such writes (partition, rejoin, replay) is out of scope. Consequence recorded in §9.

## 4. Write path

### 4.1 Local write

1. Client sends `{write, Key, Value}` or `{delete, Key}` to proxy `phash(Key) rem P`.
2. Proxy builds `Stamp = {Active, node(), Seq + 1}`.
3. Merge (§4.3) against the retained tables. If the write loses to a higher stamp already present, it is discarded and acknowledged as superseded.
4. If it wins: send `{write, Active, Key, Stamp, Value}` to every live colleague, then write to the DB, then ack the client.

Send before the DB write: an acknowledged write is never present only on a node that may die.

Batching: the proxy drains its mailbox, merges the batch, issues one multi-put, and acks the batch. Per-key order is preserved inside one proxy.

### 4.2 Remote write

Colleague receives `{write, Tag, Key, Stamp, Value}`.

- If it holds T(Tag): merge (§4.3). If the write wins: apply to the DB (batched). Never re-sent.
- If it holds no table for Tag: the write is **not applied**. Increment a straggler counter and log it. Invariants I3, I5 and the grace rule in §7 make this unreachable in scope; a non-zero counter is a bug signal, not a normal event.

A blind write here would be the worse choice: it could roll back a newer value written by a live client, whereas dropping can only lose a write from a colleague that is already gone.

### 4.3 Merge rule

```
Best = highest Stamp for Key across all retained tables (none if absent)
if Best == none or Stamp > Best:
    ets:insert(T(Tag), {Key, Stamp, Value})   % replaces value or tombstone alike
    apply to DB (put, or delete if Value == '$deleted')
else:
    drop
```

A blind DB write on a miss is safe on the hot path: every write to `Key` with a higher epoch that this proxy has applied is still in a retained table (invariant I5).

Deletes: same path with `Value = '$deleted'`. A winning delete replaces the table entry with a tombstone and issues a physical DB delete. The tombstone must stay in the table: removing it would let a concurrent write arriving later find a false miss and resurrect the key on one node only. It leaves with its epoch's table.

## 5. Epoch rotation

### 5.1 Ticker

- Any ticker whose timer fires proposes `N+1` to neighbor tickers; receivers adopt. Proposals are idempotent by number.
- On adopting `N+1`, the ticker sends `{switch, N+1}` to all local proxies and restarts its timer.
- The ticker neither proposes nor forwards `N+2` until every local proxy has reported `{activated, N+1}`. Tables per proxy stay at two (plus retained), activation stays monotonic, and cadence follows the slowest proxy in the cluster.

### 5.2 Proxy handshake, N → N+1

1. On `{switch, N+1}`: create T(N+1); send `{ready, N+1}` to every live colleague. Local writes remain tagged N.
2. Activate N+1 when either:
   - `{ready, N+1}` has arrived from every live colleague, or
   - any write tagged N+1 arrives. This is implicit readiness: the sender could only have activated after readiness from everyone, including this proxy, so T(N+1) exists everywhere.
   On activation: `Active := N+1`; send `{marker, N}` to every live colleague on the write channel; report `{activated, N+1}` to the ticker.
3. Drop T(N) when `Active >= N+1` and `{marker, N}` has arrived, or been substituted per §7, from every colleague.

`{marker, N}` is the last message tagged N on its channel; FIFO turns it into a drain proof for the receiver. The process that consumes the writes is the one that declares the epoch drained.

### 5.3 Messages

| Message | Channel | Meaning |
|---|---|---|
| `{propose, N}` | ticker → ticker | next epoch |
| `{switch, N}` | ticker → local proxies | create T(N) |
| `{ready, N}` | proxy → colleagues | T(N) exists here |
| `{write, Tag, Key, Stamp, Value}` | proxy → colleagues | replicated write |
| `{marker, N}` | proxy → colleagues | no more writes tagged N from me |
| `{activated, N}` | proxy → local ticker | active epoch is now N |

Cost per node per epoch: about 2·P·(R−1) colleague messages plus P ticker messages, negligible at any practical P and epoch length.

## 6. Invariants

- **I1.** All writes to a key, on every node, pass through the proxy for its bucket. One writer per key per node; merge plus DB write is atomic without CAS.
- **I2.** Colleague channels are FIFO.
- **I3.** A proxy sends writes tagged N+1 only after every colleague holds T(N+1).
- **I4.** `{marker, N}` follows every write tagged N on its channel.
- **I5.** T(N) is dropped only after local activation of N+1 and a marker, real or substituted after grace, from every colleague. Hence no hot-path write tagged ≤ N arrives after the drop.
- **I6.** Merge is LWW over a total order: commutative, associative, idempotent. Replicas that see the same set of writes converge regardless of order or duplication.
- **I7.** A write whose tag has no table is never applied.

## 7. Failure handling (in scope)

- Each proxy monitors its colleagues. On `'DOWN'` the colleague leaves the live set: writes are no longer sent to it, and it counts as `ready` immediately.
- **Grace rule.** The `'DOWN'` substitutes for `{marker, N}` only after a grace period: one extra epoch, or a fixed G milliseconds, whichever is simpler to implement. Reason: the `'DOWN'` arrives over distribution while the colleague's last writes may still be in `ecall`'s receive buffer; the grace lets them drain into the mailbox and merge normally before T(N) can be dropped. Without it, I7 turns those writes into dropped stragglers.
- Tickers proceed without unreachable neighbors. Membership under partition: deferred.
- Backpressure: `ecall:send` is unbounded. Rely on transport buffer-busy behavior; an explicit lag signal is deferred with ack-based retention (§9).
- Proxy crash: supervisor restarts it; colleagues re-pair on the new pid. Lost mailbox contents: deferred (replay).

## 8. Tunables

| Parameter | Effect |
|---|---|
| Epoch length | LWW concurrency window; memory per proxy (distinct keys per epoch); rotation message rate |
| P (proxies per node) | Write parallelism against the DB; must be identical across nodes |
| Batch size | DB round trips per write versus ack latency |
| Grace after `'DOWN'` | How long a dead colleague's buffered writes may still be merged before its epoch table is dropped |

## 9. Deferred

- Partition handling, rejoin, replay and anti-entropy, including membership and ticker agreement under partition and epoch reconciliation after a heal.
- Ack-based retention via `{drained, N}` as both a replay log and a lag meter for backpressure.
- Read path semantics.

**Recorded consequence of §3.3.** Because rows carry no stamp, no future repair mechanism can order a replayed write against what the DB already holds. When rejoin is designed, it will need either a stamp column added at that time (a rewrite of every row) or bucket re-copy from a peer with the bucket paused during the copy. Real deletes likewise mean a repair cannot distinguish "deleted" from "never written"; the same two options apply.

## 10. Open decisions

- Backend, and whether it supports a batched multi-put.
- Tiebreak within an epoch: fixed node order or rotating priority.
- Ack semantics: local durable (current) or wait for one colleague.
- Target epoch length.
- Grace after `'DOWN'`: one epoch or a fixed interval.
