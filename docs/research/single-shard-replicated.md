# Research: mover safety & correctness on a single-shard, Replicated table

**Scenario.** A ClickHouse cluster with **one shard** whose table is a
`ReplicatedMergeTree` with several replicas (e.g. 1 shard × 3 replicas). Does our
mover — which discovers nodes from `system.clusters` and runs
`ALTER TABLE ... MOVE PARTITION ... TO DISK` on **each** node — behave correctly
and safely here?

**Short answer.** Yes, and the per-node loop is not just correct but *required*:
`MOVE ... TO DISK` is a **local, non-replicated** operation, so it has to be run
on every replica. There are a few caveats (transient merge locks, per-replica
storage policies, zero-copy replication) covered below.

---

## 1. The core question: is `MOVE ... TO DISK` replicated?

`ReplicatedMergeTree` replicates **data** and logical metadata (inserts, merges,
mutations, most `ALTER`s) via the Keeper/ZooKeeper replication log. But
`ALTER TABLE ... MOVE PARTITION|PART ... TO DISK|VOLUME` is a **physical data
movement between disks of one node**. It is deliberately **not** replicated,
because replicas may have *different* storage configurations (different disks,
different `storage_policy`). Each replica decides where its own parts live.

### Verified empirically

`internal/clickhouse/replicated_test.go` (`TestIntegrationReplicatedMoveIsNotReplicated`)
spins up **ClickHouse Keeper + two replicas of one shard**, then:

1. creates a `ReplicatedMergeTree` table and inserts data on `ch1`;
2. waits for `ch2` to replicate it (`SYSTEM SYNC REPLICA`);
3. both replicas start with the partition on the local `default` disk;
4. runs `MOVE PARTITION ... TO DISK 'cold'` **on `ch1` only`**;
5. asserts `ch1` → `cold`, and — after a grace period + `SYSTEM SYNC REPLICA` —
   `ch2` is **still on `default`**;
6. runs the move on `ch2` too → both on `cold`; row counts intact on both.

Result: **PASS** (~23s). The move on `ch1` did **not** propagate to `ch2`.

> If `MOVE TO DISK` were replicated, step 5 would fail (ch2 would already be on
> `cold`). It doesn't — confirming the operation is local.

### Consequence for the mover

To relocate a partition across the whole single-shard replicated table, the mover
**must execute the ALTER on every replica**. That is exactly what it does
(`system.clusters` → one row per replica → loop). ✅ Correct and necessary.

Running it on a single replica (or relying on replication to "spread" the move)
would leave the other replicas' parts on the original disk.

---

## 2. Node discovery on a single-shard cluster

`SELECT DISTINCT host_name FROM system.clusters [WHERE cluster = ...]` returns one
`host_name` per replica of the shard, so the mover targets all replicas. Notes:

- **Pass `-cluster`** when the server knows several clusters; otherwise
  `host_name`s from *all* clusters are unioned, which may over- or under-target.
- `DISTINCT host_name` collapses replicas that share a hostname but differ by
  port (uncommon, but possible in test/dev setups). For such topologies the
  discovery would visit that host once.
- The mover must target the **local** `ReplicatedMergeTree` table (e.g.
  `events_local`), **not** a `Distributed` table — a `Distributed` table has no
  local parts to move.

---

## 3. Correctness caveats

### 3.1 Concurrent merges → transient `PART_IS_TEMPORARILY_LOCKED` (error 384)
Each replica merges independently, so at any instant a part may be locked by a
background merge on some replicas but not others. `MOVE` on a locked part fails
with **error 384**. In practice this means the move can succeed on some replicas
and transiently fail on others.

Current behaviour: 384 is a normal ClickHouse error (not a `TransportError`), so
it is **not retried** — the node is reported `FAILED`. Mitigations today:
`-continue-on-error` to finish the other nodes, then a re-run (idempotent, the
already-moved replicas return `SKIP`). **Possible improvement:** treat 384 as a
retryable/transient error so a busy cluster self-heals within one run.

### 3.2 Per-replica storage-policy divergence
Because storage config is per-node, a replica might not have the target disk (or
uses a different policy). `MOVE` fails there and is reported `FAILED` — which is
the correct, visible outcome (it surfaces the misconfiguration rather than hiding
it).

### 3.3 Partial failure is safe for queries
`MOVE` changes only *physical placement*, never logical data. If it succeeds on
some replicas and fails on others, the table stays fully consistent and
queryable; only the on-disk location differs until a re-run converges it. No data
loss, no correctness impact on reads/writes.

### 3.4 Zero-copy replication (S3)
With `allow_remote_fs_zero_copy_replication` enabled, replicas **share** the same
physical objects in object storage. Moving parts to/from such storage per replica
interacts with reference counting and is a distinct regime from the classic
"each replica has its own copy" model tested here. Our local test infra keeps
zero-copy **off**; on clusters that use it, validate separately before bulk moves.

---

## 4. Security review (this scenario)

- **Credentials** are sent as `X-ClickHouse-User` / `X-ClickHouse-Key` headers,
  never in the URL/query string — no leakage via logs or `system.query_log` URLs.
- **Injection**: database/table are quoted as identifiers, partition/disk as
  string literals (`quoteIdentifier` / `quoteLiteral`). The generated statement
  is `ALTER TABLE \`db\`.\`t\` MOVE PARTITION[ ID] '<lit>' TO DISK '<lit>'`.
- **TLS**: HTTPS only. `-insecure` disables certificate verification (MITM risk);
  use real certs in production.
- **Blast radius**: the mover only issues `MOVE ... TO DISK`, which cannot delete
  or corrupt data; the worst case is uneven physical placement, fixed by re-run.
- Runs the ALTER on **every** replica — expected and required (see §1).

---

## 5. Recommendations

1. **Keep the per-replica loop** — it is required for replicated tables. (No
   change needed.)
2. Consider treating **error 384 (`PART_IS_TEMPORARILY_LOCKED`)** as retryable so
   concurrent-merge races resolve within a single run instead of needing a manual
   re-run. (§3.1)
3. Document clearly that the target must be the **local** `ReplicatedMergeTree`
   table, not the `Distributed` table. (Already noted in the README; reinforce.)
4. For clusters using **zero-copy replication**, treat bulk moves with extra care
   and validate on a staging replica set first. (§3.4)
5. Optionally, a **pre-flight check** (per node: does the target disk exist in
   `system.disks`? is the table `ReplicatedMergeTree`?) would turn late `FAILED`s
   into an upfront, actionable error.

## Reproduce

```sh
go test -tags=integration -run ReplicatedMoveIsNotReplicated -v ./internal/clickhouse/
```
