# Serializable Snapshot Isolation & Optimistic Concurrency Control — Design Document

**Status:** Draft (Proposal)
**Date:** 2026-05-29
**Author:** rohit@dragonscale.ai
**Crates touched:** `uni-store`, `uni-query`, `uni-cypher`, `uni-common`, `uni`
**Related:** `docs/proposals/concurrent_writer.md`, `docs/proposals/async_l0_to_l1_flush.md`

> All `file:line` references in this document were verified against `HEAD 50695fe39`
> (branch `feat/locy-timeout-hard-error`). Line numbers will drift; the symbol names
> are the durable anchors. Sites that are **new** (no code today) are marked *(new)*.

---

## 1. Summary

uni-db's write path is **last-writer-wins with no conflict detection**. When two
transactions read the same logical row, compute a new value, and commit, the second
commit silently overwrites the first — a classic lost update. This is verified at
`crates/uni-store/src/storage/manager.rs:1964` and `:2070` ("last writer wins") and
`crates/uni-store/src/runtime/l0.rs` `merge()` (`~1030-1113`): the merge overwrites
without any version check or abort signal.

Every other graph/relational database makes a write-write collision **observable** —
either by blocking (Neo4j, Neptune) or by aborting-and-retrying (DuckDB, ArangoDB,
Dgraph, Memgraph). uni-db is the lone outlier that drops the loser on the floor. The
downstream consequence is documented in `uniko2/bugs/uni-db-rmw-primitives-wishlist.md`:
`uniko-store` carries an in-process striped mutex (`StripedLocks`) solely to serialize
read-modify-write against uni-db, and asks upstream for (1) server-side atomic SET,
(2) serializable MERGE, (3) row-level locks.

This document proposes the root-cause fix: **MVCC snapshot-isolation reads plus
optimistic commit-time conflict detection**, layered onto the already-serialized
`flush_lock` commit point. One mechanism subsumes all three wishlist requests.

The concurrency-control model is the single most irreversible decision in a database,
so the choice is made for **uni-db's** long-term trajectory, not just uniko's needs.
uni-db is embedded, OLAP-first (vector search, Locy datalog, graph analytics), with a
Lance-backed append-only versioned substrate and scan-heavy read workloads. That
profile rules out pessimistic read-locking (a lock manager on the Locy/ALONG scan path
would deadlock and lock-explode) and points squarely at optimistic CC — the model
DuckDB (uni-db's stated sibling) and every distribution-ready SI/SSI system uses.

`★ Insight ─────────────────────────────────────`
- The hard part of OCC — a serialization point for the validate phase — **already exists**.
  `commit_transaction_l0` holds `flush_lock` (`writer.rs:456`) across every commit; the
  `concurrent_writer` refactor (landed, Phase 4, `a5549c9c0`) made it the sole commit
  serialization point. Validation drops into that critical section with no new global lock.
- `Transaction::started_at_version` (`transaction.rs:126`) is **already captured** at
  tx-begin and currently **unused for read filtering** — exactly the snapshot timestamp OCC needs.
- This is the safety layer `concurrent_writer` *requires* to be correct: the moment commits
  run concurrently to widen throughput, silent LWW becomes data loss, not a curiosity.
`─────────────────────────────────────────────────`

---

## 2. The decision and its scope

### 2.1 Model: optimistic, not pessimistic

| Candidate | Verdict for uni-db |
|---|---|
| Global single-writer-for-tx-duration (SQLite) | Correct but zero write concurrency; contradicts `concurrent_writer`. |
| Fine-grained pessimistic row locks (Neo4j/Postgres/Neptune) | **Rejected** — lock manager + deadlock detection on a scan-heavy read path; incompatible with Locy fixpoint (`locy_fixpoint.rs:1175-1399`) and variable-length traversal. |
| **Optimistic CC** (DuckDB/Arango/Dgraph/Memgraph) | **Chosen** — snapshot reads never lock; validate at the existing serialized commit; distribution-ready. |

Prior art confirms the universal patterns we adopt: write-set conflict detection for
lost updates (DuckDB/Arango error-1200/Memgraph abort), unique-constraint-as-serialization-point
for MERGE (Neo4j schema lock, Postgres `ON CONFLICT`, Arango unique index), and a
pessimistic opt-in escape hatch for hot keys (Arango `exclusive:true`, Postgres `FOR UPDATE`).

### 2.2 The precise guarantee we ship

A deliberately scoped guarantee, because uniform textbook serializability collides with
uni-db's analytical core (a Locy fixpoint reads ~the entire reachable closure; tracking
that as an SSI read-set yields >90% false aborts and GB-scale read-sets — measured-by-reasoning
in the feasibility pass, see §7):

- **Read-write transactions** → **Serializable Snapshot Isolation (SSI), item-level.**
  Write-set conflict detection (first-committer-wins) + item-level read-set conflict
  detection. This prevents lost updates and read-write anomalies on **existing** items.
- **Read-only transactions** (`session.query()`, Locy, vector, graph algos) →
  **Snapshot Isolation.** They never commit a write, so they cannot lose updates or be a
  write-conflict pivot. They never track a read-set and never abort anyone.
- **Phantoms** (a concurrent insert of a *new* row matching a predicate) are **not**
  prevented by item-level tracking. Phantom-sensitive transactions opt in to the
  **`FOR UPDATE`** pessimistic escape hatch (§6.5).

> **Honesty note (verified):** a single-key `MATCH (c:Counter {id:'x'})` is a *filtered
> label scan* (`read.rs:224` → `scan_vertex_candidates`), and even
> `IndexManager::composite_lookup` (`index_manager.rs:694`) is a scan-with-`LIMIT 1`, not a
> true index point-lookup. So "keyed access" reads are predicates, not single keys, and our
> guarantee is **SSI with item-level conflict detection, not 1-copy-serializability**. The
> lost-update/RMW cases the wishlist needs are nonetheless fully covered, because those are
> caught by *write-set* conflict detection on the existing row, independent of how the read ran.

---

## 3. Architecture overview

Reads resolve against a **pinned snapshot**; writes accumulate in `tx_l0`; commit
**validates then publishes** inside `flush_lock`:

```
tx-begin:  capture snapshot = (L0 snapshot handle, pending-flush handles, base Lance versions,
                               started_at_version)                          [§4, §5]
execute:   reads → snapshot (read-your-writes via live tx_l0)               [§4]
           RW txns record read-set + write-set                              [§6.3]
commit:    acquire flush_lock  (writer.rs:456)
           ── VALIDATE (before any WAL write) ──                            [§6.4]  *** the fix ***
              • write-set vs committed-since-snapshot registry  → SerializationConflict
              • read-set  vs committed-since-snapshot registry  → SerializationConflict
              • MERGE: tx_l0.constraint_index vs main_l0.constraint_index → ConstraintConflict
           ── if OK: WAL append (473-527) → flush_wal (534) → main_l0.merge (541) ──
           record this commit's write-set into the registry; release flush_lock
```

---

## 4. Component C1 — L0 snapshot-isolation reads

**Goal:** convert L0 reads from live `Arc<RwLock<L0Buffer>>` references to a pinned,
consistent snapshot captured at tx/query begin.

**Verified current state:** `QueryContext` holds three live references —
`l0`, `transaction_l0`, `pending_flush_l0s` (`crates/uni-store/src/runtime/context.rs:12-16`).
Reads walk the chain `tx_l0(live) → main l0 → pending_flush` in e.g.
`lookup_vertex_prop` (`l0_visibility.rs:93-130`), `lookup_edge_prop` (`:132-169`),
`accumulate_vertex_props` (`:171-213`), `overlay_vertex_batch` (`:298-328`),
`overlay_edge_batch` (`:354-383`). The chain order is load-bearing: **keep `tx_l0`
live** (read-your-writes), snapshot the rest.

**Change-sites:**

| Site | File:line | Change |
|---|---|---|
| `L0Snapshot` struct *(new)* | `runtime/l0.rs` near struct `L0Buffer` (`:87-159`) | Immutable handle wrapping the **13 reader-consulted fields**: `graph`(:89), `edge_properties`(:99), `vertex_properties`(:101), `edge_endpoints`(:103), `vertex_labels`(:106), `label_to_vids`(:109), `edge_types`(:111), `tombstones`(:91), `vertex_tombstones`(:93), `vertex_versions`(:97), `edge_versions`(:95), `constraint_index`(:137), the four `*_created_at`/`*_updated_at` maps(:124-130). **Exclude** write-only metadata (`vertex_partial_keys`:144, `edge_partial_keys`:151, `pending_embeddings`:158, `mutation_*`, `wal`). |
| `QueryContext` | `runtime/context.rs:12,16` | Replace `l0` and `pending_flush_l0s` live refs with pinned snapshot handles; **keep `transaction_l0`** (`:13`) live. |
| Read functions | `l0_visibility.rs:93-130, 132-169, 171-213, 298-328, 354-383` | Read from the snapshot handle instead of taking `.read()` guards on `l0`/`pending`. |
| Snapshot capture | `runtime/l0_manager.rs` `begin_flush`(`:82-90`)/`rotate`(`:65-77`) | Produce a snapshot handle at rotate time; `pending_flush` already holds the Arc (`:88`) so a pinned snapshot cannot be freed under a reader. |

`★ Insight — Correction from verification ─────────`
A naïve `L0Snapshot` that `clone()`s the L0 `HashMap`s at every tx-begin is **too costly**:
the flush threshold is `auto_flush_threshold: 10_000` mutations (`config.rs` default; gate at
`writer.rs` `should_flush` ~`:2383-2398`), so the main L0 reaches MB-scale and an O(n) clone
runs on the hot read path. `L0Buffer` already documents clone cost (`l0.rs` `impl Clone` ~`:174-207`).
**Use structural sharing** — switch the snapshotted maps to `im::HashMap` (persistent, O(log₃₂ n)
snapshot; ~28-35% slower inserts, but inserts run on the single-threaded rotate/flush cold path,
not the read path) — or benchmark a copy-on-write alternative before committing. This is a
prerequisite, not an optimization.
`─────────────────────────────────────────────────`

**Effort:** Medium. **Risk:** Low (additive; live path stays as fallback during rollout).

**Stage-1 benchmark outcome (2026-05-29, `benches/l0_snapshot_bench.rs`).** Four
strategies measured across scalar/embed384/embed768 × 1k/10k/50k:

| Strategy | Snapshot @10k/embed768 | Memory @10k/embed768 | Read tax | Write tax |
|---|---|---|---|---|
| **A** std `L0Buffer::clone` | **13.0 ms** | 58 MB, 160k allocs | — | — |
| **B** `imbl::HashMap` | 15 ns | ~0 | **+44% `get`** | **+41% insert** |
| **C/D** `Arc::clone` | **9 ns** | **0 B, 0 allocs** | none | none |

Decision: **strategy D (epoch/generation pinning).** A is out (13 ms / 58 MB per
snapshot, 74 ms / 281 MB at 50k); B is vetoed (cheap snapshots but a permanent +44%
read tax on a scan-heavy engine); C/D snapshot in 9 ns with zero allocations and **zero
reader regression** (readers dereference the same `L0Buffer` through the `Arc`). D beats
C because C's `Arc::make_mut` pays the full 13–74 ms deep clone on the first write after
a snapshot, while D rotates the frozen generation aside (O(1), no deep clone ever).

**Prototype landed** behind the default-off `l0-snapshot` feature:
`L0Manager::snapshot_isolated` (`l0_manager.rs`) freezes the current buffer via
`rotate` and keeps it readable via the pending list; the existing `QueryContext` +
`l0_visibility` reader chain is reused unchanged (no reader rewrite needed). Correctness
test `snapshot_isolated_from_later_writes` passes, and the existing
`test_transaction_l0_takes_precedence` (read-your-writes) still passes with the feature on.

---

## 5. Component C2 — Lance base-read version pinning

**Goal:** pin base (L1/L2) reads to the transaction's snapshot so a concurrent flush
cannot leak newer committed data into a running reader's base scans.

**Verified current state — and a correction.** The plumbing *looks* present:
`SnapshotManifest` (`uni-common/src/core/snapshot.rs:9-20`) has `version_high_water_mark`(:15)
and per-table `LabelSnapshot.lance_version`(:26) / `EdgeSnapshot.lance_version`(:33);
`StorageManager` has `pinned_snapshot`(`storage/manager.rs:75`), `.pinned()`(`:409`),
`get_edge_version_by_id`(`:521`), `version_high_water_mark`(`:534`),
`apply_version_filter`(`:544`); and `VertexDataset::open_at(Option<u64>)` exists
(`storage/vertex.rs:96`, `checkout_version` at `:98-99`).

`★ Insight — Correction from verification ─────────`
**This is a stub, not a wired path.** (a) `lance_version` is **hardcoded to `0`**
(`writer.rs:3317`, `:3480`, comment: *"LanceDB tables don't expose Lance version directly"*).
(b) `SnapshotManifest` is produced **only at flush boundaries**, not per-transaction.
(c) Base reads always open **latest** — `get_or_open_table` (`backend/lance.rs:127-133`) ignores
version, and `open_at` is never called with `Some(v)`. So base reads are **not** snapshot-isolated
today, and the global-version→Lance-version map does not actually exist. Original "SMALL/wiring"
estimate was wrong; this is **MEDIUM**.
`─────────────────────────────────────────────────`

**Change-sites:**

| Site | File:line | Change |
|---|---|---|
| Populate real versions | `runtime/writer.rs:3317, 3480` | Replace `lance_version = 0` with `get_table_version()` (exists at `backend/lance.rs:530`, returns `table.version()`), captured at flush finalize under `flush_lock`. |
| Per-tx version capture *(new)* | `api/transaction.rs` tx-begin (`:177-186`) | Capture a consistent per-table Lance-version set into the snapshot handle alongside `started_at_version`. |
| Wire base reads | `storage/manager.rs` dataset factories; `storage/vertex.rs:92,96` | When the tx has a pinned snapshot, pass `Some(version)` to `open_at` instead of `None`; combine with existing `apply_version_filter`(`:544`). |

**Design note — is full base pinning even required?** Base data changes *only* at flush,
and flush is serialized at `flush_lock`. For short RW transactions, flush-epoch
granularity on base + a precise L0 snapshot is already consistent. Full per-tx base
pinning matters for **long read-only analytical queries** (Locy/vector) that must not
see a flush that lands mid-query. Since those are exactly the SI workloads we promised
to protect, C2 is in-scope — but it can land **after** C1+C3+C4 (it only tightens
read isolation; it is not on the lost-update critical path).

**Effort:** Medium. **Risk:** Medium (touches flush finalize and the scan path).

---

## 6. Component C3/C4 — read-set, write-set, committed registry, and commit-time validation

This is the heart. Split into the data the transaction carries (C3) and the validation
that runs at commit (C4).

### 6.1 Verified commit path (the chokepoint)

`Writer::commit_transaction_l0` (`runtime/writer.rs:447-652`), all under `flush_lock`:

| Step | Line | Quote |
|---|---|---|
| acquire `flush_lock` | `:456` | `let _flush_lock_guard = self.flush_lock.lock().await;` |
| WAL append loop | `:473-527` | `wal.append(&Mutation::InsertVertex { … })` |
| **`flush_wal()` — durable commit point** | `:534` | `let wal_lsn = self.flush_wal().await?;` |
| `main_l0.merge(&tx_l0)` | `:541` | `main_l0.merge(&tx_l0)?;` |
| return / release lock | `:652` | `Ok((wal_lsn, flush_pending))` |

`★ Insight — Correctness fix (verified against WAL replay) ─`
The WAL has **no commit marker**: `replay_since` + `L0Buffer::replay_mutations`
(`l0.rs:1119-1193`) replay *every* flushed mutation; each record is self-committing.
Therefore an aborting validation MUST run **after `flush_lock`@`:456` and BEFORE the WAL
append@`:473`**. Validating after `flush_wal`@`:534` would let a crash resurrect a
"rolled-back" transaction on recovery. The earlier draft's "validate after merge@`:541`"
suggestion was a latent correctness bug; it is corrected here.
`─────────────────────────────────────────────────`

### 6.2 Global commit version

**Verified gap:** there is **no per-commit monotonic sequence** today; `current_version`
increments **per-mutation** (`l0.rs:391,517,578,594,674,728,844`), and
`started_at_version` (`transaction.rs:126`) snapshots the main L0's counter at begin
(`:177-186`). We add a single monotonic **commit-sequence counter** on `Writer`, bumped
once per successful commit under `flush_lock`. (This is the version stamped into the
committed-writes registry and returned in `CommitResult`.)

### 6.3 Transaction read-set / write-set (C3)

| Site | File:line | Change |
|---|---|---|
| `Transaction` fields *(new)* | `api/transaction.rs:105-133` | Add `read_set` and `write_set` (item-level: `HashSet<Vid>` + `HashSet<Eid>`, plus touched property keys). Gate population on read-write isolation. |
| Gate on RW | `api/session.rs:828` (`validate_read_only`), `transaction.rs:39-45` (`IsolationLevel`) | Read-only queries skip read-set tracking entirely (they run at SI). |
| Write-set source | `tx_l0` | Write-set is derivable from `tx_l0` (vertex/edge property maps + tombstones) — largely free. |
| Read-set capture points | `read.rs:224` (`scan_label_with_filter`), `property_manager.rs:911` (`get_all_vertex_props_with_ctx` — the point-lookup chokepoint), `df_graph/mod.rs:503` (`get_neighbors`) | Record touched ids into `tx.read_set` for RW txns only. |

> Read-set capture has **no single chokepoint** — reads are spread across the scan,
> property-hydration, and traversal paths. For RW transactions (small, keyed access) this
> is tractable (item-level). It is deliberately **not** applied to analytical/read-only
> queries, whose read-sets would explode (§7).

### 6.4 Commit-time validation (C4)

Insert a validation block in `commit_transaction_l0` **immediately after `:456`,
before `:473`**:

```rust
let _flush_lock_guard = self.flush_lock.lock().await;     // writer.rs:456 (existing)

// ── VALIDATE (new) — before any WAL write ───────────────────────────
{
    let tx = tx_l0_arc.read();
    // (a) write-set conflict: any committed write since started_at_version
    //     touching a key in our write-set ⇒ lost-update ⇒ abort
    // (b) read-set conflict (RW txns): any committed write since started_at_version
    //     touching a key in our read-set ⇒ rw-antidependency ⇒ abort
    self.committed_registry
        .conflicts_since(self.started_at_version, &tx.write_set, &tx.read_set)
        .map_or(Ok(()), |k| Err(UniError::SerializationConflict { key: k }))?;
    // (c) MERGE uniqueness: tx_l0.constraint_index vs main_l0.constraint_index
    let main = self.l0_manager.get_current();
    for (key, vid) in &tx.constraint_index {                 // l0.rs:137
        if main.read().has_constraint_key(key, *vid) {       // l0.rs:1023
            return Err(UniError::ConstraintConflict { /* … */ });
        }
    }
}
// ── existing path unchanged: WAL append (473) → flush_wal (534) → merge (541)
```

| Site | File:line | Change |
|---|---|---|
| `committed_registry` field *(new)* | `runtime/writer.rs` Writer struct (near `flush_lock`@`:171`) | Ring buffer of `{commit_version, write_set}` for recently-committed txns; `conflicts_since()`; pruned past the oldest active `started_at_version`. Writer is `Arc<Self>` (reachable in `commit_transaction_l0` as `&Arc<Self>`), so the registry is shared correctly. |
| Validation block *(new)* | `runtime/writer.rs:456→473` | The block above, before WAL append. |
| Register on success *(new)* | `runtime/writer.rs:~541` (post-merge, pre-release) | Insert this commit's write-set at the new commit version. |
| Error variants *(new)* | `UniError` (`uni-common`) | `SerializationConflict`, `ConstraintConflict`. |

**MERGE race — verified real, fix verified sound.** Two concurrent MERGEs on the same
unique key both miss existence (each checks only its own L0 + base via
`check_unique_constraint_multi` `writer.rs:1437-1495`; neither sees the other's
uncommitted L0) and both create. `tx_l0.constraint_index` *is* populated for
MERGE-created nodes before commit (`writer.rs:1753-1754`), so step (c) has data to
compare, and because commits serialize at `flush_lock`, the check closes the race.
Executor MERGE paths that feed this: optimized single-node (`write.rs:1365-1410`) and
fallback (`write.rs:1443-1480`).

### 6.5 Component C5 — `FOR UPDATE` pessimistic escape hatch

For phantom-sensitive or hot-key transactions, an explicit opt-in pessimistic lock
(Arango `exclusive:true` / Postgres `FOR UPDATE` pattern).

| Site | File:line | Change |
|---|---|---|
| Grammar | `uni-cypher/src/grammar/cypher.pest:522` (`match_clause`), `FOR` token exists at `:86` | `match_clause = { OPTIONAL? ~ MATCH ~ pattern ~ where_clause? ~ for_update?}` |
| AST | `uni-cypher/src/ast.rs:251-255` (`MatchClause`) | Add `for_update: bool`. |
| Walker | `uni-cypher/src/...walker.rs:160-175` (`build_match_clause`) | Extract the optional clause. |
| Lock map *(new)* | `uni-store/src/backend/lance.rs:47` has table-level `table_write_locks` only — **no per-Vid map** | Add a striped `DashMap<Vid, Mutex>`; acquire at MATCH, release at commit/abort; **sorted acquisition** to avoid deadlock. |

**Effort:** Medium (grammar/AST trivial; striped lock + executor threading is the work).

### 6.6 Component C6 — CRDT fast-path (already present)

For genuinely commutative counters, the value can **merge** instead of conflicting,
sidestepping aborts. CRDTs already exist (`uni-common/src/core/schema.rs:77-86`) and L0
already attempts CRDT merge (`l0.rs` `merge_crdt_properties` `~246-283`). No new work;
documented here as the recommended pattern for high-contention counters, complementing
OCC retry.

---

## 7. Why read-only / Locy is excluded from read-set tracking

A recursive Locy query (`locy_fixpoint.rs:1175-1399`) scans whole relations each
iteration (`DerivedScanExec` returns the full prior fact set). Its read-set would be
~the entire reachable closure — tens of millions of tuples, GB-scale, with a false-abort
rate >90% against any concurrent write. SSI is built for small-cardinality transactional
reads; analytical scans are the opposite. Since these queries are **read-only**, they
cannot lose updates or be a write-conflict pivot, so running them at **Snapshot
Isolation** (C1+C2) sacrifices only the rare read-only serialization anomaly — the
standard tradeoff every hybrid OLAP/OLTP engine makes. Phantom-/anomaly-sensitive
read-write logic uses `FOR UPDATE` (§6.5).

---

## 8. Phased plan

1. **Foundation — L0 snapshot reads (C1).** `L0Snapshot` (structural sharing),
   `QueryContext` swap, `l0_visibility` read-from-snapshot. Ships SI reads for the L0 tier.
   *Independent; no behavior change for existing single-writer callers.*
2. **Write-set OCC (C3 write-set + C4 a/c).** Committed registry + commit-version +
   pre-WAL validation for **write-set** and **MERGE uniqueness**. **Closes wishlist
   Requests 1 & 2 — the lost-update and serializable-MERGE asks.**
3. **Read-set SSI (C3 read-set + C4 b).** Item-level read-set capture for RW txns +
   read-set conflict detection. Upgrades RW transactions from strong-SI to item-level SSI.
4. **Base-read pinning (C2).** Populate real Lance versions; wire `open_at`. Completes SI
   for long read-only analytical queries.
5. **`FOR UPDATE` (C5).** Pessimistic escape hatch for phantom/hot-key cases.

Phases 1-2 deliver the entire wishlist. 3-5 deliver the full scoped guarantee.

---

## 9. Risks & open questions

- **L0 snapshot cost (C1).** *Resolved (2026-05-29):* benchmarked — naive clone is
  13 ms/58 MB per snapshot, `imbl` taxes reads +44%; **strategy D (Arc-share + freeze-rotate)
  chosen** at 9 ns / 0 allocs / no read regression. Prototype landed behind `l0-snapshot`.
- **Generation lifecycle (C1, D).** The prototype rides frozen generations on the
  `pending_flush` list; production needs a dedicated generation list with reader-count GC
  and freeze-under-`flush_lock` to avoid racing an in-flight commit merge. *Open.*
- **Abort storms under hot-key contention.** Mitigated by C6 (CRDT merge), a server-side
  atomic-SET retry helper (apoc-style bounded retry, ergonomics layer over OCC), and C5
  (`FOR UPDATE`). Needs telemetry to tune retry bounds.
- **Registry retention.** Ring buffer sized by oldest active `started_at_version`; a
  long-lived reader could grow it. Cap + fall back to conservative abort if exceeded; *log
  any cap hit* (no silent truncation).
- **`concurrent_writer` interplay.** Synergistic and already landed (Phase 4,
  `a5549c9c0`); `flush_lock` remains the single commit serialization point, so validation's
  critical section is well-defined. No co-design blocker.
- **Base-pinning consistency across tables (C2).** Capturing a *consistent* multi-table
  Lance-version set at tx-begin needs care under concurrent flush. *Open.*

---

## 10. Acceptance tests

The two wishlist repros become regression tests (both currently **fail**):

- **Atomic increment** (`wishlist §Request 1`): two concurrent
  `MATCH (c:Counter {id:'x'}) SET c.n = c.n + 1` → assert `n == 2`. Passes after Phase 2
  (second committer aborts on write-set conflict; retry reads `n==1`, writes `2`).
- **Serializable MERGE** (`wishlist §Request 2`): 16 concurrent `MERGE (e:E {eid:'shared'})`
  → assert `count == 1`. Passes after Phase 2 (constraint_index conflict check under `flush_lock`).
- Plus: SI read stability under concurrent flush (Phase 1/4); read-set anomaly abort
  (Phase 3); `FOR UPDATE` mutual exclusion + no deadlock (Phase 5).

The four `test_*_concurrent_no_*` cases in
`uniko2/crates/uniko-store/tests/storage_tests.rs` continue to assert non-regression as
`uniko-store` drops its `StripedLocks` per adopted phase.

---

## 11. Appendix — verified change-site index

| Component | Primary sites (file:line) |
|---|---|
| C1 L0 snapshot | `l0.rs:87-159` (fields), `context.rs:12-16`, `l0_visibility.rs:93-130/132-169/171-213/298-328/354-383`, `l0_manager.rs:65-77/82-90` |
| C2 Lance pinning | `snapshot.rs:9-33`, `manager.rs:75/409/521/534/544`, `vertex.rs:92/96-99`, `lance.rs:127-133/530`, `writer.rs:3317/3480` |
| C3 read/write-set | `transaction.rs:105-133/39-45/177-186`, `session.rs:828`, `read.rs:224`, `property_manager.rs:911`, `df_graph/mod.rs:503` |
| C4 validation | `writer.rs:171/447-652/456/473-527/534/541`, `l0.rs:137/1023/1119-1193`, `write.rs:1365-1410/1443-1480`, `writer.rs:1437-1495/1753-1754` |
| C5 FOR UPDATE | `cypher.pest:86/522`, `ast.rs:251-255`, `walker.rs:160-175`, `lance.rs:47` |
| C6 CRDT | `schema.rs:77-86`, `l0.rs:246-283` |
