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

### Implementation status (2026-05-29, branch `feat/locy-timeout-hard-error`)

> **Update (2026-05-31): the `ssi` and `l0-snapshot` cargo features were removed.**
> SSI/OCC is now **always compiled** and toggled at runtime via
> `UniConfig::ssi_enabled`, **defaulting to `true`**. The whole stack (snapshot
> reads, write/read-set conflict detection, serializable MERGE, `FOR UPDATE`) is
> gated at its *origination* sites (`Writer::create_transaction_l0` read-set
> creation, the begin-time snapshot pin, the commit-time validation block, and
> `FOR UPDATE` lock acquisition) on `config.ssi_enabled`; everything downstream
> self-gates on the resulting `Option` (`occ_read_set` / pinned snapshot is
> `None` when disabled). With `ssi_enabled = false` the engine reproduces the
> prior last-writer-wins behavior bit-for-bit, and a `FOR UPDATE` in a query
> emits a `tracing::warn!` instead of silently doing nothing. References to the
> `ssi`/`l0-snapshot` *features* below are historical; read them as "when
> `ssi_enabled` is true".

The original behavior was behind the default-off `ssi` feature (`l0-snapshot` for
C1 reads). Tests live in `crates/uni-store/tests/common/ssi_occ_test.rs`,
`crates/uni-store/src/runtime/occ.rs`, and the `crates/uni/tests/common/ssi_*`
suites (the `ssi_default_semantics` suite opens with `ssi_enabled = false`).

| Component | Status |
|---|---|
| **C1** L0 snapshot reads | **Done — wired (2026-05-30).** RW transactions pin an L0 snapshot at begin (`L0Manager::pin_snapshot`, captured in `transaction.rs` after `occ_read_seq`), threaded to `Executor::read_snapshot`, consumed in `get_context` (reads `snapshot.main`+`extra`, `tx_l0` stays live for read-your-writes). Production wiring landed via **lazy clone-on-freeze** (see F). Read-only `session.query()` snapshots are the next increment. |
| **C3/C4** write-set OCC — lost update | **Done** (`occ.rs`, `commit_transaction_l0` validation). Closes wishlist Request 1. |
| **C4** serializable MERGE | **Done** (commit-time `constraint_index` check). Closes wishlist Request 2. |
| **C3/C4** read-set SSI (rw-antidependency) | **Done**, item-level. Covered: keyed vertex point-reads, **edge reads**, **traversals** (`record_neighbor_reads`), **filtered label/scan-all vertex scans** via a transparent `ReadSetRecordingExec` inserted *above* the residual `FilterExec` (`record_batch_ids`, only matched rows recorded), and (2026-05-30) **schemaless traversal** — both single-hop *and* variable-length — via `record_edge_adjacency` at the `build_edge_adjacency_map` choke point. **Residual gap:** phantoms (concurrent inserts newly matching a predicate) are still *not* tracked (inherent to item-level granularity); see §6.3 and the FOR UPDATE caveat (§6.5). |
| **C6** CRDT carve-out | **Done** (2026-05-30, hardened) — concurrent CRDT-only writes merge instead of aborting; `WriteSet::from_l0` excludes them via shared `try_as_crdt`. The type-mismatch lost-update is closed by a **layered** soundness fix: write-time schema-variant enforcement + a commit-time `crdt_carveout_overwrite` check against main L0 + a `merge_crdt_properties` warn. See §6.6. |
| **Retry ergonomics** | **Done** (2026-05-30) — `UniError::is_retriable()` + `Session::transact_with_retry`/`execute_with_retry` (`api/retry.rs`, unconditional, bounded + jittered backoff). `is_retriable()` covers `SerializationConflict`/`ConstraintConflict`/`TransactionConflict`/`CommitTimeout`; a plain `Timeout` is **not** retriable (re-running the same slow operation would just time out again). Closes the §9 "bounded-retry helper" item. |
| **C6** CRDT commutative fast-path | **Pre-existing** (`schema.rs:77-86`, `l0.rs` CRDT merge). |
| **C2** Lance base-read pinning | **Partial** — row-level `_version <= hwm` filtering via `version_high_water_mark`/`apply_version_filter` already exists and is wired. **Key finding:** because a transaction's storage access is *read-only* (writes go to `tx_l0`; storage writes happen at commit), a transaction can use a `storage.pinned(manifest{hwm=started_at_version})` view for all its reads cleanly. Integration point: swap `Executor::storage` to the pinned view per-tx (`executor/core.rs` field; set in `impl_query.rs:417` `execute_internal_with_tx_l0`). Coupled with F (below). **Footgun (live but dormant):** `lance_version` is hardcoded to `0` (`writer.rs`), yet `get_edge_version_by_id` already flows it end-to-end into `open_at`/`checkout_version`. Populating it carelessly (per the §5 change-site) without wiring per-tx pinning would request `checkout_version(0)` — the *empty initial* Lance version — and silently read no data. Inert today only because no per-tx pinned snapshot is ever set. |
| **C5** `FOR UPDATE` escape hatch | **Done** (`for_update.rs` lock-key extractor + `Writer::row_lock_handle` per-key lock map + `Transaction` lock pre-pass holding guards until commit/rollback). Grammar/AST/walker + 11 constructor sites updated; per-key locks acquired at MATCH for keyed single-node `FOR UPDATE` (the RMW case); other patterns log a warning. 8 tests (lock-key unit + mutual-exclusion + release). **Scope (corrected):** the lock is a predicate-string mutex acquired *only* by transactions that opt into the identical exact-key `FOR UPDATE`, so it **serializes concurrent exact-key RMW writers** — it is **not** phantom prevention (a concurrent insert that does not itself use `FOR UPDATE` is not blocked; predicate/range locks would be needed). **Note:** without the `ssi` feature, `FOR UPDATE` parses but is a documented no-op (enforcement is `ssi`-gated; the grammar is unconditional). |
| **F** C1 production wiring | **Done (2026-05-30) for RW transactions, via lazy clone-on-freeze.** Per-tx-begin freezing would fragment L0 (rotate on every tx); the fix is **lazy**: a tx only *pins* the current generation (`pin_snapshot`, O(1) Arc clones + a `PinToken`), and the freeze happens **only at commit, only if the generation is pinned by a *concurrent* transaction** (`is_current_pinned` → `freeze_current_for_snapshot` before the merge, under `flush_lock`). Crucially the committing transaction **releases its own pin before commit** (`Transaction::snapshot` is `take`n in `commit`/`rollback`), so an uncontended commit does an in-place merge with **no clone** — without this, a transaction's own begin-time pin would force a deep clone on *every* commit. Regression-tested via `uni_l0_snapshot_freezes_total` (uncontended ⇒ 0) and Writer-layer `Arc::as_ptr` generation-identity checks. Implemented as **clone-on-freeze** (strategy C, lazy) rather than the doc's original rotate-aside (strategy D): the pinned buffer is deep-copied into the new current and the original — held only by the snapshot — becomes immutable, reclaimed by `Arc` refcount on tx end. This sidesteps strategy D's frozen-generation **drain gap** (a rotate-aside gen pushed onto `pending_flush` is never `complete_flush`ed, so it would leak) and the dedicated generation-GC subproject. Cost: one in-memory deep clone per freeze (rare — only a commit that crosses an open snapshot); memory is naturally bounded by concurrent RW transactions (one frozen gen each). **Out of scope (follow-ups):** read-only `session.query()` snapshots; C2 base pinning (so cross-tier flush-boundary read-skew remains for long txns — see §5); optimizing clone-on-freeze to O(1) rotate-aside with a proper drain. The non-pinned commit path is byte-for-byte unchanged (zero overhead). All gated behind `l0-snapshot` (enabled by `ssi`). |

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
- A per-transaction snapshot timestamp is captured at tx-begin. *(Implemented: the OCC
  snapshot stamp is a dedicated per-commit `Writer::commit_sequence` recorded into
  `L0Buffer::occ_read_seq` at begin — **not** `started_at_version`, which increments
  per-mutation (§6.2) and so cannot order commits. References to `started_at_version` as
  the OCC timestamp elsewhere in this doc predate the implementation.)*
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
  prevented by item-level tracking — and **`FOR UPDATE` does not prevent them either**:
  it locks an existing exact key, not a predicate or range, so a concurrent insert that
  does not itself take the same `FOR UPDATE` lock is never blocked. `FOR UPDATE` only
  serializes concurrent read-modify-write writers contending on the *same exact key*
  (§6.5). Logic that must exclude phantoms needs predicate/range locking (not implemented)
  or an external guard.

> **Honesty note (verified):** a single-key `MATCH (c:Counter {id:'x'})` is a *filtered
> label scan* (`read.rs:224` → `scan_vertex_candidates`), and even
> `IndexManager::composite_lookup` (`index_manager.rs:694`) is a scan-with-`LIMIT 1`, not a
> true index point-lookup. So "keyed access" reads are predicates, not single keys, and our
> guarantee is **SSI with item-level conflict detection, not 1-copy-serializability**. The
> lost-update/RMW cases the wishlist needs are nonetheless fully covered, because those are
> caught by *write-set* conflict detection on the existing row, independent of how the read ran.

---

## 3. Architecture overview

Writes accumulate in `tx_l0`; commit **validates then publishes** inside `flush_lock`.
Under `l0-snapshot` (enabled by `ssi`), a **read-write transaction pins an L0 snapshot at
begin** and its reads resolve against that frozen view (with `tx_l0` kept live for
read-your-writes) — C1/item F, wired via lazy clone-on-freeze (§4). Read-only
`session.query()` still reads the **live** L0 (analytical-read SI is a follow-up). OCC
remains read-set + commit-registry based, so it is correct independent of snapshot reads;
C1 additionally removes intra-transaction L0 read-skew for RW transactions. (Cross-tier
flush-boundary read-skew on *base* persists until C2 — §5.) The flow:

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

**Implemented coverage (2026-05-30).** `OccReadSet` is `Some` only for RW transactions
(`writer.rs` `create_transaction_l0`), so every hook is a no-op for read-only/analytical
queries. Captured paths:
- **Keyed vertex point-reads** — `record_vertex_read` in `l0_visibility.rs`
  (`lookup_vertex_prop` / `accumulate_vertex_props`).
- **Edge reads** — `record_edge_read` (`lookup_edge_prop` / `accumulate_edge_props`).
- **Traversals** — `record_neighbor_reads` in `df_graph/mod.rs` `get_neighbors` /
  `get_neighbors_batch` records the source, each discovered neighbour, and each traversed
  edge id (precise to the actual fan-out).
- **Filtered label / scan-all vertex scans** — a transparent `ReadSetRecordingExec`
  (`df_graph/read_set_exec.rs`) is inserted by the planner immediately *above* each vertex
  scan and its residual `FilterExec` (at the standard label-scan site and the shared
  `finalize_schemaless_scan` funnel covering schemaless/multi-label/scan-all). It records
  the surviving `{var}._vid` / `{var}._eid` columns of each output batch into the read-set
  via `GraphExecutionContext::record_batch_ids`. Because capture is *after* the residual
  filter, the read-set is exactly the matched rows; a full-label scan with no filter
  correctly records the whole label. The wrap is a no-op unless `ssi` is on *and* the
  transaction has an optimistic read-set (RW txn). Edge scans need no separate case — the
  planner routes edges through traversal, already covered by `record_neighbor_reads`.
- **Schemaless traversal (single-hop *and* variable-length)** — `record_edge_adjacency`
  (`df_graph/traverse.rs`) records the whole type-scoped adjacency that
  `build_edge_adjacency_map` builds, at that single choke point shared by all schemaless
  BFS modes (`expand_batch`, `bfs`, `bfs_with_dag`, `bfs_endpoints_only`). This is
  *accurate*, not merely conservative: the schemaless path physically scans every edge of
  the traversed type (`find_edges_by_type_names`), so that scan **is** the read footprint.

**Now tracked (post-filter), with one residual gap.** Recording above the residual filter
means only matched rows enter the read-set, so disjoint keyed writers on the same label no
longer falsely conflict — keyed RMW is preserved. Schemaless traversal is now tracked too
(see the bullet above). Under active SSI read-set recording, a `MATCH` that would use the
`VidLookupJoin` fast path instead falls back to `HashJoinExec` so the probe-side reads are
recorded (correctness over that optimization; regression-tested). The one residual gap is
**phantoms** — a concurrent INSERT of a row that *newly* matches a predicate is not caught;
this is inherent to item-level tracking and needs predicate/range locks, which `FOR UPDATE`
does **not** provide (§2.2, §6.5). None of this affects lost-update or serializable-MERGE,
which are write-set / constraint based.

### 6.4 Commit-time validation (C4)

Insert a validation block in `commit_transaction_l0` **immediately after `:456`,
before `:473`**:

```rust
let _flush_lock_guard = self.flush_lock.lock().await;     // writer.rs:456 (existing)

// ── VALIDATE (new) — before any WAL write ───────────────────────────
{
    let tx = tx_l0_arc.read();
    // (a) write-set conflict: a committed write since our snapshot touching a key
    //     in our write-set ⇒ lost-update ⇒ abort
    // (b) read-set conflict (RW txns): a committed write since our snapshot
    //     touching a key in our read-set ⇒ rw-antidependency ⇒ abort
    // NOTE: the snapshot stamp is the per-commit `tx.occ_read_seq`, NOT
    // `started_at_version` (§1 Insight); the implemented API is
    // `CommitRegistry::check`, not the `conflicts_since` shown in earlier drafts.
    let write_set = WriteSet::from_l0(&tx);
    self.committed_writes.lock()
        .check(tx.occ_read_seq, &write_set, tx.occ_read_set.as_deref())
        .map_or(Ok(()), |c| Err(UniError::SerializationConflict { message: c.to_string() }))?;
    // (c) MERGE uniqueness: tx_l0.constraint_index vs main_l0.constraint_index
    let main = self.l0_manager.get_current();
    let main = main.read();
    for (key, vid) in &tx.constraint_index {                 // l0.rs:137
        if main.has_constraint_key(key, *vid) {              // l0.rs:1023
            return Err(UniError::ConstraintConflict { /* … */ });
        }
    }
    // (d) CRDT carve-out soundness: a carved-out pure-CRDT write whose committed
    //     value is a *different* CRDT variant would be silently overwritten by
    //     `merge_crdt_properties` ⇒ abort instead of losing the update.
    if let Some(c) = crate::runtime::occ::crdt_carveout_overwrite(&tx, &main) {
        return Err(UniError::SerializationConflict { message: c.to_string() });
    }
}
// ── existing path unchanged: WAL append (473) → flush_wal (534) → merge (541)
```

| Site | File:line | Change |
|---|---|---|
| `committed_writes` field *(implemented)* | `runtime/writer.rs` Writer struct (`Mutex<CommitRegistry>`, near `flush_lock`) | Fixed-capacity ring (`OCC_REGISTRY_CAPACITY = 4096`) of `{commit_sequence, write_set}`; `CommitRegistry::check()`. Pruning is by **capacity** (oldest entries evicted), *not* active-snapshot-bounded as earlier drafts said; a committer whose `occ_read_seq` predates the oldest retained entry gets a conservative `Conflict::HistoryTruncated` abort (never a silent skip). Shared via `Arc<Writer>`. |
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

### 6.6 Component C6 — CRDT fast-path

For genuinely commutative counters, the value **merges** instead of conflicting,
sidestepping aborts. CRDTs exist (`uni-common/src/core/schema.rs:77-86`) and L0
merges them at commit (`l0.rs` `merge_crdt_properties`).

**Implemented (2026-05-30, hardened):** the OCC write-set *carves out* CRDT-only writes so
concurrent increments to the same vertex both commit and merge instead of aborting.
`WriteSet::from_l0` excludes a vertex iff every written property is a mergeable CRDT value
(no delete, no label change), via the shared `l0.rs::try_as_crdt` predicate.

**Type-mismatch soundness (the layered fix).** `try_as_crdt` only proves the *incoming*
value is CRDT-shaped; `merge_crdt_properties` additionally requires the *committed* value to
be the **same** variant, else it falls through to a last-writer-wins overwrite. A naive
carve-out would therefore hide a lost update when two *different* CRDT variants hit one
property concurrently. Three layers close this:
1. **Write-time** (`prepare_vertex_upsert`, all builds): a schema-declared CRDT property is
   rejected with `UniError::Constraint` if written with the wrong variant — so a declared
   property only ever holds one variant and concurrent CRDT writes always merge.
2. **Commit-time** (`occ::crdt_carveout_overwrite`, `ssi`): for each carved-out CRDT write,
   if main L0 holds a *different* variant for that property, abort `SerializationConflict`.
   Covers undeclared/heterogeneous CRDT-shaped values that bypass layer 1.
3. **Observability**: `merge_crdt_properties` `tracing::warn!`s on any residual
   variant-mismatch overwrite (e.g. a single-writer variant change).

- **Effectiveness note:** a `SET` on a *labelled* vertex re-threads its existing labels
  through the write path, so `label_changed` is true and the carve-out does **not** apply —
  in practice it fires only for label-less vertices. Labelled CRDT counters stay
  conflictable (sound, but the "both commit" benefit needs label-less writes).
- **Read-set governs RMW:** a CRDT *increment* reads the prior value, so under `ssi` the
  vertex enters the read-set. Two pure CRDT incrementers do not falsely abort (neither is in
  the other's write-set), but a CRDT incrementer vs. a concurrent label/LWW writer aborts via
  the read-set — correct SSI behaviour, not a regression.
- **Accepted limitation (R1):** the carve-out is item-level, so a CRDT-only writer and a
  concurrent last-writer-wins writer to the *same property* both commit (order-dependent
  result); `merge_crdt_properties` `warn!`s when it overwrites a CRDT with a non-CRDT scalar.
- **Edges** stay always-conflictable: every edge write asserts endpoints/type
  (non-commutative topology), so no edge write qualifies for the carve-out.

CRDT properties are not expressible via Cypher (schema-declared, programmatic writes), so the
carve-out and its soundness checks are exercised at the Writer layer
(`uni-store/tests/common/ssi_occ_test.rs`).

---

## 7. Why read-only / Locy is excluded from read-set tracking

A recursive Locy query (`locy_fixpoint.rs:1175-1399`) scans whole relations each
iteration (`DerivedScanExec` returns the full prior fact set). Its read-set would be
~the entire reachable closure — tens of millions of tuples, GB-scale, with a false-abort
rate >90% against any concurrent write. SSI is built for small-cardinality transactional
reads; analytical scans are the opposite. Since these queries are **read-only**, they
cannot lose updates or be a write-conflict pivot, so running them at **Snapshot
Isolation** (C1+C2) sacrifices only the rare read-only serialization anomaly — the
standard tradeoff every hybrid OLAP/OLTP engine makes. Anomaly-sensitive read-write logic
can serialize concurrent RMW writers on a hot key with `FOR UPDATE` (§6.5) — but note that
`FOR UPDATE` is *not* phantom protection (§2.2).

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

- **L0 snapshot cost (C1).** *Resolved.* Snapshot *capture* is O(1) (a few `Arc` clones +
  a pin token); reads have zero regression (they dereference the pinned buffer through the
  same `Arc`). The freeze deep-copies once per freeze (lazy, only on a commit crossing an
  open snapshot) — see the §1 F row and the Generation-lifecycle note below for why
  clone-on-freeze was chosen over the originally-benchmarked rotate-aside.
- **Generation lifecycle (C1).** *Resolved (2026-05-30) by clone-on-freeze.* The chosen
  implementation deep-copies the pinned generation into a fresh current at commit (under
  `flush_lock`, so no race with a merge/rotate) and leaves the original held **only** by the
  snapshot — reclaimed by `Arc` refcount when the tx ends. No `pending_flush` ride, so no
  dedicated generation list / reader-count GC / drain is needed; memory is bounded by
  concurrent RW transactions. The prototype's `snapshot_isolated` (rotate-aside) is retained
  only for its unit test. *(Trade-off: a deep clone per freeze instead of O(1) rotate-aside;
  optimizing back to rotate-aside requires a frozen-generation drain — deferred.)*
- **Abort storms under hot-key contention.** Mitigated by C6 (CRDT merge), a server-side
  atomic-SET retry helper (apoc-style bounded retry, ergonomics layer over OCC), and C5
  (`FOR UPDATE`). Needs telemetry to tune retry bounds.
- **Registry retention (implemented).** Fixed-capacity ring (`OCC_REGISTRY_CAPACITY = 4096`),
  *not* sized by the oldest active snapshot. A transaction whose `occ_read_seq` predates the
  oldest retained entry cannot be verified against the evicted commits, so it aborts
  conservatively with `Conflict::HistoryTruncated` — sound (never misses a real conflict), at
  the cost of rare false aborts for very long-lived transactions. No silent truncation.
- **`concurrent_writer` interplay.** Synergistic and already landed (Phase 4,
  `a5549c9c0`); `flush_lock` remains the single commit serialization point, so validation's
  critical section is well-defined. No co-design blocker.
- **Base-pinning consistency across tables (C2).** Capturing a *consistent* multi-table
  Lance-version set at tx-begin needs care under concurrent flush. *Open.*

---

## 10. Acceptance tests

The two wishlist repros are implemented as regression tests and **pass** (2026-05-30,
`crates/uni/tests/common/ssi_occ_e2e.rs`, real `tokio::spawn` concurrency under `--features ssi`):

- **Atomic increment** (`wishlist §Request 1`): concurrent
  `MATCH (c:Counter {id:'x'}) SET c.n = c.n + 1` via `Session::transact_with_retry` →
  `n == N` (2-writer and 16-writer stress). Second committer aborts on write-set conflict;
  the retry re-reads and re-applies.
- **Serializable MERGE** (`wishlist §Request 2`): 16 concurrent `MERGE (e:E {code:'shared'})`
  → `count == 1` (losers abort on the unique-key check under `flush_lock`).
- Plus (passing): real read-write antidependency abort via edge/traversal reads;
  scan-read antidependency now aborts (`scan_read_antidependency_aborts`) with disjoint
  keyed scans still committing (`scan_read_disjoint_key_no_false_abort`, post-filter
  precision); **schemaless** traversal antidependency (`schemaless_traversal_antidependency_aborts`,
  exercising `record_edge_adjacency`); **VidLookupJoin→HashJoin fallback** records reads
  (`vid_lookup_join_records_reads_via_fallback`); read-only stability under concurrent writes
  and **read-only-transaction-is-not-a-pivot** (`read_only_transaction_is_not_a_pivot`);
  `FOR UPDATE` mutual exclusion, lock release on commit *and* rollback, multi-key no-deadlock,
  unsupported-pattern no-op.
- CRDT (Writer layer, `uni-store/tests/common/ssi_occ_test.rs`): concurrent same-variant
  increments merge; **variant-mismatch aborts** (`concurrent_crdt_variant_mismatch_aborts`,
  commit-time layer 2); **write-time variant enforcement** rejects wrong/non-CRDT writes to a
  declared CRDT property (`write_time_rejects_*`, layer 1) and accepts the declared variant;
  R1 outcome pinned (`r1_crdt_overwritten_by_lww_pins_value`); abort leaves no trace, also
  **after a flush** (`aborted_commit_leaves_no_trace_after_flush`). Unit (`occ.rs`):
  `crdt_carveout_overwrite` variant-mismatch/same-variant/new-vertex cases; registry
  truncation aborts conservatively for write-set *and* read-set txns, and a long-lived reader
  within retained history does not falsely abort.

The four `test_*_concurrent_no_*` cases in
`uniko2/crates/uniko-store/tests/storage_tests.rs` continue to assert non-regression as
`uniko-store` drops its `StripedLocks` per adopted phase.

---

## 11. Appendix — verified change-site index

| Component | Primary sites (file:line) |
|---|---|
| C1 L0 snapshot (wired) | `l0_manager.rs` (`PinToken`, `SnapshotView`, `pin_snapshot`, `is_current_pinned`, `freeze_current_for_snapshot`, `rotate` pin reset), `runtime/mod.rs` (`SnapshotView` re-export), `writer.rs` `commit_transaction_l0` (freeze hook before merge) + `insert_vertex_with_labels` (bulk carve-out warn), `transaction.rs` (`snapshot` field + `pin_snapshot` capture + `read_snapshot()` helper + 5 call sites), `impl_query.rs` (`read_snapshot` param on `*_internal_with_tx_l0`), `executor/core.rs` (`read_snapshot` field/`set_read_snapshot`/`get_context` hook). Reader chain (`l0_visibility.rs`, `context.rs`) reused unchanged. |
| C2 Lance pinning | `snapshot.rs:9-33`, `manager.rs:75/409/521/534/544`, `vertex.rs:92/96-99`, `lance.rs:127-133/530`, `writer.rs:3317/3480` |
| C3 read/write-set | `transaction.rs:105-133/39-45/177-186`, `session.rs:828`, `read.rs:224`, `property_manager.rs:911`, `df_graph/mod.rs:503`, `df_graph/read_set_exec.rs` (`ReadSetRecordingExec`), `df_graph/mod.rs` (`record_batch_ids`), `df_planner.rs` (scan-wrap + `finalize_schemaless_scan`), `df_graph/traverse.rs` (`record_edge_adjacency` at the `build_edge_adjacency_map` choke point) |
| C4 validation | `writer.rs:171/447-652/456/473-527/534/541`, `l0.rs:137/1023/1119-1193`, `write.rs:1365-1410/1443-1480`, `writer.rs:1437-1495/1753-1754`, `occ.rs` (`CommitRegistry::check`, `crdt_carveout_overwrite`) |
| C5 FOR UPDATE | `cypher.pest:86/522`, `ast.rs:251-255`, `walker.rs:160-175`, `lance.rs:47` |
| C6 CRDT | `schema.rs` (`CrdtType`, `CrdtType::type_name`), `l0.rs` (`try_as_crdt`, `merge_crdt_properties` + variant-mismatch `warn!`), `occ.rs` (`is_crdt_carveout`, `crdt_carveout_overwrite`), `writer.rs` `prepare_vertex_upsert` (write-time variant enforcement) |
