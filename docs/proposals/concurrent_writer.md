# Concurrent Writer — Design Document

**Status:** Draft (Proposal)
**Date:** 2026-05-17
**Author:** rohit@dragonscale.ai
**Crates touched:** `uni-store`, `uni-query`, `uni`
**Related:** `docs/proposals/async_l0_to_l1_flush.md`, `docs/proposals/graph_fork_plan.md`

---

## 1. Summary

Today, every mutation statement — `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`, and direct API calls like `Transaction::create_nodes_batch` — acquires the database-wide exclusive lock `Arc<tokio::sync::RwLock<Writer>>` at `crates/uni/src/api/mod.rs:74`. The lock is held for the duration of the statement: schema validation, ID allocation, embedding generation, constraint checking, and the actual write into the (per-transaction or main) L0 buffer.

At session concurrency 24, this lock is the dominant bottleneck. Cumulative per-statement exec time inflates 6–70× vs. concurrency 1, while parse + plan stay ≤0.05% of wall time and commit phase is ≤8% of wall time. The remaining ~90% of wall time is wait-on-writer-lock.

The lock is structurally unnecessary on the hot path. Audit of every `&mut self` method on `Writer` reachable from the executor shows that **none of them mutate any field of `Writer`**: all writes target Arc'd substructures (`l0_manager`, `storage`, `allocator`, `adjacency_manager`, `property_manager`), each with its own internal synchronization. The four `Writer` fields that ARE mutated (`last_flush_time`, `cached_manifest`, `fork_flush_count`, `fork_fragment_warn_fired`) are touched only by `flush_to_l1` and `tick_fork_fragment_observability`, neither of which is on the per-statement path inside transactions.

This document proposes: (a) convert the four mutated `Writer` fields to interior mutability, (b) convert the ~15 hot-path mutation methods from `&mut self` to `&self`, (c) introduce a dedicated narrow `flush_lock` for legitimately-exclusive lifecycle operations, (d) replace `Arc<tokio::sync::RwLock<Writer>>` with plain `Arc<Writer>` everywhere except the flush path.

Expected outcome: hot-path statement exec time at sess=24 drops from 6–70× inflation back to within 2–3× of sess=1, for an aggregate 5–10× wall-time reduction on mutation-heavy workloads.

`★ Insight ─────────────────────────────────────`
- The pattern is already established: `Writer::create_transaction_l0` (writer.rs:210–214) is `&self`, reads the current version from the L0 manager, and is callable concurrently. Its sibling mutation methods just inherited a `&mut self` signature that was never re-examined when per-transaction L0 buffers were introduced.
- This proposal is **orthogonal to and composable with** `docs/proposals/async_l0_to_l1_flush.md`. That proposal reduces *flush hold time* during commit; this one removes the *per-statement hold time* during execute. Both ship at the same lock site; both are needed for full concurrency.
`─────────────────────────────────────────────────`

---

## 2. Background

### 2.1 Today's mutation path

`crates/uni/src/api/mod.rs:74`:

```rust
pub(crate) writer: Option<Arc<tokio::sync::RwLock<Writer>>>,
```

One per database. Acquired exclusively (`.write().await`) on every mutation statement, both from the Cypher executor and from the direct mutation API.

**Hot-path acquisition sites (each takes `.write().await`):**

| File | Line | Operation | tx_l0 used? |
|---|---|---|---|
| `crates/uni-query/src/query/executor/write.rs` | 389 | `VACUUM` | n/a (admin) |
| `…/write.rs` | 422 | `CHECKPOINT` | n/a |
| `…/write.rs` | 713 | `COPY FROM` edges — per-row in loop | `None` |
| `…/write.rs` | 778 | `COPY FROM` vertices — per-row in loop | `None` |
| `…/write.rs` | 1359 | `MERGE` (optimized index path) | `tx_l0_override` |
| `…/write.rs` | 1393 | `MERGE` (standard path) | `tx_l0_override` |
| `crates/uni-query/src/query/df_graph/mutation_common.rs` | 542 | `CREATE`/`SET`/`REMOVE`/`DELETE` | `tx_l0_override` |
| `crates/uni-query/src/query/df_graph/mutation_foreach.rs` | 204 | `FOREACH` body mutations | `tx_l0_override` |
| `crates/uni/src/api/transaction.rs` | 360 | `Transaction::bulk_insert_vertices` | own tx_l0 |
| `crates/uni/src/api/transaction.rs` | 409 | `Transaction::bulk_insert_edges` | own tx_l0 |
| `crates/uni/src/api/transaction.rs` | 517 | tx commit-merge | n/a |
| `crates/uni/src/api/bulk.rs` | 450 | bulk vid allocation (read) | n/a |
| `crates/uni/src/api/bulk.rs` | 919 | bulk eid allocation (read) | n/a |

**Cold-path acquisition sites (write):**

| File | Line | Operation |
|---|---|---|
| `crates/uni/src/api/session.rs` | 498 | `flush_to_l1` (manual flush) |
| `crates/uni/src/api/session.rs` | 542 | `flush_to_l1` (pre-index-build) |
| `crates/uni/src/api/fork.rs` | 199 | `flush_to_l1` (fork creation) |
| `crates/uni/src/api/mod.rs` | 1011 | `flush_to_l1` (`Uni::flush`) |
| `crates/uni/src/api/mod.rs` | 1037 | `flush_to_l1` (create_snapshot) |
| `crates/uni/src/api/mod.rs` | 1415 | `flush_to_l1` (shutdown) |
| `crates/uni/src/api/mod.rs` | 1978 | `set_xervo_runtime` (init) |
| `crates/uni/src/api/mod.rs` | 2013 | `set_index_rebuild_manager` (init) |
| `crates/uni/src/api/mod.rs` | 2028 | `check_flush` (background ticker) |
| `crates/uni/src/api/mod.rs` | 2035 | `flush_to_l1` (background shutdown) |
| `crates/uni/src/api/fork_index_builder.rs` | 124 | `flush_to_l1` (pre-build flush) |
| `crates/uni-query/src/query/executor/procedure.rs` | 223 | procedure-driven flush |
| `crates/uni-query/src/query/executor/read.rs` | 3817 | analytics-path flush hint |
| `crates/uni-query/src/query/executor/write.rs` | 740, 797 | post-bulk flush after `COPY FROM` |

**Read-side acquisition sites (`.read().await`):**

| File | Line | Operation |
|---|---|---|
| `crates/uni/src/api/transaction.rs` | 174 | `create_transaction_l0` (`&self`) |
| `crates/uni/src/api/impl_locy.rs` | 129 | `create_transaction_l0` (`&self`) |
| `crates/uni/src/api/impl_query.rs` | 166, 179 | reads `l0_manager.get_current()` stats |
| `crates/uni/src/api/fork.rs` | 243 | `allocator.current_hwm()` |
| `crates/uni/src/api/mod.rs` | 1985 | `replay_wal` (already `&self`) |
| `crates/uni/src/api/mod.rs` | 2272 | test pointer-equality check |
| `crates/uni-query/src/query/executor/core.rs` | 366 | `l0_manager.get_current()` / `get_pending_flush()` |

Every read-side site either invokes a method that is already `&self` (`create_transaction_l0`, `replay_wal`, `allocator.*`, `next_eid`) or clones an `Arc`-typed field. None mutate `Writer`. None rely on the writer-exclusion barrier — the substructures they access (`L0Manager`, `IdAllocator`, etc.) carry their own internal synchronization. After Phase 4 these become bare `writer.method()` / `writer.field.clone()` calls; no semantic change.

**Acquisition-site total:** ~27 `.write().await` + ~10 `.read().await` ≈ **37 sites across `crates/uni/src` and `crates/uni-query/src`**.

### 2.2 Writer's actual mutable state

`Writer` (`crates/uni-store/src/runtime/writer.rs:45–83`) holds 15 fields. By mutability profile:

**Arc'd shared substructures (no `&mut self` needed):**
- `l0_manager: Arc<L0Manager>` (own internal `RwLock`)
- `storage: Arc<StorageManager>` (`&self` API)
- `schema_manager: Arc<SchemaManager>` (own `RwLock`)
- `allocator: Arc<IdAllocator>` (own `tokio::Mutex`)
- `xervo_runtime: Option<Arc<ModelRuntime>>`
- `property_manager: Option<Arc<PropertyManager>>` (own internal locks)
- `adjacency_manager: Arc<AdjacencyManager>` (own internal locks)
- `compaction_handle: Arc<RwLock<Option<JoinHandle>>>` (already shared)
- `index_rebuild_manager: Option<Arc<IndexRebuildManager>>`

**Immutable after construction:**
- `config: UniConfig` (Clone)
- `fork_id: Option<ForkId>` (set at construction)

**Mutable, but only from cold-path methods:**
- `last_flush_time: Instant` — written at writer.rs:2545 inside `flush_to_l1`
- `cached_manifest: Option<SnapshotManifest>` — written at writer.rs:2520 inside `flush_to_l1` (also `.take()` at writer.rs:2215)
- `fork_flush_count: u64` — written at writer.rs:2621 inside `tick_fork_fragment_observability`
- `fork_fragment_warn_fired: bool` — written at writer.rs:2630 inside `tick_fork_fragment_observability`

### 2.3 Hot-path mutation method audit

Every `&mut self` method on `Writer` reachable from the executor was audited:

| Method | Line | Writes any `Writer` field? |
|---|---|---|
| `insert_vertex_with_labels` | 1328 | **No** |
| `insert_vertices_batch` | 1447 | **No** |
| `delete_vertex` | 1581 | **No** |
| `insert_edge` | 1713 | **No** |
| `delete_edge` | 1754 | **No** |
| `commit_transaction_l0` | 237 | **No** (writes go through `l0_manager`; carries `&mut self` only because callees did) |
| `set_xervo_runtime` (init only) | 197 | yes (`xervo_runtime`) |
| `set_property_manager` (init only) | 2679 | yes (`property_manager`) |
| `set_index_rebuild_manager` (init only) | 148 | yes (`index_rebuild_manager`) |
| `flush_to_l1` | 1998 | yes (`last_flush_time`, `cached_manifest`) — cold path |
| `check_flush` | 1790 | reads `last_flush_time`, may call `flush_to_l1` |
| `tick_fork_fragment_observability` | 2619 | yes (`fork_flush_count`, `fork_fragment_warn_fired`) — cold path |
| `replay_wal` | 156 | **No** (already `&self`!) |
| `next_vid` / `next_eid` / `allocate_vids` | 182/193/188 | **No** (already `&self`!) |
| `create_transaction_l0` | 210 | **No** (already `&self`!) |

The pattern is unambiguous: the hot-path mutations carry a `&mut self` signature for no reason; the cold-path lifecycle operations (`flush_to_l1`, init setters) are the only places that genuinely mutate `Writer`-owned state.

Additionally, the hot-path methods are gated on `tx_l0` in a way that bypasses even the cold-path entry: `insert_vertex_with_labels` calls `check_flush` at line 1414 only when `tx_l0.is_none()`. Same gate at lines 1630, 1745, 1779. Inside any explicit transaction, the hot path never touches any cold-path-mutating method.

### 2.4 Secondary contributor: `check_write_pressure` inside the lock

`check_write_pressure` (writer.rs:1149–1177) is called at the start of every mutation method (lines 1336, 1472, 1588, 1724, 1763). When L1 fragmentation is at the throttle limits, it sleeps **while holding the writer write-lock**, amplifying the per-statement hold time and starving all peers. The throttle itself is a useful pacing mechanism; the placement inside the lock is the bug.

### 2.5 Tertiary contributor: bulk loader per-row lock acquisition

`COPY FROM` (`executor/write.rs:713`, `:778`) acquires `writer_lock.write().await` **inside** the row loop:

```rust
for row_idx in 0..num_rows {
    let mut writer = writer_arc.write().await;  // line 713 or 778
    let eid = writer.next_eid(...).await?;
    writer.insert_edge(...).await?;
    // drop on scope end
}
```

Independent of cross-session contention, this is quadratic in row count for a single session's bulk load: each row pays acquire+release+memory-barrier cost on a tokio async RwLock. Hoisting the acquisition outside the loop is a separable cleanup that this proposal subsumes.

### 2.6 Measured impact (from in-flight workload, sess=24 vs sess=1)

| Site | Calls/question | sess=1 avg | sess=24 avg | Inflation |
|---|---|---|---|---|
| `create_message_edges_in_tx` | 510 | 15 ms | 1057 ms | 70× |
| `batch_create_nodes_in_tx` | 793 | 18 ms | 583 ms | 33× |
| `batch_create_edges_fast_in_tx` | 1374 | 126 ms | 853 ms | 6.8× |

Parse + plan share is <0.05% of wall time at sess=24. Commit phase is 5–8% of wall time. The remaining ~90% is exec time, the bulk of which is wait-on-lock.

The variance (70× vs 6.8×) matches the queueing model: small per-call payloads pay the lock-acquisition cost N times (where N = calls/question), so per-acquisition overhead dominates. The "fast" path amortizes more work per acquisition and inflates less.

---

## 3. Goals & Non-Goals

### 3.1 Goals

- **G1**: Eliminate `Arc<tokio::sync::RwLock<Writer>>` as a hot-path serialization point. Hot-path mutation statements must execute concurrently across sessions, gated only by the per-substructure locks that semantically require it (L0 buffer's own `RwLock`, `IdAllocator`'s `tokio::Mutex`, etc.).
- **G2**: Preserve per-transaction L0 isolation semantics: a transaction's mutations remain invisible to peers until commit; uncommitted state of one tx must never leak into another tx's reads.
- **G3**: Preserve flush exclusivity. At most one `flush_to_l1` runs at a time per database; flush serializes ordered-finalize with respect to in-flight commits but does not block execute.
- **G4**: Preserve schema-mutation safety. Schema changes (`CREATE LABEL`, `ALTER LABEL`, constraint add/drop) must continue to observe a consistent view of in-flight mutations.
- **G5**: Preserve crash recovery. WAL semantics unchanged; replay still HWM-driven.
- **G6**: Land behind a feature flag with measurable A/B.

### 3.2 Non-Goals

- **NG1**: Improving single-session throughput. Sess=1 may show a small overhead from atomic operations on `last_flush_time`/`fork_flush_count` etc., but no semantic change.
- **NG2**: Removing the main-L0 merge serialization at commit. Two commits to the same L0 still serialize on the L0's own `RwLock` during merge; that is correct LSM semantics.
- **NG3**: Reducing flush hold time. Covered by `async_l0_to_l1_flush.md`. This proposal makes the path *to* that fix structurally cleaner.
- **NG4**: Implementing optimistic concurrency control or MVCC at the executor level. Per-tx L0 already provides snapshot isolation; we are not introducing new isolation modes.
- **NG5**: Changing the Cypher surface or the `Transaction` / `Session` API.

---

## 4. Design Overview

### 4.1 The four moves

1. **Field-level interior mutability for the four cold-path-mutated fields.** `last_flush_time` → `parking_lot::Mutex<Instant>`. `cached_manifest` → `parking_lot::Mutex<Option<SnapshotManifest>>`. `fork_flush_count` → `AtomicU64`. `fork_fragment_warn_fired` → `AtomicBool`. Init-time setters (`xervo_runtime`, `property_manager`, `index_rebuild_manager`) move to `OnceLock<Arc<T>>`.

2. **Signature change for hot-path methods.** `&mut self` → `&self` on the five mutation methods plus their helpers. No body changes (verified in §2.3).

3. **A dedicated `flush_lock` for the genuinely-exclusive flush path.** `tokio::sync::Mutex<()>` inside `Writer`. Acquired by `flush_to_l1` and `commit_transaction_l0` (for the rotate/finalize critical sections only). This is the seam that the async-flush proposal builds on.

4. **Drop the outer `tokio::sync::RwLock<Writer>` entirely.** `Arc<tokio::sync::RwLock<Writer>>` → `Arc<Writer>`. All ~37 call sites updated (27 write + 10 read): hot-path sites drop `.write().await` and call methods directly through the `Arc`; cold-path flush sites continue to call `flush_to_l1` directly (which now takes its own `flush_lock` internally); init-time setters become non-`async` `OnceLock` ops.

### 4.2 Lock map: before vs. after

**Before:**

```
Arc<tokio::RwLock<Writer>>
  ├── (exclusive on every mutation) ──→ writer.insert_vertex_with_labels(&mut self, ...)
  │                                       ├── self.l0_manager.get_current().write() [tx_l0 or main]
  │                                       ├── self.schema_manager.schema() [.read()]
  │                                       └── self.allocator.allocate_vid().await [Mutex]
  └── (exclusive on flush) ───────────→ writer.flush_to_l1(&mut self, ...)
                                          └── (everything; ~50–100 ms held)
```

**After:**

```
Arc<Writer>
  ├── (no outer lock) ──→ writer.insert_vertex_with_labels(&self, ...)
  │                         ├── self.l0_manager.get_current().write() [tx_l0 or main]
  │                         ├── self.schema_manager.schema() [.read()]
  │                         └── self.allocator.allocate_vid().await [Mutex]
  │
  └── writer.flush_lock.lock().await ──→ writer.flush_to_l1(&self, ...)
                                          ├── (rotate: brief)
                                          ├── (stream: long, optionally outside flush_lock per async-flush proposal)
                                          └── (finalize: brief, updates cached_manifest under field Mutex)
```

Concurrent mutation statements no longer serialize through any shared lock. They are bounded only by the per-substructure locks that semantically require it.

### 4.3 Composition with `async_l0_to_l1_flush.md`

The async-flush proposal needs a "seam" — a place where the writer-lock release happens between rotate and stream. Today there is no seam: the whole flush runs under the outer write lock. After this proposal lands, the outer lock is gone; `flush_lock` becomes that seam, and async-flush phases 2–5 (rotate→spawn→finalize) compose cleanly on top.

Specifically, async-flush §5.2's three-step split becomes:
1. **rotate**: holds `flush_lock`. Brief (~µs).
2. **stream**: releases `flush_lock`. Runs on a spawned task. Long (~50–100 ms).
3. **finalize**: re-acquires `flush_lock` in order (single-task finalizer). Brief.

This proposal does not implement that split; it makes it possible.

---

## 5. Detailed Design

### 5.1 `Writer` field changes

Current (`crates/uni-store/src/runtime/writer.rs:45–83`):

```rust
pub struct Writer {
    pub l0_manager: Arc<L0Manager>,
    pub storage: Arc<StorageManager>,
    pub schema_manager: Arc<SchemaManager>,
    pub allocator: Arc<IdAllocator>,
    pub config: UniConfig,
    pub xervo_runtime: Option<Arc<ModelRuntime>>,
    pub property_manager: Option<Arc<PropertyManager>>,
    adjacency_manager: Arc<AdjacencyManager>,
    last_flush_time: std::time::Instant,
    compaction_handle: Arc<parking_lot::RwLock<Option<tokio::task::JoinHandle<()>>>>,
    index_rebuild_manager: Option<Arc<IndexRebuildManager>>,
    cached_manifest: Option<SnapshotManifest>,
    pub fork_id: Option<ForkId>,
    fork_flush_count: u64,
    fork_fragment_warn_fired: bool,
}
```

Proposed:

```rust
pub struct Writer {
    // Unchanged: already Arc'd or immutable after construction.
    pub l0_manager: Arc<L0Manager>,
    pub storage: Arc<StorageManager>,
    pub schema_manager: Arc<SchemaManager>,
    pub allocator: Arc<IdAllocator>,
    pub config: UniConfig,
    adjacency_manager: Arc<AdjacencyManager>,
    compaction_handle: Arc<parking_lot::RwLock<Option<tokio::task::JoinHandle<()>>>>,
    pub fork_id: Option<ForkId>,

    // Init-only setters → OnceLock. Callable on &self.
    xervo_runtime: OnceLock<Arc<ModelRuntime>>,
    property_manager: OnceLock<Arc<PropertyManager>>,
    index_rebuild_manager: OnceLock<Arc<IndexRebuildManager>>,

    // Cold-path mutable fields → interior mutability.
    last_flush_time: parking_lot::Mutex<std::time::Instant>,
    cached_manifest: parking_lot::Mutex<Option<SnapshotManifest>>,
    fork_flush_count: AtomicU64,
    fork_fragment_warn_fired: AtomicBool,

    // NEW: dedicated lock for flush exclusion + commit-merge ordering.
    flush_lock: tokio::sync::Mutex<()>,
}
```

Accessor pattern for the OnceLock fields:

```rust
pub fn xervo_runtime(&self) -> Option<Arc<ModelRuntime>> {
    self.xervo_runtime.get().cloned()
}

pub fn set_xervo_runtime(&self, rt: Arc<ModelRuntime>) -> Result<()> {
    self.xervo_runtime.set(rt)
        .map_err(|_| anyhow!("xervo_runtime already set"))
}
```

`set_*` methods become `&self`. They are called exactly once during builder setup; the existing `set_*` paths in `crates/uni/src/api/mod.rs:1978`, `:2013`, and the `set_property_manager` (writer.rs:2679) move to OnceLock semantics with an explicit "already set" error.

### 5.2 Hot-path method signature changes

The following methods change from `&mut self` to `&self`. Bodies are unchanged (per §2.3 audit):

| Method | Line |
|---|---|
| `insert_vertex_with_labels` | 1328 |
| `insert_vertices_batch` | 1447 |
| `delete_vertex` | 1581 |
| `insert_edge` | 1713 |
| `delete_edge` | 1754 |
| `commit_transaction_l0` | 237 |
| `check_transaction_memory` | 1182 |
| `update_metrics` | 226 |

The following helpers are **already `&self`** and need no signature change — listed here for completeness so the Phase 2 audit doesn't try to convert them:

| Helper | Line |
|---|---|
| `process_embeddings_for_labels` | 1817 |
| `validate_vertex_constraints` / `_for_label` | 491 / 395 |
| `prepare_vertex_upsert` | 1216 |
| `prepare_edge_upsert` | 1275 |
| `check_write_pressure` | 1149 |

(Edge-constraint validation is inline within `insert_edge` / `delete_edge`; no standalone `validate_edge_constraints` exists. `check_write_pressure` is already `&self` — its throttle sleep starves peers only via the caller's `&mut self` writer-lock hold, which Phase 4 removes.)

`set_property_manager` (writer.rs:2679) — OnceLock; `&self`.
`set_index_rebuild_manager` (writer.rs:148) — OnceLock; `&self`.
`set_xervo_runtime` (writer.rs:197) — OnceLock; `&self`.

`check_flush` (writer.rs:1790) — reads `last_flush_time` via the Mutex; may call `flush_to_l1`. Becomes `&self`. The Mutex acquisition is brief (~ns); contention impossible because writes are only from `flush_to_l1` which self-serializes via `flush_lock`.

### 5.3 Cold-path methods retain stronger guarantees via `flush_lock`

| Method | Line | Signature | Exclusion |
|---|---|---|---|
| `flush_to_l1` | 1998 | `&self` | acquires `flush_lock` for the duration |
| `commit_transaction_l0` | 237 | `&self` | acquires `flush_lock` for the main-L0 merge + WAL append window |
| `tick_fork_fragment_observability` | 2619 | `&self` | atomic ops, no lock |
| `replay_wal` | 156 | `&self` (already!) | called at startup only |

`commit_transaction_l0` is the critical correctness point. Today, the writer-write-lock serializes the merge of `tx_l0` into main L0 plus the WAL append. After the change, `flush_lock` provides the same serialization for the merge + WAL window. Specifically, the lock is held across:
- WAL append of the tx's mutations
- `flush_wal().await` (group-commit)
- Main-L0 merge (writer.rs:317–357)
- Possible `check_flush` → `flush_to_l1` re-entrant call (which uses the SAME `flush_lock` — see §5.5 for re-entrancy handling)

Reads inside that window (e.g., `self.l0_manager.get_current()`) are still `&self`-safe; they are serialized only against other commits, not against execute.

### 5.4 Mutation API changes (Transaction / Session)

`crates/uni/src/api/transaction.rs:360`:

```rust
// Before:
let writer_lock = self.db.writer.as_ref().ok_or(UniError::ReadOnly { ... })?;
let mut writer = writer_lock.write().await;
let vids = writer.allocate_vids(count).await?;
writer.insert_vertices_batch(vids, props_batch, labels, Some(&self.tx_l0)).await?;

// After:
let writer = self.db.writer.as_ref().ok_or(UniError::ReadOnly { ... })?;
let vids = writer.allocate_vids(count).await?;
writer.insert_vertices_batch(vids, props_batch, labels, Some(&self.tx_l0)).await?;
```

Same pattern at `transaction.rs:409`, `executor/write.rs:1359`, `:1393`, `df_graph/mutation_common.rs:542`, and all other hot-path sites. The lock acquisition and `drop(writer)` calls disappear.

`crates/uni/src/api/mod.rs:74` storage:

```rust
// Before:
pub(crate) writer: Option<Arc<tokio::sync::RwLock<Writer>>>,

// After:
pub(crate) writer: Option<Arc<Writer>>,
```

All ~37 callers updated (27 `.write().await` + 10 `.read().await` sites). No `WriterRef` type alias exists in the tree today; the new type is just `Arc<Writer>`.

### 5.5 `flush_lock` re-entrancy and `check_flush`

`commit_transaction_l0` (writer.rs:362) calls `check_flush().await` *inside* its `flush_lock` critical section. `check_flush` may decide to call `flush_to_l1`, which also acquires `flush_lock`. `tokio::sync::Mutex` is not re-entrant — this would deadlock.

Three options:

1. **Split `flush_to_l1` into a `&self` body + a `&self`-with-lock entry point.** `commit_transaction_l0` calls the body directly (already holds the lock); external callers go through the entry point.
2. **Don't hold `flush_lock` across `check_flush`.** Drop the lock after the WAL flush + L0 merge (the parts that need exclusion), then call `check_flush`. `check_flush` re-acquires.
3. **Use `tokio::sync::Mutex` + a recursion-aware wrapper that tracks the holding task.** Avoid — re-entrant mutexes are a code smell and don't compose with `tokio::spawn`.

**Choice: option 1.** Internal helper `flush_to_l1_inner(&self) -> Result<String>` does the work; `flush_to_l1(&self)` acquires `flush_lock` and calls `flush_to_l1_inner`. `commit_transaction_l0` holds `flush_lock`, does its merge, conditionally calls `flush_to_l1_inner` directly. Total: one new internal method, one branch.

This is also exactly what the async-flush proposal needs for its three-step split (rotate → release → stream → re-acquire → finalize), so it's reusable.

### 5.6 `check_write_pressure` moves outside `flush_lock`

Today: called inside the writer-lock at every mutation, so its throttle sleeps starve peers.

After: called as `&self`, no outer lock. The throttle sleep is now self-throttling per-task without holding any shared lock. Bonus: convert from "sleep with global state" to "per-session token bucket" later if desired. Out of scope for this proposal.

### 5.7 Bulk loader hoist

`executor/write.rs:713`:

```rust
// Before:
for row_idx in 0..num_rows {
    let mut writer = writer_arc.write().await;
    let eid = writer.next_eid(...).await?;
    writer.insert_edge(...).await?;
}

// After:
let writer = writer_arc.as_ref();
for row_idx in 0..num_rows {
    let eid = writer.next_eid(...).await?;
    writer.insert_edge(...).await?;
}
```

Same at `:778`. The `Arc<Writer>` is shared; no lock to acquire. Subsumed cleanup, lands in the same PR as §5.4.

### 5.8 What happens to `write_lock_timeout`?

`crates/uni/src/api/transaction.rs:558–566` wraps `writer_lock.write()` in a 5-second timeout to surface `UniError::CommitTimeout`. After the change, there is no outer lock to time out on. The timeout moves to wrap `flush_lock.lock()` instead — same purpose (detect pathological commit delays) but only on the actually-serializing acquisition.

---

## 6. Correctness Arguments

### 6.1 Per-transaction L0 isolation

**Claim:** Each transaction's uncommitted mutations remain invisible to other transactions until commit.

**Argument:** The per-tx L0 buffer is `Arc<parking_lot::RwLock<L0Buffer>>` (writer.rs:213), created fresh per `create_transaction_l0` call. The Arc is held by exactly one `Transaction` instance. Mutation methods route to it via `resolve_l0(tx_l0)` (writer.rs:220–224). Reads via `QueryContext` (writer.rs:1197–1206, l0_visibility.rs) snapshot `(current_l0, pending_flush_l0s, tx_l0)` at query start — peer transactions' `tx_l0`s are not in that set. Removing the outer `Writer` lock does not change how `tx_l0` is plumbed; isolation is structurally preserved.

### 6.2 Commit ordering and main-L0 merge

**Claim:** Two concurrent commits cannot interleave their merges into main L0.

**Argument:** `commit_transaction_l0` (writer.rs:237) holds `flush_lock` across the merge (writer.rs:317–357). `flush_lock` is a single `tokio::sync::Mutex<()>`, so at most one merge runs at a time. The merge itself acquires the main-L0's own `parking_lot::RwLock` (writer.rs:320) for the duration of the merge body — this remains unchanged.

WAL append ordering: each commit appends its WAL batch under `flush_lock`, then `flush_wal().await` group-commits. Two concurrent commits will serialize on `flush_lock` for the append-and-flush window, exactly as today. LSN monotonicity is preserved.

### 6.3 Schema mutation safety

**Claim:** Schema changes (`CREATE LABEL`, `ALTER LABEL`, `CREATE CONSTRAINT`) cannot race with hot-path mutations in a way that violates constraints.

**Argument:** `SchemaManager` has its own internal `RwLock` (`crates/uni-common/src/core/schema.rs`). Hot-path mutations read-lock via `self.schema_manager.schema()` (writer.rs:1360). Schema mutations write-lock. Today, this is double-protected: schema lock + writer lock. After the change, only the schema lock remains. That is sufficient: if a hot-path mutation has read-locked the schema, a concurrent ALTER cannot proceed until the read-lock is released; conversely an in-flight ALTER blocks subsequent mutations from acquiring the read-lock.

The only loss is the *implicit barrier* the writer-write-lock provided: after a schema change committed, the next mutation through the writer was guaranteed to see it. We must verify that `SchemaManager` itself provides sufficient happens-before through its `RwLock`. **Action**: add a concurrent-schema-mutation stress test (§9).

### 6.4 `OnceLock` setters during `Uni::build()`

**Claim:** The init-time setters (`set_xervo_runtime`, `set_property_manager`, `set_index_rebuild_manager`) are called exactly once, before the database is exposed to user code.

**Argument:** All three are called from `UniBuilder` and `Uni::new_with_config` paths (`crates/uni/src/api/mod.rs:1978`, `:2013`, `crates/uni/src/api/builder.rs` calling `set_property_manager`). The `Uni` instance is not returned until builder completion. After the change, they go through `OnceLock::set`; a duplicate call returns `Err`, which we map to `UniError::AlreadyInitialized`. **Action**: audit all callers to verify single-call discipline, and add `debug_assert!` in tests.

Edge case: fork creation (`crates/uni/src/api/fork.rs`) creates a forked `Writer` with potentially different `xervo_runtime`/etc. The fork's `Writer` is a fresh instance, so its `OnceLock`s are fresh — no collision.

### 6.5 `last_flush_time` and `cached_manifest` Mutex correctness

**Claim:** The Mutex'd fields are only written under `flush_lock`, so the Mutex itself sees no contention; it exists only to satisfy Rust aliasing rules for `&self` access.

**Argument:** `last_flush_time` is written exclusively in `flush_to_l1` (writer.rs:2545) and read in `check_flush` (writer.rs:1806). Both are gated by `flush_lock` (after the change, `check_flush` is itself called either inside `commit_transaction_l0`'s `flush_lock` window or directly from background tickers; in the latter case it does not modify `last_flush_time`, only reads it).

`cached_manifest` is written in `flush_to_l1` (writer.rs:2520) and `.take()`'d in `flush_to_l1` (writer.rs:2215). Both inside `flush_lock`.

For both, the `parking_lot::Mutex` is essentially a static-checking aid; runtime cost is ≈3–5 ns uncontended.

### 6.6 `flush_in_progress` AtomicBool leak (pre-existing bug)

The async-flush proposal (§2.4) calls out that `StorageManager.flush_in_progress` leaks `true` on any `?` early-exit inside `flush_to_l1`. This proposal does not fix that bug, but the new `flush_lock` makes the RAII-guard fix trivial (acquire a `FlushGuard` that sets-true on acquire and sets-false on drop). Recommend installing the guard in Phase 1 of this proposal as a no-cost incidental fix.

### 6.7 Memory ordering on `AtomicU64`/`AtomicBool`

`fork_flush_count` and `fork_fragment_warn_fired` are observational only (used for fork-fragment guard rail). Use `Relaxed` ordering — no synchronization-with semantics needed. Reader sees an eventually-consistent count, which is fine for "warn if count > threshold".

---

## 7. Phased Rollout

### Phase 1: Field interior mutability + `flush_lock` introduction (no behavior change)

Convert the four cold-path-mutated fields to interior mutability. Add `flush_lock` to `Writer`. Install RAII guard for `flush_in_progress` (incidental fix).

**Files touched:**
- `crates/uni-store/src/runtime/writer.rs` (struct definition + 4 field accessors)

**Risk:** Very low. No public API change. All existing tests pass unchanged.

**Validation:** existing test suite + `cargo nextest run`.

### Phase 2: Hot-path method signature `&mut self` → `&self` (compile churn only)

Change signatures on the ~13 hot-path methods. Update internal callers within `writer.rs`. No call site outside `writer.rs` needs to change yet (the outer `RwLock` still exists; we just `.write().await` and call `&self` methods on the guard, which works trivially).

**Files touched:**
- `crates/uni-store/src/runtime/writer.rs` (signatures only)

**Risk:** Low. Pure compile-time refactor. If any method secretly mutated `Writer` state (audited not to, but Rust enforces it), the compiler catches.

**Validation:** existing test suite.

### Phase 3: `flush_to_l1` and `commit_transaction_l0` use `flush_lock` instead of `&mut self` (behavior identical)

Split `flush_to_l1` → `flush_to_l1_inner` (`&self`) + `flush_to_l1` entry (`&self`, acquires `flush_lock`). `commit_transaction_l0` holds `flush_lock` for the merge window. Outer writer-RwLock still present and still `.write().await`'d, so behavior is identical (double-locked but functionally same).

**Files touched:**
- `crates/uni-store/src/runtime/writer.rs`

**Risk:** Medium. Re-entrancy bug risk if any path holds `flush_lock` and recursively calls a method that also tries to acquire it. Audited paths: `commit_transaction_l0` → `check_flush` → `flush_to_l1_inner` (direct call, not entry point). Anywhere else? `tick_fork_fragment_observability` is called inside `flush_to_l1_inner` and does no further locking.

**Validation:** existing test suite + concurrent commit stress test.

### Phase 4: Drop the outer `tokio::sync::RwLock<Writer>` (behavior change — primary win)

Change `Arc<RwLock<Writer>>` → `Arc<Writer>` everywhere. Drop `.write().await` / `.read().await` at all ~37 call sites. Bulk loader hoist (§5.7) in the same PR. Move commit-timeout to wrap `flush_lock.lock()`.

Ship behind feature flag `uni.experimental.concurrent_writer` initially: keep both code paths (one with outer lock, one without) selected at `Uni::build` time. After 1–2 releases of telemetry, remove the flagged path.

**Files touched (Phase 4):**
- `crates/uni/src/api/mod.rs` — storage type (line 74), setter sites (1978, 2013), flush sites (1011, 1037, 1415, 2028, 2035), and read sites (1985, 2272)
- `crates/uni/src/api/transaction.rs` — write sites (360, 409, 517), read site (174), timeout wrap (558–566)
- `crates/uni/src/api/session.rs` — flush sites (498, 542)
- `crates/uni/src/api/fork.rs` — write (199) and read (243)
- `crates/uni/src/api/fork_index_builder.rs` — line 124
- `crates/uni/src/api/bulk.rs` — read sites (450, 919)
- `crates/uni/src/api/impl_query.rs` — read sites (166, 179)
- `crates/uni/src/api/impl_locy.rs` — read site (129)
- `crates/uni-query/src/query/executor/write.rs` — lines 389, 422, 713, 740, 778, 797, 1359, 1393
- `crates/uni-query/src/query/executor/core.rs` — line 366
- `crates/uni-query/src/query/executor/procedure.rs` — line 223
- `crates/uni-query/src/query/executor/read.rs` — line 3817 (and any nearby flush hooks)
- `crates/uni-query/src/query/df_graph/mutation_common.rs` — line 542
- `crates/uni-query/src/query/df_graph/mutation_foreach.rs` — line 204

**Risk:** Medium-high. The behavior change is the entire point; if any of the correctness arguments in §6 are wrong, this will surface here. Mitigation: feature flag + extensive stress testing.

**Validation:** §9 testing strategy.

### Phase 5: Enable by default

After ≥1 release in production at scale with the flag on, flip default to on. Keep flag for ≥1 more release to allow rollback.

### Phase 6: Remove the flagged path + clean up

Delete the old `RwLock<Writer>` code path. Delete the `WriterRef` type alias (if any). Clean up `commit-timeout`'s reference to outer lock.

---

## 8. Configuration

`uni.experimental.concurrent_writer: bool` — feature flag for Phase 4. Default `false` in Phase 4–5; default `true` in Phase 5 release; removed in Phase 6.

`uni.commit_timeout_ms: u64` — already exists for the writer-lock acquisition timeout (default 5000). Repurposed to wrap `flush_lock.lock()` in Phase 4. No user-visible change.

No other new configuration. The `check_write_pressure` throttle config (`config.throttle.soft_limit`, `config.throttle.hard_limit`, `config.throttle.base_delay`) is unchanged.

---

## 9. Testing Strategy

### 9.1 Unit tests

- **`writer.rs` mutation-method audit:** add `#[test]` that constructs a `Writer`, calls each hot-path mutation method, and asserts no `Writer` field has changed (by comparing field-by-field before/after). This is a regression test for §2.3 (the audit) — if someone later adds a write to a `Writer` field in a hot-path method, the test fails loudly.

### 9.2 Concurrent-writer stress (the headline test)

`crates/uni/benches/writer_lock_contention.rs` (new):

```rust
// Spawn N tokio tasks each running 100 trivial CREATE statements inside a tx.
// N ∈ {1, 4, 12, 24}. Measure wall time and per-task time.
// Expected before this proposal: per-task time ≈ N × baseline (lock-serialized).
// Expected after: per-task time ≈ baseline (concurrent).
```

Run before and after each phase. The acceptance criterion for Phase 4 is per-task time at N=24 within 2× of N=1 baseline on a workload of trivial CREATEs.

### 9.3 Concurrent-schema-mutation correctness

New test `crates/uni/tests/concurrent_schema_test.rs`: spawn one task doing `ALTER LABEL Person ADD PROPERTY age INT` while N tasks concurrently `CREATE (:Person {name: 'x', age: 5})`. Verify all post-alter creates succeed with the new property, and no pre-alter create writes a row with the new column observably absent.

### 9.4 Per-tx isolation property test

`proptest`-style: generate two random transaction scripts T1, T2 that read/write disjoint or overlapping vertex sets. Run them concurrently; assert that T1 cannot read T2's uncommitted state. Already covered partially by existing tests; extend to run at N=24 concurrency.

### 9.5 Commit ordering test

Spawn N concurrent commits, each appending a tagged WAL entry. After all commit, replay WAL and verify the entries appear in commit order (which is `flush_lock` acquire order).

### 9.6 Fork interaction

Each fork has its own `Writer` (per `crates/uni-store/src/fork/writer_factory.rs`). Verify that two forks can be written to concurrently — independent `flush_lock`s per writer, no cross-fork serialization.

### 9.7 Benchmark suite re-run

Re-run `crates/uni/benches/comprehensive.rs` and any existing `BENCH_*` benchmarks at both flag-on and flag-off; expect:
- Sess=1: ≤5% overhead from atomic ops and OnceLock indirection.
- Sess=4: 2–3× improvement.
- Sess=24: 5–10× improvement.

### 9.8 Existing test suite

`cargo nextest run` must pass at every phase. No existing test should require modification (signatures are widened from `&mut self` to `&self`, which only relaxes constraints).

---

## 10. Observability

### 10.1 New metrics

- `uni_writer_flush_lock_wait_ms` (histogram) — time spent acquiring `flush_lock` per commit. Should be near-zero except under heavy commit concurrency or during flush.
- `uni_writer_flush_lock_hold_ms` (histogram) — time `flush_lock` is held per acquisition. Distinguish commit-merge holds (~ms) from flush holds (~50–100ms today, ~µs after async-flush proposal lands).
- `uni_writer_concurrent_mutations_inflight` (gauge) — number of in-flight `insert_*`/`delete_*` calls. Should scale with session concurrency after Phase 4.

### 10.2 Removed/changed metrics

`uni_writer_lock_wait_ms` (the existing outer-writer-lock wait metric, if any) — removed in Phase 6.

### 10.3 Tracing

Add `#[instrument(level = "trace", skip(self))]` to `flush_to_l1_inner`, `commit_transaction_l0` (already has it), and the hot-path mutation methods (already have it). The existing instrumentation is sufficient.

---

## 11. Risks and Open Questions

### 11.1 Risks

1. **Hidden `&mut self` requirement in helper methods.** The audit (§2.3) covers the top-level mutation methods, but each calls 3–5 helpers (`process_embeddings_for_labels`, `validate_vertex_constraints`, `prepare_vertex_upsert`, etc.). If any helper writes to a `Writer` field, the Phase 2 conversion fails to compile. **Mitigation:** the Rust compiler is the verifier; this risk is "Phase 2 takes longer than expected", not a correctness risk.

2. **Subtle ordering bug in schema mutations.** The outer writer-lock provided an implicit happens-before barrier that we're removing. `SchemaManager`'s own `RwLock` must be sufficient. **Mitigation:** §9.3 stress test.

3. **Re-entrancy in `flush_lock`.** Phase 3 introduces the inner-vs-entry-point split; any new path that calls `flush_to_l1` while holding `flush_lock` deadlocks. **Mitigation:** §5.5 design choice + audit; explicit panic on re-acquire-by-same-task in debug builds.

4. **Performance regression at sess=1.** OnceLock indirection, Mutex on `last_flush_time`/`cached_manifest`, atomic ops on counters all add ~ns per call. At sess=1 with high throughput, this could measurably regress. **Mitigation:** §9.7 benchmark; if regression >5%, profile and inline.

5. **API breakage for downstream code holding a `&mut Writer`.** External crates (if any) that mutate `Writer` directly will break. **Mitigation:** `Writer` is `pub(crate)` exposed only via `Arc<RwLock<Writer>>`; no downstream code can hold `&mut Writer` today.

### 11.2 Open questions

- **OQ1**: Should we extend the OnceLock pattern to `xervo_runtime` even for the bulk-loader path that currently uses `set_xervo_runtime`? Need to verify the bulk loader doesn't reset xervo mid-run. (Initial scan suggests no, but verify in Phase 1.)

- **OQ2**: Does the property manager (`PropertyManager`) have any `&mut self` methods called from hot-path? Initial scan suggests no — all access goes through `Arc<PropertyManager>` and internal Mutex'd caches — but worth confirming as part of Phase 2's helper audit.

- **OQ3**: At sess=24 after Phase 4, what becomes the next bottleneck? Likely candidates: (a) main-L0's `parking_lot::RwLock` during commit merge; (b) `IdAllocator`'s `tokio::Mutex` at batch boundaries (object-store write of manifest); (c) Lance manifest contention on flush. We expect (b) and (c) to fold into the async-flush proposal's separately-tracked work. (a) is fundamental to LSM semantics and not addressable here.

- **OQ4**: Do we want a separate read-side throttle / fair scheduler now that writes don't serialize through a central point? Likely no — read concurrency was never the bottleneck — but worth observing post-Phase 4.

---

## 12. Estimated Effort

- Phase 1: 0.5–1 day (mechanical field conversion + RAII guard).
- Phase 2: 1–2 days (signature changes + helper audit).
- Phase 3: 1 day (flush_lock + inner/entry split).
- Phase 4: 2–3 days (call-site updates + feature flag + bulk hoist).
- Phase 5: 1 day (flip default + telemetry review).
- Phase 6: 0.5 day (cleanup).
- Test development (concurrent stress, schema race, isolation property): 2–3 days, can parallel Phase 1–3.

**Total**: ~2 weeks of engineering, with most of the calendar time being Phase 4–5 soak.

---

## 13. Appendix A — Acknowledgments to the existing design

`Writer::create_transaction_l0` (writer.rs:210), `Writer::replay_wal` (writer.rs:156), and the entire `IdAllocator` API are already `&self`. The `tx_l0` parameter threading through every mutation method (`tx_l0: Option<&Arc<RwLock<L0Buffer>>>`) was designed for exactly the concurrency model this proposal completes. Whoever did that work set up the substrate; this proposal removes the residual outer lock that was preventing it from delivering.

---

## 14. Appendix B — Why not Option B (read-lock instead of remove-lock)?

An alternative design: keep `Arc<tokio::sync::RwLock<Writer>>`, but change hot-path methods to `&self` and downgrade their lock to `.read().await`. `flush_to_l1` keeps `.write().await`.

Pros: smaller diff at Phase 4 (no `Arc<Writer>` migration).

Cons:
- Still pays `tokio::sync::RwLock` acquisition cost on every mutation (≈100 ns p50, more under contention from the writer side).
- The `RwLock` write-lock starves reads when the writer-side is pending; pathological mixes of "long write phase queued behind many reads" can still occur during flush.
- The model "this lock is held but doesn't serialize anyone" is confusing for readers.
- Eventually we want `Arc<Writer>` anyway for clarity.

Rejected. The proposed design (drop the outer lock entirely; use `flush_lock` only where exclusion is semantically required) is cleaner.

---

## 15. Appendix C — Quick reference of measured signatures

From the in-flight workload at sess=24 vs sess=1, the prediction "per-task time stays roughly constant as N grows" is the falsifier for the writer-lock hypothesis. The observed numbers:

| N (sess) | wall (s) | per-task (s) | inflation |
|---|---|---|---|
| 1 | 284 | 284 | 1.0× |
| 24 | 192 | ~190 | (lower wall, higher per-task) |

Wall scales 1.5×, not 24× — so we ARE getting some concurrency, just not much. After Phase 4, we expect:

| N (sess) | wall (s) | per-task (s) |
|---|---|---|
| 1 | ~284 | ~284 |
| 24 | ~30–50 | ~30–50 (bounded by L0-merge or Lance I/O at commit, not writer-lock) |

This is the acceptance target.

---

**End of document.**
