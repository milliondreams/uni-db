# Async L0→L1 Flush — Design Document

**Status:** Revised (Draft, post-concurrent_writer baseline)
**Date:** 2026-05-17 (original), 2026-05-18 (revision)
**Author:** rohit@dragonscale.ai
**Crates touched:** `uni-store`, `uni-common`, `uni`
**Related:** `docs/proposals/concurrent_writer.md` (landed; this proposal composes on top of its `flush_lock` seam), `docs/proposals/graph_fork_plan.md`, `docs/proposals/fork_pending.md`

---

## 0. Revision history

This proposal was originally drafted before the concurrent_writer refactor landed. That refactor:

- Removed `Arc<tokio::sync::RwLock<Writer>>` entirely (Phase 4) — there is no longer a "writer write-lock" to amortize against.
- Promoted `last_flush_time`, `cached_manifest`, `fork_flush_count`, `fork_fragment_warn_fired` to interior mutability (Phase 1).
- Installed `FlushInProgressGuard` RAII fixing the §2.4-#1 leak bug.
- Introduced `flush_lock: tokio::sync::Mutex<()>` and split `flush_to_l1` into a thin entry + `flush_to_l1_inner` body (Phase 3).
- Made `flush_to_l1` and `commit_transaction_l0` `&self` (Phase 2).

So the prep work in §11 Phase 1 of this proposal is **largely done**. The proposal's design still applies — but the "writer-write-lock" framing in the summary and §2.1 / §2.2 must be replaced with `flush_lock`, and method signatures updated to `&self`.

The headline empirical claim ("3–9 s wall reduction at sess=24") was also calibrated against the pre-refactor baseline. Post-refactor measurement (`crates/uni/examples/flush_pressure.rs`) shows the actual win is **regime-dependent**:

| Workload regime | Flushes per round | Wall time | Async-flush win |
|---|---|---|---|
| `auto_flush_threshold=1000`, sess=24 | 120 | 51 s | very high (~10×) |
| `auto_flush_threshold=2500`, sess=24 | 48 | 21.5 s | high (~5×) |
| `auto_flush_threshold=5000`, sess=24 | 24 | 10.9 s | high (~3×) |
| **`auto_flush_threshold=10000` (default), sess=24** | **12** | **1.7 s** | **modest (~30–40%)** |
| Cloud-storage backend (extrapolated) | any | proportional | high regardless of threshold |

The proposal still has runway, especially for cloud-storage workloads and high-mutation-rate ingestion. But it is no longer the "obvious next phase" of the concurrent_writer refactor — the U-shaped cost curve in `flush_pressure.rs` shows default-config workloads land in the sweet spot where flushes amortize well today.

---

## 1. Summary

`Writer::flush_to_l1` is invoked from `check_flush` at the tail of every `commit_transaction_l0` whose `should_flush()` predicate fires (default: every 10,000 mutations). When fired, the entire flush runs under `flush_lock` — including the long L1-streaming I/O step (~50–100 ms on local disk, potentially seconds on cloud storage). With N concurrent committers, all subsequent commits that also need to flush serialize through this lock.

The L1 streaming step does not logically need exclusivity. The rotation (`L0Manager::begin_flush`) and finalization (`L0Manager::complete_flush` + WAL truncate + manifest cache update) need brief exclusivity (~µs). Everything in between is I/O against immutable, Arc-clonable state. This document proposes splitting `flush_to_l1_inner` into **rotate / stream / finalize**, running `stream` on a background tokio task **outside `flush_lock`**, with bounded concurrency, ordered finalization, and explicit back-pressure.

Expected outcome: `flush_lock`-held time per flush drops from O(50–100 ms) to O(100 µs) for the rotate + finalize critical sections combined. The wall-time win at the application layer depends on workload:

- **Default-threshold, local-disk ingestion**: 30–40% wall reduction at sess=24 (flushes are infrequent but each one stalls others).
- **Low-threshold or cloud-storage ingestion**: 3–10× wall reduction (the original headline claim).

No on-disk format change.

`★ Insight ─────────────────────────────────────`
- The infrastructure for this is already half-built. `L0Manager.pending_flush` is `Vec<Arc<RwLock<L0Buffer>>>` (`l0_manager.rs:17`) — a *list*, not a single slot — and `min_pending_wal_lsn` (`l0_manager.rs:102-114`) already correctly handles N pending entries. `complete_flush` (`l0_manager.rs:94-97`) uses pointer-equality eviction, supporting out-of-order completion. Someone planned for this.
- The non-trivial constraint isn't the L0 data structures; it's the **manifest chain**: each manifest's `parent_snapshot` links to the previous one (`snapshot.rs:9-20`), so finalize-order must equal rotate-order even if stream-order doesn't.
- Phase 3 of `concurrent_writer.md` deliberately introduced `flush_to_l1_inner` as the seam this proposal composes against. The split was prep work for exactly this refactor.
`─────────────────────────────────────────────────`

---

## 2. Background

### 2.1 Today's commit path (post-concurrent_writer)

`crates/uni/src/api/transaction.rs:~558`:

```rust
let writer: &uni_store::Writer = writer_lock.as_ref();
let wal_lsn = tokio::time::timeout(
    Duration::from_secs(5),
    writer.commit_transaction_l0(self.tx_l0.clone()),
)
.await
.map_err(|_| UniError::CommitTimeout { ... })??;
```

`Writer::commit_transaction_l0` (`writer.rs:~284`) takes `flush_lock` and does:

1. Acquires `flush_lock: tokio::sync::Mutex<()>` (single-writer serialization for the merge + flush window).
2. WAL append per mutation — synchronous, buffered into `WalState.buffer`.
3. `flush_wal().await` — group-commit one segment to object store. Durability point. ~5–10 ms.
4. Merge tx_l0 into main L0 — in-memory, ~µs.
5. Replay edges through `adjacency_manager` — per-edge, ~21 µs each (measured).
6. `update_metrics()`.
7. `if should_flush() { flush_to_l1_inner(None).await }` — the keystone: if mutation count hit the threshold, the **same flush_lock** is held across the full `flush_to_l1_inner` body (which is itself the long L1-streaming work).
8. Release `flush_lock`.

The key cost: **when `should_flush()` fires, every concurrent committer that also wants to commit will queue on `flush_lock`** for the full duration of the flush. With default `auto_flush_threshold=10_000`, one in ~10k mutations triggers this. At sess=24 doing 24 small concurrent commits per round, this is a few times per workload — modest impact (~30–40% wall on the measured pattern). At low thresholds or with slow object-store I/O, the impact compounds dramatically.

### 2.2 Today's `flush_to_l1_inner` body

Post-concurrent_writer signature:
```rust
pub async fn flush_to_l1(&self, name: Option<String>) -> Result<String> {
    let _flush_lock_guard = self.flush_lock.lock().await;
    self.flush_to_l1_inner(name).await
}
async fn flush_to_l1_inner(&self, name: Option<String>) -> Result<String> { ... }
```

The `flush_to_l1_inner` body still runs as one ~600-line unit. Line numbers below refer to the current post-refactor code (offsets shifted by ~30 from the pre-refactor numbers in the original draft).

| Phase | What it does | Lock truly required? |
|-------|--------------|----------------------|
| **A. WAL pre-flush** | `wal.flush().await` captures LSN before rotation | No (WAL has its own internal `Mutex<WalState>`) |
| **B. L0 rotate** | `l0_manager.begin_flush(0, None)` — atomic Arc-swap of current L0, push old onto `pending_flush` | Brief (`L0Manager` &self with its own internal lock) |
| **C. WAL handoff** | Write lock on old L0 to record `wal_lsn_at_flush`, take WAL, give to new L0 | Brief (`L0Buffer` lock) |
| **D. L1 collect** | Read-lock old L0; gather edges, tombstones, vertices-by-label, timestamps | No (read on Arc'd old L0) |
| **E. Orphan resolve** | `find_vertex_labels_in_storage` fallback for orphaned tombstones | No (read against StorageBackend) |
| **F. Manifest seed** | Load `cached_manifest` (now `parking_lot::Mutex<Option<…>>`, Phase 1), set `parent_snapshot`, new `snapshot_id`, HWMs | Brief (Mutex<Manifest>) |
| **G. Lance writes** | `MainEdgeDataset::write_batch`, `MainVertexDataset::write_batch`, `DeltaDataset::write_run` per (edge_type, dir), `VertexDataset::write_batch` per label, `VidLabelsIndex` updates, inverted-index updates, UID mapping. **All eager Lance commits.** ~50–100 ms typical local, 100s of ms cloud. | No (StorageManager methods are `&self`; backend is `Arc<dyn StorageBackend>`) |
| **H. Manifest publish** | `snapshot_manager().save_snapshot(&manifest).await`, then `set_latest_snapshot(&snapshot_id)` | No (own internal sync) |
| **I. cached_manifest** | `*self.cached_manifest.lock() = Some(manifest.clone())` | Brief Mutex (interior-mut since Phase 1) |
| **J. complete_flush** | `l0_manager.complete_flush(&old_l0_arc)` | Brief (L0Manager &self) |
| **K. WAL truncate** | `wal.truncate_before(min(min_pending_wal_lsn, wal_lsn)).await` | No (WAL &self) |
| **L. Cache clear** | `property_manager.clear_cache().await` | No |
| **M. last_flush_time** | `*self.last_flush_time.lock() = Instant::now()` | Brief Mutex (interior-mut since Phase 1) |
| **N. Metrics** | Histogram + counters | No |
| **O. FlushInProgressGuard drop** | Releases `flush_in_progress` flag (Phase 1 RAII guard) | No |
| **P. Compaction status** | `compaction_status.lock(); status.l1_runs += 1` | No |
| **Q. Adjacency compact** | Optionally **spawn background** `am.compact()` — already async | No |
| **R. Index rebuild** | Schedule index rebuilds if auto-rebuild enabled | No |
| **S. Fork observability** | `tick_fork_fragment_observability()` increments `fork_flush_count` (AtomicU64 since Phase 1) | No |

Post-concurrent_writer, **only B, C, I, J, M need brief-but-not-`flush_lock`-level exclusion** — they each have their own fine-grained locks. The outer `flush_lock` held for the entire body is what serializes peer commits. Phases A, D–H, K, L, N–S are all `&self`-safe and could run on a spawned task.

### 2.3 Today's other coupling

- **`flush_in_progress: AtomicBool`** on `StorageManager` (`manager.rs:71`). Set true at flush start (writer.rs:2003–2005), false at end (writer.rs:2558–2560). Read by **compaction**, not flush: `compact_adjacency` skips the Delta-L1 clear when set (`storage/compaction.rs:562-569`) to avoid wiping rows the flush is about to append.
- **`L0Manager.pending_flush: RwLock<Vec<Arc<RwLock<L0Buffer>>>>`** (`l0_manager.rs:17`). Holds rotated L0s until `complete_flush` is called. Reads consult this via `QueryContext.pending_flush_l0s` (`runtime/l0_visibility.rs`, `runtime/context.rs:11-21`), so visibility is preserved during flush.
- **Manifest pointer**: object-store write of `catalog/manifests/{snapshot_id}.json` followed by `catalog/latest` (snapshot/manager.rs:24–30). Two-step but ordered: manifest body first, pointer second.
- **WAL truncation safety**: `safe_lsn = min(L0Manager::min_pending_wal_lsn(), this_flush_lsn)` (writer.rs:2528-2535).
- **Crash recovery**: purely WAL-driven via `wal_high_water_mark` in the most recent manifest (`snapshot.rs:9-20`). Manifest does **not** enumerate pending L0 segments. On replay, all WAL segments with LSN > HWM are replayed. Lance writes are append-only per dataset with per-flush-unique versions/EIDs — partial-flush re-writes on replay are idempotent at the read-merge layer (version dedup).
- **Read visibility during flush**: `QueryContext` is constructed by snapshotting `(current_l0, pending_flush_l0s, tx_l0)` at query start (writer.rs:1197-1206, l0_visibility.rs). Once snapshotted, Arcs are held — even if the writer subsequently runs `complete_flush`, the reader still sees the old buffer. Eager L1 writes during flush may also be seen, but version/EID dedup at merge time keeps results correct.

### 2.4 Pre-existing latent issues (worth noting but not the focus)

1. **`flush_in_progress` is leaked on every error path**. Every `?` between writer.rs:2003 (set true) and writer.rs:2558 (set false) returns early without resetting the flag. After such an error, **all subsequent compactions skip the delta-clear forever** until process restart. This is a real bug today that the refactor must fix incidentally (Phase 1 below installs an RAII guard).
2. **`flush_to_l1` is a 608-line monolith** with no internal seams. Future work (e.g., parallelizing per-edge-type writes within a flush) is gated on first decomposing it.

---

## 3. Goals & Non-Goals

### 3.1 Goals

- **G1**: Reduce writer-write-lock-held time per L0→L1 flush from O(50–100 ms) to O(100 µs).
- **G2**: Preserve current correctness guarantees: snapshot isolation, WAL truncation safety, manifest ordering, crash recovery via WAL replay.
- **G3**: Preserve current API surface. `db.flush()`, `Writer::flush_to_l1`, and `check_flush` keep their signatures (or gain a sync/async variant pair).
- **G4**: Bounded resource use: cap on pending flushes, bounded WAL growth, no unbounded queue.
- **G5**: Compatible with fork system: per-fork independence retained; `drop_fork` waits for pending flushes on that fork.
- **G6**: Fix the pre-existing `flush_in_progress` leak (incidental, but enabled by the refactor).
- **G7**: No on-disk format change. Existing manifests, WAL, Lance datasets, and `catalog/latest` continue to work; downgrade is a hot swap.

### 3.2 Non-Goals

- **NG1**: Parallelizing the *internal* I/O within a single flush (multi-dataset write concurrency). Possible follow-up.
- **NG2**: Removing the writer write-lock entirely from `commit_transaction_l0`. Commit still serializes WAL append + L0 merge; that's a separate redesign.
- **NG3**: Cross-fork flush coordination. Forks remain independent.
- **NG4**: Changing WAL durability semantics (`ObjectStore::put` is still the durability point).
- **NG5**: Background-driven flush scheduling (decoupling flush from `check_flush` calls at the tail of commits). The commit still **triggers** the flush; we only make the flush's execution non-blocking. A standalone flush scheduler is future work.

---

## 4. Design Overview

Split `Writer::flush_to_l1` into three logical steps that compose:

```text
┌─────────────────────┐  Writer lock: WRITE (brief, ~µs)
│ 1. rotate           │  - WAL flush, capture LSN
│                     │  - L0Manager::begin_flush (Arc-swap)
│                     │  - record wal_lsn_at_flush on old L0
│                     │  - hand WAL to new L0
│                     │  - acquire back-pressure permit
│                     │  - emit RotatedFlush handle
└─────────────────────┘
           │ spawn(stream_then_finalize)
           ▼
┌─────────────────────┐  NO writer lock; runs on tokio runtime
│ 2. stream (async)   │  - load cached_manifest snapshot (from Arc)
│                     │  - collect edges/vertices from rotated L0
│                     │  - Lance writes (append-only, eager)
│                     │  - save_snapshot → set_latest_snapshot
│                     │  - emit FlushOutcome
└─────────────────────┘
           │ post to ordered finalize queue
           ▼
┌─────────────────────┐  Per-Writer single-task finalizer.
│ 3. finalize         │  Lock-free: all state Arc'd.
│                     │  - wait until predecessors finalized (in-order)
│                     │  - update cached_manifest (interior-mut)
│                     │  - L0Manager::complete_flush
│                     │  - WAL truncate_before(min_pending_wal_lsn)
│                     │  - last_flush_time = Instant::now()
│                     │  - flush_in_progress_count.fetch_sub(1)
│                     │  - metrics, fork-fragment tick
│                     │  - spawn am.compact (already async today)
│                     │  - release back-pressure permit
└─────────────────────┘
```

**Key design properties:**

- Step 1 is the only step that runs under the writer write-lock.
- Step 2 runs unbounded-in-parallel up to `max_pending_flushes` (default: 2).
- Step 3 runs serially, in rotate-order, to preserve manifest parent chain.
- A flush failure aborts step 3 cleanly: complete_flush is not called, WAL is not truncated, permit is released, counter is decremented. Crash recovery via WAL replay covers any partial L1 fragments.

---

## 5. Detailed Design

### 5.1 Writer field changes

Several `Writer` fields are plain owned values today and require `&mut self` to mutate. To let `finalize` run on Arc'd state without re-locking, promote them to interior mutability.

`crates/uni-store/src/runtime/writer.rs:45-83` becomes:

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
    compaction_handle: Arc<parking_lot::RwLock<Option<JoinHandle<()>>>>,
    index_rebuild_manager: Option<Arc<IndexRebuildManager>>,
    pub fork_id: Option<ForkId>,

    // Promoted to interior mutability for async finalize:
    last_flush_time: Arc<parking_lot::Mutex<Instant>>,
    cached_manifest: Arc<parking_lot::RwLock<Option<SnapshotManifest>>>,
    fork_flush_count: Arc<AtomicU64>,
    fork_fragment_warn_fired: Arc<AtomicBool>,

    // New: flush coordination
    flush_coordinator: Arc<FlushCoordinator>,
}
```

`FlushCoordinator` is a new struct owning the back-pressure semaphore, the finalize queue, the rotate-sequence counter, and a handle to the finalizer task. See §5.4.

`StorageManager.flush_in_progress: AtomicBool` (manager.rs:71) becomes `flush_in_progress_count: AtomicUsize` (§5.5).

### 5.2 The three-step split

```rust
impl Writer {
    /// Sync (legacy) flush: rotate + stream + finalize, all awaited.
    /// Used by explicit `db.flush()`, nested-fork branching, and tests.
    /// Post-Phase-2 (concurrent_writer), all flush methods are `&self`.
    pub async fn flush_to_l1(&self, name: Option<String>) -> Result<String> {
        let _flush_lock_guard = self.flush_lock.lock().await;
        let rotated = self.flush_l0_rotate(name).await?;
        // Release flush_lock here (drop _flush_lock_guard) before stream:
        drop(_flush_lock_guard);
        let outcome = Self::flush_stream_l1(rotated.clone(), self.shared_for_stream()).await?;
        self.flush_finalize_now(rotated, outcome).await
    }

    /// Async (new) flush: rotate under flush_lock, then spawn stream+finalize.
    /// Used by `check_flush` on the commit path. Returns immediately after rotate.
    pub async fn flush_to_l1_async(&self, name: Option<String>) -> Result<FlushTicket> {
        let rotated = {
            let _flush_lock_guard = self.flush_lock.lock().await;
            self.flush_l0_rotate(name).await?
        };
        let shared = self.shared_for_stream();
        let coordinator = self.flush_coordinator.clone();
        let ticket = rotated.ticket();
        tokio::spawn(async move {
            coordinator.run_stream_and_finalize(rotated, shared).await;
        });
        Ok(ticket)
    }

    async fn flush_l0_rotate(&self, name: Option<String>) -> Result<RotatedFlush> {
        // Phase A,B,C from §2.2. Plus acquire back-pressure permit and seq number.
        let permit = self.flush_coordinator.acquire_permit().await?;
        let seq = self.flush_coordinator.next_rotate_seq();

        let wal_for_truncate = self.l0_manager.get_current().read().wal.clone();
        let wal_lsn = if let Some(ref w) = wal_for_truncate { w.flush().await? } else { 0 };

        let old_l0_arc = self.l0_manager.begin_flush(0, None);
        {
            let mut old = old_l0_arc.write();
            old.wal_lsn_at_flush = wal_lsn;
            let wal = old.wal.take();
            let current_version = old.current_version;
            let new = self.l0_manager.get_current();
            let mut new_g = new.write();
            new_g.wal = wal;
            new_g.current_version = current_version;
        }

        self.storage.flush_in_progress_count.fetch_add(1, Ordering::AcqRel);

        Ok(RotatedFlush {
            seq,
            old_l0_arc,
            wal_lsn,
            name,
            permit,
            // capture manifest snapshot for stream's parent_snapshot
            parent_manifest: self.cached_manifest.read().clone(),
        })
    }

    fn shared_for_stream(&self) -> SharedStreamCtx {
        SharedStreamCtx {
            storage: self.storage.clone(),
            schema_manager: self.schema_manager.clone(),
            l0_manager: self.l0_manager.clone(),
            adjacency_manager: self.adjacency_manager.clone(),
            property_manager: self.property_manager.clone(),
            cached_manifest: self.cached_manifest.clone(),
            last_flush_time: self.last_flush_time.clone(),
            fork_id: self.fork_id,
            fork_flush_count: self.fork_flush_count.clone(),
            fork_fragment_warn_fired: self.fork_fragment_warn_fired.clone(),
            config: self.config.clone(),
        }
    }

    /// Step 2: stream. Runs without writer lock. Reads old L0 + writes Lance.
    /// Returns a FlushOutcome carrying the new manifest and minimal state for finalize.
    async fn flush_stream_l1(
        rotated: RotatedFlush,
        shared: SharedStreamCtx,
    ) -> Result<FlushOutcome> {
        // Phases D, E, F, G, H from §2.2 — moved verbatim, no logical change.
        // Inputs come from `rotated.old_l0_arc` (read-lock only) and `shared.*` Arcs.
        // The cached_manifest read at top of step F is replaced by rotated.parent_manifest.
        // Returns: FlushOutcome { new_manifest, snapshot_id }
        ...
    }
}
```

`RotatedFlush` owns the `permit: SemaphorePermit<'static>` (acquired via `Arc<Semaphore>::acquire_owned`). The permit is released only when finalize completes (success or failure).

### 5.3 Finalizer: ordered single-task

The parent-chain constraint (`manifest.parent_snapshot = prev.snapshot_id`) requires that **finalize executes in rotate-order**. Stream can complete out of order (Flush B's stream might finish before Flush A's), but finalize must serialize.

`FlushCoordinator` owns a single tokio task that consumes `(seq, FlushOutcome, RotatedFlush)` tuples from an mpsc channel, sorts by seq via a small `BinaryHeap`, and applies them in order. If seq N+1 arrives while waiting for N, it parks in the heap.

```rust
struct FlushCoordinator {
    permits: Arc<tokio::sync::Semaphore>,        // back-pressure: N pending flushes
    next_seq: AtomicU64,
    submit_tx: mpsc::UnboundedSender<FlushSubmit>,
    // finalizer task receives FlushSubmit, finalizes in seq order
}

struct FlushSubmit {
    seq: u64,
    rotated: RotatedFlush,
    result: Result<FlushOutcome>,    // Err = stream failed
}

impl FlushCoordinator {
    async fn run_stream_and_finalize(self: Arc<Self>, rotated: RotatedFlush, shared: SharedStreamCtx) {
        let seq = rotated.seq;
        let result = Writer::flush_stream_l1(rotated.clone(), shared.clone()).await;
        let _ = self.submit_tx.send(FlushSubmit { seq, rotated, result });
        // finalizer task picks it up in seq order
    }

    // Finalizer task loop (spawned at Writer construction).
    async fn finalizer_loop(submit_rx: mpsc::UnboundedReceiver<FlushSubmit>, shared: SharedStreamCtx, ...) {
        let mut pending: BinaryHeap<Reverse<(u64, FlushSubmit)>> = BinaryHeap::new();
        let mut expected: u64 = 0;
        while let Some(submit) = submit_rx.recv().await {
            pending.push(Reverse((submit.seq, submit)));
            while let Some(Reverse((seq, _))) = pending.peek() {
                if *seq != expected { break; }
                let Reverse((_, s)) = pending.pop().unwrap();
                Self::finalize_one(s, &shared).await;
                expected += 1;
            }
        }
    }

    async fn finalize_one(submit: FlushSubmit, shared: &SharedStreamCtx) {
        let RotatedFlush { old_l0_arc, wal_lsn, permit, .. } = submit.rotated;
        match submit.result {
            Ok(outcome) => {
                *shared.cached_manifest.write() = Some(outcome.new_manifest.clone());
                shared.l0_manager.complete_flush(&old_l0_arc);
                if let Some(wal) = shared.l0_manager.get_current().read().wal.clone() {
                    let safe_lsn = shared.l0_manager.min_pending_wal_lsn()
                        .map(|m| m.min(wal_lsn)).unwrap_or(wal_lsn);
                    if let Err(e) = wal.truncate_before(safe_lsn).await {
                        tracing::warn!(error=%e, "WAL truncate failed");
                    }
                }
                *shared.last_flush_time.lock() = Instant::now();
                shared.storage.flush_in_progress_count.fetch_sub(1, Ordering::AcqRel);
                tick_fork_observability(&shared);
                metrics::histogram!("uni_flush_duration_seconds").record(/* total */);
                // spawn am.compact (as today)
            }
            Err(e) => {
                // Stream failed. Leave old_l0 in pending_flush so reads still see it.
                // Do NOT complete_flush. Do NOT truncate WAL.
                shared.storage.flush_in_progress_count.fetch_sub(1, Ordering::AcqRel);
                tracing::error!(error=%e, "L1 flush stream failed; old L0 retained, WAL preserved");
                metrics::counter!("uni_flush_failures_total").increment(1);
            }
        }
        drop(permit);  // release back-pressure permit AFTER finalize, so back-pressure
                       // counts in-flight + queued-for-finalize, not just streaming.
    }
}
```

**Why a single finalizer task?** The permit doesn't suffice — even with `max_pending_flushes = 2`, you could have flushes A, B where B's stream finishes first. B must wait for A to finalize before reading `cached_manifest` to set its `parent_snapshot`… except `parent_snapshot` is already captured at *rotation* time in `RotatedFlush.parent_manifest` (§5.2). So actually B doesn't need to wait at rotation time, and stream can also proceed in parallel — only `cached_manifest` *write* and `complete_flush` must serialize, and those are cheap. The serialization is to preserve the **invariant that `cached_manifest` always equals the most recent successfully published manifest**.

There is a subtle case: if flush A *fails* (stream error), should B's manifest's `parent_snapshot` point to A's parent (skipping A)? Yes — because A's snapshot was never persisted. The finalizer handles this by NOT updating `cached_manifest` on A's failure, so B's `parent_manifest` (captured at B's rotation, before A's failure was known) may be stale. Fix: at finalize time, if the predecessor failed, **rewrite** `outcome.new_manifest.parent_snapshot` to point to the current `cached_manifest.snapshot_id` and re-call `save_snapshot` + `set_latest_snapshot`. This is an extra object-store round-trip on the failure-recovery path only.

`★ Insight ─────────────────────────────────────`
- This is the same pattern as a log-structured database's "switch-and-flush": separating the **mutating boundary event** (rotate) from the **bulk work** (stream) from the **publishing event** (finalize). Cassandra's memtable→SSTable, RocksDB's memtable flush, and LMDB's write-back all have this shape. The interesting bit is the parent-snapshot chain — most LSMs use an unordered SSTable set, so finalize-order doesn't matter. We need ordered finalize because our manifest is a linked list.
- The alternative (drop the parent-chain ordering by making the manifest a *set* of pointed-to L1 fragments rather than a linked list) is appealing but a bigger redesign — it would let finalize fully parallelize. Out of scope here; noted as a follow-up.
`─────────────────────────────────────────────────`

### 5.4 Back-pressure

`flush_coordinator.permits` is `Arc<Semaphore>` initialized with `config.max_pending_flushes` permits. Default: **2**.

- `flush_l0_rotate` acquires a permit via `acquire_owned().await`. If saturated, the rotate (and thus the *commit* that triggered the flush via `check_flush`) blocks here.
- This blocking is the **intended** back-pressure: it pushes ingestion latency back to the producer when L1 cannot keep up, rather than letting WAL or memory grow unboundedly.
- Permit is released by `finalize_one` after finalize completes (success or failure).

**Why 2, not 1?** Permit=1 reduces to "serial flushes with the work moved off the lock" — still a big win, but doesn't pipeline rotate-of-flush-N+1 against stream-of-flush-N. Permit=2 gives one in-flight stream + one queued, which is the smallest useful pipeline depth. Permit=4 risks doubling WAL retention (each pending flush prevents WAL truncate past its LSN) and isn't motivated by benchmark data yet. Default 2; tunable via `UniConfig.max_pending_flushes`.

**WAL growth bound**: with N pending flushes, WAL retains all segments back to `min_pending_wal_lsn`, which is bounded by N × (typical flush duration × typical WAL append rate). At N=2 and flushes completing in ~100 ms each, WAL grows by ~one extra flush's worth of mutations vs today — typically tens of MB, well within object-store budgets.

### 5.5 `flush_in_progress`: bool → counter

`StorageManager.flush_in_progress: AtomicBool` becomes `flush_in_progress_count: AtomicUsize`.

- Set: `fetch_add(1, AcqRel)` in `flush_l0_rotate`.
- Reset: `fetch_sub(1, AcqRel)` in `finalize_one` (both success and failure paths).
- Consumer (`storage/compaction.rs:562-569`): `if flush_in_progress_count.load(Acquire) > 0 { skip delta-clear }` — semantically equivalent to today.

Pre-existing bug fix: the current `AtomicBool` leaks on every `?` error path in `flush_to_l1` (writer.rs:2003 → 2558). The new design has only the `fetch_add` at rotate and the `fetch_sub` in `finalize_one`. Errors in **stream** still funnel through the finalizer (which always runs and always decrements). The only remaining leak path is if `tokio::spawn` itself fails (e.g., runtime shutting down) — handled by holding the permit until the spawn returns, and if spawn fails, the rotate path itself does the decrement before propagating the error.

### 5.6 Sync vs async modes

Two public methods on `Writer`:

```rust
pub async fn flush_to_l1(&self, name: Option<String>) -> Result<String>     // sync: awaits finalize
pub async fn flush_to_l1_async(&self, name: Option<String>) -> Result<FlushTicket>  // async: returns after rotate
```

`FlushTicket` has `.await_finalize().await -> Result<String>` if a caller wants to wait. Implemented as a oneshot completion future signaled by the finalizer.

**Caller migration:**

| Caller | Today | After |
|--------|-------|-------|
| `check_flush` (writer.rs:1800, 1809) | `flush_to_l1(None).await?` | `flush_to_l1_async(None).await?` (await rotate only) |
| `Uni::flush` (public API) | `flush_to_l1(None).await?` | `flush_to_l1(None).await?` (unchanged — sync) |
| `create_fork_2pc` nested-fork pre-branch (`fork.rs:199-200`) | `flush_to_l1(None).await?` | `flush_to_l1(None).await?` (sync — needs fresh Lance tip) |
| Tests | mix | tests choose explicitly |

This preserves the "explicit user flush" contract (it really did flush) while making the commit-path-triggered flush non-blocking.

### 5.7 Error handling

| Failure point | Behavior | Reader correctness | WAL safe? |
|---------------|----------|--------------------|-----------|
| WAL pre-flush fails (rotate step A) | Rotate aborts, no L0 swap, permit released, count not incremented. Error propagates to commit. | Yes (no rotation happened) | Yes |
| L0 rotate fails (impossible — purely in-memory) | n/a | | |
| Stream fails (any of phases D–H) | Finalizer logs error, does NOT call complete_flush, does NOT truncate WAL. Old L0 stays in pending_flush. Permit released, count decremented. | Yes — reads still see old_l0 in pending_flush. Eager partial L1 writes are tolerated by version-dedup at read-merge time. | Yes — WAL not truncated, replay covers any partial L1 on next restart. |
| `tokio::spawn` fails after rotate (runtime shutting down) | Rotate path detects spawn failure, does inline finalize-as-failure (drop permit, decrement count). Old L0 retained. | Yes | Yes |
| Finalize step `complete_flush` fails | Cannot fail (in-memory). | | |
| Finalize step `wal.truncate_before` fails | Logged as warning, NOT propagated. WAL retains old segments — replay safe (re-applies idempotent writes). | Yes | Yes (extra WAL retention, no data loss) |
| Process crash anywhere | On restart: WAL replay from `wal_high_water_mark`. Any partial L1 fragments are deduplicated by version/EID at read merge. | Yes | Yes |

Crash semantics are identical to today: WAL is the only durable record before `set_latest_snapshot`; `wal_high_water_mark` advances only on successful finalize.

---

## 6. Correctness Arguments

### 6.1 Snapshot isolation for in-flight reads

**Claim**: a read that starts at any point during an async flush sees a state consistent with some single point in linearizable commit order.

**Argument**: `QueryContext::new_with_pending` (writer.rs:1197-1206) snapshots `(current_l0_arc, pending_flush_l0s, tx_l0_opt)` at construction. Whichever way the read interleaves with rotate/stream/finalize:

- **Read starts before rotate**: ctx.l0 = old L0; ctx.pending_flush_l0s = [previously-pending L0s]; L1 = pre-rotate. Consistent.
- **Read starts after rotate, before complete_flush**: ctx.l0 = new (empty) L0; ctx.pending_flush_l0s = [old L0 + previously-pending]; L1 = may contain partial new fragments. Reader sees old L0 entries via pending_flush_l0s; partial L1 entries are dedup'd by version (the version on the L1 entry equals the version that was in old L0; reader merges by max version per (vid|eid), so seeing it once or twice is equivalent).
- **Read starts after complete_flush**: ctx.l0 = new L0; ctx.pending_flush_l0s = [previously-pending]; L1 = contains all new fragments + manifest pointer published. Old L0 Arc is dropped only when no QueryContext holds it. Consistent.

The invariant that holds across all three: **for every (vid, eid) the reader queries, there exists at least one source in (ctx.l0 ∪ ctx.pending_flush_l0s ∪ L1) that contains the version that was visible at ctx construction**. This is true today and is unaffected by parallelizing stream.

### 6.2 WAL truncation safety

**Claim**: WAL is never truncated past an LSN whose mutations are not durably represented in some combination of (live L0Buffer ∪ pending_flush ∪ published L1).

**Argument**: `truncate_before(safe_lsn)` in finalize computes `safe_lsn = min(L0Manager::min_pending_wal_lsn(), this_flush.wal_lsn)`. `min_pending_wal_lsn` iterates `pending_flush` at call time. Suppose two flushes A (seq=N, wal_lsn=L_A) and B (seq=N+1, wal_lsn=L_B) are both pending, with L_A < L_B. If A finalizes first: pending = [B], so `safe_lsn = min(L_B, L_A) = L_A`. Truncate up to L_A is safe. If B's stream finished but A is still streaming: finalizer waits for A (single ordered finalizer task), so B's truncate is deferred. There is no execution path that truncates past an LSN whose mutations are only durable in an un-published L1 fragment.

### 6.3 Manifest parent-chain consistency

**Claim**: at any time, `catalog/latest` points to a manifest whose `parent_snapshot` is reachable from the previously-pointed manifest's chain.

**Argument**: finalizer applies in rotate-seq order. At rotate, `RotatedFlush.parent_manifest` is captured atomically (read on `Arc<RwLock<Option<SnapshotManifest>>>`). At finalize, `cached_manifest` is updated *before* the next finalize starts (single-task serial). On failure, `cached_manifest` is not updated; the next successful finalize must fix up its `parent_snapshot` (§5.3 last paragraph).

**Counterexample I considered**: flush A and B rotate in order, both capturing parent P. Stream A succeeds, finalize A publishes manifest M_A (parent=P), updates cached_manifest to M_A. Stream B succeeds. Finalize B sees its `outcome.new_manifest.parent_snapshot = P` (captured at B's rotate), but cached_manifest is now M_A. **Bug**: B's manifest claims parent P, but the previous published is M_A. Resolution: finalizer rewrites `outcome.new_manifest.parent_snapshot = cached_manifest.read().as_ref().map(|m| m.snapshot_id.clone())` before calling `save_snapshot`. Cheap (in-memory) and only ever does the rewrite when there's a real predecessor.

Updated step in §5.3:
```rust
// Inside finalize_one's Ok-arm, BEFORE save_snapshot:
let mut manifest = outcome.new_manifest;
if let Some(prev) = shared.cached_manifest.read().as_ref() {
    manifest.parent_snapshot = Some(prev.snapshot_id.clone());
}
shared.storage.snapshot_manager().save_snapshot(&manifest).await?;
shared.storage.snapshot_manager().set_latest_snapshot(&manifest.snapshot_id).await?;
*shared.cached_manifest.write() = Some(manifest.clone());
```

This means stream must NOT itself call `save_snapshot` — that's moved into finalize. (Today it's at writer.rs:2510 inside the unified function; we just relocate it past the parent-fixup.)

### 6.4 Crash recovery

**Claim**: after any crash, recovery produces a database state equivalent to "the most recently *finalized* flush's published manifest + WAL replay from its `wal_high_water_mark`".

**Argument**: `wal_high_water_mark` is written into a manifest only by `save_snapshot` (in finalize). `set_latest_snapshot` is the durable atomic switch. If crash before `set_latest_snapshot`: pointer still points to predecessor → replay covers all mutations since predecessor's HWM (including any committed-but-not-yet-flushed mutations that were in pending_flush). If crash after `set_latest_snapshot` but before `complete_flush` / WAL truncate: pointer points to new manifest, replay covers since new HWM. WAL still has segments below safe_lsn — harmless (just storage cost) until next successful flush truncates them. **There is no window where a `set_latest_snapshot` succeeds but the manifest it points to references Lance fragments that don't exist** — the manifest is written first (writer.rs:2510), then the pointer (writer.rs:2517).

One new case: with N pending flushes, the WAL retains LSNs going back to the oldest pending. If process crashes with all flushes pending, all those LSNs are still in WAL → replay covers them. Safe.

---

## 7. Fork Interactions

### 7.1 Per-fork independence

Each fork has its own `UniInner`, its own `writer: Arc<RwLock<Writer>>` (`mod.rs:74`), its own `Writer` with its own `L0Manager`, `StorageManager`, WAL, and now its own `FlushCoordinator`. Cross-fork flushes are fully independent. No new coordination required.

### 7.2 `drop_fork` and pending flushes

Today, `Uni::drop_fork` checks `inflight_tx_count` (`mod.rs:532`). With async flush, **a fork may have pending flushes after all transactions have committed**. Dropping the fork mid-flush would orphan the finalizer task and leave Lance fragments without a manifest pointer.

**Resolution**: `drop_fork` (and `drop_fork_cascade`) gain a "wait for pending flushes" step before tombstoning. Implementation: query `L0Manager::pending_flush_len()` (new method, trivial), and if non-zero, await `flush_coordinator.drain().await` (new method, signaled when finalize queue is empty and permits are fully returned).

Add a `PendingFlushTimeout` error variant for the timed-wait case (e.g., 10s default), mirroring `ForkInflightTx`'s shape. Configurable via `UniConfig.drop_fork_drain_timeout`.

### 7.3 Nested-fork branching

`create_fork_2pc` calls `writer.flush_to_l1(None).await` (sync) at `fork.rs:199-200` to give the child a fresh Lance tip. With the sync/async split, this stays on the sync method — no change in behavior. The sync method drains the coordinator internally before returning (since `flush_to_l1` is just `rotate + stream + finalize_now`).

### 7.4 `inflight_tx_count` and `pending_flush_count`

These remain orthogonal:

- `inflight_tx_count` counts live `Transaction` instances (incremented on `Transaction::new_with_options`, transaction.rs:215; decremented on `Transaction::drop`, transaction.rs:786). Checked by `drop_fork`.
- `pending_flush_count` (via `L0Manager::pending_flush_len()`) counts L0 buffers awaiting L1 promotion. Checked by `drop_fork` as a separate gate (§7.2).

Both must be zero to drop a fork cleanly.

### 7.5 `fork_flush_count`

Unchanged: `tick_fork_fragment_observability` is called in finalize. The atomic write through `Arc<AtomicU64>` is concurrency-safe by construction. Fork-fragment warning fires from the finalizer, which is fine.

---

## 8. Compaction Interaction

Background compaction (`StorageManager::start_background_compaction`, `manager.rs:537`) takes a `CompactionGuard` over `compaction_status.compaction_in_progress` (manager.rs:88-127). It does not contend on the writer write-lock. It consults `flush_in_progress` (now `_count`) to decide whether to skip the delta-clear (`storage/compaction.rs:562-569`).

**With async flush**: `flush_in_progress_count > 0` is now true for a *longer* wall-clock window (rotate-to-finalize, possibly seconds with N=2 pipelined flushes). Compaction's delta-clear will skip more often. **This is correct** — the signal's semantics are "is any flush about to append to deltas" — and the extra skipping is bounded by the fact that delta-clear is best-effort cleanup, not a correctness operation. Compaction's main work (Lance `optimize_table`, manifest_compactor) is unaffected.

Risk: if pending flushes are continuously held at N=2 by a busy ingestion workload, delta-clear effectively never runs and L1 deltas accumulate. Mitigation: monitor `uni_delta_runs_skipped_total` (already exists in compaction.rs); add an alert at sustained-skip > 1 hour; if observed, lower `max_pending_flushes` or expedite stream throughput.

`AdjacencyManager::compact()` is already spawned in a separate background tokio task today (writer.rs:2576-2591); that code moves into finalize unchanged.

---

## 9. Configuration

New keys on `UniConfig`:

```rust
/// Max pending L0→L1 flushes; back-pressure kicks in beyond this.
/// Default: 2. Increase to widen pipeline at the cost of WAL retention.
pub max_pending_flushes: usize,

/// Timeout for `drop_fork` to wait on pending flushes before failing.
/// Default: 10s.
pub drop_fork_drain_timeout: Duration,

/// If true, `check_flush` from the commit path uses async flush.
/// Default: true. Set false to revert to current synchronous behavior.
pub async_flush_enabled: bool,
```

`async_flush_enabled = false` makes `flush_to_l1_async` internally fall through to `flush_to_l1`, making the refactor a no-op at runtime. Used during phased rollout and as a kill-switch.

---

## 10. Observability

New metrics:

- `uni_flush_rotate_duration_seconds` (histogram) — time held under writer write-lock during step 1.
- `uni_flush_stream_duration_seconds` (histogram) — time in step 2.
- `uni_flush_finalize_duration_seconds` (histogram) — time in step 3 per flush.
- `uni_flush_pending_count` (gauge) — current pending flushes per writer.
- `uni_flush_backpressure_wait_seconds` (histogram) — time spent in `acquire_permit().await`.
- `uni_flush_failures_total` (counter) — stream or finalize failures.
- `uni_flush_parent_chain_fixups_total` (counter) — increments when finalize rewrites `parent_snapshot` due to a predecessor failure.

Existing `uni_flush_duration_seconds` becomes total (rotate + stream + finalize) for back-compat.

Add a new structured log line at each transition with `seq`, `wal_lsn`, `fork_id` for debugging.

---

## 11. Phased Rollout

### Phase 1: Prep (no behavior change) — **mostly already done by concurrent_writer.md**

- ✅ Promote `last_flush_time`, `cached_manifest`, `fork_flush_count`, `fork_fragment_warn_fired` to interior mutability (commit `89f1c263`).
- ✅ Add RAII guard around `flush_in_progress` to fix the pre-existing leak (`FlushInProgressGuard`, commit `89f1c263`).
- ⏳ Convert `flush_in_progress` from `AtomicBool` → `AtomicUsize` to support N concurrent pending flushes (this proposal only). About 30 lines including updating the `FlushInProgressGuard` to use `fetch_add`/`fetch_sub`.
- ⏳ Add new metrics scaffolding (emit only `rotate=full, stream=0, finalize=0` for now).
- ⏳ Add `UniConfig.async_flush_enabled = false` (default off).
- Tests: existing test suite must pass unchanged.

### Phase 2: Internal split (no behavior change)

- Split `flush_to_l1_inner` into private `flush_l0_rotate` + `flush_stream_l1` + `flush_finalize_now`.
- `flush_to_l1` (entry) acquires `flush_lock`, calls them sequentially, releases `flush_lock`.
- Verify byte-for-byte identical Lance output via dataset-diff in tests.

### Phase 3: Spawn behind feature flag

- Add `flush_to_l1_async` and `FlushCoordinator` with finalizer task.
- `check_flush` routes through `flush_to_l1_async` IFF `config.async_flush_enabled = true`.
- Crucially: `commit_transaction_l0`'s post-merge `should_flush` branch must call `flush_to_l1_async_inner` (since `flush_lock` is already held) and NOT block on stream completion. The current `flush_to_l1_inner` call in commit (writer.rs:~445) is replaced with a rotate-then-spawn pattern that drops `flush_lock` before the spawned task picks up the work.
- Add `drop_fork` drain step (gated on same flag).
- Tests: concurrent commits during flush, back-pressure, crash mid-stream, fork drop with pending, nested fork.

### Phase 4: Enable by default

- Flip `async_flush_enabled = true`.
- Run extended soak tests + the original sess=24 benchmark; expect 3–9 s wall reduction.
- Keep kill-switch for one release.

### Phase 5: Cleanup

- Remove the feature flag.
- Remove the synchronous-only branch in `check_flush`.
- Consider follow-up work (NG1: parallelize within-flush; manifest-as-set redesign).

---

## 12. Testing Strategy

### 12.1 Unit / focused integration

- `flush_to_l1` produces identical Lance output before and after refactor (Phase 2 gate).
- `flush_to_l1_async` returns after rotate; finalize completes asynchronously; awaiting the `FlushTicket` reflects success/failure.
- Back-pressure: with `max_pending_flushes=1`, the second flush's rotate blocks until the first finalizes.
- Ordered finalize: with two flushes whose stream completes out-of-order, manifests are published in rotate-order and parent chain is correct.
- Stream failure: simulated Lance backend error → old L0 retained in `pending_flush`, WAL not truncated, permit released, count decremented, next flush succeeds and chains parent past the failure.
- Crash mid-stream: kill process between rotate and finalize → on restart, WAL replay reproduces the L0 state, next flush succeeds.
- Crash post-stream pre-finalize: kill process after `save_snapshot` but before `set_latest_snapshot` → on restart, pointer is unchanged, replay covers from old HWM, no data loss.

### 12.2 Concurrency

- 24 concurrent committers (matching sess=24 workload) for 10k commits — verify no `CommitTimeout`, verify wall-clock reduction matches expectation.
- Reader-during-flush invariant: spawn 10 read workers issuing point lookups against a known key while flush is in flight; assert every read returns the correct value across rotate, stream, and finalize boundaries.

### 12.3 Fork interaction

- `drop_fork` with pending flushes: drains and succeeds.
- `drop_fork` with pending flushes that exceed `drop_fork_drain_timeout`: fails with `PendingFlushTimeout`.
- Nested-fork branching: child fork sees parent's latest L1 (sync flush path unchanged).
- Concurrent commits on two siblings: independent, no contention.

### 12.4 Compaction

- Continuous ingestion with `max_pending_flushes=2`: verify `flush_in_progress_count > 0` for extended periods, but compaction's main work still runs and `uni_delta_runs_skipped_total` does not grow unboundedly (delta-clear runs when count hits 0 between flush bursts).

### 12.5 Soak

- 24-hour soak: heavy ingest + concurrent reads + occasional fork creation/drop + occasional explicit `db.flush()`. Verify no growth in pending_flush, no WAL bloat beyond expected bound, no permit leaks (count returns to 0 between bursts).

---

## 13. Risks & Open Questions

### 13.1 Risks

1. **Latent races in `L0Buffer.merge`**: today, `commit_transaction_l0` merges tx_l0 into "the current L0" under the writer write-lock. With async flush, rotate happens during this same lock, so merges and rotates are still serialized at the writer-lock level. But once we spawn streaming, a *future* commit's merge can race with the previous flush's stream-read of the old L0. The old L0 is no longer the current L0 by then, so they touch different buffers. **Confirmed safe**, but warrants a focused test.

2. **`property_manager.clear_cache().await` in finalize** (writer.rs:2540-2542) — if cache clear contends with concurrent property reads, latency may spike at finalize. Today this happens under the writer lock so it's serialized with everything. Moving to finalize keeps it serial per writer, so this is unchanged in scope; just shifted to a different (less contended) lock-domain.

3. **Object-store back-pressure**: if Lance writes get slow (network blip), pending flushes pile up to `max_pending_flushes`, rotate blocks, commits queue. Same end-user observation as today (slow flush = slow commits), but the *mechanism* is now a semaphore wait rather than a write-lock wait. Easier to reason about, but error messages need to match (`CommitTimeout` already triggers on writer-lock wait; add a corresponding `FlushBackPressureTimeout` for permit wait).

4. **Test flakiness from async timing**: tests that assert post-commit state may now race with finalize. Mitigate by exposing a `db.await_flushes().await` test helper that drains the coordinator.

### 13.2 Open questions

1. **Should `inflight_tx_count` include pending flushes?** Argument for: a fork with pending flushes is "not idle" for drop purposes. Argument against: keeping them separate makes the failure modes more legible (`ForkInflightTx` vs `PendingFlushTimeout`). **Recommendation**: keep separate (§7.4).

2. **Should we expose `FlushTicket` to user-level API?** Useful for "await durability" semantics for write-heavy workloads that want explicit pipelining. Probably yes, but defer to a follow-up — not on the critical path.

3. **Should finalize run on a dedicated single-thread runtime?** The mpsc + heap finalizer task is sensitive to runtime starvation under heavy tokio load. Spawning it on a dedicated `current_thread` runtime would isolate it. **Recommendation**: defer; measure first.

4. **Manifest-as-set redesign** (NG1 + parent-chain removal): would let finalize fully parallelize. Substantial work (on-disk format change, recovery changes). Track as a separate proposal.

5. **What's the right `max_pending_flushes` default?** 2 is a conservative guess. Real choice depends on benchmark data showing how much wall-clock improvement N=4 gives over N=2 at sess=24. **Recommendation**: ship at 2, benchmark, raise after evidence.

---

## 14. Appendix: Code Pointer Index

| Subsystem | Path | Key lines |
|-----------|------|-----------|
| Writer struct | `crates/uni-store/src/runtime/writer.rs` | 45–83 |
| `flush_to_l1` | `crates/uni-store/src/runtime/writer.rs` | 1998–2605 |
| `check_flush` | `crates/uni-store/src/runtime/writer.rs` | 1790–1813 |
| `commit_transaction_l0` | `crates/uni-store/src/runtime/writer.rs` | 237–366 |
| `L0Manager` | `crates/uni-store/src/runtime/l0_manager.rs` | 9–115 |
| `L0Buffer` | `crates/uni-store/src/runtime/l0.rs` | 87–138 |
| `QueryContext` | `crates/uni-store/src/runtime/context.rs` | 11–21 |
| `l0_visibility` | `crates/uni-store/src/runtime/l0_visibility.rs` | full file |
| `WriteAheadLog` | `crates/uni-store/src/runtime/wal.rs` | 71–346 |
| `StorageManager` | `crates/uni-store/src/storage/manager.rs` | 61–86 |
| `flush_in_progress` (AtomicBool) | `crates/uni-store/src/storage/manager.rs` | 71 |
| Compaction's flush check | `crates/uni-store/src/storage/compaction.rs` | 562–569 |
| `AdjacencyManager` | `crates/uni-store/src/storage/adjacency_manager.rs` | 53–76, 303–432 |
| `SnapshotManager` | `crates/uni-store/src/snapshot/manager.rs` | 15–141 |
| `SnapshotManifest` | `crates/uni-common/src/core/snapshot.rs` | 9–20 |
| `UniInner` writer field | `crates/uni/src/api/mod.rs` | 74 |
| `Transaction::begin` writer-read lock | `crates/uni/src/api/transaction.rs` | 173–178 |
| `Transaction::commit` writer-write lock | `crates/uni/src/api/transaction.rs` | 558–581 |
| `inflight_tx_count` increment | `crates/uni/src/api/transaction.rs` | 215 |
| `inflight_tx_count` decrement | `crates/uni/src/api/transaction.rs` | 786 |
| `drop_fork` ForkInflightTx check | `crates/uni/src/api/mod.rs` | 534 |
| Nested-fork pre-flush | `crates/uni/src/api/fork.rs` | 199–200 |
| Per-fork writer factory | `crates/uni-store/src/fork/writer_factory.rs` | 44–73 |
| `ForkInflightTx` error variant | `crates/uni-common/src/api/error.rs` | 165 |
