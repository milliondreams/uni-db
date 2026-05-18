# Async L0→L1 Flush — Complete Specification

**Status:** Spec, ready to implement
**Date:** 2026-05-18
**Crates touched:** `uni-store`, `uni-common`, `uni`
**Supersedes:** `docs/proposals/async_l0_to_l1_flush.md` (original draft 2026-05-17, revised once 2026-05-18; this is the third iteration with full measurement evidence and corrected design)

---

## 0. Executive Summary

Today, `Writer::flush_to_l1_inner` runs as a 600-line monolith inside `flush_lock`. The L1-streaming I/O step (~50–500 ms typical) holds `flush_lock` for its entire duration. When `should_flush()` fires inside `commit_transaction_l0` (default: every 10,000 mutations), every concurrent committer queues behind this stall. At sess=24 ingestion workloads, this serialized flush time is a measurable wall-floor.

The fix: split `flush_to_l1_inner` into **rotate / stream / finalize** with different lock disciplines. rotate (~µs) holds `flush_lock`. stream (~50–500 ms) holds **no lock**. finalize (~µs) re-acquires `flush_lock` briefly. While a stream runs, concurrent commits make progress unblocked.

A naive MVP that just `tokio::spawn`'ed `flush_to_l1` was prototyped (commit `b096260f` reverted it). It measured **3–40× SLOWER** than sync — the spawn keeps the long stream phase under `flush_lock`, so subsequent commits convoy worse than they did synchronously. The data is in `b096260f`'s commit message.

The correct split, plus an **ordered single-task finalizer** to keep the Lance manifest parent-chain consistent under out-of-order stream completion, plus per-fork drain semantics, plus a feature-flag rollout, is what this spec covers.

**Expected wins** (per `crates/uni/examples/flush_pressure.rs` threshold sweep):

| Regime | Sync wall | Async wall (expected) | Gain |
|---|---|---|---|
| Default `auto_flush_threshold=10000`, sess=24 | 1.7 s | ~1.5 s | ~10–15% |
| `auto_flush_threshold=5000`, sess=24 | 10.9 s | ~1.8 s | **~6×** |
| `auto_flush_threshold=2500`, sess=24 | 21.5 s | ~3 s | **~7×** |
| Cloud-storage backend (any threshold) | proportionally larger | unchanged | **3–10×** |

The default-config win is modest; the win at low thresholds and on cloud storage is substantial. Both are unlocked by the same change.

---

## 1. Current state of the world (post-Phase-4 + Phase-1-residual)

Concrete state in the repo as of commit `8a2c4358`:

**`Writer` struct** (`crates/uni-store/src/runtime/writer.rs:81–138`):
- `flush_lock: tokio::sync::Mutex<()>` — Phase 3 introduced, used by `flush_to_l1` entry and `commit_transaction_l0`.
- `last_flush_time: PlMutex<Instant>` — Phase 1 made interior-mutable. **Not yet wrapped in Arc.**
- `cached_manifest: PlMutex<Option<SnapshotManifest>>` — same.
- `fork_flush_count: AtomicU64`, `fork_fragment_warn_fired: AtomicBool` — same.

**`StorageManager.flush_in_progress`** (`crates/uni-store/src/storage/manager.rs:71`): **`AtomicUsize`** (was `AtomicBool` — migrated in commit `8a2c4358`). `FlushInProgressGuard` uses fetch_add/fetch_sub. Already supports N concurrent flushes.

**`flush_to_l1` entry** (`writer.rs:2105–2108`):
```rust
pub async fn flush_to_l1(&self, name: Option<String>) -> Result<String> {
    let _flush_lock_guard = self.flush_lock.lock().await;
    self.flush_to_l1_inner(name).await
}
```

**`flush_to_l1_inner`** (`writer.rs:2113–2724`, ~600 lines): one monolithic function. Currently holds `flush_lock` (via the entry's guard) for the entire body.

**`commit_transaction_l0`** (`writer.rs:306–469`): holds `flush_lock` for its full body (WAL + merge + adjacency + maybe-flush). The post-merge `should_flush()` branch calls `flush_to_l1_inner` synchronously while still holding `flush_lock`.

**`FlushCoordinator` scaffolding** (`crates/uni-store/src/runtime/flush_coordinator.rs`, commit `6843f44c`): types `RotatedFlush`, `FlushOutcome`, `SharedFlushCtx`, `FlushTicket`, `FlushCoordinator`, `FinalizeFn` defined. The finalizer task `finalizer_loop` is implemented but never invoked because `FlushCoordinator` is never constructed.

**`UniConfig` knobs** (commit `8a2c4358`): `async_flush_enabled: bool` (default false), `max_pending_flushes: usize` (default 2), `drop_fork_drain_timeout: Duration` (default 10s). The first is currently a no-op.

**`Transaction::commit`** (`crates/uni/src/api/transaction.rs:~579`): receives `(wal_lsn, _flush_pending)` tuple from `commit_transaction_l0` — this debris from the MVP attempt should be cleaned up.

---

## 2. The problem in detail

### 2.1 Concrete measurement (`crates/uni/examples/flush_pressure.rs`)

24 sessions × 200 tx × 25 vertices = 120,000 mutations per round, default `auto_flush_threshold = 10,000`:

```
threshold   flushes   wall      ns/mutation
---------   -------   ----      -----------
     1000      120    51.0 s    425,009    ← too frequent; flush_lock serialization
     2500       48    21.5 s    178,772
     5000       24    10.9 s     90,664
   *10000       12     1.7 s     14,169    ← default; sweet spot
    25000        5     2.0 s     17,017
    50000        3     4.4 s     36,742
   100000        2    11.2 s     93,313    ← L0 too big; per-insert overhead
        MAX      1    15.1 s    126,141    ← single big flush at end
```

The U-curve has two failure modes. The **left arm** (small threshold) is what async-flush fixes: many flushes serializing through `flush_lock`. The right arm (huge threshold) is a different problem — L0's per-insert cost grows.

At threshold=5000, 24 flushes serialize. If each flush takes ~50 ms holding `flush_lock`, that's 24 × 50 = 1.2 s of pure lock-holding-during-flush. Add the actual 5,000 × 24 = 120k mutations' commit work and we see 10.9 s wall.

If async-flush released `flush_lock` during the stream phase, the 24 flushes would run in parallel (bounded by `max_pending_flushes`). The wall floor would drop to whatever the streams alone take (~50 ms × 24 / 2 parallel = ~600 ms), plus the commit work that no longer queues behind them.

### 2.2 Why the MVP made it worse

The MVP shape was: `commit_transaction_l0` returns `flush_pending: bool`; if true, transaction.rs `tokio::spawn`s `writer.flush_to_l1(None)`. Bench measured **3–40× slower** at threshold=5000.

Cause:

```
Time →
SYNC:     [commit A holding flush_lock for commit+flush]      ─────────[same for B]──── ...
ASYNC:    [commit A: just commit-work] [release]
                    [spawn A: acquires flush_lock for stream] ────────────────────
                                       ↑
                                       │
          [commit B: blocks on flush_lock waiting for spawn A's stream] ──────────
                                       │
                                       └─ ConCurrent commits + still-running flushes
                                          compete for flush_lock + L0Manager's locks
                                          → convoy worse than sync
```

The MVP fails because the spawned flush still acquires `flush_lock` for its entire duration. Spawning is **strictly worse than running inline** if the long phase keeps the same lock.

The fix isn't a different spawn pattern — it's making the long phase **lock-free**.

---

## 3. Design

### 3.1 Three phases with explicit lock discipline

```
┌─────────────────────────────────────────────────────────────────────┐
│ flush_to_l1  ─ sync path (external db.flush(), create_fork_2pc, etc) │
│   ┌─────────────────┐                                                │
│   │ acquire flush_  │                                                │
│   │ lock            │                                                │
│   └────────┬────────┘                                                │
│            ▼                                                         │
│   ┌─────────────────┐  rotate (~µs, flush_lock HELD)                 │
│   │ flush_l0_rotate │   - WAL pre-flush                              │
│   │                 │   - L0Manager::begin_flush (atomic swap)       │
│   │                 │   - record wal_lsn_at_flush on old L0          │
│   │                 │   - hand WAL to new L0                         │
│   │                 │   - capture parent_manifest snapshot           │
│   │                 │   - acquire back-pressure permit               │
│   └────────┬────────┘                                                │
│            │                                                         │
│   ┌─────────────────┐                                                │
│   │ release flush_  │  ← critical: release BEFORE stream             │
│   │ lock            │                                                │
│   └────────┬────────┘                                                │
│            ▼                                                         │
│   ┌─────────────────┐  stream (~50-500 ms, NO LOCK)                  │
│   │ flush_stream_l1 │   - read old L0 (Arc'd; readers can also       │
│   │                 │     read it via QueryContext.pending_flush_l0s)│
│   │                 │   - collect edges, tombstones, vertices        │
│   │                 │   - find_vertex_labels_in_storage for orphans  │
│   │                 │   - build manifest struct (parent tentative)   │
│   │                 │   - Lance writes (eager, append-only)          │
│   │                 │   - (NO save_snapshot here — moved to finalize)│
│   └────────┬────────┘                                                │
│            ▼                                                         │
│   ┌─────────────────┐  finalize (~µs, flush_lock RE-ACQUIRED)        │
│   │ flush_finalize  │   - parent_snapshot fixup vs current cached    │
│   │ _now            │   - save_snapshot(&manifest)                   │
│   │                 │   - set_latest_snapshot(&snapshot_id)          │
│   │                 │   - *cached_manifest.lock() = Some(...)        │
│   │                 │   - l0_manager.complete_flush(&old_l0_arc)     │
│   │                 │   - wal.truncate_before(safe_lsn)              │
│   │                 │   - property_manager.clear_cache               │
│   │                 │   - *last_flush_time.lock() = now              │
│   │                 │   - metrics, l1_runs++, fork observability     │
│   │                 │   - spawn adjacency_manager.compact (existing) │
│   │                 │   - schedule_index_rebuilds_if_needed          │
│   │                 │   - permit drops on exit                       │
│   └────────┬────────┘                                                │
│            │                                                         │
│            ▼  return Ok(snapshot_id)                                 │
└─────────────────────────────────────────────────────────────────────┘
```

The async path is the same phases, but rotate happens in the caller's tokio task and stream+finalize happen on a spawned task; the finalizer enforces rotate-order via the coordinator.

### 3.2 Async path call graph

```
commit_transaction_l0 (already holds flush_lock)
  │
  ├─ WAL append, flush_wal, main-L0 merge, adjacency replay, update_metrics
  │
  ├─ should_flush() && config.async_flush_enabled
  │   │
  │   ├─ permit = flush_coordinator.acquire_permit().await  // blocks if N in flight
  │   ├─ rotated = self.flush_l0_rotate(None, permit).await
  │   │   // rotated is a Send + 'static handle
  │   │
  │   ├─ drop(flush_lock guard)  // CRITICAL: release before submit_for_stream
  │   │
  │   └─ self.flush_coordinator.submit_for_stream(rotated, self.shared_ctx())
  │        // returns immediately; spawns a tokio task internally
  │
  └─ return Ok(wal_lsn)


// Inside the spawned task:
async move {
    let result = Writer::flush_stream_l1(stream_input, shared).await;
    submit_tx.send(FlushSubmit { seq, rotated, result, ack: None }).await;
}


// Finalizer task (one per Writer):
while let Some(submit) = submit_rx.recv().await {
    heap.push(submit);
    while heap.peek().seq == next_expected {
        let s = heap.pop();
        match s.result {
            Ok(outcome) => finalize_fn.finalize(s.rotated, outcome, shared.clone()).await,
            Err(e)      => finalize_fn.finalize_failure(s.rotated, e, shared.clone()).await,
        }
        next_expected += 1;
        pending_count.fetch_sub(1);
        drain_notify.notify_waiters();
    }
}
```

### 3.3 New types (mostly already scaffolded in `flush_coordinator.rs`)

```rust
pub struct RotatedFlush {
    pub seq: u64,
    pub old_l0_arc: Arc<parking_lot::RwLock<L0Buffer>>,
    pub wal_lsn: u64,
    pub name: Option<String>,
    pub parent_manifest: Option<SnapshotManifest>,
    pub permit: tokio::sync::OwnedSemaphorePermit,
    /// Acquired during rotate; dropped after finalize completes (or on
    /// finalize_failure). Keeps `StorageManager.flush_in_progress` counter
    /// accurate for the full duration of the async stream.
    pub flush_in_progress_guard: FlushInProgressGuard,
}

pub struct StreamInput {
    pub old_l0_arc: Arc<parking_lot::RwLock<L0Buffer>>,
    pub wal_lsn: u64,
    pub name: Option<String>,
    pub parent_manifest: Option<SnapshotManifest>,
}

impl From<&RotatedFlush> for StreamInput { /* clone the Arc + Copy fields + clone manifest */ }

pub struct FlushOutcome {
    pub new_manifest: SnapshotManifest,  // parent_snapshot is TENTATIVE
    pub snapshot_id: String,
}

pub struct SharedFlushCtx {
    pub storage: Arc<StorageManager>,
    pub l0_manager: Arc<L0Manager>,
    pub adjacency_manager: Arc<AdjacencyManager>,
    pub property_manager: Option<Arc<PropertyManager>>,
    pub schema_manager: Arc<SchemaManager>,
    pub cached_manifest: Arc<PlMutex<Option<SnapshotManifest>>>,
    pub last_flush_time: Arc<PlMutex<Instant>>,
    pub fork_id: Option<ForkId>,
    pub fork_flush_count: Arc<AtomicU64>,
    pub fork_fragment_warn_fired: Arc<AtomicBool>,
    pub fork_fragment_warn_threshold: usize,
    pub flush_lock: Arc<tokio::sync::Mutex<()>>,
    pub index_rebuild_manager: Option<Arc<IndexRebuildManager>>,
    pub compaction_handle: Arc<parking_lot::RwLock<Option<JoinHandle<()>>>>,
    pub config_for_compaction: CompactionConfig,
    pub auto_rebuild_enabled: bool,
}

pub struct FlushCoordinator {
    permits: Arc<Semaphore>,
    next_seq: AtomicU64,
    submit_tx: mpsc::UnboundedSender<FlushSubmit>,
    pending_count: Arc<AtomicUsize>,
    drain_notify: Arc<Notify>,
    finalizer_handle: Option<JoinHandle<()>>,  // tracked for ShutdownHandle integration
}

pub trait FinalizeFn: Send + Sync {
    fn finalize<'a>(
        &'a self,
        rotated: RotatedFlush,
        outcome: FlushOutcome,
        shared: SharedFlushCtx,
    ) -> BoxFuture<'a, Result<String>>;

    fn finalize_failure<'a>(
        &'a self,
        rotated: RotatedFlush,
        err: anyhow::Error,
        shared: SharedFlushCtx,
    ) -> BoxFuture<'a, anyhow::Error>;
}

pub struct FlushTicket {
    rx: oneshot::Receiver<Result<String>>,
}
impl FlushTicket {
    pub async fn await_finalize(self) -> Result<String> { ... }
}
```

`Writer` implements `FinalizeFn` via a small struct that captures the bits the finalize needs (or directly via a `WriterFinalizer` struct stored alongside the coordinator).

### 3.4 Writer field promotion

Four fields gain `Arc` so they fit in `SharedFlushCtx`:

```rust
// Before:
last_flush_time: PlMutex<std::time::Instant>,
cached_manifest: PlMutex<Option<SnapshotManifest>>,
fork_flush_count: AtomicU64,
fork_fragment_warn_fired: AtomicBool,

// After:
last_flush_time: Arc<PlMutex<std::time::Instant>>,
cached_manifest: Arc<PlMutex<Option<SnapshotManifest>>>,
fork_flush_count: Arc<AtomicU64>,
fork_fragment_warn_fired: Arc<AtomicBool>,
```

Mechanical. `self.last_flush_time.lock()` still works (Arc derefs through). The test at `writer.rs:3408+` that asserts these fields' types needs a small update.

### 3.5 Lock discipline correctness

**Invariants:**

1. `flush_lock` serializes the **publish boundary**: rotate (begin_flush is the atomic swap; readers from this point see the new L0 as "current" and the old L0 as "pending_flush") and finalize (complete_flush + cached_manifest update is the visibility flip).
2. Stream runs lock-free: it reads the old L0 (which is now in `pending_flush`, so readers see it correctly), and writes append-only Lance datasets (visible only after manifest publish in finalize).
3. Multiple streams can run in parallel — bounded by `permits` semaphore.
4. Finalizers run serially in rotate-order. The single-task finalizer is the only path that writes `cached_manifest`, calls `complete_flush`, calls `save_snapshot` + `set_latest_snapshot`, and truncates WAL.

**Why finalize must be in-order:** Lance manifest is a linked list (`parent_snapshot` references the prior `snapshot_id`). The chain must be linear and monotonic; in-order finalize keeps it so.

**Why a single task (not a Mutex):** simpler. A Mutex would require every finalizer to spin/wait. The single task's mpsc + heap is a natural sequencer. Performance is identical (single point of serialization either way) and the code is easier to reason about.

### 3.6 Parent-snapshot fixup (proposal §6.3, repeated here for completeness)

At rotate time, `rotated.parent_manifest = self.cached_manifest.lock().clone()` (a snapshot of the value AT rotate). This becomes the *tentative* parent.

Stream uses this tentative parent in `new_manifest.parent_snapshot = parent_manifest.map(|m| m.snapshot_id)` while building the manifest, BUT does NOT publish it.

At finalize: re-read `cached_manifest.lock()`. If it differs from `rotated.parent_manifest` (a predecessor finalized in between, even though by definition that's impossible with in-order finalize — but the code handles it defensively for the failure case below), rewrite `outcome.new_manifest.parent_snapshot = cached_manifest.read().as_ref().map(|m| m.snapshot_id.clone())`. THEN call `save_snapshot` + `set_latest_snapshot` + update `cached_manifest`.

**With in-order finalize, the fixup is only non-trivial in the failure case**: if a predecessor's stream failed, finalize_failure was called for it (which does NOT update `cached_manifest`), so the next successful finalize sees the same `cached_manifest` value its tentative parent referenced. The fixup is a no-op. But the fixup logic is still needed because failed-predecessor doesn't update the chain.

Concrete scenario: A's stream fails, A's finalize_failure runs (logs, no cached_manifest update), B's finalize runs and sees `cached_manifest = P` (predecessor of A), B's tentative parent was also P → no rewrite needed. Chain: ...P → M_B. A is silently skipped. Correct.

Different scenario (impossible with in-order finalize but the fixup defends): if somehow B finalized before A, B updates cached_manifest = M_B. Then A finalizes, sees cached_manifest = M_B but A's tentative was P. Fixup rewrites A's parent to M_B. Chain: ...P → M_B → M_A. Logically valid (M_A's data is the snapshot taken before A's rotate; M_B's data is after B's rotate, which was after A's rotate, so M_B is "newer" — putting M_A "after" M_B in the chain means time-travel to M_A would see M_B's writes too, which represents a moment-in-time AFTER B's rotate. This is the bug §6.3 calls out). **In-order finalize prevents this.** The fixup is belt-and-braces.

### 3.7 WAL truncation safety

`wal.truncate_before(safe_lsn)` where `safe_lsn = min(L0Manager::min_pending_wal_lsn(), this_flush.wal_lsn)`.

`min_pending_wal_lsn` iterates `pending_flush: Vec<Arc<RwLock<L0Buffer>>>` at call time. Each L0 in pending_flush has its `wal_lsn_at_flush` set during rotate (Phase C). Min over them is the safe LSN.

With N pending flushes, truncate cannot go past the oldest pending. Crash recovery via WAL replay from the latest manifest's `wal_high_water_mark` covers everything not yet finalized.

### 3.8 Crash recovery

At any point during async flush, the state on disk is:

- `catalog/latest`: points to the most-recently-finalized snapshot's id (updated by finalize, atomically last). Possibly stale if a finalize crashed mid-publish.
- `catalog/manifests/{id}.json`: bodies for every finalized snapshot. Possibly contains an extra body for a partial finalize (save_snapshot succeeded, set_latest_snapshot didn't).
- Lance datasets: append-only fragments for every stream that started. Some may belong to never-finalized flushes (their stream succeeded but finalize didn't, or stream failed).
- WAL: every mutation since the last finalized flush's `wal_high_water_mark`.

**Recovery logic** (`Uni::build()`, mod.rs:1757–1819):

1. Read `catalog/latest` → snapshot_id. Load `catalog/manifests/{snapshot_id}.json`.
   - If pointer missing: list manifests, pick most recent by ctime. Verify it loads.
   - If pointer→manifest is corrupted: panic with actionable error.
2. Initialize L0 to empty. Set `current_version = manifest.version_high_water_mark + 1`.
3. `writer.replay_wal(manifest.wal_high_water_mark)` — replays every WAL segment with LSN > HWM. These mutations become the L0 contents.
4. Lance datasets are opened at the versions specified in the manifest. Fragments at later versions (from partial-flush streams whose finalize never published) are simply **unreferenced** — the manifest is the source of truth and never points at them. They become harmless dead bytes until a future cleanup pass reclaims them.

**Invariant preserved:** the database state after recovery equals "manifest's recorded snapshot + WAL since its HWM". Any partial Lance data not referenced by any manifest is harmless dead bytes (cleanup is future work).

### 3.9 Fork interactions

Each fork has its own `Writer` (via `crates/uni-store/src/fork/writer_factory.rs:44–73`). Each Writer has its own `FlushCoordinator`. Forks are fully independent.

**`drop_fork`** (`mod.rs:531`) currently validates `inflight_tx_count == 0` before tombstoning. Add a parallel check on `pending_flush_count`:

```rust
if let Some(writer) = &uni_inner.writer {
    writer.flush_coordinator.drain(self.config.drop_fork_drain_timeout)
        .await
        .map_err(|_| UniError::PendingFlushTimeout { name: name.into() })?;
}
```

`drain` waits via `Notify` until `pending_count == 0` or timeout. New error variant `PendingFlushTimeout { name: String }` mirrors `ForkInflightTx`.

**`create_fork_2pc` pre-flush** (`crates/uni/src/api/fork.rs:181–201`, pre-flush at lines 196–200, gated by `parent.is_forked() && parent.db.writer.is_some()`) stays synchronous (calls `writer.flush_to_l1(None).await`). The sync path's rotate+stream+finalize composition makes this atomic — child fork sees a fully-published parent. No change needed.

**Fork creation under async back-pressure.** When `create_fork_2pc` invokes the sync `flush_to_l1` and the coordinator's permit pool is fully saturated by background streams, the fork's rotate phase blocks on `acquire_permit().await`. This is correct back-pressure (the fork can't proceed until the parent is consistent), but it introduces a new fork-creation latency mode bounded by `stream_duration × max_pending_flushes`. We accept the wait rather than adding a new timeout knob — sync fork creation is rare and seconds of wait are tolerable; the existing `drop_fork_drain_timeout` covers the symmetrical teardown path.

**Cross-fork Arc graph:** a fork's spawned stream task holds Arc'd pieces (storage, l0_manager, etc.) but NOT `Arc<Writer>`. When `drop_fork` clears the fork's `UniInner` from the cache, the Writer drops only when all sessions release their Arcs. The pending streams keep the fork-scoped storage alive but not the Writer — clean shutdown ordering.

### 3.10 Shutdown integration

`Uni::shutdown_blocking` (`mod.rs:1425+`) broadcasts shutdown via `ShutdownHandle`. Existing background tasks (auto-flush ticker, compaction worker, index rebuild worker, fork sweeper) follow the pattern:

```rust
tokio::select! {
    _ = work => { ... }
    _ = shutdown_rx.recv() => { /* drain or exit */ break; }
}
shutdown_handle.track_task(handle);
```

The async-flush finalizer task follows this pattern. At shutdown:
- Stop accepting new submissions (drop `submit_tx`).
- Drain any in-heap submissions and run their finalize.
- Exit.

`Uni::drop` calls `shutdown_blocking()` with a default 30s timeout. Tracked tasks must exit within that. In-flight streams whose finalize doesn't get processed: their old L0s stay in `pending_flush` (no `complete_flush`), WAL retains their data; next start recovers via WAL replay. Safe.

**In-flight stream results dropped at shutdown.** When shutdown drops `submit_tx`, any spawned stream task that completes after that point cannot deliver its result (the `submit` call returns `SendError`). The stream's Lance output sits at higher versions than the latest published manifest — unreferenced dead bytes, harmless. The mutations themselves are still in the WAL (because `wal.truncate_before` only runs in finalize, which never happened). Next start replays them via the standard WAL recovery path. Document this explicitly so operators don't worry about partial Lance fragments on graceful shutdown.

---

## 4. Configuration

(Already added in commit `8a2c4358`.)

```rust
pub async_flush_enabled: bool,       // default false; flip true after rollout
pub max_pending_flushes: usize,      // default 2
pub drop_fork_drain_timeout: Duration, // default 10s
```

`max_pending_flushes`: trade-off between pipeline depth and WAL retention. At N=2, WAL holds ~2× the per-flush mutation window. At N=4, 4×, etc. Default 2 keeps WAL bounded to ~2 × `auto_flush_threshold` mutations of segments.

`async_flush_enabled` is the kill switch. Default false means production users see no behavior change until they opt in.

---

## 5. Observability

New metrics (recorded via `metrics::histogram!` / `metrics::counter!`):

| Metric | Type | Records |
|---|---|---|
| `uni_flush_rotate_duration_seconds` | histogram | time in `flush_l0_rotate` (held under flush_lock) |
| `uni_flush_stream_duration_seconds` | histogram | time in `flush_stream_l1` (no lock held) |
| `uni_flush_finalize_duration_seconds` | histogram | time in `flush_finalize_now` (re-acquires flush_lock) |
| `uni_flush_pending_count` | gauge | current pending-flush count for this writer |
| `uni_flush_backpressure_wait_seconds` | histogram | time spent in `acquire_permit().await` |
| `uni_flush_failures_total` | counter | stream or finalize failures |
| `uni_flush_parent_chain_fixups_total` | counter | increments when finalize rewrites `parent_snapshot` (always 0 with in-order finalize succeeding; non-zero only on predecessor failure) |

Existing `uni_flush_duration_seconds` continues to record total (rotate + stream + finalize) for back-compat. Existing `uni_l0_buffer_rotations_total` counter unchanged.

Tracing: each phase gets `#[instrument]` so spans cleanly separate.

---

## 6. Implementation walkthrough (file-by-file)

### 6.1 `crates/uni-store/src/runtime/writer.rs`

**(1) Field promotion** (`Writer` struct definition, ~lines 81–138):

```rust
// 4 lines change:
last_flush_time: Arc<PlMutex<std::time::Instant>>,
cached_manifest: Arc<PlMutex<Option<SnapshotManifest>>>,
fork_flush_count: Arc<AtomicU64>,
fork_fragment_warn_fired: Arc<AtomicBool>,

// Also: add the FlushCoordinator field
flush_coordinator: Arc<FlushCoordinator>,
```

Constructors in `new_with_config` (~line 165) update accordingly:
```rust
last_flush_time: Arc::new(PlMutex::new(std::time::Instant::now())),
cached_manifest: Arc::new(PlMutex::new(None)),
fork_flush_count: Arc::new(AtomicU64::new(0)),
fork_fragment_warn_fired: Arc::new(AtomicBool::new(false)),
flush_coordinator: { /* see (8) below */ },
```

The hot-path-field test at writer.rs:3408+ updates its snapshot struct to match.

**(2) Extract `flush_l0_rotate`** — phases A, B, C from current `flush_to_l1_inner` (lines 2141–2180):

```rust
async fn flush_l0_rotate(
    &self,
    name: Option<String>,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Result<RotatedFlush> {
    let seq = self.flush_coordinator.next_rotate_seq();

    // Acquire flush-in-progress guard NOW (before any heavy work).
    // It stays alive until finalize drops the RotatedFlush, so the
    // counter accurately reflects "flush has started, has not completed"
    // for the full duration of the async stream.
    let flush_in_progress_guard = FlushInProgressGuard::new(&self.storage);

    // A: WAL pre-flush
    let wal_for_truncate = self.l0_manager.get_current().read().wal.clone();
    let wal_lsn = if let Some(ref w) = wal_for_truncate {
        w.flush().await?
    } else {
        0
    };

    // B: L0 rotate
    let old_l0_arc = self.l0_manager.begin_flush(0, None);
    metrics::counter!("uni_l0_buffer_rotations_total").increment(1);

    // C: WAL handoff
    let _current_version = {
        let mut old = old_l0_arc.write();
        old.wal_lsn_at_flush = wal_lsn;
        let wal = old.wal.take();
        let current_version = old.current_version;
        let new = self.l0_manager.get_current();
        let mut new_g = new.write();
        new_g.wal = wal;
        new_g.current_version = current_version;
        current_version
    };

    // Capture parent_manifest for stream's tentative parent
    let parent_manifest = self.cached_manifest.lock().clone();

    self.flush_coordinator.note_pending();

    Ok(RotatedFlush {
        seq,
        old_l0_arc,
        wal_lsn,
        name,
        parent_manifest,
        permit,
        flush_in_progress_guard,
    })
}
```

**(3) Extract `flush_stream_l1`** — phases D, E, F, G from current code (lines 2182–2641), MINUS `save_snapshot` and `set_latest_snapshot`:

```rust
async fn flush_stream_l1(
    input: StreamInput,
    shared: SharedFlushCtx,
) -> Result<FlushOutcome> {
    // D: L1 collect
    // ... (move lines 2182–2311 with substitutions: self.* → shared.*)
    
    // E: Orphan resolve
    // ... (move lines 2313–2336)
    
    // F: Manifest seed (use input.parent_manifest as tentative parent)
    let snapshot_id = uuid::Uuid::new_v4().to_string();
    let mut manifest = SnapshotManifest {
        snapshot_id: snapshot_id.clone(),
        parent_snapshot: input.parent_manifest.as_ref().map(|m| m.snapshot_id.clone()),
        wal_high_water_mark: input.wal_lsn,
        version_high_water_mark: /* computed */,
        name: input.name.clone(),
        // ... other fields from input/state
    };
    
    // G: Lance writes
    // ... (move lines 2364–2631 with substitutions)
    // Updates manifest with versions and counts.
    
    // NOTE: save_snapshot and set_latest_snapshot are NOT called here.
    
    Ok(FlushOutcome { new_manifest: manifest, snapshot_id })
}
```

This function takes no `&self`. Pure `Send + 'static` over its inputs.

**(4) Extract `flush_finalize_now`** — phases H through S, with parent_snapshot fixup added:

```rust
async fn flush_finalize_now(
    rotated: RotatedFlush,
    mut outcome: FlushOutcome,
    shared: SharedFlushCtx,
) -> Result<String> {
    let _flush_lock_guard = shared.flush_lock.lock().await;
    
    // Parent-snapshot fixup: re-read cached_manifest and rewrite if it diverged
    // from rotated.parent_manifest (only possible with predecessor failure).
    let current_parent_id = shared.cached_manifest.lock()
        .as_ref()
        .map(|m| m.snapshot_id.clone());
    let tentative_parent_id = rotated.parent_manifest.as_ref().map(|m| m.snapshot_id.clone());
    if current_parent_id != tentative_parent_id {
        outcome.new_manifest.parent_snapshot = current_parent_id;
        metrics::counter!("uni_flush_parent_chain_fixups_total").increment(1);
    }
    
    // H: Manifest publish
    shared.storage.snapshot_manager().save_snapshot(&outcome.new_manifest).await?;
    shared.storage.snapshot_manager().set_latest_snapshot(&outcome.snapshot_id).await?;
    
    // I: cached_manifest update — visibility flip
    *shared.cached_manifest.lock() = Some(outcome.new_manifest.clone());
    
    // J: complete_flush
    shared.l0_manager.complete_flush(&rotated.old_l0_arc);
    
    // K: WAL truncate
    if let Some(wal) = shared.l0_manager.get_current().read().wal.clone() {
        let safe_lsn = shared.l0_manager
            .min_pending_wal_lsn()
            .map(|m| m.min(rotated.wal_lsn))
            .unwrap_or(rotated.wal_lsn);
        if let Err(e) = wal.truncate_before(safe_lsn).await {
            tracing::warn!(error = %e, "WAL truncate failed (non-critical)");
        }
    }
    
    // L: property cache clear
    if let Some(pm) = &shared.property_manager {
        pm.clear_cache().await;
    }
    
    // M: last_flush_time
    *shared.last_flush_time.lock() = std::time::Instant::now();
    
    // N: metrics — record histograms for rotate / stream / finalize timing
    
    // (O: FlushInProgressGuard lives on `rotated.flush_in_progress_guard`
    //     and drops when `rotated` is dropped at the end of this function
    //     — i.e., exactly when the flush has fully completed.)
    
    // P: l1_runs increment
    {
        let mut status = uni_common::sync::acquire_mutex(
            &shared.storage.compaction_status,
            "compaction_status",
        )?;
        status.l1_runs += 1;
    }
    
    // Q: adjacency compact spawn (existing pattern)
    let am = shared.adjacency_manager.clone();
    if am.should_compact(shared.config_for_compaction.frozen_segments_compact_threshold) {
        let previous_still_running = {
            let guard = shared.compaction_handle.read();
            guard.as_ref().is_some_and(|h| !h.is_finished())
        };
        if !previous_still_running {
            let handle = tokio::spawn(async move { am.compact(); });
            *shared.compaction_handle.write() = Some(handle);
        }
    }
    
    // R: index rebuild scheduling
    if shared.auto_rebuild_enabled
        && let Some(rebuild_mgr) = &shared.index_rebuild_manager
    {
        Writer::schedule_index_rebuilds_if_needed_static(
            &outcome.new_manifest,
            rebuild_mgr.clone(),
        );
    }
    
    // S: fork observability tick
    tick_fork_fragment_observability_static(
        shared.fork_id,
        shared.fork_flush_count.clone(),
        shared.fork_fragment_warn_fired.clone(),
        shared.fork_fragment_warn_threshold,
    );
    
    drop(rotated.permit);  // explicit, although Drop would handle it
    
    Ok(outcome.snapshot_id)
}
```

**(5) Rewrite `flush_to_l1` (sync path)**:

```rust
pub async fn flush_to_l1(&self, name: Option<String>) -> Result<String> {
    let permit = self.flush_coordinator.acquire_permit().await?;
    let rotated = {
        let _flush_lock_guard = self.flush_lock.lock().await;
        self.flush_l0_rotate(name, permit).await?
    };  // flush_lock released here
    
    let stream_input = StreamInput::from(&rotated);
    let outcome = Self::flush_stream_l1(stream_input, self.shared_ctx()).await?;
    
    Self::flush_finalize_now(rotated, outcome, self.shared_ctx()).await
}
```

Behavior-equivalent to today's `flush_to_l1` for external callers.

**(6) Add `flush_to_l1_async`**:

```rust
pub async fn flush_to_l1_async(&self, name: Option<String>) -> Result<FlushTicket> {
    let permit = self.flush_coordinator.acquire_permit().await?;
    let rotated = {
        let _flush_lock_guard = self.flush_lock.lock().await;
        self.flush_l0_rotate(name, permit).await?
    };
    Ok(self.flush_coordinator.submit_for_stream(rotated, self.shared_ctx()))
}
```

**(7) Wire `commit_transaction_l0` dispatch** (around line 461). **CRITICAL: do NOT await `acquire_permit()` while holding `flush_lock`.** If the permit pool is exhausted, every concurrent committer would queue behind permit + flush_lock — the exact convoy §2.2 calls out. The fix is to rotate first under flush_lock (µs work), drop the lock, then acquire the permit:

```rust
// Gate the trigger on coordinator headroom. If we are already at the
// pipeline cap, defer this flush trigger to the NEXT should_flush()
// check rather than blocking on the permit while holding flush_lock.
if self.should_flush()
    && self.flush_coordinator.pending_flush_count()
        < self.config.max_pending_flushes
{
    if self.config.async_flush_enabled {
        // 1. Try-acquire a permit. It SHOULD succeed because we just
        //    gated on pending_count < max, but a racing fork/external
        //    flush may have grabbed the last permit. In that race we
        //    skip this trigger — the next committer will retry.
        let Some(permit) = self.flush_coordinator.try_acquire_permit() else {
            metrics::counter!("uni_flush_trigger_skipped_total").increment(1);
            return Ok(wal_lsn);
        };

        // 2. Rotate under flush_lock (µs work).
        let rotated = self.flush_l0_rotate(None, permit).await?;

        // 3. Release flush_lock BEFORE handing the rotated flush to the
        //    coordinator. From this point, concurrent committers see the
        //    rotated state (new L0 current, old L0 in pending_flush) and
        //    can proceed without serializing on us.
        drop(_flush_lock_guard);

        // 4. Submit; fire-and-forget. Coordinator spawns the stream task
        //    and handles all subsequent errors.
        let _ticket = self.flush_coordinator
            .submit_for_stream(rotated, self.shared_ctx());
    } else {
        // SYNC path under existing flush_lock guard — see (10) below.
        if let Err(e) = self.flush_inline_under_lock(None).await {
            tracing::warn!("Post-commit flush check failed (non-critical): {}", e);
        }
    }
}

Ok(wal_lsn)  // back to single-value return
```

Two invariants are preserved:

- The pending-count gate caps in-flight flushes at `max_pending_flushes` without ever awaiting a permit under `flush_lock`. The `try_acquire_permit` is non-blocking; if it fails (rare race), we skip and the next commit retries — strictly correct because should_flush remains true.
- `drop(_flush_lock_guard)` happens AFTER rotate completes (so the new-L0/old-L0 swap is visible to readers) and BEFORE `submit_for_stream` (so the coordinator's spawned stream task does not contend with subsequent commits for `flush_lock`).

`_flush_lock_guard` is the guard from commit's line ~323. Binding it as `let _flush_lock_guard` (with the explicit `_` prefix to avoid the unused-variable lint while keeping it droppable) lets us `drop(_flush_lock_guard)` explicitly.

`try_acquire_permit` is a new `FlushCoordinator` method (returns `Option<OwnedSemaphorePermit>`), trivial wrapper around `Arc<Semaphore>::clone().try_acquire_owned()`.

**(8) Construct FlushCoordinator in `Writer::new_with_config`**:

```rust
// After all other fields are wired:
let writer_finalizer: Arc<dyn FinalizeFn> = Arc::new(WriterFinalizer);
let initial_shared = SharedFlushCtx { /* clone the Arcs from the fields above */ };
let flush_coordinator = Arc::new(FlushCoordinator::new(
    config.max_pending_flushes,
    initial_shared,
    writer_finalizer,
));
```

`WriterFinalizer` is a unit struct that impls `FinalizeFn` by delegating to `Writer::flush_finalize_now` (as an associated fn, not `&self`).

**(9) `shared_ctx` helper**:

```rust
fn shared_ctx(&self) -> SharedFlushCtx {
    SharedFlushCtx {
        storage: self.storage.clone(),
        l0_manager: self.l0_manager.clone(),
        adjacency_manager: self.adjacency_manager.clone(),
        property_manager: self.property_manager.clone(),
        schema_manager: self.schema_manager.clone(),
        cached_manifest: self.cached_manifest.clone(),
        last_flush_time: self.last_flush_time.clone(),
        fork_id: self.fork_id,
        fork_flush_count: self.fork_flush_count.clone(),
        fork_fragment_warn_fired: self.fork_fragment_warn_fired.clone(),
        fork_fragment_warn_threshold: self.config.fork_fragment_warn_threshold,
        flush_lock: self.flush_lock_arc.clone(),  // need to Arc-wrap the lock too
        index_rebuild_manager: self.index_rebuild_manager.get().cloned(),
        compaction_handle: self.compaction_handle.clone(),
        config_for_compaction: self.config.compaction.clone(),
        auto_rebuild_enabled: self.config.index_rebuild.auto_rebuild_enabled,
    }
}
```

This requires `flush_lock` itself to become `Arc<tokio::sync::Mutex<()>>` (currently bare). Trivial change.

**(10) Delete `flush_to_l1_inner`; introduce `flush_inline_under_lock` for commit's sync branch.**

`flush_to_l1_inner` is private and has no external callers; once split into rotate+stream+finalize it ceases to exist. External callers go through `flush_to_l1` (which acquires/releases `flush_lock` itself). Commit's sync branch already holds `flush_lock`, so it cannot call `flush_to_l1` (would deadlock on the inner re-acquire) — it needs a helper that runs the three phases under the caller's existing lock:

```rust
/// Runs rotate + stream + finalize INLINE under the caller's existing
/// `flush_lock` guard. Used only by `commit_transaction_l0`'s sync branch
/// where the guard from line ~323 is still alive.
async fn flush_inline_under_lock(
    &self,
    name: Option<String>,
) -> Result<String> {
    // try_acquire because we're on the commit hot path; if saturated,
    // surface the failure rather than block under flush_lock.
    let permit = self
        .flush_coordinator
        .try_acquire_permit()
        .ok_or_else(|| anyhow::anyhow!("flush coordinator saturated"))?;

    let rotated = self.flush_l0_rotate(name, permit).await?;
    let stream_input = StreamInput::from(&rotated);
    let outcome = Self::flush_stream_l1(stream_input, self.shared_ctx()).await?;
    // flush_lock is already held; use the _locked variant to avoid
    // a re-acquire deadlock.
    Self::flush_finalize_locked(rotated, outcome, self.shared_ctx()).await
}
```

`flush_finalize_locked` is identical to `flush_finalize_now` except it skips the leading `let _flush_lock_guard = shared.flush_lock.lock().await;`. Two flavors: `_now` acquires; `_locked` assumes caller holds. The single shared body lives in a private helper `flush_finalize_body` to avoid drift.

Sync external callers (`db.flush()`, `create_fork_2pc`) keep using `flush_to_l1`, which already acquires the lock itself. Only commit's sync branch uses `flush_inline_under_lock`.

### 6.2 `crates/uni-store/src/runtime/flush_coordinator.rs`

Scaffolding already in place. Needed additions:

- `submit_for_stream(&self, rotated, shared) -> FlushTicket` — spawns the stream task, posts to mpsc, returns ticket. Already partially sketched.
- `drain(&self, timeout) -> Result<(), DrainError>` — waits on `drain_notify` until `pending_count == 0`. Already sketched.
- `pending_flush_count(&self) -> usize` — public accessor (the existing `pending_flush_count` method at line 174 already does this).
- `try_acquire_permit(&self) -> Option<OwnedSemaphorePermit>` — non-blocking variant of `acquire_permit`. Wraps `permits.clone().try_acquire_owned().ok()`. Used by commit-path dispatch (§6.1(7)) and `flush_inline_under_lock` (§6.1(10)) to avoid awaiting under `flush_lock`.

Plus: the finalizer task needs to track the JoinHandle for `ShutdownHandle::track_task`. Expose `take_finalizer_handle()` so `Writer::new_with_config` can register it.

Plus: `FlushInProgressGuard` (currently `pub(crate)` in `storage/manager.rs`) needs visibility to `runtime::writer` and `runtime::flush_coordinator`. Either keep `pub(crate)` (works since both modules are in the same crate) or promote to module-public. No new struct — reuse the existing one.

### 6.3 `crates/uni-common/src/api/error.rs`

```rust
#[error("Fork '{name}' has pending flushes that did not drain within timeout")]
PendingFlushTimeout { name: String },
```

### 6.4 `crates/uni/src/api/transaction.rs`

Revert the `(wal_lsn, _flush_pending)` tuple destructure at line ~579 back to `let wal_lsn = ...?;`. Remove the no-op `let _ = flush_pending;` at the end of the block (also revert in writer.rs).

### 6.5 `crates/uni/src/api/mod.rs`

`drop_fork` (around line 531) adds drain:

```rust
// After inflight_tx_count check, before begin_drop:
if let Some(writer) = &uni_inner.writer {
    if let Err(_) = writer.flush_coordinator
        .drain(self.config.drop_fork_drain_timeout)
        .await
    {
        return Err(UniError::PendingFlushTimeout { name: name.into() });
    }
}
```

`Uni::build`/`UniBuilder::build` (around line 1857) registers the finalizer task with `ShutdownHandle`:

```rust
if let Some(handle) = writer.flush_coordinator.take_finalizer_handle() {
    shutdown_handle.track_task(handle);
}
```

### 6.6 No changes needed in

- `crates/uni-store/src/fork/writer_factory.rs` — `new_for_fork` calls `Writer::new_with_config` which now wires the coordinator.
- `crates/uni-store/src/storage/manager.rs` — `flush_in_progress: AtomicUsize` already in place.
- `crates/uni-common/src/config.rs` — knobs already in place.

---

## 7. Test plan

All in `crates/uni-store/tests/` or `crates/uni/tests/` as standalone integration tests.

### 7.1 Phase-2 query-equivalence gate

**File:** `crates/uni-store/tests/async_flush_split_equivalence.rs`

```rust
#[tokio::test]
async fn async_flush_split_yields_identical_reads() -> Result<()> {
    // Fixed workload: pre-create labels, insert 10000 vertices + 5000 edges
    // through a single tx, commit, flush.
    // Capture query results for: MATCH (n) RETURN count(*); MATCH (n)-[]-(m) RETURN ...
    // for vector search, label-filtered scans.
    
    // Path A: sync flush via legacy flush_to_l1_inner (will be deleted after phase 2)
    // Path B: sync flush via new rotate+stream+finalize composition
    
    // Assert both produce identical result sets.
}
```

### 7.2 Async basic

**File:** `crates/uni-store/tests/async_flush_basic.rs`

- Single-session 10 commits, threshold=1000 (triggers ~10 flushes), `async_flush_enabled = true`.
- After all commits, call `db.flush().await` to drain.
- Assert: all 10000 mutations are queryable; manifest chain is linear; `pending_flush_count == 0`.

### 7.2b Concurrent external `db.flush()`

**File:** `crates/uni-store/tests/async_flush_concurrent_external.rs`

- 4 tokio tasks each call `db.flush().await` concurrently with `max_pending_flushes = 2`.
- Concurrently, 8 sessions commit at threshold=1000.
- Assert: all flushes finalize in rotate-order; manifest chain is linear (4+ snapshots, each referencing its predecessor); `pending_flush_count == 0` after `drain`.

### 7.3 Out-of-order stream

**File:** `crates/uni-store/tests/async_flush_order.rs`

- Use a `tokio::sync::Mutex`-based "stream delay barrier" injected via a test-only hook (e.g., feature-gated `cfg(test)` field on Writer that's an `Option<Arc<Notify>>`).
- Two flushes: A rotates first, B rotates second. Block A's stream on the barrier; let B's stream complete.
- Verify finalize order is A then B.
- Verify manifest chain: previous → M_A → M_B.

### 7.4 Stream failure

**File:** `crates/uni-store/tests/async_flush_failure.rs`

- Inject failure via a test-only `Storage` trait method override (or via a small `should_fail: Arc<AtomicBool>` flag inside a mock storage).
- Flush A fails at stream. Verify:
  - Old L0 still in `pending_flush`.
  - WAL not truncated past A's wal_lsn.
  - Subsequent flush B succeeds; B's `parent_snapshot` points past A (i.e., to A's predecessor).
- After WAL replay (simulated by replay_wal call), all of A's mutations are re-applied.

### 7.5 Back-pressure

**File:** `crates/uni-store/tests/async_flush_backpressure.rs`

- `max_pending_flushes = 1`.
- Trigger rotate A (block its stream with a barrier).
- Trigger rotate B — must block on permit acquisition.
- Release A's barrier. A's stream completes, A finalizes. Permit released.
- B's rotate proceeds.

### 7.5b `flush_in_progress` counter under async

**File:** `crates/uni-store/tests/async_flush_in_progress_counter.rs`

- `max_pending_flushes = 4`. Trigger 3 async flushes with stream barriers blocking finalize.
- While streams are blocked, assert `storage.flush_in_progress.load(Acquire) == 3`.
- Release barriers one by one; assert counter decrements to 2, 1, 0 as each finalize completes.
- Inject a stream failure for one flush; assert the counter still decrements (guard drops on finalize_failure path).

### 7.5c Convoy regression guard

**File:** `crates/uni-store/benches/async_flush_convoy.rs` (or integrated into `flush_pressure.rs`).

- sess=24, threshold=5000, `max_pending_flushes = 1` (worst case — forces every flush to fully serialize through one permit).
- Assert wall-time is at worst within 1.2× of the sync baseline at the same threshold. If async is materially slower at max=1, the permit-while-holding-flush-lock convoy has regressed.

### 7.6 Fork drain

**File:** `crates/uni/tests/async_flush_fork_drain.rs`

- Create a fork, commit on the fork triggering an async flush, immediately call `db.drop_fork(name)`.
- Assert `drop_fork` waits and succeeds within timeout.
- Verify all pending flushes finalized before drop completes.

### 7.6b Fork creation under in-flight async stream

**File:** `crates/uni/tests/async_flush_fork_create_under_pressure.rs`

- Trigger N async flushes on parent (block their streams with barriers) until permit pool is saturated.
- Concurrently call `db.create_fork("child", FromLatest)`.
- Assert: fork creation BLOCKS on the parent's flush_to_l1 acquiring a permit, completes once a barrier releases. Child fork sees the fully-published parent state.
- Verify no deadlock and no `PendingFlushTimeout`.

### 7.7 Fork drain timeout

- Same as 7.6 but inject a stream barrier longer than `drop_fork_drain_timeout`.
- Assert `drop_fork` returns `UniError::PendingFlushTimeout`.

### 7.8 Shutdown drain

**File:** `crates/uni/tests/async_flush_shutdown.rs`

- Trigger several async flushes, then `Uni::shutdown_blocking()`.
- Assert: all in-flight flushes finalize before shutdown returns; finalizer task exits.

### 7.9 Crash-recovery — pointer never written

**File:** `crates/uni-store/tests/async_flush_crash_recovery.rs`

- Force a stream to start, save_snapshot succeeds but set_latest_snapshot is skipped (mock).
- Reload database.
- Assert: latest pointer = previous manifest; WAL replay covers the un-finalized mutations.

### 7.9b Crash-recovery — pointer written, WAL not truncated

**File:** `crates/uni-store/tests/async_flush_crash_recovery_post_publish.rs`

- Force finalize to run through manifest publish + `cached_manifest` update + `complete_flush`, then crash (mock) before `wal.truncate_before`.
- Reload database.
- Assert: latest pointer = new manifest; WAL replay re-applies the already-flushed mutations idempotently. No duplicate vertices/edges (idempotent because keys are deterministic from the WAL records).
- This verifies the "WAL truncate is best-effort" property in §3.7.

### 7.10 Existing test suite

Full `cargo nextest run -p uni-store -p uni-query -p uni-db -p uni-common -p uni-locy --no-fail-fast` must pass with `async_flush_enabled = false` (default) AND with `async_flush_enabled = true` (set via test env var or test fixture).

### 7.11 Bench

- Rerun `crates/uni/examples/flush_pressure.rs` with the threshold sweep, comparing `async_flush_enabled = false` (current baseline) vs `true`. Expect the values in §0 to materialize at low threshold and remain similar at default.
- Rerun `crates/uni/benches/concurrent_mutations.rs` with both flag values. Headline: P99 commit latency should drop; overall wall similar at default config.

---

## 8. Commit boundary plan

Each commit is independently bisectable and reviewable.

| # | Commit | Files | Verify with |
|---|---|---|---|
| 1 | `refactor(writer): promote interior-mut fields to Arc<...>` | writer.rs (struct + constructor + test) | `cargo nextest run -p uni-store` |
| 2 | `refactor(writer): extract flush_l0_rotate from flush_to_l1_inner; acquire FlushInProgressGuard here` | writer.rs | nextest |
| 3 | `refactor(writer): extract flush_stream_l1 (no &self); defer save_snapshot to finalize` | writer.rs | nextest + manual db.flush() returns identical snapshot_id format |
| 4a | `refactor(writer): extract static helpers — schedule_index_rebuilds_if_needed_static, tick_fork_fragment_observability_static` | writer.rs | nextest (instance methods now thin wrappers over the static fns) |
| 4b | `refactor(writer): extract flush_finalize_now + flush_finalize_locked; introduce flush_inline_under_lock; rewrite flush_to_l1 as composition; DELETE flush_to_l1_inner` | writer.rs | **Phase-2 equivalence test in §7.1** |
| 5 | `feat(writer): wire FlushCoordinator construction; implement WriterFinalizer FinalizeFn; add try_acquire_permit` | writer.rs, flush_coordinator.rs | nextest |
| 6 | `feat(writer): flush_to_l1_async + FlushCoordinator::submit_for_stream` | writer.rs, flush_coordinator.rs | new test in §7.2 |
| 7 | `feat(writer): dispatch commit_transaction_l0 to async path when async_flush_enabled (gate on pending_count < max; try_acquire_permit; release flush_lock before submit)` | writer.rs, transaction.rs (revert tuple) | §7.2 + §7.2b + §7.5 + §7.5b + §7.5c |
| 8 | `feat(api): drop_fork drains pending flushes; PendingFlushTimeout error` | mod.rs, common/error.rs | §7.6 + §7.6b + §7.7 |
| 9 | `feat(api): track finalizer task via ShutdownHandle` | mod.rs | §7.8 |
| 10 | `test(writer): async flush correctness suite (incl. 7.9, 7.9b)` | tests/ | nextest |
| 11 | `perf(bench): re-run flush_pressure with async on, capture wins` | examples/, plus commit message with numbers | manual |
| 12 | (After soak) `feat(config): default async_flush_enabled = true` | common/config.rs | full nextest pass |

Twelve commits (4 split into 4a + 4b) before turning the flag on by default. Commit 12 ships the feature.

**Implementation order rationale:**

- Commits 1–4a are pure refactors that don't change behavior — bisectable, fast to merge, low risk.
- Commit 4b is the **gate commit**: the new composition replaces the monolith, validated by the §7.1 equivalence test. Anything broken here surfaces immediately.
- Commit 5 introduces the coordinator infrastructure without changing any production code path.
- Commit 6 adds the async API surface (`flush_to_l1_async`) but no caller invokes it yet — still dormant.
- Commit 7 is the **first commit where async actually runs in production code paths** (only when the user opts in via `async_flush_enabled = true`). Behind a default-false flag.
- Commits 8–9 close fork and shutdown loose ends.
- Commits 10–11 are pure test/bench additions.
- Commit 12 flips the default after soak.

---

## 9. Risks and mitigations

| Risk | Likelihood | Severity | Mitigation |
|---|---|---|---|
| Field promotion to Arc has a missed call site | Low | Compiler-caught | Commit 1 is mechanical; compiler enforces |
| `flush_stream_l1` signature is hard to make `Send + 'static` (some captured types aren't Send) | Medium | Build-failure | SharedFlushCtx is explicitly Arc'd; StreamInput is plain owned data. Storage backends are `Send + Sync` by trait bound. |
| Lock-acquire-release-acquire pattern in commit_transaction_l0 has subtle race | Low | Correctness | The release happens AFTER rotate succeeds and submit happens AFTER release. No window where the new L0 is invisible to readers. |
| Manifest fixup misses an edge case | Low | Data corruption | In-order finalize makes the fixup a no-op in the happy path. Failure path explicitly tested in §7.4. |
| Shutdown leaves pending streams unfinalized → stale Lance fragments | Medium | Disk waste | Fragments not referenced by any manifest are invisible. WAL replay on next start recovers their data into L0. Eventually flushed properly. Cleanup pass for stale fragments is future work. |
| Drain-on-drop_fork timeout is too short for realistic workloads | Low | Spurious errors | Configurable via `drop_fork_drain_timeout`. Default 10s should be ample for in-memory and seconds for cloud. |
| Tokio runtime starvation of single finalizer task under heavy load | Low | Latency spike | The task only runs ~µs work per submission. If starvation appears, move it to a dedicated `current_thread` runtime (future tuning). |
| Test flakiness due to async timing | High | CI noise | All async tests use explicit barriers (Notify) for ordering, not sleeps. Test helpers expose `await_pending_flushes()`. |
| Existing tests with `async_flush_enabled=true` flake | Medium | CI noise | The flag defaults to false; only the dedicated async tests flip it. Existing tests pass unchanged. |
| `ShutdownHandle` integration regression | Low | Hang on shutdown | The 30s timeout in `Uni::drop` bounds the wait. Tracked task pattern is well-established (4 existing tasks follow it). |

---

## 10. SOTA context

This design follows well-trodden LSM patterns:

- **RocksDB**: `FlushJob` and `MemTableList`. Each memtable is rotated atomically; flush runs on a dedicated background thread. Manifest commit uses a single `Version` linked list, requiring serial publish. RocksDB uses `ColumnFamily` mutex held briefly for the swap, no lock during flush, brief lock for manifest update.
- **Cassandra**: `Memtable` is swapped on flush trigger; `MemtableFlushWriter` thread writes the SSTable. `View` of memtables + SSTables is updated atomically post-flush.
- **LevelDB**: simpler — single background thread, optionally writes are paused during flush. Not the right model for us (we want commits to proceed during flush).
- **LMDB**: copy-on-write B+ tree, no flush concept. Not applicable.
- **Iceberg**: manifest commits use atomic CAS on `catalog/latest`; on conflict, rebuild manifest on new parent and retry. We use single-task finalizer (simpler, no retry needed because no cross-Writer conflict per-fork).
- **Lance** (our underlying storage): supports concurrent dataset writes; manifest is per-dataset. Our `SnapshotManifest` is a higher-level construct above Lance that bundles all dataset versions into a single linked-list of database snapshots.

**Rust-specific patterns followed:**

- `tokio::sync::Mutex` for async-held locks (`flush_lock`), `parking_lot::Mutex` for short critical sections (cached_manifest, last_flush_time).
- `Arc<Semaphore>` with `.acquire_owned()` for `Send + 'static` back-pressure permits that travel into spawned tasks.
- `mpsc::UnboundedSender` for ordered submission; channel closes naturally when all senders drop, terminating the finalizer task without explicit shutdown protocol.
- `tokio::sync::Notify` for the drain barrier — wakes all waiters; perfect for "wait until count is zero" semantics.
- `Arc<dyn Trait>` for the FinalizeFn callback (decoupling coordinator from Writer); negligible cost (one vtable indirection per finalize).
- Trait objects with `BoxFuture` return types to avoid `impl Trait` capturing self lifetimes.
- `#[instrument(skip(self))]` on each phase for trace correlation.

**Tokio gotchas avoided:**

- Never holding a `parking_lot::Mutex` across an `.await`. The new code's parking_lot uses are all `.lock().clone()` or `*x.lock() = y;` — no await between lock and unlock.
- The `tokio::sync::Mutex` (flush_lock) is held across `.await`s only when those `.await`s are part of the lock's purpose (e.g., I/O during finalize). Never recursively re-acquired.
- Spawned tasks capture only `Send + 'static` data via cloned Arcs.
- The single-finalizer task's mpsc receiver is owned by the task; no shared mutable state outside Arc'd primitives.

**Database-design gotchas avoided:**

- Manifest pointer (`catalog/latest`) write is the last step of finalize. If we crash between manifest body write and pointer write, recovery uses the previous pointer + WAL replay. No "manifest references missing data" window.
- WAL truncate happens AFTER manifest publish. Crash between publish and truncate just means slightly bigger WAL on next start — replay covers it.
- `cached_manifest` and `catalog/latest` agree at all observable moments because finalize updates them under `flush_lock` adjacently.

---

## 11. Definition of done

- All 11 commits land sequentially.
- Full test suite passes with `async_flush_enabled = false` (default).
- Full test suite passes with `async_flush_enabled = true` via test fixture override.
- All 9 new tests (§7.1–§7.9) pass.
- `flush_pressure.rs` shows expected wins (≥ 3× at threshold=5000).
- `concurrent_mutations.rs` shows no regression at default config, ≥ 2× P99 commit-latency improvement.
- Soak test: 1 hour of `flush_pressure.rs` at sess=24 threshold=2500 with async on. Verify no memory growth (mpsc unbounded receive should drain; WAL retention stable).
- Documentation: this spec lives at `docs/proposals/async_l0_to_l1_flush.md` (replacing the older draft).
- Black book, website docs, skills updated with the async-flush feature flag and tuning guidance (similar to mimalloc treatment).

After step 12 (flag default ON) ships in a release and survives one release cycle of soak, commit 13 removes the kill-switch and the `_LEGACY` paths.

---

## 12. Verification & Corrections Applied (2026-05-18)

Plan was cross-checked against the working tree by parallel exploration of `writer.rs`, `flush_coordinator.rs`, `storage/manager.rs`, `config.rs`, `transaction.rs`, `api/mod.rs`, `api/fork.rs`, `runtime/l0*.rs`, `core/snapshot.rs`, `sync.rs`, `shutdown.rs`. All 22 line-numbered code-state claims match (with drift of 1–8 lines); the FlushCoordinator scaffolding is in place. Six concrete corrections and four new test scenarios were folded back into §§3, 6, 7, 8 above. This section records what was applied and what limitations remain.

### 12.1 Corrections applied in-place

| # | Issue | Where applied |
|---|---|---|
| C1 | `create_fork_2pc` path was wrong (`uni-store/src/fork/fork.rs` → `uni/src/api/fork.rs:181–201`) | §3.9 |
| C2 | `FlushInProgressGuard` lifecycle was unspecified — must be acquired during rotate and stashed on `RotatedFlush` | §3.3 (struct field), §6.1(2) (acquisition), §6.1(4) note (O) (drop site), §6.2 (visibility note) |
| C3 | `acquire_permit().await` under `flush_lock` re-introduces the §2.2 convoy | §6.1(7) restructured: gate on `pending_count < max`, use `try_acquire_permit()`, drop `flush_lock` before `submit_for_stream` |
| C4 | Static-variant helpers (`schedule_index_rebuilds_if_needed_static`, `tick_fork_fragment_observability_static`) did not exist | New Commit 4a (extract static fns) added before the finalize commit (renumbered 4b) |
| C5 | §3.8(4) overclaimed Lance version-gating | Softened to "manifest is source of truth; later-version fragments are unreferenced dead bytes" |
| C6 | §3.10 did not document in-flight stream drop at shutdown | New paragraph appended to §3.10 |
| C7 | Fork creation back-pressure under saturated permit pool was undocumented | New paragraph appended to §3.9 |
| C8 | `flush_to_l1_inner_legacy`/deadlock confusion in §6.1(10) | Replaced with explicit `flush_inline_under_lock` helper; `flush_to_l1_inner` is deleted entirely; `flush_finalize_locked` + `flush_finalize_now` share a single `flush_finalize_body` |
| C9 | `try_acquire_permit` and `FlushInProgressGuard` visibility were missing from §6.2 | Added |

### 12.2 Test scenarios added

| New test | Location | Verifies |
|---|---|---|
| 7.2b | Concurrent external `db.flush()` ×N | Manifest chain remains linear under concurrent forced flushes |
| 7.5b | `flush_in_progress` counter under async | C2 — guard counter correctness during async streams |
| 7.5c | Convoy regression guard | C3 — at `max_pending=1`, async wall ≤ 1.2× sync wall |
| 7.6b | Fork creation under in-flight async stream | C7 — fork creation blocks on permit, not on flush_lock; no deadlock |
| 7.9b | Crash-recovery — pointer written, WAL not truncated | §3.7 idempotent WAL replay after partial finalize |

### 12.3 Order of implementation (re-summarized)

The §8 commit table is the canonical order. Conceptually:

1. **Refactor floor** (Commits 1, 2, 3, 4a, 4b): split the monolith into rotate/stream/finalize + extract static helpers. No behavior change. **Gate:** §7.1 equivalence test passes at end of 4b.
2. **Coordinator infrastructure** (Commits 5, 6): wire `FlushCoordinator`, `WriterFinalizer`, `submit_for_stream`, `try_acquire_permit`. Async API exists but no caller invokes it. **Gate:** §7.2 passes.
3. **Production dispatch** (Commit 7): commit_transaction_l0 routes to async path when flag is on. Default-false flag keeps blast radius zero. **Gate:** §7.2 + §7.2b + §7.5 + §7.5b + §7.5c all pass.
4. **Fork & shutdown integration** (Commits 8, 9). **Gate:** §7.6 + §7.6b + §7.7 + §7.8 pass.
5. **Tests & benchmarks** (Commits 10, 11). **Gate:** all §7 tests pass; §11 numbers meet §0 targets.
6. **Flag flip** (Commit 12, after soak).

### 12.4 Remaining limitations (known, accepted)

| Limitation | Why accepted | Mitigation |
|---|---|---|
| **L1: Lance stale-fragment GC is out of scope** | Crash recovery and shutdown leave Lance fragments at versions > the published manifest. They're invisible (manifest doesn't reference them) but occupy disk. | Future cleanup pass (separate spec). Disk-waste risk is bounded by `max_pending_flushes × per-flush-fragment-size × crash-rate`. |
| **L2: `try_acquire_permit` skip is silent in stress** | When the coordinator is at cap and a commit hits `should_flush`, the trigger is skipped — the next commit retries. Under sustained max-pressure this could starve flushes briefly. | `pending_flush_count() < max` gate runs FIRST, so the skip path is only reached on rare permit races. Metric `uni_flush_trigger_skipped_total` makes it observable. If sustained, operator increases `max_pending_flushes`. |
| **L3: Fork creation latency unbounded** | `create_fork_2pc` blocks on `acquire_permit().await` if pool is saturated. No timeout knob added. | Sync fork creation is rare; seconds of wait are tolerable. If this becomes an issue we add `create_fork_flush_timeout` in a follow-up. Documented in §3.9. |
| **L4: BoxFuture allocation per finalize** | `Arc<dyn FinalizeFn>` requires `BoxFuture` return; one heap alloc per finalize. | At default config (~12 flushes/sec under load) this is irrelevant. If the finalize-rate ever exceeds ~10k/sec, switch to a concrete type (no trait object). |
| **L5: Soak test depends on real Lance backend** | §11 1-hour soak at threshold=2500 needs a backend that produces realistic stream durations. In-memory backend's streams are too fast to exercise the pipeline depth. | Use `tempdir` + native Lance backend for soak. Document in commit 11. |
| **L6: `WriterFinalizer` must NOT capture `Arc<Writer>`** | That would create an `Arc<Writer> → Arc<FlushCoordinator> → Arc<dyn FinalizeFn> → Arc<Writer>` cycle, leaking the Writer. | `WriterFinalizer` is a unit struct; it delegates to associated fns (`Writer::flush_finalize_now` / `_locked`) which take `SharedFlushCtx` (Arc<...> bundle, not Arc<Writer>). Enforced by code review at Commit 5. |
| **L7: `flush_pressure.rs` numbers in §0 are projections, not measurements** | The "expected wins" table was estimated from the sync threshold sweep + reasoning about parallelism, not measured (async path doesn't exist yet). | Commit 11 produces real numbers. If they materially undershoot the §0 estimates, surface the discrepancy in the commit message and re-evaluate `max_pending_flushes` defaults. |
| **L8: FlushCoordinator is gated on `async_flush_enabled`** (discovered during Commit 5 implementation) | The spec originally said "always present". Reality: `StorageManager.fork_scope: Some(Arc<ForkScope>)` (storage/manager.rs:364) holds the fork's `ForkHolderGuard`. The coordinator's finalizer task captures `Arc<StorageManager>` via SharedFlushCtx, so the holder count never drops, breaking every fork-drop test (use_case_3_*). | `flush_coordinator: Option<Arc<FlushCoordinator>>`, populated only when `config.async_flush_enabled = true`. Commit 8's `drop_fork` drain handles the on-path. When the default flips in Commit 12, drain is mandatory before the holder_count check — and is the gating reason that flip cannot happen without Commit 8. |

### 12.5 Definition of implementation-ready

The plan is now implementation-ready when, in addition to §11's existing criteria:

- [ ] All §12.1 corrections are reflected in §§3, 6, 7, 8 (verified by inspection: yes, applied).
- [ ] All §12.2 tests are listed in §7 with file paths and assertions (verified: yes).
- [ ] Commit order in §8 matches §12.3 narrative (verified: yes).
- [ ] Each limitation in §12.4 has an explicit accept/mitigate plan (verified: yes).
- [ ] Spec doc copy step is in Commit 11 (Definition-of-done bullet exists in §11).

Plan is ready to implement.

