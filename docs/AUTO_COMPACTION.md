# Auto-Compaction in Uni

## Executive Summary

Uni implements a **multi-tier, LSM-inspired compaction system** that keeps storage efficient and read performance stable as data accumulates. Compaction operates at three distinct layers:

1. **CSR overlay compaction** (in-memory) — merges frozen L0 CSR segments into the main adjacency index after each flush, triggered when frozen segments accumulate past a threshold. **Fully automatic.**
2. **Semantic compaction** (application-level) — merges versioned vertex rows with CRDT-aware property resolution, applies tombstone cleanup, and consolidates L1 delta runs into L2 base adjacency. **Automatic** via background loop, also triggerable via `VACUUM` command or public API.
3. **Lance storage compaction** (storage-level) — calls LanceDB's `optimize(OptimizeAction::All)` to consolidate fragmented data files, rebuild indexes, and reclaim space from internal tombstones. **Automatic** via background loop after semantic compaction.

**Current state:** All three tiers run automatically. The background compaction loop runs semantic compaction (Tier 2) followed by Lance optimize (Tier 3) when any of three triggers fire: L1 run count, aggregate L1 size, or L1 age. Write backpressure exists to prevent unbounded L1 growth.

The system is designed for single-writer, multi-reader concurrency with zero-visibility-gap guarantees during compaction.

---

## Requirements and Purpose

### Why Compaction is Needed

Uni's write path is append-only by design:

- **Vertex writes** append new rows to per-label LanceDB tables, creating multiple versions of the same vertex across fragments.
- **Edge writes** flush from the in-memory L0 buffer into L1 delta tables (sorted runs in LanceDB), one per edge type and direction.
- **Adjacency updates** accumulate as frozen CSR overlay segments in memory after each flush cycle.

Without compaction, this append-only pattern causes:

| Problem | Impact |
|---------|--------|
| **Fragment proliferation** | LanceDB tables accumulate many small data files, degrading scan performance |
| **Duplicate vertex rows** | Multiple versions of the same vertex inflate storage and slow reads |
| **Unbounded L1 delta growth** | Delta tables grow indefinitely, increasing adjacency lookup latency |
| **Memory pressure from CSR overlays** | Frozen segments consume RAM and add per-lookup merge overhead |
| **Stale tombstones** | Deleted vertices/edges remain as soft-delete markers, wasting space |

### Design Goals

1. **Transparent** — compaction runs automatically without user intervention.
2. **Non-blocking** — readers never see missing data during compaction (no visibility gaps).
3. **CRDT-correct** — property merging respects CRDT semantics (commutative merge) while applying LWW for non-CRDT properties.
4. **Bounded memory** — OOM guards prevent compaction from consuming unbounded memory on large datasets.
5. **Configurable** — all thresholds and intervals are tunable via `CompactionConfig`.
6. **Observable** — compaction status is queryable via API and exposed through metrics.

---

## Design

### Storage Model (LSM-Style)

```
┌─────────────────────────────────────────────────────────┐
│  L0 — In-Memory Write Buffer                            │
│  ┌───────────────┐  ┌──────────────────────────────┐    │
│  │  L0Buffer      │  │  Active L0CsrSegment         │    │
│  │  (SimpleGraph)  │  │  (writable adjacency overlay)│    │
│  └───────┬───────┘  └──────────┬───────────────────┘    │
│          │ flush                │ freeze                  │
├──────────┼──────────────────────┼────────────────────────┤
│  L1 — Sorted Delta Runs (LanceDB)                        │
│  ┌───────▼───────┐  ┌──────────▼───────────────────┐    │
│  │ VertexDataset  │  │  DeltaDataset (per edge_type  │    │
│  │ (per label)    │  │  × direction: fwd/bwd)        │    │
│  │ append-only    │  │  L1Entry: src, dst, eid, op,  │    │
│  │ rows           │  │  version, properties           │    │
│  └───────┬───────┘  └──────────┬───────────────────┘    │
│          │ compact              │ compact                 │
├──────────┼──────────────────────┼────────────────────────┤
│  L2 — Compacted Base Storage                             │
│  ┌───────▼───────┐  ┌──────────▼───────────────────┐    │
│  │ VertexDataset  │  │  AdjacencyDataset (chunked    │    │
│  │ (deduplicated, │  │  CSR per edge_type/direction) │    │
│  │  merged props) │  │  + Main CSR (in-memory)       │    │
│  └───────────────┘  └──────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

### Compaction Tiers

#### Tier 1: CSR Overlay Compaction (In-Memory) — AUTOMATIC

**Trigger:** After each L0 flush, when `frozen_segments.len() >= 4`.

**What it does:**
1. Freezes the active `L0CsrSegment` and pushes it onto the frozen list.
2. Iterates all `(edge_type, direction)` keys across frozen segments and existing Main CSRs.
3. For each key:
   - Collects tombstones from frozen segments → moves to Shadow CSR.
   - Replays entries from old Main CSR (skipping tombstoned edges).
   - Overlays frozen segment entries oldest-first (skipping tombstoned edges).
   - Deduplicates by `Eid`, keeping the entry with the highest version.
   - Builds a new `MainCsr` via `MainCsr::from_edge_entries()`.
4. Atomically swaps the new Main CSR into the `DashMap`.
5. Clears frozen segments.

**Visibility guarantee:** Frozen segments remain readable throughout the rebuild. The `DashMap` insert is atomic — readers always see either old or new, never nothing.

**Concurrency:** A `compaction_handle` in `Writer` ensures only one CSR compaction runs at a time.

```
Writer::flush()
  └─ if frozen_segments >= 4 && no compaction running
       └─ spawn → AdjacencyManager::compact()
            ├─ freeze active overlay
            ├─ merge frozen + Main CSR → new Main CSR
            ├─ tombstones → Shadow CSR
            ├─ deduplicate by Eid (highest version wins)
            └─ clear frozen segments
```

#### Tier 2: Semantic Compaction (Application-Level) — AUTOMATIC

**Trigger:** Automatic via background loop when any of three conditions are met: L1 run count exceeds `max_l1_runs`, aggregate L1 size exceeds `max_l1_size_bytes`, or oldest L1 entry age exceeds `max_l1_age`. Also available via `VACUUM` Cypher command, `compact_label()`, `compact_edge_type()`, or `compact_all()` API.

##### Vertex Compaction (`Compactor::compact_vertices`)

1. Reads all rows for a label from LanceDB into memory.
2. Builds a `HashMap<Vid, VertexState>` tracking the latest properties and deletion status.
3. For each row:
   - **CRDT properties**: Merged commutatively via `merge_crdt_values()` — order-independent, idempotent.
   - **Non-CRDT properties**: LWW semantics — highest version wins.
   - **Tombstones**: `deleted = true` marks the vertex for removal.
4. Filters out tombstoned vertices.
5. Writes the merged result back via `replace_lancedb()` (atomic table replacement).

##### Adjacency Compaction (`Compactor::compact_adjacency`)

1. Loads all L1 delta entries via `DeltaDataset::scan_all_lancedb()`.
2. Groups entries by direction key (`src_vid` for forward, `dst_vid` for backward).
3. Sorts each VID's operations by version (ensures correct ordering of Insert → Delete → Insert sequences).
4. Streams L2 adjacency rows, applies deltas, writes merged result.
5. Processes new vertices from deltas that don't exist in L2.
6. Clears the Delta L1 table via `replace_lancedb()` with an empty batch.

**Important:** Edge properties are dual-written to both delta tables and `main_edges`. After compaction clears L1, property reads fall back to `main_edges` (Issue #53 fix).

##### VACUUM Command

The `VACUUM` Cypher command (`execute_vacuum()` in `uni-query`) is the primary entry point for full semantic compaction:

```
VACUUM
  └─ execute_vacuum()
       ├─ flush L0 buffer to L1 (ensures all in-flight data is persisted)
       ├─ Compactor::compact_all()
       │    ├─ compact_vertices() for each label
       │    └─ compact_adjacency() for each edge type × direction
       └─ re-warm in-memory CSR from compacted L2 storage
```

#### Tier 3: Lance Storage Compaction (Storage-Level) — AUTOMATIC

**Trigger:** Runs automatically after Tier 2 semantic compaction within the same `execute_compaction` cycle.

**What it does:** Calls `table.optimize(OptimizeAction::All)` on all LanceDB tables (delta, vertex, adjacency, main vertex, main edge), which internally:
- Consolidates small data file fragments into larger ones.
- Rebuilds vector and scalar indexes.
- Removes internal LanceDB tombstones.
- Reclaims disk space.

**What it does NOT do:** Semantic merging. It does not deduplicate vertex rows, merge CRDT properties, apply tombstone cleanup, or consolidate L1 deltas into L2 adjacency. Those operations are handled by Tier 2, which runs first.

### Background Compaction Loop

```
StorageManager::start_background_compaction(shutdown_rx)
  │
  │  (returns immediately if config.compaction.enabled == false)
  │
  └─ loop {
       sleep(config.compaction.check_interval)  // default 30s
       │
       ├─ check shutdown_rx → break
       │
       ├─ update_compaction_status()
       │    ├─ count non-empty delta tables → l1_runs
       │    ├─ sum(row_count × ENTRY_SIZE_ESTIMATE) → l1_size_bytes
       │    └─ min(_created_at) across deltas → oldest_l1_age
       │
       └─ pick_compaction_task()
            ├─ l1_runs >= max_l1_runs (4)    → ByRunCount  ✅
            ├─ l1_size_bytes >= 256MB         → BySize      ✅
            ├─ oldest_l1_age >= 1h            → ByAge       ✅
            └─ none met                       → skip
                 │
                 └─ execute_compaction(task)
                      ├─ acquire CompactionGuard
                      │
                      ├─ Tier 2: Semantic compaction
                      │    ├─ Compactor::compact_all()
                      │    │    ├─ compact_vertices() per label
                      │    │    └─ compact_adjacency() per edge type × direction
                      │    └─ re-warm adjacency CSR for compacted edge types
                      │
                      ├─ Tier 3: Lance optimize
                      │    ├─ optimize all delta tables
                      │    ├─ optimize all vertex tables
                      │    ├─ optimize all adjacency tables (L2)
                      │    ├─ optimize main vertex table
                      │    └─ optimize main edge table
                      │
                      └─ drop guard (updates status)
     }
     │
     └─ on shutdown: wait_for_compaction()
```

### Write Backpressure

The writer implements **exponential backpressure** to prevent unbounded L1 growth when compaction can't keep up. Controlled via `WriteThrottleConfig`:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `soft_limit` | `usize` | `8` | L1 run count to start throttling (exponential backoff) |
| `hard_limit` | `usize` | `16` | L1 run count to block writes entirely |
| `base_delay` | `Duration` | `10 ms` | Base delay multiplied by `2^(l1_runs - soft_limit)` |

**Behavior:**
- **Below soft limit:** Writes proceed at full speed.
- **At/above soft limit:** Exponential backoff — `base_delay × 2^excess` (e.g., 10ms, 20ms, 40ms, 80ms...).
- **At/above hard limit:** Writes block entirely, polling every 100ms until L1 runs drop below the limit.

Applied to all write operations: vertex upsert, batch insert, vertex delete, edge upsert, edge delete.

### Configuration

All compaction behavior is controlled via `CompactionConfig`:

| Parameter | Type | Default | Description | Status |
|-----------|------|---------|-------------|--------|
| `enabled` | `bool` | `true` | Enable/disable background compaction | ✅ Working |
| `max_l1_runs` | `usize` | `4` | Trigger compaction when non-empty L1 delta table count reaches this value | ✅ Working |
| `max_l1_size_bytes` | `u64` | `256 MB` | Trigger compaction when aggregate L1 size exceeds this value | ✅ Working |
| `max_l1_age` | `Duration` | `1 hour` | Trigger compaction when oldest L1 run exceeds this age | ✅ Working |
| `check_interval` | `Duration` | `30 seconds` | How often the background task checks compaction conditions | ✅ Working |
| `worker_threads` | `usize` | `1` | Number of compaction worker threads | ✅ Working |

> **Note on `max_l1_runs`:** This counts **non-empty** delta tables (tables with `row_count > 0`). After semantic compaction clears delta tables, `l1_runs` drops to 0 even though the tables still exist. Each edge type creates 2 tables (fwd + bwd).

Additional related configuration:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_compaction_rows` | `5,000,000` | OOM guard: max rows loaded into memory during compaction |
| CSR compaction threshold | `4` (hardcoded in Writer) | Frozen CSR segments before triggering overlay compaction |

### Concurrency and Safety

**CompactionGuard** (RAII pattern):
- Sets `compaction_in_progress = true` on construction.
- Sets `compaction_in_progress = false` and updates `last_compaction` in `Drop`.
- `Drop` never panics — handles poisoned locks gracefully (Issues #18, #150).
- Only one compaction can run at a time per guard.

**Reader isolation:**
- Snapshot-based reads are unaffected by concurrent compaction.
- CSR compaction uses frozen segments as a read-visible bridge until the new Main CSR is installed.
- LanceDB table replacement is atomic via staging table + rename.

**OOM protection:**
- `check_oom_guard()` validates row counts before loading into memory.
- Estimated per-row cost: ~145 bytes × 5M rows ≈ 725 MB ceiling.
- Exceeding the limit produces a descriptive error directing users to increase the limit or use chunked compaction.

### Observability

**Compaction status** (queryable via `compaction_status()` or Cypher `CALL uni.admin.compactionStatus()`):

```rust
CompactionStatus {
    l1_runs: usize,                // Current L1 delta table count
    l1_size_bytes: u64,            // Aggregate L1 size (estimated from row count × ENTRY_SIZE_ESTIMATE)
    oldest_l1_age: Duration,       // Age of oldest L1 run (computed from min _created_at)
    compaction_in_progress: bool,  // Whether compaction is currently running
    compaction_pending: usize,     // Tasks awaiting execution
    last_compaction: Option<SystemTime>,
    total_compactions: u64,
    total_bytes_compacted: u64,
}
```

**Metrics** (Prometheus-style):
- `uni_compaction_runs_total` — total compaction executions
- `uni_compaction_duration_seconds` — per-compaction duration (vertex, adjacency)
- `uni_compaction_rows_reclaimed_total` — rows removed via deduplication/tombstone cleanup

### Public API

```rust
impl Uni {
    /// Compact a specific vertex label (merges versions, removes tombstones).
    pub async fn compact_label(&self, label: &str) -> Result<CompactionStats>;

    /// Compact a specific edge type (merges L1 deltas into L2 adjacency).
    pub async fn compact_edge_type(&self, edge_type: &str) -> Result<CompactionStats>;

    /// Block until any in-progress compaction completes.
    pub async fn wait_for_compaction(&self) -> Result<()>;
}
```

Cypher admin procedures:
```cypher
CALL uni.admin.compactionStatus()   -- Returns current compaction state
CALL uni.admin.compact()            -- Triggers full manual compaction
VACUUM                              -- Flush L0 + full semantic compaction + CSR re-warm
```

---

## Current State

### Automation Status by Tier

| Tier | Automatic? | Trigger | What Runs |
|------|-----------|---------|-----------|
| **Tier 1: CSR Overlay** | ✅ Yes | `frozen_segments >= 4` after flush | Full in-memory CSR rebuild |
| **Tier 2: Semantic** | ✅ Yes | Background loop (ByRunCount, BySize, or ByAge) | Vertex dedup, CRDT merge, L1→L2 consolidation, tombstone cleanup |
| **Tier 3: Lance Optimize** | ✅ Yes | After Tier 2 in same cycle | Fragment consolidation, index rebuild, space reclaim |

### Background Trigger Status

| Trigger | Config Field | Status | Description |
|---------|-------------|--------|-------------|
| **ByRunCount** | `max_l1_runs` | ✅ Working | Counts non-empty delta tables (tables with `row_count > 0`) |
| **BySize** | `max_l1_size_bytes` | ✅ Working | Estimates size from `row_count × ENTRY_SIZE_ESTIMATE` (145 bytes/row) |
| **ByAge** | `max_l1_age` | ✅ Working | Queries min `_created_at` timestamp across all delta tables |

### What This Means in Practice

The system is fully launch-and-forget:
- **All three tiers run automatically** — no manual `VACUUM` required for normal operation.
- **Semantic compaction** (vertex dedup, CRDT merge, L1→L2 delta consolidation, tombstone cleanup) runs before Lance optimize in each background compaction cycle.
- **All three triggers are functional** — compaction fires based on run count, size, or age, whichever threshold is reached first.
- **Write backpressure** provides a safety net by throttling/blocking writes if L1 growth temporarily outpaces compaction.

---

## Known Gaps and Future Work

### ~~Gap 1: Semantic compaction not automatic~~ — RESOLVED

`execute_compaction()` now calls `Compactor::compact_all()` before Lance optimize. All three tiers run in the background loop.

### ~~Gap 2: BySize trigger broken~~ — RESOLVED

`l1_size_bytes` is now computed from `sum(count_rows() × ENTRY_SIZE_ESTIMATE)` across all non-empty delta tables.

### ~~Gap 3: ByAge trigger not implemented~~ — RESOLVED

`oldest_l1_age` is now computed from `min(_created_at)` across all delta table rows. The `ByAge` variant is returned from `pick_compaction_task()` when the threshold is exceeded.

### ~~Gap 4: l1_runs metric is misleading~~ — RESOLVED

`l1_runs` now counts only **non-empty** delta tables (`row_count > 0`). After semantic compaction clears deltas, `l1_runs` drops to 0.

### ~~Gap 5: Incomplete Lance optimization~~ — RESOLVED

Lance `optimize()` now runs on all table types: delta, vertex, adjacency (L2), main vertex, and main edge.

### Gap 6: Streaming vertex compaction — FUTURE

| Attribute | Detail |
|-----------|--------|
| **Problem** | All vertex rows loaded into memory for merge. |
| **Impact** | OOM risk on labels with >5M vertices (guarded but blocks compaction entirely). |
| **Fix** | Streaming merge-sort with bounded memory. |
| **Difficulty** | **Hard** — requires significant refactor of `compact_vertices()`. |
| **Location** | `crates/uni-store/src/storage/compaction.rs:82-284` |

### Gap 7: Incremental compaction — FUTURE

| Attribute | Detail |
|-----------|--------|
| **Problem** | Always compacts all data for a table (full rewrite). |
| **Impact** | Large tables incur full rewrite even for small deltas. |
| **Fix** | Track which VIDs have pending changes and compact only affected ranges. |
| **Difficulty** | **Hard** — needs change tracking infrastructure. |

### Gap 8: Adaptive tuning — FUTURE

| Attribute | Detail |
|-----------|--------|
| **Problem** | Fixed thresholds, no workload-based heuristics. |
| **Impact** | Suboptimal for mixed read/write workloads. |
| **Fix** | Auto-tune thresholds based on write rate, read latency, and compaction duration. |
| **Difficulty** | **Hard** — requires instrumentation and feedback loop. |

### Future Work Priority

| Priority | Gap | Effort | Impact | Result |
|----------|-----|--------|--------|--------|
| **1** | Streaming vertex compaction | Hard | High | Removes OOM risk for labels with >5M vertices |
| **2** | Incremental compaction | Hard | Medium | Avoids full table rewrites for small deltas |
| **3** | Adaptive tuning | Hard | Medium | Auto-tunes thresholds based on workload |

**Bottom line:** Gaps 1-5 are resolved. The auto-compaction system is now fully launch-and-forget for all practical workloads. Remaining gaps (6-8) are scalability improvements for large datasets.

---

## Regression Tests

The compaction system has extensive test coverage across 9 test files:

| Test File | Coverage |
|-----------|----------|
| `uni/tests/compaction_test.rs` | Core L1→L2 compaction, CRDT merging, admin procedures |
| `uni/tests/compaction_out_of_order_test.rs` | Version ordering for Insert/Delete/Insert sequences |
| `uni/tests/compaction_edge_cases.rs` | Null properties, empty datasets |
| `uni/tests/compaction_granular_test.rs` | Per-label and per-edge-type manual compaction |
| `uni-store/tests/background_compaction_test.rs` | Configuration propagation, write throttling |
| `uni-store/tests/crdt_compaction_tests.rs` | GCounter, GSet, VectorClock merging, mixed CRDT/LWW |
| `uni-store/tests/test_issue_53_*.rs` | Edge properties survive adjacency compaction (dual-write) |
| `uni-store/tests/test_issue_54_*.rs` | No visibility gaps during concurrent reads |
| `uni-store/tests/test_issue_143_*.rs` | OOM guard enforcement |
