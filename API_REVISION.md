# Uni-DB Public API Revision Plan

## Context

The `uni-db` Rust crate is the sole public facade over 5 internal crates. An API review against industry standards (rusqlite, Neo4j driver, DuckDB, SurrealDB) identified 14 issues across ergonomics, naming consistency, encapsulation, and missing features. Since all consumers are in-workspace (158 Rust test files, 46 Python test files, no external crates), breaking changes are acceptable — but should be incremental and independently shippable.

---

## Phase 1: Simplify Database Open Path
**Risk: Minimal | Files: 1 | Test impact: 0 broken**

Currently the simplest open requires `.build().await?`. Implement `IntoFuture` for `UniBuilder` so `Uni::open("path").await?` works directly. This is **purely additive** — existing `.build().await?` code continues working.

### Changes
- **`crates/uni/src/api/mod.rs`**: Add `impl IntoFuture for UniBuilder` delegating to `self.build()`
- Update doc-comments on `Uni::open`, `create`, `open_existing`, `temporary`, `in_memory` to show the shorter form as the primary example

### Not doing
- Do NOT deprecate `.build()` yet — that causes 97+ warnings across tests. Revisit later.

---

## Phase 2A: Rename Locy Row Type
**Risk: Low | Files: ~25 internal | Test impact: 0 external test changes**

`uni_locy::Row` (`HashMap<String, Value>`) collides with `uni_query::Row` (struct). Rename to `FactRow`.

### Changes
- **`crates/uni-locy/src/result.rs`**: `pub type Row = ...` → `pub type FactRow = ...`; add deprecated `pub type Row = FactRow` alias
- **All files in `crates/uni-locy/`** (~10 files): Replace `Row` with `FactRow` in signatures and usage
- **`crates/uni-query/src/query/df_graph/locy_*.rs`** (~10 files): Same
- **`crates/uni/src/api/impl_locy.rs`**: Update imports
- **`crates/uni/src/lib.rs`** locy module: Re-export `FactRow` instead of `Row`

---

## Phase 2B: Encapsulate Row / QueryResult / ExecuteResult Fields
**Risk: Medium | Files: ~90 | Test impact: ~80 test files (mechanical)**

Public fields on `Row`, `QueryResult`, `ExecuteResult` prevent future internal changes. Make fields private, add constructors, keep existing accessor methods.

### Changes
- **`crates/uni-query/src/types.rs`**:
  - `Row`: fields → `pub(crate)`, add `pub fn values(&self) -> &[Value]`, `pub fn into_values(self) -> Vec<Value>`, `pub(crate) fn new(...)`
  - `QueryResult`: fields → `pub(crate)`, add `pub(crate) fn new(...)`
  - `ExecuteResult`: field → `pub(crate)`, add `pub fn affected_rows(&self) -> usize`, `pub(crate) fn new(...)`
- **`crates/uni/src/api/impl_query.rs`**: Use `Row::new()`, `QueryResult::new()`, `ExecuteResult::new()`
- **`crates/uni/src/api/query_builder.rs`**, **`transaction.rs`**, **`session.rs`**: Same
- **~80 test files**: Mechanical migration:
  - `result.rows` → `result.rows()` (already returns `&[Row]`)
  - `result.rows[i]` → `result.rows()[i]`
  - `row.values[i]` → `row.values()[i]`
  - `result.columns` → `result.columns()`
  - `result.warnings` → `result.warnings()`
  - `result.affected_rows` → `result.affected_rows()`
- **Python bindings** (`bindings/uni-db/src/`): ~15 sites of `.rows` / `.affected_rows` → accessor calls

### Strategy
Use find-and-replace scripting for the mechanical parts. Run tests after each batch of ~20 files.

---

## Phase 3: Session Naming Consistency + execute_with() Docs
**Risk: Low | Files: ~5 | Test impact: 1-2 test files**

`SessionQueryBuilder::execute()` returns `QueryResult` (reads), while everywhere else `execute()` means mutation. Fix naming to match `QueryBuilder`.

### Changes
- **`crates/uni/src/api/session.rs`**:
  - Rename `SessionQueryBuilder::execute()` → `fetch_all()` (returns `QueryResult`)
  - Rename `SessionQueryBuilder::execute_mutation()` → `execute()` (returns `ExecuteResult`)
  - Add `#[deprecated]` shims for old names
- **`crates/uni/src/api/impl_query.rs`**: Update `execute_with()` doc-comment — clarify it returns `QueryBuilder` which supports both `fetch_all()` and `execute()`, not that it's "an alias for query_with"
- **`crates/uni/tests/session_test.rs`**: Update calls

---

## Phase 4: Clean Up Crate Re-exports
**Risk: Low | Files: ~5 | Test impact: 2 test files**

Raw crate re-exports (`pub use uni_algo as algo_crate`) leak internal structure and create duplicate type paths.

### Changes
- **`crates/uni/src/lib.rs`**: Remove these 4 lines:
  ```rust
  pub use uni_algo as algo_crate;
  pub use uni_common as common;
  pub use uni_query as query_crate;
  pub use uni_store as store;
  ```
  Add missing explicit re-exports: `Properties`, `QueryCursor`
- **`bindings/uni-db/src/core.rs`**: `uni_db::query_crate::QueryCursor` → `uni_db::QueryCursor`
- **`crates/uni/tests/notebook_examples.rs`**: `uni_db::common::Properties` → `uni_db::Properties`
- **`crates/uni/tests/repro_edge_export.rs`**: Same
- Keep `pub mod core/storage/runtime/query/algo` module aliases (they re-export from sub-crate modules, which is fine for internal test access). Consider `#[doc(hidden)]` on any not intended for external use.

---

## Phase 5: Enrich ExecuteResult
**Risk: Medium | Files: ~15 | Test impact: 0 broken (additive)**

`ExecuteResult` only has `affected_rows`. Add per-operation counters and created entity IDs.

### Changes
- **`crates/uni-query/src/types.rs`**: Expand `ExecuteResult`:
  ```rust
  pub struct ExecuteResult {
      affected_rows: usize,
      nodes_created: usize,
      nodes_deleted: usize,
      relationships_created: usize,
      relationships_deleted: usize,
      properties_set: usize,
      labels_added: usize,
      labels_removed: usize,
  }
  ```
  Add accessor method for each. Implement `Default`.
- **`crates/uni/src/api/impl_query.rs`**: After execution, classify the LogicalPlan operation type and populate counters from the existing mutation_count delta (Option A — coarse but correct for single-operation queries)
- **Construction sites** (5 files): Use `ExecuteResult::new(...)` or builder
- Existing tests checking `affected_rows` continue to work via the accessor

### Future
Option B (precise per-category counting via `MutationStats` in `Writer`) is a follow-up that requires deeper executor changes.

---

## Phase 6: Fill Gaps (UniSync, done(), $ prefix)
**Risk: Low | Files: ~15 | Test impact: 5 test files**

### 6A: Complete UniSync Surface
- **`crates/uni/src/api/sync.rs`**: Add to `UniSync`:
  - `locy()` → `LocyEngineSync` (new wrapper)
  - `bulk_writer()` → `BulkWriterBuilderSync` (new wrapper)
  - `explain()`, `profile()`, `session()`
- Add to `QueryBuilderSync`: `timeout()`, `max_memory()`, `execute()`
- Write tests for new sync surface

### 6B: Deprecate done()
- **`crates/uni/src/api/schema.rs`**: Add `#[deprecated]` to `LabelBuilder::done()` and `EdgeTypeBuilder::done()`
- **`crates/uni/src/api/sync.rs`**: Same for sync variants
- Update 5 test files using `.done()` to chain directly

### 6C: Strip $ Prefix in param()
- **`crates/uni/src/api/query_builder.rs`**, **`session.rs`**, **`transaction.rs`**, **`locy_builder.rs`**: In `param()`, add `let key = name.strip_prefix('$').unwrap_or(name);`
- Purely additive behavior — no tests break

---

## Phase 7: Internal Optimizations (Lower Priority)
**Risk: Minimal | All additive, no breaking changes**

### 7A: Query Plan Cache
- Add `Arc<RwLock<LruCache<String, LogicalPlan>>>` field to `Uni`
- Check cache in `execute_internal()` before parse+plan
- Add `plan_cache_capacity` to `UniConfig`
- Entirely internal — no API surface changes

### 7B: CancellationToken Support
- Add `cancellation_token(token)` to `QueryBuilder`
- Thread through `QueryContext` alongside deadline
- Purely additive

### 7C: PropertyOptions Struct
- Add `PropertyOptions { nullable, default }` struct
- Add `property_with_options()` to LabelBuilder/EdgeTypeBuilder
- Deprecate `property_nullable()`

### 7D: Parse Error Position
- Ensure `uni_cypher::parse()` always populates position/line/column
- Remove `Option` wrappers from `UniError::Parse` fields

---

## Execution Order & Dependencies

```
Phase 1 ─────────────────────────────── (independent, ship first)
Phase 2A ────────────────────────────── (independent)
Phase 2B ────── depends on 2A ────────  (largest change)
Phase 3 ─────── after 2B preferred ──── (cleaner)
Phase 4 ─────── after 2B preferred ──── (types.rs settled)
Phase 5 ─────── after 2B required ───── (fields already private)
Phase 6 ─────────────────────────────── (independent, parallel with 3-5)
Phase 7 ─────────────────────────────── (independent, lowest priority)
```

## Verification

After each phase:
1. `cargo build --workspace` — full workspace compiles
2. `cargo test -p uni-db` — all uni-db crate tests pass
3. `cargo test --workspace` — full workspace tests pass
4. `cd bindings/uni-db && poetry run pytest -n auto` — Python tests pass

---

## Forks (Phase 2, writable)

Builds on Phase 1's read-only substrate. `forked.tx().execute(...).commit()` now lands writes on the fork's Lance branches. Same Cypher / Locy as primary; primary remains untouched.

### New public API (Phase 2)

- **`forked.tx()` is writable.** The Phase 1 gate is removed; `UniError::ForkWritesNotYetSupported` is no longer surfaced.
- **`Session::fork_schema()`** — fork-local schema mutation builder. Mirrors `db.schema()`'s shape (`.label(...).description(...).apply().await`, `.edge_type(name, &[from], &[to]).apply().await`). Entries land in the fork's in-memory `SchemaManager` and in the persisted overlay file (`catalog/fork_schemas/{fork_id}.json`); primary is unaffected. Errors with `UniError::InvalidArgument` on a non-forked session.
- **`ForkRegistryHandle::update_schema_overlay(fork_id, &delta)`** — public registry method that PUTs the new overlay; mirrors `register_dataset_branch` semantics. Used by `Session::fork_schema()` under a per-scope `overlay_lock` so concurrent updates on the same fork serialize while cross-fork updates remain parallel.
- **`UniConfig::fork_fragment_warn_threshold: usize`** (default 256). Per-fork L1 flush count above which a `tracing::warn!` fires once per writer; surfaces fork-fragment growth ahead of Phase 5 fork compaction.
- **`UniError::ForkInflightTx { name }`** — `Uni::drop_fork` now refuses with this typed error when a `Transaction` is alive on the fork. Commit or roll back first, then retry.
- **Sibling-session L0 visibility.** Two `session.fork(name)` calls on the same name share a single `Arc<UniInner>` (cached as `Weak<UniInner>` so the cache never extends a session's lifetime). A commit on one session is immediately visible to the other's reads — no flush required. Schema additions through `fork_schema()` from one session are also immediately visible to the other through the shared `SchemaManager` and the `ArcSwap`'d overlay.

### Inherited from Phase 1

- **`Session::fork(name) -> ForkBuilder`** — open-or-create; `.new_().await` errors with `ForkAlreadyExists` when the name is taken.
- **`Session::is_forked() -> bool`**.
- **`Uni::list_forks() -> Vec<ForkInfo>`**, **`Uni::fork_info(name)`**, **`Uni::drop_fork(name)`** (drop is the full 2PC: tombstone → branch deletes → registry clear → file cleanup).
- **Types**: `ForkId`, `ForkInfo`, `ForkStatus`, `SchemaDelta`, `ForkRegistryFile`.
- **Errors**: `ForkNotFound`, `ForkAlreadyExists`, `ForkInUse { name, holder_count }`, `ForkCorruptRegistry`, `ForkLifecycle { name, stage, source }`.

### What the storage substrate looks like (updated for Phase 2)

- **Lance branches**: each fork has one Lance branch per dataset. The main label-agnostic `vertices` and `edges` tables, every `vertices_{label}`, and every `deltas_{type}_{fwd,bwd}` / `adjacency_{type}_{fwd,bwd}` are branched at fork creation if they exist. Datasets that don't exist yet (a label declared but never written, or a brand-new fork-only label) are materialized on-the-fly with the parent commit on `main` left empty so primary's view stays untouched.
- **Per-fork allocator**: `catalog/forks/{fork_id}/id_allocator.json`, bootstrapped from primary's HWM at fork creation so VID/EID streams don't collide.
- **Per-fork WAL**: `wal_forks/{fork_id}/`, replayed in `at_fork`. Primary's recovery never reads it.
- **Dynamic dataset → branch entries**: persisted into the fork's `ForkInfo.datasets` so a restart recovers the same view.
- **Backend wrapper**: a fork-scoped `StorageManager` swaps in a `BranchedBackend` that auto-fills `ScanRequest.branch` for every read and routes writes through `lance_branch::write_to_branch` (creating dataset+branch on the fly when needed).

## Forks (Phase 3 — nested)

Builds on Phase 2. `forked.fork(name)` now succeeds, producing a child whose reads chain through Lance `base_paths` to the parent fork's branch and whose writes/drops/schema additions remain isolated at every level.

### New public API (Phase 3)

- **`forked.fork(name)` is enabled.** The Phase 1 `InvalidArgument` gate is removed. The child's `ForkInfo.parent_fork_id` is set to the parent fork's id; primary-rooted forks keep `parent_fork_id == None`.
- **`Uni::drop_fork(name)`** now refuses with `UniError::ForkHasChildren { name, children }` when the fork has nested children. Drop the children first or use `drop_fork_cascade`.
- **`Uni::drop_fork_cascade(name)`** drops the named fork and every descendant. Pre-validates the entire subtree for live sessions / open transactions before tombstoning anything; surfaces `UniError::ForkSubtreeInUse { blockers }` on any blocker. On success it drops deepest-first via the single-fork path, so a crash mid-cascade resumes through existing tombstone recovery.
- **`Session::flush()`** flushes the session's writer to L1 (forked or primary). Phase 3 also auto-flushes the parent fork's L0 inside `create_fork_2pc` so a nested child sees the parent's committed writes via the Lance chain without the caller needing to remember.
- **New error variants:** `ForkHasChildren { name, children: Vec<String> }`, `ForkSubtreeInUse { blockers: Vec<String> }`.
- **New registry surface:** `ForkRegistryHandle::list_children(ForkId)`, `ForkRegistryHandle::get_by_id(ForkId)`, `ForkRegistryHandle::holder_count_for(ForkId)`.
- **New Lance branch helpers** in `uni-store::backend::lance_branch`: `current_version_on_branch(uri, branch)` and `create_branch_from(uri, new_branch, parent_branch, version)`. Both preserve the existing fault-injection contract.
- **`SchemaDelta::merge_atop(&self, base)`** — associative overlay composition (self wins on collision). Useful as a primitive for diagnostics and promotion logic. The fork's at-session-open overlay merge still happens implicitly via chained `SchemaManager::with_overlay`.

### Storage layout (Phase 3 additions)

- A nested fork's Lance branch has `base_paths = child_branch → parent_branch → main`. Datasets that the parent didn't have a branch for at child-creation time fall through to the existing on-the-fly creation path (`BranchedBackend::ensure_branch_for_new`) — the empty-parent commit still lands on main, which is safe because no ancestor's schema references a fork-only label.
- `ForkInfo.parent_fork_id` is serialized into `catalog/fork_registry.json`. Field has lived in the schema since Phase 1 with serde round-trip tests; no migration needed.

## Forks (Phase 5a-impl — fork-local index fusion)

Builds on Phase 5a substrate (commit `90b62131`). Adds the build pipeline, planner integration, and `FusedIndexScan` operator for the three lossless fusion types (BTree union, sorted k-way merge, fork-first VID/UID lookup). Lossy types (vector ANN, BM25 RRF) are Phase 5b.

### New public API (Phase 5a-impl)

- **`Session::build_fork_local_index(label, column, kind)`** — manual trigger that builds (or registers, for VidUid) a fork-local index. Bypasses the per-fork fragment-count threshold the background builder honors. Errors with `UniError::InvalidArgument` on a non-forked session.
- **`UniConfig::fork_index_builder_interval: Duration`** — background builder polling cadence. Default 30 seconds.
- **`UniConfig::disable_fork_index_builder: bool`** — skip spawning the background builder. Default `false`.
- **`uni_query::FusionKind` enum** (re-exported from the query crate): `BtreeUnion`, `SortedKWayMerge`, `VidUidForkFirst`. `#[non_exhaustive]` so Phase 5b's `AnnRerank` and `Bm25Rrf` are additive.
- **`uni_query::LogicalPlan::FusedIndexScan` variant** with the same fields as `Scan` plus `kind: FusionKind`. Visible in `Session::query_with(...).explain()` output as `FusedIndexScan` for testability.
- **`uni_query::rewrite_for_fork_fusion(plan, lookup)`** — pure-function logical-plan post-pass. Walks the tree, rewrites `Scan`s whose `(label, column)` has a registered fork-local index. Called once after every `planner.plan(ast)` site in `crates/uni/src/api/impl_query.rs`.
- **`uni_query::ForkIndexLookup` trait** — bridge that lets `StorageManager::fork_index_exists` participate in the rewrite without circular crate dependencies. Implemented for `StorageManager`; tests can mock by implementing on a `HashMap`.

### Substrate additions

- **Lance per-branch index spike** (`crates/uni-store/src/backend/lance_branch.rs::tests::phase5a_spike_per_branch_index`) — confirmed (outcome 1 from the plan): `Dataset::create_index_builder` against a branch-checked-out dataset writes the index file branch-locally and does not leak to main. Run with `cargo nextest run -p uni-store --lib backend::lance_branch::tests::phase5a_spike_per_branch_index --run-ignored ignored-only --no-capture`.
- **`uni_store::backend::lance_branch::create_scalar_index_on_branch(uri, branch, column, index_name)`** — thin wrapper exposing Lance's per-branch index build.
- **`uni_store::fork::index_builder::build_fork_local_index(scope, base_uri, label, column, kind)`** — entry point for both the manual and automatic build paths. VidUid is a no-op (no Lance file written; only the `ForkScope` registry entry); ScalarBtree and Sorted call through to `create_scalar_index_on_branch` and register on success.
- **`crates/uni/src/api/fork_index_builder.rs`** — background scheduler mirroring `fork_sweeper`. Polls `ForkRegistryHandle::list_active`, walks each fork's `fragment_counts`, builds `ScalarBtree` for any column primary has indexed (where the fork hasn't already registered).

### Behavior

- **Read correctness on forked sessions is preserved end-to-end through Lance `base_paths` — with or without fork-local indexes registered.** The Phase 5a-impl decay (`FusedIndexScan` → `Scan` at the physical layer) is intentional; the lossless fusion semantics fall out of Lance's chained reads. The planner emission carries observable signal for explain output.
- **Error path:** index build failures are logged-and-continued by the background builder; they propagate to the caller via `Result<()>` from the manual trigger.

### Tests

- `crates/uni/tests/fork_index_vid_uid.rs` (2) — VidUid registration triggers planner rewrite; non-forked session errors with `InvalidArgument`.
- `crates/uni/tests/fork_index_btree.rs` (1) — ScalarBtree registration triggers `BtreeUnion` rewrite; queries return correct results across primary + fork rows.
- `crates/uni/tests/fork_index_sorted.rs` (1) — Sorted registration triggers `SortedKWayMerge` rewrite; ORDER BY produces a globally sorted interleave of primary + fork rows.
- `crates/uni/tests/fork_index_auto_build.rs` (1) — auto-builder fires within 5s when fork crosses `fork_index_build_threshold` and primary has an indexed column.
- `bindings/uni-db/tests/test_fork_index_smoke.py` (1) — Python smoke confirming fork query results are correct end-to-end.

### Phase 5a-impl limits (Phase 5b will lift)

- `FusedIndexScan` decays to `Scan` at the physical planner — no bespoke `Fused*Exec` operators yet. Lossless types don't need them; lossy types in Phase 5b will.
- Vector ANN and BM25 RRF fusion (lossy types) — Phase 5b alongside recall benchmarks.
- Inverted-index fusion folds into BM25 RRF (Phase 5b).
- Auto-builder builds ScalarBtree only; Sorted and VidUid are explicit-only.
- Index fusion across nested-fork chains deeper than `parent → child` is not implemented.

## Forks (Phase 4a — lifecycle & admin, Rust)

Builds on Phase 3. Adds TTL, budget, tags, parent→child cancellation linkage, and lifts the Phase 1 pin+fork restriction. Python bindings remain pending (Phase 4b).

### New public API (Phase 4a)

- **`session.fork(name).ttl(Duration).await`** — set a wall-clock TTL on the fork. The background sweeper drops the fork (cascade) once `Utc::now()` is past `created_at + ttl`. Stamped into `ForkInfo.ttl_expires_at`. Open-or-create returns an existing fork unchanged — TTL only applies at create time.
- **`UniConfig::fork_default_ttl: Option<Duration>`** — applied when the builder doesn't supply a TTL. Default `None`.
- **`UniConfig::fork_sweeper_interval: Duration`** — polling cadence. Default `60s`.
- **`UniConfig::disable_fork_sweeper: bool`** — opt out (tests). Default `false`.
- **`UniConfig::max_forks: Option<usize>`** — cap on total fork count (Active + Pending + Tombstoned). Enforced at `begin_create`. Default `None` (unbounded).
- **`Uni::tag_fork(name, tag)` / `Uni::untag_fork(name, tag)` / `Uni::list_fork_tags(name)`** — Lance tags namespaced as `fork_{tag}_{dataset}`. Tags pin the branch's current version GC-exempt; tagged state survives Lance compaction *and* fork drops, which makes a `tag_fork` then `drop_fork` sequence safe for audit retention.
- **Cancellation parent→child linkage** — `Session::new_forked` now stores `parent_token.child_token()`. Cancelling a parent session cancels every forked descendant; cancelling a child does not affect the parent. Note: `Session::cancel()` cancels the *currently-held* token and replaces it with a fresh one; tests asserting propagation must capture token clones before calling cancel.
- **Pin/refresh on forked sessions** — `Session::pin_to_version`, `pin_to_timestamp`, `refresh`, `is_pinned` all work on forked sessions. The Phase 1 `debug_assert` in `StorageManager::pinned()` that forbade fork+pin is lifted; the pinned manager preserves `fork_scope` so reads still route through the fork's branches at the pinned version.

### New error variants

- `UniError::ForkBudgetExceeded { current: usize, max: usize }` — `Session::fork(name)` refused because `max_forks` is full.

### Substrate additions

- **Lance tags wrapper** in `uni-store::backend::lance_branch`: `create_tag(uri, tag, branch)`, `delete_tag(uri, tag)` (idempotent on missing), `list_tags(uri) -> Vec<(String, u64)>`.
- **`ForkRegistryHandle::list_expired(now) -> Vec<ForkInfo>`** — sweeper input. Returns Active forks only; Pending/Tombstoned recovery handles.
- **`ForkRegistryHandle::set_max_forks(Option<usize>)`** — set after load; `Uni::open` wires from `UniConfig`.
- **TTL sweeper** in `uni-db`'s `api::fork_sweeper` module — interval-driven loop with `ShutdownHandle` integration. Holds a `Weak<UniInner>` so the sweeper does not extend database lifetime; uses `MissedTickBehavior::Skip` to avoid catch-up bursts after a slow cascade.

### Tests (16 new in Phase 4a)

- `fork_watch.rs` (2) — fork watch isolated from primary; sibling forks isolated.
- `fork_hooks.rs` (1) — hooks do not propagate.
- `fork_params.rs` (1) — params do not propagate.
- `fork_pin.rs` (1) — pin + refresh on forked session, writes rejected while pinned.
- `fork_cancel.rs` (4) — parent cascades to child; child does not affect parent; sibling isolation; nested cascade through all levels.
- `fork_tag.rs` (2) — tag/list/untag round-trip + idempotent untag; unknown-fork errors with `ForkNotFound`.
- `fork_budget.rs` (3) — cap blocks creation; slot reused after drop; default unbounded.
- `fork_ttl.rs` (4) — TTL expires + sweeper drops; no-TTL survives; disabled sweeper keeps expired forks; `fork_default_ttl` applies.

Full fork suite: 66 tests, all green.

### Phase 4a limits (deferred)

- Python bindings for forks (Phase 4b).
- `fork_external_sandbox.rs` example (spec §3.7) and `fork_audit.rs` example (spec §3.6) — ship in Phase 4b alongside Python equivalents so the spec scenarios stay binding-symmetric.
- Cucumber TCK for fork lifecycle — see `crates/uni-tck/tck/features/fork/README.md` for the standing rationale (typed payloads don't translate to Gherkin).

### Phase 3 limits (lifted in later phases)

- Cross-fork diff at depth > 1 is Phase 6.
- No re-parenting; a fork's parent is fixed at create-time.
- Property additions to existing primary labels through `fork_schema()` remain out of scope (Lance branches share parent Arrow schema).
- TTL, tags, watch filtering, hooks/params on fork sessions remain Phase 4.

### Verification

- `cargo nextest run -p uni-db --test fork_nested` — 7 tests covering 3-level chain, snapshot isolation at each level, sibling-fork isolation, `drop_fork` child guard, cascade subtree removal, cascade subtree-in-use refusal, and nested strict-schema overlay composition.
- Full Phase 1 + 2 fork suite (`fork_read_only`, `fork_writes`, `fork_concurrent_writers`, `fork_locy_rules`, `fork_new_label`, `fork_drop_inflight`, `fork_creation_concurrency`, `fork_no_primary_blocking`, `fork_fragment_warn`, `fork_flush_known_labels`, `fork_strict_schema`, `strict_schema_test`) — 41 tests, all pass.
- `cargo nextest run -p uni-store -p uni-common` — 543 tests pass (full uni-store + uni-common suite).

- No fork compaction — long-lived heavy-write forks accumulate L1 fragments. Mitigation: drop-and-recreate, or watch the `fork_fragment_warn_threshold` signal. Phase 5 lands compaction proper.
- (Resolved in Phase 4a — see below.) TTL, budget, tag, parent→child cancellation, and the watch/hooks/params/pin contract on forked sessions.
- Vector / FTS searches on a forked session use the parent's index (Phase 5 adds fusion).
- Python bindings for forks remain pending (Phase 4b).
- (Resolved.) Strict-schema deployments now have a fork-local schema mutation path: `Session::fork_schema().label(...).apply()` and `.edge_type(...)` add entries to the fork's persisted `SchemaDelta` overlay and to the fork's in-memory `SchemaManager` without touching primary. See "Fork-local schema additions" below.

### Verification

- Phase 1 tests carried forward: `fork_read_only`, `fork_creation_concurrency`, `fork_no_primary_blocking`, `lance_branch_retention`, `recovery_fork_*`.
- Phase 2 substrate: `fork_writes`, `fork_branch_writes`, `branched_backend_writes`, `recovery_fork_wal`.
- Phase 2 Days 8–14 added:
  - `fork_concurrent_writers` — same-fork-name sessions share a writer; cross-fork writes proceed in parallel.
  - `fork_locy_rules` — registry isolation is correct by construction (Phase 1's deep clone in `at_fork`).
  - `fork_flush_known_labels` — fork flushes succeed end-to-end for primary-known labels.
  - `fork_new_label` — fork-only labels materialize a dataset+branch on the fly; primary stays empty; restart preserves the dynamic branch mapping.
  - `fork_drop_inflight` — open `Transaction` on a fork surfaces `ForkInflightTx`; commit clears it.
  - `fork_fragment_warn` — observability gauge + one-shot warn fire on the fork writer; primary writers never emit.
  - `fork_writes_soak` (`#[ignore]`) — N forks × M mutations × R restarts; opt in with `--run-ignored ignored-only`.
- Cypher TCK: 3969/3969. Locy TCK: 434/434. Zero regressions in Phase 2.
5. `cargo doc -p uni-db --no-deps` — docs generate without warnings
