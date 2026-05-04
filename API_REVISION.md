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

### Phase 2 limits (lifted in later phases)

- No nested forks (Phase 3).
- No fork compaction — long-lived heavy-write forks accumulate L1 fragments. Mitigation: drop-and-recreate, or watch the `fork_fragment_warn_threshold` signal. Phase 5 lands compaction proper.
- No TTL, watch filtering on forks, hooks/params propagation (Phase 4).
- Vector / FTS searches on a forked session use the parent's index (Phase 5 adds fusion).
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
