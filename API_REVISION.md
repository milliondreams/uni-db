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
5. `cargo doc -p uni-db --no-deps` — docs generate without warnings
