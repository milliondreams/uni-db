# L0 Refactor: Five-Phase Design

## Target State

```
Global L0           l0_manager.get_current(). Committed pre-flush data.
                    All writes ultimately merge here.

Transaction L0      Private per-tx buffer. Created by session.tx().
                    ALL writes go through Transaction. Merged on commit.

Session Locy L0     Ephemeral per-locy() buffer. Forked from Global (session)
                    or Transaction L0 (tx). DERIVE writes here. ASSUME/ABDUCE
                    fork from here. Trailing Cypher reads here. Discarded on return.
```

No `writer.transaction_l0`. No `savepoint_active`. No SQL BEGIN/COMMIT. No write methods on Session. One L0 per concern.

---

## Phase 1: Remove SQL BEGIN/COMMIT/ROLLBACK and Writer Transaction Methods

**Goal**: Remove the legacy SQL transaction path and the Writer methods it depends on.

### Changes

#### 1a. Grammar — Remove transaction keywords
**File**: `crates/uni-cypher/src/grammar/cypher.pest`
- Remove `BEGIN`, `COMMIT`, `ROLLBACK` keyword rules (lines ~93-95, 751-753)

**File**: `crates/uni-cypher/src/grammar/walker.rs`
- Remove `TransactionCommand` enum handling (line ~2542-2544)

**File**: `crates/uni-cypher/src/ast.rs`
- Remove `TransactionCommand` enum variants if they exist

#### 1b. Planner — Remove logical plan nodes
**File**: `crates/uni-query/src/query/planner.rs`
- Remove `LogicalPlan::Begin`, `LogicalPlan::Commit`, `LogicalPlan::Rollback`

**File**: `crates/uni-query/src/query/df_planner.rs`
- Remove corresponding plan node handling

#### 1c. Executor — Remove dispatch
**File**: `crates/uni-query/src/query/executor/read.rs` (lines ~2880-2905)
- Remove `LogicalPlan::Begin`, `LogicalPlan::Commit`, `LogicalPlan::Rollback` match arms

#### 1d. Writer — Remove begin/commit/rollback methods
**File**: `crates/uni-store/src/runtime/writer.rs`
- Remove `begin_transaction()` (lines 182-188)
- Remove `commit_transaction()` (lines 223-230) — keep `commit_transaction_l0()` (used by Transaction API)
- Remove `rollback_transaction()` (lines 368-372)

#### 1e. Fix `batch_upsert()` / `insert_vertices_batch()`
**File**: `crates/uni-store/src/runtime/writer.rs` (lines ~1417-1534)
- Remove `is_nested` check (`self.transaction_l0.is_some()`)
- Remove auto-begin/auto-commit/auto-rollback wrapper
- `batch_upsert()` should write directly to `l0_manager.get_current()` (it's always called with the writer lock held, so atomicity is guaranteed by the lock)
- Or: create a local L0 buffer, batch into it, merge into current — same pattern but without using `writer.transaction_l0`

#### 1f. Delete test
**File**: `crates/uni/tests/cypher_transactions.rs` — Delete entirely (only consumer of SQL BEGIN/COMMIT)

#### 1g. Update Writer tests
**Files**: `crates/uni-store/tests/test_issue_112_transaction_edge_versions.rs`, `crates/uni-store/tests/test_issue_19_tx_memory_limit.rs`
- These tests call `writer.begin_transaction()` directly
- Rewrite to use the Writer's remaining APIs or test the behavior through the Transaction API instead

### Verification

- [ ] `cargo nextest run --workspace` — no compilation errors, all tests pass
- [ ] Grammar tests still parse valid Cypher
- [ ] `BEGIN` in Cypher now produces a parse error

### Phase 1 Completion Audit

Before proceeding to Phase 2, verify EVERY item was actually completed:

- [ ] `grep -r "BEGIN\|COMMIT\|ROLLBACK" crates/uni-cypher/src/grammar/cypher.pest` → no transaction keywords
- [ ] `grep -r "TransactionCommand" crates/uni-cypher/src/` → zero results
- [ ] `grep -r "LogicalPlan::Begin\|LogicalPlan::Commit\|LogicalPlan::Rollback" crates/` → zero results
- [ ] `grep -r "begin_transaction\b" crates/uni-store/src/runtime/writer.rs` → zero results
- [ ] `grep -r "commit_transaction\b" crates/uni-store/src/runtime/writer.rs` → only `commit_transaction_l0` remains
- [ ] `grep -r "rollback_transaction\b" crates/uni-store/src/runtime/writer.rs` → zero results
- [ ] `grep -r "is_nested" crates/uni-store/src/runtime/writer.rs` → zero results
- [ ] `test -f crates/uni/tests/cypher_transactions.rs` → file does NOT exist
- [ ] `grep -r "begin_transaction\|rollback_transaction\|commit_transaction\b" crates/uni-store/tests/` → zero results (tests rewritten)
- [ ] Full test suite green: `cargo nextest run --workspace --no-fail-fast` → 0 failures

---

## Phase 2: Remove Write Ops from Session + Python Bindings

**Goal**: Session becomes truly read-only. All writes go through Transaction. Update Python bindings to match.

### Scope

**Methods to remove from Session** (Rust + Python sync + Python async):

| Method | Rust call sites | Python exposure |
|--------|----------------|-----------------|
| `session.execute()` | ~1,351 test sites | `Session.execute()`, `AsyncSession.execute()` |
| `session.execute_with()` | builder variant | `Session.execute_with()`, `AsyncSession.execute_with()` |
| `session.bulk_insert_vertices()` | ~20 test sites | `Session.bulk_insert_vertices()`, `AsyncSession.bulk_insert_vertices()` |
| `session.bulk_insert_edges()` | ~20 test sites | `Session.bulk_insert_edges()`, `AsyncSession.bulk_insert_edges()` |
| `session.bulk_writer()` | ~30 test sites | `Session.bulk_writer()`, `AsyncSession.bulk_writer()` |
| `session.appender()` | 1 test site | `Session.appender()`, `AsyncSession.appender()` |

**Methods to ADD to Transaction** (currently missing):

| Method | Notes |
|--------|-------|
| `tx.bulk_insert_vertices()` | Route through `tx_l0` |
| `tx.bulk_insert_edges()` | Route through `tx_l0` |
| `tx.bulk_writer()` | Return builder that uses `tx_l0` |
| `tx.appender()` | Return builder that uses `tx_l0` |

### Changes

#### 2a. Add bulk operations to Transaction
**File**: `crates/uni/src/api/transaction.rs`
- Add `bulk_insert_vertices()`, `bulk_insert_edges()`, `bulk_writer()`, `appender()` that route through `self.tx_l0`

#### 2b. Remove write methods from Session
**File**: `crates/uni/src/api/session.rs`
- Remove `execute()`, `execute_with()`, `bulk_insert_vertices()`, `bulk_insert_edges()`, `bulk_writer()`, `appender()`, `appender_builder()`

**File**: `crates/uni/src/api/sync.rs`
- Remove corresponding sync wrappers

#### 2c. Migrate ALL Rust tests
**~1,400+ call sites** across `crates/uni/tests/`:
- Pattern: `session.execute("CREATE ...")` → `let tx = session.tx().await?; tx.execute("CREATE ...").await?; tx.commit().await?;`
- Bulk: `session.bulk_insert_vertices(...)` → `tx.bulk_insert_vertices(...)`
- Can batch multiple `execute()` calls in one transaction
- Helper function to reduce boilerplate: `async fn setup_data(session: &Session, cypher: &str)`

#### 2d. Update Python bindings
**Files**: `bindings/uni-db/src/builders.rs`, `bindings/uni-db/src/sync_api.rs`, `bindings/uni-db/src/async_api.rs`
- Remove Session write methods from Python class
- Add Transaction bulk methods to Python class

**File**: `bindings/uni-db/uni_db/__init__.pyi`
- Update type stubs to reflect removed/added methods

#### 2e. Migrate Python tests
**Files**: `bindings/uni-db/tests/test_e2e_*.py`, `bindings/uni-db/tests/test_async_e2e_*.py`
- Same pattern: `session.execute()` → `tx = session.tx(); tx.execute(); tx.commit()`

#### 2f. Update conftest.py fixtures
**File**: `bindings/uni-db/tests/conftest.py`
- `social_db_populated` fixture uses `session.execute()` for data setup
- Change to use Transaction

#### 2g. Update uni-pydantic
**File**: `bindings/uni-pydantic/python/uni_pydantic/session.py`
- `UniSession.__init__` calls `db.session()` and stores `self._db_session`
- Internal writes go through `self._db_session.execute()` — change to use Transaction internally

### Verification

- [ ] `cargo nextest run --workspace` — all pass
- [ ] `cd bindings/uni-db && uv run maturin develop && uv run pytest tests/ -v` — all pass
- [ ] `cd bindings/uni-pydantic && uv run pytest tests/ -v` — all pass
- [ ] `session.execute()` produces a compile error (Rust) / AttributeError (Python)

### Phase 2 Completion Audit

Before proceeding to Phase 3, verify EVERY item was actually completed:

- [ ] `grep -rn "fn execute\|fn execute_with\|fn bulk_insert\|fn bulk_writer\|fn appender" crates/uni/src/api/session.rs` → zero write methods remain
- [ ] `grep -rn "fn execute\|fn execute_with\|fn bulk_insert\|fn bulk_writer\|fn appender" crates/uni/src/api/sync.rs` → zero session write wrappers remain
- [ ] `grep -rn "fn bulk_insert_vertices\|fn bulk_insert_edges\|fn bulk_writer\|fn appender" crates/uni/src/api/transaction.rs` → all four methods EXIST
- [ ] `grep -rn "session\.execute\|session()\.execute" crates/uni/tests/` → zero results (all migrated to tx)
- [ ] `grep -rn "session\.bulk_insert\|session\.bulk_writer\|session\.appender" crates/uni/tests/` → zero results
- [ ] `grep -rn "\.execute(" bindings/uni-db/src/builders.rs` → only on Transaction, not Session
- [ ] `grep -rn "session\.execute\|session\.bulk" bindings/uni-db/tests/` → zero results (all migrated)
- [ ] `grep -rn "session\.execute\|session\.bulk" bindings/uni-db/tests/conftest.py` → zero results
- [ ] `grep -rn "_db_session\.execute" bindings/uni-pydantic/` → zero results (uses tx internally)
- [ ] Python stubs: `grep -n "def execute\|def bulk" bindings/uni-db/uni_db/__init__.pyi` → only on Transaction/AsyncTransaction classes, not Session/AsyncSession
- [ ] Full test suite: `cargo nextest run --workspace --no-fail-fast` → 0 failures
- [ ] Python full suite: `uv run pytest tests/ -v -n auto` in both uni-db and uni-pydantic → 0 failures

---

## Phase 3: Remove Writer Savepoint Machinery

**Goal**: Remove all savepoint infrastructure from Writer and NativeExecutionAdapter. ASSUME/ABDUCE will be temporarily broken (fixed in Phase 4).

### Changes

#### 3a. Remove from Writer
**File**: `crates/uni-store/src/runtime/writer.rs`
- Remove `transaction_l0: Option<Arc<RwLock<L0Buffer>>>` field (line 52)
- Remove `active_l0()` method (lines 200-204) — all mutation methods must be updated to use `l0_manager.get_current()` directly
- Remove `install_transaction_l0()` method (lines 386-390)
- Remove `force_rollback()` method
- Update all mutation methods (`insert_vertex_with_labels`, `delete_vertex`, `insert_edge`, `delete_edge`, `set_edge_type`) to use `l0_manager.get_current()` instead of `active_l0()`
- Update constraint checks that reference `transaction_l0`
- Update metrics that check `transaction_l0`

#### 3b. Remove from Executor write path
**File**: `crates/uni-query/src/query/executor/write.rs`
- Remove `install_transaction_l0()` swap pattern (lines ~1307-1331, 1345-1394)
- Mutations with `transaction_l0_override` should use `execute_ast_internal_with_tx_l0()` directly (already the case for the Transaction API path)

**File**: `crates/uni-query/src/query/df_graph/mutation_common.rs`
- Remove `install_transaction_l0()` swap pattern (lines ~548-552)

#### 3c. Remove from NativeExecutionAdapter
**File**: `crates/uni/src/api/impl_locy.rs`
- Remove `savepoint_active: AtomicBool` field
- Remove `begin_savepoint()` implementation (lines 970-987)
- Remove `rollback_savepoint()` implementation (lines 990-1005)
- These trait methods will be replaced in Phase 4

#### 3d. Remove SavepointId
**File**: `crates/uni-locy/src/result.rs` — Remove `SavepointId` struct
**File**: `crates/uni-locy/src/lib.rs` — Remove from exports

#### 3e. Update LocyExecutionContext trait
**File**: `crates/uni-query/src/query/df_graph/locy_traits.rs`
- Remove `begin_savepoint()` and `rollback_savepoint()` from trait
- Add placeholder `fork_l0()` and `restore_l0()` (can return `unimplemented!()` — Phase 4 implements them)

#### 3f. Temporarily stub ASSUME/ABDUCE
**File**: `crates/uni-query/src/query/df_graph/locy_assume.rs`
- Replace `ctx.begin_savepoint()` → `ctx.fork_l0()` (placeholder)
- Replace `ctx.rollback_savepoint()` → `ctx.restore_l0()` (placeholder)
- ASSUME/ABDUCE will panic at runtime until Phase 4 lands

**File**: `crates/uni-query/src/query/df_graph/locy_abduce.rs`
- Same changes as ASSUME

### Verification

- [ ] `cargo build --workspace` — compiles
- [ ] Tests that don't use ASSUME/ABDUCE pass
- [ ] ASSUME/ABDUCE tests are expected to fail (Phase 4 fixes them)

### Phase 3 Completion Audit

Before proceeding to Phase 4, verify EVERY item was actually completed:

- [ ] `grep -rn "transaction_l0" crates/uni-store/src/runtime/writer.rs` → zero results (field removed)
- [ ] `grep -rn "active_l0" crates/uni-store/src/runtime/writer.rs` → zero results (method removed)
- [ ] `grep -rn "install_transaction_l0" crates/uni-store/src/runtime/writer.rs` → zero results
- [ ] `grep -rn "install_transaction_l0" crates/uni-query/src/` → zero results (swap pattern removed)
- [ ] `grep -rn "force_rollback" crates/uni-store/src/runtime/writer.rs` → zero results
- [ ] `grep -rn "savepoint_active" crates/uni/src/api/impl_locy.rs` → zero results
- [ ] `grep -rn "begin_savepoint\|rollback_savepoint" crates/uni/src/api/impl_locy.rs` → zero results
- [ ] `grep -rn "SavepointId" crates/uni-locy/src/` → zero results
- [ ] `grep -rn "begin_savepoint\|rollback_savepoint" crates/uni-query/src/query/df_graph/locy_traits.rs` → zero results
- [ ] `grep -rn "fork_l0\|restore_l0" crates/uni-query/src/query/df_graph/locy_traits.rs` → both methods EXIST (placeholder)
- [ ] `grep -rn "fork_l0\|restore_l0" crates/uni-query/src/query/df_graph/locy_assume.rs` → calls exist (replacing begin/rollback_savepoint)
- [ ] `grep -rn "fork_l0\|restore_l0" crates/uni-query/src/query/df_graph/locy_abduce.rs` → calls exist
- [ ] All Writer mutation methods use `l0_manager.get_current()` directly: `grep -n "active_l0" crates/uni-store/src/runtime/writer.rs` → zero results
- [ ] `cargo build --workspace` — compiles with zero errors

---

## Phase 4: Clean L0 Hierarchy

**Goal**: Implement the clean three-tier L0 model. Session Locy L0, fork/restore for ASSUME/ABDUCE, Transaction Locy L0. Fix DERIVE visibility.

### Changes

#### 4a. Ensure `L0Buffer: Clone`
**File**: `crates/uni-store/src/runtime/l0.rs`
- Add `#[derive(Clone)]` to `L0Buffer` (or implement manually)
- All fields are HashMap/HashSet/counters/`Option<Arc<WAL>>` — all cloneable

#### 4b. Add `locy_l0` to LocyEngine and NativeExecutionAdapter

**File**: `crates/uni/src/api/impl_locy.rs`

`LocyEngine`:
```rust
pub struct LocyEngine<'a> {
    pub(crate) db: &'a UniInner,
    pub(crate) tx_l0_override: Option<Arc<RwLock<L0Buffer>>>,
    pub(crate) locy_l0: Option<Arc<RwLock<L0Buffer>>>,
    pub(crate) collect_derive: bool,
}
```

`NativeExecutionAdapter`:
```rust
struct NativeExecutionAdapter<'a> {
    db: &'a UniInner,
    native_store: &'a DerivedStore,
    compiled: &'a CompiledProgram,
    graph_ctx: Arc<GraphExecutionContext>,
    session_ctx: Arc<RwLock<SessionContext>>,
    params: HashMap<String, Value>,
    tx_l0_override: Option<Arc<RwLock<L0Buffer>>>,
    locy_l0: std::sync::Mutex<Option<Arc<RwLock<L0Buffer>>>>,
}
```

#### 4c. Session path — create ephemeral Locy L0

**File**: `crates/uni/src/api/impl_locy.rs`, `evaluate_with_db_and_config()`

```rust
let locy_l0 = if let Some(ref writer) = db.writer {
    let w = writer.read().await;
    Some(w.create_transaction_l0())
} else {
    None
};
let engine = LocyEngine {
    db,
    tx_l0_override: None,
    locy_l0,
    collect_derive: true,
};
```

#### 4d. Transaction path — locy_l0 = tx_l0

**File**: `crates/uni/src/api/transaction.rs`

```rust
let engine = LocyEngine {
    db: &self.db,
    tx_l0_override: Some(self.tx_l0.clone()),
    locy_l0: Some(self.tx_l0.clone()),
    collect_derive: false,
};
```

#### 4e. Implement `fork_l0()` and `restore_l0()`

**File**: `crates/uni/src/api/impl_locy.rs`

```rust
async fn fork_l0(&self) -> Result<Arc<RwLock<L0Buffer>>, LocyError> {
    let mut guard = self.locy_l0.lock().unwrap();
    let current = guard.as_ref().ok_or_else(|| LocyError::ExecutorError {
        message: "no active Locy L0 to fork".into(),
    })?;
    let cloned = Arc::new(parking_lot::RwLock::new(current.read().clone()));
    let previous = guard.replace(cloned).unwrap();
    Ok(previous)
}

async fn restore_l0(&self, previous: Arc<RwLock<L0Buffer>>) -> Result<(), LocyError> {
    let mut guard = self.locy_l0.lock().unwrap();
    *guard = Some(previous);
    Ok(())
}
```

#### 4f. Update `execute_mutation()` — route to locy_l0

```rust
async fn execute_mutation(&self, ast: Query, params: HashMap<String, Value>) -> Result<usize, LocyError> {
    let l0 = self.locy_l0.lock().unwrap().clone();
    if let Some(l0) = l0 {
        let before = l0.read().mutation_count;
        self.db.execute_ast_internal_with_tx_l0(
            ast, "<locy>", params, self.db.config.clone(), l0.clone(),
        ).await.map_err(...)?;
        let after = l0.read().mutation_count;
        return Ok(after.saturating_sub(before));
    }
    // Fallback: standard path
    ...
}
```

#### 4g. Update `execute_cypher_read()` — route through locy_l0

Same pattern as `execute_mutation()` — use `locy_l0` if present.

#### 4h. Update `execute_pattern()` — include locy_l0 in MATCH scans

Build L0Context with `locy_l0` for the `transaction_l0` slot.

#### 4i. Update `re_evaluate_strata()` — pass locy_l0

```rust
async fn re_evaluate_strata(&self, ...) -> Result<RowStore, LocyError> {
    let locy_l0 = self.locy_l0.lock().unwrap().clone();
    let engine = LocyEngine {
        db: self.db,
        tx_l0_override: locy_l0.clone(),
        locy_l0,
        collect_derive: false,
    };
    ...
}
```

#### 4j. DERIVE dispatch — replay mutations

```rust
if collect_derive {
    let output = collect_derive_facts(dc, program, ctx).await?;
    let affected = output.affected;
    for query in &output.queries {
        ctx.execute_mutation(query.clone(), HashMap::new()).await?;
    }
    collected_derives.push(output);
    Ok(CommandResult::Derive { affected })
}
```

#### 4k. Defer post-DERIVE inline Cypher
**File**: `crates/uni-query/src/query/df_graph/locy_program.rs` — Already done.

#### 4l. Update ASSUME to use fork/restore
**File**: `crates/uni-query/src/query/df_graph/locy_assume.rs`
```rust
let saved_l0 = ctx.fork_l0().await?;
// mutations + re_evaluate_strata + body commands
ctx.restore_l0(saved_l0).await?;
```

#### 4m. Update ABDUCE to use fork/restore
**File**: `crates/uni-query/src/query/df_graph/locy_abduce.rs`
Same pattern — fork per candidate, restore after.

### Verification

- [ ] `cargo test --test locy_derive_visibility` — all 12 pass
- [ ] `cargo test --test locy_integration` — all pass (including ASSUME rollback)
- [ ] `cargo test -p uni-locy-tck` — all pass (including `ASSUME extends reachability`)
- [ ] `cargo nextest run --workspace --no-fail-fast` — full suite green, 0 failures
- [ ] `cd bindings/uni-db && uv run maturin develop && uv run pytest tests/ -v` — all pass

### Phase 4 Completion Audit

Before proceeding to Phase 5, verify EVERY item was actually completed:

- [ ] `L0Buffer` is cloneable: `grep -n "derive.*Clone\|impl Clone" crates/uni-store/src/runtime/l0.rs` → Clone derived or implemented
- [ ] `grep -rn "locy_l0" crates/uni/src/api/impl_locy.rs` → field exists on BOTH LocyEngine and NativeExecutionAdapter
- [ ] `grep -rn "fork_l0\|restore_l0" crates/uni/src/api/impl_locy.rs` → real implementations (not `unimplemented!()`)
- [ ] `grep -rn "locy_l0" crates/uni/src/api/transaction.rs` → tx path passes `locy_l0: Some(self.tx_l0.clone())`
- [ ] Session path creates ephemeral L0: `grep -n "create_transaction_l0" crates/uni/src/api/impl_locy.rs` → present in `evaluate_with_db_and_config`
- [ ] `grep -rn "savepoint_active" crates/` → zero results anywhere
- [ ] `grep -rn "begin_savepoint\|rollback_savepoint" crates/` → zero results anywhere
- [ ] `grep -rn "SavepointId" crates/` → zero results anywhere
- [ ] DERIVE trailing Cypher works: `cargo test --test locy_derive_visibility test_session_trailing_cypher_sees_derive_edges` → PASS
- [ ] ASSUME works: `cargo test --test locy_integration test_assume_rollback` → PASS
- [ ] ASSUME+ABDUCE TCK: `cargo test -p uni-locy-tck -- "ASSUME extends reachability"` → PASS
- [ ] No ephemeral L0 leak: `cargo test --test locy_derive_visibility test_session_derive_does_not_leak` → PASS
- [ ] Tx DERIVE works: `cargo test --test locy_derive_visibility test_tx_trailing_cypher_sees_derive_edges` → PASS
- [ ] Full Rust suite: `cargo nextest run --workspace --no-fail-fast` → 0 failures
- [ ] Full Python suite: `uv run pytest tests/ -v -n auto` in uni-db → 0 failures

---

## Phase 5: Migrate Notebook Generators

**Goal**: Update all notebook generators and generated notebooks to use the new Transaction-only write API.

### Changes

#### 5a. Update generators
All Python notebook generators must change:
- `session.execute("CREATE ...")` → `tx = session.tx(); tx.execute("CREATE ..."); tx.commit()`
- `session.bulk_writer()` → `tx = session.tx(); tx.bulk_writer()` (or equivalent)
- Data ingestion wrapped in transactions

**Files**:
- `bindings/uni-db/examples/generate_notebooks.py`
- `bindings/uni-pydantic/examples/generate_notebooks.py`
- `website/scripts/generate_locy_notebooks.py`
- `website/scripts/generate_cyber_flagship_notebook.py`
- `website/scripts/generate_pharma_flagship_notebook.py`
- `website/scripts/generate_semiconductor_flagship_notebook.py`

#### 5b. Locy notebooks — session.locy() API
- `session.locy()` with trailing Cypher now works (Phase 4 fixed it)
- `command_results` use typed objects (`.command_type`, `.rows`, `.affected`, etc.)
- DERIVE + trailing Cypher works in session context
- `result.derived_fact_set` for optional `tx.apply()` persistence

#### 5c. Regenerate and validate all notebooks
- Run all generators
- Run `bindings/uni-db/examples/run_notebooks.py`
- Run `website/scripts/verify_*_flagship_notebook.py`

### Note
Much of this work was already done earlier in this conversation (generators updated, notebooks regenerated). Phase 5 will need a second pass after Phase 2 changes the API (removing `session.execute()`, `session.bulk_writer()`).

### Verification

- [ ] All generators run without errors
- [ ] `uv run python bindings/uni-db/examples/run_notebooks.py` — all 5 notebooks pass
- [ ] `uv run python website/scripts/verify_semiconductor_flagship_notebook.py` — pass
- [ ] `uv run python website/scripts/verify_pharma_flagship_notebook.py` — pass
- [ ] `uv run python website/scripts/verify_cyber_flagship_notebook.py` — pass

### Phase 5 Completion Audit

Final audit — verify the entire refactor is complete:

- [ ] `grep -rn "Database\.open\|db\.query\|db\.execute\|db\.locy\|db\.bulk_writer\|db\.create_vector_index" bindings/*/examples/generate_notebooks.py website/scripts/generate_*.py` → zero results (all old API patterns gone)
- [ ] `grep -rn "session\.execute\|session\.bulk" bindings/*/examples/generate_notebooks.py website/scripts/generate_*.py` → zero results (no session writes in generators)
- [ ] All generated .ipynb files are up to date: `python website/scripts/generate_*_notebook.py --check` → no drift detected
- [ ] All 5 uni-db example notebooks execute successfully
- [ ] All 3 flagship notebooks pass verification
- [ ] Locy notebooks generate without errors (Python and Rust)
- [ ] Final full test suite: `cargo nextest run --workspace --no-fail-fast` → 0 failures
- [ ] Final Python suite: `cd bindings/uni-db && uv run pytest tests/ -v -n auto` → 0 failures
- [ ] Final pydantic suite: `cd bindings/uni-pydantic && uv run pytest tests/ -v` → 0 failures

---

## Phase Dependencies

```
Phase 1 ──→ Phase 2 ──→ Phase 3 ──┐
                                    ├──→ Phase 5
                        Phase 4 ──┘
                        (3+4 land together)
```

- Phase 1 is independent
- Phase 2 depends on Phase 1 (batch_upsert fixed)
- Phase 3+4 must land together (ASSUME broken between them)
- Phase 5 depends on Phase 2 (write API changed) and Phase 4 (DERIVE visibility)

## Risk Summary

| Phase | Risk | Mitigation |
|-------|------|-----------|
| 1 | Grammar change breaks Cypher parsing | Only removes 3 keywords; comprehensive parser tests |
| 2 | ~1,400 test call sites to migrate | Mechanical transformation; can be scripted |
| 2 | Python API breaking change | Clean removal, no deprecation period (per requirement) |
| 3 | Writer mutation routing changes | `active_l0()` → `l0_manager.get_current()` is simpler, not riskier |
| 3+4 | ASSUME/ABDUCE broken between phases | Land together; never merge Phase 3 without Phase 4 |
| 4 | `L0Buffer::clone()` cost for ABDUCE | L0 is small; profile if needed |
| 5 | Notebooks need second migration pass | Already done once; patterns established |
