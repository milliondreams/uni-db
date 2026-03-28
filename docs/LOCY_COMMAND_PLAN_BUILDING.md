# Locy Command Plan Building: Migration to Native DataFusion Execution

## Status: Design Review

## 1. Current Architecture

### What Is DataFusion-Native Today

Strata (rules + clauses) run entirely inside DataFusion:

```
LogicalPlan::LocyProgram
  └─ LocyProgramExec (DataFusion ExecutionPlan)
       └─ run_program()
            ├─ Recursive strata  → FixpointExec (DataFusion ExecutionPlan)
            │                       └─ execute_subplan() per clause (DataFusion plan + collect)
            ├─ Non-recursive     → execute_subplan() + anti-join + PROB multiply
            ├─ Post-fixpoint     → FOLD / BEST BY / PRIORITY (DataFusion operators)
            └─ Output            → DerivedStore (HashMap<String, Vec<RecordBatch>>)
```

All clause bodies (`MATCH ... WHERE ...`), IS-ref joins, complement semantics,
probability multiplication, and aggregation operators are DataFusion physical
plans executed via `HybridPhysicalPlanner::plan()` + `collect_all_partitions()`.

### What Runs Outside DataFusion Today

After `Executor::collect_batches()` returns, the orchestrator in `impl_locy.rs`
extracts the `DerivedStore` from a shared slot and dispatches commands in a
plain Rust loop:

```
evaluate_compiled_with_config()
  ├─ Executor::collect_batches(exec_plan)        ← DataFusion
  ├─ native_store = derived_store_slot.take()     ← Slot handoff
  ├─ NativeExecutionAdapter::new(&native_store)   ← Bridge
  └─ for cmd in commands:
       dispatch_native_command(cmd, ctx)           ← Rust loop, NOT DataFusion
```

The 6 command types dispatched outside DataFusion:

| Command | Handler | What It Does |
|---------|---------|--------------|
| `QUERY` | `locy_query::evaluate_query()` | SLG goal-directed resolution on DerivedStore |
| `DERIVE` | `locy_derive::derive_command()` | Reads facts, generates Cypher CREATE, executes mutations |
| `ASSUME` | `locy_assume::evaluate_assume()` | Savepoint → mutations → re-evaluate strata → body commands → rollback |
| `EXPLAIN` | `locy_explain::explain_rule()` | Provenance traversal or SLG re-execution fallback |
| `ABDUCE` | `locy_abduce::evaluate_abduce()` | EXPLAIN → candidate generation → savepoint validation loop |
| `Cypher` | `ctx.execute_cypher_read()` | Passthrough to Cypher executor |

### The Handoff: DerivedStore Slot

The `LocyProgramExec` stores results in `derived_store_slot: Arc<StdRwLock<Option<DerivedStore>>>`.
After DataFusion execution, the orchestrator `.take()`s ownership and creates a
`NativeExecutionAdapter` that implements `DerivedFactSource` + `LocyExecutionContext`
traits for command handlers to read facts and execute mutations.

---

## 2. What "Command Plan Building" Means

Moving commands inside DataFusion means each command becomes a `LogicalPlan` node
that the physical planner converts to an `ExecutionPlan`. Instead of the
orchestrator dispatching commands after strata, `run_program()` would execute
commands as part of the same DataFusion plan.

### Target Architecture

```
LogicalPlan::LocyProgram
  └─ LocyProgramExec
       └─ run_program()
            ├─ Strata evaluation (unchanged)
            ├─ LocyQueryExec        ← NEW: QUERY as DataFusion node
            ├─ LocyDeriveExec       ← NEW: DERIVE as DataFusion node
            ├─ LocyAssumeExec       ← NEW: ASSUME as DataFusion node
            ├─ LocyExplainExec      ← NEW: EXPLAIN as DataFusion node
            ├─ LocyAbduceExec       ← NEW: ABDUCE as DataFusion node
            └─ CypherPassthroughExec← NEW: Cypher as DataFusion node
```

---

## 3. Per-Command Feasibility Analysis

### 3.1 QUERY (GoalQuery) — Feasibility: HIGH

**Current implementation** (`locy_query.rs:27-88`):
- Creates `SLGResolver` from `DerivedFactSource`
- Calls `resolver.resolve_goal(rule_name, goal_bindings)`
- Applies WHERE filter + RETURN projection/ordering/skip/limit
- Returns `Vec<FactRow>`

**Migration path**:
- `LocyQueryExec` scans the `DerivedStore` for the named rule
- WHERE filtering → DataFusion `FilterExec`
- RETURN projection → DataFusion `ProjectionExec`
- ORDER BY / SKIP / LIMIT → DataFusion `SortExec` + `LocalLimitExec`
- SLG resolution is already backed by `execute_pattern()` which returns `RecordBatch`

**Touch points**:
- `locy_query.rs` — rewrite `evaluate_query` as `LocyQueryExec::execute()`
- `locy_planner.rs:build_commands()` — build `LogicalPlan::LocyQuery` node
- `df_planner.rs` — add physical plan case for `LogicalPlan::LocyQuery`
- `planner.rs` — add `LocyQuery` variant to `LogicalPlan` enum
- `planner_locy_types.rs` — update or remove `LocyCommand::GoalQuery`
- `locy_program.rs:run_program()` — execute LocyQueryExec inline after strata

**Estimated complexity**: Medium. SLG resolution complicates direct scan — may
need to keep the SLG path as a DataFusion table provider or rewrite as a
scan-filter-project plan on `DerivedStore` entries.

---

### 3.2 DERIVE — Feasibility: MEDIUM

**Current implementation** (`locy_derive.rs:38-119`):
- Reads facts via `ctx.lookup_derived_enriched(rule_name)`
- Applies WHERE filter
- For each matching fact, generates Cypher CREATE ASTs via `build_derive_create()`
- Executes mutations via `ctx.execute_mutation(query)`

**Migration path**:
- `LocyDeriveExec` reads from `DerivedStore`, filters, and materializes mutations
- The mutation execution (`execute_mutation`) requires `Writer` access, which is
  available in the graph context but not naturally a DataFusion stream operation
- Two sub-modes: `collect_derive` (session path: deferred) vs `execute` (tx path: immediate)

**Touch points**:
- `locy_derive.rs` — rewrite as `LocyDeriveExec`
- `locy_planner.rs:build_commands()` — build plan node
- `df_planner.rs` — physical plan case
- `planner.rs` — `LocyDerive` logical plan variant
- `locy_program.rs:run_program()` — execute inline

**Complication**: Mutations are side effects. DataFusion plans are typically
pure data transforms. Modeling mutations as a DataFusion node requires careful
handling of:
- Writer lock acquisition during plan execution
- Transaction L0 routing
- Error propagation for constraint violations
- The `collect_derive` vs `execute` mode split

**Estimated complexity**: Medium-High. The mutation side-effect makes this the
hardest command to model as a pure DataFusion plan.

---

### 3.3 ASSUME — Feasibility: LOW

**Current implementation** (`locy_assume.rs:24-95`):
1. `ctx.begin_savepoint()`
2. Execute mutations (Cypher clauses)
3. `ctx.re_evaluate_strata(program, config)` — full strata re-run
4. Dispatch body commands (recursive: can contain QUERY, DERIVE, EXPLAIN, ABDUCE, nested ASSUME)
5. `ctx.rollback_savepoint()`

**Migration challenges**:
- **Savepoint/rollback** requires Writer transaction control, not naturally
  expressible as a DataFusion operator
- **Re-evaluate strata** means creating a *new* `LocyProgramExec` inside the
  current one — recursive DataFusion plan execution
- **Body command dispatch** is itself recursive (ASSUME can nest ASSUME)
- **Rollback semantics** mean the plan has no durable output — it's a
  hypothetical evaluation

**Touch points**: Would require all of the above plus recursive plan execution
machinery.

**Recommendation**: **Keep orchestrator-dispatched.** ASSUME's transactional
savepoint/rollback and recursive re-evaluation semantics are fundamentally
imperative control flow, not data flow. Forcing them into a DataFusion plan
would add complexity without improving performance or observability.

---

### 3.4 EXPLAIN — Feasibility: LOW-MEDIUM

**Current implementation** (`locy_explain.rs:193-615`):
- **Mode A (provenance)**: Traverses `ProvenanceStore` annotations, builds `DerivationNode` tree
- **Mode B (re-execution)**: Creates `SLGResolver`, re-executes goal, recursively
  builds derivation tree with cycle detection

**Migration challenges**:
- Output is `DerivationNode` (recursive tree), not `RecordBatch` — doesn't fit
  the columnar stream model
- Provenance traversal is graph walk with cycle detection — not a scan/filter/project
- SLG re-execution fallback is inherently recursive

**Touch points**: Would require serializing `DerivationNode` into a RecordBatch
(e.g., JSON column), which loses the structure.

**Recommendation**: **Keep orchestrator-dispatched.** The tree-structured output
and recursive traversal don't benefit from DataFusion's columnar execution.

---

### 3.5 ABDUCE — Feasibility: LOW

**Current implementation** (`locy_abduce.rs:32-127`):
1. Call `explain_rule()` to build derivation tree
2. Extract candidates (addition/removal modifications)
3. For each candidate:
   - `begin_savepoint()`
   - Apply mutation
   - `re_evaluate_strata()` — full strata re-run
   - Check if goal holds/doesn't hold
   - `rollback_savepoint()`

**Migration challenges**:
- Depends on EXPLAIN (tree output)
- Multiple savepoint/rollback cycles (one per candidate)
- Each validation requires a full strata re-evaluation
- Inherently serial: each candidate must be validated independently

**Recommendation**: **Keep orchestrator-dispatched.** Same transactional
concerns as ASSUME, multiplied by the number of candidates.

---

### 3.6 Cypher Passthrough — Feasibility: HIGH (already exists)

**Current implementation**: `ctx.execute_cypher_read(ast)` — delegates to the
Cypher executor which already produces DataFusion plans.

**Migration path**: The Cypher query is already a `LogicalPlan` that goes
through the standard planner. Could be inlined as a subplan in `run_program()`.

**Touch points**:
- `locy_planner.rs` — plan the Cypher AST as a subplan
- `locy_program.rs` — execute via `execute_subplan()`

**Estimated complexity**: Low.

---

## 4. Recommended Migration Plan

### Phase A: QUERY + Cypher (High Value, Low Risk)

These two commands are pure reads on existing data. They fit naturally into the
DataFusion model.

#### A.1 — LocyQueryExec

**New files:**
- `crates/uni-query/src/query/df_graph/locy_query_exec.rs`

**Modified files:**

| File | Change |
|------|--------|
| `planner.rs` | Add `LocyQuery { rule_name, where_expr, return_clause, ... }` to `LogicalPlan` enum |
| `locy_planner.rs:build_commands()` | For `GoalQuery`, build `LogicalPlan::LocyQuery` instead of wrapping in `LocyCommand` |
| `df_planner.rs` | Add case for `LogicalPlan::LocyQuery` → `LocyQueryExec` |
| `locy_program.rs:run_program()` | After strata loop, execute `LocyQueryExec` plans and store results |
| `impl_locy.rs` | Remove `GoalQuery` from `dispatch_native_command` match; read results from exec output |
| `planner_locy_types.rs` | Remove `LocyCommand::GoalQuery` variant (or keep for backwards compat) |

**Implementation sketch for `LocyQueryExec`:**
```rust
struct LocyQueryExec {
    rule_name: String,
    where_filter: Option<LogicalPlan>,  // WHERE as a filter subplan
    return_clause: ReturnClause,         // Projection + ORDER BY + SKIP/LIMIT
    derived_store: Arc<StdRwLock<Option<DerivedStore>>>,
    output_schema: SchemaRef,
    properties: PlanProperties,
}

impl ExecutionPlan for LocyQueryExec {
    fn execute(&self, partition: usize, ctx: Arc<TaskContext>)
        -> DFResult<SendableRecordBatchStream>
    {
        // 1. Read batches from derived_store for rule_name
        // 2. Apply WHERE filter via execute_subplan or in-memory filter
        // 3. Apply RETURN projection
        // 4. Apply ORDER BY + SKIP + LIMIT
        // 5. Return stream
    }
}
```

#### A.2 — Cypher Passthrough

Inline the Cypher AST as a subplan in `run_program()`:
```rust
// In run_program(), after strata loop:
for cmd in &commands {
    if let LocyCommand::Cypher { query } = cmd {
        let plan = planner.plan_cypher(query)?;
        let batches = execute_subplan(&plan, ...)?;
        // Store in command_results slot
    }
}
```

### Phase B: DERIVE (Medium Value, Medium Risk)

DERIVE is a write operation. Model it as a two-phase exec:
1. **Read phase**: Scan DerivedStore, filter, project — pure DataFusion
2. **Write phase**: Materialize mutations via Writer — side-effect node

The read phase can be a DataFusion plan. The write phase uses the existing
`Writer` infrastructure accessed through `GraphExecutionContext`.

**This phase can be deferred** until Phase A is validated.

### Phase C: ASSUME / EXPLAIN / ABDUCE — Not Recommended

These commands have fundamental characteristics that make DataFusion plan
integration counterproductive:

- **ASSUME**: Transactional savepoint/rollback + recursive strata re-evaluation
- **EXPLAIN**: Tree-structured output + recursive provenance traversal
- **ABDUCE**: Multiple savepoint cycles + serial candidate validation

The orchestrator dispatch pattern is the right model for these — they are
control flow, not data flow.

---

## 5. Benefits of Migration

| Benefit | QUERY | DERIVE | ASSUME/EXPLAIN/ABDUCE |
|---------|-------|--------|----------------------|
| Unified profiling (DataFusion operator metrics) | Yes | Yes | N/A |
| Timeout/cancellation propagation | Yes | Yes | Already handled by orchestrator |
| DataFusion optimizer visibility | Yes | Partial | N/A |
| Reduced code (eliminate dispatch loop) | Partial | Partial | No |
| Zero-copy columnar output | Yes | N/A (mutations) | N/A |

---

## 6. Risks

1. **SLG Resolution**: QUERY currently uses `SLGResolver` for goal-directed
   evaluation. If we replace it with a direct DerivedStore scan, we lose
   demand-driven evaluation (SLG only computes what's needed for the goal).
   For small result sets with selective WHERE clauses, this could be slower.
   **Mitigation**: Keep SLG as a table provider that DataFusion can push
   predicates into.

2. **DERIVE Side Effects**: DataFusion plans are pure transforms. Mutations
   during plan execution break assumptions about plan re-executability and
   partition independence. **Mitigation**: Use a single-partition exec that
   acquires the Writer lock, similar to how DDL procedures work today.

3. **Result Type Mismatch**: Commands return heterogeneous types
   (`Vec<FactRow>`, `DerivationNode`, `AbductionResult`, `usize`). DataFusion
   streams produce `RecordBatch`. We'd need to serialize structured results
   into batches or use a side-channel slot (like the existing
   `derived_store_slot` pattern). **Mitigation**: Use the slot pattern for
   non-tabular results.

4. **Testing Surface**: Each new `ExecutionPlan` impl needs unit tests for
   schema, partitioning, metrics, and error handling. The existing command
   handler tests would need to be adapted.

---

## 7. Estimated Effort

| Phase | Scope | New Code | Modified Files | Effort |
|-------|-------|----------|----------------|--------|
| A.1 | LocyQueryExec | ~300 LOC | 6 files | 2-3 days |
| A.2 | Cypher inline | ~50 LOC | 2 files | 0.5 days |
| B | LocyDeriveExec | ~400 LOC | 6 files | 3-4 days |
| C | ASSUME/EXPLAIN/ABDUCE | Not recommended | — | — |

**Total recommended**: Phase A (2.5-3.5 days) + Phase B (3-4 days) = **~1 week**

Phase A can be shipped independently and validated before committing to Phase B.

---

## 8. Decision Matrix

| Command | Migrate? | Rationale |
|---------|----------|-----------|
| **QUERY** | **Yes** | Pure read on DerivedStore; natural fit for scan/filter/project |
| **Cypher** | **Yes** | Already a DataFusion plan; just inline it |
| **DERIVE** | **Maybe** | Read phase fits; write phase needs careful side-effect handling |
| **ASSUME** | **No** | Savepoint/rollback + recursive re-evaluation = imperative control flow |
| **EXPLAIN** | **No** | Tree output + recursive traversal doesn't fit columnar model |
| **ABDUCE** | **No** | Serial savepoint validation loop, depends on EXPLAIN |
