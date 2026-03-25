# Locy Native Execution Internals

## Runtime Overview

Locy native execution uses a two-phase architecture: DataFusion-backed strata evaluation followed by row-level command dispatch.

## Phase 1: Strata Evaluation

1. `LocyPlanBuilder` builds the logical program plan from compiled strata.
2. Hybrid planner maps logical nodes to physical operators via `CypherPhysicalExprCompiler`.
3. `LocyProgramExec` coordinates stratum execution as a DataFusion `ExecutionPlan`.
4. Non-recursive strata execute in a single pass; recursive strata use `FixpointExec` with semi-naive evaluation.
5. Converged facts accumulate in `DerivedStore` as `RecordBatch`es.

Expression functions in rule `WHERE` and `YIELD` clauses have full `GraphExecutionContext` access:

- `similar_to()` supports metric-aware vector scoring (Cosine, L2, Dot), auto-embedding, FTS/BM25, and multi-source fusion.
- `EXISTS` subqueries run as correlated DataFusion subplans.
- All DataFusion optimizations apply (pushdown, projection pruning, parallel execution).

## Phase 2: Command Dispatch

After strata converge, `dispatch_native_command()` processes each command:

| Command | Operation | Uses DataFusion? |
|---------|-----------|-----------------|
| `QUERY` | SLG goal resolution | Indirectly (via strata) |
| `DERIVE` | Iterate facts → generate mutations | No (row-level) |
| `ABDUCE` | Savepoint → mutate → re-evaluate → check → rollback | Yes (re-evaluates strata) |
| `ASSUME` | Savepoint → mutate → re-evaluate → dispatch body | Yes (re-evaluates strata) |
| `EXPLAIN RULE` | Build derivation tree | No (traces fact provenance) |

Command WHERE filters use `eval_expr()`, a lightweight row-level evaluator. This evaluator supports basic operations (arithmetic, comparison, boolean logic) and pure-computation functions (`similar_to()` for vector similarity only — defaults to cosine). It does not have access to `GraphExecutionContext`, so auto-embedding, FTS, metric resolution, and multi-source fusion are not available in command WHERE clauses.

## Semantics Hooks

- Fixpoint iteration counting.
- Peak memory tracking for derived relations.
- Derivation tracking for `EXPLAIN RULE`.
- Savepoint-aware behavior for `ASSUME`/`ABDUCE` paths.

## Performance Considerations

- Iteration count and branching factor dominate runtime.
- Result materialization and enrichment impact memory.
- Constrained goals and bounded abduction improve predictability.
- ABDUCE validates each candidate by re-evaluating all strata, so cost is proportional to candidates × strata evaluation time.
