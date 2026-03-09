# Locy Architecture (Internals)

## Scope

This page describes how Locy is integrated into Uni from parser to runtime execution.

## End-to-End Flow

1. Parse Locy source into Locy AST (`parse_locy`).
2. Compile into stratified `CompiledProgram`.
3. Build logical program plan (`LocyPlanBuilder`).
4. Produce physical execution plan (`LocyProgramExec` and related operators).
5. Execute strata to fixpoint (Phase 1 — DataFusion).
6. Dispatch command phase — `QUERY`, `EXPLAIN`, `ABDUCE`, `DERIVE`, `ASSUME` (Phase 2 — row-level).
7. Map outputs into `LocyResult`.

## Two-Phase Execution

Locy programs execute in two phases with different engines:

**Phase 1 — Strata Evaluation (DataFusion):**

- `LocyProgramExec` is a DataFusion `ExecutionPlan` that coordinates stratum execution.
- Rules compile to `LogicalPlan` nodes (Scan → Filter → Projection) and are physically planned via `CypherPhysicalExprCompiler`.
- Recursive strata use `FixpointExec` for semi-naive fixpoint iteration.
- Expression functions (including `similar_to()`) have full access to `GraphExecutionContext` — storage, schema, embedding models.
- Converged facts are stored in `DerivedStore` as `RecordBatch`es.

**Phase 2 — Command Dispatch (Row-Level):**

- Commands operate on converged facts converted to `Vec<Row>`.
- `DERIVE` iterates rows and generates Cypher CREATE/MERGE mutations.
- `ABDUCE` uses savepoint-rollback loops for counterfactual reasoning.
- `ASSUME` tests hypothetical mutations and re-evaluates strata.
- WHERE filters use `eval_expr()`, a lightweight row-level evaluator.
- Expression functions are limited to pure computation (e.g., vector cosine for `similar_to()`).

!!! note "Expression capabilities differ by phase"
    Rule `WHERE` and `YIELD` expressions run in DataFusion (Phase 1) with full capability — auto-embedding, FTS search, multi-source fusion. Command `WHERE` filters run in the row-level evaluator (Phase 2) with vector-only `similar_to()`.

## Key Components

- `uni-cypher` for Locy grammar and AST.
- `uni-locy` for compiler and command/result types.
- `uni` API integration via `LocyEngine`.
- `uni-query` DataFusion path for native execution and derived-store handling.

## Context Availability

| Component | `GraphExecutionContext` | `SessionContext` | Used For |
|-----------|------------------------|-------------------|----------|
| `LocyProgramExec` | Yes | Yes | Strata evaluation, graph scans |
| `FixpointExec` | Yes | Yes | Recursive fixpoint iteration |
| `NativeExecutionAdapter` | Yes | Yes | `execute_mutation()`, `re_evaluate_strata()` |
| `eval_expr()` / `eval_function()` | Not threaded through | No | Command WHERE filters |

The `NativeExecutionAdapter` holds both contexts and uses them for mutations and strata re-evaluation (ABDUCE/ASSUME). However, these contexts are not currently passed to `eval_expr()`, which is why command WHERE expressions have limited function support.

## Why This Shape

- Reuse Uni query engine and storage infrastructure.
- Keep semantics explicit in compiler phases.
- Preserve explainability and bounded execution controls.
- Commands are not queries — they perform mutations and hypothetical reasoning on converged facts, so row-level dispatch is natural.
