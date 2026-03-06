# Locy Native Execution Internals

## Runtime Overview

Locy native execution compiles to Uni's DataFusion-backed execution path with Locy-specific planning and operators.

## Pipeline

1. `LocyPlanBuilder` builds logical program plan.
2. Hybrid planner maps logical nodes to physical operators.
3. `LocyProgramExec` coordinates stratum execution.
4. Derived facts are accumulated and exposed for command resolution.
5. Command dispatch maps engine output to user-visible `LocyResult`.

## Semantics Hooks

- Fixpoint iteration counting.
- Peak memory tracking for derived relations.
- Derivation tracking for `EXPLAIN RULE`.
- Savepoint-aware behavior for `ASSUME`/`ABDUCE` paths.

## Performance Considerations

- Iteration count and branching factor dominate runtime.
- Result materialization and enrichment impact memory.
- Constrained goals and bounded abduction improve predictability.
