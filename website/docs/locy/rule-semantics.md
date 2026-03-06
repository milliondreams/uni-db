# Rule Semantics

## Evaluation Pipeline

1. Parse Locy program.
2. Build dependency graph.
3. Validate types/schema compatibility.
4. Stratify rules.
5. Evaluate each stratum to fixpoint.
6. Execute command phase (`QUERY`, `DERIVE`, `EXPLAIN`, `ABDUCE`, `ASSUME` body).

## Overloaded Rules

Multiple `CREATE RULE` clauses sharing one name define one logical relation. Clauses can be prioritized where supported.

## Negation Rules

`IS NOT` requires stratification-safe dependencies. Cyclic negation is rejected at compile time.

## Monotonic Recursion

Recursive aggregation requires monotonic operators where specified. Non-monotonic recursive shapes are rejected.

## Determinism

`BEST BY` can use deterministic tie-breaking through config (`deterministic_best_by = true`).

## Limits and Guardrails

Key guardrails come from `LocyConfig`:

- `max_iterations`
- `timeout`
- `max_derived_bytes`
- `max_explain_depth`
- `max_slg_depth`

See [Errors & Limits](reference/errors-limits.md) for operational guidance.
