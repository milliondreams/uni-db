# Rule Semantics

## Evaluation Pipeline

1. Parse Locy program.
2. Build dependency graph.
3. Validate types/schema compatibility.
4. Stratify rules.
5. Evaluate each stratum to fixpoint.
6. Execute command phase (`QUERY`, `DERIVE`, `EXPLAIN`, `ABDUCE`, `ASSUME` body).

## Two-Phase Execution

Locy execution is split into two distinct phases with different execution engines:

**Phase 1 — Strata Evaluation (DataFusion)**

Rules compile to DataFusion `LogicalPlan` nodes. The query engine runs them through a fixpoint loop per stratum. Expression functions (`similar_to()`, etc.) have full access to storage, schema, and the Xervo embedding runtime.

**Phase 2 — Command Dispatch (Row-Level)**

After strata converge, commands (`QUERY`, `DERIVE`, `ABDUCE`, `ASSUME`) execute on materialized `Vec<Row>` facts. WHERE filters use a lightweight row-level evaluator. This path supports vector cosine similarity but not auto-embedding, FTS, or multi-source fusion.

| Context | Execution | Vector | Auto-Embed | FTS |
|---------|-----------|--------|------------|-----|
| Rule `MATCH ... WHERE/YIELD` | DataFusion | ✓ | ✓ | ✓ |
| Rule `ALONG / FOLD` | DataFusion | ✓ | ✓ | ✓ |
| `DERIVE ... WHERE` | In-memory | ✓ | ✗ | ✗ |
| `ABDUCE ... WHERE` | In-memory | ✓ | ✗ | ✗ |
| `ASSUME ... WHERE` | In-memory | ✓ | ✗ | ✗ |

## Semi-Naive Evaluation

Within a recursive stratum, Locy only re-evaluates rules using *newly derived* facts (the delta) rather than all known facts each iteration. This provides exponential speedup for transitive closures:

```
Iteration 0: delta₀ = base facts from MATCH
Iteration 1: delta₁ = evaluate(rules, delta₀) − known_facts
Iteration 2: delta₂ = evaluate(rules, delta₁) − known_facts
...
Iteration n: deltaₙ = ∅ → fixpoint reached
```

## Overloaded Rules

Multiple `CREATE RULE` clauses sharing one name define one logical relation. Clauses can be prioritized where supported.

## Negation Rules

`IS NOT` requires stratification-safe dependencies. Cyclic negation is rejected at compile time.

If the referenced rule exposes a `PROB` column, `IS NOT` becomes probabilistic complement (`1 - p`) rather than Boolean anti-join. Rules without a `PROB` column keep standard Boolean negation.

## Monotonic Recursion

Recursive aggregation requires monotonic operators where specified. Non-monotonic recursive shapes are rejected.

`MNOR` and `MPROD` are monotonic and therefore legal inside recursive strata. They assume independent derivations unless `exact_probability` is enabled.

## Determinism

`BEST BY` can use deterministic tie-breaking through config (`deterministic_best_by = true`).

## Limits and Guardrails

Key guardrails come from `LocyConfig`:

- `max_iterations`
- `timeout`
- `max_derived_bytes`
- `max_explain_depth`
- `max_slg_depth`
- `strict_probability_domain`
- `probability_epsilon`
- `exact_probability`
- `max_bdd_variables`

See [Errors & Limits](reference/errors-limits.md) for operational guidance.
