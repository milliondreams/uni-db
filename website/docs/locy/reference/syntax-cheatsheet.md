# Locy Syntax Cheatsheet

## Rule

```cypher
CREATE RULE name [PRIORITY n] AS
MATCH ...
[WHERE ...]                         -- pre-aggregation filter
[ALONG x = expr]
[FOLD agg = aggregate(expr)]
[WHERE agg_condition]               -- post-FOLD filter (HAVING)
[BEST BY expr ASC|DESC]
YIELD KEY a, value AS alias, prob_expr AS PROB
-- OR, for graph mutation rules:
DERIVE (src)-[:TYPE]->(dst) [SET property = expr]
```

The second `WHERE` (after `FOLD`) filters on aggregated values — equivalent to SQL's `HAVING`. It can reference FOLD output columns and KEY columns.

### FOLD Aggregators

| Operator | Semantics | Use In Recursion? |
|----------|-----------|-------------------|
| `COUNT(*)` / `COUNT(expr)` | Row count | Non-recursive only |
| `SUM(expr)` | Arithmetic sum | Non-recursive only |
| `AVG(expr)` | Arithmetic mean | Non-recursive only |
| `MIN(expr)` | Minimum value | Non-recursive only |
| `MAX(expr)` | Maximum value | Non-recursive only |
| `COLLECT(expr)` | Collect into list | Non-recursive only |
| `MSUM(expr)` | Monotonic sum (non-decreasing) | Safe in recursion |
| `MMAX(expr)` | Monotonic maximum | Safe in recursion |
| `MMIN(expr)` | Monotonic minimum | Safe in recursion |
| `MCOUNT(expr)` | Monotonic count | Safe in recursion |
| `MNOR(expr)` | Noisy-OR probability: `1 − ∏(1 − pᵢ)` | Safe in recursion |
| `MPROD(expr)` | Product probability: `∏ pᵢ` | Safe in recursion |

Standard aggregators (`SUM`, `MAX`, etc.) can decrease between fixpoint iterations, which violates the monotonicity required for safe recursive evaluation. Use the `M`-prefixed monotonic variants in recursive strata.

## Goal Query

```cypher
QUERY name [WHERE ...] [RETURN ...]
```

## Derive Command

```cypher
DERIVE name [WHERE ...]
```

## Explain

```cypher
EXPLAIN RULE name [WHERE ...]
```

## Assume

```cypher
ASSUME { <cypher mutations> } THEN { <locy/cypher body> }
```

## Abduce

```cypher
ABDUCE [NOT] name [WHERE ...] [RETURN ...]
```

## Neural Predicates

```cypher
CREATE MODEL name AS
  INPUT (binding [: Label])
  [FEATURES feature_expr (, feature_expr)*]
  [FEATURES (subject, column) FROM source_rule]
  OUTPUT (PROB | SCORE | LABEL | VECTOR) result_name
  USING xervo('provider_alias' [, embedder = 'embed_alias'])
  [CALIBRATION (platt_scaling | isotonic_regression | temperature_scaling | beta_calibration | dirichlet_calibration)]
  [VERSION 'string']

CALIBRATE model_name USING calibration_method
VALIDATE  model_name USING metric (, metric)*
```

`metric` ∈ `brier | ece | accuracy | log_loss | auroc`. The classifier-registry key is the `CREATE MODEL <name>`, not the `USING xervo('alias')` provider hint. The feature dict the callable receives is keyed by the `INPUT` binding name; values are the evaluated argument expressions at the call site. See [Neural Predicates](../advanced/neural-predicates.md).

## Modules

```cypher
MODULE my.module
USE shared.rules
```
