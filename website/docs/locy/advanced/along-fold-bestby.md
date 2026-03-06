# Advanced: ALONG, FOLD, BEST BY

## ALONG (Path-Carried Values)

`ALONG` carries state through recursive expansion.

```cypher
CREATE RULE shortest AS
MATCH (a)-[e:EDGE]->(b)
ALONG dist = prev.dist + e.weight
YIELD KEY a, KEY b, dist
```

Use `prev.<field>` to reference prior recursive step values.

## FOLD (Aggregation)

`FOLD` aggregates rule outputs after derivation.

```cypher
CREATE RULE totals AS
MATCH (a)-[:EDGE]->(b)
FOLD total = SUM(b.value)
YIELD KEY a, total
```

For recursion, monotonic variants are used where required.

## BEST BY (Witness Selection)

`BEST BY` picks the best candidate row by ordering expression.

```cypher
CREATE RULE cheapest AS
MATCH (a)-[e:EDGE]->(b)
ALONG cost = prev.cost + e.weight
BEST BY cost ASC
YIELD KEY a, KEY b, cost
```

## Practical Guidance

- Use `ALONG` for accumulators (distance, risk, confidence).
- Use `FOLD` when you need grouped summaries.
- Use `BEST BY` when you need one witness path, not all candidates.

## Related

- [Rule Semantics](../rule-semantics.md)
- [Internals: Native Execution](../../internals/locy/native-execution.md)
