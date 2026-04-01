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

### Standard Aggregators

Standard aggregators work in **non-recursive** strata only. They can decrease between iterations, which violates fixpoint requirements:

| Operator | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Row count | `FOLD n = COUNT(*)` |
| `COUNT(expr)` | Non-null count | `FOLD n = COUNT(b.value)` |
| `SUM(expr)` | Sum | `FOLD total = SUM(b.amount)` |
| `AVG(expr)` | Average | `FOLD avg = AVG(b.score)` |
| `MIN(expr)` | Minimum | `FOLD low = MIN(b.price)` |
| `MAX(expr)` | Maximum | `FOLD high = MAX(b.price)` |
| `COLLECT(expr)` | Collect into list | `FOLD paths = COLLECT(b.name)` |

### Monotonic Aggregators (Safe in Recursion)

Monotonic variants guarantee the aggregate never decreases between iterations, enabling safe use inside recursive strata:

| Operator | Formula | Identity | Use When |
|----------|---------|----------|----------|
| `MSUM(expr)` | Running sum | `0` | Non-negative additive accumulation |
| `MMAX(expr)` | Running maximum | `−∞` | Worst-case / dominant value |
| `MMIN(expr)` | Running minimum | `+∞` | Best-case / bottleneck |
| `MCOUNT(expr)` | Running count | `0` | Monotonically growing count |
| `MNOR(expr)` | `1 − ∏(1 − pᵢ)` | `0.0` | Independent OR-causes (probabilities) |
| `MPROD(expr)` | `∏ pᵢ` | `1.0` | Independent AND-conditions (probabilities) |

### Monotonic Probabilistic Folds

For probability domains, use `MNOR` (noisy-OR) and `MPROD` (product):

```cypher
CREATE RULE failure_risk AS
MATCH (c:Component)-[:HAS_SIGNAL]->(s:QualitySignal)
FOLD risk = MNOR(1.0 - s.pass_rate)
YIELD KEY c, risk
```

See [Probabilistic Logic](probabilistic-logic.md) for full documentation.

### Multiple FOLD Clauses

A rule can have multiple `FOLD` clauses to compute several aggregates simultaneously:

```cypher
CREATE RULE exposure AS
MATCH (a:Account)-[t:TRANSFER*]->(b:Account)
WHERE b IS suspicious
FOLD total = MSUM(t.amount)
FOLD path_count = MCOUNT(*)
YIELD KEY a, total, path_count
```

## BEST BY (Witness Selection)

`BEST BY` picks the best candidate row by ordering expression.

```cypher
CREATE RULE cheapest AS
MATCH (a)-[e:EDGE]->(b)
ALONG cost = prev.cost + e.weight
BEST BY cost ASC
YIELD KEY a, KEY b, cost
```

## Using `similar_to` in ALONG and BEST BY

The `similar_to()` expression function works in ALONG accumulators and BEST BY selectors, enabling semantic similarity scoring along recursive paths.

### Semantic Relevance Along Paths

```cypher
CREATE RULE semantic_path AS
MATCH (a:Document)-[:LINKS_TO]->(b:Document)
ALONG relevance = prev.relevance * similar_to(b.embedding, $query)
YIELD KEY a, KEY b, relevance
```

### Best Semantically Similar Path

```cypher
CREATE RULE best_match AS
MATCH (a:Topic)-[:RELATED]->(b:Topic)
ALONG score = prev.score + similar_to(b.embedding, $query)
BEST BY score DESC
YIELD KEY a, KEY b, score
```

### Hybrid Scoring in Rules

```cypher
CREATE RULE hybrid_relevant AS
MATCH (q:Query)-[:SEARCHES]->(d:Document)
WHERE similar_to([d.embedding, d.content], q.text,
  {method: 'weighted', weights: [0.7, 0.3]}) > 0.5
YIELD KEY q, KEY d
```

See the [Vector Search guide](../../guides/vector-search.md#similar_to-expression-function) for full `similar_to` documentation.

## Practical Guidance

- Use `ALONG` for accumulators (distance, risk, confidence, similarity).
- Use `FOLD` when you need grouped summaries.
- Use `BEST BY` when you need one witness path, not all candidates.

## Related

- [Rule Semantics](../rule-semantics.md)
- [Internals: Architecture](../../internals/locy/architecture.md)
