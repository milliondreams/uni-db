# Locy Language Guide

## Rule Definition

```cypher
CREATE RULE rule_name [PRIORITY n] AS
MATCH ...
[WHERE ...]
[ALONG ...]
[FOLD ...]
[BEST BY ...]
YIELD ...
```

## Rule References

### Unary

```cypher
WHERE n IS suspicious
WHERE n IS NOT suspicious
```

### Binary/Tuple

```cypher
WHERE a IS reachable TO b
WHERE (x, y, c) IS control
```

## Expression Functions in Rules

Cypher expression functions work inside `WHERE`, `ALONG`, `FOLD`, `BEST BY`, and `YIELD`. The `similar_to()` function is particularly useful for semantic scoring in rules:

```cypher
-- Filter by semantic similarity in WHERE
CREATE RULE relevant_docs AS
MATCH (q:Query)-[:ABOUT]->(topic:Topic)<-[:TAGGED]-(d:Document)
WHERE similar_to(d.embedding, q.text) > 0.7
YIELD KEY q, KEY d, similar_to(d.embedding, q.text) AS score

-- Use as PROB value for probabilistic derivation
CREATE RULE related AS
MATCH (a:Paper)-[:CITES]->(b:Paper)
YIELD KEY a, KEY b, similar_to(b.embedding, a.embedding) AS PROB
```

`similar_to()` supports metric-aware vector scoring (Cosine, L2, Dot Product), FTS scoring, and multi-source hybrid fusion. See the [Vector Search guide](../guides/vector-search.md#similar_to-expression-function) for full documentation.

`PROB` can be written as `expr AS PROB`, `expr AS alias PROB`, or `expr PROB`. At most one output column per rule can be marked this way.

### Probabilistic Aggregation with MNOR and MPROD

```cypher
-- Noisy-OR: probability that at least one cause fires
CREATE RULE failure_risk AS
MATCH (c:Component)-[:HAS_SIGNAL]->(s:QualitySignal)
FOLD risk = MNOR(1.0 - s.pass_rate)
YIELD KEY c, risk

-- Product: joint probability that all conditions hold
CREATE RULE vendor_reliability AS
MATCH (v:Vendor)-[:SUPPLIES]->(c:Component)
WHERE c IS failure_risk
FOLD reliability = MPROD(1.0 - failure_risk.risk)
YIELD KEY v, reliability
```

See [Probabilistic Logic](advanced/probabilistic-logic.md) for full documentation of MNOR and MPROD.

!!! note "Rule vs command expressions"
    In rule bodies (`WHERE`, `YIELD`, `ALONG`, `FOLD`), `similar_to()` runs inside DataFusion with full capability — metric-aware vector scoring, auto-embedding, FTS, and multi-source fusion. In command WHERE clauses (`DERIVE ... WHERE`, `ABDUCE ... WHERE`), only basic vector similarity (cosine) is available because commands execute on materialized rows after strata converge without schema context.

## Goal Query

```cypher
QUERY reachable WHERE a.name = 'Alice' RETURN b
```

## DERIVE in Rules (Graph Mutation)

Rules can use `DERIVE` instead of `YIELD` to directly write graph mutations:

```cypher
-- Infer a new edge from rule output
CREATE RULE infer_risk AS
MATCH (a:Account)-[:TRANSFER]->(b:Account)
WHERE a IS flagged
DERIVE (b)-[:RISK_FROM]->(a)

-- Add a label to derived nodes
CREATE RULE flag_accounts AS
MATCH (a:Account)
WHERE a.fraud_score > 0.8
DERIVE (a:FlaggedAccount)
```

`DERIVE` rules run in Phase 2 (command dispatch) on converged derived facts. Use `YIELD` when you want to produce queryable derived facts; use `DERIVE` when you want to write mutations back to the graph.

## Derivation Commands

```cypher
DERIVE reachable WHERE a.name = 'Alice'
```

## Hypothetical Reasoning

```cypher
ASSUME {
  CREATE (:Node {name: 'Temp'})
} THEN {
  QUERY reachable RETURN b
}
```

## Abductive Reasoning

```cypher
ABDUCE NOT reachable WHERE a.name = 'Alice' RETURN b
```

## Explainability

```cypher
EXPLAIN RULE reachable WHERE a.name = 'Alice'
```

## Modules

```cypher
MODULE acme.security
USE acme.common
```

For advanced semantics of `ALONG`, `FOLD`, `BEST BY`, and mutation reasoning, continue to the advanced pages.
