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
YIELD KEY a, KEY b, PROB similar_to(b.embedding, a.embedding)
```

`similar_to()` supports metric-aware vector scoring (Cosine, L2, Dot Product), FTS scoring, and multi-source hybrid fusion. See the [Vector Search guide](../guides/vector-search.md#similar_to-expression-function) for full documentation.

!!! note "Rule vs command expressions"
    In rule bodies (`WHERE`, `YIELD`, `ALONG`, `FOLD`), `similar_to()` runs inside DataFusion with full capability — metric-aware vector scoring, auto-embedding, FTS, and multi-source fusion. In command WHERE clauses (`DERIVE ... WHERE`, `ABDUCE ... WHERE`), only basic vector similarity (cosine) is available because commands execute on materialized rows after strata converge without schema context.

## Goal Query

```cypher
QUERY reachable WHERE a.name = 'Alice' RETURN b
```

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
