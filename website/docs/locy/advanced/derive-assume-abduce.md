# Advanced: DERIVE, ASSUME, ABDUCE

## DERIVE

`DERIVE` turns inferred relations into graph mutations (edges/nodes/merges depending on clause).

```cypher
CREATE RULE candidate_friend AS
MATCH (a)-[:KNOWS]->(b)
DERIVE (a)-[:FRIEND]->(b)
```

## ASSUME

`ASSUME` executes a hypothetical mutation block and evaluates a body against that temporary state.

```cypher
ASSUME {
  CREATE (:Node {name: 'X'})-[:EDGE]->(:Node {name: 'Y'})
} THEN {
  QUERY reachable RETURN b
}
```

Think of this as savepoint-scoped what-if execution.

## ABDUCE

`ABDUCE` asks for minimal changes that would make (or prevent) a condition.

```cypher
ABDUCE NOT reachable WHERE a.name = 'Alice' RETURN b
```

Typical outputs include modification candidates, validation flags, and costs.

## Safe Usage

- Keep candidate search bounded.
- Use explicit constraints in `WHERE`.
- Validate candidate modifications before applying.

See [Errors & Limits](../reference/errors-limits.md) for runtime guardrails.
