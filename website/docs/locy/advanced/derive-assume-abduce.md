# Advanced: DERIVE, ASSUME, ABDUCE

## DERIVE

`DERIVE` has two forms with different semantics:

### DERIVE in a rule body (inline mutation)

Replaces `YIELD` when a rule should write graph mutations rather than produce queryable derived facts:

```cypher
-- Infer a new edge
CREATE RULE infer_friend AS
MATCH (a)-[:KNOWS]->(b)
DERIVE (a)-[:FRIEND]->(b)

-- Add a label
CREATE RULE flag_risky AS
MATCH (a:Account) WHERE a.fraud_score > 0.8
DERIVE (a:FlaggedAccount)

-- MERGE: combine matching paths into one node
CREATE RULE merge_paths AS
MATCH (a)-[r]->(b)
DERIVE MERGE a, b
```

These run during Phase 2 command dispatch on converged derived facts.

### DERIVE as a command

Iterates over converged facts from a named rule, applies an optional WHERE filter, and executes the mutations:

```cypher
DERIVE risk_propagation WHERE threshold > 0.5
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

`ABDUCE` asks: "what minimal changes would make (or prevent) this conclusion hold?"

```cypher
ABDUCE compromised WHERE target.name = 'ServerA' RETURN assumptions
ABDUCE NOT reachable WHERE a.name = 'Alice' RETURN b
```

**Three-phase pipeline:**

1. **Build derivation tree** via EXPLAIN to identify which facts contribute to the conclusion.
2. **Extract candidate modifications** — edge removals, property changes, edge additions that could alter the derivation.
3. **Validate each candidate**: savepoint → apply mutation → re-evaluate all strata → check if conclusion holds → rollback.

Returns the minimal set of changes that achieve (or prevent) the goal. The database is never permanently modified.

Typical outputs include modification candidates, validation flags, and estimated costs.

## Safe Usage

- Keep candidate search bounded.
- Use explicit constraints in `WHERE`.
- Validate candidate modifications before applying.

See [Errors & Limits](../reference/errors-limits.md) for runtime guardrails.
