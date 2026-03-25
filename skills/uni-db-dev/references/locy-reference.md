# Locy Reference (Logic Programming for Uni)

## Table of Contents
1. [Overview](#overview)
2. [Rule Syntax](#rule-syntax)
3. [Rule References](#rule-references)
4. [Commands](#commands)
5. [ALONG (Path-Carried Values)](#along)
6. [FOLD (Aggregation)](#fold)
7. [BEST BY (Witness Selection)](#best-by)
8. [DERIVE (Graph Materialization)](#derive)
9. [ASSUME (Hypothetical Analysis)](#assume)
10. [ABDUCE (Remediation Search)](#abduce)
11. [EXPLAIN (Proof Traces)](#explain)
12. [Module System](#module-system)
13. [Configuration](#configuration)
14. [Invocation](#invocation)

---

## Overview

Locy is Uni's logic programming layer. It extends OpenCypher with declarative rules for recursive reasoning, compliance modeling, risk propagation, and fixpoint-based derivations. Programs are evaluated bottom-up to a fixed point within each stratum.

Key capabilities:
- Recursive rules with transitive closure
- Stratified negation (safe, no paradoxes)
- Path-carried values (ALONG) for accumulating state through traversals
- Aggregation (FOLD) over derived facts
- Optimal witness selection (BEST BY)
- Hypothetical what-if analysis (ASSUME...THEN)
- Abductive reasoning (ABDUCE) for remediation
- Proof traces (EXPLAIN RULE)

---

## Rule Syntax

```cypher
CREATE RULE rule_name [PRIORITY n] AS
MATCH pattern
[WHERE conditions]
[ALONG carried_value = expression]
[FOLD aggregate = FUNCTION(expression)]
[BEST BY sort_expr ASC|DESC]
YIELD KEY key_col [, KEY key_col2, ...] [, expr AS alias, ...]
```

### Multiple Clauses (Overloading)

Multiple `CREATE RULE` with the same name form a single logical relation (like UNION). Use this for base cases and recursive cases:

```cypher
-- Base case: direct edges
CREATE RULE reachable AS
MATCH (a:Person)-[:KNOWS]->(b:Person)
YIELD KEY a, KEY b

-- Recursive case: transitive closure
CREATE RULE reachable AS
MATCH (a:Person)-[:KNOWS]->(mid:Person)
WHERE (mid, b) IS reachable
YIELD KEY a, KEY b
```

### PRIORITY

Higher priority rules take precedence in conflict resolution:

```cypher
CREATE RULE access PRIORITY 10 AS ...  -- Higher priority
CREATE RULE access PRIORITY 1 AS ...   -- Lower priority (default)
```

---

## Rule References

Rules are referenced in WHERE clauses of other rules.

### Positive References

```cypher
WHERE x IS rule_name                    -- Unary: x matches rule
WHERE x IS rule_name TO y              -- Binary: (x,y) tuple in rule
WHERE (x, y, cost) IS rule_name        -- Multi-column tuple
```

### Negative References (Stratified)

```cypher
WHERE x IS NOT rule_name               -- Postfix negation
WHERE NOT x IS rule_name               -- Prefix negation
WHERE (x, y) IS NOT rule_name          -- Negated tuple
```

Negation is stratified: a rule using `IS NOT other_rule` must be in a later stratum than `other_rule`. Cyclic negation (A IS NOT B; B IS NOT A) is rejected at compile time.

---

## Commands

### QUERY (Goal-Directed Evaluation)

```cypher
QUERY rule_name [WHERE condition] RETURN columns
```

Evaluates rules using SLG resolution (top-down with tabling). Returns matching rows.

```cypher
QUERY reachable WHERE a.name = 'Alice' RETURN b.name AS person
```

### DERIVE (Bottom-Up Materialization)

Materializes inferred facts into the graph.

```cypher
DERIVE rule_name [WHERE condition]
```

### CYPHER (Inline Cypher)

Execute standard Cypher within a Locy program:

```cypher
CYPHER {
  MATCH (p:Person) WHERE p.age > 30 RETURN p.name
}
```

---

## ALONG

Carry state through recursive path traversals. Use `prev.varname` to reference the value from the previous hop.

```cypher
CREATE RULE shortest_path AS
MATCH (a:Node)-[e:EDGE]->(b:Node)
ALONG dist = prev.dist + e.weight
YIELD KEY a, KEY b, dist
```

The initial value of `prev.dist` is 0 (or whatever the expression evaluates to when `prev` fields are 0/null).

### Use Cases
- Cumulative cost/distance through paths
- Risk score propagation with decay
- Confidence chain through inference steps

```cypher
-- Risk propagation with 20% decay per hop
CREATE RULE propagated_risk AS
MATCH (a:Account)-[:TRANSFER]->(b:Account)
WHERE b IS risky_seed
ALONG risk = prev.risk * 0.8
YIELD KEY a, risk
```

---

## FOLD

Aggregate over derived facts within a rule.

```cypher
CREATE RULE total_exposure AS
MATCH (a:Account)-[t:TRANSFER]->(b:Account)
WHERE b IS risky
FOLD total = SUM(t.amount)
YIELD KEY a, total
```

### Monotonic vs Non-Monotonic Aggregates

| Monotonic (safe in recursion) | Non-monotonic (only in non-recursive) |
|-------------------------------|---------------------------------------|
| `MSUM` | `SUM` |
| `MMAX` | `MAX` |
| `MMIN` | `MIN` |
| `MCOUNT` | `COUNT` |
| `MNOR` | — |
| `MPROD` | — |

Use monotonic variants (`MSUM`, `MMAX`, etc.) when the rule is part of a recursive stratum.

### Probabilistic Aggregation (MNOR / MPROD)

| Aggregator | Formula | Semantics |
|---|---|---|
| `MNOR` | `1 − ∏(1 − pᵢ)` | Noisy-OR: any cause can trigger the effect |
| `MPROD` | `∏ pᵢ` | Product: all conditions must hold |

Both clamp inputs to [0, 1], skip nulls, and are incompatible with `BEST BY`.

```cypher
-- Combine independent risk signals
CREATE RULE combined_risk AS
MATCH (a:Account)-[s:SIGNAL]->(f:Flag)
FOLD risk = MNOR(s.probability)
YIELD KEY a, risk
```

---

## BEST BY

Retain only optimal derivations per key group. Useful for shortest paths, cheapest routes, highest-confidence inferences.

```cypher
CREATE RULE cheapest_route AS
MATCH (a:City)-[r:ROAD]->(b:City)
ALONG cost = prev.cost + r.distance
BEST BY cost ASC              -- Keep only the cheapest per (a,b) pair
YIELD KEY a, KEY b, cost
```

- `ASC`: Keep minimum value (shortest, cheapest)
- `DESC`: Keep maximum value (highest confidence, most recent)

---

## DERIVE

Materialize inferred facts as actual graph nodes/edges.

```cypher
-- Create edges from inferred relationships
CREATE RULE inferred_friend AS
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
WHERE NOT (a)-[:KNOWS]->(c)
DERIVE (a)-[:SUGGESTED_FRIEND]->(c)

-- Merge duplicate entities
CREATE RULE duplicate AS
MATCH (a:Person), (b:Person)
WHERE a.email = b.email AND a._vid < b._vid
DERIVE MERGE a, b
```

---

## ASSUME (Hypothetical Analysis)

Execute mutations in a savepoint, evaluate the body, then rollback. The graph is unchanged after evaluation.

```cypher
ASSUME {
  -- Hypothetical mutations
  CREATE (:Server {name: 'new-server', region: 'us-east'})
  MATCH (s:Server {name: 'new-server'}), (n:Network {name: 'prod'})
  CREATE (s)-[:CONNECTED_TO]->(n)
} THEN {
  -- Evaluate with hypothetical state
  QUERY blast_radius WHERE trigger.name = 'new-server'
  RETURN affected.name AS impacted
}
```

### Use Cases
- What-if analysis: "What happens if this server goes down?"
- Capacity planning: "What if we add 10 new nodes?"
- Compliance testing: "Would this change violate any rules?"

---

## ABDUCE (Remediation Search)

Find minimal graph modifications to satisfy or prevent a condition.

```cypher
-- Find what to change to prevent Alice from being reachable
ABDUCE NOT reachable WHERE a.name = 'Alice' RETURN b.name

-- Find what to add to satisfy a compliance rule
ABDUCE compliant WHERE org.name = 'Acme' RETURN modifications
```

Returns candidate modifications with validation flags and costs.

---

## EXPLAIN (Proof Traces)

Show the derivation path for how a fact was inferred.

```cypher
EXPLAIN RULE risky WHERE a.id = 'ACCT-001' RETURN a.id, risk_level
```

Returns a proof tree showing which rules and base facts contributed to each derived fact.

---

## Module System

Organize rules into reusable modules.

```cypher
MODULE compliance.access_control

USE shared.risk_model { risky, risk_score }

CREATE RULE authorized AS
MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission)
WHERE NOT u IS risky
YIELD KEY u, KEY p
```

---

## Configuration

### Python

```python
result = db.locy_evaluate(program, config={
    "max_iterations": 500,       # Per-stratum fixpoint limit (default: 1000)
    "timeout": 60.0,             # Overall timeout in seconds (default: 300)
    "max_abduce_candidates": 30, # Candidate modifications (default: 20)
    "max_abduce_results": 10,    # Results to return (default: 10)
    "max_derived_bytes": 64 * 1024 * 1024,  # Memory cap (default: 256 MB)
    "deterministic_best_by": True,  # Tie-breaking (default: True)
})
```

### Rust

```rust
let config = LocyConfig {
    max_iterations: 500,
    timeout: Duration::from_secs(60),
    max_explain_depth: 100,
    max_slg_depth: 1000,
    max_abduce_candidates: 20,
    max_abduce_results: 10,
    max_derived_bytes: 256 * 1024 * 1024,
    deterministic_best_by: true,
};
let result = db.locy().evaluate_with_config(program, &config).await?;
```

---

## Invocation

### Python

```python
result = db.locy_evaluate(program)

# Access results
derived = result["derived"]           # dict[str, list[dict]]
stats = result["stats"]               # LocyStats
commands = result["command_results"]   # list[dict]

# QUERY results are in command_results
for cmd in commands:
    if cmd["type"] == "query":
        for row in cmd["rows"]:
            print(row)
```

### Rust

```rust
let result = db.locy().evaluate(program).await?;

let rows = result.rows();           // Extracted from command_results
let stats = result.stats();         // LocyStats
let derived = &result.derived;      // HashMap<String, Vec<Row>>
```

---

## Complete Example: Fraud Risk Propagation

```python
program = r'''
-- Base case: flagged accounts are risky
CREATE RULE risky AS
MATCH (a:Account)
WHERE a.flagged = true
YIELD KEY a

-- Recursive case: accounts that sent money to risky accounts
CREATE RULE risky AS
MATCH (a:Account)-[:TRANSFER]->(b:Account)
WHERE b IS risky
YIELD KEY a

-- Total exposure per risky account
CREATE RULE exposure AS
MATCH (a:Account)-[t:TRANSFER]->(b:Account)
WHERE b IS risky
FOLD total = SUM(t.amount)
YIELD KEY a, total

-- Query: find all risky accounts and their exposure
QUERY risky RETURN a.id AS account, a.name AS name
QUERY exposure RETURN a.id AS account, total AS total_exposure
'''

result = db.locy_evaluate(program)
risky_accounts = result["command_results"][0]["rows"]
exposures = result["command_results"][1]["rows"]
```
