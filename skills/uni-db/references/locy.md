# Locy Reference

Locy (Logic + Cypher) is a Datalog-inspired logic programming language extending OpenCypher with recursive rules, path accumulation, aggregation, probabilistic inference, hypothetical reasoning, abductive inference, and graph materialization. Every valid Cypher query is a valid Locy program. Locy compiles rules into execution plans that run inside Uni's DataFusion-based query engine.

## 1. When to Use Locy vs Cypher

| Task | Cypher | Locy |
|------|--------|------|
| Simple CRUD, one-shot pattern matching, schema DDL | Yes | Overkill / not supported |
| Transitive closure / reachability | Awkward (`[*]` paths) | Natural (recursive rules) |
| Weighted shortest path | Not expressible | ALONG + BEST BY |
| Risk/score propagation | Not expressible | Recursive FOLD |
| Probabilistic inference | Not expressible | MNOR/MPROD + PROB |
| What-if / root-cause / proof traces | Not expressible | ASSUME / ABDUCE / EXPLAIN RULE |
| Graph materialization from reasoning | Manual CREATE | DERIVE |
| Permission resolution with priorities | Complex workarounds | PRIORITY rules |

---

## 2. Program Structure

```
MODULE namespace.path              -- Optional module namespace (at most one)
USE other.module { rule1, rule2 }  -- Optional selective import
USE another.module                 -- Optional glob import (all rules)

-- Statements: rules and commands (any order, any count)
CREATE RULE ... AS ...
QUERY rule_name WHERE ... RETURN ...
DERIVE rule_name
ASSUME { ... } THEN { ... }
MATCH (n) RETURN n                 -- Plain Cypher passthrough
```

Rules are compiled first (grouped, stratified, typechecked). Commands execute second, in order. Cypher statements return `CommandResult::Cypher(Vec<FactRow>)`.

Multiple query blocks can be combined with `UNION` (dedup) or `UNION ALL` (keep duplicates).

---

## 3. Rule Syntax -- CREATE RULE

### Full Syntax Template

```
CREATE RULE name [PRIORITY n] AS
    MATCH pattern
    [WHERE conditions]
    [ALONG accumulations]
    [FOLD aggregations]
    [BEST BY selections]
    (YIELD items | DERIVE patterns)
```

Every clause is optional except MATCH and the terminal (YIELD or DERIVE).

### Rule Names

```
CREATE RULE reachable AS ...           -- Simple name
CREATE RULE acme.risk_score AS ...     -- Qualified name
CREATE RULE `my-rule` AS ...           -- Backtick-quoted for reserved words
```

Identifiers: `[a-zA-Z_][a-zA-Z0-9_]*`. Reserved keywords that must be backtick-quoted if used as identifiers: `RULE`, `ALONG`, `PREV`, `FOLD`, `BEST`, `DERIVE`, `ASSUME`, `ABDUCE`, `QUERY`.

### Multi-Clause Union Semantics

Multiple `CREATE RULE` statements with the same name define different clauses of one rule. Results are the union of all clause evaluations. All clauses must have the same YIELD schema (same column count, KEY positions, PROB annotations). Violations produce `YieldSchemaMismatch`.

```
CREATE RULE reachable AS                          -- Clause 1: base case
    MATCH (a:Node)-[:EDGE]->(b:Node)
    YIELD KEY a, KEY b

CREATE RULE reachable AS                          -- Clause 2: recursive case
    MATCH (a:Node)-[:EDGE]->(mid:Node)
    WHERE mid IS reachable TO b
    YIELD KEY a, KEY b
```

### WHERE Clause

Comma-separated conditions (comma = AND). Three condition types can be mixed:

```
WHERE a IS reachable,                   -- IS reference (positive)
      b IS NOT blocked,                 -- IS NOT reference (negation)
      a.score > 0.5,                    -- Cypher expression
      x IN [1, 2, 3]                    -- Cypher expression
```

Full Cypher expression support: comparisons, arithmetic, function calls, `$param` references, `IS NULL`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`, regex, `CASE`, list comprehensions, etc.

---

## 4. IS References

IS references compose rules -- one rule references another rule's derived relation.

### Positive IS References

| Form | Syntax | Binding |
|------|--------|---------|
| Unary | `WHERE x IS flagged` | `x` bound to first KEY column |
| Binary (TO) | `WHERE x IS reachable TO y` | `x`, `y` bound to first two columns |
| Tuple | `WHERE (x, y, cost) IS weighted_path` | All variables bound positionally |

Semantics: a semi-join -- for each MATCH row, check if subject(s) exist in the target rule's derived relation. Additional yield columns beyond bound subjects become available as `__prev_*` variables for ALONG.

Binding count must not exceed target rule's yield schema width (error: `IsArityMismatch`).

### IS NOT References (Negation)

```
WHERE x IS NOT blocked            -- Postfix form
WHERE NOT x IS blocked            -- Prefix form
WHERE x IS NOT rule TO y          -- Binary with negation
WHERE (x, y) IS NOT rule          -- Tuple with negation
```

| Target rule has PROB? | Semantics |
|-----------------------|-----------|
| No | **Boolean anti-join**: keep rows where subject NOT in target |
| Yes | **Probabilistic complement**: matched rows contribute `1 - p`; unmatched rows contribute `1.0` |

### Stratification

The negated rule must be in a completed lower stratum -- no recursive negation allowed. Violations detected as `CyclicNegation`.

---

## 5. YIELD Clause

### Basic YIELD

```
YIELD a, b.name AS neighbor, cost + 1 AS adjusted
```

Each item is a Cypher expression with optional alias. Without alias, column name is inferred from the expression.

### KEY Columns

```
YIELD KEY a, KEY b, cost
```

KEY marks a column as a grouping key:
- Defines fact identity -- rows with identical KEY values are deduplicated
- Determines fixpoint convergence in recursive evaluation
- Used as join keys for IS references from other rules
- Implicit GROUP BY for FOLD aggregation

Without KEY columns, every row is unique -- usually an anti-pattern in recursive rules.

### PROB Annotation

```
YIELD KEY a, risk PROB
YIELD KEY a, risk AS risk_score PROB
```

- At most one PROB column per rule (error: `MultipleProbColumns`)
- MNOR/MPROD fold outputs are implicitly PROB (auto-annotated)
- PROB changes IS NOT semantics from Boolean anti-join to probabilistic complement

### Schema Consistency

All clauses of a multi-clause rule must produce the same YIELD schema: same column count, same KEY positions, same PROB annotations.

---

## 6. ALONG (Path-Carried Values)

Accumulates values along recursive traversal paths.

### Syntax

```
ALONG cost = expr
ALONG cost = expr, hops = expr          -- Multiple accumulators
```

### prev.field References

```
ALONG cost = prev.cost + e.weight
```

`prev.field` accesses the value from the previous recursive hop. Rules:
- Only valid in recursive clauses (error: `PrevInBaseCase`)
- Must reference an existing column in the target rule's yield schema (error: `PrevFieldNotInSchema`)

### Base Case vs Recursive Case

```
CREATE RULE path_cost AS                              -- Base: no prev
    MATCH (a:Node)-[e:EDGE]->(b:Node)
    ALONG cost = e.weight
    YIELD KEY a, KEY b, cost

CREATE RULE path_cost AS                              -- Recursive: with prev
    MATCH (a:Node)-[e:EDGE]->(mid:Node)
    WHERE mid IS path_cost TO b
    ALONG cost = prev.cost + e.weight
    YIELD KEY a, KEY b, cost
```

Multiple accumulators are independently computed per hop:

```
ALONG distance = prev.distance + e.length,
      hops = prev.hops + 1,
      max_weight = prev.max_weight
```

---

## 7. FOLD (Aggregation)

Aggregates values across rows sharing the same KEY group.

### Standard Aggregates (Non-Recursive Only)

| Aggregate | Description | Output Type |
|-----------|-------------|-------------|
| `SUM(expr)` | Sum of values | Float64 |
| `COUNT(expr)` | Count of non-null values | Int64 |
| `COUNT(*)` | Count of all rows | Int64 |
| `AVG(expr)` | Arithmetic mean | Float64 |
| `MIN(expr)` | Minimum value | Same as input |
| `MAX(expr)` | Maximum value | Same as input |
| `COLLECT(expr)` | Collect into a list | List |

These are not monotonic and cannot be used in recursive strata (error: `NonMonotonicInRecursion`).

### Monotonic Aggregates (Safe in Recursion)

| Aggregate | Formula | Direction | Identity | Domain |
|-----------|---------|-----------|----------|--------|
| `MSUM(expr)` | `acc + val` | Non-decreasing | 0.0 | Non-negative |
| `MMAX(expr)` | `max(acc, val)` | Non-decreasing | -infinity | None |
| `MMIN(expr)` | `min(acc, val)` | Non-increasing | +infinity | None |
| `MCOUNT(expr)` | `acc + 1` | Non-decreasing | 0 | None |
| `MNOR(expr)` | `1 - (1-acc)(1-val)` | Non-decreasing | 0.0 | [0, 1] |
| `MPROD(expr)` | `acc * val` | Non-increasing | 1.0 | [0, 1] |

Fixpoint converges when: (1) no new KEY tuples produced, AND (2) all monotonic accumulators stable (change < `f64::EPSILON`).

### FOLD + BEST BY Restriction

BEST BY cannot be combined with monotonic FOLD in the same clause -- semantically contradictory. Error: `BestByWithMonotonicFold`.

---

## 8. BEST BY (Witness Selection)

Retains the single best derivation per KEY group, preserving the full witness row.

```
BEST BY cost ASC                           -- Minimum cost (ASC is default)
BEST BY reliability DESC                   -- Maximum reliability
BEST BY cost ASC, priority DESC            -- Multiple criteria (tie-breakers)
```

When `deterministic_best_by = true` (default), ties are broken by secondary sort on all remaining columns. Enables early pruning during semi-naive evaluation.

```
CREATE RULE shortest AS
    MATCH (a:City)-[r:ROAD]->(b:City)
    ALONG cost = r.distance
    BEST BY cost ASC
    YIELD KEY a, KEY b, cost

CREATE RULE shortest AS
    MATCH (a:City)-[r:ROAD]->(mid:City)
    WHERE mid IS shortest TO b
    ALONG cost = prev.cost + r.distance
    BEST BY cost ASC
    YIELD KEY a, KEY b, cost
```

---

## 9. PRIORITY

Enables defeasible reasoning: higher-priority rules override lower-priority rules for the same KEY group.

```
CREATE RULE access PRIORITY 0 AS        -- Default
    MATCH (u:User)-[:MEMBER_OF]->(g:Group {name: 'public'})
    YIELD KEY u, 'allow' AS decision

CREATE RULE access PRIORITY 100 AS      -- Exception: admin override
    MATCH (u:User)-[:HAS_ROLE]->(r:Role {name: 'admin'})
    YIELD KEY u, 'allow' AS decision
```

- Higher number = higher priority. Default (when omitted) is 0.
- Applied post-fixpoint: per KEY group, only derivations from highest-priority clause survive.
- Equal-priority clauses contribute all their derivations (union).
- All clauses must either all have PRIORITY or none have it (error: `MixedPriority`).

---

## 10. Probabilistic Reasoning

### MNOR (Noisy-OR)

Formula: `P = 1 - prod(1 - p_i)`

"Probability that at least one cause produces the effect." Inputs clamped to [0,1] unless `strict_probability_domain = true`.

```
CREATE RULE delivery_risk AS
    MATCH (warehouse:WH)-[route:ROUTE]->(customer:Customer)
    FOLD any_arrives = MNOR(route.reliability)
    YIELD KEY customer, any_arrives PROB
```

### MPROD (Product)

Formula: `P = prod(p_i)`

"Probability that all conditions hold simultaneously." Uses log-space computation when product drops below `probability_epsilon` (default `1e-15`).

```
CREATE RULE system_reliability AS
    MATCH (sys:System)-[:REQUIRES]->(comp:Component)
    FOLD all_work = MPROD(comp.reliability)
    YIELD KEY sys, all_work PROB
```

### PROB and IS NOT Complement

When a rule has PROB and another rule uses IS NOT against it:

```
CREATE RULE risky AS
    MATCH (a:Account)-[s:SIGNAL]->(f:Flag)
    FOLD risk = MNOR(s.probability)
    YIELD KEY a, risk PROB

CREATE RULE safe AS
    MATCH (a:Account)
    WHERE a IS NOT risky
    YIELD KEY a, 1.0 AS confidence PROB
```

- If `a` has risk 0.8 in `risky`: `confidence = 1 - 0.8 = 0.2`
- If `a` is not in `risky` at all: `confidence = 1.0`

Multiple IS/IS NOT references with PROB in a clause multiply their probability terms.

### Shared Proof Detection

When recursive rules have diamond-shaped derivation graphs, multiple proof paths may share base facts, violating the independence assumption. Uni detects this via a DerivationTracker and emits `SharedProbabilisticDependency` warning.

### Exact Probability (BDD-Based)

When `exact_probability = true`, shared-proof groups use BDD-based weighted model counting:
1. Collect unique base facts across derivation rows
2. Build BDD variable set (one per base fact)
3. Per derivation row: AND its base-fact variables
4. Combine rows: OR for MNOR, AND for MPROD
5. Evaluate via Shannon expansion

Fallback: when unique base facts exceed `max_bdd_variables` (default 1000), falls back to independence mode with `BddLimitExceeded` warning.

### Top-K Proof Filtering

`top_k_proofs` bounds proofs retained per derived fact (0 = unlimited). Keeps only the k highest-probability proofs. `top_k_proofs_training` optionally overrides during training.

---

## 11. Commands

Commands execute after all strata converge. They operate on converged derived relations.

> **Expression limitation:** WHERE filters in commands use `eval_expr()` (lightweight row-level evaluator), not DataFusion. Functions like `similar_to()` are limited to pure vector cosine. Rule WHERE clauses (Phase 1) have full DataFusion support.

### QUERY (Goal-Directed)

```
QUERY rule_name [WHERE expr] [RETURN items [ORDER BY ...] [SKIP n] [LIMIT n]]
```

Uses SLG resolution (top-down with tabling) for efficient point lookups without computing the full relation. Returns `CommandResult::Query(Vec<FactRow>)`.

```
QUERY reachable WHERE a.name = 'Alice' RETURN b.name AS destination
```

### DERIVE (Materialization)

```
DERIVE rule_name [WHERE expr]
```

Triggers bottom-up evaluation and applies graph mutations. Returns `CommandResult::Derive { affected: usize }`.

Rules can use DERIVE as an alternative terminal to YIELD:

```
DERIVE (a)-[:INFERRED_FRIEND]->(b)                          -- Edge derivation
DERIVE (a)-[:RISK_LINK {score: risk}]->(b)                   -- With properties
DERIVE (NEW cat:Category {name: a.type})<-[:BELONGS_TO]-(a)  -- NEW node (Skolem)
DERIVE MERGE a, b                                             -- Entity resolution
```

NEW node wardedness constraint: companion node must be bound by MATCH, not solely by IS references (error: `WardednessViolation`).

Session-level DERIVE collects mutations into `DerivedFactSet` (apply via `tx.apply()`). Transaction-level DERIVE applies immediately.

### ASSUME (Hypothetical Reasoning)

```
ASSUME {
    CREATE (x:Account {name: 'Suspicious'})-[:TRANSFER]->(existing:Account)
}
THEN {
    QUERY risk_propagation RETURN affected_nodes
}
```

Execution: fork L0 buffer -> apply mutations -> re-evaluate strata -> execute body -> rollback. Database is never permanently modified. Can be nested. Returns `CommandResult::Assume(Vec<FactRow>)`.

### ABDUCE (Abductive Reasoning)

```
ABDUCE [NOT] rule_name [WHERE expr] [RETURN items [ORDER BY ...] [LIMIT n]]
```

"What modifications would make this rule hold (or stop holding)?" Three-phase: generate candidates -> validate (ASSUME-style) -> return sorted by cost. Returns `CommandResult::Abduce(AbductionResult)`.

Modification types: `RemoveEdge`, `AddEdge`, `ChangeProperty`.

```
ABDUCE NOT reachable WHERE a.name = 'A' AND b.name = 'C' RETURN modifications
```

### EXPLAIN RULE (Proof Traces)

```
EXPLAIN RULE rule_name [WHERE expr] [RETURN items [ORDER BY ...] [LIMIT n]]
```

Returns the derivation tree showing which clauses and base facts produced a result. Returns `CommandResult::Explain(DerivationNode)`.

---

## 12. API Reference

### Python

```python
# Session-level
result = session.locy("CREATE RULE r AS ... YIELD ...", params={"key": "value"})

# Builder pattern
result = session.locy_with("QUERY r WHERE x = $val") \
    .param("val", "Alice") \
    .params({"a": 1, "b": 2}) \
    .timeout(60.0) \
    .max_iterations(500) \
    .with_config({"exact_probability": True}) \
    .run()

# Compilation-only introspection
explain = session.explain_locy("CREATE RULE r AS ...")
# explain.plan_text, explain.strata_count, explain.has_recursive_strata

# Transaction-level
with session.tx() as tx:
    result = tx.locy("DERIVE infer_edges")
    result = tx.locy_with("QUERY r WHERE x = $val").param("val", "Alice").run()
    session_result = session.locy("DERIVE infer_edges")
    tx.apply(session_result.derived_fact_set)
    tx.commit()

# Async equivalents: await session.locy(...), async with await session.tx() as tx

# Rule registry
session.rules().register("CREATE RULE reach AS ...")
session.rules().list()       # -> ["reach"]
session.rules().get("reach") # -> RuleInfo { name, clause_count, is_recursive }
session.rules().remove("reach")
session.rules().count()
session.rules().clear()
```

### Rust

```rust
// Session-level
let result = session.locy("CREATE RULE r AS ... YIELD ...").await?;

// Builder pattern
let result = session.locy_with("QUERY r WHERE x = $val")
    .param("val", "Alice")
    .params([("a", Value::from(1)), ("b", Value::from(2))])
    .params_map(hashmap)
    .timeout(Duration::from_secs(60))
    .max_iterations(500)
    .cancellation_token(token)
    .with_config(LocyConfig { .. })
    .run()
    .await?;

// Compilation-only introspection
let explain = session.locy_with("CREATE RULE r AS ...").explain()?;
// explain.plan_text, explain.strata_count, explain.has_recursive_strata

// Transaction-level
let tx = session.tx().await?;
let result = tx.locy("DERIVE infer_edges").await?;
let result = tx.locy_with("QUERY r WHERE x = $val")
    .param("val", "Alice").run().await?;

// Apply session-level derived facts
let derived = session.locy("DERIVE infer_edges").await?.derived_fact_set.unwrap();
tx.apply(derived).await?;
tx.commit().await?;

// Rule registry (same API at db / session / tx level)
session.rules().register("CREATE RULE reach AS ...")?;
session.rules().list();      // Vec<String>
session.rules().get("reach"); // Option<RuleInfo>
session.rules().remove("reach")?;
session.rules().count();
session.rules().clear();
```

### LocyResult Fields

| Field | Type | Description |
|-------|------|-------------|
| `derived` | `HashMap<String, Vec<FactRow>>` | Rule name -> derived facts |
| `stats` | `LocyStats` | Execution statistics |
| `command_results` | `Vec<CommandResult>` | Ordered command outputs |
| `warnings` | `Vec<RuntimeWarning>` | Runtime warnings |
| `approximate_groups` | `HashMap<String, Vec<String>>` | Approximate BDD groups |
| `derived_fact_set` | `Option<DerivedFactSet>` | Collected DERIVE mutations (session-level only) |

Convenience: `result.warnings()`, `result.has_warning(code)`.

### CommandResult Variants

| Variant | Payload |
|---------|---------|
| `Query` | `Vec<FactRow>` |
| `Assume` | `Vec<FactRow>` |
| `Explain` | `DerivationNode` |
| `Abduce` | `AbductionResult` (contains `Vec<ValidatedModification>`) |
| `Derive` | `{ affected: usize }` |
| `Cypher` | `Vec<FactRow>` |

### LocyStats Fields

| Field | Type |
|-------|------|
| `strata_evaluated` | `usize` |
| `total_iterations` | `usize` |
| `derived_nodes` | `usize` |
| `derived_edges` | `usize` |
| `evaluation_time` | `Duration` |
| `queries_executed` | `usize` |
| `mutations_executed` | `usize` |
| `peak_memory_bytes` | `usize` |

---

## 13. Module System

### MODULE Declaration

```
MODULE acme.compliance
```

Declares namespace for all rules in this program. Optional, at most one per program. Must appear before USE declarations.

### USE Imports

```
USE acme.common                       -- Glob import: all exported rules
USE acme.common { control, reachable } -- Selective import: named rules only
```

Imported rules are available for IS references. Qualified names resolved during compilation.

### Qualified Names

```
reachable                    -- Simple name
acme.compliance.control      -- Qualified name
a.b.c.my_rule                -- Deep nesting
```

---

## 14. Configuration Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_iterations` | `usize` | `1000` | Max fixpoint iterations per recursive stratum |
| `timeout` | `Duration` | `300s` | Overall evaluation timeout |
| `max_explain_depth` | `usize` | `100` | Max recursion depth for EXPLAIN trees |
| `max_slg_depth` | `usize` | `1000` | Max recursion depth for SLG resolution (QUERY) |
| `max_abduce_candidates` | `usize` | `20` | Max candidates generated during ABDUCE |
| `max_abduce_results` | `usize` | `10` | Max validated ABDUCE results |
| `max_derived_bytes` | `usize` | `256 MiB` | Max bytes of derived facts per relation |
| `deterministic_best_by` | `bool` | `true` | BEST BY uses secondary sort for deterministic ties |
| `strict_probability_domain` | `bool` | `false` | Reject MNOR/MPROD inputs outside [0,1] (else clamp) |
| `probability_epsilon` | `f64` | `1e-15` | MPROD switches to log-space below this threshold |
| `exact_probability` | `bool` | `false` | BDD-based exact inference for shared-proof groups |
| `max_bdd_variables` | `usize` | `1000` | Per-group BDD variable cap before fallback |
| `top_k_proofs` | `usize` | `0` | Retain at most k proofs per fact (0 = unlimited) |
| `top_k_proofs_training` | `Option<usize>` | `None` | Override top_k_proofs during training |
| `params` | `HashMap<String, Value>` | `{}` | Parameter bindings for `$name` references |

Setting configuration:

```python
# Python -- individual overrides
session.locy_with(program).timeout(60.0).max_iterations(500).run()

# Python -- full config override
session.locy_with(program).with_config({
    "exact_probability": True,
    "strict_probability_domain": True,
    "max_bdd_variables": 2000,
}).run()
```

```rust
// Rust -- full config override
let config = LocyConfig {
    exact_probability: true,
    strict_probability_domain: true,
    max_bdd_variables: 2000,
    ..Default::default()
};
session.locy_with(program).with_config(config).run().await?;
```

---

## 15. Complete Examples

### Transitive Closure

```
CREATE RULE reachable AS
    MATCH (a:Node)-[:EDGE]->(b:Node)
    YIELD KEY a, KEY b

CREATE RULE reachable AS
    MATCH (a:Node)-[:EDGE]->(mid:Node)
    WHERE mid IS reachable TO b
    YIELD KEY a, KEY b
```

### Risk Propagation with MNOR

```
CREATE RULE supplier_risk AS
    MATCH (s:Supplier)-[:HAS_SIGNAL]->(sig:Signal)
    FOLD risk = MNOR(sig.risk)
    YIELD KEY s, risk PROB

CREATE RULE product_exposure AS
    MATCH (s:Supplier)-[:SUPPLIES]->(p:Product)
    WHERE s IS supplier_risk
    FOLD exposure = MNOR(risk)
    YIELD KEY p, exposure PROB

CREATE RULE safe_product AS
    MATCH (p:Product)
    WHERE p IS NOT product_exposure
    YIELD KEY p, 1.0 AS safety PROB
```

Result: `supplier_risk(S1) = 1-(1-0.3)(1-0.5) = 0.65`, `product_exposure(Widget) = 1-(1-0.65)(1-0.2) = 0.72`, `safe_product(Widget) = 1-0.72 = 0.28`.

### RBAC with Priorities

```
CREATE RULE access PRIORITY 0 AS
    MATCH (u:User)-[:MEMBER_OF]->(g:Group {name: 'public'})
    YIELD KEY u, 'allow' AS decision

CREATE RULE access PRIORITY 50 AS
    MATCH (u:User)-[:MEMBER_OF]->(g:Group)
    WHERE g IS restricted_group
    YIELD KEY u, 'deny' AS decision

CREATE RULE access PRIORITY 100 AS
    MATCH (u:User)-[:HAS_ROLE]->(r:Role {name: 'admin'})
    YIELD KEY u, 'allow' AS decision

CREATE RULE restricted_group AS
    MATCH (g:Group) WHERE g.classification = 'restricted'
    YIELD KEY g
```

### Supply Chain Provenance (DERIVE + NEW Nodes)

```
CREATE RULE infer_categories AS
    MATCH (p:Product) WHERE p.price > 100
    DERIVE (NEW cat:Category {name: 'Premium'})<-[:BELONGS_TO]-(p)

DERIVE infer_categories
```

### What-If Analysis with ASSUME

```
CREATE RULE reachable AS
    MATCH (a:Server)-[:CONNECTS_TO]->(b:Server)
    YIELD KEY a, KEY b

CREATE RULE reachable AS
    MATCH (a:Server)-[:CONNECTS_TO]->(mid:Server)
    WHERE mid IS reachable TO b
    YIELD KEY a, KEY b

ASSUME {
    MATCH (a:Server {name: 'Gateway'})-[r:CONNECTS_TO]->(b:Server {name: 'DB'})
    DELETE r
}
THEN {
    QUERY reachable WHERE a.name = 'WebApp' AND b.name = 'DB'
    RETURN b.name
}

ABDUCE NOT reachable WHERE a.name = 'WebApp' AND b.name = 'DB'
RETURN modifications
```

---

## Anti-Patterns and Gotchas

| Anti-Pattern | Symptom | Fix |
|-------------|---------|-----|
| Missing KEY columns in recursive rules | Exponential growth, `MaxIterationsExceeded` | Add KEY columns for fact identity |
| SUM/COUNT/AVG in recursion | `NonMonotonicInRecursion` | Use MSUM/MCOUNT or restructure |
| `prev.field` in base case | `PrevInBaseCase` | Use literal values in base case ALONG |
| Cyclic negation (A IS NOT B, B IS NOT A) | `CyclicNegation` | Ensure negation flows in one direction |
| BEST BY + monotonic FOLD | `BestByWithMonotonicFold` | Use BEST BY with ALONG, or FOLD without BEST BY |
| Ignoring `SharedProbabilisticDependency` warning | Silently wrong probabilities | Enable `exact_probability` or review rule logic |
| ALONG without BEST BY in recursive rules | All path variants retained (exponential) | Add BEST BY to prune dominated paths |
| Command WHERE using DataFusion-only functions | Silent eval failure or limited behavior | Move complex filters into rule MATCH/WHERE |
