# Advanced: Modules, PRIORITY, QUERY

## Modules

Modules package reusable rule libraries.

```cypher
MODULE acme.compliance
USE acme.common
```

Benefits:

- Reuse and composition.
- Namespace separation.
- Cleaner policy/rule governance.

## PRIORITY

Priority resolves competing clauses for the same rule relation.

```cypher
CREATE RULE classify PRIORITY 1 AS MATCH (n) WHERE n.risk > 0.8 YIELD KEY n, 'high' AS level
CREATE RULE classify PRIORITY 2 AS MATCH (n) YIELD KEY n, 'normal' AS level
```

## QUERY (Goal-Directed)

`QUERY` asks for targeted answers rather than full materialization-first workflows.

```cypher
QUERY classify WHERE n.name = 'service-a' RETURN level
```

## Choosing Bottom-Up vs Goal-Directed

- Bottom-up: broad materialization, repeated downstream reuse.
- Goal-directed: targeted questions with narrower evaluation focus.
