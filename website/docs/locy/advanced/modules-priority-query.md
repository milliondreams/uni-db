# Advanced: Modules, PRIORITY, QUERY

## Modules

Modules package reusable rule libraries.

<!-- doctest: skip -->
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

```locy
CREATE RULE classify PRIORITY 1 AS MATCH (n) WHERE n.risk > 0.8 YIELD KEY n, 'high' AS level
CREATE RULE classify PRIORITY 2 AS MATCH (n) YIELD KEY n, 'normal' AS level
```

## QUERY

`QUERY` reads a rule's facts as the fixpoint derived them, then filters and
projects them; it returns exactly what `.derived` holds for that rule.

```locy
QUERY classify WHERE n.name = 'service-a' RETURN level
```

## Evaluation

Locy evaluates bottom-up: every rule is derived to its fixpoint before any
command runs. A `QUERY ... WHERE` filters the derived facts; it does not narrow
what is derived. To derive less, constrain the rule itself (a `WHERE` in its
body, or a `REQUIRE`).
