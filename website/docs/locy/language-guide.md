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
