# Locy Syntax Cheatsheet

## Rule

```cypher
CREATE RULE name [PRIORITY n] AS
MATCH ...
[WHERE ...]
[ALONG x = expr]
[FOLD agg = SUM(expr)]
[BEST BY expr ASC|DESC]
YIELD KEY a, ...
```

## Goal Query

```cypher
QUERY name [WHERE ...] [RETURN ...]
```

## Derive Command

```cypher
DERIVE name [WHERE ...]
```

## Explain

```cypher
EXPLAIN RULE name [WHERE ...]
```

## Assume

```cypher
ASSUME { <cypher mutations> } THEN { <locy/cypher body> }
```

## Abduce

```cypher
ABDUCE [NOT] name [WHERE ...] [RETURN ...]
```

## Modules

```cypher
MODULE my.module
USE shared.rules
```
