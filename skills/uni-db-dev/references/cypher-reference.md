# Cypher Query Reference

## Table of Contents
1. [Query Structure](#query-structure)
2. [MATCH Patterns](#match-patterns)
3. [WHERE Filtering](#where-filtering)
4. [RETURN & Projections](#return--projections)
5. [WITH Clause](#with-clause)
6. [Mutations](#mutations)
7. [Aggregations](#aggregations)
8. [Functions](#functions)
9. [Advanced Features](#advanced-features)

---

## Query Structure

```cypher
[MATCH pattern]
[WHERE conditions]
[WITH projections]
[RETURN expressions]
[ORDER BY columns]
[SKIP n]
[LIMIT n]
```

---

## MATCH Patterns

### Nodes

```cypher
(n)                          -- Any node
(p:Person)                   -- Labeled node
(p:Person:Employee)          -- Multiple labels
(p:Person {name: 'Alice'})   -- Inline property filter
```

### Relationships

```cypher
(a)-[r]->(b)                 -- Directed (outgoing)
(a)<-[r]-(b)                 -- Directed (incoming)
(a)-[r]-(b)                  -- Undirected (either direction)
(a)-[r:KNOWS]->(b)           -- Typed relationship
(a)-[r:KNOWS|LIKES]->(b)     -- Multiple types (disjunction)
(a)-[r:KNOWS {since: 2020}]->(b)  -- With properties
```

### Variable-Length Paths

```cypher
(a)-[:KNOWS*2]->(b)          -- Exactly 2 hops
(a)-[:KNOWS*1..3]->(b)       -- 1 to 3 hops
(a)-[:KNOWS*..5]->(b)        -- Up to 5 hops
(a)-[:KNOWS*]->(b)           -- Any length
```

### Path Variables

```cypher
path = (a)-[:KNOWS*1..5]->(b)
RETURN length(path), nodes(path), relationships(path)
```

### OPTIONAL MATCH

Left outer join — returns NULL for non-matching parts.

```cypher
MATCH (p:Person)
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN p.name, c.name  -- c.name is NULL if no match
```

---

## WHERE Filtering

### Comparison Operators
```cypher
WHERE p.age > 25
WHERE p.age >= 25 AND p.age <= 65
WHERE p.name <> 'Unknown'
WHERE p.name = 'Alice' OR p.name = 'Bob'
WHERE NOT p.active = false
```

### NULL Handling
```cypher
WHERE p.email IS NOT NULL
WHERE p.deleted IS NULL
```

### String Predicates
```cypher
WHERE p.name STARTS WITH 'A'
WHERE p.name ENDS WITH 'son'
WHERE p.name CONTAINS 'ali'
WHERE p.name =~ '(?i)alice.*'    -- Regex (Rust regex engine)
```

### List Predicates
```cypher
WHERE p.age IN [25, 30, 35]
WHERE NOT p.status IN ['banned', 'suspended']
```

### Property Existence
```cypher
WHERE EXISTS(p.email)
```

### Pattern Predicates
```cypher
WHERE (p)-[:KNOWS]->(:Person {name: 'Bob'})
```

---

## RETURN & Projections

```cypher
RETURN p.name                        -- Property access
RETURN p.name AS person_name         -- Alias
RETURN DISTINCT p.city               -- Deduplicate
RETURN p.name, p.age, p.email        -- Multiple columns
RETURN *                             -- All bound variables
RETURN p                             -- Full node (all properties)
```

### ORDER BY, SKIP, LIMIT

```cypher
RETURN p.name, p.age
ORDER BY p.age DESC, p.name ASC
SKIP 10
LIMIT 20
```

---

## WITH Clause

Pipeline intermediate results. Useful for multi-stage queries.

```cypher
MATCH (p:Person)-[:KNOWS]->(f)
WITH p, COUNT(f) AS friend_count
WHERE friend_count > 5
RETURN p.name, friend_count
ORDER BY friend_count DESC
```

---

## Mutations

### CREATE

```cypher
CREATE (p:Person {name: 'Alice', age: 30})
CREATE (p:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(q:Person {name: 'Bob'})

-- Create edge between existing nodes
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS]->(b)
```

### MERGE (Upsert)

```cypher
MERGE (p:Person {name: 'Alice'})
ON CREATE SET p.created = datetime()
ON MATCH SET p.last_seen = datetime()

MERGE (a)-[:KNOWS]->(b)
```

### SET (Update)

```cypher
MATCH (p:Person {name: 'Alice'})
SET p.age = 31, p.updated = true
```

### REMOVE

```cypher
MATCH (p:Person {name: 'Alice'})
REMOVE p.temporary_field
```

### DELETE

```cypher
-- Delete node (must have no relationships)
MATCH (p:Person {name: 'Alice'}) DELETE p

-- Delete node and all its relationships
MATCH (p:Person {name: 'Alice'}) DETACH DELETE p

-- Delete relationship only
MATCH (a)-[r:KNOWS]->(b) DELETE r
```

### UNWIND

Expand a list into rows.

```cypher
UNWIND ['Alice', 'Bob', 'Carol'] AS name
CREATE (:Person {name: name})

UNWIND $items AS item
CREATE (:Product {name: item.name, price: item.price})
```

---

## Aggregations

```cypher
RETURN COUNT(*)                      -- Row count
RETURN COUNT(p.email)                -- Non-null count
RETURN COUNT(DISTINCT p.city)        -- Distinct count
RETURN SUM(p.salary)                 -- Sum
RETURN AVG(p.age)                    -- Average
RETURN MIN(p.age), MAX(p.age)        -- Min/Max
RETURN COLLECT(p.name)               -- Collect into list
```

Non-aggregated columns in RETURN become implicit GROUP BY keys:

```cypher
MATCH (p:Person)
RETURN p.city, COUNT(*) AS population, AVG(p.age) AS avg_age
ORDER BY population DESC
```

### Window Functions

```cypher
MATCH (p:Person)
RETURN p.name, p.department,
       rank() OVER (PARTITION BY p.department ORDER BY p.salary DESC) AS rank,
       row_number() OVER (ORDER BY p.salary DESC) AS row_num
```

---

## Functions

### String Functions
| Function | Description |
|----------|-------------|
| `toUpper(s)` | Uppercase |
| `toLower(s)` | Lowercase |
| `trim(s)` | Strip whitespace |
| `ltrim(s)` / `rtrim(s)` | Left/right trim |
| `substring(s, start, len)` | Substring |
| `left(s, n)` / `right(s, n)` | Left/right chars |
| `replace(s, from, to)` | Replace substring |
| `split(s, delim)` | Split to list |
| `reverse(s)` | Reverse string |
| `size(s)` | String length |

### Numeric Functions
| Function | Description |
|----------|-------------|
| `abs(n)` | Absolute value |
| `ceil(n)` / `floor(n)` | Ceiling/floor |
| `round(n)` | Round |
| `sqrt(n)` | Square root |
| `sign(n)` | Sign (-1, 0, 1) |
| `log(n)` / `log10(n)` | Logarithm |
| `exp(n)` / `power(b, e)` | Exponential/power |
| `sin(n)` / `cos(n)` / `tan(n)` | Trigonometric |

### List Functions
| Function | Description |
|----------|-------------|
| `size(list)` | List length |
| `head(list)` | First element |
| `tail(list)` | All but first |
| `last(list)` | Last element |
| `range(start, end, step)` | Generate sequence |
| `keys(map)` | Map keys |

### Type Conversion
| Function | Description |
|----------|-------------|
| `toInteger(x)` | Convert to int |
| `toFloat(x)` | Convert to float |
| `toString(x)` | Convert to string |
| `toBoolean(x)` | Convert to bool |

### Null Handling
| Function | Description |
|----------|-------------|
| `COALESCE(a, b, ...)` | First non-null |
| `NULLIF(a, b)` | NULL if equal |

### Path Functions
| Function | Description |
|----------|-------------|
| `length(path)` | Relationship count |
| `nodes(path)` | All nodes in path |
| `relationships(path)` | All relationships |

---

## Advanced Features

### CASE Expressions

```cypher
RETURN
  CASE
    WHEN p.age < 18 THEN 'minor'
    WHEN p.age < 65 THEN 'adult'
    ELSE 'senior'
  END AS category
```

### UNION

```cypher
MATCH (p:Person) RETURN p.name AS name
UNION
MATCH (c:Company) RETURN c.name AS name
```

### Parameters

Use `$param` syntax. Never interpolate user input into Cypher strings.

```cypher
MATCH (p:Person) WHERE p.name = $name RETURN p
```

### Session Variables

```cypher
MATCH (p:Person) WHERE p.org = $session.tenant_id RETURN p
```

### Internal Properties

```cypher
-- _vid: internal vertex ID (useful for deduplication in cycles)
MATCH (a)-[]->(b)-[]->(c)-[]->(a)
WHERE a._vid < b._vid AND a._vid < c._vid
RETURN a.name, b.name, c.name
```

### EXPLAIN / PROFILE

```cypher
EXPLAIN MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, f.name
PROFILE MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, f.name
```

### Index DDL

```cypher
CREATE INDEX ON :Person(email)
CREATE VECTOR INDEX ON :Paper(embedding) OPTIONS {metric: 'cosine'}
DROP INDEX index_name
SHOW INDEXES
```
