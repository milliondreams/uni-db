# Schema Design & Indexing Guide

## Table of Contents
1. [Core Principles](#core-principles)
2. [Labels (Node Types)](#labels)
3. [Edge Types](#edge-types)
4. [Properties](#properties)
5. [Vector Properties](#vector-properties)
6. [Schema vs Overflow Properties](#schema-vs-overflow)
7. [Indexing Strategy](#indexing-strategy)
8. [Schema Evolution](#schema-evolution)
9. [Validation Checklist](#validation-checklist)
10. [Common Patterns](#common-patterns)

---

## Core Principles

1. **Model the domain, not queries** — Design around real-world entities. A Paper, Author, and Venue are better than a QueryResult or SearchIndex.
2. **Labels for types, properties for states** — `:Order {status: 'shipped'}` not `:ShippedOrder`. Labels are stable classifications; states change.
3. **Relationships as edges** — Don't store `friend_ids: [1, 2, 3]` as a property. Use `(:Person)-[:KNOWS]->(:Person)` for traversable connections.
4. **Keep vertices focused** — Each vertex should represent one cohesive entity. Avoid kitchen-sink vertices that borrow data from related entities.
5. **Define schema up front** — Schema properties are stored as typed Arrow columns (2-3x faster filtering, indexable). Overflow properties are JSON blobs (flexible but slower).

---

## Labels

### Naming
- Singular nouns in PascalCase: `Person`, `ResearchPaper`, `BankAccount`
- Descriptive, no abbreviations: `Transaction` not `Txn`
- Avoid generic labels: `Entity`, `Item`, `Thing` — too broad for efficient queries

### Granularity

**Too few** (`:Entity {type: 'paper'}`) — Every query scans all entities, no type safety.

**Too many** (`:NeurIPSPaper`, `:ICMLPaper`) — Fragmented storage, complex queries.

**Just right** (`:Paper {venue: 'NeurIPS'}`) — Fundamental types with properties for variations.

### Multi-Label Vertices

Use multiple labels when an entity has multiple stable classifications:

```cypher
CREATE (n:Person:Employee {name: 'Alice'})
CREATE (n:Paper:Preprint {title: 'Draft'})
```

---

## Edge Types

### Naming
- UPPER_SNAKE_CASE verb phrases: `KNOWS`, `WORKS_AT`, `AUTHORED_BY`, `SENT_MONEY`
- Active voice: `CITES` not `CITED_BY` (you can query in both directions regardless)
- Past or present tense based on semantics

### Direction
Choose direction based on natural reading: "Author WROTE Paper", "Paper CITES Paper". Queries can traverse in either direction using `<-` or `-` regardless of creation direction.

### Edge Properties
Use for relationship metadata: timestamps, weights, roles, quantities.

```cypher
CREATE (a)-[:TRANSFER {amount: 9500.0, date: datetime('2024-01-15')}]->(b)
CREATE (a)-[:WORKS_AT {role: 'engineer', since: 2020}]->(c)
```

### Source/Target Constraints
Edge types can optionally constrain which labels they connect:

```python
# AUTHORED can only go from Author to Paper
db.schema().edge_type("AUTHORED", ["Author"], ["Paper"]).done().apply()

# KNOWS is between Persons only
db.schema().edge_type("KNOWS", ["Person"], ["Person"]).done().apply()

# TRANSFER between any Account types
db.schema().edge_type("TRANSFER", ["Account", "ExternalAccount"], ["Account", "ExternalAccount"]).done().apply()
```

---

## Properties

### Data Types

| Type | Python | Use For |
|------|--------|---------|
| `string` | `str` | Names, IDs, text |
| `int64` | `int` | Counts, years, timestamps |
| `int32` | `int` | Small integers, positions |
| `float64` | `float` | Prices, scores, weights |
| `float32` | `float` | Lower-precision numerics |
| `bool` | `bool` | Flags, states |
| `datetime` | `datetime` | Timestamps |
| `date` | `date` | Calendar dates |
| `time` | `time` | Time of day |
| `duration` | `timedelta` | Time spans |
| `json` | `dict` | Flexible/nested data |
| `vector:N` | `list[float]` | Embeddings (fixed dimension) |
| `list:T` | `list[T]` | Typed arrays |

### Nullability

Be intentional:
- `.property(name, type)` → NOT NULL. Insert/update will fail if missing.
- `.property_nullable(name, type)` → Nullable. Existing rows get NULL when property is added.

Rule of thumb: Primary identifiers and core attributes should be non-nullable. Supplementary data can be nullable.

### Naming
- snake_case: `first_name`, `created_at`, `risk_score`
- No label prefixes: `name` not `person_name` (the label provides context)
- Descriptive: `publication_year` not `yr`

---

## Vector Properties

### Choosing Dimensions
Dimensions are immutable after creation. Plan carefully:

| Model Family | Typical Dimensions |
|---|---|
| Sentence Transformers (MiniLM) | 384 |
| Sentence Transformers (MPNet) | 768 |
| OpenAI text-embedding-3-small | 1536 |
| OpenAI text-embedding-3-large | 3072 |
| CLIP (image+text) | 512-768 |
| Cohere embed-v3 | 1024 |

### Multiple Embeddings
Use separate properties for different embedding types:

```python
db.schema().label("Paper") \
    .vector("title_embedding", 384) \
    .vector("abstract_embedding", 768) \
    .done().apply()
```

### Versioning
When upgrading embedding models, keep legacy embeddings until all data is re-embedded:

```python
db.schema().label("Paper") \
    .vector("embedding", 768) \
    .vector("embedding_v2", 1024) \
    .done().apply()
```

---

## Schema vs Overflow

Uni supports both schema-defined and overflow (schemaless) properties on every vertex.

### Schema Properties (Typed Columns)
- Stored as typed Arrow columns in Lance datasets
- 2-3x faster for WHERE filtering, ORDER BY, aggregations
- Support indexes (BTree, Hash, Vector, etc.)
- Type-checked at write time
- Best for: Frequently queried/filtered, core entity attributes

### Overflow Properties (JSON Blob)
- Stored as a single JSON binary column
- No indexing support
- Slower for filtering (JSONB parsing at query time)
- No schema migration needed — just write new keys
- Best for: Rapidly evolving fields, prototyping, optional metadata

### Migration Path
Start with overflow for exploratory properties. When a property becomes frequently queried, promote it to the schema:

```python
# Add to schema (existing overflow values are NOT migrated automatically)
db.add_property("Paper", "citation_count", "int64", nullable=True)
```

---

## Indexing Strategy

### Index Types

**BTree** — General purpose. Equality (`=`), range (`>`, `<`, `>=`, `<=`), and `IN` queries.
```python
db.create_scalar_index("Person", "email", "btree")
```

**Hash** — Fastest for pure equality (`=`). No range support.
```python
db.create_scalar_index("Person", "external_id", "hash")
```

**Bitmap** — Low-cardinality columns (status, category, type). Very compact.
```python
db.create_scalar_index("Order", "status", "bitmap")
```

**Vector (HNSW)** — Default vector index. Good recall/speed tradeoff. Best for most use cases.
```python
db.create_vector_index("Paper", "embedding", "cosine")
```

**Vector (IVF_PQ)** — For very large datasets (>10M vectors). Lower memory, slightly lower recall.

**Full-text** — BM25 keyword search on text or JSON properties.
```cypher
CREATE JSON FULLTEXT INDEX ON :Paper(metadata)
```

### When to Index

| Scenario | Index Type |
|----------|-----------|
| Filter by ID or email | BTree or Hash |
| Range queries (year > 2020) | BTree |
| Status/category filtering | Bitmap |
| Vector similarity search | HNSW (default) or IVF_PQ (large scale) |
| Keyword search in text | Full-text |

### When NOT to Index
- Properties rarely used in WHERE clauses
- Properties only used in RETURN (no filtering benefit)
- Very high-cardinality properties that are rarely filtered on
- Note: Each index adds write overhead

### Predicate Pushdown

These predicates push to the storage layer (use indexes, skip irrelevant data):
- `=`, `<>`, `<`, `>`, `<=`, `>=`
- `IN [list]`
- `IS NULL`, `IS NOT NULL`
- `AND` combinations

These require scanning (no pushdown):
- `CONTAINS`, `STARTS WITH`, `ENDS WITH`
- Regex (`=~`)
- Function calls (`lower(x)`, `toUpper(x)`)
- `OR` across different properties

Design queries to use pushable predicates when possible.

---

## Schema Evolution

### Safe Operations
- Adding new properties (existing rows get NULL)
- Adding new labels and edge types
- Adding indexes on existing properties

### Breaking Changes (Avoid)
- Changing property data types
- Changing vector dimensions (immutable)
- Renaming labels or edge types
- Changing edge direction semantics

### Deprecation Strategy
When planning to remove a property:
1. Add replacement property
2. Backfill data
3. Update queries to use new property
4. Remove old property in a future release

---

## Validation Checklist

Before committing a schema:

- [ ] Labels are singular PascalCase
- [ ] Edge types are UPPER_SNAKE_CASE verb phrases
- [ ] Properties are snake_case
- [ ] Required properties are non-nullable
- [ ] Vector dimensions match your embedding model
- [ ] Edge type constraints match domain rules
- [ ] Indexes planned for common query filter patterns
- [ ] No duplicate data that should be edges instead

---

## Common Patterns

### Social Network
```python
db.schema() \
  .label("User") \
    .property("username", "string") \
    .property("email", "string") \
    .property_nullable("bio", "string") \
  .done() \
  .edge_type("FOLLOWS", ["User"], ["User"]) \
    .property("since", "datetime") \
  .done() \
  .edge_type("LIKES", ["User"], ["Post"]) \
  .done() \
  .label("Post") \
    .property("content", "string") \
    .property("created_at", "datetime") \
  .done() \
  .apply()

db.create_scalar_index("User", "username", "btree")
db.create_scalar_index("User", "email", "hash")
```

### Knowledge Graph with Embeddings
```python
db.schema() \
  .label("Document") \
    .property("title", "string") \
    .property("source", "string") \
    .property_nullable("content", "string") \
    .vector("embedding", 768) \
  .done() \
  .label("Entity") \
    .property("name", "string") \
    .property("type", "string") \
  .done() \
  .edge_type("MENTIONS", ["Document"], ["Entity"]) \
    .property_nullable("confidence", "float64") \
  .done() \
  .edge_type("RELATED_TO", ["Entity"], ["Entity"]) \
    .property("relation", "string") \
  .done() \
  .apply()

db.create_vector_index("Document", "embedding", "cosine")
db.create_scalar_index("Entity", "name", "btree")
db.create_scalar_index("Entity", "type", "bitmap")
```

### Financial Transactions
```python
db.schema() \
  .label("Account") \
    .property("account_id", "string") \
    .property("holder_name", "string") \
    .property("balance", "float64") \
    .property_nullable("risk_score", "float32") \
    .property("flagged", "bool") \
  .done() \
  .edge_type("TRANSFER", ["Account"], ["Account"]) \
    .property("amount", "float64") \
    .property("timestamp", "datetime") \
    .property_nullable("memo", "string") \
  .done() \
  .apply()

db.create_scalar_index("Account", "account_id", "hash")
db.create_scalar_index("Account", "flagged", "bitmap")
```

### Supply Chain BOM
```python
db.schema() \
  .label("Part") \
    .property("part_number", "string") \
    .property("name", "string") \
    .property("unit_cost", "float64") \
    .property_nullable("supplier", "string") \
  .done() \
  .label("Product") \
    .property("sku", "string") \
    .property("name", "string") \
  .done() \
  .edge_type("COMPONENT_OF", ["Part"], ["Product"]) \
    .property("quantity", "int32") \
  .done() \
  .edge_type("SUBPART_OF", ["Part"], ["Part"]) \
    .property("quantity", "int32") \
  .done() \
  .apply()

db.create_scalar_index("Part", "part_number", "hash")
db.create_scalar_index("Product", "sku", "hash")
```
