---
name: uni-db-dev
description: |
  Build applications with Uni, an embedded graph database with OpenCypher queries, vector search, and Locy logic programming. Use this skill whenever the user wants to create, query, or manage a Uni/uni-db database — whether from Rust or Python. Trigger when: code imports `uni_db` or depends on `uni-db` crate, user mentions "uni", "uni-db", "graph database" in context of this project, user writes Cypher queries, user works with Locy programs, user needs vector/hybrid search on a graph, or user is building any application that stores and queries connected data. Also trigger when the user asks about schema design, data ingestion, or query optimization for Uni.
---

# Building Applications with Uni

Uni is an embedded, object-store-backed graph database. It runs in-process (no server) and persists to local disk or cloud object stores (S3/GCS). It supports OpenCypher queries, columnar analytics, vector search, full-text search, and Locy logic programming.

## Quick Start

### Python
```python
import uni_db

db = uni_db.Database("./my_graph")

# Define schema
db.schema() \
  .label("Person") \
    .property("name", "string") \
    .property_nullable("age", "int64") \
  .done() \
  .edge_type("KNOWS", ["Person"], ["Person"]) \
  .done() \
  .apply()

# Write data
db.execute("CREATE (a:Person {name: 'Alice', age: 30})")
db.execute("CREATE (b:Person {name: 'Bob', age: 25})")
db.execute("""
    MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
    CREATE (a)-[:KNOWS]->(b)
""")
db.flush()

# Query
results = db.query("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, f.name")
```

### Rust
```rust
use uni_db::{Uni, DataType};

let db = Uni::open("./my_graph").build().await?;

db.schema()
    .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int64)
    .done()
    .edge_type("KNOWS", &["Person"], &["Person"])
    .done()
    .apply().await?;

db.execute("CREATE (a:Person {name: 'Alice', age: 30})").await?;
db.flush().await?;

let results = db.query("MATCH (p:Person) RETURN p.name, p.age").await?;
```

### Python (Async)
```python
import uni_db

db = await uni_db.AsyncDatabase.open("./my_graph")
await db.schema().label("Person").property("name", "string").done().apply()
await db.execute("CREATE (:Person {name: 'Alice'})")
await db.flush()
results = await db.query("MATCH (p:Person) RETURN p.name")
```

## Database Lifecycle

### Opening / Creating

**Python:**
```python
# Simple
db = uni_db.Database("./path")

# With builder (advanced config)
db = uni_db.DatabaseBuilder.open("./path") \
    .cache_size(512 * 1024 * 1024) \
    .parallelism(4) \
    .build()

# Ephemeral (deleted on close)
db = uni_db.DatabaseBuilder.temporary().build()

# In-memory
db = uni_db.DatabaseBuilder.in_memory().build()

# Hybrid: local cache + remote storage
db = uni_db.DatabaseBuilder.open("./local_cache") \
    .hybrid("./local_cache", "s3://bucket/prefix") \
    .build()
```

**Rust:**
```rust
let db = Uni::open("./path").build().await?;
let db = Uni::temporary().build().await?;
let db = Uni::in_memory().build().await?;
let db = Uni::open("./local")
    .hybrid("./local", "s3://bucket/prefix")
    .cache_size(512 * 1024 * 1024)
    .parallelism(4)
    .build().await?;
```

### Flushing & Shutdown

Always call `flush()` after writes to persist data to storage. In Rust, call `shutdown()` for graceful cleanup.

```python
db.flush()        # Persist uncommitted changes
```
```rust
db.flush().await?;
db.shutdown().await?;
```

## Schema Design & Indexing

Schema is recommended for all production use. It gives you typed Arrow columns (2-3x faster filtering), indexing, and constraint enforcement. Schema is additive — add new labels, properties, and edge types at any time without migration.

Read `references/schema-design.md` for the full guide: modeling principles, naming conventions, data types, index types, predicate pushdown, schema evolution, and common domain patterns.

### Fluent Schema Builder

```python
db.schema() \
  .label("Paper") \
    .property("title", "string") \
    .property("year", "int64") \
    .property_nullable("abstract", "string") \
    .vector("embedding", 768) \
  .done() \
  .label("Author") \
    .property("name", "string") \
  .done() \
  .edge_type("AUTHORED", ["Author"], ["Paper"]) \
    .property("position", "int32") \
  .done() \
  .edge_type("CITES", ["Paper"], ["Paper"]) \
  .done() \
  .apply()
```

### Indexes

```python
db.create_scalar_index("Person", "email", "btree")       # Equality + range
db.create_scalar_index("Order", "status", "bitmap")       # Low-cardinality
db.create_vector_index("Paper", "embedding", "cosine")    # Vector similarity
```

## Querying with Cypher

Uni supports a substantial subset of OpenCypher. Read `references/cypher-reference.md` for the complete reference.

### Core Patterns

```cypher
-- Node matching with filtering
MATCH (p:Person) WHERE p.age > 25 RETURN p.name, p.age ORDER BY p.age DESC LIMIT 10

-- Relationship traversal
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name

-- Variable-length paths
MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b:Person) RETURN b.name

-- Aggregations
MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, COUNT(f) AS friends ORDER BY friends DESC

-- WITH for multi-stage queries
MATCH (p:Person)-[:KNOWS]->(f)
WITH p, COUNT(f) AS cnt
WHERE cnt > 5
RETURN p.name, cnt

-- OPTIONAL MATCH (left outer join)
MATCH (p:Person)
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN p.name, c.name

-- UNWIND lists
UNWIND ['Alice', 'Bob', 'Carol'] AS name
CREATE (:Person {name: name})
```

### Parameterized Queries

Always use parameters for user-supplied values — never interpolate strings into Cypher.

```python
# Python
results = db.query(
    "MATCH (p:Person) WHERE p.name = $name RETURN p",
    {"name": "Alice"}
)

# With QueryBuilder (adds timeout/memory limits)
results = db.query_with("MATCH (p:Person) WHERE p.age > $min RETURN p") \
    .param("min", 25) \
    .timeout(5.0) \
    .fetch_all()
```

```rust
// Rust
let results = db.query_with("MATCH (p:Person) WHERE p.name = $name RETURN p")
    .param("name", "Alice")
    .timeout(Duration::from_secs(5))
    .fetch_all().await?;
```

### Mutations

```cypher
-- Create nodes
CREATE (p:Person {name: 'Alice', age: 30})

-- Create edges (match endpoints first)
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2020}]->(b)

-- MERGE (upsert)
MERGE (p:Person {name: 'Alice'})
ON CREATE SET p.created = datetime()
ON MATCH SET p.last_seen = datetime()

-- Update properties
MATCH (p:Person {name: 'Alice'}) SET p.age = 31

-- Delete
MATCH (p:Person {name: 'Alice'}) DETACH DELETE p
```

## Vector Search

Uni has first-class vector similarity search integrated with graph traversal. Read `references/vector-search.md` for the complete reference.

### Basic KNN

```cypher
CALL uni.vector.query('Paper', 'embedding', $vec, 10)
YIELD node, distance
RETURN node.title, distance
ORDER BY distance
```

### With Pre-filtering and Threshold

```cypher
CALL uni.vector.query('Paper', 'embedding', $vec, 100, 'year > 2020', 0.5)
YIELD node, distance, score
RETURN node.title, score
```

### Full-Text Search

```cypher
CALL uni.fts.query('Paper', 'abstract', 'neural networks', 20)
YIELD node, score
RETURN node.title, score
```

### Hybrid Search (Vector + FTS)

```cypher
CALL uni.search('Paper', {vector: 'embedding', fts: 'abstract'}, 'transformers', null, 10)
YIELD node, score, vector_score, fts_score
RETURN node.title, score
```

## Bulk Loading

For loading large datasets, use the bulk writer instead of individual CREATE statements.

```python
# Simple bulk insert
vids = db.bulk_insert_vertices("Person", [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
    {"name": "Carol", "age": 35},
])
alice, bob, carol = vids

db.bulk_insert_edges("KNOWS", [
    (alice, bob, {"since": 2020}),
    (bob, carol, {"since": 2021}),
])
db.flush()
```

```python
# Advanced bulk writer with deferred indexes
writer = db.bulk_writer() \
    .defer_vector_indexes(True) \
    .batch_size(10000) \
    .build()

vids = writer.insert_vertices("Paper", papers_list)
writer.insert_edges("CITES", edges_list)
stats = writer.commit()  # Returns BulkStats
```

## Transactions

```python
# Python
txn = db.begin()
txn.query("CREATE (:Person {name: 'Dave'})")
txn.commit()  # or txn.rollback()
```

```rust
// Rust - closure-based (auto-rollback on error)
db.transaction(|txn| async move {
    txn.execute("CREATE (:Person {name: 'Dave'})").await?;
    Ok(())
}).await?;
```

## Sessions (Multi-tenant Context)

Sessions carry scoped variables accessible in queries as `$session.varname`.

```python
session = db.session().set("tenant_id", "acme").build()
results = session.query(
    "MATCH (p:Person) WHERE p.tenant = $session.tenant_id RETURN p"
)
```

## Locy (Logic Programming)

Locy extends Cypher with declarative rules for recursive reasoning, compliance modeling, and risk propagation. Read `references/locy-reference.md` for the complete reference.

### Basic Example

```python
program = r'''
CREATE RULE reachable AS
MATCH (a:Person)-[:KNOWS]->(b:Person)
YIELD KEY a, KEY b

CREATE RULE reachable AS
MATCH (a:Person)-[:KNOWS]->(mid:Person)
WHERE (mid, b) IS reachable
YIELD KEY a, KEY b

QUERY reachable WHERE a.name = 'Alice' RETURN b.name AS reachable_person
'''

result = db.locy_evaluate(program)
for row in result["command_results"][0]["rows"]:
    print(row["reachable_person"])
```

### Key Concepts

- **Rules**: Declarative relations computed via fixpoint evaluation
- **Recursion**: Rules can reference themselves (positive recursion) or other rules
- **Stratified negation**: `IS NOT` only for rules in earlier strata
- **ALONG**: Carry state through recursive paths (e.g., cumulative cost)
- **FOLD**: Aggregate over derived facts (SUM, MAX, COUNT, MNOR, MPROD)
- **similar_to()**: Inline similarity scoring expression (vector, FTS, or multi-source fusion)
- **BEST BY**: Keep optimal derivations per key group
- **ASSUME...THEN**: Hypothetical what-if analysis (mutations rolled back)
- **ABDUCE**: Find minimal graph changes to satisfy/prevent conditions
- **EXPLAIN RULE**: Proof traces showing derivation paths

## Query Profiling

```python
# Explain (plan without execution)
plan = db.explain("MATCH (p:Person) RETURN p.name")

# Profile (plan + execution stats)
results, stats = db.profile("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, f.name")
```

## Reference Files

For detailed API documentation, consult these reference files:

| File | Contents |
|------|----------|
| `references/schema-design.md` | Schema modeling, naming conventions, indexing strategy, common patterns |
| `references/cypher-reference.md` | Complete Cypher syntax: clauses, operators, functions, patterns |
| `references/vector-search.md` | Vector, FTS, and hybrid search procedures with all parameters |
| `references/locy-reference.md` | Locy syntax, rules, commands, ALONG/FOLD/BEST BY, ASSUME/ABDUCE |
| `references/python-api.md` | Full Python API: Database, AsyncDatabase, builders, types |
| `references/rust-api.md` | Full Rust API: Uni, query, schema, bulk, transactions, sessions |

Read the relevant reference file when you need detailed signatures, edge cases, or advanced features beyond what's shown in this overview.

## Common Patterns

### Graph + Vector (RAG)
```cypher
-- Find similar documents, then traverse to related entities
CALL uni.vector.query('Document', 'embedding', $query_vec, 5)
YIELD node, distance
MATCH (node)-[:MENTIONS]->(entity:Entity)
RETURN node.title, entity.name, distance
```

### Fraud Detection (Ring Detection)
```cypher
MATCH (a:Account)-[:TRANSFER]->(b:Account)-[:TRANSFER]->(c:Account)-[:TRANSFER]->(a)
WHERE a._vid < b._vid AND a._vid < c._vid
RETURN a.id, b.id, c.id
```

### Recommendation (Collaborative Filtering)
```cypher
MATCH (me:User {id: $uid})-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(other:User)
MATCH (other)-[:PURCHASED]->(rec:Product)
WHERE NOT (me)-[:PURCHASED]->(rec)
RETURN rec.name, COUNT(other) AS score
ORDER BY score DESC LIMIT 10
```

### Risk Propagation (Locy)
```python
program = r'''
CREATE RULE risky AS
MATCH (a:Account) WHERE a.flagged = true
YIELD KEY a

CREATE RULE risky AS
MATCH (a:Account)-[:TRANSFER]->(b:Account)
WHERE b IS risky
YIELD KEY a

QUERY risky RETURN a.id AS risky_account
'''
result = db.locy_evaluate(program)
```

## Installation

### Python
```bash
pip install uni-db
```

### Rust
```toml
[dependencies]
uni-db = "0.1"
tokio = { version = "1", features = ["full"] }
```
