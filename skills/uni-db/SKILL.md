---
name: uni-db
description: >-
  Comprehensive developer reference for uni-db, an embedded multi-model graph database
  with OpenCypher querying, Locy logic programming, vector/hybrid search, and Pydantic OGM.
  ALWAYS use this skill when: writing code that imports uni_db or uni_pydantic, writing
  Cypher queries for uni-db, writing Locy rules, configuring vector or hybrid search,
  defining uni-db schemas, using graph algorithms, or answering questions about uni-db
  architecture, APIs, or features. Trigger on mentions of: uni-db, uni, Cypher queries
  in graph DB context, Locy rules, vector search with graph data, Pydantic OGM for graphs,
  graph algorithms, ALONG/FOLD/BEST BY/DERIVE/ASSUME/ABDUCE, embedded graph database.
---

# uni-db Developer Skill

## What is uni-db?

uni-db is an **embedded, serverless multi-model graph database** (graph + vector + document + columnar) that runs inside your process with no server required. It supports **OpenCypher** queries with extensions for vector search, full-text search, DDL, and time travel. **Locy** is its Datalog-inspired logic programming language for recursive rules, probabilistic reasoning, and abductive inference. APIs are available in **Python** (sync/async via PyO3) and **Rust** (async/blocking), with a **Pydantic OGM** layer. Built-in capabilities include 8 vector index algorithms (Flat, IVF-Flat/SQ/PQ/RQ, HNSW-Flat/SQ/PQ) with scalar, product, and RaBitQ quantization, 4 scalar index types (BTree, Hash, Bitmap, LabelList), BM25 full-text search, hybrid search with RRF fusion, and 36+ graph algorithms.

---

## Architecture: Three Scopes

uni-db uses three scoping levels that separate lifecycle, reads, and writes:

```
Uni / AsyncUni (database handle)
  +-- Factory: open(), session(), schema()
  +-- Admin: flush(), snapshots, indexes, compaction
  +-- NO direct query or mutation

Session / AsyncSession (read scope)
  +-- Parameters: set(), get()
  +-- Reads: query(), locy()
  +-- Analysis: explain(), profile()
  +-- Factory: tx() -> Transaction

Transaction / AsyncTransaction (write scope)
  +-- Reads: query() (sees uncommitted writes)
  +-- Writes: execute(), bulk loading
  +-- Locy: locy() (DERIVE auto-applies mutations)
  +-- Lifecycle: commit(), rollback()
```

**Pattern**: `db = Uni.open(path)` -> `session = db.session()` -> `tx = session.tx()` -> `tx.execute(...)` -> `tx.commit()`

**Sync/async duality**: every type has an async counterpart (`Uni` / `AsyncUni`, `Session` / `AsyncSession`, `Transaction` / `AsyncTransaction`). Shared types (results, data classes, exceptions) are the same for both.

**Single-writer, multi-reader**: only one Transaction can be open at a time per Uni instance. Multiple Sessions can read concurrently with snapshot isolation.

**Facade accessors** on `Uni` / `AsyncUni`:

| Accessor | Returns | Purpose |
|---|---|---|
| `db.rules()` | `RuleRegistry` | Locy rule management (register, list, remove) |
| `db.compaction()` | `Compaction` / `AsyncCompaction` | Storage compaction |
| `db.indexes()` | `Indexes` / `AsyncIndexes` | Index management (list, rebuild) |
| `db.xervo()` | `Xervo` / `AsyncXervo` | ML model runtime (embed, generate) |

**Key builder terminal methods:**

| Builder | Created by | Terminal |
|---|---|---|
| `SessionQueryBuilder` | `session.query_with(cypher)` | `.fetch_all()`, `.fetch_one()`, `.cursor()` |
| `SessionLocyBuilder` | `session.locy_with(program)` | `.run()` |
| `TxExecuteBuilder` | `tx.execute_with(cypher)` | `.run()` |
| `TxQueryBuilder` | `tx.query_with(cypher)` | `.fetch_all()`, `.fetch_one()`, `.execute()` |
| `SchemaBuilder` | `db.schema()` | `.apply()` |
| `BulkWriterBuilder` | `tx.bulk_writer()` | `.build()` |
| `TransactionBuilder` | `session.tx_with()` | `.start()` |

---

## Quick Start: Python

```python
from uni_db import Uni, DataType

# Open or create a database
db = Uni.open("./my_db")

# Define schema
db.schema() \
    .label("Person") \
        .property("name", DataType.STRING()) \
        .property("age", DataType.INT64()) \
    .apply()

# Write via transaction
session = db.session()
with session.tx() as tx:
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})")
    tx.commit()

# Read via session
result = session.query("MATCH (p:Person) RETURN p.name, p.age")
for row in result:
    print(f"{row['p.name']}: {row['p.age']}")

db.shutdown()
```

---

## Quick Start: Rust

```rust
use uni_db::{Uni, DataType, Value};

#[tokio::main]
async fn main() -> uni_db::Result<()> {
    // Open (or create) a database
    let db = Uni::open("./my_db").build().await?;

    // Define schema
    db.schema()
        .label("Person")
            .property("name", DataType::String)
            .property("age", DataType::Int64)
        .apply().await?;

    // Write via transaction
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})").await?;
    tx.commit().await?;

    // Read via session
    let result = session.query("MATCH (p:Person) RETURN p.name, p.age").await?;
    for row in result.rows() {
        println!("{}: {}", row.get::<String>("p.name")?, row.get::<i64>("p.age")?);
    }

    db.shutdown().await
}
```

**Database factory methods** (same in Python and Rust):

| Method | Behavior |
|---|---|
| `Uni.open(path)` | Open existing or create new database at path |
| `Uni.create(path)` | Create new database; error if path exists |
| `Uni.open_existing(path)` | Open existing; error if path does not exist |
| `Uni.temporary()` | Temp directory, auto-cleaned on drop |
| `Uni.in_memory()` | Purely in-memory, no persistence |
| `Uni.builder()` | Advanced configuration via `UniBuilder` |

**Storage backends**: local filesystem, S3 (`s3://bucket/path`), GCS (`gs://bucket/path`), Azure (`az://account/container/path`).

---

## Data Types Quick Reference

| Uni Type | Python Factory | Rust Enum | Cypher DDL |
|---|---|---|---|
| String | `DataType.STRING()` | `DataType::String` | `STRING` |
| Int32 | `DataType.INT32()` | `DataType::Int32` | `INT32` |
| Int64 | `DataType.INT64()` | `DataType::Int64` | `INT64` |
| Float32 | `DataType.FLOAT32()` | `DataType::Float32` | `FLOAT32` |
| Float64 | `DataType.FLOAT64()` | `DataType::Float64` | `FLOAT64` |
| Bool | `DataType.BOOL()` | `DataType::Bool` | `BOOL` |
| Timestamp | `DataType.TIMESTAMP()` | `DataType::Timestamp` | `TIMESTAMP` |
| Date | `DataType.DATE()` | `DataType::Date` | `DATE` |
| DateTime | `DataType.DATETIME()` | `DataType::DateTime` | `DATETIME` |
| Duration | `DataType.DURATION()` | `DataType::Duration` | `DURATION` |
| Btic | `DataType.BTIC()` | `DataType::Btic` | `BTIC` |
| Vector(N) | `DataType.vector(N)` | `DataType::Vector { dimensions: N }` | `VECTOR(N)` |
| List(T) | `DataType.list(inner)` | `DataType::List(Box<T>)` | `LIST(T)` |
| Map(K,V) | `DataType.map(k, v)` | `DataType::Map(Box<K>, Box<V>)` | `MAP(K, V)` |
| JSON | `DataType.JSON()` | `DataType::CypherValue` | `JSON` |
| CRDT types | `DataType.crdt(CrdtType.G_COUNTER())` | `DataType::Crdt(CrdtKind::GCounter)` | `CRDT(GCOUNTER)` |

CRDT types: `GCounter`, `GSet`, `ORSet`, `LWWRegister`, `LWWMap`, `Rga`, `VectorClock`, `VCRegister`.

---

## Essential Patterns

### CRUD

```cypher
-- Create node
CREATE (n:Person {name: 'Alice', age: 30})

-- Create node with ext_id (for MERGE/lookup)
CREATE (n:Person {ext_id: 'user-123', name: 'Alice'})

-- Create edge
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2023}]->(b)

-- Read
MATCH (p:Person) WHERE p.age > 25 RETURN p.name, p.age

-- Update
MATCH (n:Person {name: 'Alice'}) SET n.age = 31, n.updated = datetime()

-- Delete (must have no edges)
MATCH (n:Person {name: 'Alice'}) DELETE n

-- Detach delete (removes edges first)
MATCH (n:Person {name: 'Alice'}) DETACH DELETE n
```

### Parameters

```python
# Python — inline params
result = session.query(
    "MATCH (n:Person {name: $name}) RETURN n",
    params={"name": "Alice"}
)

# Python — session-level params
session.set("min_age", 25)
result = session.query("MATCH (p:Person) WHERE p.age > $min_age RETURN p")

# Python — builder pattern
result = session.query_with("MATCH (n:Person) WHERE n.age > $age") \
    .param("age", 25) \
    .timeout(5.0) \
    .fetch_all()
```

### Transactions

```python
# Context manager — auto-rollback on exception
with session.tx() as tx:
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})")
    tx.execute("CREATE (:Person {name: 'Bob', age: 25})")
    result = tx.commit()
    print(f"Committed {result.mutations_committed} mutations at version {result.version}")

# Async equivalent
async with await session.tx() as tx:
    await tx.execute("CREATE (:Person {name: 'Alice'})")
    await tx.commit()
```

### Bulk Loading

```python
with session.tx() as tx:
    with tx.bulk_writer().batch_size(5000).build() as writer:
        vids = writer.insert_vertices("Person", [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ])
        writer.insert_edges("KNOWS", [(vids[0], vids[1], {"since": 2024})])
        stats = writer.commit()
    tx.commit()
print(f"Inserted {stats.vertices_inserted} vertices, {stats.edges_inserted} edges")
```

### Schema Definition

```python
db.schema() \
    .label("Person") \
        .property("name", DataType.STRING()) \
        .property("age", DataType.INT64()) \
        .vector("embedding", 384) \
        .index("name", "btree") \
    .label("Company") \
        .property("name", DataType.STRING()) \
    .edge_type("WORKS_AT", ["Person"], ["Company"]) \
        .property("since", DataType.DATE()) \
    .apply()
```

### Session Parameters

```python
session = db.session()
session.set("company", "Acme")
result = session.query("MATCH (c:Company {name: $company}) RETURN c")
```

### Vector Search

```cypher
-- Basic vector search
CALL uni.vector.query('Document', 'embedding', $query_vector, 10)
YIELD node, score
RETURN node.title, score ORDER BY score DESC

-- Vector search with metadata filter
CALL uni.vector.query('Document', 'embedding', $query_vector, 20, 'category = "tech"')
YIELD node, score
WHERE score > 0.7
RETURN node.title, score

-- Inline similarity scoring (no CALL/YIELD needed)
MATCH (d:Doc)
RETURN d.title, similar_to(d.embedding, $query_vector) AS score
ORDER BY score DESC

-- Hybrid search: vector + full-text with RRF fusion
CALL uni.search('Document', ['embedding', 'content'], 'graph databases', $query_vector, 10)
YIELD node, score, vector_score, fts_score
RETURN node.title, score
```

### Locy Quick Example

```python
result = session.locy("""
    CREATE RULE reachable AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a IS reachable OR a.name = 'Alice'
        YIELD KEY b

    QUERY reachable
""")
for row in result["reachable"]:
    print(row)
```

---

## Cypher Cheat Sheet

**1. Match by property:**
```cypher
MATCH (n:Person {name: 'Alice'}) RETURN n
```

**2. Match by ext_id:**
```cypher
MATCH (n:Person {ext_id: 'user-123'}) RETURN n
```

**3. Create node with properties:**
```cypher
CREATE (n:Person {ext_id: 'user-456', name: 'Bob', age: 25}) RETURN n
```

**4. Create edge:**
```cypher
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2023}]->(b)
```

**5. MERGE (upsert) -- requires ext_id:**
```cypher
MERGE (n:Person {ext_id: 'user-123'})
ON CREATE SET n.name = 'Alice', n.created = datetime()
ON MATCH SET n.last_seen = datetime()
RETURN n
```

**6. Variable-length path:**
```cypher
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.name = 'Alice'
RETURN DISTINCT b.name
```

**7. Aggregation with WITH:**
```cypher
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WITH c.name AS company, count(p) AS employees
WHERE employees > 10
RETURN company, employees ORDER BY employees DESC
```

**8. UNWIND list:**
```cypher
UNWIND $names AS name
MATCH (n:Person {name: name})
RETURN n
```

**9. OPTIONAL MATCH:**
```cypher
MATCH (p:Person)
OPTIONAL MATCH (p)-[r:MANAGES]->(m:Person)
RETURN p.name, collect(m.name) AS manages
```

**10. RETURN with ORDER BY, LIMIT, SKIP:**
```cypher
MATCH (p:Person)
RETURN p.name, p.age
ORDER BY p.age DESC
SKIP 10 LIMIT 20
```

---

## Critical Gotchas

1. **ext_id is REQUIRED for MERGE** -- Without `ext_id`, `MERGE` always creates new nodes because there is no stable identity to match against. Always include `ext_id` in the MERGE pattern.

2. **CREATE creates NEW nodes per expression** -- `CREATE (a:Node), (b:Node)` creates two separate nodes. To reference the same node later in a pattern, use variable binding: `CREATE (a:Node {name: 'X'}), (a)-[:REL]->(b:Other)`.

3. **Single-writer** -- Only one Transaction can be open at a time per Uni instance. Multiple Sessions can read concurrently.

4. **flush() for durability** -- Writes are buffered in L0; call `db.flush()` or rely on auto-flush (threshold: 10k mutations or 5s interval) for persistence to storage.

5. **VID vs UniId vs ext_id** -- VID is internal (u64 auto-increment), UniId is content-hash (SHA3-256), ext_id is user-supplied string. Use `ext_id` for MERGE and user-facing lookups.

6. **Schema-first for columnar performance** -- Define labels and properties via `db.schema()` before bulk loading. Without schema, properties go to JSONB overflow and lose columnar benefits.

7. **DETACH DELETE required when node has edges** -- `DELETE` alone fails if the node has any edges. Use `DETACH DELETE` to remove edges first.

8. **Vector index metric must match embedding model** -- Use `cosine` for normalized embeddings (most models), `l2` for raw/unnormalized embeddings. Mismatched metric produces poor search results.

9. **Locy rules are NOT standard Datalog** -- Locy has `ALONG`, `FOLD`, `BEST BY`, `PROB`, `DERIVE`, `ASSUME`, `ABDUCE` which do not exist in standard Datalog. IS/IS NOT references invoke other rules.

10. **Unbounded variable-length paths** -- `[*]` without an upper bound causes exponential expansion. Always set an upper bound: `[*..5]`.

11. **Always use $param parameters** -- String concatenation in Cypher causes injection risk and prevents plan caching.

12. **Cartesian products from disconnected patterns** -- `MATCH (a:Person), (b:Company)` creates a cross product of all persons and companies. Connect patterns or use WITH to pipeline results.

13. **BulkWriter for initial data loading** -- Always use `tx.bulk_writer()` for loading more than a few thousand records. It bypasses WAL and defers index rebuilds for 10-100x faster throughput.

14. **Context managers for transactions** -- Always use `with session.tx() as tx:` (or `async with`) to guarantee auto-rollback on exceptions. Forgetting to commit or rollback leaks the write lock.

15. **Locy DERIVE in a transaction** -- When `locy()` is called on a Transaction, DERIVE commands automatically apply mutations to the transaction. On a Session (read-only), DERIVE returns a `DerivedFactSet` that must be explicitly applied via `tx.apply(derived)`.

---

## When to Load References

When the SKILL.md overview is insufficient for the user's task, load the appropriate reference file for detailed API signatures, examples, and patterns.

| User's task | Load reference |
|---|---|
| Writing/debugging Cypher queries, WHERE clauses, pattern matching | `references/cypher.md` |
| Python API usage, async patterns, builders, result types | `references/python-api.md` |
| Rust API usage, builders, error handling, blocking API | `references/rust-api.md` |
| Pydantic models, OGM, QueryBuilder, relationships | `references/pydantic-ogm.md` |
| Vector search, FTS, hybrid search, similar_to, embeddings | `references/vector-hybrid-search.md` |
| Locy rules, recursive logic, ALONG/FOLD/DERIVE/ASSUME/ABDUCE | `references/locy.md` |
| Schema design, data types, indexes, identity (ext_id/VID) | `references/schema-indexing.md` |
| BTIC temporal intervals, Allen algebra, certainty/granularity | `references/btic.md` |
| Xervo ML runtime, providers, model catalog, auto-embedding | `references/xervo.md` |
| Graph algorithms (PageRank, WCC, shortest path, etc.) | `references/graph-algorithms.md` |

Load **multiple references** when a task spans domains. Examples:
- RAG pipeline: `references/vector-hybrid-search.md` + `references/python-api.md`
- Locy with vector similarity: `references/locy.md` + `references/vector-hybrid-search.md`
- Schema + bulk loading: `references/schema-indexing.md` + `references/python-api.md`
- Rust graph algorithms: `references/graph-algorithms.md` + `references/rust-api.md`
- BTIC temporal queries in Python: `references/btic.md` + `references/python-api.md`
- RAG with Xervo embeddings: `references/xervo.md` + `references/vector-hybrid-search.md`
