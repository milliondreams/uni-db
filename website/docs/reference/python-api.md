# Python API Reference

Uni provides full-featured Python bindings with type hints and comprehensive documentation.

## Installation

```bash
pip install uni-db
```

Or build from source:

```bash
cd bindings/uni-db
pip install maturin
maturin develop --release
```

## Quick Start

```python
import uni_db

# Open or create a database
db = uni_db.Database("/path/to/db")

# Or use the builder pattern
db = uni_db.DatabaseBuilder.open("/path/to/db").build()

# Create schema
db.create_label("Person")
db.add_property("Person", "name", "string", False)
db.add_property("Person", "age", "int", False)

# Insert data
db.query("CREATE (n:Person {name: 'Alice', age: 30})")
db.query("CREATE (n:Person {name: 'Bob', age: 25})")

# Query data
results = db.query("MATCH (n:Person) WHERE n.age > 20 RETURN n.name AS name")
for row in results:
    print(row["name"])
```

## Async Quick Start

```python
import uni_db

async def main():
    db = await uni_db.AsyncDatabase.open("/path/to/db")

    await db.execute("CREATE (:Person {name: 'Alice', age: 30})")
    results = await db.query("MATCH (n:Person) RETURN n.name AS name")

    for row in results:
        print(row["name"])
```

## Core Classes

### Database

The main database interface. Create using `Database(path)` or `DatabaseBuilder`.

```python
db = uni_db.Database("/path/to/db")

# Execute queries
results = db.query("MATCH (n) RETURN n LIMIT 10")
affected = db.execute("CREATE (n:Person {name: 'Alice'})")

# Parameterized queries
results = db.query(
    "MATCH (n:Person) WHERE n.name = $name RETURN n",
    {"name": "Alice"}
)

# Or use QueryBuilder
builder = db.query_with("MATCH (n:Person) WHERE n.age > $min RETURN n")
builder.param("min", 21)
results = builder.fetch_all()
```

### AsyncDatabase

Async API for event-loop based applications:

```python
db = await uni_db.AsyncDatabase.open("/path/to/db")

results = await db.query(
    "MATCH (n:Person) WHERE n.name = $name RETURN n",
    {"name": "Alice"}
)

affected = await db.execute("CREATE (:Person {name: 'Bob'})")

tx = await db.begin()
await tx.query("CREATE (:Person {name: 'Carol'})")
await tx.commit()
```

### DatabaseBuilder

Fluent builder for database configuration:

```python
# Create new database (fails if exists)
db = uni_db.DatabaseBuilder.create("/path/to/db").build()

# Open existing (fails if doesn't exist)
db = uni_db.DatabaseBuilder.open_existing("/path/to/db").build()

# Open or create
db = uni_db.DatabaseBuilder.open("/path/to/db").build()

# Temporary in-memory database
db = uni_db.DatabaseBuilder.temporary().build()

# With configuration
db = (
    uni_db.DatabaseBuilder.open("/path/to/db")
    .cache_size(1024 * 1024 * 100)  # 100 MB
    .parallelism(4)
    .build()
)
```

### SchemaBuilder

Fluent API for schema definition:

```python
schema = db.schema()
schema = schema.label("Person").property("name", "string").property("age", "int").done()
schema = schema.label("Company").property("name", "string").done()
schema = schema.edge_type("WORKS_AT", ["Person"], ["Company"]).property("since", "int").done()
schema.apply()
```

### Transaction

ACID transactions:

```python
tx = db.begin()
try:
    tx.query("CREATE (n:Person {name: 'Alice'})")
    tx.query("CREATE (n:Person {name: 'Bob'})")
    tx.commit()
except Exception:
    tx.rollback()
```

### Session

Scoped sessions with variables:

```python
builder = db.session()
builder.set("user_id", 123)
session = builder.build()

results = session.query("MATCH (n:Person) RETURN n")
user_id = session.get("user_id")
```

### BulkWriter

High-performance bulk loading:

```python
writer = db.bulk_writer().batch_size(10000).build()

# Insert vertices
vids = writer.insert_vertices("Person", [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
])

# Insert edges
writer.insert_edges("KNOWS", [
    (vids[0], vids[1], {"since": 2020}),
])

# Commit and build indexes
stats = writer.commit()
print(f"Inserted {stats.vertices_inserted} vertices")
```

### Vector Search

Vector similarity search is exposed via Cypher:

```python
# Create vector index
db.add_property("Document", "embedding", "vector:128", False)
db.create_vector_index("Document", "embedding", "cosine")

# Search via Cypher
query_vec = [0.1, 0.2, 0.3]  # 128 dimensions
results = db.query(
    "CALL uni.vector.query('Document', 'embedding', $vec, 10) "
    "YIELD vid, distance RETURN vid, distance",
    {"vec": query_vec},
)
```

### Locy Reasoning

Locy is available in both sync and async Python APIs:

```python
out = db.locy_evaluate(program)
# or
out = await adb.locy_evaluate(program)
```

With optional config (including probabilistic reasoning options):

```python
out = db.locy_evaluate(program, {
    "max_iterations": 500,
    "timeout": 60.0,
    "strict_probability_domain": True,
    "exact_probability": True,
    "max_bdd_variables": 1000,
})
```

Returned dict includes:

- `derived` — facts derived by each rule
- `stats` — timing and iteration counts
- `command_results` — output from `QUERY`, `ABDUCE`, `EXPLAIN RULE`
- `warnings` — runtime warnings (e.g., `SharedProbabilisticDependency`)
- `approximate_groups` — groups that fell back to approximate probability mode

See [Locy Overview](../locy/index.md) and [Locy Python API Integration](../locy/api/python.md).

### Xervo (Embedding & Generation)

Access the configured model runtime via `db.xervo()`. Requires a Xervo catalog configured at database open time:

```python
from uni_db import DatabaseBuilder, Message

db = DatabaseBuilder.open("./graph") \
    .xervo_catalog_from_file("./models.json") \
    .build()

xervo = db.xervo()

# Embed text → list[list[float]]
vectors = xervo.embed("embed/default", ["graph databases", "vector search"])

# Generate with Message objects
result = xervo.generate(
    "llm/default",
    [
        Message.system("You are a concise technical assistant."),
        Message.user("What is snapshot isolation?"),
    ],
    max_tokens=256,
    temperature=0.7,
)
print(result.text)    # Generated string
print(result.usage)   # TokenUsage | None

# Convenience wrapper — single prompt string
result = xervo.generate_text("llm/default", "Explain hybrid search in one sentence.")
```

Async Xervo:

```python
db = await AsyncDatabaseBuilder.open("./graph") \
    .xervo_catalog_from_file("./models.json") \
    .build()
xervo = db.xervo()
vectors = await xervo.embed("embed/default", ["hello"])
result = await xervo.generate_text("llm/default", "Hello!")
```

**Message constructors:**

```python
Message.user("content")        # role = "user"
Message.assistant("content")   # role = "assistant"
Message.system("content")      # role = "system"
Message(role, content)         # explicit
```

`generate()` also accepts plain dicts with `"role"` and `"content"` keys instead of `Message` objects.

## Data Types

Supported property data types:

| Type | Python | Description |
|------|--------|-------------|
| `string` | `str` | UTF-8 string |
| `int` | `int` | 64-bit integer |
| `float` | `float` | 64-bit float |
| `bool` | `bool` | Boolean |
| `vector:N` | `list[float]` | N-dimensional vector |

## Query Results

Query results are returned as `list[dict[str, Any]]`:

```python
results = db.query("MATCH (n:Person) RETURN n.name AS name, n.age AS age")
for row in results:
    print(f"Name: {row['name']}, Age: {row['age']}")
```

Both sync and async APIs currently return fully materialized result lists. Cursor-style streaming is not yet exposed in Python.

## EXPLAIN and PROFILE

Analyze query execution:

```python
# Get query plan without executing
plan = db.explain("MATCH (n:Person) RETURN n")
print(plan["plan_text"])
print(plan["cost_estimates"])

# Execute with profiling
results, profile = db.profile("MATCH (n:Person) RETURN n")
print(f"Total time: {profile['total_time_ms']}ms")
print(f"Peak memory: {profile['peak_memory_bytes']} bytes")
```

## Snapshots

Snapshot management is available directly on the `Database` object:

```python
# Create a named snapshot
snapshot_id = db.create_snapshot("baseline-import")

# List all snapshots
snapshots = db.list_snapshots()
for snap in snapshots:
    print(snap.snapshot_id, snap.name, snap.created_at)

# Restore to a previous snapshot
db.restore_snapshot(snapshot_id)
```

Async:

```python
snapshot_id = await adb.create_snapshot("pre-migration")
snapshots = await adb.list_snapshots()
await adb.restore_snapshot(snapshot_id)
```

`list_snapshots()` returns a list of `SnapshotInfo` objects with fields: `snapshot_id`, `name`, `created_at`, `version_hwm`.

## Error Handling

The library raises standard Python exceptions:

- `RuntimeError`: Query execution errors
- `ValueError`: Invalid parameters
- `OSError`: Database I/O errors

```python
try:
    db.query("INVALID CYPHER")
except RuntimeError as e:
    print(f"Query error: {e}")
```

## Full API Documentation

See the [auto-generated pdoc documentation](../api/python/index.md) for complete API details.
