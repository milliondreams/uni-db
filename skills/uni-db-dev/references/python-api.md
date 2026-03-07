# Python API Reference (uni-db)

## Table of Contents
1. [Database](#database)
2. [AsyncDatabase](#asyncdatabase)
3. [DatabaseBuilder](#databasebuilder)
4. [SchemaBuilder](#schemabuilder)
5. [QueryBuilder](#querybuilder)
6. [Transaction](#transaction)
7. [Session](#session)
8. [BulkWriter](#bulkwriter)
9. [Data Classes](#data-classes)

---

## Database

Main synchronous entry point. Opens or creates a database at a given path.

```python
db = uni_db.Database("./my_graph")
```

### Query Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `query` | `(cypher: str, params: dict \| None = None) -> list[dict]` | Execute read query |
| `execute` | `(cypher: str, params: dict \| None = None) -> int` | Execute mutation, returns affected count |
| `query_with` | `(cypher: str) -> QueryBuilder` | Parameterized query with timeout/memory |
| `explain` | `(cypher: str) -> dict` | Query plan without execution |
| `profile` | `(cypher: str) -> tuple[list[dict], dict]` | Execute with profiling stats |

### Transaction Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `begin` | `() -> Transaction` | Start new transaction |
| `flush` | `() -> None` | Persist uncommitted changes |

### Session Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `session` | `() -> SessionBuilder` | Create session with scoped variables |

### Schema Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `schema` | `() -> SchemaBuilder` | Fluent schema builder |
| `create_label` | `(name: str) -> int` | Create label, returns label ID |
| `create_edge_type` | `(name: str, from_labels: list[str] \| None, to_labels: list[str] \| None) -> int` | Create edge type |
| `add_property` | `(label_or_type: str, name: str, data_type: str, nullable: bool) -> None` | Add property |
| `label_exists` | `(name: str) -> bool` | Check label exists |
| `edge_type_exists` | `(name: str) -> bool` | Check edge type exists |
| `list_labels` | `() -> list[str]` | All label names |
| `list_edge_types` | `() -> list[str]` | All edge type names |
| `get_label_info` | `(name: str) -> LabelInfo \| None` | Label metadata |
| `get_schema` | `() -> dict` | Full schema as dict |
| `load_schema` | `(path: str) -> None` | Load schema from JSON |
| `save_schema` | `(path: str) -> None` | Save schema to JSON |

### Index Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `create_scalar_index` | `(label: str, property: str, index_type: str) -> None` | btree, hash, bitmap |
| `create_vector_index` | `(label: str, property: str, metric: str) -> None` | l2, cosine, dot |

### Bulk Loading

| Method | Signature | Description |
|--------|-----------|-------------|
| `bulk_writer` | `() -> BulkWriterBuilder` | Advanced bulk loader |
| `bulk_insert_vertices` | `(label: str, vertices: list[dict]) -> list[int]` | Insert vertices, returns VIDs |
| `bulk_insert_edges` | `(edge_type: str, edges: list[tuple[int, int, dict]]) -> None` | Insert edges (src_vid, dst_vid, props) |

### Locy

| Method | Signature | Description |
|--------|-----------|-------------|
| `locy_evaluate` | `(program: str, config: dict \| None = None) -> dict` | Evaluate Locy program |

---

## AsyncDatabase

All methods are async equivalents of `Database`. Created via:

```python
db = await uni_db.AsyncDatabase.open("./my_graph")
# or
db = await uni_db.AsyncDatabaseBuilder.open("./path").build()
db = await uni_db.AsyncDatabaseBuilder.temporary().build()
```

All query/execute/schema/bulk/locy methods must be awaited.

---

## DatabaseBuilder

Configure database before opening.

```python
db = uni_db.DatabaseBuilder.open("./path") \
    .cache_size(512 * 1024 * 1024) \
    .parallelism(4) \
    .build()
```

| Method | Description |
|--------|-------------|
| `.open(path)` | Open or create |
| `.open_existing(path)` | Open only (fail if missing) |
| `.create(path)` | Create only (fail if exists) |
| `.temporary()` | Ephemeral, deleted on close |
| `.in_memory()` | Alias for temporary |
| `.hybrid(local_path, remote_url)` | Local cache + remote storage |
| `.cache_size(bytes)` | Set cache size |
| `.parallelism(n)` | Worker thread count |
| `.build()` | Build database |

---

## SchemaBuilder

Fluent API for defining schema.

```python
db.schema() \
  .label("Person") \
    .property("name", "string") \
    .property_nullable("bio", "string") \
    .vector("embedding", 768) \
    .index("name", "btree") \
  .done() \
  .edge_type("KNOWS", ["Person"], ["Person"]) \
    .property("since", "int64") \
  .done() \
  .apply()
```

### LabelBuilder Methods

| Method | Description |
|--------|-------------|
| `.property(name, data_type)` | Required property |
| `.property_nullable(name, data_type)` | Optional property |
| `.vector(name, dimensions)` | Vector property shorthand |
| `.index(property, index_type)` | Add index |
| `.done()` | Return to SchemaBuilder |
| `.apply()` | Apply immediately |

### EdgeTypeBuilder Methods

| Method | Description |
|--------|-------------|
| `.property(name, data_type)` | Required property |
| `.property_nullable(name, data_type)` | Optional property |
| `.done()` | Return to SchemaBuilder |
| `.apply()` | Apply immediately |

---

## QueryBuilder

Advanced query execution with parameters, timeout, memory limits.

```python
results = db.query_with("MATCH (p:Person) WHERE p.age > $min RETURN p") \
    .param("min", 25) \
    .params({"extra": "value"}) \
    .timeout(5.0) \
    .max_memory(256 * 1024 * 1024) \
    .fetch_all()
```

---

## Transaction

Atomic operations with commit/rollback.

```python
txn = db.begin()
try:
    txn.query("CREATE (:Person {name: 'Alice'})")
    txn.commit()
except:
    txn.rollback()
    raise
```

| Method | Description |
|--------|-------------|
| `query(cypher, params)` | Query within transaction |
| `commit()` | Commit changes |
| `rollback()` | Discard changes |

---

## Session

Sessions carry scoped variables accessible as `$session.key` in queries.

```python
session = db.session().set("tenant", "acme").build()
results = session.query("MATCH (p:Person) WHERE p.org = $session.tenant RETURN p")
session.execute("CREATE (:Person {name: 'New', org: $session.tenant})")
```

---

## BulkWriter

High-throughput bulk loading with deferred index rebuilding.

```python
writer = db.bulk_writer() \
    .defer_vector_indexes(True) \
    .defer_scalar_indexes(True) \
    .batch_size(10000) \
    .async_indexes(True) \
    .build()

vids = writer.insert_vertices("Person", [{"name": "Alice"}, {"name": "Bob"}])
writer.insert_edges("KNOWS", [(vids[0], vids[1], {"since": 2024})])
stats = writer.commit()  # BulkStats with timing info
# or writer.abort() to discard
```

---

## Data Classes

### PropertyInfo
```python
PropertyInfo(name: str, data_type: str, nullable: bool, is_indexed: bool)
```

### LabelInfo
```python
LabelInfo(name: str, count: int, properties: list[PropertyInfo],
          indexes: list[IndexInfo], constraints: list[ConstraintInfo])
```

### BulkStats
```python
BulkStats(vertices_inserted: int, edges_inserted: int, indexes_rebuilt: int,
          duration_secs: float, index_build_duration_secs: float, indexes_pending: bool)
```

### LocyStats
```python
LocyStats(strata_evaluated: int, total_iterations: int, derived_nodes: int,
          derived_edges: int, evaluation_time_secs: float, queries_executed: int,
          mutations_executed: int, peak_memory_bytes: int)
```

### Locy Result Structure
```python
{
    "derived": {"rule_name": [{"col1": val1, ...}, ...]},
    "stats": LocyStats,
    "command_results": [
        {"type": "query", "rows": [...]},
        {"type": "explain", "rows": [...]},
    ]
}
```
