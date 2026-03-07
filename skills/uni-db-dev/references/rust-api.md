# Rust API Reference (uni-db)

## Table of Contents
1. [Uni (Main Entry Point)](#uni)
2. [UniBuilder](#unibuilder)
3. [Query Execution](#query-execution)
4. [Schema Management](#schema-management)
5. [Transactions](#transactions)
6. [Sessions](#sessions)
7. [Bulk Loading](#bulk-loading)
8. [Snapshots](#snapshots)
9. [Locy](#locy)
10. [Key Types](#key-types)
11. [UniSync (Blocking API)](#unisync)

---

## Uni

Main entry point. All query/write methods are async.

### Database Creation

```rust
let db = Uni::open("./path").build().await?;
let db = Uni::open_existing("./path").build().await?;
let db = Uni::create("./path").build().await?;
let db = Uni::temporary().build().await?;
let db = Uni::in_memory().build().await?;
```

---

## UniBuilder

```rust
let db = Uni::open("./path")
    .schema_file("schema.json")
    .cache_size(512 * 1024 * 1024)
    .parallelism(4)
    .hybrid("./local", "s3://bucket/prefix")
    .build().await?;
```

| Method | Description |
|--------|-------------|
| `.schema_file(path)` | Load schema from JSON on init |
| `.hybrid(local, remote)` | Local cache + remote storage |
| `.cache_size(bytes)` | Adjacency cache size |
| `.parallelism(n)` | Query worker threads |
| `.config(UniConfig)` | Set full config |
| `.build()` | Build async |
| `.build_sync()` | Build synchronously |

---

## Query Execution

```rust
// Simple query
let results = db.query("MATCH (p:Person) RETURN p.name").await?;

// Parameterized
let results = db.query_with("MATCH (p:Person) WHERE p.age > $min RETURN p")
    .param("min", 25)
    .timeout(Duration::from_secs(5))
    .max_memory(256 * 1024 * 1024)
    .fetch_all().await?;

// Mutation
let affected = db.execute("CREATE (:Person {name: 'Alice'})").await?;

// Streaming cursor for large results
let cursor = db.query_cursor("MATCH (p:Person) RETURN p").await?;

// Explain (plan only)
let plan = db.explain("MATCH (p:Person) RETURN p").await?;

// Profile (plan + execution stats)
let (results, stats) = db.profile("MATCH (p:Person) RETURN p").await?;
```

---

## Schema Management

### Fluent Builder

```rust
db.schema()
    .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int64)
        .vector("embedding", 768)
        .index("name", IndexType::Scalar(ScalarType::BTree))
    .done()
    .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Int64)
    .done()
    .apply().await?;
```

### Information Queries

```rust
let labels = db.list_labels().await?;           // Vec<String>
let edge_types = db.list_edge_types().await?;    // Vec<String>
let exists = db.label_exists("Person").await?;   // bool
let info = db.get_label_info("Person").await?;   // Option<LabelInfo>
let schema = db.get_schema();                     // Arc<Schema>
```

### Schema Files

```rust
db.load_schema("schema.json").await?;
db.save_schema("schema.json").await?;
```

---

## Transactions

```rust
// Explicit
let txn = db.begin().await?;
txn.execute("CREATE (:Person {name: 'Alice'})").await?;
txn.commit().await?; // or txn.rollback().await?

// Closure-based (auto-rollback on error)
db.transaction(|txn| async move {
    txn.execute("CREATE (:Person {name: 'Bob'})").await?;
    Ok(())
}).await?;
```

---

## Sessions

```rust
let session = db.session()
    .set("tenant_id", "acme")
    .build();

let results = session.query(
    "MATCH (p:Person) WHERE p.org = $session.tenant_id RETURN p"
).await?;
```

---

## Bulk Loading

```rust
// Simple
let vids = db.bulk_insert_vertices("Person", &[
    props!{"name" => "Alice", "age" => 30},
    props!{"name" => "Bob", "age" => 25},
]).await?;

db.bulk_insert_edges("KNOWS", &[
    EdgeData::new(vids[0], vids[1], props!{"since" => 2024}),
]).await?;

// Advanced BulkWriter
let writer = db.bulk_writer()
    .defer_vector_indexes(true)
    .batch_size(10_000)
    .async_indexes(true)
    .on_progress(|p| println!("{}: {}/{}", p.phase, p.rows_processed, p.total_rows.unwrap_or(0)))
    .build();

let vids = writer.insert_vertices("Person", &vertices).await?;
writer.insert_edges("KNOWS", &edges).await?;
let stats = writer.commit().await?; // BulkStats
```

---

## Snapshots

```rust
db.flush().await?;
let id = db.create_snapshot("my_snapshot").await?;
let snapshots = db.list_snapshots().await?;
db.restore_snapshot(&id).await?;
```

---

## Locy

```rust
let result = db.locy().evaluate(program).await?;

// With config
let config = LocyConfig {
    max_iterations: 500,
    timeout: Duration::from_secs(60),
    ..Default::default()
};
let result = db.locy().evaluate_with_config(program, &config).await?;

// Compile-only (validation)
let compiled = db.locy().compile_only(program)?;

// Access results
let rows = result.rows();
let stats = result.stats();
let derived = &result.derived; // HashMap<String, Vec<Row>>
```

### LocyConfig

| Field | Default | Description |
|-------|---------|-------------|
| `max_iterations` | 1000 | Per-stratum limit |
| `timeout` | 300s | Overall timeout |
| `max_explain_depth` | 100 | Proof tree depth |
| `max_slg_depth` | 1000 | SLG recursion depth |
| `max_abduce_candidates` | 20 | Candidate modifications |
| `max_abduce_results` | 10 | Results to return |
| `max_derived_bytes` | 256 MB | Memory cap per relation |
| `deterministic_best_by` | true | Deterministic tie-breaking |

---

## Key Types

### Results
- `QueryResult` — Rows with column metadata
- `ExecuteResult { affected_rows: usize }` — Mutation count
- `ExplainOutput` — Query plan
- `ProfileOutput` — Execution statistics
- `QueryCursor` — Streaming iterator for large results

### Identity
- `Vid` (u64) — Dense vertex ID: `label_id(16) | offset(48)`
- `Eid` (u64) — Edge ID: same encoding
- `UniId` — SHA3-256 content hash (32 bytes)

### Schema
- `DataType` — `String`, `Int64`, `Int32`, `Float64`, `Float32`, `Bool`, `DateTime`, `Date`, `Time`, `Duration`, `Json`, `Vector(dims)`, `List(Box<DataType>)`
- `IndexType` — `Vector(VectorAlgo, VectorMetric)`, `FullText`, `Scalar(ScalarType)`, `Inverted`
- `VectorMetric` — `Cosine`, `L2`, `Dot`
- `ScalarType` — `BTree`, `Hash`, `Bitmap`

### Values
- `Value` — `Null`, `Bool(bool)`, `Int(i64)`, `Float(f64)`, `String(String)`, `List(Vec<Value>)`, `Map(HashMap)`, `Vector(Vec<f32>)`, `Node(Node)`, `Edge(Edge)`, `Path(Path)`
- `Properties` — `HashMap<String, Value>`

### Errors
- `UniError` — Parse, Query, Type, Constraint, ReadOnly, NotFound, Schema, Io, Internal variants

---

## UniSync

Blocking wrapper for non-async code.

```rust
let db = UniSync::in_memory()?;
db.schema()
    .label("Person")
        .property("name", DataType::String)
    .done()
    .apply()?;

db.execute("CREATE (:Person {name: 'Alice'})")?;
let results = db.query("MATCH (p:Person) RETURN p.name")?;
db.shutdown()?;
```
