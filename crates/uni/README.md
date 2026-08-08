# Uni - Embedded Graph Database

[![Crates.io](https://img.shields.io/crates/v/uni-db.svg)](https://crates.io/crates/uni-db)
[![Documentation](https://docs.rs/uni-db/badge.svg)](https://docs.rs/uni-db)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Uni** is an embedded, multimodal database that combines **Property Graph** (OpenCypher), **Vector Search**, and **Columnar Storage** (Lance) into a single engine. It is designed for high-performance, local-first applications with object storage durability (S3/GCS).

Part of [The Rustic Initiative](https://www.rustic.ai) by [Dragonscale Industries Inc.](https://www.dragonscale.ai)

## Features

- **Embedded**: Runs in-process with your application (no sidecar required).
- **Multimodal**: Graph + Vector + Columnar in one engine.
- **OpenCypher**: Execute complex graph pattern matching queries.
- **Vector Search**: Native support for vector embeddings and KNN search.
- **Hybrid Storage**: Fast local WAL/ID allocation with bulk data + catalog metadata in S3/GCS.
- **Graph Algorithms**: Built-in PageRank, WCC, ShortestPath, and more.

## Installation

Add `uni-db` to your `Cargo.toml`. The crate is published as `uni-db`; the
library is imported as `uni_db`.

```toml
[dependencies]
uni-db = "3"
tokio = { version = "1", features = ["full"] }
```

## Quick Start

### 1. Open Database

```rust
use uni_db::Uni;

#[tokio::main]
async fn main() -> Result<(), uni_db::UniError> {
    // Open (or create) a local database
    let db = Uni::open("./my_graph_db")
        .build()
        .await?;
    
    // Define Schema
    db.schema()
        .label("Person")
            .property("name", uni_db::DataType::String)
            .property("age", uni_db::DataType::Int64)
            .vector("embedding", 384) // Vector index
        .apply()
        .await?;

    Ok(())
}
```

### 2. Insert Data

You can insert data using Cypher queries or the builder API.

Writes go through a transaction on a session; `Session::query` is read-only.

```rust
let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (p:Person {name: 'Alice', age: 30})").await?;
tx.commit().await?;
```

### 3. Query Data

```rust
let results = db
    .session()
    .query("MATCH (p:Person) WHERE p.age > 25 RETURN p.name, p.age")
    .await?;

for row in results.rows() {
    let name: String = row.get("p.name")?;
    let age: i64 = row.get("p.age")?;
    println!("Found: {name} ({age})");
}
```

### 4. Vector Search

Vector search is a procedure call in Cypher — `uni.vector.query(label,
property, query, k)`. `k` is required, and the yielded `score` increases with
similarity.

```rust
let results = db
    .session()
    .query(
        "CALL uni.vector.query('Person', 'embedding', $q, 5) \
         YIELD node, score \
         RETURN node.name AS name, score ORDER BY score DESC",
    )
    .bind("q", query_vec)
    .await?;
```

## Storage Backends

Uni supports local filesystem and object storage (S3, GCS, Azure).

### Hybrid Mode (Recommended for Cloud)

Keep WAL and ID allocation on fast local disk (SSD), while storing bulk data and catalog metadata in S3.

```rust
use uni_db::CloudStorageConfig;

let db = Uni::open("./local_meta")
    .remote_storage("s3://my-bucket/graph-data", CloudStorageConfig::default())
    .build()
    .await?;
```

!!! note
    `hybrid(...)` and `cloud_config(...)` exist only on the **Python**
    builder. The Rust builder's equivalent is `remote_storage`.

## Performance

For allocation-heavy workloads (many small mutations, concurrent Cypher
`CREATE`/`MERGE`, etc.), the default glibc allocator becomes the dominant
bottleneck — its per-arena locks and the kernel's per-CPU page allocator
serialize under concurrent churn. Opt in to mimalloc for ~3× throughput:

```toml
[dependencies]
uni-db = { version = "...", features = ["mimalloc"] }
```

```rust
// in your binary's main.rs:
#[global_allocator]
static GLOBAL: uni_db::MiMalloc = uni_db::MiMalloc;
```

Measured at sess=24 on `concurrent_mutations` benchmark: 1012 ms → 394 ms.
The `uni` CLI binary already does this by default.

## Documentation

- [Full Documentation](https://rustic-ai.github.io/uni-db)
- [Rust API Reference](https://docs.rs/uni-db)
- [GitHub Repository](https://github.com/rustic-ai/uni-db)

## License

Apache 2.0 - see [LICENSE](../../LICENSE) for details.

---

Developed by [Dragonscale Industries Inc.](https://www.dragonscale.ai) as part of [The Rustic Initiative](https://www.rustic.ai).
