# Uni: Embedded Graph & Vector Database

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust](https://img.shields.io/badge/rust-1.75+-orange.svg)](https://www.rust-lang.org)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org)

**Uni** is an embedded database that combines a property graph (OpenCypher), vector search, and columnar storage (Lance) into a single engine. Designed for applications requiring local, fast, multimodal data access with object storage (S3/GCS/local) durability.

Part of [The Rustic Initiative](https://www.rustic.ai) by [Dragonscale Industries Inc.](https://www.dragonscale.ai)

## Key Features

- **Embedded & Serverless:** Runs as a library within your application — no server process.
- **Property Graph:** OpenCypher queries with MATCH, CREATE, WHERE, ORDER BY, LIMIT, and aggregations.
- **Vector Search:** K-NN similarity search (L2, cosine) with pre-filter and threshold support.
- **Columnar Storage:** Lance-backed persistence on local disk or object storage (S3/GCS).
- **Graph Algorithms:** PageRank, Louvain, shortest path, and more via the built-in algorithm library.
- **Rust & Python:** Native Rust crate and Python bindings (PyO3).

## Getting Started

### Rust

Add to your `Cargo.toml`:

```toml
[dependencies]
uni-db = "0.1.3"
```

### Python

```bash
pip install uni-db
```

```python
import uni_db

# Open or create a database
db = uni_db.Database("./my_graph")

# Define schema
db.create_label("Person")
db.add_property("Person", "name", "string", False)
db.add_property("Person", "age", "int64", True)
db.create_scalar_index("Person", "name", "btree")

# Write data
db.execute("CREATE (p:Person {name: 'Alice', age: 30})")
db.execute("CREATE (p:Person {name: 'Bob', age: 25})")
db.flush()

# Query
results = db.query(
    "MATCH (p:Person) WHERE p.age > $min RETURN p.name",
    {"min": 28},
)
print(results)  # [{'p.name': 'Alice'}]
```

## Vector Search

```python
# Create schema with a vector property
db.create_label("Document")
db.add_property("Document", "text", "string", False)
db.add_property("Document", "embedding", "vector[128]", True)
db.create_vector_index("Document", "embedding", "cosine")

# Insert data
db.execute("CREATE (d:Document {text: 'hello world', embedding: [0.1, 0.2, 0.3]})")
db.flush()

# K-NN search
results = db.query("""
    CALL uni.vector.query('Document', 'embedding', $vec, 10)
    YIELD vid, distance
    RETURN vid, distance
    ORDER BY distance
""", {"vec": my_embedding})

# K-NN with pre-filter
results = db.query("""
    CALL uni.vector.query('Document', 'embedding', $vec, 10, 'category = "tech"')
    YIELD vid, distance
    RETURN vid, distance
""", {"vec": my_embedding})
```

## Async API

```python
import uni_db

db = await uni_db.AsyncDatabase.open("./my_graph")

await db.execute("CREATE (p:Person {name: 'Alice', age: 30})")
results = await db.query("MATCH (p:Person) RETURN p.name")
```

## Python OGM (uni-pydantic)

```bash
pip install uni-pydantic
```

```python
from uni_pydantic import UniNode, UniSession, Field, Relationship, Vector

class Person(UniNode):
    name: str
    age: int | None = None
    email: str = Field(unique=True, index="btree")
    embedding: Vector[128] = Field(metric="cosine")
    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

session = UniSession(db)
session.register(Person)
session.sync_schema()

alice = Person(name="Alice", email="alice@example.com")
session.add(alice)
session.commit()

adults = session.query(Person).filter(Person.age >= 18).order_by(Person.name).all()
```

## Documentation

- [Full Documentation](https://rustic-ai.github.io/uni-db)
- [GitHub Repository](https://github.com/rustic-ai/uni-db)

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.

---

**Uni** is developed by [Dragonscale Industries Inc.](https://www.dragonscale.ai) as part of [The Rustic Initiative](https://www.rustic.ai).
