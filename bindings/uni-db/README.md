# uni-db: Python Bindings for Uni Graph Database

[![PyPI](https://img.shields.io/pypi/v/uni-db.svg)](https://pypi.org/project/uni-db/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Python bindings for the **Uni** embedded graph database.

Part of [The Rustic Initiative](https://www.rustic.ai) by [Dragonscale Industries Inc.](https://www.dragonscale.ai)

## Installation

```bash
pip install uni-db
```

## Quick Start

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

## Schema Operations

```python
# Labels and edge types
db.create_label("Person")
db.create_edge_type("KNOWS", ["Person"], ["Person"])
db.add_property("Person", "name", "string", False)   # nullable=False
db.add_property("Person", "age",  "int64",  True)    # nullable=True

# Indexes
db.create_scalar_index("Person", "name", "btree")
db.create_vector_index("Person", "embedding", "cosine")  # or "l2"

# Introspection
db.list_labels()         # ['Person', ...]
db.list_edge_types()     # ['KNOWS', ...]
db.get_label_info("Person")
db.get_schema()
```

## Transactions

```python
txn = db.begin()
txn.execute("CREATE (p:Person {name: 'Charlie'})")
txn.commit()   # or txn.rollback()
```

## Bulk Loading

```python
writer = db.bulk_writer()
writer.insert_vertices("Person", [
    {"name": "Alice", "age": 30},
    {"name": "Bob",   "age": 25},
])
writer.insert_edges("KNOWS", [
    (person_vids[0], person_vids[1], {}),   # (src_vid, dst_vid, properties)
])
writer.commit()
```

## Vector Search

```python
# Create a vector index
db.create_label("Document")
db.add_property("Document", "text",      "string",     False)
db.add_property("Document", "embedding", "vector[128]", True)
db.create_vector_index("Document", "embedding", "cosine")

db.execute("CREATE (d:Document {text: 'hello world', embedding: [0.1, 0.2, 0.3]})")
db.flush()

# K-NN search
results = db.query("""
    CALL uni.vector.query('Document', 'embedding', $vec, 10)
    YIELD vid, distance
    RETURN vid, distance
    ORDER BY distance
""", {"vec": my_embedding})

# K-NN with pre-filter (SQL WHERE expression)
results = db.query("""
    CALL uni.vector.query('Document', 'embedding', $vec, 10, 'category = "tech"')
    YIELD vid, distance
    RETURN vid, distance
""", {"vec": my_embedding})

# K-NN with distance threshold
results = db.query("""
    CALL uni.vector.query('Document', 'embedding', $vec, 10, NULL, 0.5)
    YIELD vid, distance
    RETURN vid, distance
""", {"vec": my_embedding})
```

`YIELD` columns: `vid` (integer vertex ID), `distance` (float).

## Async API

```python
import uni_db

# Open
db = await uni_db.AsyncDatabase.open("./my_graph")
# or: db = await uni_db.AsyncDatabase.temporary()

await db.execute("CREATE (p:Person {name: 'Alice', age: 30})")
results = await db.query("MATCH (p:Person) RETURN p.name")
await db.flush()
```

## Forks

Named, durable, isolated branches of the graph. A fork lets a session
reason about an alternate version of the database — what-if analysis,
audit hold, scenario sandboxing — that survives across restarts.

```python
import uni_db
from datetime import timedelta

db = uni_db.Uni.builder().build()
db.schema().label("Person").property("name", "string").apply()

primary = db.session()

# Open or create a fork (Phase 2: writable; Phase 3: nestable;
# Phase 4a: TTL + tags + budget).
fork = primary.fork("scenario_1").ttl(timedelta(hours=1)).build()
tx = fork.tx()
tx.execute("CREATE (:Person {name: 'fork-only'})")
tx.commit()

# Fork sees primary state + its own writes; primary unchanged.
print(fork.query("MATCH (p:Person) RETURN count(p) AS n"))

# Pin a Lance tag for audit retention; the tag survives the drop.
db.tag_fork("scenario_1", "audit-2026-q1")
del fork
db.drop_fork("scenario_1")
print(db.list_fork_tags("scenario_1"))  # tag still resolvable
```

The async surface mirrors this exactly through `AsyncUni` /
`AsyncSession`. See `examples/fork_quickstart.py` and
`examples/fork_audit.py` for runnable demos, and the full
[Python API reference](../../docs/complete_python_api.md#24-forks-phase-4b)
for every method, type, and error variant.

## Query Utilities

```python
# Parameterized queries
results = db.query(
    "MATCH (p:Person) WHERE p.name = $name RETURN p",
    {"name": "Alice"},
)

# Explain / profile
plan    = db.explain("MATCH (p:Person) RETURN p")
results, stats = db.profile("MATCH (p:Person) RETURN p")
```

## Development

```bash
git clone https://github.com/rustic-ai/uni-db
cd uni-db/bindings/uni-db
uv sync --group dev
uv run maturin develop   # builds and installs the extension module
uv run pytest            # run tests
```

## Links

- [Documentation](https://rustic-ai.github.io/uni-db)
- [GitHub](https://github.com/rustic-ai/uni-db)
- [Issues](https://github.com/rustic-ai/uni-db/issues)

## License

Apache 2.0
