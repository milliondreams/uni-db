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
from uni_db import Uni

# Open or create a database (`Uni.in_memory()` for an ephemeral one)
db = Uni.open("./my_graph")

# Define schema
db.schema() \
    .label("Person") \
        .property("name", "string") \
        .property_nullable("age", "int64") \
        .index("name", "btree") \
    .apply()

# Write data. `Uni` is lifecycle and admin only: reads go through a Session,
# writes through a transaction on one.
session = db.session()
tx = session.tx()
tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
tx.commit()

# Query (read-only)
results = session.query(
    "MATCH (p:Person) WHERE p.age > $min RETURN p.name",
    {"min": 28},
)
print(results)  # [{'p.name': 'Alice'}]
```

## Schema Operations

```python
# Labels, edge types, properties and indexes are all declared through the
# schema builder and committed together by a single `.apply()`.
db.schema() \
    .label("Person") \
        .property("name", "string") \
        .property_nullable("age", "int64") \
        .vector("embedding", 384) \
        .index("name", "btree") \
        .index("embedding", {"type": "vector", "metric": "cosine"}) \
        .done() \
    .edge_type("KNOWS", ["Person"], ["Person"]) \
        .property_nullable("since", "date") \
    .apply()

# Introspection
db.schema().current()        # dict view of the whole schema
db.schema().current_typed()  # typed `Schema` object
```

## Transactions

```python
tx = db.session().tx()
tx.execute("CREATE (p:Person {name: 'Charlie'})")
tx.commit()   # or tx.rollback()
```

## Bulk Loading

The bulk writer is built from a transaction.

```python
tx = db.session().tx()
writer = tx.bulk_writer().build()
vids = writer.insert_vertices("Person", [
    {"name": "Alice", "age": 30},
    {"name": "Bob",   "age": 25},
])
writer.insert_edges("KNOWS", [
    (vids[0], vids[1], {}),   # (src_vid, dst_vid, properties)
])
writer.commit()
tx.commit()
```

## Vector Search

```python
# Declare the vector column and its index
db.schema() \
    .label("Document") \
        .property("text", "string") \
        .vector("embedding", 128) \
        .index("embedding", {"type": "vector", "metric": "cosine"}) \
    .apply()

session = db.session()
tx = session.tx()
tx.execute("CREATE (d:Document {text: 'hello world', embedding: $v})", {"v": my_embedding})
tx.commit()
db.flush()

# K-NN search. `k` is required.
results = session.query('''
    CALL uni.vector.query('Document', 'embedding', $vec, 10)
    YIELD node, score
    RETURN node.text AS text, score
    ORDER BY score DESC
''', {"vec": my_embedding})

# K-NN with pre-filter (SQL WHERE expression)
results = session.query('''
    CALL uni.vector.query('Document', 'embedding', $vec, 10, 'category = "tech"')
    YIELD node, score
    RETURN node.text AS text, score
''', {"vec": my_embedding})

# K-NN with a similarity floor. `threshold` is a MINIMUM SIMILARITY on the
# same scale as `score` (larger is a better match), not a maximum distance.
results = session.query('''
    CALL uni.vector.query('Document', 'embedding', $vec, 10, NULL, 0.8)
    YIELD node, score
    RETURN node.text AS text, score
''', {"vec": my_embedding})
```

`YIELD` columns: `node` (the matched vertex), `score` (similarity, larger is better) and `distance` (the raw metric distance).

## Async API

```python
from uni_db import AsyncUni

db = await AsyncUni.open("./my_graph")
# or: db = await AsyncUni.temporary()

session = db.session()
tx = await session.tx()
await tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
await tx.commit()

results = await session.query("MATCH (p:Person) RETURN p.name")
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
