# uni-pydantic

Pydantic-based OGM (Object-Graph Mapping) for the [Uni Graph Database](https://github.com/rustic-ai/uni-db).

[![PyPI](https://img.shields.io/pypi/v/uni-pydantic.svg)](https://pypi.org/project/uni-pydantic/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Features

- **Type Safety:** Full IDE autocomplete and type checking for graph operations.
- **Pydantic v2:** Leverage Pydantic's validation, computed fields, and serialization.
- **Schema-from-Models:** Auto-generate Uni schema from Pydantic model definitions.
- **Relationships:** Edges as declarative fields with direction support.
- **Query DSL:** Type-safe filter builder with raw Cypher escape hatch.
- **Vector Search:** Built-in K-NN similarity search.
- **Lifecycle Hooks:** `@before_create`, `@after_create` decorators.

## Installation

```bash
pip install uni-pydantic
```

## Quick Start

```python
import uni_db
from uni_pydantic import UniNode, UniEdge, UniSession, Field, Relationship, Vector

# Define models
class Person(UniNode):
    __label__ = "Person"

    name: str
    age: int | None = None
    email: str = Field(unique=True, index="btree")
    embedding: Vector[128] = Field(metric="cosine")

    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")


class Company(UniNode):
    __label__ = "Company"

    name: str = Field(unique=True)


class WorksAt(UniEdge):
    __edge_type__ = "WORKS_AT"
    __from__ = Person
    __to__ = Company

    role: str = "engineer"


# Connect and sync schema
db = uni_db.Database("./my_graph")
session = UniSession(db)
session.register(Person, Company, WorksAt)
session.sync_schema()

# Create
alice = Person(name="Alice", age=30, email="alice@example.com")
session.add(alice)
session.commit()

# Query with DSL
adults = (
    session.query(Person)
    .filter(Person.age >= 18)
    .order_by(Person.name)
    .limit(10)
    .all()
)

# Vector search
similar = (
    session.query(Person)
    .vector_search("embedding", query_vector, k=10)
    .all()
)
```

## Model Definition

### Nodes

```python
from uni_pydantic import UniNode, Field, Vector

class Article(UniNode):
    __label__ = "Article"      # optional — defaults to class name

    title: str
    slug: str = Field(index="btree", unique=True)
    views: int = Field(default=0)
    tags: list[str] = Field(default_factory=list)
    embedding: Vector[768] = Field(metric="cosine")
```

### Edges

```python
from uni_pydantic import UniEdge

class Authored(UniEdge):
    __edge_type__ = "AUTHORED"
    __from__ = Person
    __to__ = Article

    role: str = "primary"
```

### Relationships

```python
class Person(UniNode):
    follows:   list["Person"]        = Relationship("FOLLOWS")
    followers: list["Person"]        = Relationship("FOLLOWS", direction="incoming")
    friends:   list["Person"]        = Relationship("FRIEND_OF", direction="both")
    manager:   "Person | None"       = Relationship("REPORTS_TO")
```

## Query DSL

```python
# Filter
people = session.query(Person).filter(Person.name == "Alice").all()

# Chained
results = (
    session.query(Person)
    .filter(Person.age >= 18)
    .order_by(Person.name, descending=True)
    .limit(10)
    .skip(20)
    .all()
)

# Filter helpers
Person.name.starts_with("A")
Person.name.contains("lic")
Person.age.in_([25, 30, 35])
Person.email.is_null()
Person.email.is_not_null()

# Vector search
similar = (
    session.query(Person)
    .vector_search("embedding", query_vec, k=10)
    .all()
)
```

## CRUD

```python
# Create
person = Person(name="Bob", age=25)
session.add(person)
session.commit()

# Bulk
session.add_all([Person(name=f"User{i}") for i in range(100)])
session.commit()

# Update
person.age = 26
session.commit()

# Delete
session.delete(person)
session.commit()
```

## Lifecycle Hooks

```python
from datetime import datetime
from uni_pydantic import before_create, after_create

class Person(UniNode):
    name: str
    created_at: datetime | None = None

    @before_create
    def set_created_at(self):
        self.created_at = datetime.now()

    @after_create
    def log_creation(self):
        print(f"Created: {self.name}")
```

## Raw Cypher

```python
results = session.cypher(
    "MATCH (p:Person)-[:FRIEND_OF]->(f:Person) WHERE p.name = $name RETURN f",
    params={"name": "Alice"},
    result_type=Person,
)
```

## Async

```python
from uni_pydantic import AsyncUniSession

session = AsyncUniSession(db)
await session.sync_schema()

alice = Person(name="Alice", email="alice@example.com")
session.add(alice)
await session.commit()

results = await session.query(Person).filter(Person.age >= 18).all()
```

## Links

- [Documentation](https://rustic-ai.github.io/uni-db)
- [GitHub](https://github.com/rustic-ai/uni-db)

## License

Apache-2.0
