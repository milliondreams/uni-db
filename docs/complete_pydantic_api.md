# uni-pydantic — Complete API Reference

**Source-verified: March 2026**
**Package:** `uni-pydantic` v0.3.0

uni-pydantic is a Pydantic v2-based Object-Graph Mapping (OGM) layer for the Uni graph database. It provides type-safe model definitions, automatic schema generation, a query builder DSL, lifecycle hooks, and full async support.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Architecture Overview](#architecture-overview)
- [1. UniNode — Vertex Models](#1-uninode--vertex-models)
- [2. UniEdge — Edge Models](#2-uniedge--edge-models)
- [3. Field Configuration](#3-field-configuration)
- [4. Relationships](#4-relationships)
- [5. Vector Type](#5-vector-type)
- [6. UniSession — Synchronous Session](#6-unisession--synchronous-session)
- [7. AsyncUniSession — Asynchronous Session](#7-asyncunisession--asynchronous-session)
- [8. UniTransaction / AsyncUniTransaction](#8-unitransaction--asyncunitransaction)
- [9. QueryBuilder — Type-Safe Queries](#9-querybuilder--type-safe-queries)
- [10. AsyncQueryBuilder](#10-asyncquerybuilder)
- [11. Filter Expressions](#11-filter-expressions)
- [12. Schema Generation](#12-schema-generation)
- [13. Database Builders](#13-database-builders)
- [14. Lifecycle Hooks](#14-lifecycle-hooks)
- [15. Type Mapping](#15-type-mapping)
- [16. Exceptions](#16-exceptions)

---

## Quick Start

```python
from uni_db import Uni
from uni_pydantic import (
    UniNode, UniEdge, UniSession, Field, Relationship, Vector, before_create,
)
from datetime import date

# ── Define Models ──

class Person(UniNode):
    name: str = Field(index="btree")
    age: int | None = None
    email: str = Field(unique=True, index="btree")
    embedding: Vector[384] | None = None

    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

    @before_create
    def validate(self):
        if not self.email:
            raise ValueError("email required")

class Knows(UniEdge):
    __edge_type__ = "FRIEND_OF"
    __from__ = Person
    __to__ = Person
    since: date

# ── Connect & Sync Schema ──

db = Uni.temporary()
session = UniSession(db)
session.register(Person, Knows)
session.sync_schema()

# ── Create Data ──

alice = Person(name="Alice", age=30, email="alice@example.com")
bob = Person(name="Bob", age=25, email="bob@example.com")
session.add_all([alice, bob])
session.commit()

session.create_edge(alice, "FRIEND_OF", bob, Knows(since=date.today()))
session.commit()

# ── Query ──

adults = (
    session.query(Person)
    .filter(Person.age >= 18)
    .order_by(Person.name)
    .all()
)
```

---

## Architecture Overview

```
Pydantic Model (UniNode/UniEdge)
  │
  ├─ SchemaGenerator: model fields → db.schema().label().property().apply()
  │
  ├─ UniSession: OGM operations (add, get, delete, commit)
  │     ├─ Reads:  session._db_session.query(cypher)
  │     └─ Writes: session._db_session.tx() → tx.execute(cypher) → tx.commit()
  │
  └─ QueryBuilder: type-safe filter DSL → Cypher generation → session.query()
```

**Key design decisions:**
- All writes go through `Transaction` (aligned with refactored uni-db API)
- All reads use `Session.query()` directly (no transaction needed)
- Schema sync is additive-only — existing labels/edge types are skipped
- Dirty tracking is automatic via `__setattr__` override

---

# 1. UniNode — Vertex Models

**Source:** `bindings/uni-pydantic/src/uni_pydantic/base.py`

Base class for graph vertices. Subclass to define your node types. Extends Pydantic `BaseModel`.

```python
class Person(UniNode):
    __label__ = "Person"          # Optional, defaults to class name

    name: str                     # Required property
    age: int | None = None        # Optional property
    email: str = Field(unique=True, index="btree")
    bio: str = Field(index="fulltext")
    embedding: Vector[768]        # Auto-indexed vector

    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")
```

### Class Attributes

| Attribute | Type | Description |
|---|---|---|
| `__label__` | `str` | Vertex label name (defaults to class name) |
| `__relationships__` | `dict[str, RelationshipConfig]` | Auto-populated from `Relationship()` fields |

### Instance Properties

```python
@property
def vid(self) -> int | None             # Vertex ID assigned by database
@property
def uid(self) -> str | None             # Content-addressed identifier
@property
def is_persisted(self) -> bool          # Whether saved to database
@property
def is_dirty(self) -> bool              # Whether has unsaved changes
```

### Class Methods

```python
@classmethod
def get_property_fields(cls) -> dict[str, FieldInfo]
    # Get all property fields (excluding relationships)

@classmethod
def get_relationship_fields(cls) -> dict[str, RelationshipConfig]
    # Get all relationship configurations

@classmethod
def from_properties(
    cls, props: dict[str, Any], *,
    vid: int | None = None,
    uid: str | None = None,
    session: UniSession | None = None,
) -> UniNode
    # Create instance from property dictionary
```

### Instance Methods

```python
def to_properties(self) -> dict[str, Any]
    # Convert to database-ready property dictionary
```

---

# 2. UniEdge — Edge Models

Base class for graph edges with properties.

```python
class Follows(UniEdge):
    __edge_type__ = "FOLLOWS"     # Required
    __from__ = Person             # Source node type(s)
    __to__ = Person               # Target node type(s)

    since: date
    weight: float = 1.0
```

### Class Attributes

| Attribute | Type | Description |
|---|---|---|
| `__edge_type__` | `str` | Edge type name (defaults to class name) |
| `__from__` | `type[UniNode] \| tuple[type[UniNode], ...]` | Source node type(s) |
| `__to__` | `type[UniNode] \| tuple[type[UniNode], ...]` | Target node type(s) |

### Instance Properties

```python
@property
def eid(self) -> int | None            # Edge ID assigned by database
@property
def src_vid(self) -> int | None        # Source vertex ID
@property
def dst_vid(self) -> int | None        # Destination vertex ID
@property
def is_persisted(self) -> bool         # Whether saved to database
```

### Class Methods

```python
@classmethod
def get_from_labels(cls) -> list[str]      # Source label names
@classmethod
def get_to_labels(cls) -> list[str]        # Target label names
@classmethod
def get_property_fields(cls) -> dict[str, FieldInfo]

@classmethod
def from_properties(
    cls, props: dict[str, Any], *,
    eid: int | None = None,
    src_vid: int | None = None,
    dst_vid: int | None = None,
    session: UniSession | None = None,
) -> UniEdge

@classmethod
def from_edge_result(
    cls, data: dict[str, Any], *,
    session: UniSession | None = None,
) -> UniEdge
```

### Instance Methods

```python
def to_properties(self) -> dict[str, Any]
```

---

# 3. Field Configuration

**Source:** `bindings/uni-pydantic/src/uni_pydantic/fields.py`

The `Field()` function extends Pydantic's Field with graph database options.

```python
def Field(
    default: Any = ...,
    *,
    default_factory: Callable[[], Any] | None = None,
    alias: str | None = None,
    title: str | None = None,
    description: str | None = None,
    examples: list[Any] | None = None,
    exclude: bool = False,
    json_schema_extra: dict[str, Any] | None = None,
    # ── Uni-specific ──
    index: IndexType | None = None,
    unique: bool = False,
    tokenizer: str | None = None,
    metric: VectorMetric | None = None,
    generated: str | None = None,
) -> Any
```

### Parameters

| Parameter | Type | Description |
|---|---|---|
| `index` | `"btree" \| "hash" \| "fulltext" \| "vector"` | Index type to create |
| `unique` | `bool` | Create unique constraint (recorded, not yet enforced) |
| `tokenizer` | `str` | Tokenizer for fulltext index (e.g., `"standard"`) |
| `metric` | `"l2" \| "cosine" \| "dot"` | Distance metric for vector index |
| `generated` | `str` | Expression for computed property (recorded, not yet applied) |

Plus all standard Pydantic Field options.

### FieldConfig

```python
@dataclass
class FieldConfig:
    index: IndexType | None = None
    unique: bool = False
    tokenizer: str | None = None
    metric: VectorMetric | None = None
    generated: str | None = None
    default: Any = ...
    default_factory: Callable[[], Any] | None = None
    alias: str | None = None
    title: str | None = None
    description: str | None = None
    examples: list[Any] | None = None
    exclude: bool = False
    json_schema_extra: dict[str, Any] | None = None
```

### Type Aliases

```python
IndexType = Literal["btree", "hash", "fulltext", "vector"]
Direction = Literal["outgoing", "incoming", "both"]
VectorMetric = Literal["l2", "cosine", "dot"]
```

### Utility

```python
def get_field_config(field_info: FieldInfo) -> FieldConfig | None
    # Extract uni-pydantic config from Pydantic FieldInfo
```

**Example:**

```python
class Article(UniNode):
    title: str = Field(index="btree")
    slug: str = Field(index="btree", unique=True)
    content: str = Field(index="fulltext", tokenizer="standard")
    embedding: Vector[768] = Field(metric="cosine")
    views: int = Field(default=0)
    tags: list[str] = Field(default_factory=list)
```

---

# 4. Relationships

**Source:** `bindings/uni-pydantic/src/uni_pydantic/fields.py`

## Relationship Function

```python
def Relationship(
    edge_type: str,
    *,
    direction: Direction = "outgoing",
    edge_model: type[UniEdge] | None = None,
    eager: bool = False,
    cascade_delete: bool = False,
) -> Any
```

| Parameter | Type | Description |
|---|---|---|
| `edge_type` | `str` | Edge type name (e.g., `"FOLLOWS"`) |
| `direction` | `"outgoing" \| "incoming" \| "both"` | Traversal direction |
| `edge_model` | `type[UniEdge] \| None` | Optional edge model for typed properties |
| `eager` | `bool` | Eager-load by default |
| `cascade_delete` | `bool` | Delete edges when node deleted |

## RelationshipConfig

```python
@dataclass
class RelationshipConfig:
    edge_type: str
    direction: Direction = "outgoing"
    edge_model: type[UniEdge] | None = None
    eager: bool = False
    cascade_delete: bool = False
```

## RelationshipDescriptor

Descriptor for lazy-loading relationship fields. On instance access, loads related nodes; on class access, returns the descriptor (for query building).

```python
class RelationshipDescriptor(Generic[NodeT]):
    def __get__(self, obj, objtype) -> list[NodeT] | NodeT | None | RelationshipDescriptor
    def __set__(self, obj, value) -> None
```

**Example:**

```python
class Person(UniNode):
    # Outgoing (default)
    follows: list["Person"] = Relationship("FOLLOWS")

    # Incoming
    followers: list["Person"] = Relationship("FOLLOWS", direction="incoming")

    # Bidirectional
    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

    # Single optional
    manager: "Person | None" = Relationship("REPORTS_TO")

    # With edge model
    friendships: list[tuple["Person", FriendshipEdge]] = Relationship(
        "FRIEND_OF", edge_model=FriendshipEdge,
    )
```

---

# 5. Vector Type

**Source:** `bindings/uni-pydantic/src/uni_pydantic/types.py`

Fixed-dimension vectors for embeddings. Validated at assignment time.

```python
class Vector(Generic[N]):
    def __init__(self, values: list[float]) -> None
    @property
    def values(self) -> list[float]
    def __len__(self) -> int
    def __iter__(self) -> ...
    def __eq__(self, other: object) -> bool
```

**Usage:**

```python
class Document(UniNode):
    embedding: Vector[1536]                        # Required, auto-indexed
    summary_vec: Vector[768] | None = None         # Optional
    custom_vec: Vector[384] = Field(metric="cosine")  # Custom metric
```

At runtime, `Vector[N]` stores `list[float]` and validates that `len(values) == N`. Schema sync automatically creates a vector index.

---

# 6. UniSession — Synchronous Session

**Source:** `bindings/uni-pydantic/src/uni_pydantic/session.py`

The primary OGM session. Wraps a `uni_db.Uni` instance and provides model-level operations.

```python
class UniSession:
    def __init__(self, db: uni_db.Uni) -> None

    # ── Context Manager ──

    def __enter__(self) -> UniSession
    def __exit__(...) -> None
    def close(self) -> None

    # ── Database Access ──

    @property
    def db(self) -> uni_db.Uni

    # ── Model Registration & Schema ──

    def register(self, *models: type[UniNode] | type[UniEdge]) -> None
    def sync_schema(self) -> None

    # ── Query Builder ──

    def query(self, model: type[NodeT]) -> QueryBuilder[NodeT]

    # ── CRUD Operations ──

    def add(self, entity: UniNode) -> None
    def add_all(self, entities: Sequence[UniNode]) -> None
    def delete(self, entity: UniNode) -> None
    def get(
        self, model: type[NodeT],
        vid: int | None = None,
        uid: str | None = None,
        **kwargs: Any,
    ) -> NodeT | None
    def refresh(self, entity: UniNode) -> None

    # ── Commit / Rollback ──

    def commit(self) -> None
    def rollback(self) -> None

    # ── Transactions ──

    @contextmanager
    def transaction(self) -> Iterator[UniTransaction]
    def begin(self) -> UniTransaction

    # ── Edge Operations ──

    def create_edge(
        self, source: UniNode, edge_type: str, target: UniNode,
        properties: dict[str, Any] | UniEdge | None = None,
    ) -> None
    def delete_edge(self, source: UniNode, edge_type: str, target: UniNode) -> int
    def update_edge(
        self, source: UniNode, edge_type: str, target: UniNode,
        properties: dict[str, Any],
    ) -> int
    def get_edge(
        self, source: UniNode, edge_type: str, target: UniNode,
        edge_model: type[EdgeT] | None = None,
    ) -> list[dict[str, Any]] | list[EdgeT]

    # ── Bulk Operations ──

    def bulk_add(self, entities: Sequence[UniNode]) -> list[int]

    # ── Raw Cypher ──

    def cypher(
        self, query: str,
        params: dict[str, Any] | None = None,
        result_type: type[NodeT] | None = None,
    ) -> list[NodeT] | list[dict[str, Any]]

    # ── Locy ──

    def locy(
        self, program: str,
        params: dict[str, Any] | None = None,
    ) -> uni_db.LocyResult

    # ── Query Profiling ──

    def explain(self, cypher: str) -> uni_db.ExplainOutput
    def profile(self, cypher: str) -> tuple[uni_db.QueryResult, uni_db.ProfileOutput]

    # ── Schema Persistence ──

    def save_schema(self, path: str) -> None
    def load_schema(self, path: str) -> None
```

### Key Behaviors

- **`add(entity)`** — Stages entity for creation. Not persisted until `commit()`.
- **`commit()`** — Persists all pending creates, updates (auto-detected via dirty tracking), and deletes. Each operation opens its own `session.tx()` → `tx.execute()` → `tx.commit()`.
- **`get(Model, vid=..., email=...)`** — Looks up by VID, UID, or any keyword property.
- **`bulk_add(entities)`** — Uses `tx.bulk_writer()` for high-throughput inserts. Returns list of VIDs.
- **`cypher(query, result_type=Person)`** — Executes raw Cypher and hydrates results into model instances.

**Example:**

```python
with UniSession(db) as session:
    session.register(Person)
    session.sync_schema()

    # Create
    alice = Person(name="Alice", email="alice@example.com")
    session.add(alice)
    session.commit()
    print(alice.vid)  # assigned after commit

    # Read
    found = session.get(Person, email="alice@example.com")

    # Update (auto-detected)
    found.name = "Alice Smith"
    session.commit()

    # Delete
    session.delete(found)
    session.commit()
```

---

# 7. AsyncUniSession — Asynchronous Session

**Source:** `bindings/uni-pydantic/src/uni_pydantic/async_session.py`

Same API as `UniSession` but with async methods. Sync methods (`add`, `register`, `query`, `close`) remain sync.

```python
class AsyncUniSession:
    def __init__(self, db: uni_db.AsyncUni) -> None

    # ── Context Manager ──

    async def __aenter__(self) -> AsyncUniSession
    async def __aexit__(...) -> None
    def close(self) -> None                          # sync

    @property
    def db(self) -> uni_db.AsyncUni

    # ── Registration & Schema ──

    def register(self, *models: type[UniNode] | type[UniEdge]) -> None  # sync
    async def sync_schema(self) -> None

    # ── Query Builder ──

    def query(self, model: type[NodeT]) -> AsyncQueryBuilder[NodeT]  # sync

    # ── CRUD ──

    def add(self, entity: UniNode) -> None           # sync (just stages)
    def add_all(self, entities: Sequence[UniNode]) -> None  # sync
    def delete(self, entity: UniNode) -> None        # sync
    async def get(self, model: type[NodeT], vid: int | None = None,
                  uid: str | None = None, **kwargs: Any) -> NodeT | None
    async def refresh(self, entity: UniNode) -> None

    # ── Commit / Rollback ──

    async def commit(self) -> None
    async def rollback(self) -> None

    # ── Transactions ──

    async def transaction(self) -> AsyncUniTransaction

    # ── Edge Operations ──

    async def create_edge(self, source: UniNode, edge_type: str, target: UniNode,
                          properties: dict[str, Any] | UniEdge | None = None) -> None
    async def delete_edge(self, source: UniNode, edge_type: str, target: UniNode) -> int

    # ── Bulk Operations ──

    async def bulk_add(self, entities: Sequence[UniNode]) -> list[int]

    # ── Raw Cypher & Locy ──

    async def cypher(self, query: str, params: dict[str, Any] | None = None,
                     result_type: type[NodeT] | None = None) -> list[NodeT] | list[dict[str, Any]]
    async def locy(self, program: str, params: dict[str, Any] | None = None) -> Any

    # ── Profiling & Schema ──

    async def explain(self, cypher: str) -> Any
    async def profile(self, cypher: str) -> Any
    async def save_schema(self, path: str) -> None
    async def load_schema(self, path: str) -> None
```

**Example:**

```python
async with AsyncUniSession(db) as session:
    session.register(Person)
    await session.sync_schema()

    alice = Person(name="Alice", email="alice@example.com")
    session.add(alice)
    await session.commit()

    found = await session.get(Person, email="alice@example.com")
```

---

# 8. UniTransaction / AsyncUniTransaction

**Source:** `bindings/uni-pydantic/src/uni_pydantic/session.py`, `async_session.py`

Explicit transaction scope for grouping multiple operations atomically.

## UniTransaction (Sync)

```python
class UniTransaction:
    def __init__(self, session: UniSession) -> None

    def __enter__(self) -> UniTransaction
    def __exit__(...) -> None  # auto-commits on success, rolls back on exception

    def add(self, entity: UniNode) -> None
    def create_edge(
        self, source: UniNode, edge_type: str, target: UniNode,
        properties: UniEdge | None = None, **kwargs: Any,
    ) -> None
    def commit(self) -> None
    def rollback(self) -> None
```

## AsyncUniTransaction

```python
class AsyncUniTransaction:
    def __init__(self, session: AsyncUniSession) -> None

    async def __aenter__(self) -> AsyncUniTransaction
    async def __aexit__(...) -> None

    def add(self, entity: UniNode) -> None              # sync (just stages)
    def create_edge(self, source: UniNode, edge_type: str, target: UniNode,
                    properties: UniEdge | None = None) -> None  # sync
    async def commit(self) -> None
    async def rollback(self) -> None
```

**Example:**

```python
# Context manager — auto-commits on success, rolls back on exception
with session.transaction() as tx:
    alice = Person(name="Alice")
    bob = Person(name="Bob")
    tx.add(alice)
    tx.add(bob)
    tx.create_edge(alice, "FRIEND_OF", bob)

# Async
async with await session.transaction() as tx:
    tx.add(Person(name="Charlie"))
    await tx.commit()
```

---

# 9. QueryBuilder — Type-Safe Queries

**Source:** `bindings/uni-pydantic/src/uni_pydantic/query.py`

Immutable, type-safe query builder. Each method returns a new builder instance.

## Builder Methods

All builder methods return a new `QueryBuilder[NodeT]` (immutable):

```python
class QueryBuilder(Generic[NodeT]):
    def __init__(self, session: UniSession, model: type[NodeT]) -> None

    # ── Filters ──

    def filter(self, expr: FilterExpr) -> QueryBuilder[NodeT]
    def filter_by(self, **kwargs: Any) -> QueryBuilder[NodeT]

    # ── Ordering & Pagination ──

    def order_by(self, prop: PropertyProxy | str, descending: bool = False) -> QueryBuilder[NodeT]
    def limit(self, n: int) -> QueryBuilder[NodeT]
    def skip(self, n: int) -> QueryBuilder[NodeT]
    def distinct(self) -> QueryBuilder[NodeT]

    # ── Relationships ──

    def traverse(self, relationship: RelationshipDescriptor | str,
                 target_model: type[UniNode] | None = None) -> QueryBuilder[NodeT]
    def eager_load(self, *relationships: RelationshipDescriptor | str) -> QueryBuilder[NodeT]

    # ── Vector Search ──

    def vector_search(
        self, prop: PropertyProxy | str, query_vector: list[float],
        k: int = 10, threshold: float | None = None,
        pre_filter: str | None = None,
    ) -> QueryBuilder[NodeT]

    # ── Resource Limits ──

    def timeout(self, seconds: float) -> QueryBuilder[NodeT]
    def max_memory(self, bytes_: int) -> QueryBuilder[NodeT]

    # ── Terminal Methods (execute query) ──

    def all(self) -> list[NodeT]
    def first(self) -> NodeT | None
    def one(self) -> NodeT                  # raises QueryError if != 1 result
    def count(self) -> int
    def exists(self) -> bool
    def delete(self) -> int                 # DETACH DELETE, returns count
    def update(self, **kwargs: Any) -> int  # SET properties, returns count
```

**Example:**

```python
# Chained filtering with type-safe operators
results = (
    session.query(Person)
    .filter(Person.age >= 18)
    .filter(Person.email.is_not_null())
    .filter(Person.name.starts_with("A"))
    .order_by(Person.name)
    .skip(20)
    .limit(10)
    .all()
)

# Vector similarity search
similar = (
    session.query(Document)
    .vector_search(Document.embedding, query_vector, k=5, threshold=0.8)
    .all()
)

# Bulk operations
deleted = session.query(Person).filter(Person.age < 18).delete()
updated = session.query(Person).filter(Person.age < 18).update(status="minor")
```

---

# 10. AsyncQueryBuilder

**Source:** `bindings/uni-pydantic/src/uni_pydantic/async_query.py`

Same builder methods as `QueryBuilder`, but terminal methods are async:

```python
class AsyncQueryBuilder(Generic[NodeT]):
    def __init__(self, session: AsyncUniSession, model: type[NodeT]) -> None

    # Builder methods — same as QueryBuilder (sync, return new builder)
    def filter(self, expr: FilterExpr) -> AsyncQueryBuilder[NodeT]: ...
    def filter_by(self, **kwargs: Any) -> AsyncQueryBuilder[NodeT]: ...
    def order_by(...) -> AsyncQueryBuilder[NodeT]: ...
    def limit(self, n: int) -> AsyncQueryBuilder[NodeT]: ...
    def skip(self, n: int) -> AsyncQueryBuilder[NodeT]: ...
    def distinct(self) -> AsyncQueryBuilder[NodeT]: ...
    def traverse(...) -> AsyncQueryBuilder[NodeT]: ...
    def eager_load(...) -> AsyncQueryBuilder[NodeT]: ...
    def vector_search(...) -> AsyncQueryBuilder[NodeT]: ...
    def timeout(self, seconds: float) -> AsyncQueryBuilder[NodeT]: ...
    def max_memory(self, bytes_: int) -> AsyncQueryBuilder[NodeT]: ...

    # Terminal methods — async
    async def all(self) -> list[NodeT]: ...
    async def first(self) -> NodeT | None: ...
    async def one(self) -> NodeT: ...
    async def count(self) -> int: ...
    async def exists(self) -> bool: ...
    async def delete(self) -> int: ...
    async def update(self, **kwargs: Any) -> int: ...
```

---

# 11. Filter Expressions

**Source:** `bindings/uni-pydantic/src/uni_pydantic/query.py`

## FilterOp

```python
class FilterOp(Enum):
    EQ = "="
    NE = "<>"
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "=~"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    STARTS_WITH = "STARTS WITH"
    ENDS_WITH = "ENDS WITH"
    CONTAINS = "CONTAINS"
```

## FilterExpr

```python
@dataclass
class FilterExpr:
    property_name: str
    op: FilterOp
    value: Any = None

    def to_cypher(self, node_var: str, param_name: str) -> tuple[str, dict[str, Any]]
```

## PropertyProxy

Returned when accessing model properties at class level. Enables type-safe filter expressions.

```python
class PropertyProxy(Generic[T]):
    def __init__(self, property_name: str, model: type[UniNode]) -> None

    # Comparison operators (return FilterExpr)
    def __eq__(self, other: Any) -> FilterExpr
    def __ne__(self, other: Any) -> FilterExpr
    def __lt__(self, other: Any) -> FilterExpr
    def __le__(self, other: Any) -> FilterExpr
    def __gt__(self, other: Any) -> FilterExpr
    def __ge__(self, other: Any) -> FilterExpr

    # Collection operators
    def in_(self, values: Sequence[T]) -> FilterExpr
    def not_in(self, values: Sequence[T]) -> FilterExpr

    # String operators
    def like(self, pattern: str) -> FilterExpr
    def starts_with(self, prefix: str) -> FilterExpr
    def ends_with(self, suffix: str) -> FilterExpr
    def contains(self, substring: str) -> FilterExpr

    # Null checks
    def is_null(self) -> FilterExpr
    def is_not_null(self) -> FilterExpr
```

**Usage:**

```python
# These expressions are used with .filter()
Person.age >= 18              # FilterExpr(property_name="age", op=GE, value=18)
Person.name.starts_with("A") # FilterExpr(property_name="name", op=STARTS_WITH, value="A")
Person.email.is_null()        # FilterExpr(property_name="email", op=IS_NULL)
Person.age.in_([25, 30, 35]) # FilterExpr(property_name="age", op=IN, value=[25, 30, 35])
```

## Supporting Types

```python
@dataclass
class OrderByClause:
    property_name: str
    descending: bool = False

@dataclass
class TraversalStep:
    edge_type: str
    direction: Literal["outgoing", "incoming", "both"]
    target_label: str | None = None

@dataclass
class VectorSearchConfig:
    property_name: str
    query_vector: list[float]
    k: int
    threshold: float | None = None
    pre_filter: str | None = None
```

---

# 12. Schema Generation

**Source:** `bindings/uni-pydantic/src/uni_pydantic/schema.py`

Generates Uni database schema from registered Pydantic models.

## SchemaGenerator

```python
class SchemaGenerator:
    def __init__(self) -> None

    def register_node(self, model: type[UniNode]) -> None
    def register_edge(self, model: type[UniEdge]) -> None
    def register(self, *models: type[UniNode] | type[UniEdge]) -> None

    def generate(self) -> DatabaseSchema

    def apply_to_database(self, db: uni_db.Uni) -> None
    async def async_apply_to_database(self, db: uni_db.AsyncUni) -> None
```

### How `apply_to_database` Works

1. Calls `generate()` to produce a `DatabaseSchema` from registered models
2. Iterates labels — skips existing (`db.label_exists()`), creates new via `db.schema().label().property().done()`
3. Iterates edge types — skips existing, creates new via `db.schema().edge_type().property().done()`
4. Calls `builder.apply()` to atomically commit all schema changes
5. Second pass: creates vector and fulltext indexes via separate `db.schema().label().index().apply()` calls

### Known Limitations

- **Additive-only** — existing labels/edge types are skipped entirely (no new properties added)
- **Unique constraints** — recorded in `FieldConfig` but not created in `apply_to_database()`
- **Generated properties** — `Field(generated=...)` stored but not applied
- **Edge properties on existing types** — skipped
- **Fulltext/vector index errors** — silently swallowed (`try/except pass`)

## Schema Data Classes

```python
@dataclass
class PropertySchema:
    name: str
    data_type: str
    nullable: bool = False
    index_type: str | None = None
    unique: bool = False
    tokenizer: str | None = None
    metric: str | None = None

@dataclass
class LabelSchema:
    name: str
    properties: dict[str, PropertySchema]

@dataclass
class EdgeTypeSchema:
    name: str
    from_labels: list[str]
    to_labels: list[str]
    properties: dict[str, PropertySchema]

@dataclass
class DatabaseSchema:
    labels: dict[str, LabelSchema]
    edge_types: dict[str, EdgeTypeSchema]
```

## Convenience Function

```python
def generate_schema(*models: type[UniNode] | type[UniEdge]) -> DatabaseSchema
```

**Example:**

```python
# Manual schema generation
gen = SchemaGenerator()
gen.register(Person, Company, Follows)
schema = gen.generate()
gen.apply_to_database(db)

# Or via session (preferred)
session.register(Person, Company, Follows)
session.sync_schema()  # calls SchemaGenerator internally
```

---

# 13. Database Builders

**Source:** `bindings/uni-pydantic/src/uni_pydantic/database.py`

Thin wrappers around uni-db builders for ergonomic database creation.

## UniDatabase (Sync)

```python
class UniDatabase:
    @classmethod
    def open(cls, path: str) -> UniDatabase
    @classmethod
    def create(cls, path: str) -> UniDatabase
    @classmethod
    def open_existing(cls, path: str) -> UniDatabase
    @classmethod
    def temporary(cls) -> UniDatabase
    @classmethod
    def in_memory(cls) -> UniDatabase

    def cache_size(self, bytes_: int) -> UniDatabase
    def parallelism(self, n: int) -> UniDatabase
    def build(self) -> uni_db.Uni
```

## AsyncUniDatabase

```python
class AsyncUniDatabase:
    @classmethod
    def open(cls, path: str) -> AsyncUniDatabase
    @classmethod
    def temporary(cls) -> AsyncUniDatabase
    @classmethod
    def in_memory(cls) -> AsyncUniDatabase

    def cache_size(self, bytes_: int) -> AsyncUniDatabase
    def parallelism(self, n: int) -> AsyncUniDatabase
    async def build(self) -> uni_db.AsyncUni
```

**Example:**

```python
db = UniDatabase.open("./my_db").cache_size(256 * 1024 * 1024).build()
```

---

# 14. Lifecycle Hooks

**Source:** `bindings/uni-pydantic/src/uni_pydantic/hooks.py`

Decorators for model lifecycle events. Applied to methods on `UniNode` or `UniEdge` subclasses.

```python
@before_create   # Called before insert (entity not yet persisted)
@after_create    # Called after insert (vid/eid assigned)
@before_update   # Called before update (dirty fields detected)
@after_update    # Called after update committed
@before_delete   # Called before delete
@after_delete    # Called after delete committed
@before_load     # Called before hydration from DB result
@after_load      # Called after hydration from DB result
```

### Utility Functions

```python
def get_hooks(model: type[UniNode] | type[UniEdge], hook_type: str) -> list[Callable]
def run_hooks(entity: UniNode | UniEdge, hook_type: str, *args, **kwargs) -> None
def run_class_hooks(model: type, hook_type: str, *args, **kwargs) -> Any
```

**Example:**

```python
from uni_pydantic import before_create, after_create, before_delete
from datetime import datetime

class User(UniNode):
    name: str
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @before_create
    def set_timestamps(self):
        self.created_at = datetime.now()
        self.updated_at = datetime.now()

    @after_create
    def log_creation(self):
        print(f"Created user {self.name} with vid={self.vid}")

    @before_delete
    def guard_admin(self):
        if self.name == "admin":
            raise ValueError("Cannot delete admin user")
```

---

# 15. Type Mapping

**Source:** `bindings/uni-pydantic/src/uni_pydantic/types.py`

Automatic conversion between Python types and Uni database types.

| Python Type | Uni DataType | Notes |
|---|---|---|
| `str` | `string` | UTF-8 |
| `int` | `int` | 64-bit integer |
| `float` | `float` | 64-bit float |
| `bool` | `bool` | Boolean |
| `datetime` | `datetime` | Date and time |
| `date` | `date` | Date only |
| `time` | `time` | Time only |
| `timedelta` | `duration` | Duration |
| `bytes` | `bytes` | Binary data |
| `dict` | `json` | JSON object |
| `list[T]` | `list:T` | Typed list |
| `Vector[N]` | `vector:N` | N-dimensional vector |
| `T \| None` | `T` + nullable | Optional field |

### Utility Functions

```python
def python_type_to_uni(type_hint: Any, *, nullable: bool = False) -> tuple[str, bool]
    # Convert Python type hint to Uni DataType string. Returns (data_type, is_nullable).

def uni_to_python_type(uni_type: str) -> type
    # Convert Uni DataType string to Python type.

def get_vector_dimensions(type_hint: Any) -> int | None
    # Extract vector dimensions from Vector[N] type hint.

def is_optional(type_hint: Any) -> tuple[bool, Any]
    # Check if type is Optional (T | None). Returns (is_optional, inner_type).

def is_list_type(type_hint: Any) -> tuple[bool, Any | None]
    # Check if type is list[T]. Returns (is_list, element_type).

def unwrap_annotated(type_hint: Any) -> tuple[Any, tuple[Any, ...]]
    # Unwrap Annotated[T, ...]. Returns (base_type, metadata).

def python_to_db_value(value: Any, type_hint: Any) -> Any
    # Convert Python value to database-compatible value (e.g., Vector → list[float]).

def db_to_python_value(value: Any, type_hint: Any) -> Any
    # Convert database value back to Python value (e.g., list[float] → Vector).
```

---

# 16. Exceptions

**Source:** `bindings/uni-pydantic/src/uni_pydantic/exceptions.py`

All exceptions inherit from `UniPydanticError`.

```python
# Base
class UniPydanticError(Exception): ...

# Schema
class SchemaError(UniPydanticError):
    def __init__(self, message: str, model: type | None = None): ...

class TypeMappingError(SchemaError):
    def __init__(self, python_type: Any, message: str | None = None): ...

# Validation
class ValidationError(UniPydanticError): ...

# Session
class SessionError(UniPydanticError): ...

class NotRegisteredError(SessionError):
    def __init__(self, model: type[UniNode] | type[UniEdge]): ...

class NotPersisted(SessionError):
    def __init__(self, entity: UniNode | UniEdge): ...

class NotTrackedError(SessionError): ...

# Transaction
class TransactionError(SessionError): ...

# Query
class QueryError(UniPydanticError): ...

class CypherInjectionError(QueryError):
    def __init__(self, name: str, reason: str | None = None): ...

# Relationships
class RelationshipError(UniPydanticError): ...

class LazyLoadError(RelationshipError):
    def __init__(self, field_name: str, reason: str): ...

# Bulk
class BulkLoadError(UniPydanticError): ...
```

### When Each Exception Is Raised

| Exception | Trigger |
|---|---|
| `NotRegisteredError` | Using a model not registered with `session.register()` |
| `NotPersisted` | Calling `refresh()` or edge ops on an entity without a VID |
| `NotTrackedError` | Operating on an entity not tracked by this session |
| `QueryError` | Query execution failure or `one()` with != 1 result |
| `CypherInjectionError` | Property name fails validation (potential injection) |
| `LazyLoadError` | Accessing relationship on entity without an active session |
| `SchemaError` | Model missing `__label__` or `__edge_type__` |
| `TypeMappingError` | Unsupported Python type in model field |
| `TransactionError` | Transaction state violation (double commit, etc.) |
| `BulkLoadError` | Failure during `bulk_add()` |
