# Uni — Complete Python API Reference

**Source-verified: March 2026**
**Package:** `uni-db`

This document catalogs every public class, method, property, and exception in the Uni Python API. The API provides both **synchronous** and **asynchronous** variants. Both are documented side-by-side.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Architecture Overview](#architecture-overview)
- [Quick Reference Tables](#quick-reference-tables)
- [1. Uni / AsyncUni — The Database Handle](#1-uni--asyncuni--the-database-handle)
- [2. UniBuilder / AsyncUniBuilder — Database Configuration](#2-unibuilder--asyncunibuilder--database-configuration)
- [3. Session / AsyncSession — The Read Scope](#3-session--asyncsession--the-read-scope)
- [4. Transaction / AsyncTransaction — The Write Scope](#4-transaction--asynctransaction--the-write-scope)
- [5. Facade Types](#5-facade-types)
- [6. Query Builders](#6-query-builders)
- [7. Locy Builders](#7-locy-builders)
- [8. Transaction Builders](#8-transaction-builders)
- [9. BulkWriter & StreamingAppender](#9-bulkwriter--streamingappender)
- [10. Schema Builders](#10-schema-builders)
- [11. Result Types](#11-result-types)
- [12. Query & Row Types](#12-query--row-types)
- [13. Graph Element Types](#13-graph-element-types)
- [14. Locy Types](#14-locy-types)
- [15. Schema Info Types](#15-schema-info-types)
- [16. Configuration](#16-configuration)
- [17. Observability & Metrics](#17-observability--metrics)
- [18. Commit Notifications](#18-commit-notifications)
- [19. Prepared Statements](#19-prepared-statements)
- [20. Session Templates](#20-session-templates)
- [21. Xervo (ML Runtime)](#21-xervo-ml-runtime)
- [22. ID & Value Types](#22-id--value-types)
- [23. Exception Hierarchy](#23-exception-hierarchy)

---

## Quick Start

### Synchronous

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

### Asynchronous

```python
from uni_db import AsyncUni, DataType

async def main():
    db = await AsyncUni.open("./my_db")

    await db.schema() \
        .label("Person") \
            .property("name", DataType.STRING()) \
            .property("age", DataType.INT64()) \
        .apply()

    session = db.session()
    async with await session.tx() as tx:
        await tx.execute("CREATE (:Person {name: 'Alice', age: 30})")
        await tx.commit()

    result = await session.query("MATCH (p:Person) RETURN p.name, p.age")
    for row in result:
        print(f"{row['p.name']}: {row['p.age']}")

    await db.shutdown()
```

---

## Architecture Overview

### Three Scopes

```
Uni / AsyncUni (database handle)
  ├─ Factory: open(), session(), schema()
  ├─ Admin: flush, snapshots, indexes, compaction
  └─ NO direct query or mutation

Session / AsyncSession (read scope)
  ├─ Parameters: set(), get()
  ├─ Reads: query(), locy()
  ├─ Analysis: explain(), profile()
  └─ Factory: tx() → Transaction

Transaction / AsyncTransaction (write scope)
  ├─ Reads: query() (sees uncommitted writes)
  ├─ Writes: execute(), bulk loading
  ├─ Locy: locy() (DERIVE auto-applies)
  └─ Lifecycle: commit(), rollback()
```

### Sync / Async Duality

Every core type has both synchronous and asynchronous variants:

| Sync | Async |
|------|-------|
| `Uni` | `AsyncUni` |
| `UniBuilder` | `AsyncUniBuilder` |
| `Session` | `AsyncSession` |
| `Transaction` | `AsyncTransaction` |
| `SchemaBuilder` | `AsyncSchemaBuilder` |
| `LabelBuilder` | `AsyncLabelBuilder` |
| `EdgeTypeBuilder` | `AsyncEdgeTypeBuilder` |
| `SessionQueryBuilder` | `AsyncSessionQueryBuilder` |
| `SessionLocyBuilder` | `AsyncSessionLocyBuilder` |
| `TxQueryBuilder` | `AsyncTxQueryBuilder` |
| `TxExecuteBuilder` | `AsyncTxExecuteBuilder` |
| `TxLocyBuilder` | `AsyncTxLocyBuilder` |
| `ApplyBuilder` | `AsyncApplyBuilder` |
| `TransactionBuilder` | `AsyncTransactionBuilder` |
| `BulkWriter` | `AsyncBulkWriter` |
| `TxBulkWriterBuilder` | `AsyncTxBulkWriterBuilder` |
| `QueryCursor` | `AsyncQueryCursor` |
| `CommitStream` | `AsyncCommitStream` |
| `Compaction` | `AsyncCompaction` |
| `Indexes` | `AsyncIndexes` |
| `Xervo` | `AsyncXervo` |

Shared types (results, data classes, exceptions) are the same for both.

---

## Quick Reference Tables

### Facade Accessors

| Accessor | Returns | Purpose |
|---|---|---|
| `db.rules()` | `RuleRegistry` | Locy rule management |
| `db.compaction()` | `Compaction` / `AsyncCompaction` | Storage compaction |
| `db.indexes()` | `Indexes` / `AsyncIndexes` | Index management |
| `db.xervo()` | `Xervo` / `AsyncXervo` | ML model runtime |

### Builder Terminal Methods

| Builder | Terminal |
|---|---|
| `SessionQueryBuilder` | `.fetch_all()`, `.fetch_one()`, `.cursor()` |
| `SessionLocyBuilder` | `.run()` |
| `TxQueryBuilder` | `.fetch_all()`, `.fetch_one()`, `.execute()`, `.cursor()` |
| `TxExecuteBuilder` | `.run()` |
| `TxLocyBuilder` | `.run()` |
| `ApplyBuilder` | `.run()` |
| `TransactionBuilder` | `.start()` |
| `BulkWriterBuilder` | `.build()` |
| `AppenderBuilder` | `.build()` |
| `SchemaBuilder` | `.apply()` |
| `SessionTemplateBuilder` | `.build()` |
| `WatchBuilder` | `.build()`, `.build_async()` |

---

# 1. Uni / AsyncUni — The Database Handle

The database lifecycle and administration handle. Opens the database, manages schema and storage, and provides factories for Sessions.

## Uni (Synchronous)

```python
class Uni:
    # ── Static Factories ──

    @staticmethod
    def open(path: str) -> Uni: ...

    @staticmethod
    def create(path: str) -> Uni: ...

    @staticmethod
    def open_existing(path: str) -> Uni: ...

    @staticmethod
    def temporary() -> Uni: ...

    @staticmethod
    def in_memory() -> Uni: ...

    @staticmethod
    def builder() -> UniBuilder: ...

    # ── Session Factory ──

    def session(self) -> Session: ...
    def session_template(self) -> SessionTemplateBuilder: ...

    # ── Schema DDL & Inspection ──

    def schema(self) -> SchemaBuilder: ...
    def label_exists(self, name: str) -> bool: ...
    def edge_type_exists(self, name: str) -> bool: ...
    def list_labels(self) -> list[str]: ...
    def list_edge_types(self) -> list[str]: ...
    def get_label_info(self, name: str) -> LabelInfo | None: ...
    def get_edge_type_info(self, name: str) -> EdgeTypeInfo | None: ...
    def load_schema(self, path: str) -> None: ...
    def save_schema(self, path: str) -> None: ...

    # ── Facades ──

    def rules(self) -> RuleRegistry: ...
    def xervo(self) -> Xervo: ...
    def compaction(self) -> Compaction: ...
    def indexes(self) -> Indexes: ...

    # ── Storage Admin ──

    def flush(self) -> None: ...
    def create_snapshot(self, name: str) -> str: ...
    def list_snapshots(self) -> list[SnapshotInfo]: ...
    def restore_snapshot(self, snapshot_id: str) -> None: ...

    # ── Observability & Lifecycle ──

    def metrics(self) -> DatabaseMetrics: ...
    def config(self) -> dict[str, Any]: ...
    def write_lease(self) -> WriteLease | None: ...
    def shutdown(self) -> None: ...

    # ── Context Manager ──

    def __enter__(self) -> Uni: ...
    def __exit__(...) -> bool: ...
```

## AsyncUni (Asynchronous)

Same API as `Uni` but:
- Factory methods are async: `await AsyncUni.open("./db")`
- Admin methods are async: `await db.flush()`, `await db.shutdown()`
- Schema inspection is async: `await db.label_exists("Person")`
- `session()` is sync (returns `AsyncSession`)
- Uses `async with` context manager

```python
class AsyncUni:
    @staticmethod
    async def open(path: str) -> AsyncUni: ...

    @staticmethod
    async def temporary() -> AsyncUni: ...

    # ... same pattern for create, open_existing, in_memory

    @staticmethod
    def builder() -> AsyncUniBuilder: ...    # sync — returns builder

    def session(self) -> AsyncSession: ...   # sync — cheap, no I/O
    def rules(self) -> RuleRegistry: ...     # sync
    def xervo(self) -> AsyncXervo: ...       # sync — returns async facade
    def compaction(self) -> AsyncCompaction: ...
    def indexes(self) -> AsyncIndexes: ...
    def metrics(self) -> DatabaseMetrics: ...
    def config(self) -> dict[str, Any]: ...

    def schema(self) -> AsyncSchemaBuilder: ...

    async def flush(self) -> None: ...
    async def create_snapshot(self, name: str) -> str: ...
    async def shutdown(self) -> None: ...
    # ... etc.

    async def __aenter__(self) -> AsyncUni: ...
    async def __aexit__(...) -> bool: ...
```

---

# 2. UniBuilder / AsyncUniBuilder — Database Configuration

Created by `Uni.builder()` or `AsyncUni.builder()` for advanced configuration.

```python
class UniBuilder:
    # ── Static Factories ──

    @staticmethod
    def open(path: str) -> UniBuilder: ...
    @staticmethod
    def create(path: str) -> UniBuilder: ...
    @staticmethod
    def open_existing(path: str) -> UniBuilder: ...
    @staticmethod
    def temporary() -> UniBuilder: ...
    @staticmethod
    def in_memory() -> UniBuilder: ...

    # ── Configuration ──

    def config(self, config: dict[str, Any]) -> UniBuilder: ...
    def cache_size(self, bytes: int) -> UniBuilder: ...
    def parallelism(self, n: int) -> UniBuilder: ...
    def batch_size(self, size: int) -> UniBuilder: ...
    def wal_enabled(self, enabled: bool) -> UniBuilder: ...
    def schema_file(self, path: str) -> UniBuilder: ...
    def hybrid(self, local_path: str, remote_url: str) -> UniBuilder: ...
    def cloud_config(self, config: dict[str, Any]) -> UniBuilder: ...

    # ── Xervo ──

    def xervo_catalog_from_str(self, json: str) -> UniBuilder: ...
    def xervo_catalog_from_file(self, path: str) -> UniBuilder: ...

    # ── Multi-Agent ──

    def read_only(self) -> UniBuilder: ...
    def write_lease(self, lease: WriteLease) -> UniBuilder: ...

    # ── Build ──

    def build(self) -> Uni: ...
```

`AsyncUniBuilder` has the same configuration methods; only `build()` is async:

```python
class AsyncUniBuilder:
    # ... same config methods ...
    async def build(self) -> AsyncUni: ...
```

**Example:**

```python
db = UniBuilder.open("./my_db") \
    .cache_size(256 * 1024 * 1024) \
    .wal_enabled(True) \
    .parallelism(4) \
    .build()
```

---

# 3. Session / AsyncSession — The Read Scope

Sessions are the primary scope for reads and the factory for transactions. Cheap to create, hold scoped parameters and a private Locy rule registry.

## Session (Synchronous)

```python
class Session:
    # ── Parameters ──

    def set(self, key: str, value: Any) -> Session: ...
    def set_all(self, params: dict[str, Any]) -> None: ...
    def get(self, key: str) -> Any | None: ...

    # ── Cypher Reads ──

    def query(
        self, cypher: str, params: dict[str, Any] | None = None,
    ) -> QueryResult: ...
    def query_with(self, cypher: str) -> SessionQueryBuilder: ...
    def query_cursor(
        self, cypher: str, params: dict[str, Any] | None = None,
    ) -> QueryCursor: ...

    # ── Locy ──

    def locy(
        self, program: str, params: dict[str, Any] | None = None,
    ) -> LocyResult: ...
    def locy_with(self, program: str) -> SessionLocyBuilder: ...
    def rules(self) -> RuleRegistry: ...
    def compile_locy(self, program: str) -> CompiledProgram: ...

    # ── Planning & Introspection ──

    def explain(self, cypher: str) -> ExplainOutput: ...
    def explain_locy(self, program: str) -> LocyExplainOutput: ...
    def profile(self, cypher: str) -> tuple[QueryResult, ProfileOutput]: ...
    def profile_with(self, cypher: str) -> ProfileBuilder: ...

    # ── Prepared Statements ──

    def prepare(self, cypher: str) -> PreparedQuery: ...
    def prepare_locy(self, program: str) -> PreparedLocy: ...

    # ── Transaction Factory ──

    def tx(self) -> Transaction: ...
    def tx_with(self) -> TransactionBuilder: ...

    # ── Version Pinning ──

    def pin_to_version(self, snapshot_id: str) -> None: ...
    def pin_to_timestamp(self, epoch_secs: float) -> None: ...
    def refresh(self) -> None: ...
    def is_pinned(self) -> bool: ...

    # ── Custom Functions ──

    def register_function(self, name: str, func: Callable[..., Any]) -> None: ...

    # ── Hooks & Notifications ──

    def add_hook(self, hook: Any) -> None: ...
    def watch(self) -> CommitStream: ...
    def watch_with(self) -> WatchBuilder: ...

    # ── Cancellation ──

    def cancel(self) -> None: ...
    def cancellation_token(self) -> CancellationToken: ...

    # ── Observability ──

    def id(self) -> str: ...
    def capabilities(self) -> SessionCapabilities: ...
    def metrics(self) -> SessionMetrics: ...

    # ── Context Manager ──

    def __enter__(self) -> Session: ...
    def __exit__(...) -> bool: ...
```

## AsyncSession

Same API with async methods:

```python
class AsyncSession:
    # ── Parameters (async) ──

    async def set(self, key: str, value: Any) -> None: ...
    async def set_all(self, params: dict[str, Any]) -> None: ...
    async def get(self, key: str) -> Any | None: ...

    # ── Cypher Reads (async) ──

    async def query(
        self, cypher: str, params: dict[str, Any] | None = None,
    ) -> QueryResult: ...
    def query_with(self, cypher: str) -> AsyncSessionQueryBuilder: ...  # sync
    async def query_cursor(
        self, cypher: str, params: dict[str, Any] | None = None,
    ) -> AsyncQueryCursor: ...

    # ── Locy (async) ──

    async def locy(
        self, program: str, params: dict[str, Any] | None = None,
    ) -> LocyResult: ...
    def locy_with(self, program: str) -> AsyncSessionLocyBuilder: ...  # sync
    def rules(self) -> RuleRegistry: ...                                # sync
    async def compile_locy(self, program: str) -> CompiledProgram: ...

    # ── Planning (async) ──

    async def explain(self, cypher: str) -> ExplainOutput: ...
    async def explain_locy(self, program: str) -> LocyExplainOutput: ...
    async def profile(
        self, cypher: str, params: dict[str, Any] | None = None,
    ) -> tuple[QueryResult, ProfileOutput]: ...
    def profile_with(self, cypher: str) -> AsyncSessionProfileBuilder: ...

    # ── Prepared Statements (async) ──

    async def prepare(self, cypher: str) -> PreparedQuery: ...
    async def prepare_locy(self, program: str) -> PreparedLocy: ...

    # ── Transaction Factory ──

    async def tx(self, timeout: float | None = None) -> AsyncTransaction: ...
    def tx_with(self) -> AsyncTransactionBuilder: ...

    # ── Version Pinning (async) ──

    async def pin_to_version(self, snapshot_id: str) -> None: ...
    async def pin_to_timestamp(self, epoch_secs: float) -> None: ...
    async def refresh(self) -> None: ...
    async def is_pinned(self) -> bool: ...

    # ── Custom Functions (async) ──

    async def register_function(self, name: str, func: Callable[..., Any]) -> None: ...

    # ── Hooks & Notifications (async) ──

    async def add_hook(self, hook: Any) -> None: ...
    async def watch(self) -> AsyncCommitStream: ...
    async def watch_with(self) -> WatchBuilder: ...

    # ── Cancellation (async) ──

    async def cancel(self) -> None: ...
    async def cancellation_token(self) -> CancellationToken: ...

    # ── Observability (async) ──

    async def id(self) -> str: ...
    async def capabilities(self) -> SessionCapabilities: ...
    async def metrics(self) -> SessionMetrics: ...

    # ── Context Manager ──

    async def __aenter__(self) -> AsyncSession: ...
    async def __aexit__(...) -> bool: ...
```

**Example:**

```python
session = db.session()
session.set("min_age", 25)
result = session.query(
    "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name",
    {"min_age": 25}  # inline params also supported
)
```

---

# 4. Transaction / AsyncTransaction — The Write Scope

Transactions provide ACID guarantees. Use as context managers for automatic rollback on error.

## Transaction (Synchronous)

```python
class Transaction:
    # ── Cypher Reads ──

    def query(
        self, cypher: str, params: dict[str, Any] | None = None,
    ) -> QueryResult: ...
    def query_with(self, cypher: str) -> TxQueryBuilder: ...

    # ── Cypher Writes ──

    def execute(
        self, cypher: str, params: dict[str, Any] | None = None,
    ) -> ExecuteResult: ...
    def execute_with(self, cypher: str) -> TxExecuteBuilder: ...

    # ── Locy ──

    def locy(
        self, program: str, params: dict[str, Any] | None = None,
    ) -> LocyResult: ...
    def locy_with(self, program: str) -> TxLocyBuilder: ...

    # ── Apply DerivedFactSet ──

    def apply(self, derived: DerivedFactSet) -> ApplyResult: ...
    def apply_with(self, derived: DerivedFactSet) -> ApplyBuilder: ...

    # ── Rule Management ──

    def rules(self) -> RuleRegistry: ...

    # ── Prepared Statements ──

    def prepare(self, cypher: str) -> PreparedQuery: ...
    def prepare_locy(self, program: str) -> PreparedLocy: ...

    # ── Lifecycle ──

    def commit(self) -> CommitResult: ...
    def rollback(self) -> None: ...
    def id(self) -> str: ...
    def started_at_version(self) -> int: ...
    def is_dirty(self) -> bool: ...
    def is_completed(self) -> bool: ...

    # ── Cancellation ──

    def cancel(self) -> None: ...
    def cancellation_token(self) -> CancellationToken: ...

    # ── Bulk Loading ──

    def bulk_writer(self) -> TxBulkWriterBuilder: ...
    def appender(self, label: str) -> StreamingAppender: ...
    def appender_builder(self, label: str) -> TxAppenderBuilder: ...

    # ── Context Manager ──

    def __enter__(self) -> Transaction: ...
    def __exit__(...) -> bool: ...  # auto-rollback if not committed
```

## AsyncTransaction

```python
class AsyncTransaction:
    # ── Cypher Reads (async) ──

    async def query(
        self, cypher: str, params: dict[str, Any] | None = None,
    ) -> QueryResult: ...
    def query_with(self, cypher: str) -> AsyncTxQueryBuilder: ...

    # ── Cypher Writes (async) ──

    async def execute(
        self, cypher: str, params: dict[str, Any] | None = None,
    ) -> ExecuteResult: ...
    def execute_with(self, cypher: str) -> AsyncTxExecuteBuilder: ...

    # ── Locy (async) ──

    async def locy(
        self, program: str, params: dict[str, Any] | None = None,
    ) -> LocyResult: ...
    def locy_with(self, program: str) -> AsyncTxLocyBuilder: ...

    # ── Apply DerivedFactSet (async) ──

    async def apply(
        self, derived: DerivedFactSet,
        require_fresh: bool = False,
        max_version_gap: int | None = None,
    ) -> ApplyResult: ...
    async def apply_with(self, derived: DerivedFactSet) -> AsyncApplyBuilder: ...

    # ── Rule Management ──

    def rules(self) -> RuleRegistry: ...

    # ── Prepared Statements (async) ──

    async def prepare(self, cypher: str) -> PreparedQuery: ...
    async def prepare_locy(self, program: str) -> PreparedLocy: ...

    # ── Lifecycle (async) ──

    async def commit(self) -> CommitResult: ...
    async def rollback(self) -> None: ...
    async def id(self) -> str: ...
    async def started_at_version(self) -> int: ...
    async def is_dirty(self) -> bool: ...
    async def is_completed(self) -> bool: ...

    # ── Cancellation (async) ──

    async def cancel(self) -> None: ...
    async def cancellation_token(self) -> CancellationToken: ...

    # ── Bulk Loading ──

    def bulk_writer(self) -> AsyncTxBulkWriterBuilder: ...
    async def appender(self, label: str) -> StreamingAppender: ...

    # ── Context Manager ──

    async def __aenter__(self) -> AsyncTransaction: ...
    async def __aexit__(...) -> bool: ...
```

**Example:**

```python
# Context manager with auto-rollback
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

---

# 5. Facade Types

## RuleRegistry

Returned by `db.rules()`, `session.rules()`, or `tx.rules()`. Manages pre-compiled Locy rules.

```python
class RuleRegistry:
    def register(self, program: str) -> None: ...
    def remove(self, name: str) -> bool: ...
    def list(self) -> list[str]: ...
    def get(self, name: str) -> RuleInfo | None: ...
    def clear(self) -> None: ...
    def count(self) -> int: ...
```

## Compaction / AsyncCompaction

Returned by `db.compaction()`.

```python
class Compaction:
    def compact(self, name: str) -> CompactionStats: ...
    def wait(self) -> None: ...

class AsyncCompaction:
    async def compact(self, name: str) -> CompactionStats: ...
    async def wait(self) -> None: ...
```

## Indexes / AsyncIndexes

Returned by `db.indexes()`.

```python
class Indexes:
    def list(self, label: str | None = None) -> list[IndexDefinitionInfo]: ...
    def rebuild(self, label: str, background: bool = False) -> str | None: ...
    def rebuild_status(self) -> list[IndexRebuildTaskInfo]: ...
    def retry_failed(self) -> list[str]: ...

class AsyncIndexes:
    def list(self, label: str | None = None) -> list[IndexDefinitionInfo]: ...  # sync
    async def rebuild(self, label: str, background: bool = False) -> str | None: ...
    async def rebuild_status(self) -> list[IndexRebuildTaskInfo]: ...
    async def retry_failed(self) -> list[str]: ...
```

---

# 6. Query Builders

## SessionQueryBuilder / AsyncSessionQueryBuilder

Created by `session.query_with(cypher)`.

```python
class SessionQueryBuilder:
    def param(self, name: str, value: Any) -> SessionQueryBuilder: ...
    def params(self, params: dict[str, Any]) -> SessionQueryBuilder: ...
    def timeout(self, seconds: float) -> SessionQueryBuilder: ...
    def max_memory(self, bytes: int) -> SessionQueryBuilder: ...
    def cancellation_token(self, token: CancellationToken) -> SessionQueryBuilder: ...

    # Terminal methods
    def fetch_all(self) -> QueryResult: ...
    def fetch_one(self) -> dict[str, Any] | None: ...
    def cursor(self) -> QueryCursor: ...

class AsyncSessionQueryBuilder:
    # Same config methods (sync)...
    async def fetch_all(self) -> QueryResult: ...
    async def fetch_one(self) -> dict[str, Any] | None: ...
    async def cursor(self) -> AsyncQueryCursor: ...
```

## ProfileBuilder / AsyncSessionProfileBuilder

Created by `session.profile_with(cypher)`.

```python
class ProfileBuilder:
    def param(self, name: str, value: Any) -> ProfileBuilder: ...
    def run(self) -> tuple[QueryResult, ProfileOutput]: ...

class AsyncSessionProfileBuilder:
    def param(self, name: str, value: Any) -> AsyncSessionProfileBuilder: ...
    def params(self, params: dict[str, Any]) -> AsyncSessionProfileBuilder: ...
    async def run(self) -> tuple[QueryResult, ProfileOutput]: ...
```

## TxQueryBuilder / AsyncTxQueryBuilder

Created by `tx.query_with(cypher)`.

```python
class TxQueryBuilder:
    def param(self, name: str, value: Any) -> TxQueryBuilder: ...
    def timeout(self, seconds: float) -> TxQueryBuilder: ...

    # Terminal methods
    def fetch_all(self) -> QueryResult: ...
    def fetch_one(self) -> dict[str, Any] | None: ...
    def execute(self) -> ExecuteResult: ...
    def cursor(self) -> QueryCursor: ...

class AsyncTxQueryBuilder:
    # Same config methods (sync)...
    async def fetch_all(self) -> QueryResult: ...
    async def fetch_one(self) -> dict[str, Any] | None: ...
    async def execute(self) -> ExecuteResult: ...
    async def cursor(self) -> AsyncQueryCursor: ...
```

## TxExecuteBuilder / AsyncTxExecuteBuilder

Created by `tx.execute_with(cypher)`.

```python
class TxExecuteBuilder:
    def param(self, name: str, value: Any) -> TxExecuteBuilder: ...
    def timeout(self, seconds: float) -> TxExecuteBuilder: ...
    def run(self) -> ExecuteResult: ...

class AsyncTxExecuteBuilder:
    # Same config methods...
    async def run(self) -> ExecuteResult: ...
```

---

# 7. Locy Builders

## SessionLocyBuilder / AsyncSessionLocyBuilder

Created by `session.locy_with(program)`.

```python
class SessionLocyBuilder:
    def param(self, name: str, value: Any) -> SessionLocyBuilder: ...
    def params(self, params: dict[str, Any]) -> SessionLocyBuilder: ...
    def timeout(self, seconds: float) -> SessionLocyBuilder: ...
    def max_iterations(self, n: int) -> SessionLocyBuilder: ...
    def with_config(self, config: dict[str, Any] | LocyConfig) -> SessionLocyBuilder: ...
    def cancellation_token(self, token: CancellationToken) -> SessionLocyBuilder: ...
    def run(self) -> LocyResult: ...

class AsyncSessionLocyBuilder:
    # Same config methods (sync)...
    async def run(self) -> LocyResult: ...
```

## TxLocyBuilder / AsyncTxLocyBuilder

Created by `tx.locy_with(program)`.

```python
class TxLocyBuilder:
    def param(self, name: str, value: Any) -> TxLocyBuilder: ...
    def timeout(self, seconds: float) -> TxLocyBuilder: ...
    def max_iterations(self, n: int) -> TxLocyBuilder: ...
    def with_config(self, config: dict[str, Any] | LocyConfig) -> TxLocyBuilder: ...
    def cancellation_token(self, token: CancellationToken) -> TxLocyBuilder: ...
    def run(self) -> LocyResult: ...

class AsyncTxLocyBuilder:
    # Same config methods (sync)...
    async def run(self) -> LocyResult: ...
```

## ApplyBuilder / AsyncApplyBuilder

Created by `tx.apply_with(derived)`.

```python
class ApplyBuilder:
    def require_fresh(self, require: bool) -> ApplyBuilder: ...
    def max_version_gap(self, gap: int) -> ApplyBuilder: ...
    def run(self) -> ApplyResult: ...

class AsyncApplyBuilder:
    def require_fresh(self, require: bool) -> AsyncApplyBuilder: ...
    def max_version_gap(self, gap: int) -> AsyncApplyBuilder: ...
    async def run(self) -> ApplyResult: ...
```

---

# 8. Transaction Builders

## TransactionBuilder / AsyncTransactionBuilder

Created by `session.tx_with()`.

```python
class TransactionBuilder:
    def timeout(self, seconds: float) -> TransactionBuilder: ...
    def isolation(self, level: str) -> TransactionBuilder: ...
    def start(self) -> Transaction: ...

class AsyncTransactionBuilder:
    def timeout(self, seconds: float) -> AsyncTransactionBuilder: ...
    def isolation(self, level: str) -> AsyncTransactionBuilder: ...
    async def start(self) -> AsyncTransaction: ...
```

---

# 9. BulkWriter & StreamingAppender

## BulkWriterBuilder / TxBulkWriterBuilder

Created by `tx.bulk_writer()`.

```python
class TxBulkWriterBuilder:
    def defer_vector_indexes(self, defer: bool) -> TxBulkWriterBuilder: ...
    def defer_scalar_indexes(self, defer: bool) -> TxBulkWriterBuilder: ...
    def batch_size(self, size: int) -> TxBulkWriterBuilder: ...
    def async_indexes(self, async_: bool) -> TxBulkWriterBuilder: ...
    def validate_constraints(self, validate: bool) -> TxBulkWriterBuilder: ...
    def max_buffer_size_bytes(self, size: int) -> TxBulkWriterBuilder: ...
    def on_progress(self, callback: Callable[[BulkProgress], None]) -> TxBulkWriterBuilder: ...
    def build(self) -> BulkWriter: ...

class AsyncTxBulkWriterBuilder:
    # Same config methods...
    async def build(self) -> AsyncBulkWriter: ...
```

Also available as standalone `BulkWriterBuilder` and `AsyncBulkWriterBuilder`.

## BulkWriter / AsyncBulkWriter

```python
class BulkWriter:
    def insert_vertices(self, label: str, vertices: list[dict[str, Any]]) -> list[int]: ...
    def insert_edges(
        self, edge_type: str,
        edges: list[tuple[int, int, dict[str, Any]]],
    ) -> None: ...
    def stats(self) -> BulkStats: ...
    def touched_labels(self) -> list[str]: ...
    def touched_edge_types(self) -> list[str]: ...
    def commit(self) -> BulkStats: ...
    def abort(self) -> None: ...
    def __enter__(self) -> BulkWriter: ...
    def __exit__(...) -> bool: ...

class AsyncBulkWriter:
    async def insert_vertices(self, label: str, vertices: list[dict[str, Any]]) -> list[int]: ...
    async def insert_edges(...) -> None: ...
    def stats(self) -> BulkStats: ...          # sync
    def touched_labels(self) -> list[str]: ... # sync
    def touched_edge_types(self) -> list[str]: ... # sync
    async def commit(self) -> BulkStats: ...
    async def abort(self) -> None: ...
    async def __aenter__(self) -> AsyncBulkWriter: ...
    async def __aexit__(...) -> bool: ...
```

## AppenderBuilder / StreamingAppender

Created by `tx.appender_builder(label)`.

```python
class AppenderBuilder:
    def batch_size(self, size: int) -> AppenderBuilder: ...
    def defer_vector_indexes(self, defer: bool) -> AppenderBuilder: ...
    def max_buffer_size_bytes(self, size: int) -> AppenderBuilder: ...
    def build(self) -> StreamingAppender: ...

class StreamingAppender:
    def append(self, properties: dict[str, Any]) -> None: ...
    def write_batch(self, batch: Any) -> None: ...  # Arrow RecordBatch
    def finish(self) -> BulkStats: ...
    def abort(self) -> None: ...
    def buffered_count(self) -> int: ...
    def __enter__(self) -> StreamingAppender: ...
    def __exit__(...) -> bool: ...
```

**Example:**

```python
with tx.bulk_writer().batch_size(5000).build() as writer:
    vids = writer.insert_vertices("Person", [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ])
    writer.insert_edges("KNOWS", [(vids[0], vids[1], {"since": 2024})])
    stats = writer.commit()
    print(f"Inserted {stats.vertices_inserted} vertices, {stats.edges_inserted} edges")
```

---

# 10. Schema Builders

## SchemaBuilder / AsyncSchemaBuilder

Created by `db.schema()`.

```python
class SchemaBuilder:
    def current(self) -> dict[str, Any]: ...
    def current_typed(self) -> Schema: ...
    def label(self, name: str) -> LabelBuilder: ...
    def edge_type(self, name: str, from_labels: list[str], to_labels: list[str]) -> EdgeTypeBuilder: ...
    def apply(self) -> None: ...

class AsyncSchemaBuilder:
    # Same config methods (sync)...
    async def apply(self) -> None: ...
```

## LabelBuilder / AsyncLabelBuilder

```python
class LabelBuilder:
    def property(self, name: str, data_type: str | DataType) -> LabelBuilder: ...
    def property_nullable(self, name: str, data_type: str | DataType) -> LabelBuilder: ...
    def vector(self, name: str, dimensions: int) -> LabelBuilder: ...
    def index(self, property: str, index_type: str | dict[str, Any]) -> LabelBuilder: ...
    def done(self) -> SchemaBuilder: ...
    def apply(self) -> None: ...
```

## EdgeTypeBuilder / AsyncEdgeTypeBuilder

```python
class EdgeTypeBuilder:
    def property(self, name: str, data_type: str | DataType) -> EdgeTypeBuilder: ...
    def property_nullable(self, name: str, data_type: str | DataType) -> EdgeTypeBuilder: ...
    def done(self) -> SchemaBuilder: ...
    def apply(self) -> None: ...
```

**Example:**

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

---

# 11. Result Types

## ExecuteResult

```python
class ExecuteResult:
    affected_rows: int
    nodes_created: int
    nodes_deleted: int
    relationships_created: int
    relationships_deleted: int
    properties_set: int
    labels_added: int
    labels_removed: int
    metrics: dict[str, Any]
```

## CommitResult

```python
class CommitResult:
    mutations_committed: int
    rules_promoted: int
    version: int
    started_at_version: int
    wal_lsn: int
    duration_secs: float
    rule_promotion_errors: list[RulePromotionError]

    def version_gap(self) -> int: ...

class RulePromotionError:
    rule_text: str
    error: str
```

## ApplyResult

```python
class ApplyResult:
    facts_applied: int
    version_gap: int
```

## CompactionStats

```python
class CompactionStats:
    files_compacted: int
    bytes_before: int
    bytes_after: int
    duration_secs: float
    crdt_merges: int
```

## BulkStats

```python
class BulkStats:
    vertices_inserted: int
    edges_inserted: int
    indexes_rebuilt: int
    duration_secs: float
    index_build_duration_secs: float
    index_task_ids: list[str]
    indexes_pending: bool
```

## BulkProgress

```python
class BulkProgress:
    phase: str             # "inserting", "rebuilding_indexes", "finalizing"
    rows_processed: int
    total_rows: int | None
    current_label: str | None
    elapsed_secs: float
```

---

# 12. Query & Row Types

## QueryResult

Implements sequence protocol: `for row in result`, `result[0]`, `len(result)`.

```python
class QueryResult:
    metrics: QueryMetrics
    warnings: list[QueryWarning]
    columns: list[str]

    @property
    def rows(self) -> list[Row]: ...
    def __len__(self) -> int: ...
    def __getitem__(self, idx: int) -> Row: ...
    def __iter__(self) -> Iterator[Row]: ...
    def __bool__(self) -> bool: ...
```

## Row

Dict-like access to query result columns.

```python
class Row:
    @property
    def columns(self) -> list[str]: ...
    def get(self, column: str) -> Any: ...
    def to_dict(self) -> dict[str, Any]: ...
    def __getitem__(self, key: str | int) -> Any: ...
    def __contains__(self, key: str) -> bool: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[str]: ...
```

## QueryCursor / AsyncQueryCursor

Streaming cursor for large result sets.

```python
class QueryCursor:
    @property
    def columns(self) -> list[str]: ...
    def fetch_one(self) -> dict[str, Any] | None: ...
    def fetch_many(self, n: int) -> list[dict[str, Any]]: ...
    def fetch_all(self) -> list[dict[str, Any]]: ...
    def close(self) -> None: ...
    def __iter__(self) -> QueryCursor: ...
    def __next__(self) -> dict[str, Any]: ...
    def __enter__(self) -> QueryCursor: ...
    def __exit__(...) -> bool: ...

class AsyncQueryCursor:
    @property
    def columns(self) -> list[str]: ...
    async def fetch_one(self) -> dict[str, Any] | None: ...
    async def fetch_many(self, n: int) -> list[dict[str, Any]]: ...
    async def fetch_all(self) -> list[dict[str, Any]]: ...
    async def close(self) -> None: ...
    async def __aiter__(self) -> AsyncQueryCursor: ...
    async def __anext__(self) -> dict[str, Any]: ...
    async def __aenter__(self) -> AsyncQueryCursor: ...
    async def __aexit__(...) -> bool: ...
```

## QueryMetrics

```python
class QueryMetrics:
    parse_time_ms: float
    plan_time_ms: float
    exec_time_ms: float
    total_time_ms: float
    rows_returned: int
    rows_scanned: int
    bytes_read: int
    plan_cache_hit: bool
    l0_reads: int
    storage_reads: int
    cache_hits: int
```

## QueryWarning

```python
class QueryWarning:
    code: str
    message: str
```

## ExplainOutput

```python
class ExplainOutput:
    plan_text: str
    warnings: list[str]
    cost_estimates: Any
    index_usage: Any
    suggestions: Any
```

## ProfileOutput

```python
class ProfileOutput:
    total_time_ms: int
    peak_memory_bytes: int
    plan_text: str
    operators: Any
```

---

# 13. Graph Element Types

## Node

```python
class Node:
    @property
    def id(self) -> Vid: ...
    @property
    def element_id(self) -> Vid: ...
    @property
    def labels(self) -> list[str]: ...
    @property
    def properties(self) -> dict[str, Any]: ...

    # Dict-like access
    def get(self, key: str, default: Any = None) -> Any: ...
    def keys(self) -> list[str]: ...
    def values(self) -> list[Any]: ...
    def items(self) -> list[tuple[str, Any]]: ...
    def __getitem__(self, key: str) -> Any: ...
    def __contains__(self, key: str) -> bool: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[str]: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
```

## Edge

```python
class Edge:
    @property
    def id(self) -> Eid: ...
    @property
    def element_id(self) -> Eid: ...
    @property
    def type(self) -> str: ...
    @property
    def start_id(self) -> Vid: ...
    @property
    def end_id(self) -> Vid: ...
    @property
    def properties(self) -> dict[str, Any]: ...

    # Same dict-like access as Node
    def get(self, key: str, default: Any = None) -> Any: ...
    def keys(self) -> list[str]: ...
    def values(self) -> list[Any]: ...
    def items(self) -> list[tuple[str, Any]]: ...
    def __getitem__(self, key: str) -> Any: ...
    def __contains__(self, key: str) -> bool: ...
```

## Path

```python
class Path:
    @property
    def nodes(self) -> list[Node]: ...
    @property
    def edges(self) -> list[Edge]: ...
    @property
    def start(self) -> Node | None: ...
    @property
    def end(self) -> Node | None: ...

    def is_empty(self) -> bool: ...
    def __len__(self) -> int: ...         # number of hops
    def __getitem__(self, idx: int) -> Node | Edge: ...  # interleaved
    def __iter__(self) -> Iterator[Node | Edge]: ...
```

---

# 14. Locy Types

## LocyResult

```python
class LocyResult:
    derived: Any
    stats: Any
    command_results: Any
    warnings: Any
    approximate_groups: Any
    derived_fact_set: Any

    def has_warning(self, code: str) -> bool: ...
    def warnings_list(self) -> Any: ...
    def derived_facts(self, rule: str) -> list[dict[str, Any]] | None: ...
    def rows(self) -> list[dict[str, Any]] | None: ...
    def columns(self) -> list[str] | None: ...
    @property
    def iterations(self) -> int: ...
```

## LocyExplainOutput

```python
class LocyExplainOutput:
    plan_text: str
    strata_count: int
    rule_names: list[str]
    has_recursive_strata: bool
    warnings: list[str]
    command_count: int
```

## LocyStats

```python
class LocyStats:
    strata_evaluated: int
    total_iterations: int
    derived_nodes: int
    derived_edges: int
    evaluation_time_secs: float
    queries_executed: int
    mutations_executed: int
    peak_memory_bytes: int
```

## DerivedFactSet

Opaque wrapper obtained from `LocyResult.derived_fact_set`, passed to `tx.apply()`.

```python
class DerivedFactSet:
    @property
    def evaluated_at_version(self) -> int: ...
    @property
    def vertex_count(self) -> int: ...
    @property
    def edge_count(self) -> int: ...
    @property
    def fact_count(self) -> int: ...
    @property
    def vertices(self) -> dict[str, list[dict[str, Any]]]: ...
    @property
    def edges(self) -> list[dict[str, Any]]: ...
    def is_empty(self) -> bool: ...
```

## CompiledProgram

```python
class CompiledProgram:
    @property
    def num_strata(self) -> int: ...
    @property
    def num_rules(self) -> int: ...
    @property
    def rule_names(self) -> list[str]: ...
```

## CommandResult Classes

Returned in `LocyResult.command_results`:

```python
class QueryCommandResult:
    @property
    def command_type(self) -> str: ...
    @property
    def rows(self) -> list[dict[str, Any]]: ...

class AssumeCommandResult:
    @property
    def command_type(self) -> str: ...
    @property
    def rows(self) -> list[dict[str, Any]]: ...

class ExplainCommandResult:
    @property
    def command_type(self) -> str: ...
    @property
    def tree(self) -> Any: ...

class AbduceCommandResult:
    @property
    def command_type(self) -> str: ...
    @property
    def modifications(self) -> list[dict[str, Any]]: ...

class DeriveCommandResult:
    @property
    def command_type(self) -> str: ...
    affected: int

class CypherCommandResult:
    @property
    def command_type(self) -> str: ...
    @property
    def rows(self) -> list[dict[str, Any]]: ...
```

---

# 15. Schema Info Types

```python
class LabelInfo:
    name: str
    count: int
    properties: list[PropertyInfo]
    indexes: list[IndexInfo]
    constraints: list[ConstraintInfo]

class EdgeTypeInfo:
    name: str
    count: int
    source_labels: list[str]
    target_labels: list[str]
    properties: list[PropertyInfo]
    indexes: list[IndexInfo]
    constraints: list[ConstraintInfo]

class PropertyInfo:
    name: str
    data_type: str
    nullable: bool
    is_indexed: bool

class IndexInfo:
    name: str
    index_type: str
    properties: list[str]
    status: str

class ConstraintInfo:
    name: str
    constraint_type: str
    properties: list[str]
    enabled: bool

class RuleInfo:
    name: str
    clause_count: int
    is_recursive: bool

class SnapshotInfo:
    snapshot_id: str
    name: str | None
    created_at: str
    version_hwm: int

class IndexRebuildTaskInfo:
    id: str
    label: str
    status: str
    created_at: str
    started_at: str | None
    completed_at: str | None
    error: str | None
    retry_count: int

class IndexDefinitionInfo:
    name: str
    index_type: str
    label: str
    properties: list[str]
    state: str

class Schema:
    @property
    def version(self) -> int: ...
    @property
    def label_names(self) -> list[str]: ...
    @property
    def edge_type_names(self) -> list[str]: ...
    @property
    def label_count(self) -> int: ...
    @property
    def edge_type_count(self) -> int: ...
    def label_info(self, name: str) -> LabelInfo | None: ...
```

---

# 16. Configuration

## LocyConfig

```python
class LocyConfig:
    def __init__(
        self,
        max_iterations: int | None = None,
        timeout_secs: float | None = None,
        max_explain_depth: int | None = None,
        max_slg_depth: int | None = None,
        max_abduce_candidates: int | None = None,
        max_abduce_results: int | None = None,
        max_derived_bytes: int | None = None,
        deterministic_best_by: bool | None = None,
        strict_probability_domain: bool | None = None,
        probability_epsilon: float | None = None,
        exact_probability: bool | None = None,
        max_bdd_variables: int | None = None,
        top_k_proofs: int | None = None,
        top_k_proofs_training: int | None = None,
    ) -> None: ...

    # Properties (read-only)
    @property
    def max_iterations(self) -> int: ...
    @property
    def timeout_secs(self) -> float: ...
    @property
    def max_explain_depth(self) -> int: ...
    @property
    def max_slg_depth(self) -> int: ...
    @property
    def max_abduce_candidates(self) -> int: ...
    @property
    def max_abduce_results(self) -> int: ...
    @property
    def max_derived_bytes(self) -> int: ...
    @property
    def deterministic_best_by(self) -> bool: ...
    @property
    def strict_probability_domain(self) -> bool: ...
    @property
    def probability_epsilon(self) -> float: ...
    @property
    def exact_probability(self) -> bool: ...
    @property
    def max_bdd_variables(self) -> int: ...
    @property
    def top_k_proofs(self) -> int: ...
    @property
    def top_k_proofs_training(self) -> int | None: ...
```

## DataType

```python
class DataType:
    # Simple types (static factories)
    @staticmethod
    def STRING() -> DataType: ...
    @staticmethod
    def INT32() -> DataType: ...
    @staticmethod
    def INT64() -> DataType: ...
    @staticmethod
    def FLOAT32() -> DataType: ...
    @staticmethod
    def FLOAT64() -> DataType: ...
    @staticmethod
    def BOOL() -> DataType: ...
    @staticmethod
    def TIMESTAMP() -> DataType: ...
    @staticmethod
    def DATE() -> DataType: ...
    @staticmethod
    def TIME() -> DataType: ...
    @staticmethod
    def DATETIME() -> DataType: ...
    @staticmethod
    def DURATION() -> DataType: ...
    @staticmethod
    def JSON() -> DataType: ...

    # Parameterized types
    @staticmethod
    def vector(dimensions: int) -> DataType: ...
    @staticmethod
    def list(element_type: DataType) -> DataType: ...
    @staticmethod
    def map(key_type: DataType, value_type: DataType) -> DataType: ...
    @staticmethod
    def crdt(crdt_type: CrdtType) -> DataType: ...
```

## CrdtType

```python
class CrdtType:
    @staticmethod
    def G_COUNTER() -> CrdtType: ...
    @staticmethod
    def G_SET() -> CrdtType: ...
    @staticmethod
    def OR_SET() -> CrdtType: ...
    @staticmethod
    def LWW_REGISTER() -> CrdtType: ...
    @staticmethod
    def LWW_MAP() -> CrdtType: ...
    @staticmethod
    def RGA() -> CrdtType: ...
    @staticmethod
    def VECTOR_CLOCK() -> CrdtType: ...
    @staticmethod
    def VC_REGISTER() -> CrdtType: ...
```

## WriteLease

```python
class WriteLease:
    @staticmethod
    def LOCAL() -> WriteLease: ...
    @staticmethod
    def DYNAMODB(table: str) -> WriteLease: ...
```

---

# 17. Observability & Metrics

## DatabaseMetrics

```python
class DatabaseMetrics:
    l0_mutation_count: int
    l0_estimated_size_bytes: int
    schema_version: int
    uptime_secs: float
    active_sessions: int
    l1_run_count: int
    write_throttle_pressure: float
    compaction_in_progress: bool
    wal_size_bytes: int
    wal_lsn: int
    total_queries: int
    total_commits: int
```

## SessionMetrics

```python
class SessionMetrics:
    session_id: str
    active_since_secs: float
    queries_executed: int
    locy_evaluations: int
    total_query_time_secs: float
    transactions_committed: int
    transactions_rolled_back: int
    total_rows_returned: int
    total_rows_scanned: int
    plan_cache_hits: int
    plan_cache_misses: int
    plan_cache_size: int
```

## SessionCapabilities

```python
class SessionCapabilities:
    can_write: bool
    can_pin: bool
    isolation: str
    has_notifications: bool
    write_lease: str | None
```

---

# 18. Commit Notifications

## CommitNotification

```python
class CommitNotification:
    version: int
    mutation_count: int
    labels_affected: list[str]
    edge_types_affected: list[str]
    rules_promoted: int
    timestamp: str
    tx_id: str
    session_id: str
    causal_version: int
```

## CommitStream / AsyncCommitStream

```python
class CommitStream:
    def close(self) -> None: ...
    def __iter__(self) -> CommitStream: ...
    def __next__(self) -> CommitNotification | None: ...
    def __enter__(self) -> CommitStream: ...
    def __exit__(...) -> bool: ...

class AsyncCommitStream:
    async def close(self) -> None: ...
    def __aiter__(self) -> AsyncCommitStream: ...
    async def __anext__(self) -> CommitNotification: ...
```

## WatchBuilder

Created by `session.watch_with()`.

```python
class WatchBuilder:
    def labels(self, labels: list[str]) -> WatchBuilder: ...
    def edge_types(self, types: list[str]) -> WatchBuilder: ...
    def debounce(self, seconds: float) -> WatchBuilder: ...
    def exclude_session(self, session_id: str) -> WatchBuilder: ...
    def build(self) -> CommitStream: ...
    def build_async(self) -> AsyncCommitStream: ...
```

## CancellationToken

```python
class CancellationToken:
    def cancel(self) -> None: ...
    def is_cancelled(self) -> bool: ...
```

---

# 19. Prepared Statements

## PreparedQuery

```python
PreparedQuery = PyPreparedQuery

class PyPreparedQuery:
    def execute(self, params: dict[str, Any] | None = None) -> QueryResult: ...
    def query_text(self) -> str: ...
    def bind(self) -> PreparedQueryBinder: ...

class PreparedQueryBinder:
    def param(self, name: str, value: Any) -> PreparedQueryBinder: ...
    def execute(self) -> QueryResult: ...
```

## PreparedLocy

```python
class PreparedLocy:
    def execute(self, params: dict[str, Any] | None = None) -> LocyResult: ...
    def program_text(self) -> str: ...
    def bind(self) -> PreparedLocyBinder: ...

class PreparedLocyBinder:
    def param(self, name: str, value: Any) -> PreparedLocyBinder: ...
    def execute(self) -> LocyResult: ...
```

---

# 20. Session Templates

```python
class SessionTemplateBuilder:
    def param(self, key: str, value: Any) -> SessionTemplateBuilder: ...
    def rules(self, program: str) -> SessionTemplateBuilder: ...
    def hook(self, hook: Any) -> SessionTemplateBuilder: ...
    def query_timeout(self, seconds: float) -> SessionTemplateBuilder: ...
    def transaction_timeout(self, seconds: float) -> SessionTemplateBuilder: ...
    def build(self) -> SessionTemplate: ...

class SessionTemplate:
    def create(self) -> Session: ...
```

**Example:**

```python
template = db.session_template() \
    .param("tenant_id", 42) \
    .rules("similar_to(x, y) :- ...") \
    .query_timeout(30.0) \
    .build()

session = template.create()  # Pre-configured with params, rules, timeouts
```

---

# 21. Xervo (ML Runtime)

## Xervo (Synchronous)

Returned by `db.xervo()`.

```python
class Xervo:
    def is_available(self) -> bool: ...

    def embed(self, alias: str, texts: list[str]) -> list[list[float]]: ...

    def generate(
        self,
        alias: str,
        messages: list[Message | dict[str, Any]],
        max_tokens: int | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
    ) -> GenerationResult: ...

    def generate_text(
        self,
        alias: str,
        prompt: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
    ) -> GenerationResult: ...
```

## AsyncXervo

```python
class AsyncXervo:
    def is_available(self) -> bool: ...     # sync
    async def embed(self, alias: str, texts: list[str]) -> list[list[float]]: ...
    async def generate(...) -> GenerationResult: ...
    async def generate_text(...) -> GenerationResult: ...
```

## Message

```python
class Message:
    role: str
    content: str

    def __init__(self, role: str, content: str) -> None: ...
    @staticmethod
    def user(text: str) -> Message: ...
    @staticmethod
    def assistant(text: str) -> Message: ...
    @staticmethod
    def system(text: str) -> Message: ...
```

## GenerationResult / TokenUsage

```python
class GenerationResult:
    text: str
    usage: TokenUsage | None

class TokenUsage:
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
```

---

# 22. ID & Value Types

## Vid / Eid

```python
class Vid:
    def __init__(self, id: int) -> None: ...
    def as_int(self) -> int: ...
    def __int__(self) -> int: ...
    def __index__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...

class Eid:
    def __init__(self, id: int) -> None: ...
    def as_int(self) -> int: ...
    def __int__(self) -> int: ...
    def __index__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
```

## UniId

```python
class UniId:
    def __init__(self, multibase: str) -> None: ...
    def to_multibase(self) -> str: ...
    def as_bytes(self) -> bytes: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    def __str__(self) -> str: ...
```

## Value

Opt-in wrapper for explicit type discrimination. Query results return native Python types by default.

```python
class Value:
    @staticmethod
    def null() -> Value: ...
    @staticmethod
    def bool(v: bool) -> Value: ...
    @staticmethod
    def int(v: int) -> Value: ...
    @staticmethod
    def float(v: float) -> Value: ...
    @staticmethod
    def string(v: str) -> Value: ...
    @staticmethod
    def bytes(v: bytes) -> Value: ...
    @staticmethod
    def vector(v: list[float]) -> Value: ...

    @property
    def type_name(self) -> str: ...
    def is_null(self) -> bool: ...
    def is_bool(self) -> bool: ...
    def is_int(self) -> bool: ...
    def is_float(self) -> bool: ...
    def is_string(self) -> bool: ...
    def to_python(self) -> Any: ...
```

---

# 23. Exception Hierarchy

All exceptions inherit from `UniError`.

```python
# Base
class UniError(Exception): ...

# Database lifecycle
class UniNotFoundError(UniError): ...
class UniDatabaseLockedError(UniError): ...

# Schema
class UniSchemaError(UniError): ...
class UniLabelNotFoundError(UniError): ...
class UniEdgeTypeNotFoundError(UniError): ...
class UniPropertyNotFoundError(UniError): ...
class UniIndexNotFoundError(UniError): ...
class UniLabelAlreadyExistsError(UniError): ...
class UniEdgeTypeAlreadyExistsError(UniError): ...
class UniConstraintError(UniError): ...
class UniInvalidIdentifierError(UniError): ...

# Query & parse
class UniParseError(UniError): ...
class UniQueryError(UniError): ...
class UniTypeError(UniError): ...

# Transaction
class UniTransactionError(UniError): ...
class UniTransactionConflictError(UniError): ...
class UniTransactionAlreadyCompletedError(UniError): ...
class UniTransactionExpiredError(UniError): ...
class UniCommitTimeoutError(UniError): ...

# Resource limits
class UniMemoryLimitExceededError(UniError): ...
class UniTimeoutError(UniError): ...

# Access control
class UniReadOnlyError(UniError): ...
class UniPermissionDeniedError(UniError): ...

# Concurrency
class UniWriteContextAlreadyActiveError(UniError): ...
class UniCancelledError(UniError): ...

# Storage & I/O
class UniStorageError(UniError): ...
class UniIOError(UniError): ...
class UniInternalError(UniError): ...

# Snapshot
class UniSnapshotNotFoundError(UniError): ...

# Arguments
class UniInvalidArgumentError(UniError): ...

# Locy-specific
class UniStaleDerivedFactsError(UniError): ...
class UniRuleConflictError(UniError): ...
class UniHookRejectedError(UniError): ...
class UniLocyCompileError(UniError): ...
class UniLocyRuntimeError(UniError): ...
```

## Hook Context Types

```python
class HookContext:
    session_id: str
    query_text: str
    query_type: str

class CommitHookContext:
    session_id: str
    tx_id: str
    mutation_count: int

class QueryType:
    @staticmethod
    def CYPHER() -> str: ...
    @staticmethod
    def LOCY() -> str: ...
    @staticmethod
    def EXECUTE() -> str: ...
```
