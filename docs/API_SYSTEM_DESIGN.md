# Uni API & System Design Document

**Version 2.0 — March 2026**

This document is the single source of truth for the Uni public API across Rust and Python.
It unifies three workstreams: Rust API revision, Python API parity, and Locy Session isolation.

---

# Table of Contents

1. [Goals & Principles](#1-goals--principles)
2. [Current State & Problems](#2-current-state--problems)
3. [Target Rust API](#3-target-rust-api)
4. [Target Python API](#4-target-python-api)
5. [Cross-Language Alignment Rules](#5-cross-language-alignment-rules)
6. [Locy Session: Multi-Client Isolation](#6-locy-session-multi-client-isolation)
7. [Type Encapsulation & Result Types](#7-type-encapsulation--result-types)
8. [Re-export & Module Hygiene](#8-re-export--module-hygiene)
9. [Implementation Phases](#9-implementation-phases)
10. [Files to Modify](#10-files-to-modify)
11. [Test Plan](#11-test-plan)

---

# 1. Goals & Principles

## 1.1 Goals

- **Ergonomic defaults, powerful options**: The simplest path (open, query, close) should require minimal ceremony. Advanced configuration (timeouts, parallelism, hybrid storage) is opt-in via builders.
- **One canonical path per type**: Every public type has exactly one import path. No duplicate re-exports.
- **Rust ↔ Python parity**: Every Rust API method has a Python counterpart. Method names, parameter semantics, and return shapes match across languages.
- **Encapsulated internals**: Public types expose accessor methods, not raw fields. Internal representation can change without breaking consumers.
- **Isolated Locy evaluation**: Multiple clients can evaluate Locy programs concurrently with write isolation, rule isolation, and OCC conflict detection at commit.

## 1.2 Design Principles

| Principle | Implication |
|-----------|-------------|
| Facade pattern | `uni-db` is the only crate consumers depend on. Internal crates are never exposed. |
| Async-first, sync-available | Primary API is async (`Uni`). Blocking wrapper (`UniSync`) mirrors the full async surface. |
| Builder for configuration | Every subsystem accessor (`query_with`, `evaluate_with`, `bulk_writer`, `schema`) returns a fluent builder. Each builder has exactly one domain-specific terminal method. |
| Domain-specific terminals | Four terminal verbs, each tied to one domain. A user never chooses between them — each builder offers only the terminal that matches its semantics. See §1.3. |
| Subsystem accessors | `db.locy()`, `db.xervo()`, `db.schema()` return lightweight borrowed handles. The database owns all state. |

## 1.3 Terminal Methods

The API uses four terminal verbs. Each is domain-specific — a user never chooses between them because each builder offers only the one that matches its semantics.

| Terminal | Domain | Semantics | Available On |
|----------|--------|-----------|-------------|
| `fetch_all()` | Query (read) | "Fetch the result rows" | `QueryBuilder`, `TransactionQueryBuilder`, `SessionQueryBuilder` |
| `execute()` | Query (write) | "Execute this mutation" | `QueryBuilder`, `TransactionQueryBuilder`, `SessionQueryBuilder` |
| `run()` | Locy | "Run this logic program evaluation" | `LocyBuilder`, `LocySessionBuilder` |
| `apply()` | Schema | "Apply this declarative schema definition" | `SchemaBuilder` (via `LabelBuilder`, `EdgeTypeBuilder`) |
| `commit()` | Lifecycle | "Finalize this transactional unit of work" | `Transaction`, `LocySession`, `BulkWriter` |

**Why not a single `execute()` everywhere?**

- `apply()` is declarative ("make the schema look like this"), not imperative. Calling it `execute()` would imply a one-shot command when it's actually an idempotent reconciliation.
- `run()` evaluates a logic program — it's not a mutation, and it returns `LocyResult` (derived facts, stats, warnings), not an affected-row count. Calling it `execute()` would create a return-type surprise.
- `commit()` is a lifecycle operation on a stateful object (transaction, session), not a one-shot builder terminal. Transactions already have `execute()` for running queries within them — reusing the name for finalization would be ambiguous.

The tradeoff is 4 names to learn instead of 1, but each name is self-documenting and unambiguous in context. A user on a `SchemaBuilder` sees only `apply()`; a user on a `LocyBuilder` sees only `run()`. There is no decision point.

**`QueryBuilder` is the only builder with two terminals** (`fetch_all()` and `execute()`), because a Cypher query can be either a read or a write, and the return types differ (`QueryResult` vs `ExecuteResult`). This is the necessary split — the builder doesn't know at construction time whether the query is a read or write.

---

# 2. Current State & Problems

## 2.1 Rust API Issues

| ID | Issue | Severity |
|----|-------|----------|
| R1 | `Uni::open("path")` returns `UniBuilder`, forcing `.build().await?` even for the simple case | High |
| R2 | `uni_locy::Row` (`HashMap<String, Value>`) collides with `uni_query::Row` (struct) | High |
| R3 | `Row.columns`, `Row.values`, `QueryResult.rows`, `ExecuteResult.affected_rows` are public fields | High |
| R4 | Raw crate re-exports (`pub use uni_algo as algo_crate`) leak internals; types reachable via 4+ paths | Medium |
| R5 | `ExecuteResult` only has `affected_rows: usize` — no per-operation counters | Medium |
| R6 | `SessionQueryBuilder::execute()` returns `QueryResult` (reads); everywhere else `execute()` means mutation | Medium |
| R7 | `execute_with()` documented as "alias for `query_with()`" — confusing identity | Low |
| R8 | `UniSync` missing `locy()`, `bulk_writer()`, `explain()`, `profile()`, `session()`; `QueryBuilderSync` missing `timeout()`, `max_memory()`, `execute()` | Medium |
| R9 | `LabelBuilder::done()` is dead API surface (`.label()` and `.apply()` are available directly) | Low |
| R10 | `param()` doc says "don't include $" but doesn't enforce or strip it | Low |
| R11 | No query plan caching — every call re-parses and re-plans | Medium |
| R12 | No `LocySession` — Locy evaluation has no client isolation, ASSUME serializes on shared Writer | High |

## 2.2 Python API Issues

| ID | Issue | Severity |
|----|-------|----------|
| P1 | `AsyncLocyBuilder.run()` vs Rust `LocyBuilder.run()` — name matches but `AsyncQueryBuilder` has both `run()` and `fetch_all()` creating confusion | Low |
| P2 | Python `QueryBuilder.params()` returns `None` instead of `self` — broken fluent chain | Critical |
| P3 | Python `QueryBuilder` missing `.execute()` terminal — no builder path for mutations | High |
| P4 | Python missing `db.locy().explain()` | Medium |
| P5 | Sync `Database` has static constructors (`Database.open()`) but they were recently added — verify parity with async | Low |
| P6 | `locy_compile` / `locy_clear` sync on `AsyncDatabase` — undocumented | Low |
| P7 | Python `Session.execute_with()` not available — only `query_with()` | Medium |
| P8 | No `LocySession` Python bindings | High |

## 2.3 Cross-Language Misalignment

| Concept | Rust | Python Sync | Python Async | Problem |
|---------|------|-------------|--------------|---------|
| Read query terminal | `fetch_all()` | `fetch_all()` | `fetch_all()` | OK |
| Mutation terminal | `execute()` | — | — | **Missing on Python builders** |
| Locy evaluate terminal | `run()` | `run()` | `run()` | OK |
| Locy register | `register()` | `register()` | `register()` | OK |
| Locy entry | `db.locy()` → `LocyEngine` | `db.locy()` → `LocyEngine` | `db.locy()` → `AsyncLocyEngine` | OK |
| Locy session | — | — | — | **Missing everywhere** |
| Streaming cursor | `query_cursor()` | `query_cursor()` / `cursor()` | `query_cursor()` / `cursor()` | OK |

---

# 3. Target Rust API

## 3.1 Database Lifecycle

```rust
// Simple open (new — via IntoFuture)
let db = Uni::open("./mydb").await?;
let db = Uni::temporary().await?;
let db = Uni::in_memory().await?;

// Advanced configuration (existing — unchanged)
let db = Uni::open("./mydb")
    .schema_file("schema.yaml")
    .cache_size(512 * 1024 * 1024)
    .parallelism(8)
    .build().await?;

// Blocking
let db = Uni::open("./mydb").build_sync()?;

// Shutdown
db.shutdown().await;
```

**Change**: Implement `IntoFuture` for `UniBuilder`. Existing `.build().await?` continues to work.

## 3.2 Query Execution

```rust
// Direct
let result: QueryResult = db.query("MATCH (n:Person) RETURN n").await?;
let affected: ExecuteResult = db.execute("CREATE (:Person {name: 'Alice'})").await?;

// Builder (parameterized)
let result = db.query_with("MATCH (n) WHERE n.age > $min RETURN n")
    .param("min", 25)
    .timeout(Duration::from_secs(30))
    .fetch_all().await?;

let affected = db.query_with("CREATE (:Person {name: $name})")
    .param("name", "Bob")
    .execute().await?;

// Streaming
let cursor = db.query_with("MATCH (n) RETURN n")
    .query_cursor().await?;

// Explain / Profile
let plan = db.explain("MATCH (n)-[r]->(m) RETURN n").await?;
let (rows, profile) = db.profile("MATCH (n)-[r]->(m) RETURN n").await?;
```

**Changes**:
- `execute_with()` removed — `query_with()` already supports both `fetch_all()` and `execute()` terminals. Deprecate with `#[deprecated]` shim.
- `param()` strips leading `$` if present.

## 3.3 Accessing Results

```rust
// QueryResult — all access via methods
let columns: &[String] = result.columns();
let rows: &[Row] = result.rows();
let warnings: &[QueryWarning] = result.warnings();

for row in &result {
    let name: String = row.get("name")?;
    let age: i64 = row.get_idx(1)?;
    let raw: &Value = &row.values()[0];
}

// ExecuteResult — detailed counters
let affected = db.execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})").await?;
assert_eq!(affected.affected_rows(), 2);
assert_eq!(affected.nodes_created(), 2);
assert_eq!(affected.relationships_created(), 1);
```

**Changes**:
- `Row`, `QueryResult`, `ExecuteResult` fields become `pub(crate)`. Access only via methods.
- `ExecuteResult` gains per-operation counters.

## 3.4 Transactions

```rust
// Manual
let tx = db.begin().await?;
tx.query("MATCH (n) RETURN n").await?;
tx.execute("CREATE (:Foo)").await?;
tx.query_with("MATCH (n) WHERE n.x > $v RETURN n")
    .param("v", 10)
    .fetch_all().await?;
tx.commit().await?;
// Drop without commit/rollback → auto-rollback + warning

// Closure (existing — unchanged)
db.transaction(|tx| Box::pin(async move {
    tx.execute("CREATE (:Foo)").await?;
    Ok(())
})).await?;
```

**Changes**:
- Add `TransactionQueryBuilder::fetch_all()` — currently missing, only has `execute()`.
- Transaction already auto-rolls back on Drop (verified in code).

## 3.5 Sessions (Query-Scoped Variables)

```rust
let session = db.session()
    .set("tenant_id", 42)
    .build();

let result = session.query("MATCH (n) WHERE n.tenant = $tenant_id RETURN n").await?;
session.query_with("MATCH (n) WHERE n.x > $v RETURN n")
    .param("v", 10)
    .fetch_all().await?;    // was: execute() — renamed
session.execute("CREATE (:Foo)").await?;
```

**Changes**:
- `SessionQueryBuilder::execute()` renamed to `fetch_all()` (returns `QueryResult`).
- `SessionQueryBuilder::execute_mutation()` renamed to `execute()` (returns `ExecuteResult`).
- Deprecated shims for old names.

## 3.6 Schema

```rust
db.schema()
    .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .property_nullable("email", DataType::String)
        .vector("embedding", 384)
        .index("name", IndexType::Scalar(ScalarType::BTree))
    .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Date)
    .apply().await?;
```

**Changes**:
- `done()` deprecated on `LabelBuilder` / `EdgeTypeBuilder`.
- (Future) `PropertyOptions` struct for extensible property configuration.

## 3.7 Bulk Loading

```rust
let mut writer = db.bulk_writer()
    .batch_size(10_000)
    .defer_vector_indexes(true)
    .async_indexes(true)
    .build().await?;

let vids = writer.insert_vertices("Person", props).await?;
writer.insert_edges("KNOWS", edges).await?;
let stats = writer.commit().await?;
```

No changes — this is the strongest part of the API.

## 3.8 Locy Engine (Stateless)

```rust
let locy = db.locy();

// Simple evaluation
let result = locy.evaluate("?- ancestor(X, 'Alice').").await?;

// Builder
let result = locy.evaluate_with("?- path(X, $target).")
    .param("target", "Bob")
    .timeout(Duration::from_secs(10))
    .max_iterations(1000)
    .run().await?;

// Register rules for reuse
locy.register("ancestor(X, Y) :- parent(X, Y).")?;
locy.evaluate("?- ancestor(X, 'Alice').").await?;
locy.clear_registry();

// Compile without executing
let compiled = locy.compile_only("ancestor(X, Y) :- parent(X, Y).")?;

// Explain
let result = locy.explain("EXPLAIN ancestor.").await?;
```

**Changes**:
- Rename Locy's internal `Row` type alias to `FactRow` to avoid collision with `uni_query::Row`.

## 3.9 Locy Session (Isolated — NEW)

```rust
let db: Arc<Uni> = Arc::new(Uni::open("./mydb").await?);

// Create isolated session
let mut session = db.locy_session().await?;

// Register rules (session-private)
session.locy().register("path(X,Y) :- edge(X,Y). path(X,Z) :- edge(X,Y), path(Y,Z).")?;

// Evaluate (reads shared + session-private data)
let result = session.locy().evaluate("?- path('a', X).").await?;

// DERIVE (writes to session-private L0)
session.locy().evaluate("DERIVE reachable(X, Y) :- path(X, Y).").await?;

// ASSUME (savepoints on session Writer — no shared lock contention)
session.locy().evaluate(r#"
    ASSUME { CREATE (:City {name: "Atlantis"})-[:ROAD]->(:City {name: "Athens"}) }
    ?- path("Atlantis", X).
"#).await?;

// Check state
assert!(session.is_dirty());

// Commit (OCC conflict detection, WAL write, L0 merge, rule promotion)
let commit_result = session.commit().await?;
println!("Mutations: {}, Rules: {}", commit_result.mutations_committed, commit_result.rules_promoted);

// Or rollback
// session.rollback();

// Drop without commit → auto-rollback + warning if dirty
```

**Parallel sessions:**

```rust
let (r1, r2) = tokio::join!(
    {
        let db = Arc::clone(&db);
        async move {
            let mut s = db.locy_session().await?;
            s.locy().evaluate("DERIVE hot(X) :- temp(X, T), T > 100.").await?;
            s.commit().await
        }
    },
    {
        let db = Arc::clone(&db);
        async move {
            let mut s = db.locy_session().await?;
            s.locy().evaluate("DERIVE cold(X) :- temp(X, T), T < 0.").await?;
            s.commit().await
        }
    },
);
r1?; r2?; // Both succeed — disjoint write sets
```

## 3.10 Xervo (ML Runtime)

```rust
let xervo = db.xervo();
let embeddings = xervo.embed("model-alias", &["hello", "world"]).await?;
let result = xervo.generate("gpt-4", messages, options).await?;
```

No changes.

## 3.11 Blocking API (`UniSync`)

```rust
let db = UniSync::in_memory()?;

// Full surface parity with async
db.query("MATCH (n) RETURN n")?;
db.execute("CREATE (:Foo)")?;
db.query_with("...").param("x", 1).timeout(Duration::from_secs(5)).fetch_all()?;
db.query_with("...").param("x", 1).execute()?;
db.explain("MATCH (n) RETURN n")?;
db.profile("MATCH (n) RETURN n")?;

let tx = db.begin()?;
tx.query("...")?;
tx.commit()?;

let session = db.session().set("k", "v").build();
session.query("...")?;

db.locy().evaluate("?- foo(X).")?;
db.locy().register("bar(X) :- foo(X).")?;

let mut bw = db.bulk_writer().batch_size(1000).build()?;
bw.insert_vertices("L", props)?;
bw.commit()?;

db.shutdown()?;
```

**Changes**: Fill all gaps — `locy()`, `bulk_writer()`, `explain()`, `profile()`, `session()`, plus `QueryBuilderSync::timeout()`, `max_memory()`, `execute()`.

---

# 4. Target Python API

## 4.1 Naming Convention

Python method names match Rust exactly, with these rules:

| Rust | Python | Notes |
|------|--------|-------|
| `fetch_all()` | `fetch_all()` | Read query terminal |
| `execute()` | `execute()` | Mutation terminal (returns int) |
| `run()` | `run()` | Locy evaluation terminal |
| `apply()` | `apply()` | Schema DDL terminal |
| `commit()` | `commit()` | Transaction/session/bulk terminal |
| `query_with()` | `query_with()` | Returns builder |
| `evaluate_with()` | `evaluate_with()` | Returns LocyBuilder |
| `locy()` | `locy()` | Returns LocyEngine handle |
| `locy_session()` | `locy_session()` | Returns LocySession |

## 4.2 Sync Python API

```python
import uni_db as uni

# Open
db = uni.Database.open("./mydb")
db = uni.Database.temporary()
db = uni.Database.in_memory()

# Builder
db = (uni.Database.builder()
      .open("./mydb")
      .schema_file("schema.yaml")
      .cache_size(512 * 1024 * 1024)
      .build())

# Query
rows = db.query("MATCH (n:Person) RETURN n")
rows = db.query("MATCH (n) WHERE n.age > $min RETURN n", params={"min": 25})

# Query builder
rows = (db.query_with("MATCH (n) WHERE n.age > $min RETURN n")
          .param("min", 25)
          .timeout(30.0)
          .fetch_all())

# Mutation
affected = db.execute("CREATE (:Person {name: 'Alice'})")

# Mutation builder (NEW — currently missing)
affected = (db.query_with("CREATE (:Person {name: $name})")
              .param("name", "Bob")
              .execute())

# Cursor
with db.query_cursor("MATCH (n) RETURN n") as cursor:
    for row in cursor:
        print(row)

# Explain / Profile
plan = db.explain("MATCH (n) RETURN n")
rows, profile = db.profile("MATCH (n) RETURN n")

# Transaction
with db.begin() as tx:
    tx.query("MATCH (n) RETURN n")
    tx.execute("CREATE (:Foo)")
    rows = tx.query_with("...").param("v", 10).fetch_all()
    affected = tx.query_with("...").param("v", 10).execute()
    tx.commit()
# Auto-rollback if exception or no commit

# Session
session = db.session().set("tenant_id", 42).build()
rows = session.query("MATCH (n) WHERE n.tenant = $tenant_id RETURN n")

# Schema
(db.schema()
   .label("Person")
       .property("name", "String")
       .property("age", "Int64")
       .vector("embedding", 384)
       .index("name", "btree")
   .edge_type("KNOWS", ["Person"], ["Person"])
       .property("since", "Date")
   .apply())

# Bulk loading
with db.bulk_writer().batch_size(10_000).build() as writer:
    vids = writer.insert_vertices("Person", vertices)
    writer.insert_edges("KNOWS", edges)
    stats = writer.commit()

# Locy (stateless)
locy = db.locy()
result = locy.evaluate("?- ancestor(X, 'Alice').")
result = (locy.evaluate_with("?- path(X, $target).")
              .param("target", "Bob")
              .timeout(10.0)
              .max_iterations(1000)
              .run())
locy.register("ancestor(X, Y) :- parent(X, Y).")
locy.clear_registry()
compiled = locy.compile_only("ancestor(X, Y) :- parent(X, Y).")
result = locy.explain("EXPLAIN ancestor.")

# Locy session (isolated — NEW)
with db.locy_session() as session:
    session.locy().register("path(X,Y) :- edge(X,Y).")
    result = session.locy().evaluate("?- path('a', X).")
    session.locy().evaluate("DERIVE reachable(X,Y) :- path(X,Y).")
    session.commit()
# Auto-rollback on exception

# Xervo
embeddings = db.xervo().embed("model", ["hello", "world"])
```

## 4.3 Async Python API

```python
import uni_db as uni

# Open
db = await uni.AsyncDatabase.open("./mydb")
db = await uni.AsyncDatabase.temporary()

# Builder
db = await (uni.AsyncDatabase.builder()
               .open("./mydb")
               .schema_file("schema.yaml")
               .build())

# Query
rows = await db.query("MATCH (n) RETURN n")
rows = await (db.query_with("MATCH (n) WHERE n.age > $min RETURN n")
                .param("min", 25)
                .timeout(30.0)
                .fetch_all())

# Mutation builder (NEW)
affected = await (db.query_with("CREATE (:Person {name: $name})")
                    .param("name", "Bob")
                    .execute())

# Cursor
async with db.query_cursor("MATCH (n) RETURN n") as cursor:
    async for row in cursor:
        print(row)

# Transaction
async with db.begin() as tx:
    await tx.query("MATCH (n) RETURN n")
    await tx.execute("CREATE (:Foo)")
    rows = await tx.query_with("...").param("v", 10).fetch_all()
    affected = await tx.query_with("...").param("v", 10).execute()
    await tx.commit()

# Locy session (isolated — NEW)
async with db.locy_session() as session:
    session.locy().register("path(X,Y) :- edge(X,Y).")
    result = await session.locy().evaluate("?- path('a', X).")
    await session.locy().evaluate("DERIVE reachable(X,Y) :- path(X,Y).")
    await session.commit()
```

## 4.4 Python API Fixes (from API_REVIEW.md)

| Fix | Description | Priority |
|-----|-------------|----------|
| `params()` return self | `QueryBuilder.params()` and `AsyncQueryBuilder.params()` must return `self` for chaining | P0 |
| Add `execute()` terminal | Both `QueryBuilder` and `AsyncQueryBuilder` gain `.execute() -> int` | P1 |
| Add `TransactionQueryBuilder.fetch_all()` | Currently only has `.execute()` — add `.fetch_all()` for reads within transactions | P1 |
| Document sync methods on async classes | `register()`, `clear_registry()`, `compile_only()` are sync (CPU-only, no I/O) — add docstrings | P2 |

---

# 5. Cross-Language Alignment Rules

## 5.1 Terminal Method Matrix

Every builder in both Rust and Python must support these terminals:

| Builder | `fetch_all()` | `execute()` | `query_cursor()` / `cursor()` |
|---------|:---:|:---:|:---:|
| `QueryBuilder` | Yes | Yes | Yes |
| `TransactionQueryBuilder` | **Add** | Yes | No |
| `SessionQueryBuilder` | **Rename** (was `execute`) | **Rename** (was `execute_mutation`) | No |
| `LocyBuilder` | N/A | N/A | N/A (uses `run()`) |

## 5.2 Subsystem Accessor Matrix

Every subsystem accessor must exist on all database types:

| Accessor | `Uni` (async) | `UniSync` | Python `Database` | Python `AsyncDatabase` |
|----------|:---:|:---:|:---:|:---:|
| `query()` | Yes | Yes | Yes | Yes |
| `execute()` | Yes | Yes | Yes | Yes |
| `query_with()` | Yes | **Add** (complete) | Yes | Yes |
| `explain()` | Yes | **Add** | Yes | Yes |
| `profile()` | Yes | **Add** | Yes | Yes |
| `begin()` | Yes | Yes | Yes | Yes |
| `session()` | Yes | **Add** | Yes | Yes |
| `schema()` | Yes | Yes | Yes | Yes |
| `locy()` | Yes | **Add** | Yes | Yes |
| `locy_session()` | **Add** | **Add** | **Add** | **Add** |
| `bulk_writer()` | Yes | **Add** | Yes | Yes |
| `xervo()` | Yes | N/A | Yes | Yes |

## 5.3 Builder Method Matrix

Every builder must have these methods in both languages:

| Method | `QueryBuilder` | `LocyBuilder` | `BulkWriterBuilder` | `SchemaBuilder` |
|--------|:---:|:---:|:---:|:---:|
| `param()` | Yes | Yes | N/A | N/A |
| `params()` | Yes (**fix Python**) | Yes | N/A | N/A |
| `timeout()` | Yes | Yes | N/A | N/A |
| `max_memory()` | Yes | N/A | N/A | N/A |
| `max_iterations()` | N/A | Yes | N/A | N/A |

## 5.4 Context Manager Matrix

| Type | Python Sync | Python Async |
|------|:---:|:---:|
| `Database` | `__enter__`/`__exit__` | `__aenter__`/`__aexit__` |
| `Transaction` | `__enter__`/`__exit__` | `__aenter__`/`__aexit__` |
| `QueryCursor` | `__enter__`/`__exit__` | `__aenter__`/`__aexit__` |
| `BulkWriter` | `__enter__`/`__exit__` | `__aenter__`/`__aexit__` |
| `LocySession` | **Add** `__enter__`/`__exit__` | **Add** `__aenter__`/`__aexit__` |

---

# 6. Locy Session: Multi-Client Isolation

## 6.1 Motivation

Today, `LocyEngine` borrows `&Uni` and operates directly on the shared Writer, L0 buffer, and rule registry. Three problems:

1. **ASSUME serializes on the shared Writer.** `begin_savepoint()` holds `Arc<RwLock<Writer>>` for the entire ASSUME block. Concurrent clients block each other.
2. **DERIVE mutations are globally visible immediately.** Another client may observe partially-materialized facts mid-fixpoint.
3. **Rule registration leaks across clients.** The shared `locy_rule_registry` has no per-client namespace.

## 6.2 Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     Uni (shared)                         │
│  StorageManager    SchemaManager    PropertyManager      │
│  IdAllocator (Arc) AdjacencyManager Writer (shared)      │
│  locy_rule_registry (Arc<RwLock<LocyRuleRegistry>>)      │
└──────────┬──────────────────┬────────────────────────────┘
           │                  │
   ┌───────▼────────┐  ┌─────▼──────────┐
   │  LocySession A │  │  LocySession B │
   │                │  │                │
   │  session_writer│  │  session_writer│  ← private Writer
   │   ├ L0Manager  │  │   ├ L0Manager  │  ← private L0 buffer
   │   ├ AdjMgr     │  │   ├ AdjMgr     │  ← private adjacency
   │   └ IdAlloc ───┼──┼───┘ (shared)   │  ← shared Arc<IdAllocator>
   │                │  │                │
   │  rule_registry │  │  rule_registry │  ← cloned at session start
   └────────────────┘  └────────────────┘
```

Each session owns a **session-scoped Writer** with its own `L0Manager` and `AdjacencyManager`, but shares the `IdAllocator`, `StorageManager`, `SchemaManager`, and `PropertyManager` via Arc clones.

## 6.3 Core Types

### LocySession

```rust
/// An isolated Locy evaluation context.
///
/// Mutations (DERIVE, Cypher within Locy) write to a session-private L0 buffer.
/// Reads see shared database state (read-committed) plus session mutations.
/// On commit, session mutations merge into the shared database with OCC conflict detection.
/// On rollback (or Drop), session state is discarded.
pub struct LocySession {
    db: Arc<Uni>,
    session_writer: Arc<RwLock<Writer>>,
    rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
    id: String,
    completed: bool,
}

impl LocySession {
    /// Access the session-scoped Locy engine.
    pub fn locy(&self) -> LocySessionEngine<'_>;

    /// Merge session mutations and rules into the shared database.
    /// Acquires shared Writer lock, performs OCC conflict detection,
    /// writes to WAL, merges L0, replays adjacency, promotes rules.
    pub async fn commit(&mut self) -> Result<CommitResult>;

    /// Discard all session state. No I/O, no lock acquisition.
    pub fn rollback(&mut self);

    /// True if session has uncommitted graph mutations or rule changes.
    pub fn is_dirty(&self) -> bool;

    /// Set the conflict policy for rule promotion on commit.
    pub fn set_rule_conflict_policy(&mut self, policy: RuleConflictPolicy);

    /// Session identifier (UUID) for logging.
    pub fn id(&self) -> &str;
}

impl Drop for LocySession {
    fn drop(&mut self) {
        if !self.completed {
            // Auto-rollback; warn if dirty
        }
    }
}
```

### LocySessionEngine

```rust
/// Locy evaluation engine scoped to a session.
/// Analogous to `LocyEngine` but reads/writes against session-private state.
pub struct LocySessionEngine<'a> {
    session: &'a LocySession,
}

impl<'a> LocySessionEngine<'a> {
    pub fn register(&self, program: &str) -> Result<()>;
    pub fn clear_registry(&self);
    pub fn compile_only(&self, program: &str) -> Result<CompiledProgram>;
    pub async fn evaluate(&self, program: &str) -> Result<LocyResult>;
    pub fn evaluate_with(&self, program: &str) -> LocySessionBuilder<'a>;
    pub async fn evaluate_with_config(&self, program: &str, config: &LocyConfig) -> Result<LocyResult>;
    pub async fn explain(&self, program: &str) -> Result<LocyResult>;
}
```

### Supporting Types

```rust
pub struct CommitResult {
    pub mutations_committed: usize,
    pub rules_promoted: usize,
}

pub enum RuleConflictPolicy {
    Error,     // Default — fail on name collision
    Replace,   // Overwrite shared rules
    Skip,      // Keep shared rules, skip session duplicates
}
```

### New Error Variants

```rust
// Added to UniError:
SessionConflict { entity: String },
SessionAlreadyCompleted,
RuleConflict { rule_name: String },
```

## 6.4 Isolation Model

**Read isolation: Read Committed.**
Each `evaluate()` within a session sees the latest shared state plus session-private mutations. If another session commits between two evaluations, the second sees the committed data.

**Write isolation: Session-Private L0.**
All mutations write to the session Writer's L0 buffer. Invisible to other sessions until commit.

**Rule isolation: Cloned Registry.**
Rules registered in a session are visible only within that session until commit.

## 6.5 Commit Protocol

```
commit()
  ├─ 1. Acquire shared Writer lock (write)
  ├─ 2. Write-set conflict detection (entity-level intersection)
  │     • For each VID/EID in session L0: check if shared L0 was modified
  │     • Also check pending_flush_l0s
  │     • New IDs (from shared IdAllocator) never conflict
  ├─ 3. Write session L0 mutations to WAL
  ├─ 4. Flush WAL — THIS IS THE COMMIT POINT
  ├─ 5. Merge session L0 → shared main L0
  ├─ 6. Replay edges → shared AdjacencyManager
  ├─ 7. Promote rules to shared registry (with conflict policy)
  ├─ 8. Release shared Writer lock
  └─ 9. Clear session state, set completed = true
```

**Durability**: WAL flush (step 4) is the commit point. Crash before flush → session lost. Crash after flush → WAL replay recovers.

**Why pure inserts rarely conflict**: The shared `IdAllocator` guarantees globally unique IDs via batch reservation. DERIVE creates new entities with fresh IDs that cannot exist in any other L0. Conflicts only arise when sessions update/delete pre-existing entities.

## 6.6 Session-Scoped Writer Construction

```rust
// In Writer:
pub fn new_session_writer(shared: &Writer) -> Result<Writer> {
    let session_l0 = Arc::new(RwLock::new(L0Buffer::new(
        shared.l0_manager.get_current().read().current_version,
        None, // No WAL — written at commit time
    )));
    Ok(Writer {
        l0_manager: Arc::new(L0Manager::new_with_l0(session_l0)),
        storage: shared.storage.clone(),
        schema_manager: shared.schema_manager.clone(),
        allocator: shared.allocator.clone(),       // Shared — unique IDs
        config: shared.config.clone(),
        xervo_runtime: shared.xervo_runtime.clone(),
        transaction_l0: None,
        property_manager: shared.property_manager.clone(),
        adjacency_manager: Arc::new(AdjacencyManager::new()), // Private
        last_flush_time: Instant::now(),
        compaction_handle: Arc::new(RwLock::new(None)),
        index_rebuild_manager: None,
    })
}
```

## 6.7 NativeSessionAdapter

Implements `LocyExecutionContext` trait, routing all mutations to the session Writer:

```rust
pub(crate) struct NativeSessionAdapter<'a> {
    db: &'a Uni,
    session_writer: Arc<RwLock<Writer>>,
    // ... same fields as NativeExecutionAdapter but using session_writer
}

#[async_trait(?Send)]
impl LocyExecutionContext for NativeSessionAdapter<'_> {
    async fn execute_mutation(&self, ast: Query, params: HashMap<String, Value>) -> Result<usize, LocyError> {
        // Routes to session_writer instead of shared writer
    }

    async fn begin_savepoint(&self) -> Result<SavepointId, LocyError> {
        let mut w = self.session_writer.write().await;
        w.begin_transaction()?; // Nested savepoint on session Writer
        Ok(SavepointId(0))
    }

    async fn rollback_savepoint(&self, _id: SavepointId) -> Result<(), LocyError> {
        let mut w = self.session_writer.write().await;
        w.rollback_transaction()?;
        Ok(())
    }
    // ... other trait methods
}
```

## 6.8 Shared Evaluation Logic

Extract the core of `LocyEngine::evaluate_with_config()` into a reusable function:

```rust
pub(crate) async fn evaluate_core(
    db: &Uni,
    registry: &LocyRuleRegistry,
    writer: &Arc<RwLock<Writer>>,
    adapter_factory: impl FnOnce(/*...*/) -> Box<dyn LocyExecutionContext>,
    program: &str,
    config: &LocyConfig,
) -> Result<LocyResult> {
    // Body of current evaluate_with_config
}
```

`LocyEngine` calls this with shared writer + `NativeExecutionAdapter`.
`LocySessionEngine` calls this with session writer + `NativeSessionAdapter`.

---

# 7. Type Encapsulation & Result Types

## 7.1 Row

```rust
// Before (public fields):
pub struct Row {
    pub columns: Arc<Vec<String>>,
    pub values: Vec<Value>,
}

// After (encapsulated):
pub struct Row {
    columns: Arc<Vec<String>>,
    values: Vec<Value>,
}

impl Row {
    pub(crate) fn new(columns: Arc<Vec<String>>, values: Vec<Value>) -> Self;
    pub fn columns(&self) -> &[String];       // existing
    pub fn values(&self) -> &[Value];         // NEW
    pub fn into_values(self) -> Vec<Value>;   // NEW
    pub fn get<T: FromValue>(&self, column: &str) -> Result<T>;  // existing
    pub fn get_idx<T: FromValue>(&self, index: usize) -> Result<T>;  // existing
    pub fn try_get<T: FromValue>(&self, column: &str) -> Option<T>;  // existing
    pub fn value(&self, column: &str) -> Option<&Value>;  // existing
    pub fn as_map(&self) -> HashMap<&str, &Value>;  // existing
    pub fn to_json(&self) -> serde_json::Value;  // existing
}
impl Index<usize> for Row { /* existing */ }
impl IntoIterator for Row { /* values iterator */ }
```

## 7.2 QueryResult

```rust
// After (encapsulated):
pub struct QueryResult {
    columns: Arc<Vec<String>>,
    rows: Vec<Row>,
    warnings: Vec<QueryWarning>,
}

impl QueryResult {
    pub(crate) fn new(columns: Arc<Vec<String>>, rows: Vec<Row>, warnings: Vec<QueryWarning>) -> Self;
    pub fn columns(&self) -> &[String];        // existing
    pub fn rows(&self) -> &[Row];              // existing
    pub fn into_rows(self) -> Vec<Row>;        // existing
    pub fn len(&self) -> usize;                // existing
    pub fn is_empty(&self) -> bool;            // existing
    pub fn iter(&self) -> impl Iterator<Item = &Row>;  // existing
    pub fn warnings(&self) -> &[QueryWarning]; // existing
    pub fn has_warnings(&self) -> bool;        // existing
}
impl IntoIterator for QueryResult { /* existing */ }
```

## 7.3 ExecuteResult

```rust
// After (enriched + encapsulated):
#[derive(Debug, Default)]
pub struct ExecuteResult {
    affected_rows: usize,
    nodes_created: usize,
    nodes_deleted: usize,
    relationships_created: usize,
    relationships_deleted: usize,
    properties_set: usize,
    labels_added: usize,
    labels_removed: usize,
}

impl ExecuteResult {
    pub fn affected_rows(&self) -> usize;
    pub fn nodes_created(&self) -> usize;
    pub fn nodes_deleted(&self) -> usize;
    pub fn relationships_created(&self) -> usize;
    pub fn relationships_deleted(&self) -> usize;
    pub fn properties_set(&self) -> usize;
    pub fn labels_added(&self) -> usize;
    pub fn labels_removed(&self) -> usize;
}
```

## 7.4 LocyResult Row Type

```rust
// Before:
pub type Row = HashMap<String, Value>;  // in uni-locy

// After:
pub type FactRow = HashMap<String, Value>;  // in uni-locy

#[deprecated(note = "Use FactRow instead")]
pub type Row = FactRow;
```

---

# 8. Re-export & Module Hygiene

## 8.1 Remove Raw Crate Re-exports

```rust
// REMOVE from lib.rs:
pub use uni_algo as algo_crate;
pub use uni_common as common;
pub use uni_query as query_crate;
pub use uni_store as store;
```

## 8.2 Add Missing Explicit Re-exports

```rust
// ADD to lib.rs:
pub use uni_query::QueryCursor;
pub use uni_common::Properties;
```

## 8.3 Canonical Import Paths

Every public type has exactly ONE canonical path:

| Type | Canonical Path | Removed Aliases |
|------|---------------|-----------------|
| `Value` | `uni_db::Value` | `uni_db::query::Value`, `uni_db::common::Value` |
| `Row` | `uni_db::Row` | `uni_db::query::Row` |
| `Node` | `uni_db::Node` | `uni_db::query::Node` |
| `Edge` | `uni_db::Edge` | `uni_db::query::Edge` |
| `QueryResult` | `uni_db::QueryResult` | `uni_db::query::QueryResult` |
| `ExecuteResult` | `uni_db::ExecuteResult` | `uni_db::query::ExecuteResult` |
| `FactRow` | `uni_db::locy::FactRow` | `uni_db::locy::Row` (deprecated) |
| `LocySession` | `uni_db::LocySession` | — |
| `CommitResult` | `uni_db::locy::CommitResult` | — |

## 8.4 Internal Module Aliases

Keep `pub mod core/storage/runtime/query/algo` for internal test access, but mark with `#[doc(hidden)]`:

```rust
#[doc(hidden)]
pub mod core { pub use crate::common::core::*; }
#[doc(hidden)]
pub mod storage { pub use crate::store::storage::*; }
// etc.
```

---

# 9. Implementation Phases

## Phase 0: Python P0 Bug Fixes (1 day)
**Zero dependency. Ship immediately.**

- Fix `QueryBuilder.params()` / `AsyncQueryBuilder.params()` to return `self`
- Add `.execute() -> int` terminal to Python `QueryBuilder` / `AsyncQueryBuilder`
- Add `TransactionQueryBuilder.fetch_all()` in both Rust and Python

Files: `bindings/uni-db/src/builders.rs`, `bindings/uni-db/src/async_api.rs`, `crates/uni/src/api/transaction.rs`

## Phase 1: Simplify Open + Param $ Stripping (1 day)
**Zero breaking changes. Purely additive.**

- `impl IntoFuture for UniBuilder`
- `param()` strips leading `$` on all builders
- Update doc-comments

Files: `crates/uni/src/api/mod.rs`, `crates/uni/src/api/query_builder.rs`, `session.rs`, `transaction.rs`, `locy_builder.rs`

## Phase 2: Type Encapsulation (2-3 days)
**Largest mechanical change. ~90 files.**

### 2A: Rename Locy Row → FactRow (~25 files)
- `crates/uni-locy/src/result.rs`: type alias rename
- All internal usages in `crates/uni-locy/` and `crates/uni-query/src/query/df_graph/`

### 2B: Encapsulate Row/QueryResult/ExecuteResult fields (~80 files)
- `crates/uni-query/src/types.rs`: fields → `pub(crate)`, add constructors + accessors
- ~80 test files: mechanical `.rows` → `.rows()`, `.values[i]` → `.values()[i]`
- Python bindings: ~15 sites

### Strategy: Scripted find-and-replace, batch by 20 files, test between batches.

## Phase 3: Naming Consistency + Re-export Cleanup (1 day)

- Rename `SessionQueryBuilder::execute()` → `fetch_all()`, `execute_mutation()` → `execute()`
- Deprecate `execute_with()` on `Uni` with doc pointing to `query_with()`
- Deprecate `LabelBuilder::done()` / `EdgeTypeBuilder::done()`
- Remove raw crate re-exports from `lib.rs`
- Add `QueryCursor`, `Properties` to explicit re-exports
- Mark internal module aliases `#[doc(hidden)]`

Files: `session.rs`, `impl_query.rs`, `schema.rs`, `lib.rs`, bindings

## Phase 4: Enrich ExecuteResult (2 days)

- Expand `ExecuteResult` with per-operation counters
- Plumb counters through executor (Option A: classify LogicalPlan, populate from mutation_count delta)
- Update construction sites

Files: `crates/uni-query/src/types.rs`, `crates/uni/src/api/impl_query.rs`, `query_builder.rs`, `transaction.rs`, `session.rs`

## Phase 5: Complete UniSync + Python Parity (2 days)

- Add `UniSync::locy()`, `bulk_writer()`, `explain()`, `profile()`, `session()`
- Add `QueryBuilderSync::timeout()`, `max_memory()`, `execute()`
- New sync wrapper types: `LocyEngineSync`, `BulkWriterBuilderSync`
- Document sync methods on async Python classes

Files: `crates/uni/src/api/sync.rs`, `bindings/uni-db/src/async_api.rs`

## Phase 6: Locy Session — Rust (3-4 days)

### 6A: Infrastructure
- Add error variants to `UniError`
- Add `Writer::new_session_writer()` and `Writer::merge_session_l0()`
- Add `L0Manager::new_with_l0()` if needed
- Add `L0Buffer::is_empty()` helper

### 6B: Session Types
- Create `crates/uni/src/api/locy_session.rs`:
  - `LocySession`, `LocySessionEngine`, `LocySessionBuilder`
  - `NativeSessionAdapter` implementing `LocyExecutionContext`
  - `CommitResult`, `RuleConflictPolicy`
- Extract shared eval logic from `impl_locy.rs` into `evaluate_core()`
- Add `Uni::locy_session()` method

### 6C: Wire up
- `mod locy_session` in `api/mod.rs`
- Re-exports in `lib.rs`
- UniSync wrapper

Files: `crates/uni-store/src/runtime/writer.rs`, `crates/uni-store/src/runtime/l0.rs`, `crates/uni-common/src/api/error.rs`, `crates/uni/src/api/locy_session.rs` (NEW), `crates/uni/src/api/impl_locy.rs`, `crates/uni/src/api/mod.rs`, `crates/uni/src/lib.rs`

## Phase 7: Locy Session — Python Bindings (2 days)

- Add core functions in `bindings/uni-db/src/core.rs`
- Add `LocySession` pyclass (sync) with context manager
- Add `AsyncLocySession` pyclass (async) with async context manager
- Add `Database.locy_session()` and `AsyncDatabase.locy_session()`
- Register classes in module

Files: `bindings/uni-db/src/core.rs`, `sync_api.rs`, `async_api.rs`, `lib.rs`

## Phase 8: Internal Optimizations (ongoing, lower priority)

- **8A**: Query plan cache (internal LRU, no API change)
- **8B**: CancellationToken on QueryBuilder
- **8C**: PropertyOptions struct (replaces `property_nullable`)
- **8D**: Parse error always has position info

## Dependency Graph

```
Phase 0 ─────────────── (independent, ship first — bug fixes)
Phase 1 ─────────────── (independent)
Phase 2A ────────────── (independent)
Phase 2B ── depends on 2A
Phase 3 ─── after 2B preferred
Phase 4 ─── after 2B required (fields already private)
Phase 5 ─── independent, parallel with 3-4
Phase 6 ─── after 2A (FactRow), after 3 (re-exports clean)
Phase 7 ─── after 6 (Rust LocySession exists)
Phase 8 ─── independent, lowest priority
```

---

# 10. Files to Modify

## Rust — Core API

| File | Phases | Changes |
|------|--------|---------|
| `crates/uni/src/api/mod.rs` | 1, 6 | IntoFuture, locy_session() method |
| `crates/uni/src/api/impl_query.rs` | 2B, 3, 4 | Field encapsulation, deprecate execute_with, ExecuteResult plumbing |
| `crates/uni/src/api/query_builder.rs` | 1, 2B | $ stripping, field encapsulation |
| `crates/uni/src/api/transaction.rs` | 0, 1, 2B | Add fetch_all, $ stripping, field encapsulation |
| `crates/uni/src/api/session.rs` | 1, 2B, 3 | $ stripping, field encapsulation, naming renames |
| `crates/uni/src/api/locy_builder.rs` | 1 | $ stripping |
| `crates/uni/src/api/schema.rs` | 3 | Deprecate done() |
| `crates/uni/src/api/sync.rs` | 5 | Complete UniSync surface |
| `crates/uni/src/api/impl_locy.rs` | 6 | Extract evaluate_core() |
| `crates/uni/src/api/locy_session.rs` | 6 | **NEW**: LocySession, LocySessionEngine, NativeSessionAdapter |
| `crates/uni/src/lib.rs` | 2A, 3, 6 | Re-export cleanup, FactRow, LocySession exports |

## Rust — Internal Crates

| File | Phases | Changes |
|------|--------|---------|
| `crates/uni-query/src/types.rs` | 2B, 4 | Field encapsulation, ExecuteResult expansion |
| `crates/uni-locy/src/result.rs` | 2A | Row → FactRow rename |
| `crates/uni-locy/src/` (~10 files) | 2A | FactRow usages |
| `crates/uni-common/src/api/error.rs` | 6 | SessionConflict, SessionAlreadyCompleted, RuleConflict |
| `crates/uni-store/src/runtime/writer.rs` | 6 | new_session_writer(), merge_session_l0() |
| `crates/uni-store/src/runtime/l0.rs` | 6 | is_empty() helper |
| `crates/uni-store/src/runtime/l0_manager.rs` | 6 | new_with_l0() constructor |

## Python Bindings

| File | Phases | Changes |
|------|--------|---------|
| `bindings/uni-db/src/builders.rs` | 0, 2B | Fix params() return, add execute(), field accessors |
| `bindings/uni-db/src/sync_api.rs` | 0, 7 | Transaction fetch_all, LocySession class |
| `bindings/uni-db/src/async_api.rs` | 0, 7 | Async execute(), AsyncLocySession class |
| `bindings/uni-db/src/core.rs` | 3, 7 | Re-export fix, locy_session core functions |
| `bindings/uni-db/src/lib.rs` | 7 | Register LocySession classes |

## Tests (~90 files for Phase 2B mechanical migration)

| File | Phase | Change |
|------|-------|--------|
| `crates/uni/tests/*.rs` (~80 files) | 2B | `.rows` → `.rows()`, `.values[i]` → `.values()[i]` |
| `crates/uni/tests/locy_session_test.rs` | 6 | **NEW**: 19 integration tests |
| `bindings/uni-db/tests/test_locy_session.py` | 7 | **NEW**: 6+ Python tests |

---

# 11. Test Plan

## Phase 0 Tests

| Test | Description |
|------|-------------|
| `test_python_params_returns_self` | `db.query_with(q).params({"k": v}).fetch_all()` doesn't raise |
| `test_python_builder_execute` | `db.query_with("CREATE ...").param("k", v).execute()` returns int |
| `test_transaction_query_builder_fetch_all` | `tx.query_with("MATCH...").param("v", 1).fetch_all()` returns rows |

## Phase 1 Tests

| Test | Description |
|------|-------------|
| `test_into_future_open` | `Uni::open("path").await?` works without `.build()` |
| `test_into_future_temporary` | `Uni::temporary().await?` works |
| `test_param_strips_dollar` | `.param("$name", "Alice")` works same as `.param("name", "Alice")` |

## Phase 2B Tests

All existing tests pass after mechanical migration. No new tests — this is a refactoring.

## Phase 6 Tests (Locy Session — 19 tests)

| Test | Description |
|------|-------------|
| `test_session_basic_evaluate` | Create session, evaluate query, verify results |
| `test_session_register_and_evaluate` | Register rules in session, evaluate using them |
| `test_session_write_isolation` | Session A DERIVEs facts, Session B cannot see them |
| `test_session_read_committed` | Session A commits, Session B sees committed data on next evaluate |
| `test_session_commit_promotes_mutations` | After commit, shared queries see derived facts |
| `test_session_commit_promotes_rules` | After commit, shared `locy().evaluate()` sees registered rules |
| `test_session_rollback_discards` | After rollback, shared state is unchanged |
| `test_session_auto_rollback_on_drop` | Drop without commit discards changes, warns if dirty |
| `test_session_occ_conflict_same_entity` | Two sessions update same vertex, second gets SessionConflict |
| `test_session_occ_no_conflict_disjoint` | Two sessions write disjoint entities, both commit |
| `test_session_occ_no_conflict_pure_inserts` | Two sessions DERIVE new facts, both commit |
| `test_session_assume_isolation` | ASSUME mutations invisible after block, session L0 unchanged |
| `test_session_assume_concurrent` | Two sessions run ASSUME concurrently without blocking |
| `test_session_rule_isolation` | Rules in Session A invisible to Session B |
| `test_session_rule_conflict_error` | Commit fails with RuleConflict (Error policy) |
| `test_session_rule_conflict_replace` | Commit succeeds and replaces (Replace policy) |
| `test_session_double_commit_error` | Second commit returns SessionAlreadyCompleted |
| `test_session_evaluate_after_commit_error` | Evaluate after commit returns error |
| `test_session_is_dirty` | False initially, true after DERIVE, false after commit |

## Phase 7 Tests (Python Locy Session — 6 tests)

| Test | Description |
|------|-------------|
| `test_sync_session_lifecycle` | Create, evaluate, commit via sync API |
| `test_sync_context_manager` | `with` block auto-rollback on exception |
| `test_async_session_lifecycle` | Create, evaluate, commit via async API |
| `test_async_context_manager` | `async with` block auto-rollback |
| `test_sync_parallel_sessions` | Two threads, each with own session, both commit |
| `test_async_parallel_sessions` | Two tasks via `asyncio.gather`, both commit |

## Verification Commands

After each phase:
```bash
cargo build --workspace
cargo test -p uni-db -- -n auto
cargo test --workspace -- -n auto
cd bindings/uni-db && poetry run pytest -n auto
cargo doc -p uni-db --no-deps
```
