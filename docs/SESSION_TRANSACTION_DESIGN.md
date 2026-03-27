# Uni Session & Transaction Architecture

**RFC — March 2026**

A proposal to restructure the Uni public API around two core concepts: **Sessions** (isolated workspaces) and **Transactions** (atomic write boundaries). This document is intended for team discussion.

---

# Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Motivation](#2-motivation)
3. [The Model](#3-the-model)
4. [Uni — The Database Handle](#4-uni--the-database-handle)
5. [Session — The Isolated Workspace](#5-session--the-isolated-workspace)
6. [Transaction — The Write Boundary](#6-transaction--the-write-boundary)
7. [How Locy Fits In](#7-how-locy-fits-in)
8. [Lifecycle & Ownership](#8-lifecycle--ownership)
9. [Isolation Model](#9-isolation-model)
10. [Commit Protocol](#10-commit-protocol)
11. [Rust API Reference](#11-rust-api-reference)
12. [Python API Reference](#12-python-api-reference)
13. [Open Questions](#13-open-questions)
14. [Migration Path](#14-migration-path)
15. [Comparison to Other Databases](#15-comparison-to-other-databases)

---

# 1. Executive Summary

Today the Uni API has three overlapping scoping concepts:

| Concept | What it does | Problem |
|---------|-------------|---------|
| `Session` | Holds a `HashMap` of query parameters | Misleading name — no isolation, no commit |
| `Transaction` | ACID writes via nested L0 on shared Writer | Cypher-only, single-writer, no Locy support |
| `LocySession` (proposed) | Isolated Locy evaluation with private Writer | Locy-only, duplicates Transaction semantics |

We propose collapsing these into **two concepts**:

| Concept | Role | Rule |
|---------|------|------|
| **Session** | Isolated workspace — reads, Locy evaluation, rule registration, scoped params | All data access goes through a Session |
| **Transaction** | Write boundary within a Session — Cypher mutations, DERIVE, ASSUME | All writes go through a Transaction |

The database handle (`Uni`) becomes a pure lifecycle and admin object. No reads or writes happen on `Uni` directly.

---

# 2. Motivation

## 2.1 Why no reads outside a Session?

**Predictable isolation.** Today, `db.query()` uses read-committed isolation with no consistency guarantees between calls. Two consecutive queries may see different data if another thread writes between them. By requiring a Session, the user explicitly chooses their isolation boundary. The Session can provide read-committed (default) or snapshot isolation (future).

**State management.** Users frequently need scoped parameters (tenant ID, user context) and private Locy rules. Today these require separate `Session` and `LocyEngine::register()` calls with no shared lifetime. A unified Session holds all read-time state in one place.

**Concurrent safety.** With explicit Sessions, the runtime knows exactly which clients are reading what. This enables future features like session-level query caching, read snapshots, and connection tracking.

## 2.2 Why no writes outside a Transaction?

**Atomicity.** Today, `db.execute("CREATE (:A)"); db.execute("CREATE (:B)");` are two independent auto-committed writes. If the second fails, the first is already visible. A Transaction groups them into an atomic unit.

**Isolation.** Writes in a Transaction go to a private L0 buffer. Other Sessions cannot see them until commit. Today, `db.execute()` writes directly to the shared Writer, making partial results visible immediately.

**Conflict detection.** With OCC at commit time, concurrent Transactions can write independently and detect conflicts only when promoting to shared state. Today's single-writer model serializes all writes.

## 2.3 Why merge the three concepts?

**Cognitive load.** A user learning the API today must understand Session (param bag), Transaction (Cypher ACID), and LocySession (Locy isolation) — three concepts with different APIs, different capabilities, and different isolation models. The new design has two concepts: Session (read) and Transaction (write). The query language (Cypher vs Locy) is orthogonal to the scoping model.

**Mixed workloads.** Today, a workflow that does Cypher reads → Locy evaluation → Cypher mutation → Locy DERIVE requires juggling multiple scoping objects. In the new model, a single Session holds the read state, and a single Transaction handles all writes regardless of query language.

---

# 3. The Model

```
┌─────────────────────────────────────────────────────────┐
│                    Uni (database handle)                  │
│                                                           │
│  Lifecycle: open, shutdown                                │
│  Admin: schema, snapshots, compaction, indexes            │
│  Factory: session()                                       │
│                                                           │
│  NO query(), execute(), or locy() here.                   │
└───────────────────────┬─────────────────────────────────┘
                        │ session()
                        ▼
┌─────────────────────────────────────────────────────────┐
│                   Session (read scope)                    │
│                                                           │
│  Owns: private LocyRuleRegistry, scoped params            │
│  Reads: shared DB via read-committed (or snapshot)        │
│                                                           │
│  Methods: query(), query_with(), explain(), profile()     │
│           locy().evaluate(), locy().register()             │
│           set(), get() — scoped params                    │
│           begin() — spawn a Transaction                   │
│                                                           │
│  NO execute(). NO DERIVE. NO Cypher mutations.            │
└───────────────────────┬─────────────────────────────────┘
                        │ begin()
                        ▼
┌─────────────────────────────────────────────────────────┐
│                 Transaction (write scope)                 │
│                                                           │
│  Owns: private Writer with own L0 buffer                  │
│  Inherits: Session's params, rules                        │
│  Reads: shared DB + Transaction's uncommitted writes      │
│  Writes: to private L0 (invisible to others)              │
│                                                           │
│  Methods: query(), execute(), query_with()                │
│           locy().evaluate() (incl. DERIVE, ASSUME)        │
│           locy().register() — promoted on commit          │
│           commit(), rollback()                            │
│                                                           │
│  commit() = acquire shared Writer lock briefly,           │
│             OCC conflict check, WAL write,                │
│             merge L0, promote rules, release lock         │
└─────────────────────────────────────────────────────────┘
```

The rule is simple:
- **Need to read?** Create a Session.
- **Need to write?** Open a Transaction within that Session.

---

# 4. Uni — The Database Handle

`Uni` is a lifecycle and administration handle. It opens the database, manages storage, and provides the factory method for Sessions. It does **not** execute queries or mutations.

## 4.1 API Surface

```rust
impl Uni {
    // ── Lifecycle ──
    fn open(uri: impl Into<String>) -> UniBuilder;
    fn temporary() -> UniBuilder;
    fn in_memory() -> UniBuilder;
    async fn shutdown(self) -> Result<()>;

    // ── Session Factory (the ONLY entry point for data access) ──
    async fn session(&self) -> Result<Session>;

    // ── Schema DDL (admin — operates outside session/transaction) ──
    fn schema(&self) -> SchemaBuilder<'_>;
    async fn load_schema(&self, path: impl AsRef<Path>) -> Result<()>;
    async fn save_schema(&self, path: impl AsRef<Path>) -> Result<()>;

    // ── Schema Inspection ──
    fn get_schema(&self) -> Arc<Schema>;
    async fn label_exists(&self, name: &str) -> Result<bool>;
    async fn edge_type_exists(&self, name: &str) -> Result<bool>;
    async fn list_labels(&self) -> Result<Vec<String>>;
    async fn list_edge_types(&self) -> Result<Vec<String>>;
    async fn get_label_info(&self, name: &str) -> Result<Option<LabelInfo>>;

    // ── Storage Admin ──
    async fn flush(&self) -> Result<()>;
    async fn compact_label(&self, label: &str) -> Result<CompactionStats>;
    async fn compact_edge_type(&self, edge_type: &str) -> Result<CompactionStats>;
    async fn wait_for_compaction(&self) -> Result<()>;

    // ── Snapshots ──
    async fn create_snapshot(&self, name: Option<&str>) -> Result<String>;
    async fn list_snapshots(&self) -> Result<Vec<SnapshotManifest>>;
    async fn restore_snapshot(&self, snapshot_id: &str) -> Result<()>;

    // ── Index Admin ──
    async fn rebuild_indexes(&self, label: &str, background: bool) -> Result<Option<String>>;
    async fn index_rebuild_status(&self) -> Result<Vec<IndexRebuildTask>>;
    async fn retry_index_rebuilds(&self) -> Result<Vec<String>>;
    async fn is_index_building(&self, label: &str) -> Result<bool>;
    fn list_indexes(&self, label: &str) -> Vec<IndexDefinition>;
    fn list_all_indexes(&self) -> Vec<IndexDefinition>;

    // ── Bulk Loading ──
    fn bulk_writer(&self) -> BulkWriterBuilder<'_>;

    // ── Global Locy Rules (cloned into new Sessions) ──
    fn locy(&self) -> LocyAdmin<'_>;  // register/clear global rules only

    // ── Xervo (ML Runtime) ──
    fn xervo(&self) -> UniXervo<'_>;

    // ── Configuration ──
    fn config(&self) -> &UniConfig;
}
```

## 4.2 What Stays on Uni

| Category | Rationale |
|----------|-----------|
| Lifecycle (open, shutdown) | Database-level, not session-scoped |
| Schema DDL | Structural metadata — affects all sessions equally. Analogous to `ALTER TABLE` in PostgreSQL, which is auto-committed and not transactional in most databases. |
| Schema inspection | Read-only metadata, not user data |
| Storage admin (flush, compact) | Operational concern, not data access |
| Snapshots | Database-level point-in-time management |
| Index admin | Operational concern |
| Bulk loading | High-throughput path that bypasses normal isolation (deferred indexes, direct L0 writes) |
| Global Locy rules | Default rules cloned into every new Session |
| Xervo | ML runtime, not data scoping |

## 4.3 What Moves Off Uni

| Old | New Home | Rationale |
|-----|----------|-----------|
| `db.query()` | `session.query()` | Reads require a Session |
| `db.execute()` | `tx.execute()` | Writes require a Transaction |
| `db.query_with()` | `session.query_with()` | Reads require a Session |
| `db.query_cursor()` | `session.query_cursor()` | Reads require a Session |
| `db.explain()` | `session.explain()` | Read operation |
| `db.profile()` | `session.profile()` | Read operation |
| `db.begin()` | `session.begin()` | Transactions live within Sessions |
| `db.locy().evaluate()` | `session.locy().evaluate()` | Locy reads require a Session |
| `db.locy().register()` | `session.locy().register()` or `db.locy().register()` (global) | Rules scoped to session or global |

## 4.4 LocyAdmin (Global Rules Only)

```rust
/// Admin-level access to the global Locy rule registry.
/// Rules registered here are cloned into every new Session.
/// This is NOT for evaluation — use session.locy() for that.
pub struct LocyAdmin<'a> { db: &'a Uni }

impl<'a> LocyAdmin<'a> {
    /// Register rules in the global registry (cloned into future Sessions).
    pub fn register(&self, program: &str) -> Result<()>;

    /// Clear all global rules.
    pub fn clear_registry(&self);
}
```

---

# 5. Session — The Isolated Workspace

A Session is a long-lived, isolated read context. It holds scoped parameters, a private Locy rule registry, and provides the factory for Transactions.

## 5.1 What a Session Owns

```rust
pub struct Session {
    /// Shared database reference.
    db: Arc<Uni>,

    /// Scoped query parameters (injected into every query).
    params: HashMap<String, Value>,

    /// Private Locy rule registry (cloned from global at creation).
    /// Rules registered here are visible only within this Session
    /// and its Transactions.
    rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,

    /// Session identifier (UUID) for logging and diagnostics.
    id: String,
}
```

A Session does **not** own a Writer or L0 buffer. It reads directly from the shared database (read-committed). It is cheap to create — just an Arc clone and a rule registry clone.

## 5.2 API Surface

```rust
impl Session {
    // ── Scoped Parameters ──

    /// Set a session-scoped parameter. Injected into every query
    /// executed through this Session and its Transactions.
    pub fn set<K: Into<String>, V: Into<Value>>(&mut self, key: K, value: V);

    /// Get a session parameter.
    pub fn get(&self, key: &str) -> Option<&Value>;

    // ── Cypher Reads ──

    /// Execute a read-only Cypher query.
    /// Errors if the query contains mutations (CREATE, SET, DELETE, MERGE).
    pub async fn query(&self, cypher: &str) -> Result<QueryResult>;

    /// Fluent query builder with parameter binding and timeouts.
    pub fn query_with(&self, cypher: &str) -> SessionQueryBuilder<'_>;

    /// Streaming cursor for large result sets.
    pub async fn query_cursor(&self, cypher: &str) -> Result<QueryCursor>;

    /// Explain a query plan without executing.
    pub async fn explain(&self, cypher: &str) -> Result<ExplainOutput>;

    /// Execute and profile a query.
    pub async fn profile(&self, cypher: &str) -> Result<(QueryResult, ProfileOutput)>;

    // ── Locy (Read-Only Evaluation + Rule Management) ──

    /// Access the session-scoped Locy engine.
    /// Supports evaluation (read-only — no DERIVE) and rule registration.
    pub fn locy(&self) -> SessionLocyEngine<'_>;

    // ── Transaction Factory ──

    /// Begin a Transaction within this Session.
    /// The Transaction inherits session params and rules,
    /// and owns a private Writer for isolated writes.
    pub async fn begin(&self) -> Result<Transaction>;

    // ── Lifecycle ──

    /// Session identifier (UUID).
    pub fn id(&self) -> &str;
}

impl Drop for Session {
    fn drop(&mut self) {
        // Lightweight — no I/O, no locks. Just drops the rule registry clone.
    }
}
```

## 5.3 SessionLocyEngine

```rust
/// Locy engine scoped to a Session.
/// Read-only: can evaluate queries and register rules.
/// Cannot DERIVE or execute mutations — those require a Transaction.
pub struct SessionLocyEngine<'a> {
    session: &'a Session,
}

impl<'a> SessionLocyEngine<'a> {
    /// Register rules in the session's private registry.
    pub fn register(&self, program: &str) -> Result<()>;

    /// Clear all rules in the session's registry.
    pub fn clear_registry(&self);

    /// Compile a program without executing.
    pub fn compile_only(&self, program: &str) -> Result<CompiledProgram>;

    /// Evaluate a Locy program (read-only).
    /// Errors if the program contains DERIVE or Cypher mutations.
    pub async fn evaluate(&self, program: &str) -> Result<LocyResult>;

    /// Fluent evaluation builder.
    pub fn evaluate_with(&self, program: &str) -> SessionLocyBuilder<'a>;

    /// Explain rule evaluation strategy.
    pub async fn explain(&self, program: &str) -> Result<LocyResult>;
}
```

## 5.4 Enforcement

The Session enforces read-only access:

- `session.query()` validates the Cypher AST after parsing — if it contains `CREATE`, `SET`, `DELETE`, `MERGE`, or `REMOVE` clauses, it returns `UniError::ReadOnly`.
- `session.locy().evaluate()` validates the compiled program — if it contains `DERIVE` commands or Cypher mutations within rule bodies, it returns `UniError::ReadOnly`.
- There is no `session.execute()` method. The method simply does not exist.

This is compile-time enforcement (no `execute()` to call) layered with runtime enforcement (read-only validation on `query()` and `evaluate()`).

---

# 6. Transaction — The Write Boundary

A Transaction is a short-lived, isolated write context within a Session. It owns a private Writer with its own L0 buffer. All mutations — Cypher and Locy — write to this private buffer. On commit, the buffer is atomically promoted to the shared database.

## 6.1 What a Transaction Owns

```rust
pub struct Transaction {
    /// The Session this Transaction belongs to.
    session: Arc<Session>,  // or reference, TBD

    /// Private Writer with own L0 buffer and AdjacencyManager.
    /// Shares IdAllocator, StorageManager, SchemaManager with
    /// the shared Writer via Arc clones.
    tx_writer: Arc<RwLock<Writer>>,

    /// Rule registry inherited from session, may be mutated
    /// by locy().register() within the transaction.
    /// On commit, new rules are promoted to the session's registry.
    rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,

    /// Transaction identifier (UUID) for logging.
    id: String,

    /// Whether commit or rollback has been called.
    completed: bool,
}
```

## 6.2 API Surface

```rust
impl Transaction {
    // ── Cypher Reads (same as Session, but also sees uncommitted writes) ──

    /// Execute a Cypher query. Sees shared DB + this Transaction's writes.
    pub async fn query(&self, cypher: &str) -> Result<QueryResult>;

    /// Fluent query builder.
    pub fn query_with(&self, cypher: &str) -> TransactionQueryBuilder<'_>;

    // ── Cypher Writes ──

    /// Execute a Cypher mutation. Writes to the Transaction's private L0.
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult>;

    /// Fluent mutation builder.
    pub fn execute_with(&self, cypher: &str) -> TransactionQueryBuilder<'_>;

    // ── Locy (Full Access — Read + Write) ──

    /// Access the transaction-scoped Locy engine.
    /// Full access: evaluate (including DERIVE, ASSUME), register rules.
    pub fn locy(&self) -> TransactionLocyEngine<'_>;

    // ── Lifecycle ──

    /// Commit the Transaction.
    /// Acquires the shared Writer lock, performs OCC conflict detection,
    /// writes to WAL, merges L0, promotes rules to session registry.
    pub async fn commit(mut self) -> Result<CommitResult>;

    /// Rollback the Transaction. Discards all writes. No I/O.
    pub fn rollback(mut self);

    /// True if there are uncommitted mutations.
    pub fn is_dirty(&self) -> bool;

    /// Transaction identifier (UUID).
    pub fn id(&self) -> &str;
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.completed {
            // Auto-rollback. Warn if dirty.
            if self.is_dirty() {
                tracing::warn!(
                    tx_id = %self.id,
                    "Transaction dropped with uncommitted writes — auto-rolling back"
                );
            }
        }
    }
}
```

## 6.3 TransactionLocyEngine

```rust
/// Locy engine scoped to a Transaction.
/// Full access: evaluate (including DERIVE, ASSUME), register rules.
pub struct TransactionLocyEngine<'a> {
    tx: &'a Transaction,
}

impl<'a> TransactionLocyEngine<'a> {
    /// Register rules. On commit, promoted to the owning Session's registry.
    pub fn register(&self, program: &str) -> Result<()>;

    /// Clear transaction-local rules.
    pub fn clear_registry(&self);

    /// Compile without executing.
    pub fn compile_only(&self, program: &str) -> Result<CompiledProgram>;

    /// Evaluate a Locy program with full write access.
    /// DERIVE writes to the Transaction's private L0.
    /// ASSUME creates savepoints on the Transaction's Writer.
    pub async fn evaluate(&self, program: &str) -> Result<LocyResult>;

    /// Fluent evaluation builder.
    pub fn evaluate_with(&self, program: &str) -> TransactionLocyBuilder<'a>;

    /// Explain rule evaluation strategy.
    pub async fn explain(&self, program: &str) -> Result<LocyResult>;
}
```

## 6.4 CommitResult

```rust
pub struct CommitResult {
    /// Number of graph mutations (vertex/edge creates/deletes) committed.
    pub mutations_committed: usize,
    /// Number of rules promoted to the owning Session's registry.
    pub rules_promoted: usize,
}
```

---

# 7. How Locy Fits In

Locy is not a separate scoping system. It follows the same Session/Transaction rules as Cypher:

| Locy Operation | Where | Why |
|---------------|-------|-----|
| `evaluate("?- query.")` | Session or Transaction | Read-only evaluation |
| `register("rule(X) :- ...")` | Session or Transaction | Session-private rules; tx rules promoted on commit |
| `evaluate("DERIVE fact(X) :- ...")` | Transaction only | DERIVE creates graph mutations |
| `ASSUME { ... }` body | Transaction only | ASSUME creates temporary savepoints on the Writer |
| `compile_only()` | Session or Transaction | Pure CPU — no I/O, no isolation concern |
| `explain()` | Session or Transaction | Read-only introspection |

The key distinction: **read-only Locy** (queries, rule registration, compilation) works in a Session. **Write Locy** (DERIVE, ASSUME with mutations) requires a Transaction.

### Example: Mixed Cypher + Locy Workflow

```rust
let session = db.session().await?;

// Register rules (session-scoped, persist across transactions)
session.locy().register("
    path(X, Y) :- edge(X, Y).
    path(X, Z) :- edge(X, Y), path(Y, Z).
")?;

// Read-only Locy evaluation (no transaction needed)
let result = session.locy().evaluate("?- path('a', X).").await?;

// Now do some writes — need a transaction
let tx = session.begin().await?;

// Cypher mutation
tx.execute("CREATE (:City {name: 'Atlantis'})").await?;

// Locy DERIVE (writes derived facts to tx's private L0)
tx.locy().evaluate("DERIVE reachable(X, Y) :- path(X, Y).").await?;

// Locy ASSUME (savepoint on tx's Writer)
tx.locy().evaluate("
    ASSUME { CREATE (:City {name: 'Lemuria'})-[:ROAD]->(:City {name: 'Athens'}) }
    ?- path('Lemuria', X).
").await?;

tx.commit().await?;
// Cypher mutations + DERIVE facts promoted to shared DB
// Rules still in session (they were registered before the transaction)

// Session still alive — rules still registered
let result = session.locy().evaluate("?- reachable('a', X).").await?;
```

---

# 8. Lifecycle & Ownership

## 8.1 Object Graph

```
Uni (Arc<Uni>)
 └─► Session (holds Arc<Uni>)
      ├─► params: HashMap<String, Value>
      ├─► rule_registry: Arc<RwLock<LocyRuleRegistry>>
      └─► Transaction (holds Arc<Session> or &Session)
           ├─► tx_writer: Arc<RwLock<Writer>>  [private]
           ├─► rule_registry: Arc<RwLock<LocyRuleRegistry>>  [cloned from session]
           └─► completed: bool
```

## 8.2 Creation Costs

| Object | Cost | What Happens |
|--------|------|-------------|
| `Uni::open()` | Heavy | Opens storage, loads schema, initializes Writer, allocates IDs |
| `db.session()` | Cheap | Arc clone + HashMap alloc + LocyRuleRegistry clone (O(n) in registered rules) |
| `session.begin()` | Moderate | Reads shared Writer (briefly) to clone Arc fields, creates private L0 buffer and AdjacencyManager |
| `tx.commit()` | Moderate | Acquires shared Writer lock, OCC check, WAL write, L0 merge, adjacency replay |
| `tx.rollback()` | Cheap | Drops private Writer and L0 buffer. No I/O. |
| `session.drop()` | Cheap | Drops rule registry and params. No I/O. |

## 8.3 Lifetime Rules

- A **Session** can outlive any Transaction created from it.
- A **Transaction** must not outlive its Session (enforced by Rust lifetimes or Arc).
- Multiple **Sessions** can coexist concurrently (each has independent state).
- Multiple **Transactions** can coexist concurrently (each has independent Writer).
- A Session can have **multiple sequential Transactions** (begin, commit, begin again).
- A Session can have **at most one active Transaction** at a time (begin while another is active → error). This simplifies rule promotion and param inheritance.

## 8.4 Rule Promotion Flow

```
Global Registry (on Uni)
    │
    │  cloned at session creation
    ▼
Session Rule Registry
    │
    │  cloned at transaction begin
    ▼
Transaction Rule Registry
    │
    │  tx.locy().register() modifies this
    │
    │  on tx.commit() — new/modified rules promoted ──► Session Rule Registry
    │  on tx.rollback() — discarded
    │
    │  NOT promoted to Global Registry
    │  (use db.locy().register() for that)
```

Rules flow **inward** (global → session → transaction) on creation, and **one level up** (transaction → session) on commit. They never jump two levels (transaction → global). This keeps the promotion logic simple and predictable.

---

# 9. Isolation Model

## 9.1 Session Reads: Read-Committed

A Session reads the latest committed state of the shared database. If another Transaction commits between two queries in the same Session, the second query sees the committed data.

```
Session A:                          Transaction B (different session):
  session.query("?- count(X).")
    → sees 10 rows
                                      tx.execute("CREATE (:New)").await?;
                                      tx.commit().await?;
  session.query("?- count(X).")
    → sees 11 rows                  ← read-committed: sees B's commit
```

**Future option:** Snapshot isolation via `db.session_at_snapshot(id)`, which pins the Session to a specific point-in-time. Not in scope for this proposal.

## 9.2 Transaction Writes: Private L0

All mutations within a Transaction write to its private L0 buffer. Other Sessions and Transactions cannot see these writes until commit.

```
Session A → Transaction A:          Session B:
  tx.execute("CREATE (:Secret)")
    → writes to A's private L0
                                      session.query("MATCH (n:Secret) RETURN n")
                                        → 0 rows (A's writes invisible)
  tx.commit()
                                      session.query("MATCH (n:Secret) RETURN n")
                                        → 1 row (A's writes now visible)
```

## 9.3 Transaction Reads: Private + Shared

A Transaction sees its own uncommitted writes overlaid on the shared database:

```rust
let tx = session.begin().await?;
tx.execute("CREATE (:Foo {id: 1})").await?;

// This query sees the uncommitted :Foo node
let result = tx.query("MATCH (n:Foo) RETURN n").await?;
assert_eq!(result.len(), 1);  // sees private write

// But another session doesn't
let other = db.session().await?;
let result = other.query("MATCH (n:Foo) RETURN n").await?;
assert_eq!(result.len(), 0);  // doesn't see A's private write
```

## 9.4 Concurrent Transactions: OCC

Multiple Transactions (from different Sessions) can operate concurrently. Conflict detection happens at commit time using write-set intersection.

```
Transaction A:                       Transaction B:
  tx.execute("SET n.x = 1           tx.execute("SET n.x = 2
    WHERE n.id = 42")                  WHERE n.id = 42")

  tx.commit()  → succeeds
                                      tx.commit()  → SessionConflict!
                                      // A already modified entity 42
```

**Pure inserts rarely conflict.** The shared `IdAllocator` guarantees globally unique IDs. Two Transactions that both `CREATE` new nodes get disjoint ID ranges — no conflict.

**DERIVE is almost always conflict-free.** DERIVE creates new entities with fresh IDs. Two Transactions running DERIVE on different rules will both commit successfully.

---

# 10. Commit Protocol

```
tx.commit()
  │
  ├─ 1. Acquire shared Writer lock (write)
  │     Brief — held only for steps 2–7
  │
  ├─ 2. OCC conflict detection (write-set intersection)
  │     For each VID/EID in transaction's L0:
  │       - Check if entity was modified in shared L0
  │         since the transaction was created
  │       - Also check pending_flush_l0s
  │       - New IDs never conflict (unique from IdAllocator)
  │
  ├─ 3. Write transaction L0 mutations to WAL
  │
  ├─ 4. Flush WAL ← THIS IS THE COMMIT POINT
  │     If this fails, transaction remains active, can retry
  │
  ├─ 5. Merge transaction L0 → shared main L0
  │     (L0Buffer::merge — vertex props, edge endpoints,
  │      tombstones, timestamps, constraint indexes)
  │
  ├─ 6. Replay edges → shared AdjacencyManager
  │     (insert_edge / add_tombstone for each edge in tx L0)
  │
  ├─ 7. Release shared Writer lock
  │
  ├─ 8. Promote new rules → Session rule registry
  │     (diff transaction registry against session baseline,
  │      apply conflict policy)
  │
  └─ 9. Clear transaction state, set completed = true
```

**Durability guarantee:** The WAL flush (step 4) is the commit point.
- Crash before flush → transaction lost, no shared state modified.
- Crash after flush, before merge → WAL replay on restart recovers.
- Crash after merge → fully durable.

---

# 11. Rust API Reference

## 11.1 Complete Usage Examples

### Basic Read

```rust
let db = Uni::open("./mydb").await?;
let session = db.session().await?;

let rows = session.query("MATCH (n:Person) RETURN n.name").await?;
for row in &rows {
    let name: String = row.get("n.name")?;
    println!("{name}");
}
```

### Parameterized Read

```rust
let mut session = db.session().await?;
session.set("tenant", 42);

let rows = session.query_with("MATCH (n) WHERE n.tenant = $tenant AND n.age > $min RETURN n")
    .param("min", 25)
    .timeout(Duration::from_secs(5))
    .fetch_all().await?;
```

### Write Transaction

```rust
let session = db.session().await?;
let tx = session.begin().await?;

tx.execute("CREATE (:Person {name: 'Alice', age: 30})").await?;
tx.execute("CREATE (:Person {name: 'Bob', age: 25})").await?;
tx.execute("
    MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
    CREATE (a)-[:KNOWS]->(b)
").await?;

let result = tx.commit().await?;
println!("Committed: {} mutations", result.mutations_committed);
```

### Mixed Cypher + Locy

```rust
let mut session = db.session().await?;

// Register rules (session-scoped)
session.locy().register("
    reachable(X, Y) :- knows(X, Y).
    reachable(X, Z) :- knows(X, Y), reachable(Y, Z).
")?;

// Read-only Locy query
let result = session.locy().evaluate("?- reachable('Alice', X).").await?;

// Write transaction for DERIVE
let tx = session.begin().await?;
tx.locy().evaluate("DERIVE connected(X, Y) :- reachable(X, Y).").await?;
tx.commit().await?;

// Read the materialized facts
let rows = session.query("MATCH (n:connected) RETURN n").await?;
```

### Locy with Parameters

```rust
let session = db.session().await?;

let result = session.locy()
    .evaluate_with("?- path($source, X).")
    .param("source", "Alice")
    .timeout(Duration::from_secs(10))
    .max_iterations(500)
    .run().await?;
```

### Concurrent Sessions

```rust
let db = Arc::new(Uni::open("./mydb").await?);

let (r1, r2) = tokio::join!(
    {
        let db = Arc::clone(&db);
        async move {
            let session = db.session().await?;
            let tx = session.begin().await?;
            tx.locy().evaluate("DERIVE hot(X) :- temp(X, T), T > 100.").await?;
            tx.commit().await
        }
    },
    {
        let db = Arc::clone(&db);
        async move {
            let session = db.session().await?;
            let tx = session.begin().await?;
            tx.locy().evaluate("DERIVE cold(X) :- temp(X, T), T < 0.").await?;
            tx.commit().await
        }
    },
);
r1?; r2?; // Both succeed — disjoint write sets
```

### OCC Conflict Handling

```rust
let s1 = db.session().await?;
let s2 = db.session().await?;

let tx1 = s1.begin().await?;
let tx2 = s2.begin().await?;

tx1.execute("MATCH (n {id: 1}) SET n.status = 'hot'").await?;
tx2.execute("MATCH (n {id: 1}) SET n.status = 'cold'").await?;

tx1.commit().await?;  // succeeds — first committer wins

match tx2.commit().await {
    Err(UniError::TransactionConflict { entity }) => {
        println!("Conflict on {entity} — retry with fresh transaction");
    }
    other => other?,
}
```

### Bulk Loading

```rust
// Bulk loading bypasses session/transaction (admin operation)
let mut writer = db.bulk_writer()
    .batch_size(10_000)
    .defer_vector_indexes(true)
    .build().await?;

writer.insert_vertices("Person", props).await?;
writer.insert_edges("KNOWS", edges).await?;
writer.commit().await?;
```

---

# 12. Python API Reference

## 12.1 Sync

```python
import uni_db as uni

db = uni.Database.open("./mydb")

# ── Session ──
session = db.session()
session.set("tenant", 42)

# Read
rows = session.query("MATCH (n) WHERE n.tenant = $tenant RETURN n")
rows = (session.query_with("MATCH (n) WHERE n.age > $min RETURN n")
              .param("min", 25)
              .timeout(5.0)
              .fetch_all())

# Locy read-only
session.locy().register("path(X,Y) :- edge(X,Y).")
result = session.locy().evaluate("?- path('a', X).")

# ── Transaction ──
with session.begin() as tx:
    tx.execute("CREATE (:Person {name: 'Alice'})")
    tx.locy().evaluate("DERIVE reachable(X,Y) :- path(X,Y).")
    tx.commit()
# Auto-rollback if exception or no commit

# Session survives
rows = session.query("MATCH (n:reachable) RETURN n")
```

## 12.2 Async

```python
import uni_db as uni

db = await uni.AsyncDatabase.open("./mydb")

# ── Session ──
session = await db.session()
session.set("tenant", 42)

rows = await session.query("MATCH (n) WHERE n.tenant = $tenant RETURN n")
rows = await (session.query_with("MATCH (n) WHERE n.age > $min RETURN n")
                    .param("min", 25)
                    .fetch_all())

# Locy
session.locy().register("path(X,Y) :- edge(X,Y).")
result = await session.locy().evaluate("?- path('a', X).")

# ── Transaction ──
async with session.begin() as tx:
    await tx.execute("CREATE (:Person {name: 'Alice'})")
    await tx.locy().evaluate("DERIVE reachable(X,Y) :- path(X,Y).")
    await tx.commit()
```

## 12.3 Context Manager Behavior

| Type | `__enter__` / `__aenter__` | `__exit__` / `__aexit__` |
|------|--------------------------|-------------------------|
| `Database` | Returns self | Calls `shutdown()` |
| `Session` | Returns self | Drops session (lightweight) |
| `Transaction` | Returns self | Auto-rollback if not committed |
| `BulkWriter` | Returns self | Calls `abort()` if not committed |

---

# 13. Open Questions

These are decisions for team discussion.

## Q1: Schema DDL — on Uni or in Transaction?

**Option A (proposed): Schema on Uni directly.**
Schema DDL (`schema().apply()`) is an admin operation. It auto-commits and is visible immediately. This matches most embedded databases (SQLite, DuckDB) where DDL is auto-committed.

```rust
db.schema().label("Person").property("name", DataType::String).apply().await?;
```

**Option B: Schema in Transaction.**
Schema changes are transactional. You can roll back a schema change. This matches PostgreSQL where DDL can be transactional.

```rust
let tx = session.begin().await?;
tx.schema().label("Person").property("name", DataType::String).apply().await?;
tx.commit().await?;
```

**Recommendation:** Option A. Schema changes are rare, structural, and affect all sessions equally. Making them transactional adds complexity (schema rollback, schema version pinning per session) with little practical benefit for an embedded database.

## Q2: Session creation — sync or async?

**Option A: Async** (`db.session().await?`)
Allows future async work during session creation (e.g., capturing a read snapshot, reserving resources).

**Option B: Sync** (`db.session()`)
Session creation is cheap today (Arc clone + HashMap + registry clone). No I/O needed.

**Recommendation:** Sync creation, with an async variant for snapshot sessions in the future:
```rust
let session = db.session();                          // sync, cheap
let session = db.session_at_snapshot(id).await?;     // async, future
```

## Q3: Multiple active Transactions per Session?

**Option A (proposed): One at a time.**
`session.begin()` errors if a Transaction is already active. Simple to reason about — rules always promote to the session on commit.

**Option B: Multiple concurrent.**
Session can have multiple Transactions active. More flexible but complicates rule promotion (which Transaction's rules win?).

**Recommendation:** Option A. If a user needs concurrent writes, they create multiple Sessions.

## Q4: Can a Transaction read data committed by a previous Transaction in the same Session?

**Yes.** The Transaction reads from the shared DB (read-committed), which includes anything previously committed — whether by this Session's previous Transactions or by other Sessions. The Session itself has no private L0 (only Transactions do).

## Q5: db.locy() — should it exist?

The global `db.locy().register()` is a convenience for setting default rules that get cloned into every new Session. Without it, users would need to call `session.locy().register()` on every new Session.

**Keep it?** Yes, but rename to `db.locy()` with only `register()` and `clear_registry()` — no `evaluate()`. Evaluation always goes through a Session.

## Q6: Bulk loading — session/transaction or admin?

Bulk loading (`db.bulk_writer()`) bypasses normal isolation for performance (deferred indexes, direct L0 writes, no OCC). It stays on `Uni` as an admin operation.

## Q7: What about the existing db.query()/db.execute() usage?

The codebase has ~3000 call sites using `db.query()` and `db.execute()`. Options:

**Option A: Hard break.** Remove `db.query()` and `db.execute()`. Fix all call sites.

**Option B: Deprecate with shim.** Keep `db.query()` as a deprecated convenience that creates an ephemeral session internally. Same for `db.execute()` (ephemeral session + transaction).

```rust
#[deprecated(note = "Use db.session().query() instead")]
pub async fn query(&self, cypher: &str) -> Result<QueryResult> {
    let session = self.session().await?;
    session.query(cypher).await
}
```

**Recommendation:** Option B for migration. The shims can be removed in a future major version.

---

# 14. Migration Path

## Phase 1: Add Session/Transaction (non-breaking)

Add the new `Session` and `Transaction` types alongside the existing API. Both old and new paths work.

```rust
// Old (still works)
db.query("MATCH (n) RETURN n").await?;

// New (also works)
let session = db.session();
session.query("MATCH (n) RETURN n").await?;
```

## Phase 2: Deprecate old paths

Add `#[deprecated]` to `db.query()`, `db.execute()`, `db.begin()`, `db.locy().evaluate()`. Compiler warnings guide users to the new API.

## Phase 3: Internal migration

Migrate the test suite and Python bindings to the new API. ~3000 call sites — mechanical but high volume.

## Phase 4: Remove old paths

Remove deprecated methods. The Session/Transaction model is the only way to access data.

---

# 15. Comparison to Other Databases

| Feature | Uni (proposed) | PostgreSQL | Neo4j | SQLite | DuckDB |
|---------|:---:|:---:|:---:|:---:|:---:|
| Reads require session/connection | **Yes** | Yes | Yes | Yes (conn) | Yes (conn) |
| Writes require transaction | **Yes** | Yes | Yes | Yes | Yes |
| Auto-commit convenience | Deprecated shim | `autocommit=True` | Auto-commit mode | `execute()` auto-commits | `execute()` auto-commits |
| Concurrent transactions | **Yes (OCC)** | Yes (MVCC) | Yes (per-session) | No (file lock) | Yes (MVCC) |
| Session-scoped state | **Params + rules** | GUC variables | Bookmarks | Pragmas | Settings |
| DDL transactional | **No (admin)** | Yes | No | Partial | No |
| Session outlives transaction | **Yes** | Yes | Yes | N/A | N/A |

The proposed model is closest to **Neo4j's driver architecture**: Driver → Session → Transaction, where Sessions are the read scope and Transactions are the write scope. The main difference is that Uni Sessions also hold Locy rules, which has no equivalent in Neo4j.
