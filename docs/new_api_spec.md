# Uni Session & Transaction API — Implementation Specification

**Version 3.0 — March 2026**
**Status: Ready for Implementation**

This document is the definitive specification for the Uni public API restructuring around Sessions and Transactions. It supersedes all prior RFCs, discussion documents, and the v2.1 draft. Every design decision herein is final and implementation-ready.

---

## Table of Contents

1. [Design Principles](#1-design-principles)
2. [Changes from Prior Concurrency Model](#2-changes-from-prior-concurrency-model)
3. [The Model](#3-the-model)
4. [Uni — The Database Handle](#4-uni--the-database-handle)
5. [Session — The Read Scope](#5-session--the-read-scope)
6. [Transaction — The Write Scope](#6-transaction--the-write-scope)
7. [How Locy Fits In](#7-how-locy-fits-in)
8. [BulkWriter — High-Throughput Writes](#8-bulkwriter--high-throughput-writes)
9. [Streaming Appender](#9-streaming-appender)
10. [Prepared Statements & Plan Cache](#10-prepared-statements--plan-cache)
11. [Query Cancellation](#11-query-cancellation)
12. [Timeouts](#12-timeouts)
13. [Metrics & Observability](#13-metrics--observability)
14. [Commit Notifications](#14-commit-notifications)
15. [Session Hooks](#15-session-hooks)
16. [Session Templates](#16-session-templates)
17. [Concurrency Model](#17-concurrency-model)
18. [Isolation Model](#18-isolation-model)
19. [Commit Protocol](#19-commit-protocol)
20. [Lifecycle & Ownership](#20-lifecycle--ownership)
21. [Multi-Agent Access](#21-multi-agent-access)
22. [Rust API Reference](#22-rust-api-reference)
23. [Python API Reference](#23-python-api-reference)
24. [Migration](#24-migration)
25. [Implementation Phases](#25-implementation-phases)
26. [Comparison to Other Databases](#26-comparison-to-other-databases)

---

# 1. Design Principles

These principles govern every decision in this specification. When in doubt, refer here.

**P1. Two concepts, not three.** All data access goes through a Session (read scope) or a Transaction (write scope). The query language (Cypher vs Locy) is orthogonal to scoping. There is no separate LocySession, no LocyEngine accessor object, no third scoping mechanism.

**P2. Uni is lifecycle and admin.** The database handle opens, shuts down, manages schema, and stamps out Sessions. It does not execute queries or mutations.

**P3. Sessions are cheap, Transactions are explicit.** Session creation is synchronous, infallible, and zero-I/O. Transaction creation is async and allocates a private write buffer. Users pay only for what they use.

**P4. Commit-time serialization.** Transactions execute concurrently with private L0 buffers. The global writer lock is acquired only at commit time, held briefly for the merge, then released. No OCC conflict detection, no retry loops. Last committer wins for overlapping writes. The API does not preclude future OCC.

**P5. Locy is a peer of Cypher, not a subsystem.** `session.locy(program)` sits at the same level as `session.query(cypher)`. No accessor chains. The method name is the query language selector.

**P6. Computation is separate from materialization.** Locy DERIVE runs as pure computation against readable data, producing a `DerivedFactSet`. The expensive evaluation holds no locks. Materialization (writing derived facts) happens through a brief transaction commit. Heavy computation never blocks other writers.

**P7. Clean cut, no migration shims.** `db.query()`, `db.execute()`, and `db.begin()` are removed, not deprecated. The compiler tells users what to fix. The common single-statement write pattern is preserved as `session.execute()` (auto-committed).

**P8. Observability is not optional.** Every query returns metrics. Every session accumulates statistics. Every operation emits tracing spans. This is built in from day one, not bolted on later.

**P9. Consistent `_with` pattern.** Bare methods do the common thing: `query()`, `locy()`, `tx()`. The `_with` suffix gives the builder: `query_with()`, `locy_with()`, `tx_with()`. No exceptions.

**P10. Distributed-ready by design.** Public API types use `#[non_exhaustive]` enums for isolation levels, write leases, and capability flags. Application code written today compiles unchanged when new concurrency modes, coordination backends, or isolation levels ship. Internal evolution does not break external contracts.

---

# 2. Changes from Prior Concurrency Model

> **This section exists because the spec changes observable concurrency semantics.** Anyone familiar with the Black Book or the prior API must read this.

## 2.1 What Changed

The prior model (documented in the Black Book, Part XI) used **begin-time serialization**:

```
Old model:
  db.begin()     → acquires global writer lock
  tx.execute()   → writes under lock
  tx.commit()    → releases lock

  One writer at a time. No concurrent writes. No write-write conflicts possible.
```

This spec uses **commit-time serialization**:

```
New model:
  session.tx()   → allocates private L0 buffer, NO lock acquired
  tx.execute()   → writes to private L0, NO lock held
  tx.commit()    → acquires lock, merges L0, releases lock

  Multiple transactions execute concurrently. Commits serialize.
  Last committer wins for overlapping writes.
```

## 2.2 What Is Different for Users

| Behavior | Old Model | New Model |
|----------|-----------|-----------|
| Concurrent transactions | Not possible — `begin()` blocks | Allowed — each has private L0 |
| Write-write conflicts | Impossible by design | Resolved by last-writer-wins |
| Lock hold time | Entire transaction lifetime | Commit duration only (ms) |
| Long DERIVE blocks writers | Yes | No — no lock until commit |
| Conflict detection | Unnecessary | Not available (Phase 1). Opt-in OCC planned (Phase 3). |
| `CommitResult` version gap | N/A | Available — `started_at_version` vs `version` shows if others committed in between |

## 2.3 Why This Changed

The old model paid the cost of exclusive locking for the entire transaction lifetime. A 30-second Locy DERIVE evaluation blocked all other writers for 30 seconds. The new model moves the serialization point to commit, where the actual merge is sub-millisecond to a few milliseconds. This is strictly more concurrent — the old behavior (one writer at a time) is a special case of the new model where only one Session ever creates transactions.

## 2.4 Overlap Detection Without OCC

Until Phase 3 OCC ships, users who need to detect concurrent modifications can inspect `CommitResult`:

```rust
let result = tx.commit().await?;
if result.started_at_version != result.version - 1 {
    // Other transactions committed between our start and our commit.
    // For many workloads this is fine. For conflict-sensitive workloads,
    // the application can re-read and verify.
    tracing::info!(
        gap = result.version - result.started_at_version - 1,
        "Commits occurred during transaction lifetime"
    );
}
```

This is informational, not a guarantee of conflict-free execution. True conflict detection requires Phase 3 OCC.

## 2.5 Black Book Update Required

When this spec ships, the Black Book's Part XI ("Transactions, Sessions & Concurrency") must be updated to reflect commit-time serialization. The Design Principles section's "Single-Writer Simplicity" entry should be revised to "Single-Committer Simplicity" with a note that the serialization point moved from `begin()` to `commit()`.

---

# 3. The Model

```
┌──────────────────────────────────────────────────────────────┐
│                     Uni (database handle)                     │
│                                                              │
│  Lifecycle: open, shutdown                                   │
│  Admin: schema, snapshots, compaction, indexes               │
│  Factory: session(), session_template()                      │
│  Global rules: register_rules(), clear_rules()               │
│                                                              │
│  NO query(), begin(), or locy() here.                        │
└─────────────────────────┬────────────────────────────────────┘
                          │ session()
                          ▼
┌──────────────────────────────────────────────────────────────┐
│                    Session (read scope)                       │
│                                                              │
│  Owns: scoped params, private rule registry, plan cache      │
│  Reads: shared DB via read-committed (or pinned version)     │
│                                                              │
│  Cypher:  query(), query_with()                              │
│  Locy:    locy(), locy_with()  — ALL ops incl DERIVE, ASSUME│
│  Auto-tx: execute(), execute_with()  — single-shot writes   │
│  Rules:   register_rules(), clear_rules()                    │
│  Plans:   prepare(), prepare_locy()                          │
│  Intro:   explain(), explain_locy(), compile_locy()          │
│  Cursor:  query_cursor()                                     │
│  Observe: metrics(), watch(), watch_with()                   │
│  Info:    capabilities()                                     │
│  Factory: tx() → Transaction                                 │
│           bulk_writer() → BulkWriter                         │
│           appender() → StreamingAppender                     │
│                                                              │
│  Session never mutates the shared database directly.         │
│  execute() creates an internal auto-committed Transaction.   │
│  DERIVE returns a DerivedFactSet for explicit commit.        │
│  ASSUME uses a temporary eval-scoped buffer, then discards.  │
└─────────────────────────┬────────────────────────────────────┘
                          │ tx()
                          ▼
┌──────────────────────────────────────────────────────────────┐
│                  Transaction (write scope)                    │
│                                                              │
│  Owns: private L0 buffer (no lock until commit)              │
│  Inherits: Session's params, rules, plan cache               │
│  Reads: shared DB + Transaction's uncommitted writes         │
│  Writes: to private L0 (invisible to others until commit)    │
│                                                              │
│  Cypher:  query(), query_with(), execute(), execute_with()   │
│  Locy:    locy(), locy_with() — DERIVE auto-applies to L0   │
│  Apply:   apply(), apply_with() — from session-level DERIVE  │
│  Rules:   register_rules(), clear_rules()                    │
│  Plans:   prepare(), prepare_locy()                          │
│  Commit:  commit(), rollback(), is_dirty()                   │
│                                                              │
│  tx() = creates private L0, NO lock acquired                 │
│  commit() = acquires writer lock, WAL write, merge L0,       │
│             promote rules, release lock                      │
└──────────────────────────────────────────────────────────────┘
```

The rules are simple:
- **Need to read?** Use a Session.
- **Need a single-shot write?** `session.execute(cypher)` (auto-committed).
- **Need to compute derived facts?** Use `session.locy("DERIVE ...")` — no lock needed.
- **Need a multi-statement write?** Open a Transaction, make your mutations, commit.
- **Need to materialize derived facts?** `tx.apply(derived)` then `tx.commit()`.
- **Need high-throughput loading?** Use a BulkWriter or Appender within a Session.

---

# 4. Uni — The Database Handle

Uni is a lifecycle and administration handle. It opens the database, manages storage, and provides factories for Sessions and SessionTemplates. It does not execute queries or mutations.

## 4.1 API Surface

```rust
impl Uni {
    // ── Lifecycle ──
    fn open(uri: impl Into<String>) -> UniBuilder;
    fn open_existing(uri: impl Into<String>) -> UniBuilder;   // open only if exists
    fn create(uri: impl Into<String>) -> UniBuilder;          // create only (error if exists)
    fn temporary() -> UniBuilder;
    fn in_memory() -> UniBuilder;
    async fn shutdown(self) -> Result<()>;

    // ── Session Factories ──
    fn session(&self) -> Session;                            // sync, cheap, infallible
    fn session_template(&self) -> SessionTemplateBuilder;    // pre-configured factory

    // ── Schema DDL (admin — auto-committed, globally visible) ──
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

    // ── Global Locy Rules (cloned into every new Session) ──
    fn register_rules(&self, program: &str) -> Result<()>;
    fn clear_rules(&self);

    // ── Storage Admin ──
    async fn flush(&self) -> Result<()>;
    async fn compact_label(&self, label: &str) -> Result<CompactionStats>;
    async fn compact_edge_type(&self, edge_type: &str) -> Result<CompactionStats>;
    async fn wait_for_compaction(&self) -> Result<()>;

    // ── Snapshots ──
    async fn create_snapshot(&self, name: Option<&str>) -> Result<String>;
    async fn create_named_snapshot(&self, name: &str) -> Result<String>;  // persisted named snapshot
    async fn list_snapshots(&self) -> Result<Vec<SnapshotManifest>>;
    async fn restore_snapshot(&self, snapshot_id: &str) -> Result<()>;

    // ── Index Admin ──
    async fn rebuild_indexes(&self, label: &str, async_: bool) -> Result<Option<String>>;
    async fn index_rebuild_status(&self) -> Result<Vec<IndexRebuildTask>>;
    async fn retry_index_rebuilds(&self) -> Result<Vec<String>>;
    async fn is_index_building(&self, label: &str) -> Result<bool>;
    fn list_indexes(&self, label: &str) -> Vec<IndexDefinition>;
    fn list_all_indexes(&self) -> Vec<IndexDefinition>;

    // ── Xervo (ML Runtime) ──
    fn xervo(&self) -> UniXervo<'_>;

    // ── Multi-Agent Introspection ──
    fn write_lease(&self) -> Option<&WriteLease>;

    // ── Database Metrics ──
    fn metrics(&self) -> DatabaseMetrics;

    // ── Configuration ──
    fn config(&self) -> &UniConfig;
}
```

## 4.2 UniBuilder

```rust
impl UniBuilder {
    fn build(self) -> impl Future<Output = Result<Uni>>;

    // ── Configuration ──
    fn config(self, config: UniConfig) -> Self;
    fn cache_size(self, bytes: usize) -> Self;
    fn parallelism(self, n: usize) -> Self;
    fn cloud_config(self, config: CloudStorageConfig) -> Self;

    // ── Schema ──
    fn schema_file(self, path: impl AsRef<Path>) -> Self;

    // ── Xervo (ML Runtime) ──
    fn xervo_catalog(self, catalog: Vec<ModelAliasSpec>) -> Self;
    fn xervo_catalog_from_str(self, json: &str) -> Result<Self>;
    fn xervo_catalog_from_file(self, path: impl AsRef<Path>) -> Result<Self>;

    // ── Hybrid Storage ──
    fn hybrid(self, local_path: impl AsRef<Path>, remote_url: &str) -> Self;

    // ── Multi-agent modes (Phase 2) ──
    fn read_only(self) -> Self;
    fn write_lease(self, lease: WriteLease) -> Self;
}
```

## 4.3 What Lives on Uni

| Category | Rationale |
|----------|-----------|
| Lifecycle (open, shutdown) | Database-level, not session-scoped |
| Schema DDL | Structural metadata — auto-committed, globally visible |
| Schema inspection | Read-only metadata, not user data |
| Storage admin (flush, compact) | Operational concern, not data access |
| Snapshots | Database-level point-in-time management |
| Index admin | Operational concern |
| Global Locy rules | Default rules cloned into every new Session |
| Xervo | ML runtime, not data scoping |
| Database metrics | Global operational view |

## 4.4 What Is Removed from Uni

| Removed | New Location | Reason |
|---------|-------------|--------|
| `db.query()` | `session.query()` | Reads require a Session |
| `db.execute()` | `session.execute()` (auto-tx) or `tx.execute()` | Writes require a Session context |
| `db.query_with()` | `session.query_with()` | Reads require a Session |
| `db.query_cursor()` | `session.query_cursor()` | Reads require a Session |
| `db.explain()` | `session.explain()` | Read operation |
| `db.profile()` | `session.profile()` | Read operation |
| `db.begin()` | `session.tx()` | Transactions live within Sessions |
| `db.locy().evaluate()` | `session.locy()` | Locy reads require a Session |
| `db.locy().register()` | `db.register_rules()` (global) or `session.register_rules()` | Flattened |
| `db.bulk_writer()` | `session.bulk_writer()` | Moved to Session |

These methods are **removed, not deprecated**. No shims, no compatibility layer.

---

# 5. Session — The Read Scope

A Session is a long-lived, isolated read context. It holds scoped parameters, a private Locy rule registry, a plan cache, and provides factories for Transactions, BulkWriters, and Appenders. It also provides a convenience `execute()` method for single-shot writes via an internal auto-committed Transaction.

## 5.1 Struct Layout

```rust
pub struct Session {
    /// Shared database reference.
    db: Arc<UniInner>,

    /// Session identifier (UUID) for logging and diagnostics.
    id: String,

    /// Scoped query parameters (injected into every query).
    params: HashMap<String, Value>,

    /// Private Locy rule registry (cloned from global at creation).
    rule_registry: Arc<RwLock<LocyRuleRegistry>>,

    /// Transparent plan cache (keyed on query string hash).
    plan_cache: PlanCache,

    /// Schema version at cache population time (for invalidation).
    cached_schema_version: AtomicU64,

    /// Cancellation token (parent for all child operations).
    cancellation_token: CancellationToken,

    /// Hooks registered on this session.
    hooks: Vec<Box<dyn SessionHook>>,

    /// Accumulated session metrics.
    metrics: Arc<RwLock<SessionMetricsAccumulator>>,

    /// Commit notification broadcast sender.
    commit_tx: broadcast::Sender<CommitNotification>,

    /// Guards: tracks whether a Transaction or BulkWriter is active.
    active_write_guard: Arc<AtomicBool>,
}
```

A Session does **not** own a Writer or L0 buffer. It reads directly from the shared database (read-committed by default). Session creation is synchronous, infallible, and cheap — just Arc clones, a HashMap allocation, and a rule registry clone.

**Cheapness invariant:** Session creation involves cloning the global rule registry. This is O(n) in the number of registered rules. For typical workloads (tens to low hundreds of rules), this is sub-microsecond. If the global registry grows to thousands of rules, consider using `session_template()` which pre-compiles and caches the cloned form.

## 5.2 API Surface

```rust
impl Session {
    // ── Scoped Parameters ──

    /// Set a session-scoped parameter. Injected into every query.
    pub fn set<K: Into<String>, V: Into<Value>>(&mut self, key: K, value: V) -> &mut Self;

    /// Get a session parameter.
    pub fn get(&self, key: &str) -> Option<&Value>;

    /// Set multiple parameters from an iterator.
    pub fn set_all<I, K, V>(&mut self, params: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<Value>;

    // ── Version Pinning ──

    /// Pin this session to a specific snapshot version.
    /// All subsequent reads see data as of this version.
    /// Async because it validates the snapshot exists.
    pub async fn pin_to_version(&mut self, snapshot_id: &str) -> Result<()>;

    /// Pin this session to a specific timestamp.
    pub async fn pin_to_timestamp(&mut self, ts: DateTime<Utc>) -> Result<()>;

    /// Refresh: pick up the latest committed version from storage.
    /// Only meaningful for read-only instances in multi-agent mode.
    pub async fn refresh(&mut self) -> Result<()>;

    /// Whether this session is currently pinned to a specific version.
    pub fn is_pinned(&self) -> bool;

    // ── Cypher Reads ──

    /// Execute a read-only Cypher query.
    /// Errors if the query contains mutations (CREATE, SET, DELETE, MERGE).
    pub async fn query(&self, cypher: &str) -> Result<QueryResult>;

    /// Fluent query builder with parameter binding, timeout, cancellation.
    pub fn query_with(&self, cypher: &str) -> QueryBuilder<'_>;

    /// Streaming cursor for large result sets.
    pub async fn query_cursor(&self, cypher: &str) -> Result<QueryCursor>;

    // ── Auto-Committed Writes ──

    /// Execute a single Cypher mutation as an auto-committed transaction.
    ///
    /// Equivalent to:
    ///   let tx = self.tx().await?;
    ///   let result = tx.execute(cypher).await?;
    ///   tx.commit().await?;
    ///
    /// For multi-statement writes, use tx() instead.
    pub async fn execute(&self, cypher: &str) -> Result<AutoCommitResult>;

    /// Fluent auto-commit builder with parameter binding, timeout.
    pub fn execute_with(&self, cypher: &str) -> AutoCommitBuilder<'_>;

    // ── Locy (Full Evaluation — Queries, DERIVE, ASSUME) ──

    /// Evaluate a Locy program.
    ///
    /// All Locy operations are supported:
    /// - `?- query.` → results returned in LocyResult::rows
    /// - `DERIVE fact :- ...` → derived facts returned in LocyResult::derived
    ///   as a DerivedFactSet. Nothing is written to the shared database.
    ///   Use tx.apply(derived) to materialize.
    /// - `ASSUME { ... } ?- ...` → hypothetical mutations in a temporary
    ///   eval-scoped buffer, discarded after evaluation.
    ///
    /// The session never mutates the shared database. DERIVE computes
    /// but does not materialize. ASSUME is ephemeral.
    pub async fn locy(&self, program: &str) -> Result<LocyResult>;

    /// Fluent Locy builder with parameters, timeout, max_iterations.
    pub fn locy_with(&self, program: &str) -> LocyBuilder<'_>;

    // ── Rule Management ──
    //
    // Note: takes &self (not &mut self) because rules are behind Arc<RwLock<...>>.
    // This is intentional: rule registration is safe to call from shared references,
    // including from multiple threads holding the same Session via Arc<Session>.

    /// Register rules in the session's private registry.
    /// Visible to this Session and its Transactions.
    pub fn register_rules(&self, program: &str) -> Result<()>;

    /// Clear all rules in the session's registry.
    pub fn clear_rules(&self);

    // ── Planning & Introspection ──

    /// Explain a Cypher query plan without executing.
    pub async fn explain(&self, cypher: &str) -> Result<ExplainOutput>;

    /// Execute and profile a Cypher query.
    pub async fn profile(&self, cypher: &str) -> Result<(QueryResult, ProfileOutput)>;

    /// Fluent profile builder with parameter binding.
    pub fn profile_with(&self, cypher: &str) -> ProfileBuilder<'_>;

    /// Explain Locy rule evaluation strategy.
    /// Note: synchronous — compile-only, no I/O.
    pub fn explain_locy(&self, program: &str) -> Result<LocyExplainOutput>;

    /// Compile a Locy program without executing.
    pub fn compile_locy(&self, program: &str) -> Result<CompiledProgram>;

    // ── Prepared Statements ──

    /// Prepare a Cypher query. Parses, plans, and caches.
    pub async fn prepare(&self, cypher: &str) -> Result<PreparedQuery>;

    /// Prepare a Locy program. Compiles, stratifies, and caches.
    /// Note: synchronous — compile-only, no I/O.
    pub fn prepare_locy(&self, program: &str) -> Result<PreparedLocy>;

    // ── Transaction & Writer Factories ──

    /// Start a Transaction within this Session.
    /// Creates a private L0 buffer. Does NOT acquire the writer lock.
    /// The lock is acquired only at commit time.
    /// Errors if a Transaction, BulkWriter, or Appender is already active.
    pub async fn tx(&self) -> Result<Transaction>;

    /// Start a configured Transaction.
    pub fn tx_with(&self) -> TransactionBuilder<'_>;

    /// Create a BulkWriter within this Session.
    /// Errors if another write context is already active.
    pub fn bulk_writer(&self) -> BulkWriterBuilder<'_>;

    /// Create a streaming appender for a specific label.
    /// Errors if another write context is already active.
    pub fn appender(&self, label: &str) -> AppenderBuilder<'_>;

    // ── Admin Bulk Insert (convenience — bypasses BulkWriter, no batching/deferred indexes) ──

    /// Bulk insert vertices directly. For admin/benchmarking use.
    /// For production bulk loading, prefer `bulk_writer()` or `appender()`.
    pub async fn bulk_insert_vertices(&self, label: &str, properties_list: Vec<Properties>) -> Result<Vec<Vid>>;

    /// Bulk insert edges directly. For admin/benchmarking use.
    pub async fn bulk_insert_edges(&self, edge_type: &str, edges: Vec<(Vid, Vid, Properties)>) -> Result<()>;

    // ── Custom Functions ──

    /// Register a custom scalar function usable in Cypher queries.
    ///
    /// Uses `&self` (not `&mut self`) with interior mutability (`RwLock`-protected
    /// registry) to support `Arc<Session>` sharing in Python bindings and
    /// concurrent access patterns.
    pub fn register_function<F>(&self, name: &str, func: F) -> Result<()>
    where
        F: Fn(&[Value]) -> Result<Value> + Send + Sync + 'static;

    // ── Capabilities ──

    /// Runtime capability introspection.
    /// Allows application code to adapt to the current deployment mode
    /// without catching errors.
    pub fn capabilities(&self) -> SessionCapabilities;

    // ── Observability ──

    /// Session-level accumulated metrics.
    pub fn metrics(&self) -> SessionMetrics;

    /// Watch for commits visible to this session (basic).
    pub fn watch(&self) -> CommitStream;

    /// Watch with filtering and guardrails.
    pub fn watch_with(&self) -> WatchBuilder<'_>;

    // ── Hooks ──

    /// Register a hook that fires on query/commit events.
    pub fn add_hook(&mut self, hook: impl SessionHook + 'static);

    // ── Cancellation ──

    /// Cancel all active operations on this session.
    pub fn cancel(&self);

    /// Get the session's cancellation token for manual use.
    pub fn cancellation_token(&self) -> CancellationToken;

    // ── Lifecycle ──

    /// Session identifier (UUID).
    pub fn id(&self) -> &str;
}

impl Drop for Session {
    fn drop(&mut self) {
        // Lightweight — no I/O, no locks.
        // Cancels the session's CancellationToken (cascading to active operations).
        // Drops rule registry, plan cache, params.
    }
}
```

## 5.3 Enforcement: What "Read Scope" Means

The session never mutates the shared database. This is enforced as follows:

**Compile-time enforcement:** There is no `session.execute()` that writes directly. The `execute()` method creates an internal Transaction, executes the mutation, and auto-commits. The Session itself holds no write buffer.

**Runtime enforcement for Cypher:** `session.query()` validates the Cypher AST after parsing. If it contains `CREATE`, `SET`, `DELETE`, `MERGE`, or `REMOVE` clauses → `UniError::ReadOnly`. These are direct Cypher mutations with no deferred-commit equivalent.

**Locy: all operations allowed.** `session.locy()` allows all Locy operations including DERIVE and ASSUME. This does NOT violate the read-only contract because:
- **DERIVE** computes derived facts using an internal evaluation buffer, then returns them as a `DerivedFactSet`. Nothing is written to the shared database. The user materializes them via `tx.apply(derived)`.
- **ASSUME** uses a temporary buffer scoped to the evaluation. Hypothetical mutations exist only during evaluation and are discarded when it completes. No persistent output.
- **`?-` queries** are pure reads.

The session's contract is: the shared database state is never modified. Internal computation buffers used by the Locy engine during evaluation are not shared state. The `execute()` convenience method creates a Transaction internally — the Session itself never writes.

## 5.4 Auto-Committed Writes

For the common single-statement write pattern, `session.execute()` provides a convenience wrapper:

```rust
// These are equivalent:
session.execute("CREATE (:Person {name: 'Alice'})").await?;

// Longhand:
let tx = session.tx().await?;
tx.execute("CREATE (:Person {name: 'Alice'})").await?;
tx.commit().await?;
```

### AutoCommitResult

```rust
pub struct AutoCommitResult {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
    pub properties_removed: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
    pub version: u64,             // Database version after commit
    pub metrics: QueryMetrics,
}

impl AutoCommitResult {
    /// Total affected rows (nodes + relationships created/deleted + properties set/removed).
    pub fn affected_rows(&self) -> usize;
}
```

### AutoCommitBuilder

```rust
pub struct AutoCommitBuilder<'a> { session: &'a Session, cypher: &'a str, /* ... */ }

impl<'a> AutoCommitBuilder<'a> {
    pub fn param<K: Into<String>, V: Into<Value>>(self, key: K, value: V) -> Self;
    pub fn timeout(self, duration: Duration) -> Self;
    pub async fn run(self) -> Result<AutoCommitResult>;
}
```

## 5.5 SessionCapabilities

```rust
pub struct SessionCapabilities {
    /// Whether this session can create write transactions.
    /// False in read_only() mode.
    pub can_write: bool,

    /// Whether snapshot pinning is available.
    pub can_pin: bool,

    /// Current isolation level for new transactions.
    pub isolation: IsolationLevel,

    /// Active write lease summary (None for local single-process mode).
    /// Note: `WriteLeaseSummary` (not `WriteLease`) because the provider trait object is non-Clone.
    pub write_lease: Option<WriteLeaseSummary>,

    /// Whether commit notifications are available.
    pub has_notifications: bool,
}
```

This allows application code to introspect the runtime's deployment mode without catching errors. When multi-agent or OCC ships, `capabilities()` reflects the new state.

## 5.6 Builder Methods

### QueryBuilder

```rust
pub struct QueryBuilder<'a> { session: &'a Session, cypher: &'a str, /* ... */ }

impl<'a> QueryBuilder<'a> {
    pub fn param<K: Into<String>, V: Into<Value>>(self, key: K, value: V) -> Self;
    pub fn timeout(self, duration: Duration) -> Self;
    pub fn cancellation_token(self, token: CancellationToken) -> Self;
    pub async fn fetch_all(self) -> Result<QueryResult>;
    pub async fn fetch_one(self) -> Result<Option<Row>>;
    pub async fn cursor(self) -> Result<QueryCursor>;
}
```

### LocyBuilder

```rust
pub struct LocyBuilder<'a> { session: &'a Session, program: &'a str, /* ... */ }

impl<'a> LocyBuilder<'a> {
    pub fn param<K: Into<String>, V: Into<Value>>(self, key: K, value: V) -> Self;
    pub fn timeout(self, duration: Duration) -> Self;
    pub fn max_iterations(self, n: usize) -> Self;
    pub fn cancellation_token(self, token: CancellationToken) -> Self;
    pub async fn run(self) -> Result<LocyResult>;
}
```

### TransactionBuilder

```rust
pub struct TransactionBuilder<'a> { session: &'a Session, /* ... */ }

impl<'a> TransactionBuilder<'a> {
    pub fn timeout(self, duration: Duration) -> Self; // Wall-clock limit
    pub fn isolation(self, level: IsolationLevel) -> Self; // Default: Serialized
    pub async fn start(self) -> Result<Transaction>;
}
```

---

# 6. Transaction — The Write Scope

A Transaction is a short-lived, isolated write context within a Session. It owns a private L0 buffer. All Cypher mutations write to this buffer. DERIVE auto-applies derived facts to this buffer. On commit, the writer lock is acquired briefly, the buffer is merged to shared state, and the lock is released.

## 6.1 Struct Layout

```rust
pub struct Transaction {
    /// The Session this Transaction belongs to.
    session: Arc<Session>,

    /// Private Writer with own L0 buffer and AdjacencyManager.
    /// NOT protected by the global writer lock.
    /// The lock is only acquired during commit().
    tx_writer: Arc<RwLock<Writer>>,

    /// Rule registry (cloned from session at tx creation).
    /// Mutations via register_rules() promoted to session on commit.
    rule_registry: Arc<RwLock<LocyRuleRegistry>>,

    /// Transaction identifier (UUID).
    id: String,

    /// Whether commit or rollback has been called.
    completed: bool,

    /// Wall-clock deadline (None = no timeout).
    deadline: Option<Instant>,

    /// Child cancellation token (derived from session's token).
    cancellation_token: CancellationToken,

    /// Accumulated transaction metrics.
    metrics: TransactionMetricsAccumulator,

    /// Database version at transaction start (for staleness tracking and overlap detection).
    started_at_version: u64,
}
```

## 6.2 Key Design: No Lock Until Commit

`session.tx()` creates the private L0 buffer **without acquiring the global writer lock**. The transaction runs freely — `execute()`, `query()`, `locy()` all operate on the private L0 with no contention. The writer lock is acquired only during `commit()`, held for the brief duration of the WAL write + L0 merge, then released.

This means multiple Transactions from different Sessions can be active concurrently. They each write to their own private L0. Commits serialize — they queue up and merge one at a time.

```
Session A:  ──── tx.execute() ──── tx.execute() ──── tx.commit() ────
                  (private L0)      (private L0)      (lock, merge, unlock)

Session B:  ── tx.execute() ─── tx.locy() ────── tx.commit() ───────
                (private L0)     (private L0)      (waits for A's commit
                                                    then: lock, merge, unlock)
```

No transaction blocks another during execution. They only serialize at the commit point, and the serialization window is the time to merge a private L0 into the shared L0 (typically sub-millisecond to a few milliseconds).

**Last-writer-wins for overlapping writes.** If Transaction A and Transaction B both modify entity 42, whichever commits last overwrites the other's changes. There is no conflict detection in the default isolation mode. `CommitResult::started_at_version` lets users detect that concurrent commits occurred. See §2 for the full concurrency model change and §17.5 for future OCC opt-in.

## 6.3 API Surface

```rust
impl Transaction {
    // ── Cypher Reads (sees shared DB + uncommitted writes) ──

    pub async fn query(&self, cypher: &str) -> Result<QueryResult>;
    pub fn query_with(&self, cypher: &str) -> TxQueryBuilder<'_>;

    // ── Cypher Writes ──

    /// Execute a Cypher mutation. Writes to the Transaction's private L0.
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult>;

    /// Fluent mutation builder.
    pub fn execute_with(&self, cypher: &str) -> ExecuteBuilder<'_>;

    // ── Locy (Full Access — Auto-Applies to L0) ──

    /// Evaluate a Locy program with full write access.
    ///
    /// All Locy operations are supported, same as on Session.
    /// The difference: DERIVE auto-applies derived facts to the
    /// Transaction's private L0 instead of returning a DerivedFactSet.
    ///
    /// - `?- query.` → reads from shared DB + tx's uncommitted writes
    /// - `DERIVE fact :- ...` → derived facts written to tx's private L0
    /// - `ASSUME { ... } ?- ...` → temporary buffer, discarded after eval
    pub async fn locy(&self, program: &str) -> Result<LocyResult>;

    /// Fluent Locy builder.
    pub fn locy_with(&self, program: &str) -> TxLocyBuilder<'_>;

    // ── Apply DerivedFactSet (from session-level DERIVE) ──

    /// Apply a DerivedFactSet computed by a session-level DERIVE.
    /// Writes the derived facts to this Transaction's private L0.
    /// Logs an info-level message if the DerivedFactSet was evaluated
    /// against an older database version than the current one.
    pub async fn apply(&self, derived: DerivedFactSet) -> Result<ApplyResult>;

    /// Fluent apply builder with staleness policy.
    pub fn apply_with(&self, derived: DerivedFactSet) -> ApplyBuilder<'_>;

    // ── Rule Management ──

    /// Register rules. On commit, promoted to the owning Session's registry.
    pub fn register_rules(&self, program: &str) -> Result<()>;

    /// Clear transaction-local rules.
    pub fn clear_rules(&self);

    // ── Prepared Statements ──

    pub async fn prepare(&self, cypher: &str) -> Result<PreparedQuery>;
    /// Note: synchronous — compile-only, no I/O.
    pub fn prepare_locy(&self, program: &str) -> Result<PreparedLocy>;

    // ── Lifecycle ──

    /// Commit. Acquires the global writer lock, writes to WAL, merges
    /// private L0 into shared L0, promotes rules (best-effort), releases lock.
    /// Returns CommitResult with version, mutation count, timing, overlap info.
    pub async fn commit(mut self) -> Result<CommitResult>;

    /// Rollback. Discards all writes. No I/O. No lock needed.
    pub fn rollback(mut self);

    /// True if there are uncommitted mutations.
    pub fn is_dirty(&self) -> bool;

    /// Transaction identifier (UUID).
    pub fn id(&self) -> &str;

    /// Database version when this transaction was created.
    pub fn started_at_version(&self) -> u64;

    /// Cancel all active operations in this transaction.
    pub fn cancel(&self);

    /// Get the transaction's cancellation token for manual use.
    pub fn cancellation_token(&self) -> CancellationToken;
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.completed {
            if self.is_dirty() {
                tracing::warn!(
                    tx_id = %self.id,
                    "Transaction dropped with uncommitted writes — auto-rolling back"
                );
            }
            // Auto-rollback: discard private L0, clear active_write_guard.
            // No lock needed — rollback just drops the private buffer.
        }
    }
}
```

## 6.4 ExecuteBuilder

```rust
pub struct ExecuteBuilder<'a> { tx: &'a Transaction, cypher: &'a str, /* ... */ }

impl<'a> ExecuteBuilder<'a> {
    pub fn param<K: Into<String>, V: Into<Value>>(self, key: K, value: V) -> Self;
    pub fn timeout(self, duration: Duration) -> Self;
    pub async fn run(self) -> Result<ExecuteResult>;
}
```

## 6.5 ApplyBuilder — Staleness Policy for DerivedFactSets

```rust
pub struct ApplyBuilder<'a> { tx: &'a Transaction, derived: DerivedFactSet, /* ... */ }

impl<'a> ApplyBuilder<'a> {
    /// Require that the DerivedFactSet was evaluated against the current
    /// database version. Returns UniError::StaleDerivedFacts if version_gap > 0.
    pub fn require_fresh(self) -> Self;

    /// Allow up to `max` versions of gap. Error if exceeded.
    pub fn max_version_gap(self, max: u64) -> Self;

    /// Execute the apply with the configured staleness policy.
    pub async fn run(self) -> Result<ApplyResult>;
}
```

**Default behavior (via `tx.apply(derived)`):** Apply the facts, log an info-level message if stale, return `ApplyResult` with `version_gap`. No error for staleness.

**Strict behavior (via `tx.apply_with(derived).require_fresh().run()`):** Error if the database version has advanced since evaluation. Use for critical derivations where stale facts would be a correctness problem.

**Threshold behavior (via `tx.apply_with(derived).max_version_gap(5).run()`):** Allow small gaps (common in active systems), error on large gaps that suggest a fundamentally stale computation.

## 6.6 Result Types

### ExecuteResult

```rust
pub struct ExecuteResult {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
    pub metrics: QueryMetrics,
}
```

### CommitResult

```rust
pub struct CommitResult {
    pub mutations_committed: usize,
    pub rules_promoted: usize,
    pub version: u64,                // New database version after commit
    pub started_at_version: u64,     // Database version when tx was created
    pub wal_lsn: u64,               // WAL LSN of the commit
    pub duration: Duration,          // Time spent in commit (lock + WAL + merge)

    /// Non-empty if rule promotion encountered errors.
    /// The commit is still durable — only rule visibility may be affected.
    /// See §19.4 for rule promotion semantics.
    pub rule_promotion_errors: Vec<RulePromotionError>,
}

impl CommitResult {
    /// Number of versions that committed between this transaction's
    /// start and its commit. 0 means no concurrent commits occurred.
    pub fn version_gap(&self) -> u64 {
        self.version.saturating_sub(self.started_at_version + 1)
    }
}
```

### ApplyResult

```rust
pub struct ApplyResult {
    pub facts_applied: usize,
    pub version_gap: u64,       // 0 if evaluated_at == current version
}
```

### RulePromotionError

```rust
pub struct RulePromotionError {
    pub rule_text: String,
    pub error: String,
}
```

## 6.7 Timeout Enforcement

If a Transaction has a deadline (set via `session.tx_with().timeout(duration).start().await?`), every operation checks:

```rust
fn check_deadline(&self) -> Result<()> {
    if let Some(deadline) = self.deadline {
        if Instant::now() > deadline {
            return Err(UniError::TransactionExpired {
                tx_id: self.id.clone(),
                hint: "Transaction exceeded its timeout. All operations are rejected. \
                       The transaction will auto-rollback on drop.",
            });
        }
    }
    Ok(())
}
```

Operations that return `TransactionExpired`: `query`, `execute`, `locy`, `apply`, `commit`. The transaction is not auto-rolled-back on expiry — it stays in the expired state until the user drops it or calls `rollback()`. This lets users inspect `is_dirty()` and decide how to handle it.

## 6.8 Mutual Exclusion Within a Session

A Session enforces that at most one write context (Transaction, BulkWriter, or Appender) is active at a time on that session:

```rust
// In session.tx(), session.bulk_writer(), session.appender():
if self.active_write_guard.compare_exchange(false, true, ...).is_err() {
    return Err(UniError::WriteContextAlreadyActive {
        session_id: self.id.clone(),
        hint: "Only one Transaction, BulkWriter, or Appender can be active \
               per Session at a time. Commit or rollback the active one first, \
               or create a separate Session with db.session() for concurrent writes.",
    });
}

// On Transaction/BulkWriter/Appender drop:
self.session.active_write_guard.store(false, ...);
```

**Panic safety note:** The `active_write_guard` is cleared in `Drop` implementations. If a panic occurs between the `compare_exchange` and the construction of the RAII guard, the flag could remain set. Implementations should use a scopeguard pattern to ensure the flag is always cleared:

```rust
let _guard = scopeguard::guard((), |_| {
    self.active_write_guard.store(false, Ordering::Release);
});
// ... construct Transaction ...
std::mem::forget(_guard); // Transaction's Drop will clear the flag instead
```

Note: this is per-session mutual exclusion. Different sessions CAN have concurrent Transactions — they run independently and only serialize at commit time.

---

# 7. How Locy Fits In

Locy is not a separate scoping system. It follows the same Session/Transaction structure as Cypher, with one key insight: Locy evaluation is computation, not mutation.

## 7.1 The Locy Evaluation Engine

Every Locy evaluation — whether on a Session or Transaction — uses the same internal engine. The engine maintains an evaluation buffer where derived facts accumulate during semi-naive fixpoint iteration. Facts derived in iteration N are visible to iteration N+1. This buffer is internal to the evaluation and is not the session's state or the transaction's L0.

## 7.2 DERIVE: Computation vs Materialization

DERIVE is structurally a two-phase operation:

**Phase 1: Evaluation.** Read the graph, apply rules recursively, iterate to fixpoint. This is pure computation over reads. The evaluation engine uses its internal buffer for intermediate results.

**Phase 2: Materialization.** Take the derived facts and make them persistent graph data.

These phases have very different performance profiles. A DERIVE that scans 10M nodes to find 500 high-risk accounts spends 99.99% of time in Phase 1 (reads) and 0.01% in Phase 2 (writes).

### DERIVE on Session

Both phases run, but Phase 2 produces a `DerivedFactSet` instead of writing to the database. The user materializes explicitly:

```rust
let result = session.locy("DERIVE high_risk(A) :- ...").await?;
// Phase 1: 30 seconds of computation, no locks held
// Phase 2: derived facts packaged as DerivedFactSet

if let Some(derived) = result.derived {
    let tx = session.tx().await?;

    // Default: apply and log if stale
    tx.apply(derived).await?;

    // Or with staleness policy:
    // tx.apply_with(derived).require_fresh().run().await?;
    // tx.apply_with(derived).max_version_gap(3).run().await?;

    tx.commit().await?;
}
```

### DERIVE on Transaction

Both phases run, and Phase 2 auto-applies to the transaction's private L0:

```rust
let tx = session.tx().await?;
tx.locy("DERIVE high_risk(A) :- ...").await?;
// Phase 1: 30 seconds of computation (no lock held — tx has no lock)
// Phase 2: derived facts auto-applied to tx's private L0
tx.commit().await?;  // Lock acquired here, held briefly for merge
```

In both cases, no lock is held during the expensive evaluation phase. The only difference is whether the user explicitly applies the results (session) or they auto-apply (transaction).

### When to Use Which

**Use session-level DERIVE when:**
- The evaluation is expensive and you don't want to keep a Transaction open
- You want to inspect the derived facts before committing
- You want to combine derived facts from multiple DERIVE evaluations into a single commit
- The DERIVE doesn't need to see uncommitted writes
- You need staleness control (e.g., `require_fresh()`)

**Use transaction-level DERIVE when:**
- The DERIVE needs to see uncommitted writes in the same transaction
- The evaluation is lightweight and the convenience of auto-apply outweighs any benefit of separation
- You're doing a mixed workflow (Cypher writes + DERIVE) that should be atomic

## 7.3 ASSUME: Always Ephemeral

ASSUME creates hypothetical mutations, evaluates queries against them, then discards the mutations:

```
ASSUME { CREATE (:City {name: 'Atlantis'})-[:ROAD]->(:City {name: 'Athens'}) }
?- path('Atlantis', X).
```

ASSUME works identically on Session and Transaction. The hypothetical mutations go into a temporary buffer scoped to the evaluation. When the ASSUME block exits, the buffer is discarded. ASSUME mutations never reach the session's state, the transaction's L0, or the shared database. They are purely internal to the evaluation.

The only difference: on a Transaction, the ASSUME evaluation can see the transaction's uncommitted writes in addition to the hypothetical mutations and the shared database.

## 7.4 LocyResult

```rust
pub struct LocyResult {
    /// Query results (for ?- queries).
    pub rows: Vec<Row>,

    /// Derived facts pending materialization (for DERIVE programs).
    /// Present when evaluated on a Session. None when evaluated on
    /// a Transaction (facts are auto-applied to the tx's L0).
    pub derived: Option<DerivedFactSet>,

    /// Evaluation statistics.
    pub stats: LocyStats,

    /// Query metrics.
    pub metrics: QueryMetrics,
}
```

## 7.5 DerivedFactSet

```rust
pub struct DerivedFactSet {
    /// Derived vertices to create, keyed by label.
    pub vertices: HashMap<String, Vec<Properties>>,

    /// Derived edges to create.
    pub edges: Vec<DerivedEdge>,

    /// Evaluation statistics.
    pub stats: LocyStats,

    /// Database version this was evaluated against.
    pub evaluated_at_version: u64,
}

impl DerivedFactSet {
    /// Total number of facts (vertices + edges) in the set.
    pub fn fact_count(&self) -> usize;

    /// True if no facts were derived.
    pub fn is_empty(&self) -> bool;
}
```

A DerivedFactSet is pure data. No side effects, no locks, no references to database internals. It can be inspected, logged, serialized, discarded, or applied to a transaction.

## 7.6 Complete Operation Reference

| Locy Operation | Session | Transaction | Behavior |
|---|---|---|---|
| `?- query.` | Yes | Yes | Pure read. Tx also sees uncommitted writes. |
| `DERIVE fact :- ...` | Yes | Yes | Same evaluation engine. Session: returns DerivedFactSet. Transaction: auto-applies to L0. |
| `ASSUME { ... } ?- ...` | Yes | Yes | Temporary eval-scoped buffer. Discarded after evaluation. No persistent output. |
| `register_rules(prog)` | Yes | Yes | Modifies rule registry. Tx rules promoted to session on commit (best-effort). |
| `clear_rules()` | Yes | Yes | Clears registry at the respective scope. |
| `compile_locy(prog)` | Yes | Yes | Pure CPU — no I/O, no isolation concern. |
| `explain_locy(prog)` | Yes | Yes | Read-only introspection. |

---

# 8. BulkWriter — High-Throughput Writes

BulkWriter lives on Session as a peer of Transaction. It bypasses normal isolation for performance: deferred indexes, direct L0 writes, optional WAL bypass.

> **Consistency class: at-least-once, no rollback.** BulkWriter flushes directly to storage in large batches for throughput. Already-flushed batches **cannot be rolled back**. If `commit()` fails partway through, partial data may be visible. Use `abort()` to stop further writes, but already-flushed data persists. For atomicity guarantees, use Transaction instead. BulkWriter and Transaction have fundamentally different consistency properties — the shared API surface (`session.bulk_writer()` vs `session.tx()`) should not be read as implying equivalent guarantees.

## 8.1 API

```rust
impl Session {
    pub fn bulk_writer(&self) -> BulkWriterBuilder<'_>;
}

pub struct BulkWriterBuilder<'a> { /* ... */ }

impl<'a> BulkWriterBuilder<'a> {
    pub fn batch_size(self, n: usize) -> Self;                // Default: 10,000
    pub fn max_buffer_size_bytes(self, n: usize) -> Self;     // Default: 1 GB
    pub fn defer_vector_indexes(self, defer: bool) -> Self;   // Default: true
    pub fn defer_scalar_indexes(self, defer: bool) -> Self;   // Default: false
    pub fn on_progress<F: Fn(BulkProgress) + Send + 'static>(self, f: F) -> Self;
    pub fn async_indexes(self, async_: bool) -> Self;         // Rebuild indexes asynchronously
    pub fn validate_constraints(self, validate: bool) -> Self; // Default: true
    /// Note: synchronous — no I/O in build; guard acquisition only.
    pub fn build(self) -> Result<BulkWriter>;
}

impl BulkWriter {
    pub async fn insert_vertices(&mut self, label: &str, data: impl IntoArrow) -> Result<()>;
    pub async fn insert_edges(&mut self, edge_type: &str, data: impl IntoArrow) -> Result<()>;
    pub async fn commit(self) -> Result<BulkStats>;
    /// Note: returns Result because abort performs real I/O (LanceDB version rollback).
    pub async fn abort(self) -> Result<()>;
    pub fn stats(&self) -> &BulkStats;
}
```

## 8.2 Why Not in Transaction

BulkWriter and Transaction have incompatible semantics. A Transaction provides isolation (private L0, invisible until commit) and WAL-based atomicity. A BulkWriter provides throughput by bypassing all of that — flushing directly to storage in large batches. Rolling back a BulkWriter after flushing millions of rows to Lance tables is not possible through normal rollback. The abstraction would leak.

BulkWriter inherits session params (useful for multi-tenant bulk loads) but does not inherit transaction semantics.

---

# 9. Streaming Appender

> **Consistency class: at-least-once, no rollback.** Same as BulkWriter — the Appender wraps BulkWriter internally. Already-flushed batches cannot be rolled back. For atomicity guarantees, use Transaction instead.

## 9.1 API

```rust
impl Session {
    pub fn appender(&self, label: &str) -> AppenderBuilder<'_>;
}

pub struct AppenderBuilder<'a> { /* ... */ }

impl<'a> AppenderBuilder<'a> {
    pub fn batch_size(self, n: usize) -> Self;                // Auto-flush threshold
    pub fn defer_vector_indexes(self, defer: bool) -> Self;   // Default: true
    pub fn max_buffer_size_bytes(self, size: usize) -> Self;  // Memory limit
    /// Note: synchronous — no I/O in build; guard acquisition only.
    pub fn build(self) -> Result<StreamingAppender>;
}

pub struct StreamingAppender { /* wraps BulkWriter internally */ }

impl StreamingAppender {
    /// Append a single row. Auto-flushes at batch_size.
    pub async fn append(&mut self, properties: impl Into<Properties>) -> Result<()>;

    /// Append an Arrow RecordBatch.
    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;

    /// Flush remaining buffered rows and finalize.
    pub async fn finish(self) -> Result<BulkStats>;

    /// Abort without flushing remaining rows.
    /// Note: synchronous — only discards in-memory buffers, no I/O.
    pub fn abort(self);
}
```

## 9.2 Semantics

The Appender is a thin wrapper around BulkWriter that accumulates rows and auto-flushes at the configured batch size. It shares the mutual exclusion constraint: only one write context per Session.

Like BulkWriter, there is no rollback for already-flushed batches.

---

# 10. Prepared Statements & Plan Cache

## 10.1 Transparent Plan Cache

Every Session maintains an internal plan cache keyed on query string hash. This is invisible to users.

```rust
struct PlanCache {
    cache: HashMap<u64, CachedPlan>,     // hash(query_string) → plan
    schema_version: u64,                  // Invalidation marker
    max_entries: usize,                   // Default: 1000, configurable
}

struct CachedPlan {
    physical_plan: Arc<dyn ExecutionPlan>,
    created_at: Instant,
    hit_count: AtomicU64,
}
```

**Invalidation:** `db.schema().apply()` bumps the global schema version. On each query, the session checks its cached version. Mismatch → flush entire cache. O(1) per query.

**Behavior:**
- First call: parse, plan, cache, execute.
- Second call with same query string: bind params, execute. No re-parse, no re-plan.
- Schema change: next query flushes cache, re-plans.

## 10.2 Explicit Prepared Statements

```rust
pub struct PreparedQuery {
    plan: Arc<dyn ExecutionPlan>,
    param_names: Vec<String>,
    query_text: String,
}

impl PreparedQuery {
    pub async fn execute(&self, params: &[(&str, Value)]) -> Result<QueryResult>;
    pub fn bind(&self) -> PreparedQueryBinder<'_>;
}

pub struct PreparedLocy {
    compiled: CompiledProgram,
    param_names: Vec<String>,
    program_text: String,
}

impl PreparedLocy {
    pub async fn execute(&self, params: &[(&str, Value)]) -> Result<LocyResult>;
    pub fn bind(&self) -> PreparedLocyBinder<'_>;
}
```

Prepared statements auto-replan transparently if the schema changes between calls. The user sees no error — just a slightly slower first call after a schema change.

---

# 11. Query Cancellation

## 11.1 Architecture

```
Session
 └── CancellationToken (parent)
      ├── query() → child token → DataFusion executor
      ├── locy() → child token → Locy fixpoint loop
      └── Transaction → child token
           ├── query() → grandchild → DataFusion
           ├── execute() → grandchild → DataFusion
           └── locy() → grandchild → Locy fixpoint
```

Cancelling a parent cascades to all children. Cancelling a child does not affect siblings.

## 11.2 Cancellation Points

Cooperative cancellation. The runtime checks `token.is_cancelled()` at natural yield points:

| Component | Cancellation Point |
|-----------|-------------------|
| DataFusion physical operators | Between `poll_next()` calls on RecordBatchStream |
| Locy fixpoint loop | Between semi-naive iterations |
| Graph algorithms | Between iteration steps (PageRank rounds, BFS levels) |
| L0/L1 scans | Between page reads from Lance |
| WAL writes | NOT cancellable (must complete for consistency) |
| Commit merge | NOT cancellable (must complete for consistency) |

Cancelled operations return `UniError::Cancelled`. The session/transaction remains valid.

## 11.3 API

```rust
// Session-level: cancel all active operations
session.cancel();

// Per-query: cancel a specific operation
let token = CancellationToken::new();
session.query_with("MATCH (n)-[*1..20]->(m) RETURN n, m")
    .cancellation_token(token.clone())
    .fetch_all().await;
token.cancel();

// Transaction-level: cancel active operations (tx remains open)
tx.cancel();
```

## 11.4 Python Integration

```python
# Sync: KeyboardInterrupt cancels the active query
try:
    result = session.query("MATCH (n)-[*1..20]->(m) RETURN n, m")
except KeyboardInterrupt:
    pass  # Query cancelled, session still usable

# Async: asyncio task cancellation
task = asyncio.create_task(session.query("..."))
task.cancel()
```

PyO3 bindings call `PyErr_CheckSignals()` periodically during long-running operations.

---

# 12. Timeouts

## 12.1 Query Timeouts

```rust
session.query_with("MATCH (n)-[*1..20]->(m) RETURN n")
    .timeout(Duration::from_secs(5))
    .fetch_all().await?;
// Returns UniError::QueryTimeout after 5 seconds
```

Implemented as `tokio::time::timeout` wrapping the query future. On expiry, the cancellation token is triggered.

## 12.2 Transaction Timeouts

Wall-clock limit on transaction lifetime:

```rust
let tx = session.tx_with()
    .timeout(Duration::from_secs(30))
    .start().await?;
// After 30 seconds, all tx operations return UniError::TransactionExpired
```

**Default:** 60 seconds, configurable via `UniConfig::default_transaction_timeout`. Set to `None` to disable.

**On expiry:** `query`, `execute`, `locy`, `apply`, `commit` → `UniError::TransactionExpired`. `rollback()` always succeeds.

## 12.3 Locy-Specific Limits

```rust
session.locy_with("?- path($src, X).")
    .timeout(Duration::from_secs(10))
    .max_iterations(500)
    .run().await?;
```

---

# 13. Metrics & Observability

## 13.1 Per-Query Metrics

Every query result includes metrics:

```rust
pub struct QueryMetrics {
    pub parse_time: Duration,
    pub plan_time: Duration,
    pub exec_time: Duration,
    pub total_time: Duration,
    pub rows_scanned: usize,
    pub rows_returned: usize,
    pub bytes_read: usize,
    pub plan_cache_hit: bool,
    pub l0_reads: usize,
    pub storage_reads: usize,
    pub cache_hits: usize,
}
```

Accessible on `QueryResult::metrics()`, `LocyResult::metrics`, `ExecuteResult::metrics`.

## 13.2 Session Metrics

```rust
pub struct SessionMetrics {
    pub session_id: String,
    pub active_since: Instant,
    pub queries_executed: u64,
    pub locy_evaluations: u64,
    pub total_query_time: Duration,
    pub total_rows_scanned: u64,
    pub total_rows_returned: u64,
    pub transactions_committed: u64,
    pub transactions_rolled_back: u64,
    pub plan_cache_hits: u64,
    pub plan_cache_misses: u64,
    pub plan_cache_size: usize,
}
```

## 13.3 Database Metrics

```rust
pub struct DatabaseMetrics {
    pub active_sessions: usize,
    pub l0_mutation_count: usize,
    pub l0_estimated_size_bytes: usize,
    pub l1_run_count: usize,
    pub write_throttle_pressure: ThrottlePressure,
    pub compaction_status: CompactionStatus,
    pub wal_size_bytes: u64,
    pub wal_lsn: u64,
    pub schema_version: u64,
    pub uptime: Duration,
    pub total_queries: u64,
    pub total_commits: u64,
}
```

## 13.4 Tracing Integration

Every public method emits a `tracing` span:

```rust
#[instrument(
    skip(self),
    fields(session_id = %self.id, query_type = "cypher", otel.kind = "client")
)]
pub async fn query(&self, cypher: &str) -> Result<QueryResult> { ... }
```

Users who add a tracing subscriber get full observability with no additional integration.

---

# 14. Commit Notifications

## 14.1 API

```rust
impl Session {
    /// Basic watcher — receives all commits.
    pub fn watch(&self) -> CommitStream;

    /// Filtered watcher with guardrails.
    pub fn watch_with(&self) -> WatchBuilder<'_>;
}

pub struct WatchBuilder<'a> { /* ... */ }

impl<'a> WatchBuilder<'a> {
    /// Only notify for commits affecting these labels.
    pub fn labels(self, labels: &[&str]) -> Self;

    /// Only notify for commits affecting these edge types.
    pub fn edge_types(self, types: &[&str]) -> Self;

    /// Debounce notifications — at most one per interval.
    pub fn debounce(self, interval: Duration) -> Self;

    /// Exclude commits from a specific session (for self-loop prevention).
    pub fn exclude_session(self, session_id: &str) -> Self;

    /// Build the filtered stream.
    pub fn build(self) -> CommitStream;
}

pub struct CommitStream { /* broadcast::Receiver with optional filter */ }

impl CommitStream {
    pub async fn next(&mut self) -> Option<CommitNotification>;
}

pub struct CommitNotification {
    pub version: u64,
    pub mutation_count: usize,
    pub labels_affected: Vec<String>,
    pub edge_types_affected: Vec<String>,
    pub rules_promoted: usize,
    pub timestamp: DateTime<Utc>,
    pub tx_id: String,
    pub session_id: String,          // NEW: for self-loop filtering
    pub causal_version: u64,         // NEW: version the committing tx started from
}
```

## 14.2 Implementation

On every successful `tx.commit()`, the commit result is broadcast to all watchers sharing the same database. Intra-process only for Phase 1. Multi-agent notifications are Phase 2.

## 14.3 Reactive DERIVE — With Guardrails

Commit-triggered reactive DERIVE is a powerful pattern but has inherent risks. This section specifies the guardrails.

**Risk 1: Self-triggering loops.** A watcher that re-derives on every commit will trigger itself if the DERIVE produces a commit.

**Mitigation:** Use `watch_with().exclude_session(session.id())` to ignore commits from the watcher's own session:

```rust
let session = db.session();
session.register_rules("high_risk(A) :- ...")?;

let mut watcher = session.watch_with()
    .labels(&["Transaction"])
    .exclude_session(session.id())    // Don't react to own commits
    .debounce(Duration::from_secs(1)) // At most once per second
    .build();

tokio::spawn(async move {
    while let Some(commit) = watcher.next().await {
        let derived = session.locy("DERIVE high_risk(A) :- ...").await.unwrap();
        if let Some(facts) = derived.derived {
            if !facts.is_empty() {
                let tx = session.tx().await.unwrap();
                tx.apply(facts).await.unwrap();
                tx.commit().await.unwrap();
            }
        }
    }
});
```

**Risk 2: Thundering herd.** Multiple watchers reacting to the same commit can amplify load.

**Mitigation:** Use `debounce()` to coalesce bursts. Application-level coordination (e.g., single-watcher pattern, leader election) is the user's responsibility. Uni provides the building blocks.

**Risk 3: Stale re-derivation.** The watcher may re-derive against data that has already changed again.

**Mitigation:** Use `causal_version` to detect if the triggering commit is still the latest. Use `apply_with().max_version_gap()` to reject stale results.

**Invariant:** Commit notifications do not carry causal ordering guarantees beyond `causal_version`. Users are responsible for implementing idempotence and cycle-breaking logic in their reactive handlers.

---

# 15. Session Hooks

## 15.1 API

```rust
pub trait SessionHook: Send + Sync {
    fn before_query(&self, ctx: &HookContext) -> Result<()> { Ok(()) }
    fn after_query(&self, ctx: &HookContext, metrics: &QueryMetrics) {}
    fn before_commit(&self, ctx: &CommitHookContext) -> Result<()> { Ok(()) }
    fn after_commit(&self, ctx: &CommitHookContext, result: &CommitResult) {}
}

pub struct HookContext {
    pub session_id: String,
    pub query_text: String,
    pub query_type: QueryType,    // Cypher, Locy, Execute
    pub params: HashMap<String, Value>,
}

pub struct CommitHookContext {
    pub session_id: String,
    pub tx_id: String,
    pub mutation_count: usize,
}
```

Use cases: authorization, audit logging, metrics collection, query rewriting.

**Failure semantics:**
- `before_query` returns `Err` → query is not executed, error propagated to caller.
- `after_query` is infallible (no return value) — panics are caught and logged.
- `before_commit` returns `Err` → commit is aborted (no lock acquired), error propagated.
- `after_commit` is infallible — the commit is already durable. Panics are caught and logged.

---

# 16. Session Templates

## 16.1 API

```rust
impl Uni {
    pub fn session_template(&self) -> SessionTemplateBuilder;
}

impl SessionTemplateBuilder {
    pub fn param<K: Into<String>, V: Into<Value>>(self, key: K, value: V) -> Self;
    pub fn rules(self, program: &str) -> Result<Self, LocyCompileError>;
    pub fn hook(self, hook: impl SessionHook + 'static) -> Self;
    pub fn query_timeout(self, duration: Duration) -> Self;
    pub fn transaction_timeout(self, duration: Duration) -> Self;
    pub fn build(self) -> Result<SessionTemplate>;
}

impl SessionTemplate {
    /// Create a new Session from this template. Sync, cheap.
    /// Rules are pre-compiled — only the compiled form is cloned.
    pub fn create(&self) -> Session;
}
```

## 16.2 Use Case

```rust
// At startup (once):
let template = db.session_template()
    .param("tenant_id", tenant)
    .rules("path(X,Y) :- edge(X,Y). path(X,Z) :- edge(X,Y), path(Y,Z).")?
    .hook(Box::new(AuditHook::new()))
    .transaction_timeout(Duration::from_secs(30))
    .build()?;

// Per request (cheap):
let session = template.create();
let result = session.locy("?- path($start, X).").await?;
```

---

# 17. Concurrency Model

## 17.1 Decision: Commit-Time Serialization

Uni uses a **concurrent-execution, serialized-commit** model:

- **Multiple transactions execute concurrently.** `session.tx()` creates a private L0 buffer with no lock. Multiple transactions from different sessions run in parallel, each writing to their own private L0.
- **Commits serialize.** `tx.commit()` acquires the global writer lock, merges the private L0 into the shared L0, writes to WAL, then releases the lock. One commit at a time.
- **Many concurrent readers.** Sessions read from the shared database without locks. Readers never block writers; writers never block readers.
- **Auto-rollback on drop.** If a Transaction is dropped without commit or rollback, it auto-rolls back.

> **This is a change from the prior model.** See §2 for the full comparison and migration notes.

## 17.2 Why This Model

**Compared to begin-time locking (old Uni model, SQLite):** Transactions don't block each other during execution. A 30-second Locy evaluation in Transaction A doesn't prevent Transaction B from doing work. Only the brief commit (milliseconds) serializes.

**Compared to full OCC:** No conflict detection, no retry loops, no write-set tracking. Simpler to implement. Last-writer-wins for overlapping entity modifications, which matches the behavior users already expect from concurrent `db.execute()` calls.

**Compared to pessimistic locking (Neo4j model):** No deadlocks, no lock ordering requirements, no per-entity lock management. The global writer lock at commit time is a single, simple synchronization point.

## 17.3 Writer Lock Behavior

```rust
// tx.commit() internally:
async fn commit(mut self) -> Result<CommitResult> {
    self.check_deadline()?;
    self.run_before_commit_hooks()?;

    // Acquire the global writer lock with timeout
    let _guard = tokio::time::timeout(
        self.session.db.config.commit_lock_timeout,  // Default: 5 seconds
        self.session.db.writer_lock.lock()
    ).await
    .map_err(|_| UniError::CommitTimeout {
        tx_id: self.id.clone(),
        hint: "Another commit is in progress and taking longer than expected. \
               Your transaction is still active — you can retry commit().",
    })?;

    // Now exclusively holding the writer lock.
    // WAL write, L0 merge, adjacency replay all happen here.
    let result = self.do_merge(&_guard).await?;

    // Lock released when _guard drops (end of scope).

    self.promote_rules_best_effort(&mut result)?;
    self.broadcast_commit_notification(&result);
    self.run_after_commit_hooks(&result);

    self.completed = true;
    Ok(result)
}
```

**Commit lock timeout:** If another commit is in progress, `commit()` waits up to 5 seconds (configurable). On timeout, returns `UniError::CommitTimeout`. The transaction is NOT rolled back — the user can retry `commit()` or choose to rollback.

## 17.4 Last-Writer-Wins Semantics

If Transaction A and Transaction B both modify entity 42:

```
Tx A: execute("MATCH (n {id: 42}) SET n.status = 'hot'")
Tx B: execute("MATCH (n {id: 42}) SET n.status = 'cold'")

Tx A commits first → n.status = 'hot'
Tx B commits second → n.status = 'cold' (overwrites A's value)
```

No conflict error. No retry needed. The last commit wins. This is the simplest model that provides meaningful concurrency.

Users can detect overlapping commits via `CommitResult::version_gap()`:

```rust
let result = tx.commit().await?;
if result.version_gap() > 0 {
    tracing::info!(
        gap = result.version_gap(),
        "Concurrent commits detected during transaction lifetime"
    );
}
```

## 17.5 Isolation Levels — Extension Point

```rust
/// Isolation level for transactions. Non-exhaustive: new levels
/// will be added in future releases without breaking existing code.
#[non_exhaustive]
pub enum IsolationLevel {
    /// Default. Concurrent execution, commit-time serialization, last-writer-wins.
    /// No conflict detection. CommitResult::version_gap() provides
    /// informational overlap detection.
    Serialized,

    // ── Future (Phase 3+) ──

    // /// Optimistic concurrency control. Commit checks the write-set
    // /// against concurrent commits. Returns UniError::TransactionConflict
    // /// if any entity in this transaction's write-set was modified since
    // /// this transaction started.
    // Optimistic,
}
```

Application code written today against `IsolationLevel::Serialized` compiles unchanged when `Optimistic` ships:

```rust
// Works today:
let tx = session.tx_with()
    .isolation(IsolationLevel::Serialized)
    .start().await?;

// Works in Phase 3 (additive, no existing code breaks):
let tx = session.tx_with()
    .isolation(IsolationLevel::Optimistic)
    .start().await?;
```

---

# 18. Isolation Model

## 18.1 Session Reads: Read-Committed

A Session reads the latest committed state. If another Transaction commits between two queries, the second query sees the committed data.

```
Session A:                          Transaction B (different session):
  session.query("MATCH (n) ...")
    → sees 10 rows
                                      tx.execute("CREATE (:New)").await?;
                                      tx.commit().await?;
  session.query("MATCH (n) ...")
    → sees 11 rows                  ← read-committed: sees B's commit
```

## 18.2 Session Reads: Pinned Version

With `session.pin_to_version(id)` or `session.pin_to_timestamp(ts)`:

```
Session A (pinned to v5):           Transaction B:
  session.query("...")
    → sees data as of v5
                                      tx.commit().await?;  // creates v6
  session.query("...")
    → still sees data as of v5     ← pinned: does NOT see B's commit
  session.refresh().await?;         // now sees v6
```

This is orthogonal to `VERSION AS OF` / `TIMESTAMP AS OF` in Cypher (per-query time travel). Session pinning provides per-session consistency.

## 18.3 Transaction Writes: Private L0

All mutations within a Transaction write to its private L0 buffer. Other Sessions and Transactions cannot see these writes until commit.

## 18.4 Transaction Reads: Private + Shared

A Transaction sees its own uncommitted writes overlaid on the shared database:

```rust
let tx = session.tx().await?;
tx.execute("CREATE (:Foo {id: 1})").await?;

// Sees the uncommitted :Foo node
let result = tx.query("MATCH (n:Foo) RETURN n").await?;
assert_eq!(result.len(), 1);

// Another session does NOT see it
let other = db.session();
let result = other.query("MATCH (n:Foo) RETURN n").await?;
assert_eq!(result.len(), 0);
```

## 18.5 Cross-Transaction Visibility

Because transactions don't hold locks during execution, concurrent transactions do NOT see each other's uncommitted writes:

```
Tx A (Session 1):                    Tx B (Session 2):
  tx.execute("CREATE (:Secret)")
                                      tx.query("MATCH (n:Secret) RETURN n")
                                        → 0 rows (A's writes invisible)
  tx.commit()
                                      tx.query("MATCH (n:Secret) RETURN n")
                                        → 1 row (A committed, B sees it
                                                  because B reads shared DB
                                                  at read-committed)
```

---

# 19. Commit Protocol

## 19.1 Steps

```
tx.commit()
  │
  ├─ 1. Check transaction not expired (deadline check)
  │
  ├─ 2. Run before_commit hooks (can reject)
  │
  ├─ 3. Acquire global writer lock (with timeout)
  │     ── blocks if another commit is in progress ──
  │
  ├─ 4. Write transaction L0 mutations to WAL
  │
  ├─ 5. Flush WAL ← THIS IS THE COMMIT POINT
  │     If this fails, transaction remains active, can retry
  │
  ├─ 6. Merge transaction L0 → shared main L0
  │     (L0Buffer::merge — vertex props, edge endpoints,
  │      tombstones, timestamps, constraint indexes)
  │
  ├─ 7. Replay edges → shared AdjacencyManager
  │     (insert_edge / add_tombstone for each edge in tx L0)
  │
  ├─ 8. Release global writer lock
  │
  ├─ 9. Promote new rules → Session rule registry (best-effort)
  │     (diff transaction registry against session baseline)
  │
  ├─ 10. Broadcast CommitNotification to all watchers
  │
  ├─ 11. Run after_commit hooks
  │
  ├─ 12. Record metrics, set completed = true
  │
  └─ 13. Return CommitResult
```

**Durability guarantee:** The WAL flush (step 5) is the commit point.
- Crash before flush → transaction lost, no shared state modified.
- Crash after flush, before merge → WAL replay on restart recovers.
- Crash after merge → fully durable.

**Lock hold time:** Steps 3–8 only. This is the serialization window. Typically sub-millisecond to a few milliseconds depending on the size of the transaction's L0.

## 19.2 Commit Failure Matrix

| Step | Failure | Transaction State | Data State | User Action |
|------|---------|-------------------|------------|-------------|
| 1. Deadline check | `TransactionExpired` | Active (not completed) | No shared state touched | `rollback()` or drop |
| 2. before_commit hook | Hook returns `Err` | Active (not completed) | No shared state touched, no lock acquired | Fix hook condition, retry `commit()`, or `rollback()` |
| 3. Acquire writer lock | `CommitTimeout` (5s default) | Active (not completed) | No shared state touched | Retry `commit()` (another commit was slow) or `rollback()` |
| 4. WAL write | I/O error | Active, lock released | No shared state touched (WAL not flushed) | Retry `commit()` or `rollback()` |
| 5. WAL flush | I/O error | Active, lock released | WAL partially written, no shared merge | Retry `commit()` or `rollback()`. Partial WAL cleaned on restart. |
| 6. L0 merge | Internal error (post-WAL-flush) | Completed (error returned with partial result) | **Durable via WAL.** WAL replay on restart will complete the merge. | Log error. Data is safe. Restart recovers. |
| 7. Adjacency replay | Internal error (post-WAL-flush) | Same as step 6 | **Durable via WAL.** Adjacency rebuilt from WAL + storage on restart. | Same as step 6. |
| 8. Lock release | Cannot fail (scope drop) | — | — | — |
| 9. Rule promotion | `RulePromotionError` | Completed | **Commit is durable.** Rules may not be visible in session. | Inspect `CommitResult::rule_promotion_errors`. Re-register rules manually if needed. |
| 10. Notification broadcast | Watcher panics/drops | Completed | **Commit is durable.** Some watchers may miss the notification. | Watchers should handle missed notifications (polling fallback). |
| 11. after_commit hook | Hook panics | Completed | **Commit is durable.** Panic caught and logged. | Fix the hook. Commit succeeded regardless. |

**Key invariant:** Once step 5 (WAL flush) succeeds, the commit is durable regardless of what happens after. Steps 9–11 are best-effort post-commit effects that do not affect data durability.

**Idempotency:** `commit()` consumes `self` by move. It cannot be called twice on the same Transaction — this is a compile-time guarantee. If an error occurs before the WAL flush (steps 1–5), the Transaction is still active and `commit()` can be called again on the same binding only if the error path returns the Transaction back (which it does not — the Transaction is consumed). In practice, a commit failure before WAL flush means the user should create a new Transaction and replay the mutations.

## 19.3 Rule Promotion Semantics

Rule promotion (step 9) is **best-effort after a durable commit**. The key invariants:

- Graph data is durable after WAL flush (step 5). Rule promotion cannot undo this.
- If promotion fails (e.g., a rule fails to compile against a concurrently-changed schema), the `CommitResult` includes `rule_promotion_errors` describing which rules failed and why.
- The commit is **not rolled back** on promotion failure. The return type is `Ok(CommitResult)` with a non-empty `rule_promotion_errors` vec, not `Err(...)`.
- Failed rules are not promoted. The session's rule registry retains its pre-commit state for those specific rules.
- Users can re-register failed rules manually via `session.register_rules()`.

**Rationale:** Rules are ephemeral in-memory session state. Coupling their compilation success to data durability would make the commit protocol unreasonably complex for a condition that is extremely rare (schema changes concurrent with rule-bearing commits).

## 19.4 Rule Promotion Flow

```
Global Registry (on Uni)
    │
    │  cloned at session creation
    ▼
Session Rule Registry
    │
    │  cloned at transaction creation
    ▼
Transaction Rule Registry
    │
    │  tx.register_rules() modifies this
    │
    │  on tx.commit() — new/modified rules promoted ──► Session Rule Registry
    │    (best-effort; failures reported in CommitResult, commit still durable)
    │  on tx.rollback() — discarded
    │
    │  NOT promoted to Global Registry
    │  (use db.register_rules() for that)
```

Rules flow inward (global → session → transaction) on creation, and one level up (transaction → session) on commit. They never jump two levels.

---

# 20. Lifecycle & Ownership

## 20.1 Object Graph

```
Uni (Arc<UniInner>)
 └─► Session (holds Arc<UniInner>)
      ├─► params: HashMap<String, Value>
      ├─► rule_registry: Arc<RwLock<LocyRuleRegistry>>
      ├─► plan_cache: PlanCache
      ├─► cancellation_token: CancellationToken
      └─► Transaction (holds Arc<Session>)
           ├─► tx_writer: Arc<RwLock<Writer>>  [private, no lock]
           ├─► rule_registry: Arc<RwLock<LocyRuleRegistry>>
           ├─► started_at_version: u64
           └─► completed: bool
```

## 20.2 Creation Costs

| Object | Cost | What Happens |
|--------|------|-------------|
| `Uni::open()` | Heavy | Opens storage, loads schema, initializes Writer, allocates IDs |
| `db.session()` | Cheap | Arc clone + HashMap alloc + rule registry clone (O(n) in registered rules) |
| `template.create()` | Cheap | Same as session() but clones pre-compiled rules |
| `session.tx()` | Moderate | Creates private L0 buffer and AdjacencyManager. **No lock.** |
| `session.execute()` | Moderate | Creates tx + execute + commit internally |
| `tx.commit()` | Moderate | Acquires writer lock, WAL write, L0 merge, adjacency replay. Lock released. |
| `tx.rollback()` | Cheap | Drops private L0 buffer. No I/O. No lock. |
| `session.drop()` | Cheap | Drops rule registry, plan cache, params. No I/O. |

## 20.3 Lifetime Rules

- A **Session** can outlive any Transaction created from it.
- A **Transaction** must not outlive its Session (enforced by Arc — the Transaction's Arc keeps the Session alive, which is correct for memory safety but means a leaked Transaction keeps the Session alive; see note below).
- Multiple **Sessions** can coexist concurrently (each has independent state).
- Multiple **Transactions** (from different Sessions) can coexist concurrently. They write to independent private L0s and only serialize at commit.
- A Session can have **multiple sequential Transactions** (tx, commit, tx again).
- A Session can have **at most one active write context** (Transaction, BulkWriter, or Appender) at a time. This simplifies rule promotion and param inheritance within a session.

**Leaked Transaction warning:** Because Transaction holds `Arc<Session>`, a leaked Transaction keeps the entire Session alive (plan cache, rule registry, metrics accumulator, broadcast sender). The auto-rollback on Drop mitigates this when the Transaction is eventually dropped. For long-lived applications, implementations should consider a diagnostic warning when a Transaction has been alive for longer than 10× its configured timeout.

---

# 21. Multi-Agent Access

## 21.1 Architecture: Multi-Reader, Single-Writer with Lease (Phase 2)

```
Agent 1 (reader) ──► Uni::open("s3://bucket/graph").read_only()  ──┐
Agent 2 (reader) ──► Uni::open("s3://bucket/graph").read_only()  ──┤──► S3
Agent 3 (writer) ──► Uni::open("s3://bucket/graph")              ──┘
                     .write_lease(WriteLease::DynamoDB { .. })
```

## 21.2 WriteLease

```rust
/// Write lease coordination backend. Non-exhaustive: new backends
/// will be added in future releases without breaking existing code.
#[non_exhaustive]
pub enum WriteLease {
    /// Default: single-process, no external coordination.
    Local,

    /// AWS DynamoDB-based lease coordination.
    DynamoDB { table: String },

    /// User-provided lease implementation.
    Custom(Box<dyn WriteLeaseProvider>),
}

/// Trait for custom write lease implementations.
/// Enables coordination backends beyond the built-in options.
pub trait WriteLeaseProvider: Send + Sync {
    /// Acquire the write lease. Returns a guard that must be held
    /// for the duration of write access.
    async fn acquire(&self) -> Result<LeaseGuard>;

    /// Heartbeat to maintain the lease. Called periodically.
    async fn heartbeat(&self, guard: &LeaseGuard) -> Result<()>;

    /// Release the lease explicitly.
    async fn release(&self, guard: LeaseGuard) -> Result<()>;
}
```

## 21.3 Reader Behavior

- `read_only()` mode: `session.tx()` returns `UniError::ReadOnly`. `session.execute()` returns `UniError::ReadOnly`.
- `session.capabilities().can_write` returns `false`.
- Reads from latest Lance table versions in S3.
- `session.refresh()` picks up new versions.
- Session pinning works normally.

## 21.4 Writer Behavior

- Normal Session/Transaction API. Writes go to local L0 → WAL → flush to S3.
- Commits make new Lance versions visible to readers on their next refresh.

## 21.5 Staleness

Readers see data as of their last `refresh()`. They do NOT see the writer's in-memory L0. Visibility latency = writer's flush interval + reader's refresh interval.

## 21.6 Future: Write Coordinator (Phase 3)

When multiple agents need to write, a gRPC coordinator serializes writes. Agents read locally via `read_only()` mode. The API doesn't change — `WriteLeaseProvider` abstracts the coordination.

---

# 22. Rust API Reference

## 22.1 Complete Usage Examples

### Basic Read

```rust
let db = Uni::open("./mydb").build().await?;
let session = db.session();

let rows = session.query("MATCH (n:Person) RETURN n.name").await?;
for row in &rows {
    println!("{}", row.get::<String>("n.name")?);
}
println!("Scanned {} rows in {:?}", rows.metrics().rows_scanned, rows.metrics().total_time);
```

### Parameterized Read

```rust
let mut session = db.session();
session.set("tenant", 42);

let rows = session.query_with("MATCH (n) WHERE n.tenant = $tenant AND n.age > $min RETURN n")
    .param("min", 25)
    .timeout(Duration::from_secs(5))
    .fetch_all().await?;
```

### Single-Shot Write (Auto-Committed)

```rust
let session = db.session();

// Simple — auto-committed transaction under the hood
session.execute("CREATE (:Person {name: 'Alice', age: 30})").await?;

// With parameters
session.execute_with("CREATE (:Person {name: $name, age: $age})")
    .param("name", "Bob")
    .param("age", 25)
    .run().await?;
```

### Locy Evaluation

```rust
let session = db.session();
session.register_rules("
    path(X, Y) :- edge(X, Y).
    path(X, Z) :- edge(X, Y), path(Y, Z).
")?;

// Read-only query
let result = session.locy("?- path('a', X).").await?;

// With parameters
let result = session.locy_with("?- path($source, X).")
    .param("source", "alice")
    .max_iterations(500)
    .timeout(Duration::from_secs(10))
    .run().await?;
```

### DERIVE on Session (Preferred for Heavy Computation)

```rust
let session = db.session();
session.register_rules("
    high_risk(A) :- MATCH (a:Account)-[:MADE]->(t:Transaction)
                    WHERE t.amount > 10000
                    GROUP BY a HAVING count(t) > 5.
")?;

// Evaluate — no locks held, takes as long as it needs
let result = session.locy("DERIVE high_risk(A) :- ...").await?;
println!("Derived {} facts in {:?}",
    result.derived.as_ref().map_or(0, |d| d.fact_count()),
    result.metrics.exec_time);

// Materialize — brief transaction, lock held only during commit
if let Some(derived) = result.derived {
    let tx = session.tx().await?;

    // Choose your staleness policy:
    let applied = tx.apply(derived).await?;                           // default: warn
    // let applied = tx.apply_with(derived).require_fresh().run()?;   // strict
    // let applied = tx.apply_with(derived).max_version_gap(3).run()?; // threshold

    println!("Applied {} facts (version gap: {})", applied.facts_applied, applied.version_gap);
    tx.commit().await?;
}
```

### DERIVE on Transaction (When You Need Tx Isolation)

```rust
let tx = session.tx().await?;
tx.execute("CREATE (:Evidence {data: 'new_finding'})").await?;

// DERIVE can see the uncommitted Evidence
tx.locy("DERIVE conclusion(X) :- evidence(X).").await?;

tx.commit().await?;
```

### Hypothetical Reasoning (ASSUME)

```rust
// Works on Session — no transaction needed
let result = session.locy("
    ASSUME { CREATE (:Account {name: 'suspect'})-[:MADE]->(:Transaction {amount: 50000}) }
    ?- high_risk('suspect').
").await?;
// Hypothetical mutations discarded. Nothing written.
println!("Would be high risk: {}", !result.rows.is_empty());
```

### Write Transaction (Multi-Statement)

```rust
let session = db.session();
let tx = session.tx().await?;

tx.execute("CREATE (:Person {name: 'Alice', age: 30})").await?;
tx.execute("CREATE (:Person {name: 'Bob', age: 25})").await?;
tx.execute("
    MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
    CREATE (a)-[:KNOWS]->(b)
").await?;

let result = tx.commit().await?;
println!("Committed {} mutations at version {} (gap: {})",
    result.mutations_committed, result.version, result.version_gap());
```

### Concurrent Transactions (Different Sessions)

```rust
let db = Arc::new(Uni::open("./mydb").build().await?);

let (r1, r2) = tokio::join!(
    {
        let db = Arc::clone(&db);
        async move {
            let session = db.session();
            let tx = session.tx().await?;
            tx.execute("CREATE (:Hot {temp: 200})").await?;
            tx.commit().await  // serializes at commit
        }
    },
    {
        let db = Arc::clone(&db);
        async move {
            let session = db.session();
            let tx = session.tx().await?;
            tx.execute("CREATE (:Cold {temp: -40})").await?;
            tx.commit().await  // waits for first commit if concurrent
        }
    },
);
r1?; r2?; // Both succeed — disjoint writes, commits serialize
```

### Overlap Detection

```rust
let session = db.session();
let tx = session.tx().await?;
tx.execute("MATCH (n {id: 42}) SET n.status = 'verified'").await?;

let result = tx.commit().await?;
if result.version_gap() > 0 {
    // Another transaction committed while we were working.
    // For critical workflows, re-read and verify.
    tracing::warn!(gap = result.version_gap(), "Concurrent commit detected");
}
```

### Prepared Statements

```rust
let session = db.session();
let stmt = session.prepare("MATCH (n:Person) WHERE n.age > $min RETURN n.name").await?;

for min_age in [20, 30, 40, 50] {
    let rows = stmt.execute(&[("min", min_age.into())]).await?;
    println!("{} people over {}", rows.len(), min_age);
}
```

### Cancellation

```rust
let session = db.session();
let token = CancellationToken::new();

let handle = tokio::spawn({
    let token = token.clone();
    async move {
        session.query_with("MATCH (n)-[*1..20]->(m) RETURN n, m")
            .cancellation_token(token)
            .fetch_all().await
    }
});

tokio::time::sleep(Duration::from_millis(100)).await;
token.cancel();

match handle.await? {
    Err(UniError::Cancelled) => println!("Query cancelled"),
    other => other.map(|_| ())?,
}
```

### Session Template

```rust
let template = db.session_template()
    .param("tenant_id", 42)
    .rules("path(X,Y) :- edge(X,Y). path(X,Z) :- edge(X,Y), path(Y,Z).")?
    .transaction_timeout(Duration::from_secs(30))
    .build()?;

// Per-request:
let session = template.create();
let result = session.locy("?- path($start, X).").await?;
```

### Capability Introspection

```rust
let session = db.session();
let caps = session.capabilities();

if caps.can_write {
    session.execute("CREATE (:Marker)").await?;
} else {
    println!("Read-only mode — refresh for latest data");
    // session.refresh().await?;  // Phase 2
}

println!("Isolation: {:?}", caps.isolation);
```

### Bulk Loading

```rust
let session = db.session();
let mut writer = session.bulk_writer()
    .batch_size(10_000)
    .defer_vector_indexes(true)
    .build().await?;

writer.insert_vertices("Person", props).await?;
writer.insert_edges("KNOWS", edges).await?;
let stats = writer.commit().await?;
```

### Streaming Appender

```rust
let session = db.session();
let mut appender = session.appender("Person")
    .batch_size(5_000)
    .build().await?;

for record in data_stream {
    appender.append(record.into_properties()).await?;
}
let stats = appender.finish().await?;
```

---

# 23. Python API Reference

## 23.1 Sync API

```python
import uni_db as uni

db = uni.Database.open("./mydb")

# ── Session ──
session = db.session()
session.set("tenant", 42)

# Cypher read
rows = session.query("MATCH (n) WHERE n.tenant = $tenant RETURN n")
rows = (session.query_with("MATCH (n) WHERE n.age > $min RETURN n")
              .param("min", 25)
              .timeout(5.0)
              .fetch_all())

# Single-shot write (auto-committed)
session.execute("CREATE (:Person {name: 'Alice'})")
result = (session.execute_with("CREATE (:Person {name: $name})")
                .param("name", "Bob")
                .run())

# Locy — all operations
session.register_rules("path(X,Y) :- edge(X,Y).")
result = session.locy("?- path('a', X).")

# DERIVE on session
result = session.locy("DERIVE high_risk(A) :- ...")
if result.derived:
    print(f"Derived {result.derived.fact_count()} facts")
    with session.tx() as tx:
        tx.apply(result.derived)
        # Or with staleness control:
        # tx.apply_with(result.derived).require_fresh().run()
        tx.commit()

# ASSUME on session
result = session.locy("""
    ASSUME { CREATE (:Account {name: 'test'})-[:MADE]->(:Transaction {amount: 50000}) }
    ?- high_risk('test').
""")

# Prepared statements
stmt = session.prepare("MATCH (n:Person) WHERE n.age > $min RETURN n.name")
for min_age in [20, 30, 40, 50]:
    rows = stmt.execute({"min": min_age})

# Capabilities
caps = session.capabilities()
print(f"Can write: {caps.can_write}, Isolation: {caps.isolation}")

# ── Transaction (multi-statement) ──
with session.tx() as tx:
    tx.execute("CREATE (:Person {name: 'Alice'})")
    tx.locy("DERIVE reachable(X,Y) :- path(X,Y).")
    result = tx.commit()
    print(f"Version gap: {result.version_gap()}")
# Auto-rollback if exception or no explicit commit

# ── Metrics ──
print(rows.metrics)     # {'parse_time_ms': 0.5, 'exec_time_ms': 12.3, ...}
print(session.metrics)  # {'queries_executed': 42, ...}
print(db.metrics)       # {'active_sessions': 1, ...}
```

## 23.2 Async API

```python
import uni_db as uni

db = await uni.AsyncDatabase.open("./mydb")
session = db.session()

rows = await session.query("MATCH (n) RETURN n")

# Auto-committed write
await session.execute("CREATE (:Person {name: 'Alice'})")

# DERIVE
result = await session.locy("DERIVE high_risk(A) :- ...")

if result.derived:
    async with session.tx() as tx:
        await tx.apply(result.derived)
        await tx.commit()
```

## 23.3 Context Manager Behavior

| Type | `__enter__` / `__aenter__` | `__exit__` / `__aexit__` |
|------|--------------------------|-------------------------|
| `Database` | Returns self | Calls `shutdown()` |
| `Session` | Returns self | Drops session |
| `Transaction` | Returns self | Auto-rollback if not committed |
| `BulkWriter` | Returns self | Calls `abort()` if not committed |
| `StreamingAppender` | Returns self | Calls `abort()` if not finished |

---

# 24. Migration

## 24.1 Strategy: Clean Break

All legacy methods on Uni are removed. No shims. The `session.execute()` convenience method preserves the common single-shot write pattern.

## 24.2 API Transformation Guide

| Before | After | Behavioral Change |
|--------|-------|-------------------|
| `db.query(cypher)` | `db.session().query(cypher)` | None — same read semantics |
| `db.execute(cypher)` | `db.session().execute(cypher)` | Same auto-commit semantics |
| `db.execute(cypher)` (in multi-statement flow) | `let s = db.session(); let tx = s.tx().await?; tx.execute(cypher).await?; tx.commit().await?;` | Explicit transaction boundary |
| `db.begin()` | `db.session().tx()` | **Changed:** lock acquired at commit, not begin. See §2. |
| `db.query_with(cypher).param(k, v)` | `db.session().query_with(cypher).param(k, v)` | None |
| `db.locy().evaluate(prog)` | `db.session().locy(prog)` | None |
| `db.locy().register(prog)` | `db.register_rules(prog)` | None (global scope unchanged) |
| `db.explain(cypher)` | `db.session().explain(cypher)` | None |
| `db.bulk_writer()` | `db.session().bulk_writer()` | Inherits session params |

## 24.3 Behavioral Differences (Not Just API Relocation)

These changes affect semantics, not just method location:

| Change | Old Behavior | New Behavior | Impact |
|--------|-------------|-------------|--------|
| Transaction lock timing | Lock at `begin()` | Lock at `commit()` | Concurrent transactions now possible. See §2. |
| Overlapping writes | Impossible (single writer) | Last-writer-wins | Applications relying on exclusive write access need review. See §17.4. |
| DERIVE locking | Held writer lock during evaluation | No lock during evaluation | Pure improvement — no negative impact. |
| Rule promotion | Immediate (on `register()`) | Best-effort on commit (for tx-scoped rules) | `CommitResult::rule_promotion_errors` may be non-empty. |

## 24.4 Common Pattern Rewrites

```rust
// ── Before: Simple single-statement write ──
db.execute("CREATE (:Person {name: 'Alice'})").await?;

// ── After: Same ergonomics via session.execute() ──
let session = db.session();
session.execute("CREATE (:Person {name: 'Alice'})").await?;


// ── Before: Multi-statement transaction ──
let txn = db.begin().await?;                    // Lock acquired HERE
txn.execute("CREATE (:Person {name: 'A'})").await?;
txn.execute("CREATE (:Person {name: 'B'})").await?;
txn.commit().await?;                            // Lock released HERE

// ── After: Same, but lock timing changed ──
let session = db.session();
let tx = session.tx().await?;                   // No lock yet
tx.execute("CREATE (:Person {name: 'A'})").await?;
tx.execute("CREATE (:Person {name: 'B'})").await?;
tx.commit().await?;                             // Lock acquired AND released HERE


// ── Before: Locy evaluation ──
let engine = db.locy();
engine.register("path(X,Y) :- edge(X,Y).").await?;
let result = engine.evaluate("?- path('a', X).").await?;

// ── After: Flattened ──
let session = db.session();
session.register_rules("path(X,Y) :- edge(X,Y).")?;
let result = session.locy("?- path('a', X).").await?;
```

---

# 25. Implementation Phases

## Phase 1: Core API (This Release)

- [x] Session struct with params, rule registry, plan cache
- [x] Transaction struct with private L0, **commit-time locking**
- [x] `session.execute()` / `session.execute_with()` auto-commit convenience
- [x] Locy flattened: `session.locy()`, `tx.locy()`, `register_rules()`, `clear_rules()`
- [x] DERIVE on Session returning DerivedFactSet
- [x] ASSUME on Session with eval-scoped temporary buffer
- [x] `tx.apply()` and `tx.apply_with()` with staleness policy (`require_fresh`, `max_version_gap`)
- [x] `tx()` / `tx_with()` naming with `_with` pattern
- [x] `IsolationLevel` enum (`#[non_exhaustive]`, single variant `Serialized`)
- [x] `CommitResult::started_at_version` and `version_gap()` method
- [x] `CommitResult::rule_promotion_errors` for best-effort rule promotion
- [x] Commit failure matrix implemented per §19.2
- [x] Mutual exclusion (one write context per session) with scopeguard pattern
- [x] Concurrent transactions across sessions (no lock until commit)
- [x] Transparent plan cache with schema-version invalidation
- [x] Explicit `prepare()` / `PreparedQuery`
- [x] Query cancellation via CancellationToken
- [x] Transaction timeouts
- [x] Per-query metrics on every result
- [x] Session-level and database-level metrics
- [x] `SessionCapabilities` struct
- [x] Tracing spans on all public methods
- [x] Remove all legacy methods from Uni (clean break)
- [ ] "Changes from Prior Concurrency Model" section in release notes
- [x] Update entire test suite (~3000 call sites)
- [x] Update Python bindings (sync + async)
- [ ] Migration guide with behavioral diff table
- [x] `ExecuteBuilder` type (not `QueryBuilder`) for `tx.execute_with()`
- [x] Version pinning: `pin_to_version()`, `pin_to_timestamp()`
- [x] Hook failure semantics per §15.1

## Phase 2: Ecosystem (Next Release)

- [x] Commit notifications (`session.watch()`, `session.watch_with()`)
- [x] `WatchBuilder` with label filtering, debounce, session exclusion
- [x] `CommitNotification::session_id` and `causal_version` fields
- [x] Session hooks (before/after query/commit)
- [x] SessionTemplate for pre-configured sessions
- [x] Streaming Appender on Session
- [x] Multi-agent: `read_only()` mode
- [x] Multi-agent: `write_lease(WriteLease::DynamoDB)` and `WriteLeaseProvider` trait
- [x] `WriteLease` enum (`#[non_exhaustive]`)
- [x] `session.refresh()` for reader instances
- [x] Custom function registration (`session.register_function()`)

## Phase 3: Advanced (Future)

- [ ] `IsolationLevel::Optimistic` — OCC with write-set conflict detection
- [ ] Write coordinator server (gRPC) for multi-agent writes via `WriteLeaseProvider`
- [ ] Reactive queries / subscriptions
- [ ] Distributed commit notifications (cross-agent)

---

# 26. Comparison to Other Databases

| Feature | Uni (this spec) | Neo4j | SurrealDB | SQLite | DuckDB | FoundationDB |
|---------|:---:|:---:|:---:|:---:|:---:|:---:|
| **DERIVE without lock** | **Yes** | N/A | N/A | N/A | N/A | N/A |
| Logic programming | **Yes (Locy)** | No | No | No | No | No |
| Reads require session | **Yes** | Yes | Yes | Yes (conn) | Yes (conn) | Yes (tx) |
| Writes require transaction | **Yes** | Yes | Yes | Yes | Yes | Yes |
| Auto-commit convenience | **Yes (`session.execute`)** | Yes | Yes | Yes | Yes | N/A |
| Session-scoped state | **Params + rules + plan cache** | Bookmarks | Scopes | Pragmas | Settings | None |
| Tx creation acquires lock | **No** | No (lock per entity) | No | Yes | No | No |
| Commit serialization | **Yes (global lock)** | Yes (per-entity) | Yes | Yes (global) | Yes (MVCC) | Yes (OCC) |
| Overlap detection | **Yes (version_gap)** | No | No | N/A | N/A | Yes (conflict) |
| Staleness policy (DERIVE) | **Yes (require_fresh)** | N/A | N/A | N/A | N/A | N/A |
| Plan cache | **Yes (transparent + explicit)** | Yes | Yes | Yes | Yes | N/A |
| Prepared statements | **Yes** | Yes | Yes | Yes | Yes | N/A |
| Query cancellation | **Yes (CancellationToken)** | Yes | Yes | Limited | Yes | Yes |
| Transaction timeout | **Yes** | Yes | Yes | Busy timeout | No | 5s hard limit |
| Per-query metrics | **Yes (always)** | Yes | Yes | Limited | Yes | Limited |
| Commit notifications | **Yes (Phase 2)** | Yes (reactive) | Yes (LIVE) | No | No | Watches |
| Commit failure matrix | **Documented** | Partial | No | No | No | Partial |
| Session hooks | **Yes (Phase 2)** | Plugins | Events | No | Extensions | Layers |
| Streaming inserts | **Yes (Appender)** | Yes | Yes | No | Yes | Yes |
| DDL transactional | **No (admin)** | No | No | Partial | No | N/A |
| Multi-agent (Phase 2) | **Read replicas + write lease** | Cluster | Cluster | WAL readers | N/A | Cluster |
| Isolation extensibility | **`#[non_exhaustive]` enum** | Fixed | Fixed | Fixed | Fixed | Fixed |

---

*End of specification.*
