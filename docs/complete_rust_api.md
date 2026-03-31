# Uni — Complete Rust Public API Reference

**Auto-generated from source: March 2026**
**Crate:** `uni-db` v0.3.0

This document catalogs every public type, method, and field in the Uni Rust API. It is organized by the three core scoping concepts: **Uni** (lifecycle & admin), **Session** (read scope), and **Transaction** (write scope).

---

## Table of Contents

1. [Uni — The Database Handle](#1-uni--the-database-handle)
2. [UniBuilder — Database Configuration](#2-unibuilder--database-configuration)
3. [Session — The Read Scope](#3-session--the-read-scope)
4. [Transaction — The Write Scope](#4-transaction--the-write-scope)
5. [BulkWriter — High-Throughput Writes](#5-bulkwriter--high-throughput-writes)
6. [StreamingAppender — Row-by-Row Loading](#6-streamingappender--row-by-row-loading)
7. [Builders](#7-builders)
8. [Result Types](#8-result-types)
9. [Query & Row Types](#9-query--row-types)
10. [Locy Types](#10-locy-types)
11. [Schema Types](#11-schema-types)
12. [Index Types](#12-index-types)
13. [Configuration](#13-configuration)
14. [Observability & Metrics](#14-observability--metrics)
15. [Commit Notifications](#15-commit-notifications)
16. [Session Hooks](#16-session-hooks)
17. [Prepared Statements](#17-prepared-statements)
18. [Session Templates](#18-session-templates)
19. [Multi-Agent Access](#19-multi-agent-access)
20. [Synchronous (Blocking) API](#20-synchronous-blocking-api)
21. [Error Types](#21-error-types)

---

# 1. Uni — The Database Handle

Uni is the lifecycle and administration handle. It opens the database, manages schema and storage, and provides factories for Sessions. It does **not** execute queries or mutations.

**Source:** `crates/uni/src/api/mod.rs`

```rust
impl Uni {
    // ── Factory Methods ──

    /// Open or create a database at the given path.
    pub fn open(uri: impl Into<String>) -> UniBuilder;

    /// Open an existing database. Errors if the path does not exist.
    pub fn open_existing(uri: impl Into<String>) -> UniBuilder;

    /// Create a new database. Errors if the path already exists.
    pub fn create(uri: impl Into<String>) -> UniBuilder;

    /// Create a database in a temporary directory (auto-cleaned on drop).
    pub fn temporary() -> UniBuilder;

    /// Create a purely in-memory database (no persistence).
    pub fn in_memory() -> UniBuilder;

    // ── Session Factories ──

    /// Create a new Session. Synchronous, infallible, cheap.
    pub fn session(&self) -> Session;

    /// Create a session template builder for pre-configured sessions.
    pub fn session_template(&self) -> SessionTemplateBuilder;

    // ── Schema DDL ──

    /// Start building a schema modification (labels, edge types, properties).
    pub fn schema(&self) -> SchemaBuilder<'_>;

    /// Load schema from a JSON file.
    pub async fn load_schema(&self, path: impl AsRef<Path>) -> Result<()>;

    /// Save current schema to a JSON file.
    pub async fn save_schema(&self, path: impl AsRef<Path>) -> Result<()>;

    // ── Schema Inspection ──

    /// Get the current schema (thread-safe snapshot).
    pub fn get_schema(&self) -> Arc<Schema>;

    /// Check if a label exists.
    pub async fn label_exists(&self, name: &str) -> Result<bool>;

    /// Check if an edge type exists.
    pub async fn edge_type_exists(&self, name: &str) -> Result<bool>;

    /// List all active label names.
    pub async fn list_labels(&self) -> Result<Vec<String>>;

    /// List all active edge type names.
    pub async fn list_edge_types(&self) -> Result<Vec<String>>;

    /// Get detailed info about a label (properties, indexes, constraints).
    pub async fn get_label_info(&self, name: &str) -> Result<Option<LabelInfo>>;

    // ── Global Locy Rules ──

    /// Register Locy rules globally (cloned into every new Session).
    pub fn register_rules(&self, program: &str) -> Result<()>;

    /// Clear all globally registered Locy rules.
    pub fn clear_rules(&self);

    // ── Storage Admin ──

    /// Flush all pending mutations to durable storage.
    pub async fn flush(&self) -> Result<()>;

    /// Compact a label's storage (merge L1 runs).
    pub async fn compact_label(&self, label: &str) -> Result<CompactionStats>;

    /// Compact an edge type's storage.
    pub async fn compact_edge_type(&self, edge_type: &str) -> Result<CompactionStats>;

    /// Wait for all background compaction tasks to complete.
    pub async fn wait_for_compaction(&self) -> Result<()>;

    // ── Snapshots ──

    /// Create a snapshot (optional name, auto-generated if None).
    pub async fn create_snapshot(&self, name: Option<&str>) -> Result<String>;

    /// Create a named snapshot (convenience wrapper).
    pub async fn create_named_snapshot(&self, name: &str) -> Result<String>;

    /// List all available snapshots.
    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotManifest>>;

    /// Restore the database to a snapshot.
    pub async fn restore_snapshot(&self, snapshot_id: &str) -> Result<()>;

    // ── Index Admin ──

    /// Rebuild indexes for a label. If `async_` is true, returns a task ID.
    pub async fn rebuild_indexes(&self, label: &str, async_: bool)
        -> Result<Option<String>>;

    /// Get status of all active index rebuild tasks.
    pub async fn index_rebuild_status(&self) -> Result<Vec<IndexRebuildTask>>;

    /// Retry all failed index rebuilds.
    pub async fn retry_index_rebuilds(&self) -> Result<Vec<String>>;

    /// Check if any index is currently building for a label.
    pub async fn is_index_building(&self, label: &str) -> Result<bool>;

    /// List indexes for a specific label (synchronous).
    pub fn list_indexes(&self, label: &str) -> Vec<IndexDefinition>;

    /// List all indexes across all labels (synchronous).
    pub fn list_all_indexes(&self) -> Vec<IndexDefinition>;

    // ── Metrics ──

    /// Get database-level metrics snapshot.
    pub fn metrics(&self) -> DatabaseMetrics;

    // ── Configuration ──

    /// Get the active configuration.
    pub fn config(&self) -> &UniConfig;

    /// Get the active write lease (if any).
    pub fn write_lease(&self) -> Option<&WriteLease>;

    // ── Lifecycle ──

    /// Shut down the database, flushing all pending data.
    pub async fn shutdown(self) -> Result<()>;
}
```

---

# 2. UniBuilder — Database Configuration

**Source:** `crates/uni/src/api/mod.rs`

```rust
impl UniBuilder {
    // ── Configuration ──

    /// Set the database configuration.
    pub fn config(mut self, config: UniConfig) -> Self;

    /// Set the page/block cache size in bytes.
    pub fn cache_size(mut self, bytes: usize) -> Self;

    /// Set the query execution parallelism.
    pub fn parallelism(mut self, n: usize) -> Self;

    /// Load schema from a JSON file at build time.
    pub fn schema_file(mut self, path: impl AsRef<Path>) -> Self;

    /// Configure cloud storage backend.
    pub fn cloud_config(mut self, config: CloudStorageConfig) -> Self;

    /// Configure hybrid local+remote storage.
    pub fn hybrid(mut self, local_path: impl AsRef<Path>, remote_url: &str) -> Self;

    // ── Xervo (ML Runtime) ──

    /// Set the Xervo model alias catalog.
    pub fn xervo_catalog(mut self, catalog: Vec<ModelAliasSpec>) -> Self;

    /// Set the Xervo catalog from a JSON string.
    pub fn xervo_catalog_from_str(mut self, json: &str) -> Result<Self>;

    /// Set the Xervo catalog from a JSON file.
    pub fn xervo_catalog_from_file(mut self, path: impl AsRef<Path>) -> Result<Self>;

    // ── Multi-Agent Modes ──

    /// Open in read-only mode (no writes allowed).
    pub fn read_only(mut self) -> Self;

    /// Set a write lease for multi-agent coordination.
    pub fn write_lease(mut self, lease: WriteLease) -> Self;

    // ── Build ──

    /// Build the database instance (async).
    pub async fn build(self) -> Result<Uni>;

    /// Build the database instance (blocking, creates its own runtime).
    pub fn build_sync(self) -> Result<Uni>;
}
```

---

# 3. Session — The Read Scope

A Session is a long-lived, isolated read context. It holds scoped parameters, a private Locy rule registry, a plan cache, and provides factories for Transactions, BulkWriters, and Appenders.

**Source:** `crates/uni/src/api/session.rs`

```rust
impl Session {
    // ── Scoped Parameters ──

    /// Set a session-scoped parameter (injected into every query).
    pub fn set<K: Into<String>, V: Into<Value>>(
        &mut self, key: K, value: V,
    ) -> &mut Self;

    /// Get a session-scoped parameter.
    pub fn get(&self, key: &str) -> Option<&Value>;

    /// Set multiple parameters from an iterator.
    pub fn set_all<I, K, V>(&mut self, params: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<Value>;

    // ── Cypher Reads ──

    /// Execute a read-only Cypher query.
    pub async fn query(&self, cypher: &str) -> Result<QueryResult>;

    /// Fluent query builder with parameters, timeout, cancellation.
    pub fn query_with(&self, cypher: &str) -> QueryBuilder<'_>;

    /// Execute a query returning a streaming cursor.
    pub async fn query_cursor(&self, cypher: &str) -> Result<QueryCursor>;

    // ── Auto-Committed Writes ──

    /// Execute a Cypher mutation as an auto-committed transaction.
    pub async fn execute(&self, cypher: &str) -> Result<AutoCommitResult>;

    /// Fluent auto-commit builder with parameters, timeout.
    pub fn execute_with(&self, cypher: &str) -> AutoCommitBuilder<'_>;

    // ── Locy Evaluation ──

    /// Evaluate a Locy program (queries, DERIVE, ASSUME).
    pub async fn locy(&self, program: &str) -> Result<LocyResult>;

    /// Fluent Locy builder with parameters, timeout, max_iterations.
    pub fn locy_with(&self, program: &str) -> LocyBuilder<'_>;

    // ── Locy Rule Management ──

    /// Register rules in the session's private registry.
    pub fn register_rules(&self, program: &str) -> Result<()>;

    /// Clear all rules in the session's registry.
    pub fn clear_rules(&self);

    /// Compile a Locy program without executing (synchronous).
    pub fn compile_locy(&self, program: &str) -> Result<CompiledProgram>;

    // ── Planning & Introspection ──

    /// Explain a Cypher query plan without executing.
    pub async fn explain(&self, cypher: &str) -> Result<ExplainOutput>;

    /// Execute and profile a Cypher query.
    pub async fn profile(&self, cypher: &str) -> Result<(QueryResult, ProfileOutput)>;

    /// Profile with parameters via builder.
    pub fn profile_with(&self, cypher: &str) -> ProfileBuilder<'_>;

    /// Explain a Locy evaluation strategy (synchronous).
    pub fn explain_locy(&self, program: &str) -> Result<LocyExplainOutput>;

    // ── Prepared Statements ──

    /// Prepare a Cypher query for repeated execution.
    pub async fn prepare(&self, cypher: &str) -> Result<PreparedQuery>;

    /// Prepare a Locy program for repeated evaluation (synchronous).
    pub fn prepare_locy(&self, program: &str) -> Result<PreparedLocy>;

    // ── Transaction & Writer Factories ──

    /// Start a new Transaction (creates private L0 buffer, no lock).
    pub async fn tx(&self) -> Result<Transaction>;

    /// Start a configured Transaction via builder.
    pub fn tx_with(&self) -> TransactionBuilder<'_>;

    /// Create a BulkWriter builder for high-throughput loading.
    pub fn bulk_writer(&self) -> BulkWriterBuilder;

    /// Create a streaming appender for a specific label.
    pub fn appender(&self, label: &str) -> AppenderBuilder<'_>;

    // ── Convenience Bulk Insert ──

    /// Bulk insert vertices (convenience wrapper over BulkWriter).
    pub async fn bulk_insert_vertices(
        &self, label: &str, properties_list: Vec<Properties>,
    ) -> Result<Vec<Vid>>;

    /// Bulk insert edges using pre-allocated VIDs.
    pub async fn bulk_insert_edges(
        &self, edge_type: &str,
        edges: Vec<(Vid, Vid, Properties)>,
    ) -> Result<()>;

    // ── Version Pinning ──

    /// Pin this session to a specific snapshot version.
    pub async fn pin_to_version(&mut self, snapshot_id: &str) -> Result<()>;

    /// Pin this session to a specific timestamp.
    pub async fn pin_to_timestamp(
        &mut self, ts: chrono::DateTime<chrono::Utc>,
    ) -> Result<()>;

    /// Unpin: return to the live database state.
    pub async fn refresh(&mut self) -> Result<()>;

    /// Check if the session is pinned to a specific version.
    pub fn is_pinned(&self) -> bool;

    // ── Custom Functions ──

    /// Register a custom scalar function for Cypher queries.
    pub fn register_function<F>(&self, name: &str, func: F) -> Result<()>
    where
        F: Fn(&[Value]) -> Result<Value> + Send + Sync + 'static;

    // ── Cancellation ──

    /// Cancel all in-flight queries in this session.
    pub fn cancel(&self);

    /// Get a clone of this session's cancellation token.
    pub fn cancellation_token(&self) -> CancellationToken;

    // ── Hooks ──

    /// Add a session hook for query/commit interception.
    pub fn add_hook(&mut self, hook: impl SessionHook + 'static);

    // ── Commit Notifications ──

    /// Watch for all commit notifications.
    pub fn watch(&self) -> CommitStream;

    /// Watch for commit notifications with filters and debouncing.
    pub fn watch_with(&self) -> WatchBuilder;

    // ── Observability ──

    /// Session identifier (UUID).
    pub fn id(&self) -> &str;

    /// Runtime capability introspection.
    pub fn capabilities(&self) -> SessionCapabilities;

    /// Snapshot the session's accumulated metrics.
    pub fn metrics(&self) -> SessionMetrics;
}

impl Drop for Session {
    // Decrements active session count. No I/O, no locks.
}
```

---

# 4. Transaction — The Write Scope

A Transaction is a short-lived, isolated write context within a Session. It owns a private L0 buffer. No lock is held until `commit()`.

**Source:** `crates/uni/src/api/transaction.rs`

```rust
impl Transaction {
    // ── Cypher Reads (sees shared DB + uncommitted writes) ──

    /// Execute a Cypher query within the transaction.
    pub async fn query(&self, cypher: &str) -> Result<QueryResult>;

    /// Fluent query builder within the transaction.
    pub fn query_with(&self, cypher: &str) -> TxQueryBuilder<'_>;

    // ── Cypher Writes ──

    /// Execute a Cypher mutation (writes to private L0).
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult>;

    /// Fluent mutation builder.
    pub fn execute_with(&self, cypher: &str) -> ExecuteBuilder<'_>;

    // ── Locy (Auto-Applies DERIVE to L0) ──

    /// Evaluate a Locy program. DERIVE auto-applies to the tx's private L0.
    pub async fn locy(&self, program: &str) -> Result<LocyResult>;

    /// Fluent Locy builder within the transaction.
    pub fn locy_with(&self, program: &str) -> TxLocyBuilder<'_>;

    // ── Apply DerivedFactSet ──

    /// Apply a DerivedFactSet (from a session-level DERIVE) to this tx.
    pub async fn apply(&self, derived: DerivedFactSet) -> Result<ApplyResult>;

    /// Fluent apply builder with staleness controls.
    pub fn apply_with(&self, derived: DerivedFactSet) -> ApplyBuilder<'_>;

    // ── Rule Management ──

    /// Register rules scoped to this transaction (promoted on commit).
    pub fn register_rules(&self, program: &str) -> Result<()>;

    /// Clear all transaction-scoped rules.
    pub fn clear_rules(&self);

    // ── Prepared Statements ──

    /// Prepare a Cypher query within this transaction.
    pub async fn prepare(&self, cypher: &str) -> Result<PreparedQuery>;

    /// Prepare a Locy program within this transaction (synchronous).
    pub fn prepare_locy(&self, program: &str) -> Result<PreparedLocy>;

    // ── Lifecycle ──

    /// Commit: acquire writer lock, WAL write, merge L0, release lock.
    pub async fn commit(mut self) -> Result<CommitResult>;

    /// Rollback: discard all writes. No I/O, no lock needed.
    pub fn rollback(mut self);

    /// Check if there are uncommitted mutations.
    pub fn is_dirty(&self) -> bool;

    /// Transaction identifier (UUID).
    pub fn id(&self) -> &str;

    /// Cancel all in-flight queries in this transaction.
    pub fn cancel(&self);

    /// Get a clone of this transaction's cancellation token.
    pub fn cancellation_token(&self) -> CancellationToken;
}

impl Drop for Transaction {
    // Auto-rollback if not committed. Releases write guard.
    // Logs warning if dirty.
}
```

---

# 5. BulkWriter — High-Throughput Writes

BulkWriter bypasses normal isolation for performance. Already-flushed batches cannot be rolled back. For atomicity, use Transaction instead.

**Source:** `crates/uni/src/api/bulk.rs`

```rust
impl BulkWriter {
    /// Insert vertices for a label. Accepts any type implementing IntoArrow.
    pub async fn insert_vertices(
        &mut self, label: &str, vertices: impl IntoArrow,
    ) -> Result<Vec<Vid>>;

    /// Insert edges. Accepts a vector of EdgeData.
    pub async fn insert_edges(
        &mut self, edge_type: &str, edges: Vec<EdgeData>,
    ) -> Result<Vec<Eid>>;

    /// Commit: finalize all writes, rebuild deferred indexes.
    pub async fn commit(mut self) -> Result<BulkStats>;

    /// Abort: stop further writes (already-flushed data persists).
    pub async fn abort(mut self) -> Result<()>;

    /// Get current statistics.
    pub fn stats(&self) -> &BulkStats;

    /// Get labels touched by this writer.
    pub fn touched_labels(&self) -> Vec<String>;

    /// Get edge types touched by this writer.
    pub fn touched_edge_types(&self) -> Vec<String>;
}
```

### BulkWriterBuilder

```rust
impl BulkWriterBuilder {
    /// Defer vector index rebuilds until commit (default: true).
    pub fn defer_vector_indexes(mut self, defer: bool) -> Self;

    /// Defer scalar index rebuilds until commit.
    pub fn defer_scalar_indexes(mut self, defer: bool) -> Self;

    /// Set the auto-flush batch size (default: 10,000).
    pub fn batch_size(mut self, size: usize) -> Self;

    /// Register a progress callback.
    pub fn on_progress<F: Fn(BulkProgress) + Send + 'static>(mut self, f: F) -> Self;

    /// Run index rebuilds asynchronously after commit.
    pub fn async_indexes(mut self, async_: bool) -> Self;

    /// Validate constraints during insert.
    pub fn validate_constraints(mut self, validate: bool) -> Self;

    /// Set the maximum buffer size in bytes before auto-flush.
    pub fn max_buffer_size_bytes(mut self, size: usize) -> Self;

    /// Build the BulkWriter (synchronous — no I/O).
    pub fn build(self) -> Result<BulkWriter>;
}
```

### Supporting Types

```rust
/// Data for a single edge insertion.
pub struct EdgeData {
    pub src_vid: Vid,
    pub dst_vid: Vid,
    pub properties: Properties,
}

impl EdgeData {
    pub fn new(src_vid: Vid, dst_vid: Vid, properties: Properties) -> Self;
}

/// Trait for types convertible to property maps (used by insert_vertices).
pub trait IntoArrow {
    fn into_property_maps(self) -> Vec<HashMap<String, Value>>;
}

/// Bulk operation statistics.
pub struct BulkStats {
    pub vertices_inserted: usize,
    pub edges_inserted: usize,
    pub indexes_rebuilt: usize,
    pub duration: Duration,
    pub index_build_duration: Duration,
    pub index_task_ids: Vec<String>,
    pub indexes_pending: bool,
}

/// Progress update during bulk operations.
pub struct BulkProgress {
    pub vertices_inserted: usize,
    pub edges_inserted: usize,
    pub current_label: Option<String>,
}
```

---

# 6. StreamingAppender — Row-by-Row Loading

A thin wrapper around BulkWriter that accumulates rows and auto-flushes at the configured batch size. Same consistency class as BulkWriter (at-least-once, no rollback for flushed batches).

**Source:** `crates/uni/src/api/appender.rs`

```rust
impl StreamingAppender {
    /// Append a single row. Auto-flushes when batch_size is reached.
    pub async fn append(
        &mut self, properties: impl Into<HashMap<String, Value>>,
    ) -> Result<()>;

    /// Append an Arrow RecordBatch.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;

    /// Flush remaining rows and finalize.
    pub async fn finish(mut self) -> Result<BulkStats>;

    /// Abort without flushing remaining rows.
    pub fn abort(mut self);

    /// Number of rows currently buffered (not yet flushed).
    pub fn buffered_count(&self) -> usize;
}
```

### AppenderBuilder

```rust
impl<'a> AppenderBuilder<'a> {
    /// Set the auto-flush batch size.
    pub fn batch_size(mut self, size: usize) -> Self;

    /// Defer vector index rebuilds until finish.
    pub fn defer_vector_indexes(mut self, defer: bool) -> Self;

    /// Set the maximum buffer size in bytes.
    pub fn max_buffer_size_bytes(mut self, size: usize) -> Self;

    /// Build the appender (synchronous — no I/O).
    pub fn build(self) -> Result<StreamingAppender>;
}
```

---

# 7. Builders

## QueryBuilder (Session-level)

**Source:** `crates/uni/src/api/query_builder.rs`

```rust
impl<'a> QueryBuilder<'a> {
    /// Bind a parameter.
    pub fn param(mut self, name: &str, value: impl Into<Value>) -> Self;

    /// Bind multiple parameters.
    pub fn params<'p>(
        mut self, params: impl IntoIterator<Item = (&'p str, Value)>,
    ) -> Self;

    /// Set query timeout.
    pub fn timeout(mut self, duration: Duration) -> Self;

    /// Set maximum memory for query execution.
    pub fn max_memory(mut self, bytes: usize) -> Self;

    /// Attach a cancellation token.
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self;

    /// Execute and fetch all results.
    pub async fn fetch_all(self) -> Result<QueryResult>;

    /// Execute and fetch the first row (or None).
    pub async fn fetch_one(self) -> Result<Option<Row>>;

    /// Execute and return a streaming cursor.
    pub async fn cursor(self) -> Result<QueryCursor>;
}
```

## AutoCommitBuilder (Session-level)

```rust
impl<'a> AutoCommitBuilder<'a> {
    /// Bind a parameter.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self;

    /// Bind multiple parameters.
    pub fn params<'p>(
        mut self, params: impl IntoIterator<Item = (&'p str, Value)>,
    ) -> Self;

    /// Set execution timeout.
    pub fn timeout(mut self, duration: Duration) -> Self;

    /// Execute the mutation.
    pub async fn run(self) -> Result<AutoCommitResult>;
}
```

## TxQueryBuilder (Transaction-level)

**Source:** `crates/uni/src/api/transaction.rs`

```rust
impl<'a> TxQueryBuilder<'a> {
    /// Bind a parameter.
    pub fn param(mut self, name: &str, value: impl Into<Value>) -> Self;

    /// Attach a cancellation token.
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self;

    /// Set query timeout.
    pub fn timeout(mut self, duration: Duration) -> Self;

    /// Execute as a mutation and return ExecuteResult.
    pub async fn execute(self) -> Result<ExecuteResult>;

    /// Execute as a query and fetch all rows.
    pub async fn fetch_all(self) -> Result<QueryResult>;

    /// Execute and fetch the first row (or None).
    pub async fn fetch_one(self) -> Result<Option<Row>>;

    /// Execute and return a streaming cursor.
    pub async fn cursor(self) -> Result<QueryCursor>;
}
```

## ExecuteBuilder (Transaction-level)

```rust
impl<'a> ExecuteBuilder<'a> {
    /// Bind a parameter.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self;

    /// Bind multiple parameters.
    pub fn params<'p>(
        mut self, params: impl IntoIterator<Item = (&'p str, Value)>,
    ) -> Self;

    /// Set execution timeout.
    pub fn timeout(mut self, duration: Duration) -> Self;

    /// Execute the mutation.
    pub async fn run(self) -> Result<ExecuteResult>;
}
```

## ApplyBuilder (Transaction-level)

```rust
impl<'a> ApplyBuilder<'a> {
    /// Require that no commits occurred since DERIVE evaluation.
    pub fn require_fresh(mut self) -> Self;

    /// Allow up to `n` versions of gap between evaluation and apply.
    pub fn max_version_gap(mut self, n: u64) -> Self;

    /// Execute the apply operation.
    pub async fn run(self) -> Result<ApplyResult>;
}
```

## TransactionBuilder (Session-level)

```rust
impl<'a> TransactionBuilder<'a> {
    /// Set the transaction wall-clock timeout.
    pub fn timeout(mut self, d: Duration) -> Self;

    /// Set the isolation level.
    pub fn isolation(mut self, level: IsolationLevel) -> Self;

    /// Start the transaction.
    pub async fn start(self) -> Result<Transaction>;
}
```

## LocyBuilder (Session-level)

**Source:** `crates/uni/src/api/locy_builder.rs`

```rust
impl<'a> LocyBuilder<'a> {
    /// Bind a parameter.
    pub fn param(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self;

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(
        mut self, params: impl IntoIterator<Item = (&'p str, Value)>,
    ) -> Self;

    /// Bind parameters from a HashMap.
    pub fn params_map(mut self, params: HashMap<String, Value>) -> Self;

    /// Set evaluation timeout.
    pub fn timeout(mut self, duration: Duration) -> Self;

    /// Set maximum fixpoint iterations.
    pub fn max_iterations(mut self, n: usize) -> Self;

    /// Attach a cancellation token.
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self;

    /// Override evaluation configuration.
    pub fn with_config(mut self, config: LocyConfig) -> Self;

    /// Run the evaluation.
    pub async fn run(self) -> Result<LocyResult>;
}
```

## TxLocyBuilder (Transaction-level)

```rust
impl<'a> TxLocyBuilder<'a> {
    /// Bind a parameter.
    pub fn param(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self;

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(
        mut self, params: impl IntoIterator<Item = (&'p str, Value)>,
    ) -> Self;

    /// Bind parameters from a HashMap.
    pub fn params_map(mut self, params: HashMap<String, Value>) -> Self;

    /// Set evaluation timeout.
    pub fn timeout(mut self, duration: Duration) -> Self;

    /// Set maximum fixpoint iterations.
    pub fn max_iterations(mut self, n: usize) -> Self;

    /// Attach a cancellation token.
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self;

    /// Override evaluation configuration.
    pub fn with_config(mut self, config: LocyConfig) -> Self;

    /// Run the evaluation (DERIVE auto-applies to tx's L0).
    pub async fn run(self) -> Result<LocyResult>;
}
```

## ProfileBuilder (Session-level)

```rust
impl<'a> ProfileBuilder<'a> {
    /// Bind a parameter.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self;

    /// Bind multiple parameters.
    pub fn params<'p>(
        mut self, params: impl IntoIterator<Item = (&'p str, Value)>,
    ) -> Self;

    /// Execute and profile the query.
    pub async fn run(self) -> Result<(QueryResult, ProfileOutput)>;
}
```

---

# 8. Result Types

## AutoCommitResult

**Source:** `crates/uni/src/api/session.rs`

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
    pub version: u64,
    pub metrics: QueryMetrics,
}

impl AutoCommitResult {
    /// Total number of affected rows.
    pub fn affected_rows(&self) -> usize;
}
```

## ExecuteResult

**Source:** `crates/uni-query/src/types.rs`

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

impl ExecuteResult {
    pub fn affected_rows(&self) -> usize;
    pub fn nodes_created(&self) -> usize;
    pub fn nodes_deleted(&self) -> usize;
    pub fn relationships_created(&self) -> usize;
    pub fn relationships_deleted(&self) -> usize;
    pub fn properties_set(&self) -> usize;
    pub fn labels_added(&self) -> usize;
    pub fn labels_removed(&self) -> usize;
    pub fn metrics(&self) -> &QueryMetrics;
}
```

## CommitResult

**Source:** `crates/uni/src/api/transaction.rs`

```rust
pub struct CommitResult {
    pub mutations_committed: usize,
    pub rules_promoted: usize,
    pub version: u64,
    pub started_at_version: u64,
    pub wal_lsn: u64,
    pub duration: Duration,
    pub rule_promotion_errors: Vec<RulePromotionError>,
}

impl CommitResult {
    /// Number of concurrent commits between tx start and commit.
    pub fn version_gap(&self) -> u64;
}

pub struct RulePromotionError {
    pub rule_text: String,
    pub error: String,
}
```

## ApplyResult

```rust
pub struct ApplyResult {
    pub facts_applied: usize,
    pub version_gap: u64,
}
```

## IsolationLevel

```rust
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum IsolationLevel {
    #[default]
    Serialized,
}
```

---

# 9. Query & Row Types

**Source:** `crates/uni-query/src/types.rs`

## QueryResult

```rust
impl QueryResult {
    /// Column names in result order.
    pub fn columns(&self) -> &[String];

    /// Number of rows.
    pub fn len(&self) -> usize;

    /// True if no rows.
    pub fn is_empty(&self) -> bool;

    /// Borrow all rows.
    pub fn rows(&self) -> &[Row];

    /// Consume and return owned rows.
    pub fn into_rows(self) -> Vec<Row>;

    /// Iterate over rows.
    pub fn iter(&self) -> impl Iterator<Item = &Row>;

    /// Query warnings (e.g., missing indexes).
    pub fn warnings(&self) -> &[QueryWarning];

    /// True if any warnings were emitted.
    pub fn has_warnings(&self) -> bool;

    /// Per-query execution metrics.
    pub fn metrics(&self) -> &QueryMetrics;
}
```

## Row

```rust
impl Row {
    /// Column names.
    pub fn columns(&self) -> &[String];

    /// All values in column order.
    pub fn values(&self) -> &[Value];

    /// Consume and return owned values.
    pub fn into_values(self) -> Vec<Value>;

    /// Get a typed value by column name.
    pub fn get<T: FromValue>(&self, column: &str) -> Result<T>;

    /// Get a typed value by column index.
    pub fn get_idx<T: FromValue>(&self, index: usize) -> Result<T>;

    /// Try to get a typed value (returns None on missing/type-mismatch).
    pub fn try_get<T: FromValue>(&self, column: &str) -> Option<T>;

    /// Get a raw Value by column name.
    pub fn value(&self, column: &str) -> Option<&Value>;

    /// Convert to a column-name → value map.
    pub fn as_map(&self) -> HashMap<&str, &Value>;

    /// Convert to a JSON value.
    pub fn to_json(&self) -> serde_json::Value;
}
```

## QueryCursor

```rust
impl QueryCursor {
    /// Column names.
    pub fn columns(&self) -> &[String];

    /// Fetch the next batch of rows.
    pub async fn next_batch(&mut self) -> Option<Result<Vec<Row>>>;

    /// Collect all remaining rows.
    pub async fn collect_remaining(mut self) -> Result<Vec<Row>>;
}
```

## QueryMetrics

```rust
pub struct QueryMetrics {
    pub parse_time: Duration,
    pub plan_time: Duration,
    pub exec_time: Duration,
    pub total_time: Duration,
    pub rows_returned: usize,
    pub rows_scanned: usize,
    pub bytes_read: usize,
    pub plan_cache_hit: bool,
    pub l0_reads: usize,
    pub storage_reads: usize,
    pub cache_hits: usize,
}
```

## QueryWarning

```rust
pub enum QueryWarning {
    IndexUnavailable { label: String, index_name: String, reason: String },
    NoIndexForFilter { label: String, property: String },
    RrfPointContext,
    Other(String),
}
```

---

# 10. Locy Types

## LocyResult (API wrapper)

**Source:** `crates/uni/src/api/locy_result.rs`

Wraps `uni_locy::LocyResult` and adds `QueryMetrics`. Implements `Deref<Target = uni_locy::LocyResult>`.

```rust
impl LocyResult {
    /// Per-query execution metrics.
    pub fn metrics(&self) -> &QueryMetrics;

    /// Derived facts pending materialization (from session-level DERIVE).
    pub fn derived(&self) -> Option<&DerivedFactSet>;

    /// Unwrap to the inner Locy result.
    pub fn into_inner(self) -> uni_locy::LocyResult;

    /// Split into inner result and metrics.
    pub fn into_parts(self) -> (uni_locy::LocyResult, QueryMetrics);
}
```

## LocyResult (inner)

**Source:** `crates/uni-locy/src/result.rs`

```rust
pub struct LocyResult {
    pub derived: HashMap<String, Vec<FactRow>>,
    pub stats: LocyStats,
    pub command_results: Vec<CommandResult>,
    pub warnings: Vec<RuntimeWarning>,
    pub approximate_groups: HashMap<String, Vec<String>>,
    pub derived_fact_set: Option<DerivedFactSet>,
}

impl LocyResult {
    pub fn derived_facts(&self, rule: &str) -> Option<&Vec<FactRow>>;
    pub fn rows(&self) -> Option<&Vec<FactRow>>;
    pub fn columns(&self) -> Option<Vec<String>>;
    pub fn stats(&self) -> &LocyStats;
    pub fn iterations(&self) -> usize;
    pub fn warnings(&self) -> &[RuntimeWarning];
    pub fn has_warning(&self, code: &RuntimeWarningCode) -> bool;
}
```

## DerivedFactSet

```rust
pub struct DerivedFactSet {
    pub vertices: HashMap<String, Vec<Properties>>,
    pub edges: Vec<DerivedEdge>,
    pub stats: LocyStats,
    pub evaluated_at_version: u64,
}

impl DerivedFactSet {
    /// Total number of facts (vertices + edges).
    pub fn fact_count(&self) -> usize;

    /// True if no facts were derived.
    pub fn is_empty(&self) -> bool;
}
```

## DerivedEdge

```rust
pub struct DerivedEdge {
    pub edge_type: String,
    pub source_label: String,
    pub source_properties: Properties,
    pub target_label: String,
    pub target_properties: Properties,
    pub edge_properties: Properties,
}
```

## LocyStats

```rust
pub struct LocyStats {
    pub strata_evaluated: usize,
    pub total_iterations: usize,
    pub derived_nodes: usize,
    pub derived_edges: usize,
    pub evaluation_time: Duration,
    pub queries_executed: usize,
    pub mutations_executed: usize,
    pub peak_memory_bytes: usize,
}
```

## LocyExplainOutput

```rust
pub struct LocyExplainOutput {
    pub plan_text: String,
    pub strata_count: usize,
    pub rule_names: Vec<String>,
    pub has_recursive_strata: bool,
    pub warnings: Vec<String>,
    pub command_count: usize,
}
```

## CommandResult

```rust
pub enum CommandResult {
    Query(Vec<FactRow>),
    Assume(Vec<FactRow>),
    Explain(DerivationNode),
    Abduce(AbductionResult),
    Derive { affected: usize },
    Cypher(Vec<FactRow>),
}

impl CommandResult {
    pub fn as_explain(&self) -> Option<&DerivationNode>;
    pub fn as_query(&self) -> Option<&Vec<FactRow>>;
    pub fn as_abduce(&self) -> Option<&AbductionResult>;
}
```

## DerivationNode

```rust
pub struct DerivationNode {
    pub rule: String,
    pub clause_index: usize,
    pub priority: Option<i64>,
    pub bindings: HashMap<String, Value>,
    pub along_values: HashMap<String, Value>,
    pub children: Vec<DerivationNode>,
    pub graph_fact: Option<String>,
    pub approximate: bool,
    pub proof_probability: Option<f64>,
}
```

---

# 11. Schema Types

**Source:** `crates/uni-common/src/core/schema.rs`

## SchemaBuilder

**Source:** `crates/uni/src/api/schema.rs`

```rust
impl<'a> SchemaBuilder<'a> {
    /// Start defining a label (node type).
    pub fn label(self, name: &str) -> LabelBuilder<'a>;

    /// Start defining an edge type with source/target label constraints.
    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str])
        -> EdgeTypeBuilder<'a>;

    /// Apply all pending schema changes.
    pub async fn apply(self) -> Result<()>;
}
```

## LabelBuilder

```rust
impl<'a> LabelBuilder<'a> {
    /// Add a non-nullable property.
    pub fn property(mut self, name: &str, data_type: DataType) -> Self;

    /// Add a nullable property.
    pub fn property_nullable(mut self, name: &str, data_type: DataType) -> Self;

    /// Add a vector property (shorthand for DataType::Vector { dimensions }).
    pub fn vector(self, name: &str, dimensions: usize) -> Self;

    /// Add an index on a property.
    pub fn index(mut self, property: &str, index_type: IndexType) -> Self;

    /// Finish this label and return to SchemaBuilder.
    pub fn done(mut self) -> SchemaBuilder<'a>;

    /// Chain directly to another label definition.
    pub fn label(self, name: &str) -> LabelBuilder<'a>;

    /// Chain directly to an edge type definition.
    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str])
        -> EdgeTypeBuilder<'a>;

    /// Apply all pending schema changes (shortcut).
    pub async fn apply(self) -> Result<()>;
}
```

## EdgeTypeBuilder

```rust
impl<'a> EdgeTypeBuilder<'a> {
    /// Add a non-nullable property.
    pub fn property(mut self, name: &str, data_type: DataType) -> Self;

    /// Add a nullable property.
    pub fn property_nullable(mut self, name: &str, data_type: DataType) -> Self;

    /// Finish this edge type and return to SchemaBuilder.
    pub fn done(mut self) -> SchemaBuilder<'a>;

    /// Chain directly to a label definition.
    pub fn label(self, name: &str) -> LabelBuilder<'a>;

    /// Chain directly to another edge type definition.
    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str])
        -> EdgeTypeBuilder<'a>;

    /// Apply all pending schema changes (shortcut).
    pub async fn apply(self) -> Result<()>;
}
```

## DataType

```rust
pub enum DataType {
    String, Int32, Int64, Float32, Float64, Bool,
    Timestamp, Date, Time, DateTime, Duration,
    CypherValue,
    Point(PointType),
    Vector { dimensions: usize },
    Crdt(CrdtType),
    List(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
}
```

## Schema

```rust
pub struct Schema {
    pub schema_version: u32,
    pub labels: HashMap<String, LabelMeta>,
    pub edge_types: HashMap<String, EdgeTypeMeta>,
    pub properties: HashMap<String, HashMap<String, PropertyMeta>>,
    pub indexes: Vec<IndexDefinition>,
    pub constraints: Vec<Constraint>,
    pub schemaless_registry: SchemalessEdgeTypeRegistry,
}
```

## Property, Label, and Edge Metadata

```rust
pub struct PropertyMeta {
    pub r#type: DataType,
    pub nullable: bool,
    pub added_in: u32,
    pub state: SchemaElementState,
    pub generation_expression: Option<String>,
}

pub struct LabelMeta {
    pub id: u16,
    pub created_at: DateTime<Utc>,
    pub state: SchemaElementState,
}

pub struct EdgeTypeMeta {
    pub id: u32,
    pub src_labels: Vec<String>,
    pub dst_labels: Vec<String>,
    pub state: SchemaElementState,
}

pub enum SchemaElementState {
    Active,
    Hidden { since: DateTime<Utc>, last_active_snapshot: String },
    Tombstone { since: DateTime<Utc> },
}
```

---

# 12. Index Types

**Source:** `crates/uni-common/src/core/schema.rs`

```rust
#[non_exhaustive]
pub enum IndexDefinition {
    Vector(VectorIndexConfig),
    FullText(FullTextIndexConfig),
    Scalar(ScalarIndexConfig),
    Inverted(InvertedIndexConfig),
    JsonFullText(JsonFtsIndexConfig),
}

impl IndexDefinition {
    pub fn name(&self) -> &str;
    pub fn label(&self) -> &str;
    pub fn metadata(&self) -> &IndexMetadata;
    pub fn metadata_mut(&mut self) -> &mut IndexMetadata;
}
```

## Index Configs

```rust
pub struct VectorIndexConfig {
    pub name: String,
    pub label: String,
    pub property: String,
    pub index_type: VectorIndexType,
    pub metric: DistanceMetric,
    pub embedding_config: Option<EmbeddingConfig>,
    pub metadata: IndexMetadata,
}

pub struct ScalarIndexConfig {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,
    pub index_type: ScalarIndexType,
    pub where_clause: Option<String>,
    pub metadata: IndexMetadata,
}

pub struct FullTextIndexConfig {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,
    pub tokenizer: TokenizerConfig,
    pub with_positions: bool,
    pub metadata: IndexMetadata,
}

pub struct InvertedIndexConfig {
    pub name: String,
    pub label: String,
    pub property: String,
    pub normalize: bool,
    pub max_terms_per_doc: usize,
    pub metadata: IndexMetadata,
}

pub struct IndexMetadata {
    pub status: IndexStatus,
    pub last_built_at: Option<DateTime<Utc>>,
    pub row_count_at_build: Option<u64>,
}

pub enum IndexStatus { Online, Building, Stale, Failed }

#[non_exhaustive]
pub enum VectorIndexType {
    IvfPq { num_partitions: u32, num_sub_vectors: u32, bits_per_subvector: u8 },
    Hnsw { m: u32, ef_construction: u32, ef_search: u32 },
    Flat,
}

#[non_exhaustive]
pub enum ScalarIndexType { BTree, Hash, Bitmap }

#[non_exhaustive]
pub enum DistanceMetric { Cosine, L2, Dot }

#[non_exhaustive]
pub enum TokenizerConfig {
    Standard,
    Whitespace,
    Ngram { min: u8, max: u8 },
    Custom { name: String },
}
```

---

# 13. Configuration

**Source:** `crates/uni-common/src/config.rs`

```rust
pub struct UniConfig {
    pub cache_size: usize,
    pub parallelism: usize,
    pub batch_size: usize,
    pub max_frontier_size: usize,
    pub auto_flush_threshold: usize,
    pub auto_flush_interval: Option<Duration>,
    pub auto_flush_min_mutations: usize,
    pub wal_enabled: bool,
    pub compaction: CompactionConfig,
    pub throttle: WriteThrottleConfig,
    pub file_sandbox: FileSandboxConfig,
    pub query_timeout: Duration,
    pub max_query_memory: usize,
    pub max_transaction_memory: usize,
    pub max_compaction_rows: usize,
    pub enable_vid_labels_index: bool,
    pub max_recursive_cte_iterations: usize,
    pub object_store: ObjectStoreConfig,
    pub index_rebuild: IndexRebuildConfig,
}

pub struct CompactionConfig {
    pub enabled: bool,
    pub max_l1_runs: usize,
    pub max_l1_size_bytes: u64,
    pub max_l1_age: Duration,
    pub check_interval: Duration,
    pub worker_threads: usize,
}

pub struct WriteThrottleConfig {
    pub soft_limit: usize,
    pub hard_limit: usize,
    pub base_delay: Duration,
}

pub struct IndexRebuildConfig {
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub worker_check_interval: Duration,
    pub growth_trigger_ratio: f64,
    pub max_index_age: Option<Duration>,
    pub auto_rebuild_enabled: bool,
}

#[non_exhaustive]
pub enum CloudStorageConfig {
    S3 { bucket: String, region: Option<String>, endpoint: Option<String>,
          access_key_id: Option<String>, secret_access_key: Option<String>,
          session_token: Option<String>, virtual_hosted_style: bool },
    Gcs { bucket: String, service_account_path: Option<String>,
          service_account_key: Option<String> },
    Azure { container: String, account: String,
            access_key: Option<String>, sas_token: Option<String> },
}

impl CloudStorageConfig {
    pub fn s3_from_env(bucket: &str) -> Self;
    pub fn gcs_from_env(bucket: &str) -> Self;
    pub fn azure_from_env(container: &str) -> Self;
    pub fn bucket_name(&self) -> &str;
    pub fn to_url(&self) -> String;
}
```

---

# 14. Observability & Metrics

## DatabaseMetrics

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

## SessionMetrics

```rust
pub struct SessionMetrics {
    pub session_id: String,
    pub active_since: Instant,
    pub queries_executed: u64,
    pub locy_evaluations: u64,
    pub total_query_time: Duration,
    pub transactions_committed: u64,
    pub transactions_rolled_back: u64,
    pub total_rows_returned: u64,
    pub total_rows_scanned: u64,
    pub plan_cache_hits: u64,
    pub plan_cache_misses: u64,
    pub plan_cache_size: usize,
}
```

## SessionCapabilities

```rust
pub struct SessionCapabilities {
    pub can_write: bool,
    pub can_pin: bool,
    pub isolation: IsolationLevel,
    pub has_notifications: bool,
    pub write_lease: Option<WriteLeaseSummary>,
}

pub enum WriteLeaseSummary {
    Local,
    DynamoDB { table: String },
    Custom,
}
```

---

# 15. Commit Notifications

**Source:** `crates/uni/src/api/notifications.rs`

```rust
pub struct CommitNotification {
    pub version: u64,
    pub mutation_count: usize,
    pub labels_affected: Vec<String>,
    pub edge_types_affected: Vec<String>,
    pub rules_promoted: usize,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub tx_id: String,
    pub session_id: String,
    pub causal_version: u64,
}

impl CommitStream {
    /// Await the next commit notification.
    pub async fn next(&mut self) -> Option<CommitNotification>;
}
```

## WatchBuilder

```rust
impl WatchBuilder {
    /// Filter to commits affecting these labels.
    pub fn labels(mut self, labels: &[&str]) -> Self;

    /// Filter to commits affecting these edge types.
    pub fn edge_types(mut self, types: &[&str]) -> Self;

    /// Debounce: at most one notification per interval.
    pub fn debounce(mut self, interval: Duration) -> Self;

    /// Exclude commits from a specific session (self-loop prevention).
    pub fn exclude_session(mut self, session_id: &str) -> Self;

    /// Build the filtered commit stream.
    pub fn build(self) -> CommitStream;
}
```

---

# 16. Session Hooks

**Source:** `crates/uni/src/api/hooks.rs`

```rust
pub trait SessionHook: Send + Sync {
    /// Called before query execution. Return Err to reject.
    fn before_query(&self, _ctx: &HookContext) -> Result<()> { Ok(()) }

    /// Called after query execution (infallible).
    fn after_query(&self, _ctx: &HookContext, _metrics: &QueryMetrics) {}

    /// Called before commit. Return Err to abort the commit.
    fn before_commit(&self, _ctx: &CommitHookContext) -> Result<()> { Ok(()) }

    /// Called after commit (infallible — commit is already durable).
    fn after_commit(&self, _ctx: &CommitHookContext, _result: &CommitResult) {}
}

pub struct HookContext {
    pub session_id: String,
    pub query_text: String,
    pub query_type: QueryType,
    pub params: HashMap<String, Value>,
}

pub struct CommitHookContext {
    pub session_id: String,
    pub tx_id: String,
    pub mutation_count: usize,
}

pub enum QueryType {
    Cypher,
    Locy,
    Execute,
}
```

---

# 17. Prepared Statements

**Source:** `crates/uni/src/api/prepared.rs`

## PreparedQuery

```rust
impl PreparedQuery {
    /// Execute with positional parameters.
    pub async fn execute(&self, params: &[(&str, Value)]) -> Result<QueryResult>;

    /// Start a parameter binder.
    pub fn bind(&self) -> PreparedQueryBinder<'_>;

    /// Get the original query text.
    pub fn query_text(&self) -> &str;
}

impl<'a> PreparedQueryBinder<'a> {
    /// Bind a parameter.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self;

    /// Execute with bound parameters.
    pub async fn execute(self) -> Result<QueryResult>;
}
```

## PreparedLocy

```rust
impl PreparedLocy {
    /// Execute with positional parameters.
    pub async fn execute(&self, params: &[(&str, Value)]) -> Result<LocyResult>;

    /// Start a parameter binder.
    pub fn bind(&self) -> PreparedLocyBinder<'_>;

    /// Get the original program text.
    pub fn program_text(&self) -> &str;
}

impl<'a> PreparedLocyBinder<'a> {
    /// Bind a parameter.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self;

    /// Execute with bound parameters.
    pub async fn execute(self) -> Result<LocyResult>;
}
```

---

# 18. Session Templates

**Source:** `crates/uni/src/api/template.rs`

```rust
impl SessionTemplateBuilder {
    /// Pre-set a scoped parameter.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self;

    /// Pre-register Locy rules.
    pub fn rules(mut self, program: &str) -> Result<Self>;

    /// Pre-register a session hook.
    pub fn hook(mut self, hook: impl SessionHook + 'static) -> Self;

    /// Set the default query timeout for sessions created from this template.
    pub fn query_timeout(mut self, duration: Duration) -> Self;

    /// Set the default transaction timeout.
    pub fn transaction_timeout(mut self, duration: Duration) -> Self;

    /// Build the template.
    pub fn build(self) -> Result<SessionTemplate>;
}

impl SessionTemplate {
    /// Create a new Session from this template.
    pub fn create(&self) -> Session;
}
```

---

# 19. Multi-Agent Access

**Source:** `crates/uni/src/api/multi_agent.rs`

```rust
#[non_exhaustive]
pub enum WriteLease {
    /// Local single-process mode (default).
    Local,
    /// DynamoDB-based distributed lease.
    DynamoDB { table: String },
    /// Custom lease provider.
    Custom(Box<dyn WriteLeaseProvider>),
}

pub struct LeaseGuard {
    pub lease_id: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

#[async_trait]
pub trait WriteLeaseProvider: Send + Sync {
    async fn acquire(&self) -> Result<LeaseGuard>;
    async fn heartbeat(&self, guard: &LeaseGuard) -> Result<()>;
    async fn release(&self, guard: LeaseGuard) -> Result<()>;
}
```

---

# 20. Synchronous (Blocking) API

**Source:** `crates/uni/src/api/sync.rs`

The synchronous API mirrors the async API with blocking wrappers. Each async type has a `*Sync` counterpart.

| Async Type | Blocking Type |
|---|---|
| `Uni` (build only) | `UniBuilder::build_sync()` |
| — | `UniSync` |
| `Session` | `SessionSync<'a>` |
| `Transaction` | `TransactionSync<'a>` |
| `QueryBuilder` | `QueryBuilderSync<'s, 'a>` |
| `AutoCommitBuilder` | `AutoCommitBuilderSync<'s, 'a>` |
| `ProfileBuilder` | `ProfileBuilderSync<'s, 'a>` |
| `LocyBuilder` | `LocyBuilderSync<'s, 'a>` |
| `TransactionBuilder` | `TransactionBuilderSync<'s, 'a>` |
| `ExecuteBuilder` | `ExecuteBuilderSync<'t, 'a>` |
| `TxQueryBuilder` | `TxQueryBuilderSync<'t, 'a>` |
| `ApplyBuilder` | `ApplyBuilderSync<'t, 'a>` |
| `TxLocyBuilder` | `TxLocyBuilderSync<'t, 'a>` |
| `SchemaBuilder` | `SchemaBuilderSync<'a>` |
| `LabelBuilder` | `LabelBuilderSync<'a>` |
| `EdgeTypeBuilder` | `EdgeTypeBuilderSync<'a>` |

### UniSync

```rust
impl UniSync {
    /// Create from an existing Uni instance.
    pub fn new(inner: Uni) -> Result<Self>;

    /// Create an in-memory database (blocking).
    pub fn in_memory() -> Result<Self>;

    /// Create a session.
    pub fn session(&self) -> SessionSync<'_>;

    /// Get the current schema.
    pub fn schema_meta(&self) -> Arc<Schema>;

    /// Start a schema modification.
    pub fn schema(&self) -> SchemaBuilderSync<'_>;

    /// Shut down the database.
    pub fn shutdown(mut self) -> Result<()>;
}
```

All `*Sync` types have the same methods as their async counterparts, with `async fn` replaced by `fn` (internally using `tokio::Runtime::block_on`).

---

# 21. Error Types

**Source:** `crates/uni-common/src/api/error.rs`

```rust
#[non_exhaustive]
pub enum UniError {
    // ── Resource Errors ──
    NotFound { path: PathBuf },
    LabelNotFound { label: String },
    EdgeTypeNotFound { edge_type: String },
    PropertyNotFound { property: String, entity_type: String, label: String },
    IndexNotFound { index: String },
    SnapshotNotFound { snapshot_id: String },

    // ── Schema Errors ──
    Schema { message: String },
    LabelAlreadyExists { label: String },
    EdgeTypeAlreadyExists { edge_type: String },
    InvalidIdentifier { name: String, reason: String },

    // ── Query Errors ──
    Parse { message: String, position: Option<usize>,
            line: Option<usize>, column: Option<usize>,
            context: Option<String> },
    Query { message: String, query: Option<String> },
    Type { expected: String, actual: String },
    InvalidArgument { arg: String, message: String },

    // ── Transaction Errors ──
    Transaction { message: String },
    TransactionConflict { message: String },
    TransactionAlreadyCompleted,
    TransactionExpired { tx_id: String, hint: &'static str },
    CommitTimeout { tx_id: String, hint: &'static str },

    // ── Access Control ──
    ReadOnly { operation: String },
    PermissionDenied { action: String },
    DatabaseLocked,
    WriteContextAlreadyActive { session_id: String, hint: &'static str },

    // ── Resource Limits ──
    MemoryLimitExceeded { limit_bytes: usize },
    Timeout { timeout_ms: u64 },
    Cancelled,

    // ── Locy-Specific ──
    StaleDerivedFacts { version_gap: u64 },
    RuleConflict { rule_name: String },

    // ── Hooks ──
    HookRejected { message: String },

    // ── Constraint Violations ──
    Constraint { message: String },

    // ── Storage & I/O ──
    Storage { message: String, source: Option<Box<dyn std::error::Error + Send + Sync>> },
    Io(std::io::Error),

    // ── Internal ──
    Internal(anyhow::Error),
}
```
