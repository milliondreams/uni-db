// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::api::Uni;
use crate::api::locy_builder::{LocyBuilder, TxLocyBuilder};
use crate::api::session::{
    AutoCommitBuilder, AutoCommitResult, ProfileBuilder, QueryBuilder, Session, TransactionBuilder,
};
use crate::api::transaction::{
    ApplyBuilder, ApplyResult, CommitResult, Transaction, TxQueryBuilder,
};
use std::sync::Arc;
use uni_common::core::schema::{DataType, Schema};
use uni_common::{Result, UniError, Value};
use uni_locy::DerivedFactSet;

use crate::api::locy_result::LocyResult;
use uni_query::{ExecuteResult, ExplainOutput, ProfileOutput, QueryCursor, QueryResult, Row};

/// Blocking API wrapper for Uni.
pub struct UniSync {
    inner: Option<Uni>,
    rt: tokio::runtime::Runtime,
}

impl UniSync {
    pub fn new(inner: Uni) -> Result<Self> {
        let rt = tokio::runtime::Runtime::new().map_err(UniError::Io)?;
        Ok(Self {
            inner: Some(inner),
            rt,
        })
    }

    /// Open an in-memory database (blocking)
    pub fn in_memory() -> Result<Self> {
        let rt = tokio::runtime::Runtime::new().map_err(UniError::Io)?;
        let inner = rt.block_on(Uni::in_memory().build())?;
        Ok(Self {
            inner: Some(inner),
            rt,
        })
    }

    fn inner(&self) -> &Uni {
        self.inner.as_ref().expect("UniSync already shut down")
    }

    /// Create a new session (sync wrapper).
    pub fn session(&self) -> SessionSync<'_> {
        SessionSync {
            session: self.inner().session(),
            rt: &self.rt,
        }
    }

    pub fn schema_meta(&self) -> Arc<Schema> {
        self.inner().get_schema()
    }

    pub fn schema(&self) -> SchemaBuilderSync<'_> {
        SchemaBuilderSync {
            inner: self.inner().schema(),
            rt: &self.rt,
        }
    }

    /// Shutdown the database gracefully (blocking).
    ///
    /// Note: This consumes self, which prevents the Drop impl from also
    /// triggering shutdown. Use this for explicit shutdown with error handling.
    pub fn shutdown(mut self) -> Result<()> {
        // Take ownership of the inner Uni to prevent Drop from also running
        if let Some(uni) = self.inner.take() {
            let result = self.rt.block_on(uni.shutdown());

            // Prevent Drop from running by forgetting self
            // (we've already done the cleanup in the async shutdown)
            std::mem::forget(self);

            result
        } else {
            Ok(()) // Already shut down
        }
    }
}

impl Drop for UniSync {
    fn drop(&mut self) {
        if let Some(ref uni) = self.inner {
            uni.inner.shutdown_handle.shutdown_blocking();
            tracing::debug!("UniSync dropped");
        }
    }
}

// ── SessionSync ──────────────────────────────────────────────────────────

/// Blocking wrapper around [`Session`].
///
/// All async methods on `Session` are wrapped with `block_on()` to provide
/// a synchronous API. Created via [`UniSync::session()`].
pub struct SessionSync<'a> {
    session: Session,
    rt: &'a tokio::runtime::Runtime,
}

impl<'a> SessionSync<'a> {
    // ── Cypher Reads ──────────────────────────────────────────────────

    /// Execute a read-only Cypher query.
    pub fn query(&self, cypher: &str) -> Result<QueryResult> {
        self.rt.block_on(self.session.query(cypher))
    }

    /// Execute a read-only Cypher query with a builder for parameters.
    pub fn query_with<'s>(&'s self, cypher: &str) -> QueryBuilderSync<'s, 'a> {
        QueryBuilderSync {
            inner: self.session.query_with(cypher),
            rt: self.rt,
        }
    }

    /// Execute a query returning a cursor for streaming results.
    pub fn query_cursor(&self, cypher: &str) -> Result<QueryCursor> {
        self.rt.block_on(self.session.query_cursor(cypher))
    }

    // ── Cypher Writes ─────────────────────────────────────────────────

    /// Execute a Cypher mutation (CREATE, SET, DELETE, etc.).
    pub fn execute(&self, cypher: &str) -> Result<AutoCommitResult> {
        self.rt.block_on(self.session.execute(cypher))
    }

    /// Execute a Cypher mutation with a builder for parameters.
    pub fn execute_with<'s>(&'s self, cypher: &str) -> AutoCommitBuilderSync<'s, 'a> {
        AutoCommitBuilderSync {
            inner: self.session.execute_with(cypher),
            rt: self.rt,
        }
    }

    // ── Planning & Introspection ──────────────────────────────────────

    /// Explain a Cypher query plan without executing it.
    pub fn explain(&self, cypher: &str) -> Result<ExplainOutput> {
        self.rt.block_on(self.session.explain(cypher))
    }

    /// Profile a Cypher query execution.
    pub fn profile(&self, cypher: &str) -> Result<(QueryResult, ProfileOutput)> {
        self.rt.block_on(self.session.profile(cypher))
    }

    /// Profile a Cypher query with a builder for parameters.
    pub fn profile_with(&self, cypher: &str) -> ProfileBuilderSync<'_, 'a> {
        ProfileBuilderSync {
            inner: self.session.profile_with(cypher),
            rt: self.rt,
        }
    }

    // ── Locy Evaluation ───────────────────────────────────────────────

    /// Evaluate a Locy program with default configuration.
    pub fn locy(&self, program: &str) -> Result<LocyResult> {
        self.rt.block_on(self.session.locy(program))
    }

    /// Evaluate a Locy program with parameters using a builder.
    pub fn locy_with<'s>(&'s self, program: &str) -> LocyBuilderSync<'s, 'a> {
        LocyBuilderSync {
            inner: self.session.locy_with(program),
            rt: self.rt,
        }
    }

    // ── Rule Management ───────────────────────────────────────────────

    /// Register Locy rules for this session.
    pub fn register_rules(&self, program: &str) -> Result<()> {
        self.session.register_rules(program)
    }

    /// Clear all registered Locy rules for this session.
    pub fn clear_rules(&self) {
        self.session.clear_rules()
    }

    /// Compile a Locy program without executing it.
    pub fn compile_locy(&self, program: &str) -> Result<uni_locy::CompiledProgram> {
        self.session.compile_locy(program)
    }

    // ── Custom Functions ──────────────────────────────────────────────

    /// Register a custom scalar function available to Cypher queries.
    pub fn register_function<F>(&self, name: &str, func: F) -> Result<()>
    where
        F: Fn(&[Value]) -> Result<Value> + Send + Sync + 'static,
    {
        self.session.register_function(name, func)
    }

    // ── Transactions ──────────────────────────────────────────────────

    /// Create a new transaction for multi-statement writes.
    pub fn tx(&self) -> Result<TransactionSync<'a>> {
        let tx = self.rt.block_on(self.session.tx())?;
        Ok(TransactionSync { tx, rt: self.rt })
    }

    /// Create a transaction with builder options (timeout, isolation level).
    pub fn tx_with(&self) -> TransactionBuilderSync<'_, 'a> {
        TransactionBuilderSync {
            inner: self.session.tx_with(),
            rt: self.rt,
        }
    }

    /// Create a bulk writer for efficient data loading.
    pub fn bulk_writer(&self) -> crate::api::bulk::BulkWriterBuilder {
        self.session.bulk_writer()
    }

    /// Create a streaming appender for row-by-row data loading.
    pub fn appender(&self, label: &str) -> crate::api::appender::AppenderBuilder<'_> {
        self.session.appender(label)
    }

    // ── Commit Notifications ─────────────────────────────────────────

    /// Watch for all commit notifications.
    pub fn watch(&self) -> crate::api::notifications::CommitStream {
        self.session.watch()
    }

    /// Watch for commit notifications with filters.
    pub fn watch_with(&self) -> crate::api::notifications::WatchBuilder {
        self.session.watch_with()
    }

    // ── Hooks ─────────────────────────────────────────────────────────

    /// Add a session hook for query/commit interception.
    pub fn add_hook(&mut self, hook: impl crate::api::hooks::SessionHook + 'static) {
        self.session.add_hook(hook)
    }

    // ── Version Pinning ──────────────────────────────────────────────

    /// Pin this session to a specific snapshot version.
    pub fn pin_to_version(&mut self, snapshot_id: &str) -> Result<()> {
        self.rt.block_on(self.session.pin_to_version(snapshot_id))
    }

    /// Pin this session to a specific timestamp.
    pub fn pin_to_timestamp(&mut self, ts: chrono::DateTime<chrono::Utc>) -> Result<()> {
        self.rt.block_on(self.session.pin_to_timestamp(ts))
    }

    /// Unpin the session, returning to the live database state.
    pub fn refresh(&mut self) -> Result<()> {
        self.rt.block_on(self.session.refresh())
    }

    // ── Planning & Introspection ──────────────────────────────────────

    /// Explain a Locy program without executing it.
    pub fn explain_locy(
        &self,
        program: &str,
    ) -> Result<crate::api::locy_result::LocyExplainOutput> {
        self.session.explain_locy(program)
    }

    // ── Prepared Statements ──────────────────────────────────────────

    /// Prepare a Cypher query for repeated execution.
    pub fn prepare(&self, cypher: &str) -> Result<crate::api::prepared::PreparedQuery> {
        self.rt.block_on(self.session.prepare(cypher))
    }

    /// Prepare a Locy program for repeated evaluation.
    pub fn prepare_locy(&self, program: &str) -> Result<crate::api::prepared::PreparedLocy> {
        self.session.prepare_locy(program)
    }

    // ── Scoped Parameters ─────────────────────────────────────────────

    /// Set a session-scoped parameter.
    pub fn set<K: Into<String>, V: Into<Value>>(&mut self, key: K, value: V) -> &mut Self {
        self.session.set(key, value);
        self
    }

    /// Get a session-scoped parameter.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.session.get(key)
    }

    // ── Lifecycle & Observability ─────────────────────────────────────

    /// Get the session ID.
    pub fn id(&self) -> &str {
        self.session.id()
    }

    /// Query the capabilities of this session.
    pub fn capabilities(&self) -> crate::api::session::SessionCapabilities {
        self.session.capabilities()
    }

    /// Snapshot the session's accumulated metrics.
    pub fn metrics(&self) -> crate::api::session::SessionMetrics {
        self.session.metrics()
    }

    /// Cancel all in-flight queries in this session.
    pub fn cancel(&self) {
        self.session.cancel()
    }
}

// ── QueryBuilderSync ──────────────────────────────────────────────

/// Blocking wrapper around [`QueryBuilder`].
pub struct QueryBuilderSync<'s, 'a> {
    inner: QueryBuilder<'s>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'s, 'a> QueryBuilderSync<'s, 'a> {
    /// Bind a parameter to the query.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.inner = self.inner.param(key, value);
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        self.inner = self.inner.params(params);
        self
    }

    /// Set maximum execution time for this query.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.inner = self.inner.timeout(duration);
        self
    }

    /// Set maximum memory per query in bytes.
    pub fn max_memory(mut self, bytes: usize) -> Self {
        self.inner = self.inner.max_memory(bytes);
        self
    }

    /// Execute the query and fetch all results.
    pub fn fetch_all(self) -> Result<QueryResult> {
        self.rt.block_on(self.inner.fetch_all())
    }

    /// Execute the query and return the first row, or `None` if empty.
    pub fn fetch_one(self) -> Result<Option<Row>> {
        self.rt.block_on(self.inner.fetch_one())
    }
}

// ── AutoCommitBuilderSync ───────────────────────────────────────────────

/// Blocking wrapper around [`AutoCommitBuilder`].
pub struct AutoCommitBuilderSync<'s, 'a> {
    inner: AutoCommitBuilder<'s>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'s, 'a> AutoCommitBuilderSync<'s, 'a> {
    /// Bind a parameter to the mutation.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.inner = self.inner.param(key, value);
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        self.inner = self.inner.params(params);
        self
    }

    /// Set maximum execution time for this mutation.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.inner = self.inner.timeout(duration);
        self
    }

    /// Execute the mutation and return the result with database version.
    pub fn run(self) -> Result<AutoCommitResult> {
        self.rt.block_on(self.inner.run())
    }
}

// ── ProfileBuilderSync ──────────────────────────────────────────────────

/// Blocking wrapper around [`ProfileBuilder`].
pub struct ProfileBuilderSync<'s, 'a> {
    inner: ProfileBuilder<'s>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'s, 'a> ProfileBuilderSync<'s, 'a> {
    /// Bind a parameter to the profiled query.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.inner = self.inner.param(key, value);
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        self.inner = self.inner.params(params);
        self
    }

    /// Execute the profiled query and return results with profiling output.
    pub fn run(self) -> Result<(QueryResult, ProfileOutput)> {
        self.rt.block_on(self.inner.run())
    }
}

// ── LocyBuilderSync ──────────────────────────────────────────────

/// Blocking wrapper around [`LocyBuilder`].
pub struct LocyBuilderSync<'s, 'a> {
    inner: LocyBuilder<'s>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'s, 'a> LocyBuilderSync<'s, 'a> {
    /// Bind a single parameter.
    pub fn param(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.inner = self.inner.param(name, value);
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        self.inner = self.inner.params(params);
        self
    }

    /// Override the evaluation timeout.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.inner = self.inner.timeout(duration);
        self
    }

    /// Override the maximum fixpoint iteration count.
    pub fn max_iterations(mut self, n: usize) -> Self {
        self.inner = self.inner.max_iterations(n);
        self
    }

    /// Apply a fully configured [`LocyConfig`](uni_locy::LocyConfig).
    pub fn with_config(mut self, config: uni_locy::LocyConfig) -> Self {
        self.inner = self.inner.with_config(config);
        self
    }

    /// Evaluate the program and return the full [`LocyResult`].
    pub fn run(self) -> Result<LocyResult> {
        self.rt.block_on(self.inner.run())
    }
}

// ── TransactionSync ──────────────────────────────────────────────────────

pub struct TransactionSync<'a> {
    tx: Transaction,
    rt: &'a tokio::runtime::Runtime,
}

impl<'a> TransactionSync<'a> {
    pub fn query(&self, cypher: &str) -> Result<QueryResult> {
        self.rt.block_on(self.tx.query(cypher))
    }

    /// Execute a Cypher query with parameters using a builder.
    pub fn query_with<'t>(&'t self, cypher: &str) -> TxQueryBuilderSync<'t, 'a> {
        TxQueryBuilderSync {
            inner: self.tx.query_with(cypher),
            rt: self.rt,
        }
    }

    pub fn execute(&self, cypher: &str) -> Result<ExecuteResult> {
        self.rt.block_on(self.tx.execute(cypher))
    }

    /// Execute a mutation with parameters using a builder.
    pub fn execute_with<'t>(&'t self, cypher: &str) -> ExecuteBuilderSync<'t, 'a> {
        ExecuteBuilderSync {
            inner: self.tx.execute_with(cypher),
            rt: self.rt,
        }
    }

    /// Evaluate a Locy program within the transaction.
    pub fn locy(&self, program: &str) -> Result<LocyResult> {
        self.rt.block_on(self.tx.locy(program))
    }

    /// Evaluate a Locy program with parameters using a builder.
    pub fn locy_with<'t>(&'t self, program: &str) -> TxLocyBuilderSync<'t, 'a> {
        TxLocyBuilderSync {
            inner: self.tx.locy_with(program),
            rt: self.rt,
        }
    }

    /// Apply a `DerivedFactSet` to this transaction.
    pub fn apply(&self, derived: DerivedFactSet) -> Result<ApplyResult> {
        self.rt.block_on(self.tx.apply(derived))
    }

    /// Apply a `DerivedFactSet` with staleness controls.
    pub fn apply_with(&self, derived: DerivedFactSet) -> ApplyBuilderSync<'_, 'a> {
        ApplyBuilderSync {
            inner: self.tx.apply_with(derived),
            rt: self.rt,
        }
    }

    /// Prepare a Cypher query for repeated execution.
    pub fn prepare(&self, cypher: &str) -> Result<crate::api::prepared::PreparedQuery> {
        self.rt.block_on(self.tx.prepare(cypher))
    }

    /// Prepare a Locy program for repeated evaluation.
    pub fn prepare_locy(&self, program: &str) -> Result<crate::api::prepared::PreparedLocy> {
        self.tx.prepare_locy(program)
    }

    pub fn commit(self) -> Result<CommitResult> {
        self.rt.block_on(self.tx.commit())
    }

    pub fn rollback(self) {
        self.tx.rollback()
    }

    /// Check if the transaction has uncommitted changes.
    pub fn is_dirty(&self) -> bool {
        self.tx.is_dirty()
    }

    /// Get the transaction ID.
    pub fn id(&self) -> &str {
        self.tx.id()
    }
}

// ── ExecuteBuilderSync ──────────────────────────────────────────────────

/// Blocking wrapper around [`ExecuteBuilder`](crate::api::transaction::ExecuteBuilder).
pub struct ExecuteBuilderSync<'t, 'a> {
    inner: crate::api::transaction::ExecuteBuilder<'t>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'t, 'a> ExecuteBuilderSync<'t, 'a> {
    /// Bind a parameter to the mutation.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.inner = self.inner.param(key, value);
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        self.inner = self.inner.params(params);
        self
    }

    /// Set maximum execution time for this mutation.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.inner = self.inner.timeout(duration);
        self
    }

    /// Execute the mutation and return affected row count with detailed stats.
    pub fn run(self) -> Result<ExecuteResult> {
        self.rt.block_on(self.inner.run())
    }
}

// ── TransactionBuilderSync ───────────────────────────────────────────────

/// Blocking wrapper around [`TransactionBuilder`](crate::api::session::TransactionBuilder).
pub struct TransactionBuilderSync<'s, 'a> {
    inner: TransactionBuilder<'s>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'s, 'a> TransactionBuilderSync<'s, 'a> {
    /// Set the transaction timeout.
    pub fn timeout(mut self, d: std::time::Duration) -> Self {
        self.inner = self.inner.timeout(d);
        self
    }

    /// Set the isolation level.
    pub fn isolation(mut self, level: crate::api::transaction::IsolationLevel) -> Self {
        self.inner = self.inner.isolation(level);
        self
    }

    /// Start the transaction.
    pub fn start(self) -> Result<TransactionSync<'a>> {
        let tx = self.rt.block_on(self.inner.start())?;
        Ok(TransactionSync { tx, rt: self.rt })
    }
}

// ── TxQueryBuilderSync ─────────────────────────────────────────

/// Blocking wrapper around [`TxQueryBuilder`].
pub struct TxQueryBuilderSync<'t, 'a> {
    inner: TxQueryBuilder<'t>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'t, 'a> TxQueryBuilderSync<'t, 'a> {
    /// Bind a parameter.
    pub fn param(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.inner = self.inner.param(name, value);
        self
    }

    /// Execute a mutation and return affected row count with detailed stats.
    pub fn execute(self) -> Result<ExecuteResult> {
        self.rt.block_on(self.inner.execute())
    }

    /// Execute as a query and return rows.
    pub fn fetch_all(self) -> Result<QueryResult> {
        self.rt.block_on(self.inner.fetch_all())
    }

    /// Execute the query and return the first row, or `None` if empty.
    pub fn fetch_one(self) -> Result<Option<Row>> {
        self.rt.block_on(self.inner.fetch_one())
    }
}

// ── ApplyBuilderSync ────────────────────────────────────────────────────

/// Blocking wrapper around [`ApplyBuilder`].
pub struct ApplyBuilderSync<'t, 'a> {
    inner: ApplyBuilder<'t>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'t, 'a> ApplyBuilderSync<'t, 'a> {
    /// Require that no commits occurred between DERIVE evaluation and apply.
    pub fn require_fresh(mut self) -> Self {
        self.inner = self.inner.require_fresh();
        self
    }

    /// Allow up to `n` versions of gap.
    pub fn max_version_gap(mut self, n: u64) -> Self {
        self.inner = self.inner.max_version_gap(n);
        self
    }

    /// Execute the apply operation.
    pub fn run(self) -> Result<ApplyResult> {
        self.rt.block_on(self.inner.run())
    }
}

// ── TxLocyBuilderSync ──────────────────────────────────────────

/// Blocking wrapper around [`TxLocyBuilder`].
pub struct TxLocyBuilderSync<'t, 'a> {
    inner: TxLocyBuilder<'t>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'t, 'a> TxLocyBuilderSync<'t, 'a> {
    /// Bind a single parameter.
    pub fn param(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.inner = self.inner.param(name, value);
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        self.inner = self.inner.params(params);
        self
    }

    /// Override the evaluation timeout.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.inner = self.inner.timeout(duration);
        self
    }

    /// Override the maximum fixpoint iteration count.
    pub fn max_iterations(mut self, n: usize) -> Self {
        self.inner = self.inner.max_iterations(n);
        self
    }

    /// Apply a fully configured [`LocyConfig`](uni_locy::LocyConfig).
    pub fn with_config(mut self, config: uni_locy::LocyConfig) -> Self {
        self.inner = self.inner.with_config(config);
        self
    }

    /// Evaluate the program and return the full [`LocyResult`].
    pub fn run(self) -> Result<LocyResult> {
        self.rt.block_on(self.inner.run())
    }
}

// ── Schema Builders (unchanged) ──────────────────────────────────────────

pub struct SchemaBuilderSync<'a> {
    inner: crate::api::schema::SchemaBuilder<'a>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'a> SchemaBuilderSync<'a> {
    pub fn label(self, name: &str) -> LabelBuilderSync<'a> {
        LabelBuilderSync {
            inner: self.inner.label(name),
            rt: self.rt,
        }
    }

    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str]) -> EdgeTypeBuilderSync<'a> {
        EdgeTypeBuilderSync {
            inner: self.inner.edge_type(name, from, to),
            rt: self.rt,
        }
    }

    pub fn apply(self) -> Result<()> {
        self.rt.block_on(self.inner.apply())
    }
}

pub struct LabelBuilderSync<'a> {
    inner: crate::api::schema::LabelBuilder<'a>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'a> LabelBuilderSync<'a> {
    pub fn property(mut self, name: &str, data_type: DataType) -> Self {
        self.inner = self.inner.property(name, data_type);
        self
    }

    pub fn property_nullable(mut self, name: &str, data_type: DataType) -> Self {
        self.inner = self.inner.property_nullable(name, data_type);
        self
    }

    pub fn vector(mut self, name: &str, dimensions: usize) -> Self {
        self.inner = self.inner.vector(name, dimensions);
        self
    }

    pub fn done(self) -> SchemaBuilderSync<'a> {
        SchemaBuilderSync {
            inner: self.inner.done(),
            rt: self.rt,
        }
    }

    pub fn label(self, name: &str) -> LabelBuilderSync<'a> {
        self.done().label(name)
    }

    pub fn apply(self) -> Result<()> {
        self.rt.block_on(self.inner.apply())
    }
}

pub struct EdgeTypeBuilderSync<'a> {
    inner: crate::api::schema::EdgeTypeBuilder<'a>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'a> EdgeTypeBuilderSync<'a> {
    pub fn property(mut self, name: &str, data_type: DataType) -> Self {
        self.inner = self.inner.property(name, data_type);
        self
    }

    pub fn property_nullable(mut self, name: &str, data_type: DataType) -> Self {
        self.inner = self.inner.property_nullable(name, data_type);
        self
    }

    pub fn done(self) -> SchemaBuilderSync<'a> {
        SchemaBuilderSync {
            inner: self.inner.done(),
            rt: self.rt,
        }
    }

    pub fn apply(self) -> Result<()> {
        self.rt.block_on(self.inner.apply())
    }
}
