// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Session — the primary read scope for all database access.
//!
//! Sessions are cheap, synchronous, and infallible to create. All reads go
//! through sessions, and sessions are the factory for transactions (writes).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::instrument;
use uuid::Uuid;

use crate::api::UniInner;
use crate::api::hooks::{HookContext, QueryType, SessionHook};
use crate::api::impl_locy::{self, LocyRuleRegistry};
use crate::api::locy_result::LocyResult;
use crate::api::transaction::{IsolationLevel, Transaction};
use uni_common::{Result, UniError, Value};
use uni_query::{
    ExecuteResult, ExplainOutput, ProfileOutput, QueryCursor, QueryMetrics, QueryResult, Row,
};

/// Atomic counters for plan cache hits/misses, shared between Session and its
/// query execution helpers.
pub(crate) struct PlanCacheMetrics {
    pub(crate) hits: AtomicU64,
    pub(crate) misses: AtomicU64,
}

/// Describes the capabilities of a session in its current mode.
///
/// This is a snapshot — capabilities may change if the underlying database
/// configuration changes (e.g., read-only mode toggled).
#[derive(Debug, Clone)]
pub struct SessionCapabilities {
    /// Whether the session can create transactions and execute writes.
    pub can_write: bool,
    /// Whether the session supports version pinning (read-at-version).
    pub can_pin: bool,
    /// The isolation level used for transactions in this session.
    pub isolation: IsolationLevel,
    /// Whether commit notifications are available.
    pub has_notifications: bool,
    /// Write lease strategy in effect, if any.
    pub write_lease: Option<WriteLeaseSummary>,
}

/// Summary of the write lease strategy, suitable for capability snapshots.
///
/// This is a `Clone`-friendly description of the [`WriteLease`](crate::WriteLease)
/// variant without carrying the actual provider trait object.
#[derive(Debug, Clone)]
pub enum WriteLeaseSummary {
    /// Local single-process lock.
    Local,
    /// DynamoDB-based distributed lease.
    DynamoDB { table: String },
    /// Custom lease provider (opaque).
    Custom,
}

/// Result of an auto-committed mutation executed via [`AutoCommitBuilder::run()`].
///
/// Compared to [`ExecuteResult`], this additionally carries the database
/// `version` after the commit — useful for optimistic concurrency control.
#[derive(Debug)]
pub struct AutoCommitResult {
    /// Number of affected rows (result rows or mutation count delta).
    affected: usize,
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
    /// Number of properties removed by `REMOVE` clauses.
    pub properties_removed: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
    /// Database version after the auto-committed mutation.
    pub version: u64,
    pub metrics: QueryMetrics,
}

impl AutoCommitResult {
    /// Number of affected rows (result rows or mutation count).
    ///
    /// Convenience accessor that mirrors [`ExecuteResult::affected_rows()`] so
    /// code switching from `ExecuteResult` to `AutoCommitResult` compiles unchanged.
    pub fn affected_rows(&self) -> usize {
        self.affected
    }
}

/// Internal atomic counters for session-level metrics.
pub(crate) struct SessionMetricsInner {
    pub(crate) queries_executed: AtomicU64,
    pub(crate) locy_evaluations: AtomicU64,
    pub(crate) total_query_time_us: AtomicU64,
    pub(crate) transactions_committed: AtomicU64,
    pub(crate) transactions_rolled_back: AtomicU64,
    pub(crate) total_rows_returned: AtomicU64,
    pub(crate) total_rows_scanned: AtomicU64,
}

impl SessionMetricsInner {
    fn new() -> Self {
        Self {
            queries_executed: AtomicU64::new(0),
            locy_evaluations: AtomicU64::new(0),
            total_query_time_us: AtomicU64::new(0),
            transactions_committed: AtomicU64::new(0),
            transactions_rolled_back: AtomicU64::new(0),
            total_rows_returned: AtomicU64::new(0),
            total_rows_scanned: AtomicU64::new(0),
        }
    }
}

/// Snapshot of session-level metrics.
#[derive(Debug, Clone)]
pub struct SessionMetrics {
    /// The session ID.
    pub session_id: String,
    /// When the session was created.
    pub active_since: Instant,
    /// Number of queries executed.
    pub queries_executed: u64,
    /// Number of Locy evaluations.
    pub locy_evaluations: u64,
    /// Total time spent executing queries.
    pub total_query_time: Duration,
    /// Number of transactions that were committed.
    pub transactions_committed: u64,
    /// Number of transactions that were rolled back.
    pub transactions_rolled_back: u64,
    /// Total rows returned across all queries.
    pub total_rows_returned: u64,
    /// Total rows scanned across all queries (0 until executor instrumentation).
    pub total_rows_scanned: u64,
    /// Number of plan cache hits.
    pub plan_cache_hits: u64,
    /// Number of plan cache misses.
    pub plan_cache_misses: u64,
    /// Current plan cache size (entries).
    pub plan_cache_size: usize,
}

/// A database session — the primary scope for reads.
///
/// All data access goes through sessions. Sessions hold scoped query parameters
/// and a private copy of the Locy rule registry. They are the factory for
/// [`Transaction`]s (write scope).
///
/// Sessions are cheap to create (sync, no I/O) and cheap to clone (Arc-based).
///
/// # Examples
///
/// ```no_run
/// # use uni_db::Uni;
/// # async fn example(db: &Uni) -> uni_db::Result<()> {
/// let mut session = db.session();
/// session.set("tenant", 42);
///
/// let rows = session.query("MATCH (n) WHERE n.tenant = $tenant RETURN n").await?;
///
/// // Transactions for writes
/// let tx = session.tx().await?;
/// tx.execute("CREATE (:Person {name: 'Alice'})").await?;
/// tx.commit().await?;
/// # Ok(())
/// # }
/// ```
pub struct Session {
    pub(crate) db: Arc<UniInner>,
    /// When pinned via `pin_to_version`/`pin_to_timestamp`, holds the original
    /// (live) db reference so `refresh()` can restore it.
    original_db: Option<Arc<UniInner>>,
    id: String,
    params: HashMap<String, Value>,
    rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
    /// Mutual exclusion for write contexts (transaction, bulk writer).
    /// Only one write context can be active per session.
    active_write_guard: Arc<AtomicBool>,
    /// Atomic session-level metrics counters.
    pub(crate) metrics_inner: Arc<SessionMetricsInner>,
    /// Timestamp when this session was created.
    created_at: Instant,
    /// Cancellation token for cooperative query cancellation.
    cancellation_token: CancellationToken,
    /// Transparent plan cache for parsed/planned queries.
    plan_cache: std::sync::Mutex<PlanCache>,
    /// Atomic plan cache hit/miss counters.
    plan_cache_metrics: Arc<PlanCacheMetrics>,
    /// Session-level hooks for query/commit interception.
    pub(crate) hooks: Vec<Arc<dyn SessionHook>>,
    /// Default query timeout (from template or explicit configuration).
    pub(crate) query_timeout: Option<Duration>,
    /// Default transaction timeout (from template or explicit configuration).
    pub(crate) transaction_timeout: Option<Duration>,
}

impl Session {
    /// Create a new session from a shared database reference.
    pub(crate) fn new(db: Arc<UniInner>) -> Self {
        // Clone the global rule registry into this session
        let global_registry = db.locy_rule_registry.read().unwrap();
        let session_registry = global_registry.clone();
        drop(global_registry);

        db.active_session_count.fetch_add(1, Ordering::Relaxed);

        Self {
            db,
            original_db: None,
            id: Uuid::new_v4().to_string(),
            params: HashMap::new(),
            rule_registry: Arc::new(std::sync::RwLock::new(session_registry)),
            active_write_guard: Arc::new(AtomicBool::new(false)),
            metrics_inner: Arc::new(SessionMetricsInner::new()),
            created_at: Instant::now(),
            cancellation_token: CancellationToken::new(),
            plan_cache: std::sync::Mutex::new(PlanCache::new(1000)),
            plan_cache_metrics: Arc::new(PlanCacheMetrics {
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
            }),
            hooks: Vec::new(),
            query_timeout: None,
            transaction_timeout: None,
        }
    }

    /// Create a new session from a template's pre-compiled state.
    pub(crate) fn new_from_template(
        db: Arc<UniInner>,
        params: HashMap<String, Value>,
        rule_registry: LocyRuleRegistry,
        hooks: Vec<Arc<dyn SessionHook>>,
        query_timeout: Option<Duration>,
        transaction_timeout: Option<Duration>,
    ) -> Self {
        db.active_session_count.fetch_add(1, Ordering::Relaxed);

        Self {
            db,
            original_db: None,
            id: Uuid::new_v4().to_string(),
            params,
            rule_registry: Arc::new(std::sync::RwLock::new(rule_registry)),
            active_write_guard: Arc::new(AtomicBool::new(false)),
            metrics_inner: Arc::new(SessionMetricsInner::new()),
            created_at: Instant::now(),
            cancellation_token: CancellationToken::new(),
            plan_cache: std::sync::Mutex::new(PlanCache::new(1000)),
            plan_cache_metrics: Arc::new(PlanCacheMetrics {
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
            }),
            hooks,
            query_timeout,
            transaction_timeout,
        }
    }

    // ── Scoped Parameters ─────────────────────────────────────────────

    /// Set a session-scoped parameter. Available to all queries in this session.
    pub fn set<K: Into<String>, V: Into<Value>>(&mut self, key: K, value: V) -> &mut Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// Get a session-scoped parameter.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.params.get(key)
    }

    /// Set multiple session-scoped parameters.
    pub fn set_all<I, K, V>(&mut self, params: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<Value>,
    {
        for (k, v) in params {
            self.params.insert(k.into(), v.into());
        }
        self
    }

    // ── Cypher Reads ──────────────────────────────────────────────────

    /// Execute a read-only Cypher query.
    ///
    /// Uses the transparent plan cache: repeated queries with the same text
    /// skip parsing and planning. Cache entries auto-invalidate on schema
    /// changes.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn query(&self, cypher: &str) -> Result<QueryResult> {
        let params = self.merge_params(HashMap::new());
        self.run_before_query_hooks(cypher, QueryType::Cypher, &params)?;
        let start = Instant::now();
        let result = self.execute_cached(cypher, params.clone()).await;
        self.metrics_inner
            .queries_executed
            .fetch_add(1, Ordering::Relaxed);
        self.db.total_queries.fetch_add(1, Ordering::Relaxed);
        self.metrics_inner
            .total_query_time_us
            .fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
        if let Ok(ref qr) = result {
            self.metrics_inner
                .total_rows_returned
                .fetch_add(qr.len() as u64, Ordering::Relaxed);
            self.run_after_query_hooks(cypher, QueryType::Cypher, &params, qr.metrics());
        }
        result
    }

    /// Execute a read-only Cypher query with a builder for parameters.
    pub fn query_with(&self, cypher: &str) -> QueryBuilder<'_> {
        QueryBuilder {
            session: self,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout: self.query_timeout,
            max_memory: None,
            cancellation_token: None,
        }
    }

    /// Execute a query returning a cursor for streaming results.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn query_cursor(&self, cypher: &str) -> Result<QueryCursor> {
        let params = self.merge_params(HashMap::new());
        self.db.execute_cursor_internal(cypher, params).await
    }

    // ── Cypher Writes (auto-committed) ────────────────────────────────

    /// Execute a Cypher mutation (CREATE, SET, DELETE, etc.).
    ///
    /// The mutation is auto-committed. For multi-statement writes, use a
    /// [`Transaction`] via [`tx()`](Self::tx).
    ///
    /// Returns `ReadOnly` error if the session is pinned to a snapshot.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn execute(&self, cypher: &str) -> Result<AutoCommitResult> {
        if self.is_pinned() {
            return Err(UniError::ReadOnly {
                operation: "execute".to_string(),
            });
        }
        let params = self.merge_params(HashMap::new());
        self.run_before_query_hooks(cypher, QueryType::Execute, &params)?;
        let start = Instant::now();
        let before = self.db.get_mutation_count().await;
        let before_stats = self.db.get_mutation_stats().await;
        let result = self.db.execute_internal(cypher, params.clone()).await?;
        let after_stats = self.db.get_mutation_stats().await;
        let affected_rows = if result.is_empty() {
            self.db.get_mutation_count().await.saturating_sub(before)
        } else {
            result.len()
        };
        let diff = after_stats.diff(&before_stats);

        // Read L0 version after auto-commit
        let version = match self.db.writer.as_ref() {
            Some(w) => {
                w.read()
                    .await
                    .l0_manager
                    .get_current()
                    .read()
                    .current_version
            }
            None => 0,
        };

        self.metrics_inner
            .queries_executed
            .fetch_add(1, Ordering::Relaxed);
        self.db.total_queries.fetch_add(1, Ordering::Relaxed);
        self.metrics_inner
            .total_query_time_us
            .fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
        self.metrics_inner
            .total_rows_returned
            .fetch_add(affected_rows as u64, Ordering::Relaxed);
        self.run_after_query_hooks(cypher, QueryType::Execute, &params, result.metrics());
        Ok(AutoCommitResult {
            affected: affected_rows,
            nodes_created: diff.nodes_created,
            nodes_deleted: diff.nodes_deleted,
            relationships_created: diff.relationships_created,
            relationships_deleted: diff.relationships_deleted,
            properties_set: diff.properties_set,
            properties_removed: diff.properties_removed,
            labels_added: diff.labels_added,
            labels_removed: diff.labels_removed,
            version,
            metrics: result.metrics().clone(),
        })
    }

    /// Execute a Cypher mutation with a builder for parameters.
    ///
    /// Returns an [`AutoCommitBuilder`] that provides `.param()` chaining
    /// and a `.run()` method that returns [`AutoCommitResult`] with the
    /// database version after commit.
    pub fn execute_with(&self, cypher: &str) -> AutoCommitBuilder<'_> {
        AutoCommitBuilder {
            session: self,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout: None,
        }
    }

    // ── Planning & Introspection ──────────────────────────────────────

    /// Explain a Cypher query plan without executing it.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn explain(&self, cypher: &str) -> Result<ExplainOutput> {
        self.db.explain_internal(cypher).await
    }

    /// Profile a Cypher query execution.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn profile(&self, cypher: &str) -> Result<(QueryResult, ProfileOutput)> {
        let params = self.merge_params(HashMap::new());
        self.db.profile_internal(cypher, params).await
    }

    /// Profile a Cypher query with a builder for parameters.
    pub fn profile_with(&self, cypher: &str) -> ProfileBuilder<'_> {
        ProfileBuilder {
            session: self,
            cypher: cypher.to_string(),
            params: HashMap::new(),
        }
    }

    // ── Locy Evaluation ───────────────────────────────────────────────

    /// Evaluate a Locy program with default configuration.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn locy(&self, program: &str) -> Result<LocyResult> {
        self.run_before_query_hooks(program, QueryType::Locy, &HashMap::new())?;
        let result = self.locy_with(program).run().await;
        self.metrics_inner
            .locy_evaluations
            .fetch_add(1, Ordering::Relaxed);
        result
    }

    /// Explain a Locy program. Currently equivalent to `locy()`.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn explain_locy(&self, program: &str) -> Result<LocyResult> {
        self.locy(program).await
    }

    /// Evaluate a Locy program with parameters using a builder.
    pub fn locy_with(&self, program: &str) -> crate::api::locy_builder::LocyBuilder<'_> {
        crate::api::locy_builder::LocyBuilder::new(self, program)
    }

    // ── Rule Management ───────────────────────────────────────────────

    /// Register Locy rules for this session. Session-scoped rules are visible
    /// to all evaluations and transactions within this session.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub fn register_rules(&self, program: &str) -> Result<()> {
        impl_locy::register_rules_on_registry(&self.rule_registry, program)
    }

    /// Clear all registered Locy rules for this session.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub fn clear_rules(&self) {
        let mut registry = self.rule_registry.write().unwrap();
        registry.rules.clear();
        registry.strata.clear();
    }

    /// Compile a Locy program without executing it, using this session's rule registry.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub fn compile_locy(&self, program: &str) -> Result<uni_locy::CompiledProgram> {
        let ast = uni_cypher::parse_locy(program).map_err(|e| UniError::Parse {
            message: format!("LocyParseError: {e}"),
            position: None,
            line: None,
            column: None,
            context: None,
        })?;
        let registry = self.rule_registry.read().unwrap();
        if registry.rules.is_empty() {
            drop(registry);
            uni_locy::compile(&ast).map_err(|e| UniError::Query {
                message: format!("LocyCompileError: {e}"),
                query: None,
            })
        } else {
            let external_names: Vec<String> = registry.rules.keys().cloned().collect();
            drop(registry);
            uni_locy::compile_with_external_rules(&ast, &external_names).map_err(|e| {
                UniError::Query {
                    message: format!("LocyCompileError: {e}"),
                    query: None,
                }
            })
        }
    }

    // ── Transaction & Writer Factories ────────────────────────────────

    /// Create a new transaction for multi-statement writes.
    ///
    /// Only one write context (transaction or bulk writer) can be active
    /// per session at a time. Returns `ReadOnly` if the session is pinned.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn tx(&self) -> Result<Transaction> {
        if self.is_pinned() {
            return Err(UniError::ReadOnly {
                operation: "start_transaction".to_string(),
            });
        }
        Transaction::new(self).await
    }

    /// Create a transaction with builder options (timeout, isolation level).
    pub fn tx_with(&self) -> TransactionBuilder<'_> {
        TransactionBuilder {
            session: self,
            timeout: self.transaction_timeout,
            isolation: IsolationLevel::default(),
        }
    }

    /// Create a bulk writer for efficient data loading.
    ///
    /// Only one write context (transaction or bulk writer) can be active
    /// per session at a time.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub fn bulk_writer(&self) -> Result<crate::api::bulk::BulkWriterBuilder> {
        if self.is_pinned() {
            return Err(UniError::ReadOnly {
                operation: "bulk_writer".to_string(),
            });
        }
        // Acquire write guard
        if self
            .active_write_guard
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(UniError::WriteContextAlreadyActive {
                session_id: self.id.to_string(),
                hint: "Only one Transaction, BulkWriter, or Appender can be active per Session at a time. Commit or rollback the active one first, or create a separate Session for concurrent writes.",
            });
        }
        Ok(crate::api::bulk::BulkWriterBuilder::new_with_guard(
            self.db.clone(),
            self.active_write_guard.clone(),
        ))
    }

    // ── Bulk Insert (admin convenience) ─────────────────────────────

    /// Bulk insert vertices for a given label.
    ///
    /// Low-level admin API for bulk loading and benchmarking. For
    /// application-level bulk loading prefer `bulk_writer()` or `appender()`.
    ///
    /// Returns the allocated VIDs in the same order as the input.
    #[instrument(skip(self, properties_list), fields(session_id = %self.id))]
    pub async fn bulk_insert_vertices(
        &self,
        label: &str,
        properties_list: Vec<uni_common::Properties>,
    ) -> Result<Vec<uni_common::core::id::Vid>> {
        let schema = self.db.schema.schema();
        schema
            .labels
            .get(label)
            .ok_or_else(|| UniError::LabelNotFound {
                label: label.to_string(),
            })?;
        if let Some(writer_lock) = &self.db.writer {
            let mut writer = writer_lock.write().await;
            if properties_list.is_empty() {
                return Ok(Vec::new());
            }
            let vids = writer
                .allocate_vids(properties_list.len())
                .await
                .map_err(UniError::Internal)?;
            let _props = writer
                .insert_vertices_batch(vids.clone(), properties_list, vec![label.to_string()])
                .await
                .map_err(UniError::Internal)?;
            Ok(vids)
        } else {
            Err(UniError::ReadOnly {
                operation: "bulk_insert_vertices".to_string(),
            })
        }
    }

    /// Bulk insert edges for a given edge type using pre-allocated VIDs.
    ///
    /// Low-level admin API for bulk loading and benchmarking. For
    /// application-level bulk loading prefer `bulk_writer()` or `appender()`.
    #[instrument(skip(self, edges), fields(session_id = %self.id))]
    pub async fn bulk_insert_edges(
        &self,
        edge_type: &str,
        edges: Vec<(
            uni_common::core::id::Vid,
            uni_common::core::id::Vid,
            uni_common::Properties,
        )>,
    ) -> Result<()> {
        let schema = self.db.schema.schema();
        let edge_meta =
            schema
                .edge_types
                .get(edge_type)
                .ok_or_else(|| UniError::EdgeTypeNotFound {
                    edge_type: edge_type.to_string(),
                })?;
        let type_id = edge_meta.id;
        if let Some(writer_lock) = &self.db.writer {
            let mut writer = writer_lock.write().await;
            for (src_vid, dst_vid, props) in edges {
                let eid = writer.next_eid(type_id).await.map_err(UniError::Internal)?;
                writer
                    .insert_edge(
                        src_vid,
                        dst_vid,
                        type_id,
                        eid,
                        props,
                        Some(edge_type.to_string()),
                    )
                    .await
                    .map_err(UniError::Internal)?;
            }
            Ok(())
        } else {
            Err(UniError::ReadOnly {
                operation: "bulk_insert_edges".to_string(),
            })
        }
    }

    // ── Version Pinning ──────────────────────────────────────────────

    /// Pin this session to a specific snapshot version.
    ///
    /// All subsequent reads see data as of that version. Writes are rejected.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn pin_to_version(&mut self, snapshot_id: &str) -> Result<()> {
        let pinned = self.live_db().at_snapshot(snapshot_id).await?;
        if self.original_db.is_none() {
            self.original_db = Some(self.db.clone());
        }
        self.db = Arc::new(pinned);
        Ok(())
    }

    /// Pin this session to a specific timestamp.
    ///
    /// Resolves the closest snapshot at or before the given timestamp, then
    /// pins the session to that snapshot. Writes are rejected while pinned.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn pin_to_timestamp(&mut self, ts: chrono::DateTime<chrono::Utc>) -> Result<()> {
        let snapshot_id = self.live_db().resolve_time_travel_timestamp(ts).await?;
        self.pin_to_version(&snapshot_id).await
    }

    /// Refresh: unpin the session, returning to the live database state.
    ///
    /// In single-process mode, this simply unpins the session.
    /// In multi-agent mode (Phase 2), this picks up the latest
    /// committed version from storage.
    pub async fn refresh(&mut self) -> Result<()> {
        if let Some(original) = self.original_db.take() {
            self.db = original;
        }
        Ok(())
    }

    /// Returns `true` if the session is pinned to a specific version.
    pub fn is_pinned(&self) -> bool {
        self.original_db.is_some()
    }

    /// Get the live (unpinned) db reference for resolving snapshots.
    fn live_db(&self) -> &Arc<UniInner> {
        self.original_db.as_ref().unwrap_or(&self.db)
    }

    // ── Cancellation ─────────────────────────────────────────────────

    /// Cancel all in-flight queries in this session.
    ///
    /// Queries check `is_cancelled()` at each operator boundary via
    /// `check_timeout()`. After cancellation, a fresh token is created
    /// so the session remains usable.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub fn cancel(&mut self) {
        self.cancellation_token.cancel();
        self.cancellation_token = CancellationToken::new();
    }

    /// Get a clone of this session's cancellation token.
    ///
    /// Useful for external cancellation (e.g. from a timeout task).
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    // ── Prepared Statements ──────────────────────────────────────────

    /// Prepare a Cypher query for repeated execution.
    ///
    /// The query is parsed and planned once; subsequent executions skip those
    /// phases. If the schema changes, the prepared query auto-replans.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn prepare(&self, cypher: &str) -> Result<crate::api::prepared::PreparedQuery> {
        crate::api::prepared::PreparedQuery::new(self.db.clone(), cypher).await
    }

    /// Prepare a Locy program for repeated evaluation.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub fn prepare_locy(&self, program: &str) -> Result<crate::api::prepared::PreparedLocy> {
        crate::api::prepared::PreparedLocy::new(
            self.db.clone(),
            self.rule_registry.clone(),
            program,
        )
    }

    // ── Hooks ─────────────────────────────────────────────────────────

    /// Add a session hook for query/commit interception.
    pub fn add_hook(&mut self, hook: impl SessionHook + 'static) {
        self.hooks.push(Arc::new(hook));
    }

    /// Run before-query hooks. Returns `Err(HookRejected)` if any hook rejects.
    pub(crate) fn run_before_query_hooks(
        &self,
        query_text: &str,
        query_type: QueryType,
        params: &HashMap<String, Value>,
    ) -> Result<()> {
        if self.hooks.is_empty() {
            return Ok(());
        }
        let ctx = HookContext {
            session_id: self.id.clone(),
            query_text: query_text.to_string(),
            query_type,
            params: params.clone(),
        };
        for hook in &self.hooks {
            hook.before_query(&ctx)?;
        }
        Ok(())
    }

    /// Run after-query hooks. Panics in hooks are caught and logged.
    pub(crate) fn run_after_query_hooks(
        &self,
        query_text: &str,
        query_type: QueryType,
        params: &HashMap<String, Value>,
        metrics: &uni_query::QueryMetrics,
    ) {
        if self.hooks.is_empty() {
            return;
        }
        let ctx = HookContext {
            session_id: self.id.clone(),
            query_text: query_text.to_string(),
            query_type,
            params: params.clone(),
        };
        for hook in &self.hooks {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hook.after_query(&ctx, metrics);
            }));
            if let Err(e) = result {
                tracing::error!("after_query hook panicked: {:?}", e);
            }
        }
    }

    // ── Commit Notifications ─────────────────────────────────────────

    /// Watch for all commit notifications.
    pub fn watch(&self) -> crate::api::notifications::CommitStream {
        let rx = self.db.commit_tx.subscribe();
        crate::api::notifications::WatchBuilder::new(rx).build()
    }

    /// Watch for commit notifications with filters.
    pub fn watch_with(&self) -> crate::api::notifications::WatchBuilder {
        let rx = self.db.commit_tx.subscribe();
        crate::api::notifications::WatchBuilder::new(rx)
    }

    // ── Streaming Appender ───────────────────────────────────────────

    /// Create a streaming appender for efficient row-by-row data loading.
    ///
    /// The appender is scoped to a single label. Rows are buffered and
    /// flushed in batches to the underlying bulk writer.
    pub fn appender(&self, label: &str) -> crate::api::appender::AppenderBuilder<'_> {
        crate::api::appender::AppenderBuilder::new(self, label)
    }

    // ── Custom Functions ──────────────────────────────────────────────

    /// Register a custom scalar function available to Cypher queries.
    ///
    /// The function is registered at the database level and is visible to
    /// all sessions sharing the same database instance.
    ///
    /// # Example
    ///
    /// ```ignore
    /// session.register_function("double", |args| {
    ///     let n = args[0].as_i64().unwrap_or(0);
    ///     Ok(Value::Int(n * 2))
    /// })?;
    /// let result = session.query("RETURN double(21) AS val").await?;
    /// ```
    #[instrument(skip(self, func), fields(session_id = %self.id))]
    pub fn register_function<F>(&self, name: &str, func: F) -> Result<()>
    where
        F: Fn(&[Value]) -> Result<Value> + Send + Sync + 'static,
    {
        let mut registry = self.db.custom_functions.write().map_err(|_| {
            UniError::Internal(anyhow::anyhow!("custom function registry lock poisoned"))
        })?;
        registry.register(name.to_string(), std::sync::Arc::new(func));
        Ok(())
    }

    // ── Lifecycle & Observability ──────────────────────────────────────

    /// Get the session ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Query the capabilities of this session.
    ///
    /// Returns a snapshot of what the session can do in its current mode.
    pub fn capabilities(&self) -> SessionCapabilities {
        use crate::api::multi_agent::WriteLease;
        let write_lease = self.db.write_lease.as_ref().map(|wl| match wl {
            WriteLease::Local => WriteLeaseSummary::Local,
            WriteLease::DynamoDB { table } => WriteLeaseSummary::DynamoDB {
                table: table.clone(),
            },
            WriteLease::Custom(_) => WriteLeaseSummary::Custom,
        });
        SessionCapabilities {
            can_write: self.db.writer.is_some() && !self.is_pinned(),
            can_pin: true,
            isolation: IsolationLevel::default(),
            has_notifications: true,
            write_lease,
        }
    }

    /// Snapshot the session's accumulated metrics.
    pub fn metrics(&self) -> SessionMetrics {
        let m = &self.metrics_inner;
        SessionMetrics {
            session_id: self.id.clone(),
            active_since: self.created_at,
            queries_executed: m.queries_executed.load(Ordering::Relaxed),
            locy_evaluations: m.locy_evaluations.load(Ordering::Relaxed),
            total_query_time: Duration::from_micros(m.total_query_time_us.load(Ordering::Relaxed)),
            transactions_committed: m.transactions_committed.load(Ordering::Relaxed),
            transactions_rolled_back: m.transactions_rolled_back.load(Ordering::Relaxed),
            total_rows_returned: m.total_rows_returned.load(Ordering::Relaxed),
            total_rows_scanned: m.total_rows_scanned.load(Ordering::Relaxed),
            plan_cache_hits: self.plan_cache_metrics.hits.load(Ordering::Relaxed),
            plan_cache_misses: self.plan_cache_metrics.misses.load(Ordering::Relaxed),
            plan_cache_size: self.plan_cache.lock().map(|c| c.len()).unwrap_or(0),
        }
    }

    // ── Internal Helpers ──────────────────────────────────────────────

    /// Execute a query using the transparent plan cache.
    ///
    /// On cache hit: reuses the parsed AST and logical plan, skipping parse
    /// and planning phases entirely. On cache miss: parses, plans, caches,
    /// then executes normally. Cache entries are invalidated when the schema
    /// version changes.
    pub(crate) async fn execute_cached(
        &self,
        cypher: &str,
        params: HashMap<String, Value>,
    ) -> Result<QueryResult> {
        let schema_version = self.db.schema.schema().schema_version;
        let cache_key = plan_cache_key(cypher);

        // Try cache lookup (brief lock, then release)
        let cached = self.plan_cache.lock().ok().and_then(|mut cache| {
            cache
                .get(cache_key, schema_version)
                .map(|entry| (entry.ast.clone(), entry.plan.clone()))
        });

        if let Some((_ast, plan)) = cached {
            // Cache hit — skip parse and plan, execute the cached plan directly
            self.plan_cache_metrics.hits.fetch_add(1, Ordering::Relaxed);
            return self
                .db
                .execute_plan_internal(plan, cypher, params, self.db.config.clone(), None)
                .await;
        }

        // Cache miss — parse, plan, cache, execute via the normal path
        self.plan_cache_metrics
            .misses
            .fetch_add(1, Ordering::Relaxed);

        // Parse
        let ast = uni_cypher::parse(cypher).map_err(crate::api::impl_query::into_parse_error)?;

        // Time-travel queries bypass the cache entirely
        if matches!(ast, uni_cypher::ast::Query::TimeTravel { .. }) {
            return self
                .db
                .execute_internal_with_config(cypher, params, self.db.config.clone())
                .await;
        }

        // Plan
        let planner = uni_query::QueryPlanner::new(self.db.schema.schema().clone())
            .with_params(params.clone());
        let plan = planner
            .plan(ast.clone())
            .map_err(|e| crate::api::impl_query::into_query_error(e, cypher))?;

        // Cache the entry
        if let Ok(mut cache) = self.plan_cache.lock() {
            cache.insert(
                cache_key,
                PlanCacheEntry {
                    ast,
                    plan: plan.clone(),
                    schema_version,
                    hit_count: 0,
                },
            );
        }

        // Execute the freshly planned query
        self.db
            .execute_plan_internal(plan, cypher, params, self.db.config.clone(), None)
            .await
    }

    /// Get the database inner reference (for Transaction, LocyEngine, etc.)
    pub(crate) fn db(&self) -> &Arc<UniInner> {
        &self.db
    }

    /// Get the session's rule registry.
    pub(crate) fn rule_registry(&self) -> &Arc<std::sync::RwLock<LocyRuleRegistry>> {
        &self.rule_registry
    }

    /// Get the active write guard.
    pub(crate) fn active_write_guard(&self) -> &Arc<AtomicBool> {
        &self.active_write_guard
    }

    /// Merge session params with per-query params (per-query takes precedence).
    pub(crate) fn merge_params(
        &self,
        mut query_params: HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        if !self.params.is_empty() {
            let session_map: HashMap<String, Value> = self.params.clone();
            if let Some(Value::Map(existing)) = query_params.get_mut("session") {
                for (k, v) in session_map {
                    existing.entry(k).or_insert(v);
                }
            } else {
                query_params.insert("session".to_string(), Value::Map(session_map));
            }
        }
        query_params
    }
}

/// Builder for parameterized queries within a session.
pub struct QueryBuilder<'a> {
    session: &'a Session,
    cypher: String,
    params: HashMap<String, Value>,
    timeout: Option<std::time::Duration>,
    max_memory: Option<usize>,
    cancellation_token: Option<CancellationToken>,
}

impl<'a> QueryBuilder<'a> {
    /// Bind a parameter to the query.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        for (k, v) in params {
            self.params.insert(k.to_string(), v);
        }
        self
    }

    /// Set maximum execution time for this query.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Set maximum memory per query in bytes.
    pub fn max_memory(mut self, bytes: usize) -> Self {
        self.max_memory = Some(bytes);
        self
    }

    /// Attach a cancellation token for cooperative query cancellation.
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Execute the query and fetch all results.
    ///
    /// Uses the session's transparent plan cache when no custom timeout or
    /// memory limit is set.
    pub async fn fetch_all(self) -> Result<QueryResult> {
        let has_overrides = self.timeout.is_some()
            || self.max_memory.is_some()
            || self.cancellation_token.is_some();
        if has_overrides {
            // Custom config — bypass cache and use the config-aware path
            let mut db_config = self.session.db.config.clone();
            if let Some(t) = self.timeout {
                db_config.query_timeout = t;
            }
            if let Some(m) = self.max_memory {
                db_config.max_query_memory = m;
            }
            let params = self.session.merge_params(self.params);
            self.session
                .db
                .execute_internal_with_config_and_token(
                    &self.cypher,
                    params,
                    db_config,
                    self.cancellation_token,
                )
                .await
        } else {
            // Default config — use the plan cache
            let params = self.session.merge_params(self.params);
            self.session.execute_cached(&self.cypher, params).await
        }
    }

    /// Alias for `fetch_all()`.
    pub async fn execute(self) -> Result<QueryResult> {
        self.fetch_all().await
    }

    /// Execute the query and return the first row, or `None` if empty.
    pub async fn fetch_one(self) -> Result<Option<Row>> {
        let result = self.fetch_all().await?;
        Ok(result.into_rows().into_iter().next())
    }

    /// Execute a mutation and return affected row count with detailed stats.
    pub async fn execute_mutation(self) -> Result<ExecuteResult> {
        let db = &self.session.db;
        let before = db.get_mutation_count().await;
        let before_stats = db.get_mutation_stats().await;
        let result = self.fetch_all().await?;
        let after_stats = db.get_mutation_stats().await;
        let affected_rows = if result.is_empty() {
            db.get_mutation_count().await.saturating_sub(before)
        } else {
            result.len()
        };
        let diff = after_stats.diff(&before_stats);
        Ok(ExecuteResult::with_details(
            affected_rows,
            &diff,
            result.metrics().clone(),
        ))
    }

    /// Execute the query and return a cursor for streaming results.
    pub async fn cursor(self) -> Result<QueryCursor> {
        let mut db_config = self.session.db.config.clone();
        if let Some(t) = self.timeout {
            db_config.query_timeout = t;
        }
        if let Some(m) = self.max_memory {
            db_config.max_query_memory = m;
        }
        let params = self.session.merge_params(self.params);
        self.session
            .db
            .execute_cursor_internal_with_config(&self.cypher, params, db_config)
            .await
    }
}

/// Builder for auto-committed mutations with parameter binding.
///
/// Created by [`Session::execute_with()`]. Chain `.param()` calls to bind
/// parameters, then call `.run()` to execute and get an [`AutoCommitResult`]
/// that includes the database version after commit.
pub struct AutoCommitBuilder<'a> {
    session: &'a Session,
    cypher: String,
    params: HashMap<String, Value>,
    timeout: Option<Duration>,
}

impl<'a> AutoCommitBuilder<'a> {
    /// Bind a parameter to the mutation.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        for (k, v) in params {
            self.params.insert(k.to_string(), v);
        }
        self
    }

    /// Set maximum execution time for this mutation.
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Execute the mutation and return the result with database version.
    pub async fn run(self) -> Result<AutoCommitResult> {
        if self.session.is_pinned() {
            return Err(UniError::ReadOnly {
                operation: "execute".to_string(),
            });
        }
        let params = self.session.merge_params(self.params);
        self.session
            .run_before_query_hooks(&self.cypher, QueryType::Execute, &params)?;
        let start = Instant::now();
        let before = self.session.db.get_mutation_count().await;
        let before_stats = self.session.db.get_mutation_stats().await;

        let result = if let Some(t) = self.timeout {
            let mut config = self.session.db.config.clone();
            config.query_timeout = t;
            self.session
                .db
                .execute_internal_with_config(&self.cypher, params.clone(), config)
                .await?
        } else {
            self.session
                .db
                .execute_internal(&self.cypher, params.clone())
                .await?
        };

        let after_stats = self.session.db.get_mutation_stats().await;
        let affected_rows = if result.is_empty() {
            self.session
                .db
                .get_mutation_count()
                .await
                .saturating_sub(before)
        } else {
            result.len()
        };
        let diff = after_stats.diff(&before_stats);

        // Read L0 version after mutation (same pattern as Transaction::commit)
        let version = match self.session.db.writer.as_ref() {
            Some(w) => {
                w.read()
                    .await
                    .l0_manager
                    .get_current()
                    .read()
                    .current_version
            }
            None => 0,
        };

        self.session
            .metrics_inner
            .queries_executed
            .fetch_add(1, Ordering::Relaxed);
        self.session
            .db
            .total_queries
            .fetch_add(1, Ordering::Relaxed);
        self.session
            .metrics_inner
            .total_query_time_us
            .fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
        self.session
            .metrics_inner
            .total_rows_returned
            .fetch_add(affected_rows as u64, Ordering::Relaxed);
        self.session.run_after_query_hooks(
            &self.cypher,
            QueryType::Execute,
            &params,
            result.metrics(),
        );

        Ok(AutoCommitResult {
            affected: affected_rows,
            nodes_created: diff.nodes_created,
            nodes_deleted: diff.nodes_deleted,
            relationships_created: diff.relationships_created,
            relationships_deleted: diff.relationships_deleted,
            properties_set: diff.properties_set,
            properties_removed: diff.properties_removed, // MutationStats doesn't track removals yet
            labels_added: diff.labels_added,
            labels_removed: diff.labels_removed,
            version,
            metrics: result.metrics().clone(),
        })
    }
}

/// Builder for profiling a Cypher query with parameters.
pub struct ProfileBuilder<'a> {
    session: &'a Session,
    cypher: String,
    params: HashMap<String, Value>,
}

impl<'a> ProfileBuilder<'a> {
    /// Bind a parameter to the profiled query.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        for (k, v) in params {
            self.params.insert(k.to_string(), v);
        }
        self
    }

    /// Execute the profiled query and return results with profiling output.
    pub async fn run(self) -> Result<(QueryResult, ProfileOutput)> {
        let params = self.session.merge_params(self.params);
        self.session.db.profile_internal(&self.cypher, params).await
    }
}

/// Builder for starting a transaction with options.
pub struct TransactionBuilder<'a> {
    session: &'a Session,
    timeout: Option<Duration>,
    isolation: IsolationLevel,
}

impl<'a> TransactionBuilder<'a> {
    /// Set the transaction timeout. The transaction will expire if operations
    /// are attempted after this duration.
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    /// Set the isolation level for the transaction.
    pub fn isolation(mut self, level: IsolationLevel) -> Self {
        self.isolation = level;
        self
    }

    /// Start the transaction.
    pub async fn start(self) -> Result<Transaction> {
        if self.session.is_pinned() {
            return Err(UniError::ReadOnly {
                operation: "start_transaction".to_string(),
            });
        }
        Transaction::new_with_options(self.session, self.timeout, self.isolation).await
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.db.active_session_count.fetch_sub(1, Ordering::Relaxed);
    }
}

// ── Plan Cache (internal) ─────────────────────────────────────────────

/// Entry in the transparent plan cache.
struct PlanCacheEntry {
    ast: uni_query::CypherQuery,
    plan: uni_query::LogicalPlan,
    schema_version: u32,
    hit_count: u64,
}

/// Transparent plan cache keyed by query text hash.
///
/// Caches parsed ASTs and logical plans to skip parsing and planning for
/// repeated queries. Entries are evicted LFU-style when the cache is full.
struct PlanCache {
    entries: HashMap<u64, PlanCacheEntry>,
    max_entries: usize,
}

impl PlanCache {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries,
        }
    }

    fn get(&mut self, key: u64, current_schema_version: u32) -> Option<&PlanCacheEntry> {
        if let Some(entry) = self.entries.get_mut(&key) {
            if entry.schema_version == current_schema_version {
                entry.hit_count += 1;
                return self.entries.get(&key);
            }
            // Schema changed — evict stale entry
            self.entries.remove(&key);
        }
        None
    }

    fn insert(&mut self, key: u64, entry: PlanCacheEntry) {
        if self.entries.len() >= self.max_entries {
            // Evict entry with lowest hit_count
            if let Some((&evict_key, _)) = self.entries.iter().min_by_key(|(_, e)| e.hit_count) {
                self.entries.remove(&evict_key);
            }
        }
        self.entries.insert(key, entry);
    }

    /// Number of cached plans.
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Compute a hash key from a query string.
fn plan_cache_key(cypher: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    cypher.hash(&mut hasher);
    hasher.finish()
}
