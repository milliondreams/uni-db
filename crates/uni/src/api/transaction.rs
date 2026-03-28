// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Transaction — the explicit write scope.
//!
//! Transactions provide ACID guarantees for multi-statement writes.
//! Changes are isolated until commit.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use metrics;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, warn};
use uuid::Uuid;

use crate::api::UniInner;
use crate::api::impl_locy::{self, LocyRuleRegistry};
use crate::api::session::Session;
use uni_common::{Result, UniError};
use uni_locy::DerivedFactSet;

use crate::api::locy_result::LocyResult;
use uni_query::{ExecuteResult, QueryResult, Row, Value};

/// Transaction isolation level.
///
/// Uses commit-time serialization: `tx()` allocates a private L0 buffer
/// without acquiring the writer lock; the writer lock is only acquired
/// at commit time for WAL + merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum IsolationLevel {
    /// Serialized isolation with begin-time writer lock.
    #[default]
    Serialized,
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IsolationLevel::Serialized => write!(f, "Serialized"),
        }
    }
}

/// Result of committing a transaction.
#[derive(Debug)]
pub struct CommitResult {
    /// Number of mutations committed.
    pub mutations_committed: usize,
    /// Number of rules promoted to the parent session.
    pub rules_promoted: usize,
    /// Database version after commit.
    pub version: u64,
    /// Database version when the transaction was created.
    pub started_at_version: u64,
    /// WAL log sequence number of the commit (0 when no WAL is configured).
    pub wal_lsn: u64,
    /// Duration of the commit operation (lock + WAL + merge).
    pub duration: Duration,
    /// Errors encountered during rule promotion (best-effort).
    pub rule_promotion_errors: Vec<RulePromotionError>,
}

impl CommitResult {
    /// Number of versions that committed between tx start and commit.
    /// 0 means no concurrent commits occurred.
    pub fn version_gap(&self) -> u64 {
        self.version.saturating_sub(self.started_at_version + 1)
    }
}

/// Error encountered during rule promotion at commit time.
#[derive(Debug, Clone)]
pub struct RulePromotionError {
    pub rule_text: String,
    pub error: String,
}

/// A database transaction — the explicit write scope.
///
/// Transactions provide ACID guarantees for multiple operations.
/// Changes are isolated until [`commit()`](Self::commit).
///
/// # Concurrency
///
/// Uses commit-time serialization: each transaction owns a private L0 buffer.
/// `tx()` only takes a reader lock (to snapshot the version); the writer lock
/// is acquired briefly per-mutation and once at commit for WAL + merge.
/// Multiple transactions can coexist; isolation is provided by private L0 buffers.
///
/// # Drop Behavior
///
/// If dropped without calling `commit()` or `rollback()`, the private L0 is
/// simply discarded (no writer lock needed) and a warning is logged if dirty.
pub struct Transaction {
    pub(crate) db: Arc<UniInner>,
    /// Private L0 buffer — mutations within this transaction are routed here.
    pub(crate) tx_l0: Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
    /// Session-level write guard (set false on complete)
    session_write_guard: Arc<std::sync::atomic::AtomicBool>,
    /// Session's rule registry (for rule promotion on commit)
    session_rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
    /// Transaction-scoped rule registry
    rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
    /// Session's metrics counters (for commit/rollback tracking)
    session_metrics: Arc<crate::api::session::SessionMetricsInner>,
    completed: bool,
    id: String,
    /// Session ID (for commit notifications and hooks).
    session_id: String,
    start_time: Instant,
    started_at_version: u64,
    /// Optional deadline for the transaction.
    deadline: Option<Instant>,
    /// Child cancellation token derived from the session's parent token.
    cancellation_token: CancellationToken,
    /// Hooks inherited from the session.
    hooks: Vec<Arc<dyn crate::api::hooks::SessionHook>>,
}

impl Transaction {
    pub(crate) async fn new(session: &Session) -> Result<Self> {
        Self::new_with_options(session, None, IsolationLevel::default()).await
    }

    pub(crate) async fn new_with_options(
        session: &Session,
        timeout: Option<Duration>,
        _isolation: IsolationLevel,
    ) -> Result<Self> {
        // Ensure no other write context is active on this session
        if session
            .active_write_guard()
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(UniError::WriteContextAlreadyActive {
                session_id: session.id().to_string(),
                hint: "Only one Transaction, BulkWriter, or Appender can be active per Session at a time. Commit or rollback the active one first, or create a separate Session for concurrent writes.",
            });
        }

        let db = session.db().clone();
        let writer_lock = db.writer.clone().ok_or_else(|| {
            session.active_write_guard().store(false, Ordering::SeqCst);
            UniError::ReadOnly {
                operation: "start_transaction".to_string(),
            }
        })?;

        // READ lock only — create a private L0 buffer without blocking other writers.
        // This is the key commit-time serialization change: no writer WRITE lock
        // is taken at transaction begin; it's deferred to commit().
        let (started_at_version, tx_l0) = {
            let writer = writer_lock.read().await;
            let l0 = writer.create_transaction_l0();
            let version = l0.read().current_version;
            (version, l0)
        };

        let id = Uuid::new_v4().to_string();
        info!(transaction_id = %id, "Transaction started");

        // Clone session's rule registry for transaction-scoped modifications
        let session_registry = session.rule_registry().read().unwrap().clone();

        let deadline = timeout.map(|d| Instant::now() + d);
        // Child token from session — cancelled when session.cancel() fires
        let cancellation_token = session.cancellation_token().child_token();

        Ok(Self {
            db,
            tx_l0,
            session_write_guard: session.active_write_guard().clone(),
            session_rule_registry: session.rule_registry().clone(),
            rule_registry: Arc::new(std::sync::RwLock::new(session_registry)),
            session_metrics: session.metrics_inner.clone(),
            completed: false,
            id,
            session_id: session.id().to_string(),
            start_time: Instant::now(),
            started_at_version,
            deadline,
            cancellation_token,
            hooks: session.hooks.clone(),
        })
    }

    // ── Cypher Reads (sees shared DB + uncommitted writes) ────────────

    /// Execute a Cypher query within the transaction.
    /// Reads see the private L0 buffer (uncommitted writes).
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub async fn query(&self, cypher: &str) -> Result<QueryResult> {
        self.check_completed()?;
        self.db
            .execute_internal_with_tx_l0(cypher, HashMap::new(), self.tx_l0.clone())
            .await
    }

    /// Execute a Cypher query with parameters.
    pub fn query_with(&self, cypher: &str) -> TxQueryBuilder<'_> {
        TxQueryBuilder {
            tx: self,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            cancellation_token: None,
        }
    }

    // ── Cypher Writes ─────────────────────────────────────────────────

    /// Execute a Cypher mutation within the transaction.
    /// Mutation count is read from the private L0 (not the global writer).
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult> {
        self.check_completed()?;
        let (before, before_stats) = {
            let l0 = self.tx_l0.read();
            (l0.mutation_count, l0.mutation_stats.clone())
        };
        let result = self.query(cypher).await?;
        let (after, after_stats) = {
            let l0 = self.tx_l0.read();
            (l0.mutation_count, l0.mutation_stats.clone())
        };
        let affected_rows = if result.is_empty() {
            after.saturating_sub(before)
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

    /// Execute a mutation with parameters using a builder.
    ///
    /// Returns an [`ExecuteBuilder`] that provides `.param()` chaining
    /// and a `.run()` method that returns [`ExecuteResult`].
    pub fn execute_with(&self, cypher: &str) -> ExecuteBuilder<'_> {
        ExecuteBuilder {
            tx: self,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout: None,
        }
    }

    // ── DerivedFactSet Application ─────────────────────────────────────

    /// Apply a `DerivedFactSet` (from a session-level DERIVE) to this transaction.
    ///
    /// Replays the collected Cypher mutation ASTs against the transaction's
    /// private L0 buffer. Logs an info-level warning if the database version
    /// has advanced since the DERIVE was evaluated (version gap > 0).
    #[instrument(skip(self, derived), fields(transaction_id = %self.id))]
    pub async fn apply(&self, derived: DerivedFactSet) -> Result<ApplyResult> {
        self.apply_internal(derived, false, None).await
    }

    /// Start building an apply operation with staleness controls.
    pub fn apply_with(&self, derived: DerivedFactSet) -> ApplyBuilder<'_> {
        ApplyBuilder {
            tx: self,
            derived,
            require_fresh: false,
            max_version_gap: None,
        }
    }

    async fn apply_internal(
        &self,
        derived: DerivedFactSet,
        require_fresh: bool,
        max_gap: Option<u64>,
    ) -> Result<ApplyResult> {
        self.check_completed()?;
        let current_version = self.tx_l0.read().current_version;
        let version_gap = current_version.saturating_sub(derived.evaluated_at_version);

        if require_fresh && version_gap > 0 {
            return Err(UniError::StaleDerivedFacts { version_gap });
        }
        if let Some(max) = max_gap
            && version_gap > max
        {
            return Err(UniError::StaleDerivedFacts { version_gap });
        }
        if version_gap > 0 {
            info!(
                transaction_id = %self.id,
                version_gap,
                "Applying DerivedFactSet with version gap"
            );
        }

        let mut facts_applied = 0;
        for query in derived.mutation_queries {
            self.db
                .execute_ast_internal_with_tx_l0(
                    query,
                    "<locy-apply>",
                    HashMap::new(),
                    self.db.config.clone(),
                    self.tx_l0.clone(),
                )
                .await?;
            facts_applied += 1;
        }

        Ok(ApplyResult {
            facts_applied,
            version_gap,
        })
    }

    // ── Locy Evaluation ───────────────────────────────────────────────

    /// Evaluate a Locy program within the transaction.
    ///
    /// DERIVE commands auto-apply to the transaction's write buffer.
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub async fn locy(&self, program: &str) -> Result<LocyResult> {
        self.check_completed()?;
        // Create a LocyEngine directly from UniInner (which sees tx L0).
        // Transaction path: auto-apply DERIVE mutations to the private L0.
        let engine = impl_locy::LocyEngine {
            db: &self.db,
            tx_l0_override: Some(self.tx_l0.clone()),
            collect_derive: false,
        };
        engine.evaluate(program).await
    }

    /// Evaluate a Locy program with parameters using a builder.
    pub fn locy_with(&self, program: &str) -> crate::api::locy_builder::TxLocyBuilder<'_> {
        crate::api::locy_builder::TxLocyBuilder::new(self, program)
    }

    // ── Prepared Statements ──────────────────────────────────────────

    /// Prepare a Cypher query for repeated execution within this transaction.
    ///
    /// The query is parsed and planned once; subsequent executions skip those
    /// phases. If the schema changes, the prepared query auto-replans.
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub async fn prepare(&self, cypher: &str) -> Result<crate::api::prepared::PreparedQuery> {
        self.check_completed()?;
        crate::api::prepared::PreparedQuery::new(self.db.clone(), cypher).await
    }

    /// Prepare a Locy program for repeated evaluation within this transaction.
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub fn prepare_locy(&self, program: &str) -> Result<crate::api::prepared::PreparedLocy> {
        self.check_completed()?;
        crate::api::prepared::PreparedLocy::new(
            self.db.clone(),
            self.rule_registry.clone(),
            program,
        )
    }

    // ── Rule Management ───────────────────────────────────────────────

    /// Register Locy rules scoped to this transaction.
    /// On commit, new rules are promoted to the session (best-effort).
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub fn register_rules(&self, program: &str) -> Result<()> {
        impl_locy::register_rules_on_registry(&self.rule_registry, program)
    }

    /// Clear all registered rules in this transaction scope.
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub fn clear_rules(&self) {
        let mut registry = self.rule_registry.write().unwrap();
        registry.rules.clear();
        registry.strata.clear();
    }

    // ── Lifecycle ─────────────────────────────────────────────────────

    /// Commit the transaction.
    ///
    /// Persists all changes made during the transaction. Returns a
    /// [`CommitResult`] with commit metadata.
    #[instrument(skip(self), fields(transaction_id = %self.id, duration_ms), level = "info")]
    pub async fn commit(mut self) -> Result<CommitResult> {
        self.check_completed()?;

        let writer_lock = self.db.writer.as_ref().ok_or_else(|| UniError::ReadOnly {
            operation: "commit".to_string(),
        })?;

        // Read mutation count from the private L0 (no lock needed)
        let mutations = self.tx_l0.read().mutation_count;

        // Run before-commit hooks BEFORE acquiring writer lock (rejection point)
        if !self.hooks.is_empty() {
            let ctx = crate::api::hooks::CommitHookContext {
                session_id: self.session_id.clone(),
                tx_id: self.id.clone(),
                mutation_count: mutations,
            };
            for hook in &self.hooks {
                hook.before_commit(&ctx)?;
            }
        }

        // Snapshot labels and edge types from L0 BEFORE commit consumes the buffer
        let (labels_affected, edge_types_affected) = {
            let l0 = self.tx_l0.read();
            let labels: Vec<String> = l0
                .vertex_labels
                .values()
                .flatten()
                .cloned()
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            let edge_types: Vec<String> = l0
                .edge_types
                .values()
                .cloned()
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            (labels, edge_types)
        };

        // Acquire writer WRITE lock for WAL + merge
        let mut writer = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            writer_lock.write(),
        )
        .await
        .map_err(|_| UniError::CommitTimeout {
            tx_id: self.id.clone(),
            hint: "Another commit is in progress and taking longer than expected. Your transaction is still active \u{2014} you can retry commit().",
        })?;
        let wal_lsn = writer.commit_transaction_l0(self.tx_l0.clone()).await?;
        // Update cached metrics atomics while we still hold the writer lock
        {
            let l0 = writer.l0_manager.get_current();
            let l0_guard = l0.read();
            self.db
                .cached_l0_mutation_count
                .store(l0_guard.mutation_count, Ordering::Relaxed);
            self.db
                .cached_l0_estimated_size
                .store(l0_guard.estimated_size, Ordering::Relaxed);
        }
        self.db.cached_wal_lsn.store(wal_lsn, Ordering::Relaxed);
        let version = writer.l0_manager.get_current().read().current_version;
        drop(writer);

        self.completed = true;

        let duration = self.start_time.elapsed();
        tracing::Span::current().record("duration_ms", duration.as_millis());
        metrics::histogram!("uni_transaction_duration_seconds").record(duration.as_secs_f64());
        metrics::counter!("uni_transaction_commits_total").increment(1);

        // Best-effort rule promotion: promote new rules from tx → session
        let mut rule_promotion_errors = Vec::new();
        let rules_promoted = {
            match (
                self.rule_registry.read(),
                self.session_rule_registry.write(),
            ) {
                (Ok(tx_reg), Ok(mut session_reg)) => {
                    let mut promoted = 0;
                    for (name, rule) in &tx_reg.rules {
                        if !session_reg.rules.contains_key(name) {
                            session_reg.rules.insert(name.clone(), rule.clone());
                            promoted += 1;
                        }
                    }
                    promoted
                }
                (Err(e), _) => {
                    rule_promotion_errors.push(RulePromotionError {
                        rule_text: "<all>".into(),
                        error: format!("tx rule registry lock poisoned: {e}"),
                    });
                    0
                }
                (_, Err(e)) => {
                    rule_promotion_errors.push(RulePromotionError {
                        rule_text: "<all>".into(),
                        error: format!("session rule registry lock poisoned: {e}"),
                    });
                    0
                }
            }
        };

        // Release write guard
        self.session_write_guard.store(false, Ordering::SeqCst);

        // Increment session-level commit counter
        self.session_metrics
            .transactions_committed
            .fetch_add(1, Ordering::Relaxed);
        self.db.total_commits.fetch_add(1, Ordering::Relaxed);

        let commit_result = CommitResult {
            mutations_committed: mutations,
            rules_promoted,
            version,
            started_at_version: self.started_at_version,
            wal_lsn,
            duration,
            rule_promotion_errors,
        };

        // Broadcast commit notification (ignore send error — no receivers is fine)
        let notif = crate::api::notifications::CommitNotification {
            version,
            mutation_count: mutations,
            labels_affected,
            edge_types_affected,
            rules_promoted,
            timestamp: chrono::Utc::now(),
            tx_id: self.id.clone(),
            session_id: self.session_id.clone(),
            causal_version: self.started_at_version,
        };
        let _ = self.db.commit_tx.send(Arc::new(notif));

        // Run after-commit hooks (infallible — panics caught and logged)
        if !self.hooks.is_empty() {
            let ctx = crate::api::hooks::CommitHookContext {
                session_id: self.session_id.clone(),
                tx_id: self.id.clone(),
                mutation_count: mutations,
            };
            for hook in &self.hooks {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    hook.after_commit(&ctx, &commit_result);
                }));
                if let Err(e) = result {
                    tracing::error!("after_commit hook panicked: {:?}", e);
                }
            }
        }

        info!("Transaction committed");

        Ok(commit_result)
    }

    /// Rollback the transaction, discarding all changes.
    ///
    /// No writer lock needed — the private L0 is simply dropped. This method
    /// is infallible and synchronous. If the transaction is already completed,
    /// this is a silent no-op (idempotent).
    pub fn rollback(mut self) {
        if self.completed {
            return;
        }
        self.completed = true;

        // Release write guard
        self.session_write_guard.store(false, Ordering::SeqCst);

        let duration = self.start_time.elapsed();
        metrics::histogram!("uni_transaction_duration_seconds").record(duration.as_secs_f64());
        metrics::counter!("uni_transaction_rollbacks_total").increment(1);

        // Increment session-level rollback counter
        self.session_metrics
            .transactions_rolled_back
            .fetch_add(1, Ordering::Relaxed);

        info!("Transaction rolled back");
    }

    /// Check if the transaction has uncommitted changes.
    pub fn is_dirty(&self) -> bool {
        self.tx_l0.read().mutation_count > 0
    }

    /// Get the transaction ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Cancel all in-flight queries in this transaction.
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub fn cancel(&self) {
        self.cancellation_token.cancel();
    }

    /// Get a clone of this transaction's cancellation token.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    fn check_completed(&self) -> Result<()> {
        if self.completed {
            return Err(UniError::TransactionAlreadyCompleted);
        }
        if let Some(deadline) = self.deadline
            && Instant::now() > deadline
        {
            return Err(UniError::TransactionExpired {
                tx_id: self.id.clone(),
                hint: "Transaction exceeded its timeout. All operations are rejected. The transaction will auto-rollback on drop.",
            });
        }
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.completed {
            if self.is_dirty() {
                warn!(
                    transaction_id = %self.id,
                    "Transaction dropped with uncommitted writes — discarding private L0"
                );
            }
            // No writer lock needed — the private L0 drops with the Transaction.
            // Release write guard
            self.session_write_guard.store(false, Ordering::SeqCst);
        }
    }
}

/// Builder for parameterized mutations within a transaction.
///
/// Created by [`Transaction::execute_with()`]. Chain `.param()` calls to bind
/// parameters, then call `.run()` to execute and get an [`ExecuteResult`].
pub struct ExecuteBuilder<'a> {
    tx: &'a Transaction,
    cypher: String,
    params: HashMap<String, Value>,
    timeout: Option<Duration>,
}

impl<'a> ExecuteBuilder<'a> {
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

    /// Execute the mutation and return affected row count with detailed stats.
    pub async fn run(self) -> Result<ExecuteResult> {
        self.tx.check_completed()?;
        let (before, before_stats) = {
            let l0 = self.tx.tx_l0.read();
            (l0.mutation_count, l0.mutation_stats.clone())
        };
        let result = if let Some(t) = self.timeout {
            tokio::time::timeout(
                t,
                self.tx.db.execute_internal_with_tx_l0(
                    &self.cypher,
                    self.params,
                    self.tx.tx_l0.clone(),
                ),
            )
            .await
            .map_err(|_| UniError::Timeout {
                timeout_ms: t.as_millis() as u64,
            })??
        } else {
            self.tx
                .db
                .execute_internal_with_tx_l0(&self.cypher, self.params, self.tx.tx_l0.clone())
                .await?
        };
        let (after, after_stats) = {
            let l0 = self.tx.tx_l0.read();
            (l0.mutation_count, l0.mutation_stats.clone())
        };
        let affected_rows = if result.is_empty() {
            after.saturating_sub(before)
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
}

/// Builder for parameterized queries within a transaction.
pub struct TxQueryBuilder<'a> {
    tx: &'a Transaction,
    cypher: String,
    params: HashMap<String, Value>,
    cancellation_token: Option<CancellationToken>,
}

impl<'a> TxQueryBuilder<'a> {
    /// Bind a parameter to the mutation.
    pub fn param(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.params.insert(name.to_string(), value.into());
        self
    }

    /// Attach a cancellation token for cooperative query cancellation.
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Execute the mutation and return affected row count with detailed stats.
    pub async fn execute(self) -> Result<ExecuteResult> {
        self.tx.check_completed()?;
        let (before, before_stats) = {
            let l0 = self.tx.tx_l0.read();
            (l0.mutation_count, l0.mutation_stats.clone())
        };
        let result = self
            .tx
            .db
            .execute_internal_with_tx_l0(&self.cypher, self.params, self.tx.tx_l0.clone())
            .await?;
        let (after, after_stats) = {
            let l0 = self.tx.tx_l0.read();
            (l0.mutation_count, l0.mutation_stats.clone())
        };
        let affected_rows = if result.is_empty() {
            after.saturating_sub(before)
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

    /// Execute as a query and return rows.
    pub async fn fetch_all(self) -> Result<QueryResult> {
        self.tx.check_completed()?;
        self.tx
            .db
            .execute_internal_with_tx_l0(&self.cypher, self.params, self.tx.tx_l0.clone())
            .await
    }

    /// Execute the query and return the first row, or `None` if empty.
    pub async fn fetch_one(self) -> Result<Option<Row>> {
        let result = self.fetch_all().await?;
        Ok(result.into_rows().into_iter().next())
    }
}

/// Result of applying a `DerivedFactSet` to a transaction.
#[derive(Debug)]
pub struct ApplyResult {
    /// Number of mutation queries replayed.
    pub facts_applied: usize,
    /// Number of versions that committed between DERIVE evaluation and apply.
    /// 0 means the data was fresh.
    pub version_gap: u64,
}

/// Builder for applying a `DerivedFactSet` with staleness controls.
pub struct ApplyBuilder<'a> {
    tx: &'a Transaction,
    derived: DerivedFactSet,
    require_fresh: bool,
    max_version_gap: Option<u64>,
}

impl<'a> ApplyBuilder<'a> {
    /// Require that no commits occurred between DERIVE evaluation and apply.
    /// Returns `StaleDerivedFacts` if the version gap is > 0.
    pub fn require_fresh(mut self) -> Self {
        self.require_fresh = true;
        self
    }

    /// Allow up to `n` versions of gap between evaluation and apply.
    /// Returns `StaleDerivedFacts` if the gap exceeds `n`.
    pub fn max_version_gap(mut self, n: u64) -> Self {
        self.max_version_gap = Some(n);
        self
    }

    /// Execute the apply operation.
    pub async fn run(self) -> Result<ApplyResult> {
        self.tx
            .apply_internal(self.derived, self.require_fresh, self.max_version_gap)
            .await
    }
}
