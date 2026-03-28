// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Python data classes for query results, schema info, and statistics.

use pyo3::prelude::*;

/// Information about a vertex label in the schema.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct LabelInfo {
    /// Label name.
    pub name: String,
    /// Approximate count of vertices with this label.
    pub count: usize,
    /// Properties defined on this label.
    pub properties: Vec<PropertyInfo>,
    /// Indexes defined on this label.
    pub indexes: Vec<IndexInfo>,
    /// Constraints defined on this label.
    pub constraints: Vec<ConstraintInfo>,
}

#[pymethods]
impl LabelInfo {
    fn __repr__(&self) -> String {
        format!(
            "LabelInfo(name='{}', count={}, properties={}, indexes={})",
            self.name,
            self.count,
            self.properties.len(),
            self.indexes.len()
        )
    }
}

/// Information about a property in the schema.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct PropertyInfo {
    /// Property name.
    pub name: String,
    /// Data type (e.g., "String", "Int64", "Vector{128}").
    pub data_type: String,
    /// Whether null values are allowed.
    pub nullable: bool,
    /// Whether an index exists on this property.
    pub is_indexed: bool,
}

#[pymethods]
impl PropertyInfo {
    fn __repr__(&self) -> String {
        format!(
            "PropertyInfo(name='{}', type='{}', nullable={})",
            self.name, self.data_type, self.nullable
        )
    }
}

/// Information about an index in the schema.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Type of index (SCALAR, VECTOR, FULLTEXT).
    pub index_type: String,
    /// Properties covered by the index.
    pub properties: Vec<String>,
    /// Current status (ONLINE, BUILDING, FAILED).
    pub status: String,
}

#[pymethods]
impl IndexInfo {
    fn __repr__(&self) -> String {
        format!(
            "IndexInfo(name='{}', type='{}', properties={:?})",
            self.name, self.index_type, self.properties
        )
    }
}

/// Information about a constraint in the schema.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct ConstraintInfo {
    /// Constraint name.
    pub name: String,
    /// Type of constraint (UNIQUE, EXISTS, CHECK).
    pub constraint_type: String,
    /// Properties covered by the constraint.
    pub properties: Vec<String>,
    /// Whether the constraint is currently enforced.
    pub enabled: bool,
}

#[pymethods]
impl ConstraintInfo {
    fn __repr__(&self) -> String {
        format!(
            "ConstraintInfo(name='{}', type='{}', enabled={})",
            self.name, self.constraint_type, self.enabled
        )
    }
}

/// Statistics from a bulk loading operation.
#[pyclass(get_all)]
#[derive(Debug, Clone, Default)]
pub struct BulkStats {
    /// Number of vertices inserted.
    pub vertices_inserted: usize,
    /// Number of edges inserted.
    pub edges_inserted: usize,
    /// Number of indexes rebuilt.
    pub indexes_rebuilt: usize,
    /// Total duration in seconds.
    pub duration_secs: f64,
    /// Duration spent building indexes in seconds.
    pub index_build_duration_secs: f64,
    /// Whether indexes are still building in background.
    pub indexes_pending: bool,
}

#[pymethods]
impl BulkStats {
    fn __repr__(&self) -> String {
        format!(
            "BulkStats(vertices={}, edges={}, duration={:.2}s)",
            self.vertices_inserted, self.edges_inserted, self.duration_secs
        )
    }
}

/// Progress callback data during bulk loading.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct BulkProgress {
    /// Current phase of bulk loading.
    pub phase: String,
    /// Number of rows processed so far.
    pub rows_processed: usize,
    /// Total rows if known.
    pub total_rows: Option<usize>,
    /// Current label being processed.
    pub current_label: Option<String>,
}

#[pymethods]
impl BulkProgress {
    fn __repr__(&self) -> String {
        format!(
            "BulkProgress(phase='{}', processed={})",
            self.phase, self.rows_processed
        )
    }
}

/// Statistics from a Locy program evaluation.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct LocyStats {
    /// Number of strata evaluated.
    pub strata_evaluated: usize,
    /// Total fixpoint iterations across all strata.
    pub total_iterations: usize,
    /// Total derived nodes across all rules.
    pub derived_nodes: usize,
    /// Total derived edges across all rules.
    pub derived_edges: usize,
    /// Total evaluation time in seconds.
    pub evaluation_time_secs: f64,
    /// Number of Cypher queries executed.
    pub queries_executed: usize,
    /// Number of mutations executed.
    pub mutations_executed: usize,
    /// Peak memory used by derived relations in bytes.
    pub peak_memory_bytes: usize,
}

#[pymethods]
impl LocyStats {
    fn __repr__(&self) -> String {
        format!(
            "LocyStats(strata={}, iterations={}, time={:.3}s)",
            self.strata_evaluated, self.total_iterations, self.evaluation_time_secs
        )
    }
}

/// Result of a Locy program evaluation, mirroring the Rust `LocyResult`.
#[pyclass(get_all, name = "LocyResult")]
#[derive(Debug)]
pub struct PyLocyResult {
    /// Derived relations as a dict of relation-name → list[dict].
    pub derived: Py<PyAny>,
    /// Evaluation statistics (`LocyStats`).
    pub stats: Py<PyAny>,
    /// Command results (goal queries, explanations, etc.).
    pub command_results: Py<PyAny>,
    /// Runtime warnings emitted during evaluation.
    pub warnings: Py<PyAny>,
    /// Groups flagged as approximate (shared-proof detection).
    pub approximate_groups: Py<PyAny>,
    /// Opaque derived fact set, pass to `tx.apply()` to materialize.
    pub derived_fact_set: Py<PyAny>,
}

#[pymethods]
impl PyLocyResult {
    fn __repr__(&self, py: Python<'_>) -> String {
        let n_warnings = self.warnings.bind(py).len().unwrap_or(0);
        format!(
            "LocyResult(stats={}, warnings={})",
            self.stats
                .bind(py)
                .repr()
                .map_or_else(|_| "?".to_string(), |r| r.to_string(),),
            n_warnings,
        )
    }

    /// Check whether any warning with the given code string was emitted.
    fn has_warning(&self, py: Python<'_>, code: &str) -> PyResult<bool> {
        let list = self.warnings.bind(py);
        let len = list.len()?;
        for i in 0..len {
            let item = list.get_item(i)?;
            // Each warning is expected to have a `.code` attribute (string).
            if let Ok(c) = item.getattr("code")
                && let Ok(s) = c.extract::<String>()
                && s == code
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Return the list of runtime warnings.
    fn warnings_list(&self, py: Python<'_>) -> Py<PyAny> {
        self.warnings.clone_ref(py)
    }
}

/// A compiled Locy program ready for evaluation.
#[pyclass(name = "CompiledProgram")]
pub struct PyCompiledProgram {
    pub(crate) inner: uni_locy::CompiledProgram,
}

#[pymethods]
impl PyCompiledProgram {
    fn __repr__(&self) -> String {
        format!(
            "CompiledProgram(strata={}, rules={})",
            self.inner.strata.len(),
            self.inner.rule_catalog.len(),
        )
    }

    /// Number of strata in the compiled program.
    #[getter]
    fn num_strata(&self) -> usize {
        self.inner.strata.len()
    }

    /// Number of compiled rules.
    #[getter]
    fn num_rules(&self) -> usize {
        self.inner.rule_catalog.len()
    }

    /// Names of all compiled rules.
    #[getter]
    fn rule_names(&self) -> Vec<String> {
        self.inner.rule_catalog.keys().cloned().collect()
    }
}

// ============================================================================
// Xervo types
// ============================================================================

/// A message in a conversation (role + text content).
#[pyclass(get_all, name = "Message")]
#[derive(Debug, Clone)]
pub struct PyMessage {
    /// Role: "user", "assistant", or "system".
    pub role: String,
    /// Text content of the message.
    pub content: String,
}

#[pymethods]
impl PyMessage {
    #[new]
    fn new(role: String, content: String) -> Self {
        Self { role, content }
    }

    /// Create a user message.
    #[staticmethod]
    fn user(text: String) -> Self {
        Self {
            role: "user".to_string(),
            content: text,
        }
    }

    /// Create an assistant message.
    #[staticmethod]
    fn assistant(text: String) -> Self {
        Self {
            role: "assistant".to_string(),
            content: text,
        }
    }

    /// Create a system message.
    #[staticmethod]
    fn system(text: String) -> Self {
        Self {
            role: "system".to_string(),
            content: text,
        }
    }

    fn __repr__(&self) -> String {
        format!("Message(role='{}', content='{}')", self.role, self.content)
    }
}

/// Token usage statistics from a generation call.
#[pyclass(get_all, name = "TokenUsage")]
#[derive(Debug, Clone)]
pub struct PyTokenUsage {
    pub prompt_tokens: usize,
    pub completion_tokens: usize,
    pub total_tokens: usize,
}

#[pymethods]
impl PyTokenUsage {
    fn __repr__(&self) -> String {
        format!(
            "TokenUsage(prompt={}, completion={}, total={})",
            self.prompt_tokens, self.completion_tokens, self.total_tokens
        )
    }
}

/// Result of a Xervo generation call.
#[pyclass(get_all, name = "GenerationResult")]
#[derive(Debug)]
pub struct PyGenerationResult {
    /// The generated text.
    pub text: String,
    /// Token usage statistics, if available.
    pub usage: Option<Py<PyTokenUsage>>,
}

#[pymethods]
impl PyGenerationResult {
    fn __repr__(&self) -> String {
        format!(
            "GenerationResult(text='{}...')",
            &self.text.chars().take(40).collect::<String>()
        )
    }
}

// ============================================================================
// Snapshot + Index types
// ============================================================================

/// Information about a database snapshot.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// Unique snapshot identifier.
    pub snapshot_id: String,
    /// Optional human-readable name.
    pub name: Option<String>,
    /// ISO 8601 creation timestamp.
    pub created_at: String,
    /// Version high-water mark at snapshot time.
    pub version_hwm: u64,
}

#[pymethods]
impl SnapshotInfo {
    fn __repr__(&self) -> String {
        format!(
            "SnapshotInfo(id='{}', name={:?}, created_at='{}')",
            self.snapshot_id, self.name, self.created_at
        )
    }
}

/// Status of a background index rebuild task.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct IndexRebuildTaskInfo {
    pub id: String,
    pub label: String,
    pub status: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub error: Option<String>,
    pub retry_count: u32,
}

#[pymethods]
impl IndexRebuildTaskInfo {
    fn __repr__(&self) -> String {
        format!(
            "IndexRebuildTaskInfo(id='{}', label='{}', status='{}')",
            self.id, self.label, self.status
        )
    }
}

/// Definition of an index in the schema.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct IndexDefinitionInfo {
    pub name: String,
    pub index_type: String,
    pub label: String,
    pub properties: Vec<String>,
    pub state: String,
}

#[pymethods]
impl IndexDefinitionInfo {
    fn __repr__(&self) -> String {
        format!(
            "IndexDefinitionInfo(name='{}', type='{}', label='{}')",
            self.name, self.index_type, self.label
        )
    }
}

// ============================================================================
// Commit notification
// ============================================================================

/// A commit notification describing the effects of a committed transaction.
#[pyclass(get_all, name = "CommitNotification")]
#[derive(Debug, Clone)]
pub struct PyCommitNotification {
    /// Database version after commit.
    pub version: u64,
    /// Number of mutations in the committed transaction.
    pub mutation_count: usize,
    /// Vertex labels affected by the commit.
    pub labels_affected: Vec<String>,
    /// Edge types affected by the commit.
    pub edge_types_affected: Vec<String>,
    /// Number of Locy rules promoted.
    pub rules_promoted: usize,
    /// ISO 8601 timestamp of the commit.
    pub timestamp: String,
    /// Transaction ID.
    pub tx_id: String,
    /// Session ID that committed the transaction.
    pub session_id: String,
    /// Database version when the transaction started.
    pub causal_version: u64,
}

#[pymethods]
impl PyCommitNotification {
    fn __repr__(&self) -> String {
        format!(
            "CommitNotification(version={}, mutations={}, labels={:?})",
            self.version, self.mutation_count, self.labels_affected
        )
    }
}

impl From<::uni_db::CommitNotification> for PyCommitNotification {
    fn from(n: ::uni_db::CommitNotification) -> Self {
        Self {
            version: n.version,
            mutation_count: n.mutation_count,
            labels_affected: n.labels_affected,
            edge_types_affected: n.edge_types_affected,
            rules_promoted: n.rules_promoted,
            timestamp: n.timestamp.to_rfc3339(),
            tx_id: n.tx_id,
            session_id: n.session_id,
            causal_version: n.causal_version,
        }
    }
}

/// Session capabilities snapshot.
#[pyclass(get_all, name = "SessionCapabilities")]
#[derive(Debug, Clone)]
pub struct PySessionCapabilities {
    /// Whether the session can create transactions and execute writes.
    pub can_write: bool,
    /// Whether the session supports version pinning.
    pub can_pin: bool,
    /// The isolation level used for transactions.
    pub isolation: String,
    /// Whether commit notifications are available.
    pub has_notifications: bool,
}

#[pymethods]
impl PySessionCapabilities {
    fn __repr__(&self) -> String {
        format!(
            "SessionCapabilities(can_write={}, has_notifications={})",
            self.can_write, self.has_notifications
        )
    }
}

// ============================================================================
// Transaction commit result
// ============================================================================

/// Result of committing a transaction.
#[pyclass(name = "CommitResult")]
pub struct PyCommitResult {
    /// Number of mutations committed.
    #[pyo3(get)]
    pub mutations_committed: usize,
    /// Number of rules promoted to the parent session.
    #[pyo3(get)]
    pub rules_promoted: usize,
    /// Database version after commit.
    #[pyo3(get)]
    pub version: u64,
    /// Database version when the transaction was created.
    #[pyo3(get)]
    pub started_at_version: u64,
    /// WAL log sequence number.
    #[pyo3(get)]
    pub wal_lsn: u64,
    /// Duration of the commit operation in seconds.
    #[pyo3(get)]
    pub duration_secs: f64,
}

impl From<::uni_db::CommitResult> for PyCommitResult {
    fn from(r: ::uni_db::CommitResult) -> Self {
        Self {
            mutations_committed: r.mutations_committed,
            rules_promoted: r.rules_promoted,
            version: r.version,
            started_at_version: r.started_at_version,
            wal_lsn: r.wal_lsn,
            duration_secs: r.duration.as_secs_f64(),
        }
    }
}

#[pymethods]
impl PyCommitResult {
    /// Number of versions between start and commit (0 means no concurrent commits).
    fn version_gap(&self) -> u64 {
        self.version.saturating_sub(self.started_at_version + 1)
    }

    fn __repr__(&self) -> String {
        format!(
            "CommitResult(mutations={}, version={}, duration={:.3}s)",
            self.mutations_committed, self.version, self.duration_secs
        )
    }
}

// ============================================================================
// PreparedQuery
// ============================================================================

/// A prepared Cypher query that can be executed multiple times with different parameters.
#[pyclass]
pub struct PyPreparedQuery {
    pub inner: std::sync::Mutex<::uni_db::PreparedQuery>,
}

#[pymethods]
impl PyPreparedQuery {
    /// Execute the prepared query with optional parameter bindings.
    #[pyo3(signature = (params=None))]
    fn execute(
        &self,
        py: pyo3::Python,
        params: Option<std::collections::HashMap<String, pyo3::Py<pyo3::PyAny>>>,
    ) -> pyo3::PyResult<Vec<pyo3::Py<pyo3::PyAny>>> {
        let rust_params: Vec<(String, ::uni_db::Value)> = if let Some(p) = params {
            p.into_iter()
                .map(|(k, v)| {
                    let val = crate::convert::py_object_to_value(py, &v)?;
                    Ok((k, val))
                })
                .collect::<pyo3::PyResult<Vec<_>>>()?
        } else {
            Vec::new()
        };
        let param_refs: Vec<(&str, ::uni_db::Value)> = rust_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(guard.execute(&param_refs))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        crate::convert::rows_to_py(py, result.into_rows())
    }

    /// Get the original query text.
    fn query_text(&self) -> pyo3::PyResult<String> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(guard.query_text().to_string())
    }

    fn __repr__(&self) -> String {
        let text = self
            .inner
            .lock()
            .map(|g| g.query_text().to_string())
            .unwrap_or_else(|_| "<locked>".to_string());
        format!("PreparedQuery({:?})", text)
    }
}

// ============================================================================
// AutoCommitResult
// ============================================================================

/// Result of an auto-committed session.execute() call.
#[pyclass(get_all, name = "AutoCommitResult")]
#[derive(Debug)]
pub struct PyAutoCommitResult {
    pub affected_rows: usize,
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
    pub properties_removed: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
    pub version: u64,
    pub metrics: Py<pyo3::types::PyDict>,
}

#[pymethods]
impl PyAutoCommitResult {
    fn __repr__(&self) -> String {
        format!(
            "AutoCommitResult(affected={}, version={})",
            self.affected_rows, self.version
        )
    }
}

// ============================================================================
// ExecuteResult
// ============================================================================

/// Result of a transaction.execute() call.
#[pyclass(get_all, name = "ExecuteResult")]
#[derive(Debug)]
pub struct PyExecuteResult {
    pub affected_rows: usize,
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
    pub metrics: Py<pyo3::types::PyDict>,
}

#[pymethods]
impl PyExecuteResult {
    fn __repr__(&self) -> String {
        format!(
            "ExecuteResult(affected={}, nodes_created={})",
            self.affected_rows, self.nodes_created
        )
    }
}

// ============================================================================
// ApplyResult
// ============================================================================

/// Result of applying a DerivedFactSet to a transaction.
#[pyclass(get_all, name = "ApplyResult")]
#[derive(Debug, Clone)]
pub struct PyApplyResult {
    pub facts_applied: usize,
    pub version_gap: u64,
}

#[pymethods]
impl PyApplyResult {
    fn __repr__(&self) -> String {
        format!(
            "ApplyResult(facts={}, version_gap={})",
            self.facts_applied, self.version_gap
        )
    }
}

// ============================================================================
// DerivedFactSet
// ============================================================================

/// Opaque wrapper around a Locy-derived fact set.
///
/// Obtained from `LocyResult.derived_fact_set` and passed to `tx.apply()`.
#[pyclass(name = "DerivedFactSet")]
pub struct PyDerivedFactSet {
    pub(crate) inner: Option<uni_locy::DerivedFactSet>,
}

#[pymethods]
impl PyDerivedFactSet {
    /// Database version at evaluation time.
    #[getter]
    fn evaluated_at_version(&self) -> PyResult<u64> {
        self.inner
            .as_ref()
            .map(|d| d.evaluated_at_version)
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("DerivedFactSet already consumed")
            })
    }

    /// Number of derived vertices.
    #[getter]
    fn vertex_count(&self) -> PyResult<usize> {
        self.inner
            .as_ref()
            .map(|d| d.vertices.values().map(|v| v.len()).sum())
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("DerivedFactSet already consumed")
            })
    }

    /// Number of derived edges.
    #[getter]
    fn edge_count(&self) -> PyResult<usize> {
        self.inner.as_ref().map(|d| d.edges.len()).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("DerivedFactSet already consumed")
        })
    }

    /// Total number of derived facts.
    #[getter]
    fn fact_count(&self) -> PyResult<usize> {
        self.inner.as_ref().map(|d| d.fact_count()).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("DerivedFactSet already consumed")
        })
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            Some(d) => format!(
                "DerivedFactSet(facts={}, version={})",
                d.fact_count(),
                d.evaluated_at_version
            ),
            None => "DerivedFactSet(<consumed>)".to_string(),
        }
    }
}

// ============================================================================
// SessionMetrics
// ============================================================================

/// Metrics for a session's lifetime.
#[pyclass(get_all, name = "SessionMetrics")]
#[derive(Debug, Clone)]
pub struct PySessionMetrics {
    pub session_id: String,
    pub queries_executed: u64,
    pub locy_evaluations: u64,
    pub total_query_time_secs: f64,
    pub transactions_committed: u64,
    pub transactions_rolled_back: u64,
    pub total_rows_returned: u64,
    pub total_rows_scanned: u64,
    pub plan_cache_hits: u64,
    pub plan_cache_misses: u64,
    pub plan_cache_size: usize,
}

#[pymethods]
impl PySessionMetrics {
    fn __repr__(&self) -> String {
        format!(
            "SessionMetrics(session='{}', queries={}, txns={})",
            self.session_id, self.queries_executed, self.transactions_committed
        )
    }
}

// ============================================================================
// PreparedLocy
// ============================================================================

/// A prepared Locy program that can be executed multiple times.
#[pyclass(name = "PreparedLocy")]
pub struct PyPreparedLocy {
    pub(crate) inner: std::sync::Mutex<::uni_db::PreparedLocy>,
}

#[pymethods]
impl PyPreparedLocy {
    /// Execute the prepared Locy program.
    fn execute(&self, py: pyo3::Python) -> pyo3::PyResult<PyLocyResult> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(guard.execute())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        crate::convert::locy_result_to_py_class(py, result)
    }

    /// Get the original program text.
    fn program_text(&self) -> pyo3::PyResult<String> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(guard.program_text().to_string())
    }

    fn __repr__(&self) -> String {
        let text = self
            .inner
            .lock()
            .map(|g| {
                let t = g.program_text();
                if t.len() > 60 {
                    format!("{}...", &t[..60])
                } else {
                    t.to_string()
                }
            })
            .unwrap_or_else(|_| "<locked>".to_string());
        format!("PreparedLocy({:?})", text)
    }
}

// ============================================================================
// DatabaseMetrics
// ============================================================================

/// Database-wide metrics snapshot.
#[pyclass(get_all, name = "DatabaseMetrics")]
#[derive(Debug, Clone)]
pub struct PyDatabaseMetrics {
    pub l0_mutation_count: usize,
    pub l0_estimated_size_bytes: usize,
    pub schema_version: u32,
    pub uptime_secs: f64,
    pub active_sessions: usize,
    pub l1_run_count: usize,
    pub write_throttle_pressure: f64,
    pub compaction_status: Option<String>,
    pub wal_size_bytes: usize,
    pub wal_lsn: u64,
    pub total_queries: u64,
    pub total_commits: u64,
}

#[pymethods]
impl PyDatabaseMetrics {
    fn __repr__(&self) -> String {
        format!(
            "DatabaseMetrics(queries={}, commits={}, sessions={}, uptime={:.1}s)",
            self.total_queries, self.total_commits, self.active_sessions, self.uptime_secs
        )
    }
}

// ============================================================================
// CommitStream (sync iterator)
// ============================================================================

/// A synchronous iterator over commit notifications.
///
/// Use as a context manager or call `close()` when done.
#[pyclass(name = "CommitStream")]
pub struct PyCommitStream {
    pub(crate) inner: std::sync::Mutex<Option<::uni_db::CommitStream>>,
}

#[pymethods]
impl PyCommitStream {
    fn __iter__(slf: pyo3::PyRef<'_, Self>) -> pyo3::PyRef<'_, Self> {
        slf
    }

    fn __next__(&self) -> pyo3::PyResult<Option<PyCommitNotification>> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let stream = match guard.as_mut() {
            Some(s) => s,
            None => return Ok(None),
        };
        let notification = pyo3_async_runtimes::tokio::get_runtime().block_on(stream.next());
        match notification {
            Some(n) => Ok(Some(PyCommitNotification::from(n))),
            None => Ok(None),
        }
    }

    /// Close the stream, releasing resources.
    fn close(&self) -> pyo3::PyResult<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        guard.take();
        Ok(())
    }

    fn __enter__(slf: pyo3::PyRef<'_, Self>) -> pyo3::PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<pyo3::Py<pyo3::PyAny>>,
        _exc_val: Option<pyo3::Py<pyo3::PyAny>>,
        _exc_tb: Option<pyo3::Py<pyo3::PyAny>>,
    ) -> pyo3::PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}

// ============================================================================
// WatchBuilder
// ============================================================================

/// Builder for configuring a commit watch stream.
#[pyclass(name = "WatchBuilder")]
pub struct PyWatchBuilder {
    pub(crate) inner: Option<::uni_db::WatchBuilder>,
}

#[pymethods]
impl PyWatchBuilder {
    /// Filter notifications to only include commits affecting these labels.
    fn labels<'py>(
        mut slf: pyo3::PyRefMut<'py, Self>,
        labels: Vec<String>,
    ) -> pyo3::PyResult<pyo3::PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("WatchBuilder already consumed")
        })?;
        let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
        slf.inner = Some(builder.labels(&label_refs));
        Ok(slf)
    }

    /// Filter notifications to only include commits affecting these edge types.
    fn edge_types<'py>(
        mut slf: pyo3::PyRefMut<'py, Self>,
        types: Vec<String>,
    ) -> pyo3::PyResult<pyo3::PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("WatchBuilder already consumed")
        })?;
        let type_refs: Vec<&str> = types.iter().map(|s| s.as_str()).collect();
        slf.inner = Some(builder.edge_types(&type_refs));
        Ok(slf)
    }

    /// Set a debounce interval in seconds.
    fn debounce<'py>(
        mut slf: pyo3::PyRefMut<'py, Self>,
        seconds: f64,
    ) -> pyo3::PyResult<pyo3::PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("WatchBuilder already consumed")
        })?;
        slf.inner = Some(builder.debounce(std::time::Duration::from_secs_f64(seconds)));
        Ok(slf)
    }

    /// Exclude notifications from a specific session.
    fn exclude_session<'py>(
        mut slf: pyo3::PyRefMut<'py, Self>,
        session_id: &str,
    ) -> pyo3::PyResult<pyo3::PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("WatchBuilder already consumed")
        })?;
        slf.inner = Some(builder.exclude_session(session_id));
        Ok(slf)
    }

    /// Build and return a synchronous CommitStream.
    fn build(&mut self) -> pyo3::PyResult<PyCommitStream> {
        let builder = self.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("WatchBuilder already consumed")
        })?;
        Ok(PyCommitStream {
            inner: std::sync::Mutex::new(Some(builder.build())),
        })
    }

    /// Build and return an async CommitStream.
    fn build_async(&mut self) -> pyo3::PyResult<crate::async_api::AsyncCommitStream> {
        let builder = self.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("WatchBuilder already consumed")
        })?;
        Ok(crate::async_api::AsyncCommitStream {
            inner: std::sync::Arc::new(tokio::sync::Mutex::new(Some(builder.build()))),
        })
    }
}
