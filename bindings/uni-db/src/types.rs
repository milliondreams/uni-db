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
