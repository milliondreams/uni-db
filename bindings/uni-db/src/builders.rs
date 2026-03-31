// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Builder types for schema, labels, edge types, sessions, and bulk writers.

use crate::convert;
use crate::core::{self, OpenMode};
use crate::types::BulkStats;
use ::uni_db::Uni;
use pyo3::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use uni_common::core::schema::{DataType, IndexDefinition};

// ============================================================================
// DatabaseBuilder
// ============================================================================

/// Builder for creating and configuring a Uni instance.
#[pyclass(name = "UniBuilder")]
#[derive(Debug, Clone)]
pub struct DatabaseBuilder {
    pub(crate) uri: String,
    pub(crate) mode: OpenMode,
    pub(crate) hybrid_local: Option<String>,
    pub(crate) hybrid_remote: Option<String>,
    pub(crate) cache_size: Option<usize>,
    pub(crate) parallelism: Option<usize>,
    pub(crate) schema_file: Option<String>,
    pub(crate) xervo_catalog_json: Option<String>,
    pub(crate) xervo_catalog_file: Option<String>,
    pub(crate) cloud_config: Option<uni_common::CloudStorageConfig>,
    pub(crate) uni_config: Option<uni_common::UniConfig>,
    pub(crate) read_only: bool,
    pub(crate) write_lease: Option<crate::types::PyWriteLease>,
}

#[pymethods]
impl DatabaseBuilder {
    /// Open or create a database at the given path.
    #[staticmethod]
    fn open(path: &str) -> Self {
        Self {
            uri: path.to_string(),
            mode: OpenMode::Open,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
            schema_file: None,
            xervo_catalog_json: None,
            xervo_catalog_file: None,
            cloud_config: None,
            uni_config: None,
            read_only: false,
            write_lease: None,
        }
    }

    /// Open an existing database. Fails if it does not exist.
    #[staticmethod]
    fn open_existing(path: &str) -> Self {
        Self {
            uri: path.to_string(),
            mode: OpenMode::OpenExisting,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
            schema_file: None,
            xervo_catalog_json: None,
            xervo_catalog_file: None,
            cloud_config: None,
            uni_config: None,
            read_only: false,
            write_lease: None,
        }
    }

    /// Create a new database. Fails if it already exists.
    #[staticmethod]
    fn create(path: &str) -> Self {
        Self {
            uri: path.to_string(),
            mode: OpenMode::Create,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
            schema_file: None,
            xervo_catalog_json: None,
            xervo_catalog_file: None,
            cloud_config: None,
            uni_config: None,
            read_only: false,
            write_lease: None,
        }
    }

    /// Create a temporary database that is deleted when dropped.
    #[staticmethod]
    pub fn temporary() -> Self {
        Self {
            uri: String::new(),
            mode: OpenMode::Temporary,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
            schema_file: None,
            xervo_catalog_json: None,
            xervo_catalog_file: None,
            cloud_config: None,
            uni_config: None,
            read_only: false,
            write_lease: None,
        }
    }

    /// Create an in-memory database (alias for temporary).
    #[staticmethod]
    fn in_memory() -> Self {
        Self::temporary()
    }

    /// Configure hybrid storage with local metadata and remote data.
    fn hybrid(
        mut slf: PyRefMut<'_, Self>,
        local_path: String,
        remote_url: String,
    ) -> PyRefMut<'_, Self> {
        slf.hybrid_local = Some(local_path);
        slf.hybrid_remote = Some(remote_url);
        slf
    }

    /// Set maximum cache size in bytes.
    fn cache_size(mut slf: PyRefMut<'_, Self>, bytes: usize) -> PyRefMut<'_, Self> {
        slf.cache_size = Some(bytes);
        slf
    }

    /// Set query parallelism (number of worker threads).
    fn parallelism(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.parallelism = Some(n);
        slf
    }

    /// Load schema from a JSON file on initialization.
    fn schema_file(mut slf: PyRefMut<'_, Self>, path: String) -> PyRefMut<'_, Self> {
        slf.schema_file = Some(path);
        slf
    }

    /// Configure the Xervo model catalog from a JSON string.
    fn xervo_catalog_from_str(mut slf: PyRefMut<'_, Self>, json: String) -> PyRefMut<'_, Self> {
        slf.xervo_catalog_json = Some(json);
        slf.xervo_catalog_file = None;
        slf
    }

    /// Configure the Xervo model catalog from a JSON file path.
    fn xervo_catalog_from_file(mut slf: PyRefMut<'_, Self>, path: String) -> PyRefMut<'_, Self> {
        slf.xervo_catalog_file = Some(path);
        slf.xervo_catalog_json = None;
        slf
    }

    /// Configure cloud storage credentials (dict with 'provider' key: 's3', 'gcs', or 'azure').
    fn cloud_config(
        mut slf: PyRefMut<'_, Self>,
        config: HashMap<String, Py<PyAny>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let py = slf.py();
        slf.cloud_config = Some(convert::extract_cloud_config(py, &config)?);
        Ok(slf)
    }

    /// Configure database options (query_timeout, max_query_memory, etc.).
    fn config(
        mut slf: PyRefMut<'_, Self>,
        config: HashMap<String, Py<PyAny>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let py = slf.py();
        slf.uni_config = Some(convert::extract_uni_config(py, &config)?);
        Ok(slf)
    }

    /// Set I/O batch size (default 1024).
    fn batch_size(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .batch_size = size;
        slf
    }

    /// Enable or disable write-ahead log (default true).
    fn wal_enabled(mut slf: PyRefMut<'_, Self>, enabled: bool) -> PyRefMut<'_, Self> {
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .wal_enabled = enabled;
        slf
    }

    /// Open the database in read-only mode (no writes allowed).
    fn read_only(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.read_only = true;
        slf
    }

    /// Configure write lease for multi-agent coordination.
    fn write_lease(
        mut slf: PyRefMut<'_, Self>,
        lease: crate::types::PyWriteLease,
    ) -> PyRefMut<'_, Self> {
        slf.write_lease = Some(lease);
        slf
    }

    /// Build and return the Database instance.
    fn build(&self) -> PyResult<crate::sync_api::Database> {
        let rust_write_lease = self.write_lease.as_ref().map(|wl| match &wl.variant {
            crate::types::WriteLeaseVariant::Local => ::uni_db::api::multi_agent::WriteLease::Local,
            crate::types::WriteLeaseVariant::DynamoDB { table } => {
                ::uni_db::api::multi_agent::WriteLease::DynamoDB {
                    table: table.clone(),
                }
            }
        });
        let uni = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::build_database_core(
                &self.uri,
                self.mode,
                self.hybrid_local.as_deref(),
                self.hybrid_remote.as_deref(),
                self.cache_size,
                self.parallelism,
                self.schema_file.as_deref(),
                self.xervo_catalog_json.as_deref(),
                self.xervo_catalog_file.as_deref(),
                self.cloud_config.clone(),
                self.uni_config.clone(),
                self.read_only,
                rust_write_lease,
            ))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;

        Ok(crate::sync_api::Database {
            inner: Arc::new(uni),
        })
    }
}

// ============================================================================
// SchemaBuilder, LabelBuilder, EdgeTypeBuilder
// ============================================================================

/// Builder for defining and modifying the graph schema.
#[pyclass]
#[derive(Clone)]
pub struct SchemaBuilder {
    pub(crate) inner: Arc<Uni>,
    pub(crate) pending_labels: Vec<String>,
    pub(crate) pending_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    pub(crate) pending_properties: Vec<(String, String, DataType, bool)>,
    pub(crate) pending_indexes: Vec<IndexDefinition>,
}

#[pymethods]
impl SchemaBuilder {
    /// Start defining a new label.
    fn label(&self, name: &str) -> PyResult<LabelBuilder> {
        Ok(LabelBuilder {
            parent_inner: self.inner.clone(),
            parent_labels: self.pending_labels.clone(),
            parent_edge_types: self.pending_edge_types.clone(),
            parent_properties: self.pending_properties.clone(),
            parent_indexes: self.pending_indexes.clone(),
            name: name.to_string(),
            properties: Vec::new(),
            indexes: Vec::new(),
        })
    }

    /// Start defining a new edge type.
    fn edge_type(
        &self,
        name: &str,
        from_labels: Vec<String>,
        to_labels: Vec<String>,
    ) -> PyResult<EdgeTypeBuilder> {
        Ok(EdgeTypeBuilder {
            parent_inner: self.inner.clone(),
            parent_labels: self.pending_labels.clone(),
            parent_edge_types: self.pending_edge_types.clone(),
            parent_properties: self.pending_properties.clone(),
            parent_indexes: self.pending_indexes.clone(),
            name: name.to_string(),
            from_labels,
            to_labels,
            properties: Vec::new(),
        })
    }

    /// Apply all pending schema changes.
    fn apply(&self) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::apply_schema_core(
                &self.inner,
                &self.pending_labels,
                &self.pending_edge_types,
                &self.pending_properties,
                &self.pending_indexes,
            ))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(())
    }
}

/// Parse an index config from either a string or dict Python object.
pub(crate) fn parse_index_config(
    py: Python<'_>,
    label: &str,
    property: &str,
    index_type: &Py<PyAny>,
) -> PyResult<IndexDefinition> {
    let bound = index_type.bind(py);
    if let Ok(type_str) = bound.extract::<String>() {
        core::create_index_definition(label, property, &type_str)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)
    } else if bound.is_instance_of::<pyo3::types::PyDict>() {
        let dict: HashMap<String, Py<PyAny>> = bound.extract()?;
        let mut config = std::collections::HashMap::new();
        for (k, v) in &dict {
            let val = crate::convert::py_object_to_json(py, v)?;
            config.insert(k.clone(), val);
        }
        core::create_index_definition_from_config(label, property, &config)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "index_type must be a string or dict",
        ))
    }
}

/// Builder for defining a label with its properties and indexes.
#[pyclass]
#[derive(Clone)]
pub struct LabelBuilder {
    parent_inner: Arc<Uni>,
    parent_labels: Vec<String>,
    parent_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    parent_properties: Vec<(String, String, DataType, bool)>,
    parent_indexes: Vec<IndexDefinition>,
    name: String,
    properties: Vec<(String, DataType, bool)>,
    indexes: Vec<IndexDefinition>,
}

#[pymethods]
impl LabelBuilder {
    /// Add a required property to this label.
    fn property<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        data_type: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let dt = if let Ok(py_dt) = data_type.extract::<crate::types::PyDataType>() {
            py_dt.inner
        } else {
            let s: String = data_type.extract()?;
            crate::core::parse_data_type(&s)
                .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?
        };
        slf.properties.push((name, dt, false));
        Ok(slf)
    }

    /// Add a nullable property to this label.
    fn property_nullable<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        data_type: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let dt = if let Ok(py_dt) = data_type.extract::<crate::types::PyDataType>() {
            py_dt.inner
        } else {
            let s: String = data_type.extract()?;
            crate::core::parse_data_type(&s)
                .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?
        };
        slf.properties.push((name, dt, true));
        Ok(slf)
    }

    /// Add a vector property (shorthand for vector type + index).
    fn vector(mut slf: PyRefMut<'_, Self>, name: String, dimensions: usize) -> PyRefMut<'_, Self> {
        slf.properties
            .push((name, DataType::Vector { dimensions }, false));
        slf
    }

    /// Add an index on a property.
    ///
    /// `index_type` can be a string (e.g., `"btree"`, `"vector"`, `"fulltext"`, `"inverted"`)
    /// or a dict with a `"type"` key and additional configuration:
    ///
    /// ```python
    /// # Simple (string):
    /// builder.index("name", "btree")
    ///
    /// # Rich (dict):
    /// builder.index("embedding", {
    ///     "type": "vector", "algorithm": "hnsw",
    ///     "m": 32, "ef_construction": 400, "metric": "l2"
    /// })
    /// builder.index("content", {
    ///     "type": "fulltext", "tokenizer": "ngram",
    ///     "ngram_min": 2, "ngram_max": 4
    /// })
    /// ```
    fn index<'py>(
        mut slf: PyRefMut<'py, Self>,
        property: String,
        index_type: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        let label = slf.name.clone();
        let idx = parse_index_config(py, &label, &property, &index_type)?;
        slf.indexes.push(idx);
        Ok(slf)
    }

    /// Finish this label and return to SchemaBuilder.
    fn done(&self) -> PyResult<SchemaBuilder> {
        let mut labels = self.parent_labels.clone();
        labels.push(self.name.clone());

        let mut properties = self.parent_properties.clone();
        for (prop_name, dt, nullable) in &self.properties {
            properties.push((self.name.clone(), prop_name.clone(), dt.clone(), *nullable));
        }

        let mut indexes = self.parent_indexes.clone();
        indexes.extend(self.indexes.clone());

        Ok(SchemaBuilder {
            inner: self.parent_inner.clone(),
            pending_labels: labels,
            pending_edge_types: self.parent_edge_types.clone(),
            pending_properties: properties,
            pending_indexes: indexes,
        })
    }

    /// Apply schema changes immediately.
    fn apply(&self) -> PyResult<()> {
        self.done()?.apply()
    }
}

/// Builder for defining an edge type with its properties.
#[pyclass]
#[derive(Clone)]
pub struct EdgeTypeBuilder {
    parent_inner: Arc<Uni>,
    parent_labels: Vec<String>,
    parent_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    parent_properties: Vec<(String, String, DataType, bool)>,
    parent_indexes: Vec<IndexDefinition>,
    name: String,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
    properties: Vec<(String, DataType, bool)>,
}

#[pymethods]
impl EdgeTypeBuilder {
    /// Add a required property to this edge type.
    fn property<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        data_type: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let dt = if let Ok(py_dt) = data_type.extract::<crate::types::PyDataType>() {
            py_dt.inner
        } else {
            let s: String = data_type.extract()?;
            crate::core::parse_data_type(&s)
                .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?
        };
        slf.properties.push((name, dt, false));
        Ok(slf)
    }

    /// Add a nullable property to this edge type.
    fn property_nullable<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        data_type: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let dt = if let Ok(py_dt) = data_type.extract::<crate::types::PyDataType>() {
            py_dt.inner
        } else {
            let s: String = data_type.extract()?;
            crate::core::parse_data_type(&s)
                .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?
        };
        slf.properties.push((name, dt, true));
        Ok(slf)
    }

    /// Finish this edge type and return to SchemaBuilder.
    fn done(&self) -> PyResult<SchemaBuilder> {
        let mut edge_types = self.parent_edge_types.clone();
        edge_types.push((
            self.name.clone(),
            self.from_labels.clone(),
            self.to_labels.clone(),
        ));

        let mut properties = self.parent_properties.clone();
        for (prop_name, dt, nullable) in &self.properties {
            properties.push((self.name.clone(), prop_name.clone(), dt.clone(), *nullable));
        }

        Ok(SchemaBuilder {
            inner: self.parent_inner.clone(),
            pending_labels: self.parent_labels.clone(),
            pending_edge_types: edge_types,
            pending_properties: properties,
            pending_indexes: self.parent_indexes.clone(),
        })
    }

    /// Apply schema changes immediately.
    fn apply(&self) -> PyResult<()> {
        self.done()?.apply()
    }
}

// ============================================================================
// Session
// ============================================================================

/// A query session with scoped variables.
///
/// Sessions are the primary scope for reads and the factory for transactions.
/// Create via `db.session()`.
#[pyclass]
pub struct Session {
    pub(crate) inner: ::uni_db::Session,
}

#[pymethods]
impl Session {
    /// Set a session-scoped parameter.
    fn set<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: String,
        value: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        let val = convert::py_object_to_value(py, &value)?;
        slf.inner.set(key, val);
        Ok(slf)
    }

    /// Get a session-scoped parameter.
    fn get(&self, py: Python, key: &str) -> PyResult<Option<Py<PyAny>>> {
        match self.inner.get(key) {
            Some(v) => {
                let py_val = convert::value_to_py(py, v)?;
                Ok(Some(py_val))
            }
            None => Ok(None),
        }
    }

    /// Execute a read query within this session.
    ///
    /// Returns a `QueryResult` with `.rows`, `.metrics`, `.warnings`, `.columns`.
    /// The result also implements the sequence protocol (`for row in result` works).
    #[pyo3(signature = (cypher, params=None))]
    fn query(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<crate::types::PyQueryResult> {
        let result = if let Some(p) = params {
            let mut builder = self.inner.query_with(cypher);
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                builder = builder.param(k, val);
            }
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(builder.fetch_all())
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(self.inner.query(cypher))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        };
        convert::query_result_to_py_class(py, result)
    }

    /// Create a new transaction for multi-statement writes.
    fn tx(&self) -> PyResult<super::sync_api::Transaction> {
        let tx = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.tx())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(super::sync_api::Transaction { inner: Some(tx) })
    }

    /// Get the session ID.
    fn id(&self) -> &str {
        self.inner.id()
    }

    /// Add a session hook (Python object with optional before_query/after_query/before_commit/after_commit methods).
    fn add_hook(&mut self, hook: Py<PyAny>) {
        self.inner.add_hook(PySessionHook { py_obj: hook });
    }

    /// Get session capabilities.
    fn capabilities(&self) -> crate::types::PySessionCapabilities {
        let caps = self.inner.capabilities();
        let write_lease = caps.write_lease.map(|wl| format!("{:?}", wl));
        crate::types::PySessionCapabilities {
            can_write: caps.can_write,
            can_pin: caps.can_pin,
            isolation: caps.isolation.to_string(),
            has_notifications: caps.has_notifications,
            write_lease,
        }
    }

    /// Evaluate a Locy program within this session.
    #[pyo3(signature = (program, params=None))]
    fn locy(
        &self,
        py: Python,
        program: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<crate::types::PyLocyResult> {
        let result = if let Some(p) = params {
            let mut builder = self.inner.locy_with(program);
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                builder = builder.param(&k, val);
            }
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(builder.run())
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(self.inner.locy(program))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        };
        convert::locy_result_to_py_class(py, result)
    }

    /// Register Locy rules for reuse across evaluations in this session.
    fn register_rules(&self, program: &str) -> PyResult<()> {
        self.inner
            .register_rules(program)
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Prepare a Cypher query for repeated execution.
    fn prepare(&self, cypher: &str) -> PyResult<crate::types::PyPreparedQuery> {
        let prepared = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.prepare(cypher))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(crate::types::PyPreparedQuery {
            inner: std::sync::Mutex::new(prepared),
        })
    }

    /// Explain a query plan without executing.
    ///
    /// Returns a typed `ExplainOutput` with `.plan_text`, `.warnings`, `.cost_estimates`,
    /// `.index_usage`, `.suggestions`.
    fn explain(&self, py: Python, cypher: &str) -> PyResult<crate::types::PyExplainOutput> {
        let output = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.explain(cypher))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::explain_output_to_py_class(py, output)
    }

    /// Explain a Locy program's evaluation plan.
    ///
    /// Returns a typed `LocyExplainOutput`.
    fn explain_locy(
        &self,
        _py: Python,
        program: &str,
    ) -> PyResult<crate::types::PyLocyExplainOutput> {
        let result = self
            .inner
            .explain_locy(program)
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(convert::locy_explain_to_py_class(result))
    }

    /// Profile a query with operator-level statistics.
    ///
    /// Returns `(QueryResult, ProfileOutput)`.
    fn profile(
        &self,
        py: Python,
        cypher: &str,
    ) -> PyResult<(crate::types::PyQueryResult, crate::types::PyProfileOutput)> {
        let (results, profile) = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.profile(cypher))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        let query_result = convert::query_result_to_py_class(py, results)?;
        let profile_output = convert::profile_output_to_py_class(py, profile)?;
        Ok((query_result, profile_output))
    }

    /// Clear all registered Locy rules.
    fn clear_rules(&self) {
        self.inner.clear_rules();
    }

    /// Compile a Locy program without executing it.
    fn compile_locy(&self, program: &str) -> PyResult<crate::types::PyCompiledProgram> {
        let compiled = self
            .inner
            .compile_locy(program)
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(crate::types::PyCompiledProgram { inner: compiled })
    }

    /// Get session metrics.
    fn metrics(&self) -> crate::types::PySessionMetrics {
        let m = self.inner.metrics();
        crate::types::PySessionMetrics {
            session_id: m.session_id,
            active_since_secs: m.active_since.elapsed().as_secs_f64(),
            queries_executed: m.queries_executed,
            locy_evaluations: m.locy_evaluations,
            total_query_time_secs: m.total_query_time.as_secs_f64(),
            transactions_committed: m.transactions_committed,
            transactions_rolled_back: m.transactions_rolled_back,
            total_rows_returned: m.total_rows_returned,
            total_rows_scanned: m.total_rows_scanned,
            plan_cache_hits: m.plan_cache_hits,
            plan_cache_misses: m.plan_cache_misses,
            plan_cache_size: m.plan_cache_size,
        }
    }

    /// Set multiple session-scoped parameters at once.
    fn set_all(&mut self, py: Python, params: HashMap<String, Py<PyAny>>) -> PyResult<()> {
        for (k, v) in params {
            let val = convert::py_object_to_value(py, &v)?;
            self.inner.set(k, val);
        }
        Ok(())
    }

    /// Register a user-defined function callable from Cypher/Locy.
    fn register_function(&self, py: Python, name: &str, func: Py<PyAny>) -> PyResult<()> {
        // Wrap the Python callable in a Send+Sync struct.
        struct PyUdfWrapper {
            py_obj: Py<PyAny>,
        }
        // Safety: Py<PyAny> is reference-counted and GIL-independent.
        // All access goes through Python::attach.
        unsafe impl Send for PyUdfWrapper {}
        unsafe impl Sync for PyUdfWrapper {}

        let wrapper = PyUdfWrapper {
            py_obj: func.clone_ref(py),
        };
        self.inner
            .register_function(name, move |args: &[::uni_db::Value]| {
                Python::attach(|py| {
                    let py_args: Vec<Py<PyAny>> = args
                        .iter()
                        .map(|v| convert::value_to_py(py, v))
                        .collect::<PyResult<Vec<_>>>()
                        .map_err(|e| {
                            uni_common::UniError::Internal(anyhow::anyhow!(
                                "UDF arg conversion: {}",
                                e
                            ))
                        })?;
                    let py_list = pyo3::types::PyList::new(py, &py_args).map_err(|e| {
                        uni_common::UniError::Internal(anyhow::anyhow!("UDF list creation: {}", e))
                    })?;
                    let result = wrapper.py_obj.call1(py, (py_list,)).map_err(|e| {
                        uni_common::UniError::Internal(anyhow::anyhow!("UDF call: {}", e))
                    })?;
                    convert::py_object_to_value(py, &result).map_err(|e| {
                        uni_common::UniError::Internal(anyhow::anyhow!(
                            "UDF result conversion: {}",
                            e
                        ))
                    })
                })
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Get a cancellation token for this session.
    fn cancellation_token(&self) -> crate::types::PyCancellationToken {
        crate::types::PyCancellationToken {
            inner: self.inner.cancellation_token(),
        }
    }

    /// Cancel in-progress operations on this session.
    fn cancel(&mut self) {
        self.inner.cancel();
    }

    /// Pin this session to a specific snapshot version.
    fn pin_to_version(&mut self, snapshot_id: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.pin_to_version(snapshot_id))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Pin this session to a specific timestamp (seconds since epoch).
    fn pin_to_timestamp(&mut self, epoch_secs: f64) -> PyResult<()> {
        let ts = chrono::DateTime::from_timestamp(
            epoch_secs as i64,
            ((epoch_secs.fract()) * 1_000_000_000.0) as u32,
        )
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid timestamp"))?;
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.pin_to_timestamp(ts))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Refresh session to latest database version (unpins if pinned).
    fn refresh(&mut self) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.refresh())
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Create a query builder for parameterized queries.
    fn query_with(slf: Py<Self>, cypher: &str) -> SessionQueryBuilder {
        SessionQueryBuilder {
            session: slf,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_memory: None,
            cancellation_token: None,
        }
    }

    /// Create a builder for parameterized Locy evaluation.
    fn locy_with(slf: Py<Self>, program: &str) -> SessionLocyBuilder {
        SessionLocyBuilder {
            session: slf,
            program: program.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_iterations: None,
            locy_config: None,
            cancellation_token: None,
        }
    }

    /// Create a builder for parameterized profile.
    fn profile_with(slf: Py<Self>, cypher: &str) -> SessionProfileBuilder {
        SessionProfileBuilder {
            session: slf,
            cypher: cypher.to_string(),
            params: HashMap::new(),
        }
    }

    /// Create a cursor-based query for streaming large result sets.
    #[pyo3(signature = (cypher, params=None))]
    fn query_cursor(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<super::sync_api::QueryCursor> {
        let cursor = if let Some(p) = params {
            let mut builder = self.inner.query_with(cypher);
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                builder = builder.param(&k, val);
            }
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(builder.cursor())
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(self.inner.query_cursor(cypher))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        };
        let columns = cursor.columns().to_vec();
        Ok(super::sync_api::QueryCursor {
            cursor: std::sync::Mutex::new(Some(cursor)),
            buffer: std::sync::Mutex::new(std::collections::VecDeque::new()),
            columns,
        })
    }

    /// Check if this session is pinned to a specific version.
    fn is_pinned(&self) -> bool {
        self.inner.is_pinned()
    }

    /// Prepare a Locy program for repeated execution.
    fn prepare_locy(&self, program: &str) -> PyResult<crate::types::PyPreparedLocy> {
        let prepared = self
            .inner
            .prepare_locy(program)
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(crate::types::PyPreparedLocy {
            inner: std::sync::Mutex::new(prepared),
        })
    }

    /// Watch for commit notifications (returns a synchronous CommitStream).
    fn watch(&self) -> crate::types::PyCommitStream {
        let stream = self.inner.watch();
        crate::types::PyCommitStream {
            inner: std::sync::Mutex::new(Some(stream)),
        }
    }

    /// Create a WatchBuilder for configuring commit notification filters.
    fn watch_with(&self) -> crate::types::PyWatchBuilder {
        let builder = self.inner.watch_with();
        crate::types::PyWatchBuilder {
            inner: Some(builder),
        }
    }

    /// Create a transaction builder for configuring transaction options.
    fn tx_with(slf: Py<Self>) -> PyTransactionBuilder {
        PyTransactionBuilder {
            session: slf,
            timeout_secs: None,
            isolation_level: None,
        }
    }

    /// Context manager enter.
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Context manager exit — cancel in-progress operations.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> bool {
        self.inner.cancel();
        false
    }
}

// ============================================================================
// Session builders (sync)
// ============================================================================

/// Builder for parameterized queries on a Session.
#[pyclass(name = "SessionQueryBuilder")]
pub struct SessionQueryBuilder {
    pub(crate) session: Py<Session>,
    pub(crate) cypher: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
    pub(crate) max_memory: Option<usize>,
    pub(crate) cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl SessionQueryBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Bind multiple parameters.
    fn params(
        mut slf: PyRefMut<'_, Self>,
        params: HashMap<String, Py<PyAny>>,
    ) -> PyRefMut<'_, Self> {
        slf.params.extend(params);
        slf
    }

    /// Set query timeout in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Set maximum memory in bytes.
    fn max_memory(mut slf: PyRefMut<'_, Self>, bytes: usize) -> PyRefMut<'_, Self> {
        slf.max_memory = Some(bytes);
        slf
    }

    /// Attach a cancellation token to this query.
    fn cancellation_token(
        mut slf: PyRefMut<'_, Self>,
        token: crate::types::PyCancellationToken,
    ) -> PyRefMut<'_, Self> {
        slf.cancellation_token = Some(token);
        slf
    }

    /// Fetch all results as a `QueryResult`.
    fn fetch_all(&self, py: Python) -> PyResult<crate::types::PyQueryResult> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.query_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(m) = self.max_memory {
            builder = builder.max_memory(m);
        }
        if let Some(ref ct) = self.cancellation_token {
            builder = builder.cancellation_token(ct.inner.clone());
        }
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.fetch_all())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::query_result_to_py_class(py, result)
    }

    /// Fetch a single row or None.
    fn fetch_one(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let result = self.fetch_all(py)?;
        Ok(result.rows.into_iter().next())
    }

    /// Open a streaming cursor.
    fn cursor(&self, py: Python) -> PyResult<crate::sync_api::QueryCursor> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.query_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(m) = self.max_memory {
            builder = builder.max_memory(m);
        }
        if let Some(ref ct) = self.cancellation_token {
            builder = builder.cancellation_token(ct.inner.clone());
        }
        let cursor = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.cursor())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        let columns = cursor.columns().to_vec();
        Ok(crate::sync_api::QueryCursor {
            cursor: std::sync::Mutex::new(Some(cursor)),
            buffer: std::sync::Mutex::new(VecDeque::new()),
            columns,
        })
    }
}

/// Builder for Locy evaluation on a Session.
#[pyclass(name = "SessionLocyBuilder")]
pub struct SessionLocyBuilder {
    pub(crate) session: Py<Session>,
    pub(crate) program: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
    pub(crate) max_iterations: Option<usize>,
    pub(crate) locy_config: Option<::uni_locy::LocyConfig>,
    pub(crate) cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl SessionLocyBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Bind multiple parameters.
    fn params(
        mut slf: PyRefMut<'_, Self>,
        params: HashMap<String, Py<PyAny>>,
    ) -> PyRefMut<'_, Self> {
        slf.params.extend(params);
        slf
    }

    /// Set evaluation timeout in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Set maximum fixpoint iterations.
    fn max_iterations(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.max_iterations = Some(n);
        slf
    }

    /// Apply a full Locy configuration (LocyConfig object or dict).
    fn with_config<'py>(
        mut slf: PyRefMut<'py, Self>,
        config: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let locy_config = if let Ok(cfg) = config.extract::<crate::types::PyLocyConfig>() {
            cfg.inner
        } else {
            let dict: HashMap<String, Py<PyAny>> = config.extract()?;
            crate::convert::extract_locy_config(config.py(), dict)?
        };
        slf.locy_config = Some(locy_config);
        Ok(slf)
    }

    /// Attach a cancellation token to this evaluation.
    fn cancellation_token(
        mut slf: PyRefMut<'_, Self>,
        token: crate::types::PyCancellationToken,
    ) -> PyRefMut<'_, Self> {
        slf.cancellation_token = Some(token);
        slf
    }

    /// Execute the Locy evaluation.
    fn run(&self, py: Python) -> PyResult<crate::types::PyLocyResult> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.locy_with(&self.program);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(n) = self.max_iterations {
            builder = builder.max_iterations(n);
        }
        if let Some(ref config) = self.locy_config {
            builder = builder.with_config(config.clone());
        }
        if let Some(ref ct) = self.cancellation_token {
            builder = builder.cancellation_token(ct.inner.clone());
        }
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.run())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::locy_result_to_py_class(py, result)
    }
}

/// Builder for profiling on a Session.
#[pyclass(name = "ProfileBuilder")]
pub struct SessionProfileBuilder {
    pub(crate) session: Py<Session>,
    pub(crate) cypher: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
}

#[pymethods]
impl SessionProfileBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Execute the profile, returning `(QueryResult, ProfileOutput)`.
    fn run(
        &self,
        py: Python,
    ) -> PyResult<(crate::types::PyQueryResult, crate::types::PyProfileOutput)> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.profile_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        let (results, profile) = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.run())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        let query_result = convert::query_result_to_py_class(py, results)?;
        let profile_output = convert::profile_output_to_py_class(py, profile)?;
        Ok((query_result, profile_output))
    }
}

/// Builder for transaction configuration.
#[pyclass(name = "TransactionBuilder")]
pub struct PyTransactionBuilder {
    pub(crate) session: Py<Session>,
    pub(crate) timeout_secs: Option<f64>,
    pub(crate) isolation_level: Option<String>,
}

#[pymethods]
impl PyTransactionBuilder {
    /// Set transaction timeout in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Set isolation level (currently only "serialized" is supported).
    fn isolation(mut slf: PyRefMut<'_, Self>, level: String) -> PyRefMut<'_, Self> {
        slf.isolation_level = Some(level);
        slf
    }

    /// Start the transaction.
    fn start(&self, py: Python) -> PyResult<super::sync_api::Transaction> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.tx_with();
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(ref level) = self.isolation_level {
            match level.to_lowercase().as_str() {
                "serialized" => {
                    builder = builder.isolation(::uni_db::IsolationLevel::Serialized);
                }
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Unknown isolation level: {}",
                        level
                    )));
                }
            }
        }
        let tx = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.start())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(super::sync_api::Transaction { inner: Some(tx) })
    }
}

// ============================================================================
// Transaction builders (sync)
// ============================================================================

/// Builder for parameterized queries on a Transaction.
#[pyclass(name = "TxQueryBuilder")]
pub struct PyTxQueryBuilder {
    pub(crate) tx: Py<super::sync_api::Transaction>,
    pub(crate) cypher: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
}

#[pymethods]
impl PyTxQueryBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Set query timeout in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Fetch all results as a `QueryResult`.
    fn fetch_all(&self, py: Python) -> PyResult<crate::types::PyQueryResult> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let mut builder = tx.query_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.fetch_all())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::query_result_to_py_class(py, result)
    }

    /// Fetch a single row or None.
    fn fetch_one(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let result = self.fetch_all(py)?;
        Ok(result.rows.into_iter().next())
    }

    /// Execute as a mutation and return ExecuteResult.
    fn execute(&self, py: Python) -> PyResult<crate::types::PyExecuteResult> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let mut builder = tx.execute_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.run())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::execute_result_to_py(py, result)
    }

    /// Open a streaming cursor.
    fn cursor(&self, py: Python) -> PyResult<crate::sync_api::QueryCursor> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let mut builder = tx.query_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        let cursor = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.cursor())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        let columns = cursor.columns().to_vec();
        Ok(crate::sync_api::QueryCursor {
            cursor: std::sync::Mutex::new(Some(cursor)),
            buffer: std::sync::Mutex::new(VecDeque::new()),
            columns,
        })
    }
}

/// Builder for parameterized mutations on a Transaction.
#[pyclass(name = "TxExecuteBuilder")]
pub struct PyTxExecuteBuilder {
    pub(crate) tx: Py<super::sync_api::Transaction>,
    pub(crate) cypher: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
}

#[pymethods]
impl PyTxExecuteBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Set execution timeout.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Execute the mutation.
    fn run(&self, py: Python) -> PyResult<crate::types::PyExecuteResult> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let mut builder = tx.execute_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.run())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::execute_result_to_py(py, result)
    }
}

/// Builder for Locy evaluation on a Transaction.
#[pyclass(name = "TxLocyBuilder")]
pub struct PyTxLocyBuilder {
    pub(crate) tx: Py<super::sync_api::Transaction>,
    pub(crate) program: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
    pub(crate) max_iterations: Option<usize>,
    pub(crate) locy_config: Option<::uni_locy::LocyConfig>,
    pub(crate) cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl PyTxLocyBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Set evaluation timeout.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Set maximum fixpoint iterations.
    fn max_iterations(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.max_iterations = Some(n);
        slf
    }

    /// Apply a full Locy configuration (LocyConfig object or dict).
    fn with_config<'py>(
        mut slf: PyRefMut<'py, Self>,
        config: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let locy_config = if let Ok(cfg) = config.extract::<crate::types::PyLocyConfig>() {
            cfg.inner
        } else {
            let dict: HashMap<String, Py<PyAny>> = config.extract()?;
            crate::convert::extract_locy_config(config.py(), dict)?
        };
        slf.locy_config = Some(locy_config);
        Ok(slf)
    }

    /// Attach a cancellation token to this evaluation.
    fn cancellation_token(
        mut slf: PyRefMut<'_, Self>,
        token: crate::types::PyCancellationToken,
    ) -> PyRefMut<'_, Self> {
        slf.cancellation_token = Some(token);
        slf
    }

    /// Execute the Locy evaluation.
    fn run(&self, py: Python) -> PyResult<crate::types::PyLocyResult> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let mut builder = tx.locy_with(&self.program);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(n) = self.max_iterations {
            builder = builder.max_iterations(n);
        }
        if let Some(ref config) = self.locy_config {
            builder = builder.with_config(config.clone());
        }
        if let Some(ref ct) = self.cancellation_token {
            builder = builder.cancellation_token(ct.inner.clone());
        }
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.run())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::locy_result_to_py_class(py, result)
    }
}

/// Builder for applying a DerivedFactSet with options.
#[pyclass(name = "ApplyBuilder")]
pub struct PyApplyBuilder {
    pub(crate) tx: Py<super::sync_api::Transaction>,
    pub(crate) derived: Option<uni_locy::DerivedFactSet>,
    pub(crate) require_fresh: bool,
    pub(crate) max_version_gap: Option<u64>,
}

#[pymethods]
impl PyApplyBuilder {
    /// Require fresh version (fail if version gap is non-zero).
    fn require_fresh(mut slf: PyRefMut<'_, Self>, require: bool) -> PyRefMut<'_, Self> {
        slf.require_fresh = require;
        slf
    }

    /// Set maximum allowed version gap.
    fn max_version_gap(mut slf: PyRefMut<'_, Self>, gap: u64) -> PyRefMut<'_, Self> {
        slf.max_version_gap = Some(gap);
        slf
    }

    /// Execute the apply.
    fn run(&mut self, py: Python) -> PyResult<crate::types::PyApplyResult> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let dfs = self.derived.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("DerivedFactSet already consumed")
        })?;
        let mut builder = tx.apply_with(dfs);
        if self.require_fresh {
            builder = builder.require_fresh();
        }
        if let Some(gap) = self.max_version_gap {
            builder = builder.max_version_gap(gap);
        }
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.run())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(crate::types::PyApplyResult {
            facts_applied: result.facts_applied,
            version_gap: result.version_gap,
        })
    }
}

// ============================================================================
// Python hook bridge
// ============================================================================

/// Bridge from Python hook objects to Rust SessionHook trait.
pub(crate) struct PySessionHook {
    pub(crate) py_obj: Py<PyAny>,
}

// Safety: The Py<PyAny> handle is GIL-independent (reference-counted).
// All access to the Python object goes through `Python::attach`.
unsafe impl Send for PySessionHook {}
unsafe impl Sync for PySessionHook {}

impl ::uni_db::SessionHook for PySessionHook {
    fn before_query(&self, ctx: &::uni_db::HookContext) -> uni_common::Result<()> {
        Python::attach(|py| {
            if let Ok(method) = self.py_obj.getattr(py, "before_query") {
                let py_ctx = pyo3::types::PyDict::new(py);
                py_ctx.set_item("session_id", &ctx.session_id).ok();
                py_ctx.set_item("query_text", &ctx.query_text).ok();
                py_ctx
                    .set_item("query_type", format!("{:?}", ctx.query_type))
                    .ok();
                // Enrich: pass params dict
                let params_dict = pyo3::types::PyDict::new(py);
                for (k, v) in &ctx.params {
                    if let Ok(py_v) = convert::value_to_py(py, v) {
                        params_dict.set_item(k, py_v).ok();
                    }
                }
                py_ctx.set_item("params", params_dict).ok();
                if let Err(e) = method.call1(py, (py_ctx,)) {
                    return Err(uni_common::UniError::HookRejected {
                        message: e.to_string(),
                    });
                }
            }
            Ok(())
        })
    }

    fn after_query(&self, ctx: &::uni_db::HookContext, metrics: &::uni_db::QueryMetrics) {
        Python::attach(|py| {
            if let Ok(method) = self.py_obj.getattr(py, "after_query") {
                let py_ctx = pyo3::types::PyDict::new(py);
                py_ctx.set_item("session_id", &ctx.session_id).ok();
                py_ctx.set_item("query_text", &ctx.query_text).ok();
                // Enrich: pass metrics as second argument (with fallback to 1-arg)
                if let Ok(py_metrics) = convert::query_metrics_to_py_class(py, metrics) {
                    if method
                        .call1(py, (py_ctx.as_any(), py_metrics.bind(py).as_any()))
                        .is_err()
                    {
                        // Fallback: try 1-arg call for backward compat
                        let py_ctx2 = pyo3::types::PyDict::new(py);
                        py_ctx2.set_item("session_id", &ctx.session_id).ok();
                        py_ctx2.set_item("query_text", &ctx.query_text).ok();
                        let _ = method.call1(py, (py_ctx2,));
                    }
                } else {
                    let _ = method.call1(py, (py_ctx,));
                }
            }
        });
    }

    fn before_commit(&self, ctx: &::uni_db::CommitHookContext) -> uni_common::Result<()> {
        Python::attach(|py| {
            if let Ok(method) = self.py_obj.getattr(py, "before_commit") {
                let py_ctx = pyo3::types::PyDict::new(py);
                py_ctx.set_item("session_id", &ctx.session_id).ok();
                py_ctx.set_item("tx_id", &ctx.tx_id).ok();
                py_ctx.set_item("mutation_count", ctx.mutation_count).ok();
                if let Err(e) = method.call1(py, (py_ctx,)) {
                    return Err(uni_common::UniError::HookRejected {
                        message: e.to_string(),
                    });
                }
            }
            Ok(())
        })
    }

    fn after_commit(&self, ctx: &::uni_db::CommitHookContext, result: &::uni_db::CommitResult) {
        Python::attach(|py| {
            if let Ok(method) = self.py_obj.getattr(py, "after_commit") {
                let py_ctx = pyo3::types::PyDict::new(py);
                py_ctx.set_item("session_id", &ctx.session_id).ok();
                py_ctx.set_item("tx_id", &ctx.tx_id).ok();
                py_ctx.set_item("mutation_count", ctx.mutation_count).ok();
                // Enrich: pass commit result as second argument (with fallback)
                let py_result = crate::types::PyCommitResult {
                    mutations_committed: result.mutations_committed,
                    rules_promoted: result.rules_promoted,
                    version: result.version,
                    started_at_version: result.started_at_version,
                    wal_lsn: result.wal_lsn,
                    duration_secs: result.duration.as_secs_f64(),
                    rule_promotion_errors: result
                        .rule_promotion_errors
                        .iter()
                        .map(|e| crate::types::PyRulePromotionError {
                            rule_text: e.rule_text.clone(),
                            error: e.error.clone(),
                        })
                        .collect(),
                };
                if let Ok(bound) = Py::new(py, py_result) {
                    if method
                        .call1(py, (py_ctx.as_any(), bound.bind(py).as_any()))
                        .is_err()
                    {
                        let py_ctx2 = pyo3::types::PyDict::new(py);
                        py_ctx2.set_item("session_id", &ctx.session_id).ok();
                        py_ctx2.set_item("tx_id", &ctx.tx_id).ok();
                        py_ctx2.set_item("mutation_count", ctx.mutation_count).ok();
                        let _ = method.call1(py, (py_ctx2,));
                    }
                } else {
                    let _ = method.call1(py, (py_ctx,));
                }
            }
        });
    }
}

// ============================================================================
// StreamingAppender
// ============================================================================

/// A streaming appender for single-label data loading.
///
/// Rows are buffered and flushed in batches. Use as a context manager:
/// ```python
/// with session.appender("Person") as app:
///     app.append({"name": "Alice", "age": 30})
///     stats = app.finish()
/// ```
#[pyclass]
pub struct StreamingAppender {
    pub(crate) inner: std::sync::Mutex<Option<::uni_db::StreamingAppender>>,
}

#[pymethods]
impl StreamingAppender {
    /// Append a single row of properties.
    fn append(&self, py: Python, properties: HashMap<String, Py<PyAny>>) -> PyResult<()> {
        let mut guard = self.inner.lock().unwrap();
        let appender = guard.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Appender already finished")
        })?;
        let mut rust_props = HashMap::new();
        for (k, v) in properties {
            rust_props.insert(k, convert::py_object_to_value(py, &v)?);
        }
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(appender.append(rust_props))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Flush remaining rows and commit.
    fn finish(&self) -> PyResult<BulkStats> {
        let mut guard = self.inner.lock().unwrap();
        let appender = guard.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Appender already finished")
        })?;
        let stats = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(appender.finish())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(BulkStats {
            vertices_inserted: stats.vertices_inserted,
            edges_inserted: stats.edges_inserted,
            indexes_rebuilt: stats.indexes_rebuilt,
            duration_secs: stats.duration.as_secs_f64(),
            index_build_duration_secs: stats.index_build_duration.as_secs_f64(),
            index_task_ids: stats.index_task_ids.clone(),
            indexes_pending: stats.indexes_pending,
        })
    }

    /// Abort without committing.
    fn abort(&self) -> PyResult<()> {
        let mut guard = self.inner.lock().unwrap();
        if let Some(appender) = guard.take() {
            appender.abort();
        }
        Ok(())
    }

    /// Write an Arrow RecordBatch of rows.
    ///
    /// Accepts a PyArrow RecordBatch. Uses the Arrow PyCapsule C Data Interface
    /// (`__arrow_c_array__`) for zero-copy transfer.
    fn write_batch(&self, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        // Use Arrow PyCapsule interface (__arrow_c_array__) for zero-copy
        let capsule_tuple = batch.call_method0("__arrow_c_array__").map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Expected a PyArrow RecordBatch with __arrow_c_array__ support: {}",
                e
            ))
        })?;
        let schema_capsule = capsule_tuple.get_item(0)?;
        let array_capsule = capsule_tuple.get_item(1)?;

        // Extract raw pointers from PyCapsules and convert via Arrow FFI
        let (ffi_schema, ffi_array) = unsafe {
            let schema_ptr =
                pyo3::ffi::PyCapsule_GetPointer(schema_capsule.as_ptr(), c"arrow_schema".as_ptr())
                    as *mut arrow_array::ffi::FFI_ArrowSchema;
            let array_ptr =
                pyo3::ffi::PyCapsule_GetPointer(array_capsule.as_ptr(), c"arrow_array".as_ptr())
                    as *mut arrow_array::ffi::FFI_ArrowArray;
            // Move out of the capsule pointers (Arrow C Data Interface: consumer owns the data)
            (std::ptr::read(schema_ptr), std::ptr::read(array_ptr))
        };

        let array_data = unsafe {
            arrow_array::ffi::from_ffi(ffi_array, &ffi_schema).map_err(
                |e: arrow_schema::ArrowError| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
                },
            )?
        };
        let struct_array = arrow_array::StructArray::from(array_data);
        let schema = arrow_schema::Schema::new(
            struct_array
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>(),
        );
        let record_batch = arrow_array::RecordBatch::try_new(
            std::sync::Arc::new(schema),
            struct_array.columns().to_vec(),
        )
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        let mut guard = self.inner.lock().unwrap();
        let appender = guard.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Appender already finished")
        })?;
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(appender.write_batch(&record_batch))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Number of rows currently buffered.
    fn buffered_count(&self) -> PyResult<usize> {
        let guard = self.inner.lock().unwrap();
        guard.as_ref().map(|a| a.buffered_count()).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Appender already finished")
        })
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        let guard = self.inner.lock().unwrap();
        if guard.is_some() {
            drop(guard);
            self.abort()?;
        }
        Ok(false)
    }
}

// ============================================================================
// SessionTemplateBuilder
// ============================================================================

/// Builder for creating pre-configured session templates.
#[pyclass]
pub struct SessionTemplateBuilder {
    pub(crate) inner: Option<::uni_db::SessionTemplateBuilder>,
}

#[pymethods]
impl SessionTemplateBuilder {
    /// Bind a parameter that all sessions created from this template will inherit.
    fn param<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: String,
        value: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        let val = convert::py_object_to_value(py, &value)?;
        let builder = slf.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Builder already consumed")
        })?;
        slf.inner = Some(builder.param(key, val));
        Ok(slf)
    }

    /// Pre-compile Locy rules.
    fn rules<'py>(mut slf: PyRefMut<'py, Self>, program: &str) -> PyResult<PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Builder already consumed")
        })?;
        slf.inner = Some(
            builder
                .rules(program)
                .map_err(crate::exceptions::uni_error_to_pyerr)?,
        );
        Ok(slf)
    }

    /// Attach a hook.
    fn hook<'py>(mut slf: PyRefMut<'py, Self>, hook: Py<PyAny>) -> PyResult<PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Builder already consumed")
        })?;
        slf.inner = Some(builder.hook(PySessionHook { py_obj: hook }));
        Ok(slf)
    }

    /// Set default query timeout in seconds.
    fn query_timeout<'py>(
        mut slf: PyRefMut<'py, Self>,
        seconds: f64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Builder already consumed")
        })?;
        slf.inner = Some(builder.query_timeout(std::time::Duration::from_secs_f64(seconds)));
        Ok(slf)
    }

    /// Set default transaction timeout in seconds.
    fn transaction_timeout<'py>(
        mut slf: PyRefMut<'py, Self>,
        seconds: f64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Builder already consumed")
        })?;
        slf.inner = Some(builder.transaction_timeout(std::time::Duration::from_secs_f64(seconds)));
        Ok(slf)
    }

    /// Build the session template.
    fn build(&mut self) -> PyResult<SessionTemplate> {
        let builder = self.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Builder already consumed")
        })?;
        let template = builder
            .build()
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(SessionTemplate {
            inner: std::sync::Arc::new(template),
        })
    }
}

// ============================================================================
// SessionTemplate
// ============================================================================

/// A pre-configured session factory.
///
/// Create sessions cheaply from pre-compiled templates:
/// ```python
/// template = db.session_template().param("tenant", 42).rules("...").build()
/// session = template.create()
/// ```
#[pyclass]
pub struct SessionTemplate {
    inner: std::sync::Arc<::uni_db::SessionTemplate>,
}

#[pymethods]
impl SessionTemplate {
    /// Create a new session from this template (cheap, no I/O).
    fn create(&self) -> Session {
        Session {
            inner: self.inner.create(),
        }
    }
}

// ============================================================================
// BulkWriter (wrapping real Rust BulkWriter)
// ============================================================================

/// Bulk writer for high-throughput data ingestion.
///
/// Wraps the real Rust `BulkWriter` via `Mutex<Option<T>>` ownership pattern.
#[pyclass]
pub struct BulkWriter {
    pub(crate) inner: std::sync::Mutex<Option<::uni_db::api::bulk::BulkWriter>>,
}

impl BulkWriter {
    fn with_writer<F, R>(&self, op: &str, f: F) -> PyResult<R>
    where
        F: FnOnce(&mut ::uni_db::api::bulk::BulkWriter) -> PyResult<R>,
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let writer = guard.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "BulkWriter already completed ({})",
                op
            ))
        })?;
        f(writer)
    }
}

#[pymethods]
impl BulkWriter {
    /// Insert vertices in bulk, returning allocated VIDs.
    fn insert_vertices(
        &self,
        py: Python,
        label: &str,
        vertices: Vec<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Vec<u64>> {
        let mut rust_props: Vec<HashMap<String, ::uni_db::Value>> =
            Vec::with_capacity(vertices.len());
        for v in vertices {
            let mut map = HashMap::new();
            for (k, val) in v {
                map.insert(k, convert::py_object_to_value(py, &val)?);
            }
            rust_props.push(map);
        }
        self.with_writer("insert_vertices", |writer| {
            let vids = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(writer.insert_vertices(label, rust_props))
                .map_err(crate::exceptions::anyhow_to_pyerr)?;
            Ok(vids.into_iter().map(|v| v.as_u64()).collect())
        })
    }

    /// Insert edges in bulk.
    fn insert_edges(
        &self,
        py: Python,
        edge_type: &str,
        edges: Vec<(u64, u64, HashMap<String, Py<PyAny>>)>,
    ) -> PyResult<()> {
        let mut rust_edges = Vec::with_capacity(edges.len());
        for (src, dst, props) in edges {
            let mut map = HashMap::new();
            for (k, v) in props {
                map.insert(k, convert::py_object_to_value(py, &v)?);
            }
            rust_edges.push(::uni_db::api::bulk::EdgeData::new(
                ::uni_db::Vid::from(src),
                ::uni_db::Vid::from(dst),
                map,
            ));
        }
        self.with_writer("insert_edges", |writer| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(writer.insert_edges(edge_type, rust_edges))
                .map_err(crate::exceptions::anyhow_to_pyerr)?;
            Ok(())
        })
    }

    /// Get current bulk load statistics.
    fn stats(&self) -> PyResult<BulkStats> {
        self.with_writer("stats", |writer| {
            let s = writer.stats();
            Ok(BulkStats {
                vertices_inserted: s.vertices_inserted,
                edges_inserted: s.edges_inserted,
                indexes_rebuilt: s.indexes_rebuilt,
                duration_secs: s.duration.as_secs_f64(),
                index_build_duration_secs: s.index_build_duration.as_secs_f64(),
                index_task_ids: s.index_task_ids.clone(),
                indexes_pending: s.indexes_pending,
            })
        })
    }

    /// Get labels that have been written to.
    fn touched_labels(&self) -> PyResult<Vec<String>> {
        self.with_writer("touched_labels", |writer| Ok(writer.touched_labels()))
    }

    /// Get edge types that have been written to.
    fn touched_edge_types(&self) -> PyResult<Vec<String>> {
        self.with_writer("touched_edge_types", |writer| {
            Ok(writer.touched_edge_types())
        })
    }

    /// Commit all pending data and rebuild indexes.
    fn commit(&self) -> PyResult<BulkStats> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let writer = guard.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("BulkWriter already completed")
        })?;
        let stats = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(writer.commit())
            .map_err(crate::exceptions::anyhow_to_pyerr)?;
        Ok(BulkStats {
            vertices_inserted: stats.vertices_inserted,
            edges_inserted: stats.edges_inserted,
            indexes_rebuilt: stats.indexes_rebuilt,
            duration_secs: stats.duration.as_secs_f64(),
            index_build_duration_secs: stats.index_build_duration.as_secs_f64(),
            index_task_ids: stats.index_task_ids.clone(),
            indexes_pending: stats.indexes_pending,
        })
    }

    /// Abort bulk loading and discard uncommitted changes.
    fn abort(&self) -> PyResult<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        if let Some(writer) = guard.take() {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(writer.abort())
                .map_err(crate::exceptions::anyhow_to_pyerr)?;
        }
        Ok(())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Auto-abort on exception if not committed.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        let guard = self.inner.lock().unwrap();
        if guard.is_some() {
            drop(guard);
            self.abort()?;
        }
        Ok(false)
    }
}
