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

/// Builder for creating and configuring a Database instance.
#[pyclass]
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

    /// Open the database in read-only mode (no writes allowed).
    fn read_only(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.read_only = true;
        slf
    }

    /// Build and return the Database instance.
    fn build(&self) -> PyResult<crate::sync_api::Database> {
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
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyIOError, _>)?;

        Ok(crate::sync_api::Database {
            inner: Arc::new(uni),
        })
    }
}

// ============================================================================
// QueryBuilder
// ============================================================================

/// Builder for constructing and executing parameterized queries.
#[pyclass]
pub struct QueryBuilder {
    pub(crate) inner: Arc<Uni>,
    pub(crate) cypher: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
    pub(crate) max_memory: Option<usize>,
}

#[pymethods]
impl QueryBuilder {
    /// Bind a parameter to the query.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Bind multiple parameters from a dictionary.
    fn params(
        mut slf: PyRefMut<'_, Self>,
        params: HashMap<String, Py<PyAny>>,
    ) -> PyRefMut<'_, Self> {
        slf.params.extend(params);
        slf
    }

    /// Set maximum execution time in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Set maximum memory for this query in bytes.
    fn max_memory(mut slf: PyRefMut<'_, Self>, bytes: usize) -> PyRefMut<'_, Self> {
        slf.max_memory = Some(bytes);
        slf
    }

    /// Open a streaming cursor for this query.
    fn cursor(&self, py: Python) -> PyResult<crate::sync_api::QueryCursor> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }
        let cursor = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::query_cursor_core(
                &self.inner,
                &self.cypher,
                rust_params,
                self.timeout_secs,
                self.max_memory,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        let columns = cursor.columns().to_vec();
        Ok(crate::sync_api::QueryCursor {
            cursor: std::sync::Mutex::new(Some(cursor)),
            buffer: std::sync::Mutex::new(VecDeque::new()),
            columns,
        })
    }

    /// Execute a mutation query and return affected row count.
    fn execute(&self, py: Python) -> PyResult<usize> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }

        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::execute_with_params_core(
                &self.inner,
                &self.cypher,
                rust_params,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(result)
    }

    /// Execute the query and fetch all results.
    fn fetch_all(&self, py: Python) -> PyResult<Vec<Py<PyAny>>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }

        let rows = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::query_builder_core(
                &self.inner,
                &self.cypher,
                rust_params,
                self.timeout_secs,
                self.max_memory,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        convert::rows_to_py(py, rows.into_rows())
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
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
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
    fn property(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        data_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let dt = core::parse_data_type(&data_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
        slf.properties.push((name, dt, false));
        Ok(slf)
    }

    /// Add a nullable property to this label.
    fn property_nullable(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        data_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let dt = core::parse_data_type(&data_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
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
    fn index(
        mut slf: PyRefMut<'_, Self>,
        property: String,
        index_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let label = slf.name.clone();
        let idx = core::create_index_definition(&label, &property, &index_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
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
    fn property(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        data_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let dt = core::parse_data_type(&data_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
        slf.properties.push((name, dt, false));
        Ok(slf)
    }

    /// Add a nullable property to this edge type.
    fn property_nullable(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        data_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let dt = core::parse_data_type(&data_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
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
// SessionBuilder and Session
// ============================================================================

/// Builder for creating query sessions with scoped variables.
#[pyclass]
#[derive(Clone)]
pub struct SessionBuilder {
    pub(crate) inner: Arc<Uni>,
    pub(crate) variables: HashMap<String, serde_json::Value>,
}

#[pymethods]
impl SessionBuilder {
    /// Set a session variable.
    fn set<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: String,
        value: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        let json_val = convert::py_object_to_json(py, &value)?;
        slf.variables.insert(key, json_val);
        Ok(slf)
    }

    /// Build the session.
    fn build(&self) -> PyResult<Session> {
        let mut rust_session = self.inner.session();
        for (k, v) in &self.variables {
            let val = ::uni_db::Value::from(v.clone());
            rust_session.set(k.clone(), val);
        }
        Ok(Session {
            inner: rust_session,
        })
    }
}

/// A query session with scoped variables.
///
/// Sessions are the primary scope for reads and the factory for transactions.
/// Create via `db.session()` or `db.session_builder().set(...).build()`.
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
    #[pyo3(signature = (cypher, params=None))]
    fn query(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let result = if let Some(p) = params {
            let mut builder = self.inner.query_with(cypher);
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                builder = builder.param(k, val);
            }
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(builder.fetch_all())
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(self.inner.query(cypher))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        };
        convert::rows_to_py(py, result.into_rows())
    }

    /// Execute a mutation query, returning an AutoCommitResult.
    #[pyo3(signature = (cypher, params=None))]
    fn execute(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<crate::types::PyAutoCommitResult> {
        let result = if let Some(p) = params {
            let mut builder = self.inner.execute_with(cypher);
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                builder = builder.param(&k, val);
            }
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(builder.run())
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(self.inner.execute(cypher))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        };
        convert::auto_commit_result_to_py(py, result)
    }

    /// Create a new transaction for multi-statement writes.
    fn tx(&self) -> PyResult<super::sync_api::Transaction> {
        let tx = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.tx())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
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

    /// Create a streaming appender for the given label.
    fn appender(&self, label: &str) -> PyResult<StreamingAppender> {
        let builder = self.inner.appender(label);
        let appender = builder
            .build()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(StreamingAppender {
            inner: std::sync::Mutex::new(Some(appender)),
        })
    }

    /// Get session capabilities.
    fn capabilities(&self) -> crate::types::PySessionCapabilities {
        let caps = self.inner.capabilities();
        crate::types::PySessionCapabilities {
            can_write: caps.can_write,
            can_pin: caps.can_pin,
            isolation: caps.isolation.to_string(),
            has_notifications: caps.has_notifications,
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
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(self.inner.locy(program))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        };
        convert::locy_result_to_py_class(py, result)
    }

    /// Register Locy rules for reuse across evaluations in this session.
    fn register_rules(&self, program: &str) -> PyResult<()> {
        self.inner
            .register_rules(program)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Prepare a Cypher query for repeated execution.
    fn prepare(&self, cypher: &str) -> PyResult<crate::types::PyPreparedQuery> {
        let prepared = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.prepare(cypher))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(crate::types::PyPreparedQuery {
            inner: std::sync::Mutex::new(prepared),
        })
    }

    /// Explain a query plan without executing.
    fn explain(&self, py: Python, cypher: &str) -> PyResult<Py<PyAny>> {
        let output = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.explain(cypher))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        convert::explain_output_to_py(py, output)
    }

    /// Explain a Locy program's evaluation plan.
    fn explain_locy(&self, py: Python, program: &str) -> PyResult<crate::types::PyLocyResult> {
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.explain_locy(program))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        convert::locy_result_to_py_class(py, result)
    }

    /// Profile a query with operator-level statistics.
    fn profile(&self, py: Python, cypher: &str) -> PyResult<(Vec<Py<PyAny>>, Py<PyAny>)> {
        let (results, profile) = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.profile(cypher))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let rows = convert::rows_to_py(py, results.into_rows())?;
        let profile_dict = convert::profile_output_to_py(py, profile)?;
        Ok((rows, profile_dict))
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
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(crate::types::PyCompiledProgram { inner: compiled })
    }

    /// Get session metrics.
    fn metrics(&self) -> crate::types::PySessionMetrics {
        let m = self.inner.metrics();
        crate::types::PySessionMetrics {
            session_id: m.session_id,
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

    /// Cancel in-progress operations on this session.
    fn cancel(&mut self) {
        self.inner.cancel();
    }

    /// Pin this session to a specific snapshot version.
    fn pin_to_version(&mut self, snapshot_id: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.pin_to_version(snapshot_id))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Pin this session to a specific timestamp (seconds since epoch).
    fn pin_to_timestamp(&mut self, epoch_secs: f64) -> PyResult<()> {
        let ts = chrono::DateTime::from_timestamp(
            epoch_secs as i64,
            ((epoch_secs.fract()) * 1_000_000_000.0) as u32,
        )
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid timestamp")
        })?;
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.pin_to_timestamp(ts))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Refresh session to latest database version (unpins if pinned).
    fn refresh(&mut self) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.refresh())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Create a query builder for parameterized queries.
    fn query_with(slf: Py<Self>, cypher: &str) -> SessionQueryBuilder {
        SessionQueryBuilder {
            session: slf,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_memory: None,
        }
    }

    /// Create a builder for parameterized auto-commit mutations.
    fn execute_with(slf: Py<Self>, cypher: &str) -> SessionAutoCommitBuilder {
        SessionAutoCommitBuilder {
            session: slf,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
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
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(self.inner.query_cursor(cypher))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
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
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
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

    /// Fetch all results.
    fn fetch_all(&self, py: Python) -> PyResult<Vec<Py<PyAny>>> {
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
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.fetch_all())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        convert::rows_to_py(py, result.into_rows())
    }

    /// Fetch a single row or None.
    fn fetch_one(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let rows = self.fetch_all(py)?;
        Ok(rows.into_iter().next())
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
        let cursor = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.cursor())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let columns = cursor.columns().to_vec();
        Ok(crate::sync_api::QueryCursor {
            cursor: std::sync::Mutex::new(Some(cursor)),
            buffer: std::sync::Mutex::new(VecDeque::new()),
            columns,
        })
    }
}

/// Builder for auto-commit mutations on a Session.
#[pyclass(name = "AutoCommitBuilder")]
pub struct SessionAutoCommitBuilder {
    pub(crate) session: Py<Session>,
    pub(crate) cypher: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
}

#[pymethods]
impl SessionAutoCommitBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Set execution timeout in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Execute the mutation and return an AutoCommitResult.
    fn run(&self, py: Python) -> PyResult<crate::types::PyAutoCommitResult> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.execute_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.run())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        convert::auto_commit_result_to_py(py, result)
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
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.run())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
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

    /// Execute the profile.
    fn run(&self, py: Python) -> PyResult<(Vec<Py<PyAny>>, Py<PyAny>)> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.profile_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        let (results, profile) = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.run())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let rows = convert::rows_to_py(py, results.into_rows())?;
        let profile_dict = convert::profile_output_to_py(py, profile)?;
        Ok((rows, profile_dict))
    }
}

/// Builder for transaction configuration.
#[pyclass(name = "TransactionBuilder")]
pub struct PyTransactionBuilder {
    pub(crate) session: Py<Session>,
    pub(crate) timeout_secs: Option<f64>,
}

#[pymethods]
impl PyTransactionBuilder {
    /// Set transaction timeout in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Start the transaction.
    fn start(&self, py: Python) -> PyResult<super::sync_api::Transaction> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.tx_with();
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        let tx = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.start())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
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
}

#[pymethods]
impl PyTxQueryBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Fetch all results.
    fn fetch_all(&self, py: Python) -> PyResult<Vec<Py<PyAny>>> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let mut builder = tx.query_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.fetch_all())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        convert::rows_to_py(py, result.into_rows())
    }

    /// Fetch a single row or None.
    fn fetch_one(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let rows = self.fetch_all(py)?;
        Ok(rows.into_iter().next())
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
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        convert::execute_result_to_py(py, result)
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
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
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
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(builder.run())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
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
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
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
                if let Err(e) = method.call1(py, (py_ctx,)) {
                    return Err(uni_common::UniError::HookRejected {
                        message: e.to_string(),
                    });
                }
            }
            Ok(())
        })
    }

    fn after_query(&self, ctx: &::uni_db::HookContext, _metrics: &::uni_db::QueryMetrics) {
        Python::attach(|py| {
            if let Ok(method) = self.py_obj.getattr(py, "after_query") {
                let py_ctx = pyo3::types::PyDict::new(py);
                py_ctx.set_item("session_id", &ctx.session_id).ok();
                py_ctx.set_item("query_text", &ctx.query_text).ok();
                let _ = method.call1(py, (py_ctx,));
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

    fn after_commit(&self, ctx: &::uni_db::CommitHookContext, _result: &::uni_db::CommitResult) {
        Python::attach(|py| {
            if let Ok(method) = self.py_obj.getattr(py, "after_commit") {
                let py_ctx = pyo3::types::PyDict::new(py);
                py_ctx.set_item("session_id", &ctx.session_id).ok();
                py_ctx.set_item("tx_id", &ctx.tx_id).ok();
                py_ctx.set_item("mutation_count", ctx.mutation_count).ok();
                let _ = method.call1(py, (py_ctx,));
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
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Flush remaining rows and commit.
    fn finish(&self) -> PyResult<BulkStats> {
        let mut guard = self.inner.lock().unwrap();
        let appender = guard.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Appender already finished")
        })?;
        let stats = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(appender.finish())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(BulkStats {
            vertices_inserted: stats.vertices_inserted,
            edges_inserted: stats.edges_inserted,
            indexes_rebuilt: stats.indexes_rebuilt,
            duration_secs: stats.duration.as_secs_f64(),
            index_build_duration_secs: stats.index_build_duration.as_secs_f64(),
            indexes_pending: stats.indexes_pending,
        })
    }

    /// Abort without committing.
    fn abort(&self) -> PyResult<()> {
        let mut guard = self.inner.lock().unwrap();
        if let Some(appender) = guard.as_mut() {
            appender.abort();
        }
        Ok(())
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
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?,
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
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
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
// BulkWriterBuilder and BulkWriter
// ============================================================================

/// Builder for configuring bulk data loading.
#[pyclass]
#[derive(Clone)]
pub struct BulkWriterBuilder {
    pub(crate) inner: Arc<Uni>,
    pub(crate) defer_vector_indexes: bool,
    pub(crate) defer_scalar_indexes: bool,
    pub(crate) batch_size: usize,
    pub(crate) async_indexes: bool,
}

#[pymethods]
impl BulkWriterBuilder {
    /// Defer vector index building until commit.
    fn defer_vector_indexes(mut slf: PyRefMut<'_, Self>, defer: bool) -> PyRefMut<'_, Self> {
        slf.defer_vector_indexes = defer;
        slf
    }

    /// Defer scalar index building until commit.
    fn defer_scalar_indexes(mut slf: PyRefMut<'_, Self>, defer: bool) -> PyRefMut<'_, Self> {
        slf.defer_scalar_indexes = defer;
        slf
    }

    /// Set batch size for flushing to storage.
    fn batch_size(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.batch_size = size;
        slf
    }

    /// Build indexes asynchronously after commit.
    fn async_indexes(mut slf: PyRefMut<'_, Self>, async_: bool) -> PyRefMut<'_, Self> {
        slf.async_indexes = async_;
        slf
    }

    /// Build the BulkWriter.
    fn build(&self) -> PyResult<BulkWriter> {
        Ok(BulkWriter {
            inner: self.inner.clone(),
            stats: BulkStats::default(),
            aborted: false,
            committed: false,
        })
    }
}

/// Bulk writer for high-throughput data ingestion.
#[pyclass]
pub struct BulkWriter {
    inner: Arc<Uni>,
    stats: BulkStats,
    aborted: bool,
    committed: bool,
}

#[pymethods]
impl BulkWriter {
    /// Insert vertices in bulk, returning allocated VIDs.
    fn insert_vertices(
        &mut self,
        py: Python,
        label: &str,
        vertices: Vec<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Vec<u64>> {
        if self.aborted || self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "BulkWriter already completed",
            ));
        }

        let mut rust_props = Vec::new();
        for v in vertices {
            let mut map = HashMap::new();
            for (k, val) in v {
                let value = convert::py_object_to_value(py, &val)?;
                map.insert(k, serde_json::Value::from(value));
            }
            rust_props.push(map);
        }

        let vids = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::bulk_insert_vertices_core(
                &self.inner,
                label,
                rust_props,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        self.stats.vertices_inserted += vids.len();
        Ok(vids.into_iter().map(|v| v.as_u64()).collect())
    }

    /// Insert edges in bulk.
    fn insert_edges(
        &mut self,
        py: Python,
        edge_type: &str,
        edges: Vec<(u64, u64, HashMap<String, Py<PyAny>>)>,
    ) -> PyResult<()> {
        if self.aborted || self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "BulkWriter already completed",
            ));
        }

        let edge_count = edges.len();
        let mut rust_edges = Vec::new();
        for (src, dst, props) in edges {
            let mut map = HashMap::new();
            for (k, v) in props {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, serde_json::Value::from(val));
            }
            rust_edges.push((::uni_db::Vid::from(src), ::uni_db::Vid::from(dst), map));
        }

        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::bulk_insert_edges_core(
                &self.inner,
                edge_type,
                rust_edges,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        self.stats.edges_inserted += edge_count;
        Ok(())
    }

    /// Commit all pending data and rebuild indexes.
    fn commit(&mut self) -> PyResult<BulkStats> {
        if self.aborted || self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "BulkWriter already completed",
            ));
        }

        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::flush_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        self.committed = true;
        Ok(self.stats.clone())
    }

    /// Abort bulk loading and discard uncommitted changes.
    fn abort(&mut self) -> PyResult<()> {
        if self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Cannot abort: already committed",
            ));
        }
        self.aborted = true;
        Ok(())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Auto-abort on exception if not committed.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        if !self.committed && !self.aborted {
            self.aborted = true;
        }
        Ok(false)
    }
}

// ============================================================================
// LocyBuilder (sync)
// ============================================================================

/// Builder for constructing and executing Locy evaluations.
#[pyclass]
pub struct LocyBuilder {
    pub(crate) inner: Arc<Uni>,
    pub(crate) program: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
    pub(crate) max_iterations: Option<usize>,
    pub(crate) config: Option<HashMap<String, Py<PyAny>>>,
}

#[pymethods]
impl LocyBuilder {
    /// Bind a single parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Bind multiple parameters from a dictionary.
    fn params(
        mut slf: PyRefMut<'_, Self>,
        params: HashMap<String, Py<PyAny>>,
    ) -> PyRefMut<'_, Self> {
        slf.params.extend(params);
        slf
    }

    /// Set maximum execution time in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Set maximum fixpoint iterations.
    fn max_iterations(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.max_iterations = Some(n);
        slf
    }

    /// Set full Locy config dict.
    fn config(
        mut slf: PyRefMut<'_, Self>,
        config: HashMap<String, Py<PyAny>>,
    ) -> PyRefMut<'_, Self> {
        slf.config = Some(config);
        slf
    }

    /// Execute the Locy evaluation.
    fn run(&self, py: Python) -> PyResult<crate::types::PyLocyResult> {
        // Extract params while we have the GIL
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }

        // Build config from the config dict if provided, otherwise default
        let mut locy_config = if let Some(ref cfg) = self.config {
            // Re-extract: convert HashMap<String, Py<PyAny>> to owned HashMap for extract_locy_config
            let cfg_owned: HashMap<String, Py<PyAny>> = cfg
                .iter()
                .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                .collect();
            convert::extract_locy_config(py, cfg_owned)?
        } else {
            ::uni_db::locy::LocyConfig::default()
        };

        // Merge explicit params
        locy_config.params.extend(rust_params);

        if let Some(t) = self.timeout_secs {
            locy_config.timeout = std::time::Duration::from_secs_f64(t);
        }
        if let Some(n) = self.max_iterations {
            locy_config.max_iterations = n;
        }

        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::locy_evaluate_with_config_core(
                &self.inner,
                &self.program,
                locy_config,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        convert::locy_result_to_py_class(py, result)
    }
}
