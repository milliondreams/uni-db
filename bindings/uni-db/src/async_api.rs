// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Async Python API — `AsyncDatabase`, `AsyncTransaction`, `AsyncSession`,
//! `AsyncBulkWriter`.

use crate::convert;
use crate::core::{self, OpenMode};
use crate::types::*;
use ::uni_db::Uni;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

// ============================================================================
// AsyncDatabase
// ============================================================================

/// Async entry point for the Uni embedded graph database.
#[pyclass]
pub struct AsyncDatabase {
    inner: Arc<Uni>,
}

#[pymethods]
impl AsyncDatabase {
    /// Open or create a database at the given path.
    #[staticmethod]
    fn open<'py>(py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = Uni::open(&path)
                .build()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
            Ok(AsyncDatabase {
                inner: Arc::new(uni),
            })
        })
    }

    /// Create a temporary in-memory database.
    #[staticmethod]
    fn temporary<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = Uni::temporary()
                .build()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
            Ok(AsyncDatabase {
                inner: Arc::new(uni),
            })
        })
    }

    /// Return an AsyncDatabaseBuilder for advanced configuration.
    #[staticmethod]
    fn builder() -> AsyncDatabaseBuilder {
        AsyncDatabaseBuilder {
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
        }
    }

    // ========================================================================
    // Query Methods
    // ========================================================================

    /// Execute a Cypher query and return results.
    #[pyo3(signature = (cypher, params=None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::prepare_params(py, params)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = core::query_core(&inner, &cypher, rust_params)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Python::attach(|py| convert::rows_to_py(py, rows.rows))
        })
    }

    /// Execute a mutation query, returning affected row count.
    #[pyo3(signature = (cypher, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::prepare_params(py, params)?;
        let inner = self.inner.clone();
        if rust_params.is_empty() {
            pyo3_async_runtimes::tokio::future_into_py(py, async move {
                core::execute_core(&inner, &cypher)
                    .await
                    .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
            })
        } else {
            pyo3_async_runtimes::tokio::future_into_py(py, async move {
                core::execute_with_params_core(&inner, &cypher, rust_params)
                    .await
                    .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
            })
        }
    }

    /// Explain the query plan without executing.
    fn explain<'py>(&self, py: Python<'py>, cypher: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let output = core::explain_core(&inner, &cypher)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

            Python::attach(|py| {
                let dict = PyDict::new(py);
                dict.set_item("plan_text", &output.plan_text)?;
                dict.set_item("warnings", &output.warnings)?;

                let cost_dict = PyDict::new(py);
                cost_dict.set_item("estimated_rows", output.cost_estimates.estimated_rows)?;
                cost_dict.set_item("estimated_cost", output.cost_estimates.estimated_cost)?;
                dict.set_item("cost_estimates", cost_dict)?;

                let index_usage = PyList::empty(py);
                for usage in &output.index_usage {
                    let usage_dict = PyDict::new(py);
                    usage_dict.set_item("label_or_type", &usage.label_or_type)?;
                    usage_dict.set_item("property", &usage.property)?;
                    usage_dict.set_item("index_type", &usage.index_type)?;
                    usage_dict.set_item("used", usage.used)?;
                    if let Some(reason) = &usage.reason {
                        usage_dict.set_item("reason", reason)?;
                    }
                    index_usage.append(usage_dict)?;
                }
                dict.set_item("index_usage", index_usage)?;

                let suggestions = PyList::empty(py);
                for suggestion in &output.suggestions {
                    let sug_dict = PyDict::new(py);
                    sug_dict.set_item("label_or_type", &suggestion.label_or_type)?;
                    sug_dict.set_item("property", &suggestion.property)?;
                    sug_dict.set_item("index_type", &suggestion.index_type)?;
                    sug_dict.set_item("reason", &suggestion.reason)?;
                    sug_dict.set_item("create_statement", &suggestion.create_statement)?;
                    suggestions.append(sug_dict)?;
                }
                dict.set_item("suggestions", suggestions)?;

                dict.into_py_any(py)
            })
        })
    }

    /// Profile query execution with operator-level statistics.
    fn profile<'py>(&self, py: Python<'py>, cypher: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (results, profile) = core::profile_core(&inner, &cypher)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

            Python::attach(|py| {
                let rows = convert::rows_to_py(py, results.rows)?;

                let profile_dict = PyDict::new(py);
                profile_dict.set_item("total_time_ms", profile.total_time_ms)?;
                profile_dict.set_item("peak_memory_bytes", profile.peak_memory_bytes)?;
                profile_dict.set_item("plan_text", &profile.explain.plan_text)?;

                let ops = PyList::empty(py);
                for op in &profile.runtime_stats {
                    let op_dict = PyDict::new(py);
                    op_dict.set_item("operator", &op.operator)?;
                    op_dict.set_item("actual_rows", op.actual_rows)?;
                    op_dict.set_item("time_ms", op.time_ms)?;
                    op_dict.set_item("memory_bytes", op.memory_bytes)?;
                    if let Some(hits) = op.index_hits {
                        op_dict.set_item("index_hits", hits)?;
                    }
                    if let Some(misses) = op.index_misses {
                        op_dict.set_item("index_misses", misses)?;
                    }
                    ops.append(op_dict)?;
                }
                profile_dict.set_item("operators", ops)?;

                let tuple = (rows, profile_dict.into_py_any(py)?);
                tuple.into_py_any(py)
            })
        })
    }

    /// Flush all uncommitted changes to persistent storage.
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::flush_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    // ========================================================================
    // Transaction Methods
    // ========================================================================

    /// Begin a new async transaction.
    fn begin<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::begin_transaction_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Ok(AsyncTransaction {
                inner,
                completed: false,
            })
        })
    }

    // ========================================================================
    // Schema Methods
    // ========================================================================

    /// Create a label.
    fn create_label<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::create_label_core(&inner, &name)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Create an edge type.
    #[pyo3(signature = (name, from_labels=None, to_labels=None))]
    fn create_edge_type<'py>(
        &self,
        py: Python<'py>,
        name: String,
        from_labels: Option<Vec<String>>,
        to_labels: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let from = from_labels.unwrap_or_default();
        let to = to_labels.unwrap_or_default();
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::create_edge_type_core(&inner, &name, from, to)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Add a property to a label or edge type.
    fn add_property<'py>(
        &self,
        py: Python<'py>,
        label_or_type: String,
        name: String,
        data_type: String,
        nullable: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dt = core::parse_data_type(&data_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::add_property_core(&inner, &label_or_type, &name, dt, nullable)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Check if a label exists.
    fn label_exists<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::label_exists_core(&inner, &name)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Check if an edge type exists.
    fn edge_type_exists<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::edge_type_exists_core(&inner, &name)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// List all label names.
    fn list_labels<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::list_labels_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// List all edge type names.
    fn list_edge_types<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::list_edge_types_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Get detailed information about a label.
    fn get_label_info<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let info = core::get_label_info_core(&inner, &name)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

            Ok(info.map(|i| LabelInfo {
                name: i.name,
                count: i.count,
                properties: i
                    .properties
                    .into_iter()
                    .map(|p| PropertyInfo {
                        name: p.name,
                        data_type: p.data_type,
                        nullable: p.nullable,
                        is_indexed: p.is_indexed,
                    })
                    .collect(),
                indexes: i
                    .indexes
                    .into_iter()
                    .map(|idx| IndexInfo {
                        name: idx.name,
                        index_type: idx.index_type,
                        properties: idx.properties,
                        status: idx.status,
                    })
                    .collect(),
                constraints: i
                    .constraints
                    .into_iter()
                    .map(|c| ConstraintInfo {
                        name: c.name,
                        constraint_type: c.constraint_type,
                        properties: c.properties,
                        enabled: c.enabled,
                    })
                    .collect(),
            }))
        })
    }

    /// Get the full schema as a dictionary.
    fn get_schema<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        // Schema is synchronous, no need for async wrapper
        let schema = self.inner.get_schema();
        let dict = PyDict::new(py);

        let labels = PyDict::new(py);
        for (name, meta) in &schema.labels {
            let label_dict = PyDict::new(py);
            label_dict.set_item("id", meta.id)?;
            labels.set_item(name, label_dict)?;
        }
        dict.set_item("labels", labels)?;

        let edge_types = PyDict::new(py);
        for (name, meta) in &schema.edge_types {
            let et_dict = PyDict::new(py);
            et_dict.set_item("id", meta.id)?;
            edge_types.set_item(name, et_dict)?;
        }
        dict.set_item("edge_types", edge_types)?;

        Ok(dict.into())
    }

    /// Load schema from a JSON file.
    fn load_schema<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::load_schema_core(&inner, &path)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Save schema to a JSON file.
    fn save_schema<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::save_schema_core(&inner, &path)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    // ========================================================================
    // Index Methods
    // ========================================================================

    /// Create a scalar index.
    fn create_scalar_index<'py>(
        &self,
        py: Python<'py>,
        label: String,
        property: String,
        index_type: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::create_scalar_index_core(&inner, &label, &property, &index_type)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Create a vector index.
    fn create_vector_index<'py>(
        &self,
        py: Python<'py>,
        label: String,
        property: String,
        metric: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::create_vector_index_core(&inner, &label, &property, &metric)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    // ========================================================================
    // Session Methods
    // ========================================================================

    /// Create an async session builder.
    fn session(&self) -> AsyncSessionBuilder {
        AsyncSessionBuilder {
            inner: self.inner.clone(),
            variables: HashMap::new(),
        }
    }

    // ========================================================================
    // Bulk Loading Methods
    // ========================================================================

    /// Create an async bulk writer builder.
    fn bulk_writer(&self) -> AsyncBulkWriterBuilder {
        AsyncBulkWriterBuilder {
            inner: self.inner.clone(),
            defer_vector_indexes: true,
            defer_scalar_indexes: true,
            batch_size: 10_000,
            async_indexes: false,
        }
    }

    /// Bulk insert vertices (convenience method).
    fn bulk_insert_vertices<'py>(
        &self,
        py: Python<'py>,
        label: String,
        properties_list: Vec<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_props = Vec::new();
        for p in properties_list {
            let mut map = HashMap::new();
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, serde_json::Value::from(val));
            }
            rust_props.push(map);
        }

        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let vids = core::bulk_insert_vertices_core(&inner, &label, rust_props)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Ok(vids.into_iter().map(|v| v.as_u64()).collect::<Vec<_>>())
        })
    }

    /// Bulk insert edges (convenience method).
    fn bulk_insert_edges<'py>(
        &self,
        py: Python<'py>,
        edge_type: String,
        edges: Vec<(u64, u64, HashMap<String, Py<PyAny>>)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_edges = Vec::new();
        for (src, dst, props) in edges {
            let mut map = HashMap::new();
            for (k, v) in props {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, serde_json::Value::from(val));
            }
            rust_edges.push((::uni_db::Vid::from(src), ::uni_db::Vid::from(dst), map));
        }

        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::bulk_insert_edges_core(&inner, &edge_type, rust_edges)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Open a streaming cursor for a query.
    #[pyo3(signature = (cypher, params=None))]
    fn query_cursor<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::prepare_params(py, params)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let cursor = core::query_cursor_core(&inner, &cypher, rust_params, None, None)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            let columns = cursor.columns().to_vec();
            Ok(AsyncQueryCursor {
                cursor: Arc::new(tokio::sync::Mutex::new(Some(cursor))),
                buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
                columns,
            })
        })
    }

    /// Create a query builder for parameterized queries.
    fn query_with(&self, cypher: String) -> AsyncQueryBuilder {
        AsyncQueryBuilder {
            inner: self.inner.clone(),
            cypher,
            params: HashMap::new(),
            timeout_secs: None,
            max_memory: None,
        }
    }

    /// Create a schema builder.
    fn schema(&self) -> AsyncSchemaBuilder {
        AsyncSchemaBuilder {
            inner: self.inner.clone(),
            pending_labels: Vec::new(),
            pending_edge_types: Vec::new(),
            pending_properties: Vec::new(),
            pending_indexes: Vec::new(),
        }
    }

    /// Evaluate a Locy program and return derived facts, stats, and command results.
    #[pyo3(signature = (program, config=None))]
    fn locy_evaluate<'py>(
        &self,
        py: Python<'py>,
        program: String,
        config: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        let locy_config = config
            .map(|cfg| convert::extract_locy_config(py, cfg))
            .transpose()?;
        // The locy future is !Send due to QueryPlanner's Cell<usize>.
        // Use spawn_blocking + block_on to run it from a blocking thread.
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    match locy_config {
                        Some(cfg) => core::locy_evaluate_with_config_core(&db, &program, cfg).await,
                        None => core::locy_evaluate_core(&db, &program).await,
                    }
                })
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Python::attach(|py| convert::locy_result_to_py(py, result))
        })
    }

    /// Get an AsyncXervo facade for embedding and generation operations.
    fn xervo(&self) -> PyResult<AsyncXervo> {
        Ok(AsyncXervo {
            inner: self.inner.clone(),
        })
    }

    // -----------------------------------------------------------------------
    // Snapshot management
    // -----------------------------------------------------------------------

    /// Create a point-in-time snapshot. Returns the snapshot ID.
    #[pyo3(signature = (name=None))]
    fn create_snapshot<'py>(
        &self,
        py: Python<'py>,
        name: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::create_snapshot_core(&db, name)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// List all available snapshots.
    fn list_snapshots<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let manifests = core::list_snapshots_core(&db)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Python::attach(|py| {
                manifests
                    .into_iter()
                    .map(|m| convert::snapshot_manifest_to_py(py, m))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Restore the database to a specific snapshot.
    fn restore_snapshot<'py>(
        &self,
        py: Python<'py>,
        snapshot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::restore_snapshot_core(&db, &snapshot_id)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    // -----------------------------------------------------------------------
    // Index administration
    // -----------------------------------------------------------------------

    /// Get status of background index rebuild tasks.
    fn index_rebuild_status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tasks = core::index_rebuild_status_core(&db)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Python::attach(|py| {
                tasks
                    .into_iter()
                    .map(|t| convert::index_rebuild_task_to_py(py, t))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Retry failed index rebuild tasks.
    fn retry_index_rebuilds<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::retry_index_rebuilds_core(&db)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Force rebuild indexes for a label. If async_=True, returns a task ID.
    #[pyo3(signature = (label, async_=false))]
    fn rebuild_indexes<'py>(
        &self,
        py: Python<'py>,
        label: String,
        async_: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::rebuild_indexes_core(&db, &label, async_)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Check if an index is currently being rebuilt for a label.
    fn is_index_building<'py>(
        &self,
        py: Python<'py>,
        label: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::is_index_building_core(&db, &label)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// List all indexes defined on a specific label.
    fn list_indexes<'py>(
        &self,
        py: Python<'py>,
        label: String,
    ) -> PyResult<Vec<crate::types::IndexDefinitionInfo>> {
        core::list_indexes_core(&self.inner, &label)
            .into_iter()
            .map(|i| convert::index_definition_to_py(py, i))
            .collect()
    }

    /// List all indexes in the database.
    fn list_all_indexes<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Vec<crate::types::IndexDefinitionInfo>> {
        core::list_all_indexes_core(&self.inner)
            .into_iter()
            .map(|i| convert::index_definition_to_py(py, i))
            .collect()
    }
}

// ============================================================================
// AsyncXervo
// ============================================================================

/// Async facade for Uni-Xervo embedding and generation.
#[pyclass]
pub struct AsyncXervo {
    inner: Arc<Uni>,
}

#[pymethods]
impl AsyncXervo {
    /// Embed texts using a configured model alias (async).
    fn embed<'py>(
        &self,
        py: Python<'py>,
        alias: String,
        texts: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::xervo_embed_core(&db, &alias, texts)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Generate text using structured messages (async).
    ///
    /// Each message may be a `Message` instance or a dict with `"role"` and `"content"` keys.
    #[pyo3(signature = (alias, messages, max_tokens=None, temperature=None, top_p=None))]
    fn generate<'py>(
        &self,
        py: Python<'py>,
        alias: String,
        messages: Vec<Py<PyAny>>,
        max_tokens: Option<usize>,
        temperature: Option<f32>,
        top_p: Option<f32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        // Extract messages while the GIL is held, before entering the async block.
        let msg_pairs = crate::convert::extract_messages(py, messages)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result =
                core::xervo_generate_core(&db, &alias, msg_pairs, max_tokens, temperature, top_p)
                    .await
                    .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Python::attach(|py| crate::convert::generation_result_to_py(py, result))
        })
    }

    /// Generate text from a single user prompt (async). Convenience wrapper around `generate()`.
    #[pyo3(signature = (alias, prompt, max_tokens=None, temperature=None, top_p=None))]
    fn generate_text<'py>(
        &self,
        py: Python<'py>,
        alias: String,
        prompt: String,
        max_tokens: Option<usize>,
        temperature: Option<f32>,
        top_p: Option<f32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        let msg_pairs = vec![("user".to_string(), prompt)];
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result =
                core::xervo_generate_core(&db, &alias, msg_pairs, max_tokens, temperature, top_p)
                    .await
                    .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Python::attach(|py| crate::convert::generation_result_to_py(py, result))
        })
    }
}

// ============================================================================
// AsyncDatabaseBuilder
// ============================================================================

/// Async builder for creating and configuring an AsyncDatabase instance.
#[pyclass]
#[derive(Debug, Clone)]
pub struct AsyncDatabaseBuilder {
    uri: String,
    mode: OpenMode,
    hybrid_local: Option<String>,
    hybrid_remote: Option<String>,
    cache_size: Option<usize>,
    parallelism: Option<usize>,
    schema_file: Option<String>,
    xervo_catalog_json: Option<String>,
    xervo_catalog_file: Option<String>,
    cloud_config: Option<uni_common::CloudStorageConfig>,
    uni_config: Option<uni_common::UniConfig>,
}

#[pymethods]
impl AsyncDatabaseBuilder {
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
        }
    }

    /// Open an existing database.
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
        }
    }

    /// Create a new database.
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
        }
    }

    /// Create a temporary database.
    #[staticmethod]
    fn temporary() -> Self {
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
        }
    }

    /// Create an in-memory database (alias for temporary).
    #[staticmethod]
    fn in_memory() -> Self {
        Self::temporary()
    }

    /// Configure hybrid storage.
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

    /// Set query parallelism.
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
        config: std::collections::HashMap<String, Py<PyAny>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let py = slf.py();
        slf.cloud_config = Some(crate::convert::extract_cloud_config(py, &config)?);
        Ok(slf)
    }

    /// Configure database options (query_timeout, max_query_memory, etc.).
    fn config(
        mut slf: PyRefMut<'_, Self>,
        config: std::collections::HashMap<String, Py<PyAny>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let py = slf.py();
        slf.uni_config = Some(crate::convert::extract_uni_config(py, &config)?);
        Ok(slf)
    }

    /// Build and return the AsyncDatabase instance (returns awaitable).
    fn build<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let uri = self.uri.clone();
        let mode = self.mode;
        let hybrid_local = self.hybrid_local.clone();
        let hybrid_remote = self.hybrid_remote.clone();
        let cache_size = self.cache_size;
        let parallelism = self.parallelism;
        let schema_file = self.schema_file.clone();
        let xervo_catalog_json = self.xervo_catalog_json.clone();
        let xervo_catalog_file = self.xervo_catalog_file.clone();
        let cloud_config = self.cloud_config.clone();
        let uni_config = self.uni_config.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = core::build_database_core(
                &uri,
                mode,
                hybrid_local.as_deref(),
                hybrid_remote.as_deref(),
                cache_size,
                parallelism,
                schema_file.as_deref(),
                xervo_catalog_json.as_deref(),
                xervo_catalog_file.as_deref(),
                cloud_config,
                uni_config,
            )
            .await
            .map_err(PyErr::new::<pyo3::exceptions::PyIOError, _>)?;

            Ok(AsyncDatabase {
                inner: Arc::new(uni),
            })
        })
    }
}

// ============================================================================
// AsyncTransaction
// ============================================================================

/// An async database transaction.
#[pyclass]
pub struct AsyncTransaction {
    inner: Arc<Uni>,
    completed: bool,
}

#[pymethods]
impl AsyncTransaction {
    /// Execute a query within this transaction.
    #[pyo3(signature = (cypher, params=None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        let rust_params = convert::prepare_params(py, params)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = core::query_core(&inner, &cypher, rust_params)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Python::attach(|py| convert::rows_to_py(py, rows.rows))
        })
    }

    /// Commit the transaction.
    fn commit<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        self.completed = true;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::commit_transaction_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Rollback the transaction.
    fn rollback<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        self.completed = true;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::rollback_transaction_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Async context manager support.
    fn __aenter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj: Py<PyAny> = slf.into_any().unbind();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(obj) })
    }

    /// Async context manager exit — commits on success, rolls back on error.
    fn __aexit__<'py>(
        &mut self,
        py: Python<'py>,
        exc_type: Py<PyAny>,
        _exc_val: Py<PyAny>,
        _exc_tb: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.completed {
            // Already committed or rolled back
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok(false) // Don't suppress exceptions
            });
        }
        self.completed = true;
        let inner = self.inner.clone();
        let has_exception = !exc_type.is_none(py);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if has_exception {
                core::rollback_transaction_core(&inner)
                    .await
                    .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            } else {
                core::commit_transaction_core(&inner)
                    .await
                    .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            }
            Ok(false) // Don't suppress exceptions
        })
    }
}

// ============================================================================
// AsyncSession
// ============================================================================

/// Builder for async sessions.
#[pyclass]
#[derive(Clone)]
pub struct AsyncSessionBuilder {
    inner: Arc<Uni>,
    variables: HashMap<String, serde_json::Value>,
}

#[pymethods]
impl AsyncSessionBuilder {
    /// Set a session variable.
    fn set(&mut self, py: Python, key: String, value: Py<PyAny>) -> PyResult<()> {
        let json_val = convert::py_object_to_json(py, &value)?;
        self.variables.insert(key, json_val);
        Ok(())
    }

    /// Build the session.
    fn build(&self) -> PyResult<AsyncSession> {
        Ok(AsyncSession {
            inner: self.inner.clone(),
            variables: self.variables.clone(),
        })
    }
}

/// An async query session with scoped variables.
#[pyclass]
#[derive(Clone)]
pub struct AsyncSession {
    inner: Arc<Uni>,
    variables: HashMap<String, serde_json::Value>,
}

impl AsyncSession {
    /// Build session params: creates a nested Value::Map under key "session".
    fn build_session_params(
        &self,
        py: Python,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<HashMap<String, ::uni_db::Value>> {
        let mut rust_params = HashMap::new();

        // Build session variable map
        let mut session_map = HashMap::new();
        for (k, v) in &self.variables {
            let val = ::uni_db::Value::from(v.clone());
            session_map.insert(k.clone(), val);
        }
        rust_params.insert("session".to_string(), ::uni_db::Value::Map(session_map));

        // Add query params (may override if same key)
        if let Some(p) = params {
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                rust_params.insert(k, val);
            }
        }

        Ok(rust_params)
    }
}

#[pymethods]
impl AsyncSession {
    /// Execute a query with session variables.
    #[pyo3(signature = (cypher, params=None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = self.build_session_params(py, params)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = core::query_core(&inner, &cypher, rust_params)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Python::attach(|py| convert::rows_to_py(py, rows.rows))
        })
    }

    /// Execute a mutation query.
    fn execute<'py>(&self, py: Python<'py>, cypher: String) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = self.build_session_params(py, None)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::execute_with_params_core(&inner, &cypher, rust_params)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Get a session variable value.
    fn get(&self, py: Python, key: &str) -> PyResult<Option<Py<PyAny>>> {
        match self.variables.get(key) {
            Some(v) => Ok(Some(convert::json_value_to_py(py, v)?)),
            None => Ok(None),
        }
    }
}

// ============================================================================
// AsyncBulkWriter
// ============================================================================

/// Builder for async bulk writer.
#[pyclass]
#[derive(Clone)]
pub struct AsyncBulkWriterBuilder {
    inner: Arc<Uni>,
    defer_vector_indexes: bool,
    defer_scalar_indexes: bool,
    batch_size: usize,
    async_indexes: bool,
}

#[pymethods]
impl AsyncBulkWriterBuilder {
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

    /// Build the AsyncBulkWriter.
    fn build(&self) -> PyResult<AsyncBulkWriter> {
        Ok(AsyncBulkWriter {
            inner: self.inner.clone(),
            stats: Arc::new(std::sync::Mutex::new(BulkStats::default())),
            aborted: false,
            committed: false,
        })
    }
}

/// Async bulk writer for high-throughput data ingestion.
#[pyclass]
pub struct AsyncBulkWriter {
    inner: Arc<Uni>,
    stats: Arc<std::sync::Mutex<BulkStats>>,
    aborted: bool,
    committed: bool,
}

#[pymethods]
impl AsyncBulkWriter {
    /// Insert vertices in bulk.
    fn insert_vertices<'py>(
        &mut self,
        py: Python<'py>,
        label: String,
        vertices: Vec<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
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

        let inner = self.inner.clone();
        let stats = self.stats.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let vids = core::bulk_insert_vertices_core(&inner, &label, rust_props)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            if let Ok(mut s) = stats.lock() {
                s.vertices_inserted += vids.len();
            }
            Ok(vids.into_iter().map(|v| v.as_u64()).collect::<Vec<_>>())
        })
    }

    /// Insert edges in bulk.
    fn insert_edges<'py>(
        &mut self,
        py: Python<'py>,
        edge_type: String,
        edges: Vec<(u64, u64, HashMap<String, Py<PyAny>>)>,
    ) -> PyResult<Bound<'py, PyAny>> {
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

        let inner = self.inner.clone();
        let stats = self.stats.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::bulk_insert_edges_core(&inner, &edge_type, rust_edges)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            if let Ok(mut s) = stats.lock() {
                s.edges_inserted += edge_count;
            }
            Ok(())
        })
    }

    /// Commit all pending data.
    fn commit<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.aborted || self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "BulkWriter already completed",
            ));
        }
        self.committed = true;
        let stats = self.stats.clone();
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::flush_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            let final_stats = stats.lock().unwrap().clone();
            Ok(final_stats)
        })
    }

    /// Abort bulk loading.
    fn abort(&mut self) -> PyResult<()> {
        if self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Cannot abort: already committed",
            ));
        }
        self.aborted = true;
        Ok(())
    }
}

// ============================================================================
// AsyncQueryBuilder
// ============================================================================

/// Async builder for constructing and executing parameterized queries.
#[pyclass]
pub struct AsyncQueryBuilder {
    inner: Arc<Uni>,
    cypher: String,
    params: HashMap<String, Py<PyAny>>,
    timeout_secs: Option<f64>,
    max_memory: Option<usize>,
}

#[pymethods]
impl AsyncQueryBuilder {
    /// Bind a parameter to the query.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Bind multiple parameters from a dictionary.
    fn params(&mut self, params: Bound<'_, PyDict>) {
        for (k, v) in params {
            if let Ok(key) = k.extract::<String>() {
                self.params.insert(key, v.into());
            }
        }
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

    /// Open a streaming cursor for this query (returns awaitable).
    fn cursor<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }

        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        let max_memory = self.max_memory;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let cursor =
                core::query_cursor_core(&inner, &cypher, rust_params, timeout_secs, max_memory)
                    .await
                    .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            let columns = cursor.columns().to_vec();
            Ok(AsyncQueryCursor {
                cursor: Arc::new(tokio::sync::Mutex::new(Some(cursor))),
                buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
                columns,
            })
        })
    }

    /// Execute the query and fetch all results (returns awaitable).
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }

        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        let max_memory = self.max_memory;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows =
                core::query_builder_core(&inner, &cypher, rust_params, timeout_secs, max_memory)
                    .await
                    .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Python::attach(|py| convert::rows_to_py(py, rows.rows))
        })
    }
}

// ============================================================================
// AsyncQueryCursor
// ============================================================================

/// Async cursor-based result streaming for large query result sets.
///
/// Implements Python's async iterator (`__aiter__`/`__anext__`) and async context
/// manager (`__aenter__`/`__aexit__`).
#[pyclass]
pub struct AsyncQueryCursor {
    cursor: Arc<tokio::sync::Mutex<Option<core::QueryCursor>>>,
    buffer: Arc<tokio::sync::Mutex<VecDeque<core::Row>>>,
    #[pyo3(get)]
    columns: Vec<String>,
}

impl AsyncQueryCursor {
    /// Pull the next single row, refilling from the batch stream as needed.
    async fn next_row_async(&self) -> Result<Option<core::Row>, String> {
        {
            let mut buf = self.buffer.lock().await;
            if let Some(row) = buf.pop_front() {
                return Ok(Some(row));
            }
        }
        // Buffer empty – fetch next batch from cursor.
        let mut guard = self.cursor.lock().await;
        let cursor = match guard.as_mut() {
            Some(c) => c,
            None => return Ok(None),
        };
        match cursor.next_batch().await {
            Some(Ok(rows)) => {
                let mut buf = self.buffer.lock().await;
                let mut iter = rows.into_iter();
                let first = iter.next();
                buf.extend(iter);
                Ok(first)
            }
            Some(Err(e)) => Err(e.to_string()),
            None => Ok(None),
        }
    }
}

#[pymethods]
impl AsyncQueryCursor {
    /// Fetch a single row, or `None` if exhausted.
    fn fetch_one<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor.clone();
        let buffer = self.buffer.clone();
        let self_clone = AsyncQueryCursor {
            cursor,
            buffer,
            columns: self.columns.clone(),
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match self_clone.next_row_async().await {
                Ok(Some(row)) => Python::attach(|py| {
                    let dict = PyDict::new(py);
                    for (col, val) in row.as_map() {
                        dict.set_item(col, convert::value_to_py(py, val)?)?;
                    }
                    Ok(Some(dict.into_py_any(py)?))
                }),
                Ok(None) => Ok(None),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e)),
            }
        })
    }

    /// Fetch up to `n` rows.
    #[pyo3(signature = (n))]
    fn fetch_many<'py>(&self, py: Python<'py>, n: usize) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor.clone();
        let buffer = self.buffer.clone();
        let self_clone = AsyncQueryCursor {
            cursor,
            buffer,
            columns: self.columns.clone(),
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut rows = Vec::with_capacity(n);
            for _ in 0..n {
                match self_clone.next_row_async().await {
                    Ok(Some(row)) => rows.push(row),
                    Ok(None) => break,
                    Err(e) => {
                        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
                    }
                }
            }
            Python::attach(|py| convert::rows_to_py(py, rows))
        })
    }

    /// Fetch all remaining rows.
    fn fetch_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cursor_arc = self.cursor.clone();
        let buffer_arc = self.buffer.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Drain buffer first
            let mut rows: Vec<core::Row> = {
                let mut buf = buffer_arc.lock().await;
                buf.drain(..).collect()
            };
            // Take and consume the cursor
            let cursor_opt = {
                let mut guard = cursor_arc.lock().await;
                guard.take()
            };
            if let Some(cursor) = cursor_opt {
                let remaining = cursor.collect_remaining().await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                rows.extend(remaining);
            }
            Python::attach(|py| convert::rows_to_py(py, rows))
        })
    }

    /// Close the cursor, releasing resources.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cursor_arc = self.cursor.clone();
        let buffer_arc = self.buffer.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _ = cursor_arc.lock().await.take();
            buffer_arc.lock().await.clear();
            Ok(())
        })
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor.clone();
        let buffer = self.buffer.clone();
        let self_clone = AsyncQueryCursor {
            cursor,
            buffer,
            columns: self.columns.clone(),
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match self_clone.next_row_async().await {
                Ok(Some(row)) => Python::attach(|py| {
                    let dict = PyDict::new(py);
                    for (col, val) in row.as_map() {
                        dict.set_item(col, convert::value_to_py(py, val)?)?;
                    }
                    dict.into_py_any(py)
                }),
                Ok(None) => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(
                    "end of cursor",
                )),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e)),
            }
        })
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let obj: Py<PyAny> = slf.into_py_any(py)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(obj) })
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cursor_arc = self.cursor.clone();
        let buffer_arc = self.buffer.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _ = cursor_arc.lock().await.take();
            buffer_arc.lock().await.clear();
            Ok(false) // don't suppress exceptions
        })
    }
}

// ============================================================================
// AsyncSchemaBuilder, AsyncLabelBuilder, AsyncEdgeTypeBuilder
// ============================================================================

/// Async builder for defining and modifying the graph schema.
#[pyclass]
#[derive(Clone)]
pub struct AsyncSchemaBuilder {
    inner: Arc<Uni>,
    pending_labels: Vec<String>,
    pending_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    pending_properties: Vec<(String, String, uni_common::core::schema::DataType, bool)>,
    pending_indexes: Vec<uni_common::core::schema::IndexDefinition>,
}

#[pymethods]
impl AsyncSchemaBuilder {
    /// Start defining a new label.
    fn label(&self, name: &str) -> PyResult<AsyncLabelBuilder> {
        Ok(AsyncLabelBuilder {
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
    ) -> PyResult<AsyncEdgeTypeBuilder> {
        Ok(AsyncEdgeTypeBuilder {
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

    /// Apply all pending schema changes (returns awaitable).
    fn apply<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let labels = self.pending_labels.clone();
        let edge_types = self.pending_edge_types.clone();
        let properties = self.pending_properties.clone();
        let indexes = self.pending_indexes.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::apply_schema_core(&inner, &labels, &edge_types, &properties, &indexes)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }
}

/// Async builder for defining a label with its properties and indexes.
#[pyclass]
#[derive(Clone)]
pub struct AsyncLabelBuilder {
    parent_inner: Arc<Uni>,
    parent_labels: Vec<String>,
    parent_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    parent_properties: Vec<(String, String, uni_common::core::schema::DataType, bool)>,
    parent_indexes: Vec<uni_common::core::schema::IndexDefinition>,
    name: String,
    properties: Vec<(String, uni_common::core::schema::DataType, bool)>,
    indexes: Vec<uni_common::core::schema::IndexDefinition>,
}

#[pymethods]
impl AsyncLabelBuilder {
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

    /// Add a vector property.
    fn vector(mut slf: PyRefMut<'_, Self>, name: String, dimensions: usize) -> PyRefMut<'_, Self> {
        slf.properties.push((
            name,
            uni_common::core::schema::DataType::Vector { dimensions },
            false,
        ));
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

    /// Finish this label and return to AsyncSchemaBuilder.
    fn done(&self) -> PyResult<AsyncSchemaBuilder> {
        let mut labels = self.parent_labels.clone();
        labels.push(self.name.clone());

        let mut properties = self.parent_properties.clone();
        for (prop_name, dt, nullable) in &self.properties {
            properties.push((self.name.clone(), prop_name.clone(), dt.clone(), *nullable));
        }

        let mut indexes = self.parent_indexes.clone();
        indexes.extend(self.indexes.clone());

        Ok(AsyncSchemaBuilder {
            inner: self.parent_inner.clone(),
            pending_labels: labels,
            pending_edge_types: self.parent_edge_types.clone(),
            pending_properties: properties,
            pending_indexes: indexes,
        })
    }

    /// Apply schema changes immediately (returns awaitable).
    fn apply<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.done()?.apply(py)
    }
}

/// Async builder for defining an edge type with its properties.
#[pyclass]
#[derive(Clone)]
pub struct AsyncEdgeTypeBuilder {
    parent_inner: Arc<Uni>,
    parent_labels: Vec<String>,
    parent_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    parent_properties: Vec<(String, String, uni_common::core::schema::DataType, bool)>,
    parent_indexes: Vec<uni_common::core::schema::IndexDefinition>,
    name: String,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
    properties: Vec<(String, uni_common::core::schema::DataType, bool)>,
}

#[pymethods]
impl AsyncEdgeTypeBuilder {
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

    /// Finish this edge type and return to AsyncSchemaBuilder.
    fn done(&self) -> PyResult<AsyncSchemaBuilder> {
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

        Ok(AsyncSchemaBuilder {
            inner: self.parent_inner.clone(),
            pending_labels: self.parent_labels.clone(),
            pending_edge_types: edge_types,
            pending_properties: properties,
            pending_indexes: self.parent_indexes.clone(),
        })
    }

    /// Apply schema changes immediately (returns awaitable).
    fn apply<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.done()?.apply(py)
    }
}
