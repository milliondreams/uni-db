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
use pyo3::types::PyDict;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

// ============================================================================
// AsyncDatabase
// ============================================================================

/// Async entry point for the Uni embedded graph database.
#[pyclass(name = "AsyncUni")]
pub struct AsyncDatabase {
    inner: Arc<Uni>,
}

#[pymethods]
impl AsyncDatabase {
    // ========================================================================
    // Static Factory Methods
    // ========================================================================

    /// Open or create a database at the given path.
    #[staticmethod]
    fn open<'py>(py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = Uni::open(&path)
                .build()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
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
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(AsyncDatabase {
                inner: Arc::new(uni),
            })
        })
    }

    /// Create an in-memory database (alias for temporary).
    #[staticmethod]
    fn in_memory<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Self::temporary(py)
    }

    /// Create a new database. Fails if it already exists.
    #[staticmethod]
    fn create<'py>(py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = Uni::create(&path)
                .build()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(AsyncDatabase {
                inner: Arc::new(uni),
            })
        })
    }

    /// Open an existing database. Fails if it does not exist.
    #[staticmethod]
    fn open_existing<'py>(py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = Uni::open_existing(&path)
                .build()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
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
            read_only: false,
            write_lease: None,
        }
    }

    // ========================================================================
    // Context Manager
    // ========================================================================

    fn __aenter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj: Py<PyAny> = slf.into_any().unbind();
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
        // Shutdown on exit.
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _ = inner.flush().await;
            Ok(false)
        })
    }

    fn __repr__(&self) -> String {
        "AsyncUni(open)".to_string()
    }

    /// Flush all uncommitted changes to persistent storage.
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::flush_core(&inner)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    // ========================================================================
    // Schema Methods
    // ========================================================================

    /// Check if a label exists.
    fn label_exists<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::label_exists_core(&inner, &name)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Check if an edge type exists.
    fn edge_type_exists<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::edge_type_exists_core(&inner, &name)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// List all label names.
    fn list_labels<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::list_labels_core(&inner)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// List all edge type names.
    fn list_edge_types<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::list_edge_types_core(&inner)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Get detailed information about a label.
    fn get_label_info<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let info = core::get_label_info_core(&inner, &name)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;

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

    /// Get the schema as a typed `Schema` object.
    fn get_schema_typed(&self) -> crate::types::PySchema {
        crate::types::PySchema {
            inner: self.inner.get_schema(),
        }
    }

    /// Load schema from a JSON file.
    fn load_schema<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::load_schema_core(&inner, &path)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Save schema to a JSON file.
    fn save_schema<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::save_schema_core(&inner, &path)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    // ========================================================================
    // Index Methods
    // ========================================================================

    // ========================================================================
    // Session Methods
    // ========================================================================

    /// Create a new async session wrapping a real Rust Session.
    fn session(&self) -> AsyncSession {
        AsyncSession {
            inner: Arc::new(tokio::sync::Mutex::new(self.inner.session())),
        }
    }

    /// Create a session template builder.
    fn session_template(&self) -> crate::builders::SessionTemplateBuilder {
        crate::builders::SessionTemplateBuilder {
            inner: Some(self.inner.session_template()),
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

    /// Get an AsyncXervo facade for embedding and generation operations.
    fn xervo(&self) -> PyResult<AsyncXervo> {
        Ok(AsyncXervo {
            inner: self.inner.clone(),
        })
    }

    /// Register Locy rules at the database level.
    fn register_rules(&self, program: &str) -> PyResult<()> {
        self.inner
            .register_rules(program)
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Clear all registered Locy rules at the database level.
    fn clear_rules(&self) {
        self.inner.clear_rules();
    }

    /// Flush data and prepare for shutdown.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            db.flush()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Get database-wide metrics.
    fn metrics(&self, py: Python) -> PyResult<crate::types::PyDatabaseMetrics> {
        let m = self.inner.metrics();
        convert::database_metrics_to_py(py, m)
    }

    /// Get the current database configuration as a dict.
    fn config(&self, py: Python) -> PyResult<Py<pyo3::PyAny>> {
        convert::uni_config_to_py(py, self.inner.config())
    }

    /// Get the configured write lease, if any.
    fn write_lease(&self) -> Option<crate::types::PyWriteLease> {
        self.inner.write_lease().map(|wl| match wl {
            ::uni_db::api::multi_agent::WriteLease::Local => crate::types::PyWriteLease {
                variant: crate::types::WriteLeaseVariant::Local,
            },
            ::uni_db::api::multi_agent::WriteLease::DynamoDB { table } => {
                crate::types::PyWriteLease {
                    variant: crate::types::WriteLeaseVariant::DynamoDB {
                        table: table.clone(),
                    },
                }
            }
            _ => crate::types::PyWriteLease {
                variant: crate::types::WriteLeaseVariant::Local,
            },
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
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// List all available snapshots.
    fn list_snapshots<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let manifests = core::list_snapshots_core(&db)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
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
                .map_err(crate::exceptions::uni_error_to_pyerr)
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
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
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
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Force rebuild indexes for a label. If background=True, returns a task ID.
    #[pyo3(signature = (label, background=false))]
    fn rebuild_indexes<'py>(
        &self,
        py: Python<'py>,
        label: String,
        background: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::rebuild_indexes_core(&db, &label, background)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    // -----------------------------------------------------------------------
    // Compaction
    // -----------------------------------------------------------------------

    /// Compact a label's storage files.
    fn compact_label<'py>(&self, py: Python<'py>, label: String) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::compact_label_core(&db, &label)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Compact an edge type's storage files.
    fn compact_edge_type<'py>(
        &self,
        py: Python<'py>,
        edge_type: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::compact_edge_type_core(&db, &edge_type)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Wait for any ongoing compaction to complete.
    fn wait_for_compaction<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::wait_for_compaction_core(&db)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
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
                .map_err(crate::exceptions::uni_error_to_pyerr)
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
    /// Check if Xervo (embedding/generation) is available for this database.
    fn is_available(&self) -> bool {
        self.inner.xervo().is_available()
    }

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
                .map_err(crate::exceptions::uni_error_to_pyerr)
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
                    .map_err(crate::exceptions::uni_error_to_pyerr)?;
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
                    .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| crate::convert::generation_result_to_py(py, result))
        })
    }
}

// ============================================================================
// AsyncDatabaseBuilder
// ============================================================================

/// Async builder for creating and configuring an AsyncUni instance.
#[pyclass(name = "AsyncUniBuilder")]
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
    read_only: bool,
    write_lease: Option<crate::types::PyWriteLease>,
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
            read_only: false,
            write_lease: None,
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
            read_only: false,
            write_lease: None,
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
            read_only: false,
            write_lease: None,
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
            read_only: false,
            write_lease: None,
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
        let read_only = self.read_only;
        let rust_write_lease = self.write_lease.as_ref().map(|wl| match &wl.variant {
            crate::types::WriteLeaseVariant::Local => ::uni_db::api::multi_agent::WriteLease::Local,
            crate::types::WriteLeaseVariant::DynamoDB { table } => {
                ::uni_db::api::multi_agent::WriteLease::DynamoDB {
                    table: table.clone(),
                }
            }
        });

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
                read_only,
                rust_write_lease,
            )
            .await
            .map_err(crate::exceptions::uni_error_to_pyerr)?;

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
///
/// Wraps a Rust `Transaction` which provides ACID guarantees.
/// Use as an async context manager for automatic rollback on error.
#[pyclass]
pub struct AsyncTransaction {
    pub(crate) inner: Arc<tokio::sync::Mutex<Option<::uni_db::Transaction>>>,
}

#[pymethods]
impl AsyncTransaction {
    /// Execute a query within this transaction.
    ///
    /// Returns a `QueryResult` with `.rows`, `.metrics`, `.warnings`, `.columns`.
    #[pyo3(signature = (cypher, params=None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = if let Some(p) = params {
            let mut map = HashMap::new();
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, val);
            }
            Some(map)
        } else {
            None
        };
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let result = if let Some(params) = rust_params {
                let mut builder = tx.query_with(&cypher);
                for (k, v) in params {
                    builder = builder.param(&k, v);
                }
                builder
                    .fetch_all()
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            } else {
                tx.query(&cypher)
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            };
            Python::attach(|py| convert::query_result_to_py_class(py, result))
        })
    }

    /// Execute a mutation query within this transaction, returning an ExecuteResult.
    #[pyo3(signature = (cypher, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = if let Some(p) = params {
            let mut map = HashMap::new();
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, val);
            }
            Some(map)
        } else {
            None
        };
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let result = if let Some(params) = rust_params {
                let mut builder = tx.execute_with(&cypher);
                for (k, v) in params {
                    builder = builder.param(&k, v);
                }
                builder
                    .run()
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            } else {
                tx.execute(&cypher)
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            };
            Python::attach(|py| {
                let py_result = convert::execute_result_to_py(py, result)?;
                Ok(py_result.into_pyobject(py)?.into_any().unbind())
            })
        })
    }

    /// Evaluate a Locy program within this transaction.
    #[pyo3(signature = (program, params=None))]
    fn locy<'py>(
        &self,
        py: Python<'py>,
        program: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = if let Some(p) = params {
            let mut map = HashMap::new();
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, val);
            }
            Some(map)
        } else {
            None
        };
        let inner = self.inner.clone();
        // Locy future is !Send — use spawn_blocking
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let guard = inner.lock().await;
                    let tx = guard.as_ref().ok_or_else(|| {
                        crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                            "Transaction already completed",
                        )
                    })?;
                    if let Some(params) = rust_params {
                        let mut builder = tx.locy_with(&program);
                        for (k, v) in params {
                            builder = builder.param(&k, v);
                        }
                        builder
                            .run()
                            .await
                            .map_err(crate::exceptions::uni_error_to_pyerr)
                    } else {
                        tx.locy(&program)
                            .await
                            .map_err(crate::exceptions::uni_error_to_pyerr)
                    }
                })
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Python::attach(|py| convert::locy_result_to_py_class(py, result))
        })
    }

    /// Apply a DerivedFactSet to this transaction.
    #[pyo3(signature = (derived, require_fresh=false, max_version_gap=None))]
    fn apply<'py>(
        &self,
        py: Python<'py>,
        derived: &mut crate::types::PyDerivedFactSet,
        require_fresh: bool,
        max_version_gap: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dfs = derived.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("DerivedFactSet already consumed")
        })?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let result = if require_fresh || max_version_gap.is_some() {
                let mut builder = tx.apply_with(dfs);
                if require_fresh {
                    builder = builder.require_fresh();
                }
                if let Some(gap) = max_version_gap {
                    builder = builder.max_version_gap(gap);
                }
                builder.run().await
            } else {
                tx.apply(dfs).await
            }
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyApplyResult {
                facts_applied: result.facts_applied,
                version_gap: result.version_gap,
            })
        })
    }

    /// Prepare a Cypher query for repeated execution within this transaction.
    fn prepare<'py>(&self, py: Python<'py>, cypher: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let prepared = tx
                .prepare(&cypher)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyPreparedQuery {
                inner: std::sync::Mutex::new(prepared),
            })
        })
    }

    /// Prepare a Locy program for repeated execution within this transaction.
    fn prepare_locy<'py>(&self, py: Python<'py>, program: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let prepared = tx
                .prepare_locy(&program)
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyPreparedLocy {
                inner: std::sync::Mutex::new(prepared),
            })
        })
    }

    /// Register Locy rules for reuse within this transaction.
    fn register_rules<'py>(&self, py: Python<'py>, program: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            tx.register_rules(&program)
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Clear all registered Locy rules in this transaction.
    fn clear_rules<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            tx.clear_rules();
            Ok(())
        })
    }

    /// Cancel in-progress operations on this transaction.
    fn cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            tx.cancel();
            Ok(())
        })
    }

    /// Commit the transaction, returning a CommitResult.
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let tx = guard.take().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let result = tx
                .commit()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                Ok(crate::types::PyCommitResult::from(result)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind())
            })
        })
    }

    /// Rollback the transaction.
    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let tx = guard.take().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            tx.rollback();
            Ok(())
        })
    }

    /// Get the transaction ID.
    fn id<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            Ok(tx.id().to_string())
        })
    }

    /// Database version when this transaction was started.
    fn started_at_version<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            Ok(tx.started_at_version())
        })
    }

    /// Check if the transaction has uncommitted changes.
    fn is_dirty<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            Ok(tx.is_dirty())
        })
    }

    /// Check if the transaction has been completed (committed or rolled back).
    fn is_completed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            Ok(guard.is_none())
        })
    }

    // ── Builder factories (per-operation timeout, cancellation, etc.) ──

    /// Create a query builder for this transaction.
    fn query_with(&self, cypher: &str) -> AsyncTxQueryBuilder {
        AsyncTxQueryBuilder {
            inner: self.inner.clone(),
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
        }
    }

    /// Create a mutation builder for this transaction.
    fn execute_with(&self, cypher: &str) -> AsyncTxExecuteBuilder {
        AsyncTxExecuteBuilder {
            inner: self.inner.clone(),
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
        }
    }

    /// Create a Locy evaluation builder for this transaction.
    fn locy_with(&self, program: &str) -> AsyncTxLocyBuilder {
        AsyncTxLocyBuilder {
            inner: self.inner.clone(),
            program: program.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_iterations: None,
            locy_config: None,
            cancellation_token: None,
        }
    }

    /// Create an apply builder with staleness controls.
    fn apply_with(
        &self,
        py: Python<'_>,
        derived: Py<crate::types::PyDerivedFactSet>,
    ) -> PyResult<AsyncApplyBuilder> {
        let mut dfs_ref = derived.borrow_mut(py);
        let dfs = dfs_ref.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("DerivedFactSet already consumed")
        })?;
        Ok(AsyncApplyBuilder {
            inner: self.inner.clone(),
            derived: Some(dfs),
            require_fresh: false,
            max_version_gap: None,
        })
    }

    /// Get the transaction's cancellation token.
    fn cancellation_token<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            Ok(crate::types::PyCancellationToken {
                inner: tx.cancellation_token(),
            })
        })
    }

    /// Create a bulk writer builder for high-throughput data ingestion.
    fn bulk_writer(&self) -> AsyncTxBulkWriterBuilder {
        AsyncTxBulkWriterBuilder {
            tx: self.inner.clone(),
            defer_vector_indexes: true,
            defer_scalar_indexes: true,
            batch_size: None,
            async_indexes: false,
            validate_constraints: None,
            max_buffer_size_bytes: None,
            on_progress: None,
        }
    }

    /// Create a streaming appender for the given label.
    fn appender<'py>(&self, py: Python<'py>, label: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let builder = tx.appender(&label);
            let appender = builder
                .build()
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::builders::StreamingAppender {
                inner: std::sync::Mutex::new(Some(appender)),
            })
        })
    }

    /// Async context manager support.
    fn __aenter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj: Py<PyAny> = slf.into_any().unbind();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(obj) })
    }

    /// Async context manager exit — rolls back if still active.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            if let Some(tx) = guard.take() {
                // Transaction still active — roll it back.
                // The Rust Drop impl will also auto-rollback, but explicit is clearer.
                tx.rollback();
            }
            Ok(false) // Don't suppress exceptions
        })
    }
}

// ============================================================================
// AsyncTransaction Bulk Writer Builder
// ============================================================================

/// Builder for async bulk writer within a transaction.
#[pyclass(name = "AsyncTxBulkWriterBuilder")]
pub struct AsyncTxBulkWriterBuilder {
    tx: Arc<tokio::sync::Mutex<Option<::uni_db::Transaction>>>,
    defer_vector_indexes: bool,
    defer_scalar_indexes: bool,
    batch_size: Option<usize>,
    async_indexes: bool,
    validate_constraints: Option<bool>,
    max_buffer_size_bytes: Option<usize>,
    on_progress: Option<Py<PyAny>>,
}

#[pymethods]
impl AsyncTxBulkWriterBuilder {
    fn defer_vector_indexes(mut slf: PyRefMut<'_, Self>, defer: bool) -> PyRefMut<'_, Self> {
        slf.defer_vector_indexes = defer;
        slf
    }

    fn defer_scalar_indexes(mut slf: PyRefMut<'_, Self>, defer: bool) -> PyRefMut<'_, Self> {
        slf.defer_scalar_indexes = defer;
        slf
    }

    fn batch_size(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.batch_size = Some(size);
        slf
    }

    fn async_indexes(mut slf: PyRefMut<'_, Self>, async_: bool) -> PyRefMut<'_, Self> {
        slf.async_indexes = async_;
        slf
    }

    fn validate_constraints(mut slf: PyRefMut<'_, Self>, validate: bool) -> PyRefMut<'_, Self> {
        slf.validate_constraints = Some(validate);
        slf
    }

    fn max_buffer_size_bytes(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.max_buffer_size_bytes = Some(size);
        slf
    }

    fn on_progress(mut slf: PyRefMut<'_, Self>, callback: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.on_progress = Some(callback);
        slf
    }

    fn build<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let defer_vector = self.defer_vector_indexes;
        let defer_scalar = self.defer_scalar_indexes;
        let batch_size = self.batch_size;
        let async_indexes = self.async_indexes;
        let validate_constraints = self.validate_constraints;
        let max_buffer_size_bytes = self.max_buffer_size_bytes;
        let on_progress = self.on_progress.as_ref().map(|cb| cb.clone_ref(py));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = tx.lock().await;
            let tx_ref = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let mut builder = tx_ref
                .bulk_writer()
                .defer_vector_indexes(defer_vector)
                .defer_scalar_indexes(defer_scalar)
                .async_indexes(async_indexes);
            if let Some(bs) = batch_size {
                builder = builder.batch_size(bs);
            }
            if let Some(vc) = validate_constraints {
                builder = builder.validate_constraints(vc);
            }
            if let Some(mbs) = max_buffer_size_bytes {
                builder = builder.max_buffer_size_bytes(mbs);
            }
            if let Some(callback) = on_progress {
                struct PyProgressWrapper {
                    py_obj: Py<PyAny>,
                }
                unsafe impl Send for PyProgressWrapper {}

                let wrapper = PyProgressWrapper { py_obj: callback };
                builder =
                    builder.on_progress(move |progress: ::uni_db::api::bulk::BulkProgress| {
                        Python::attach(|py| {
                            let py_progress = crate::types::BulkProgress {
                                phase: format!("{:?}", progress.phase),
                                rows_processed: progress.rows_processed,
                                total_rows: progress.total_rows,
                                current_label: progress.current_label.clone(),
                                elapsed_secs: progress.elapsed.as_secs_f64(),
                            };
                            if let Ok(bound) = Py::new(py, py_progress) {
                                let _ = wrapper.py_obj.call1(py, (bound,));
                            }
                        });
                    });
            }
            let real_writer = builder
                .build()
                .map_err(crate::exceptions::anyhow_to_pyerr)?;
            Ok(AsyncBulkWriter {
                inner: Arc::new(std::sync::Mutex::new(Some(real_writer))),
            })
        })
    }
}

// ============================================================================
// AsyncSession
// ============================================================================

/// An async query session with scoped variables.
///
/// Sessions are the primary scope for reads and the factory for transactions.
/// Create via `db.session()`.
#[pyclass]
pub struct AsyncSession {
    pub(crate) inner: Arc<tokio::sync::Mutex<::uni_db::Session>>,
}

#[pymethods]
impl AsyncSession {
    /// Set a session-scoped parameter.
    fn set<'py>(
        &self,
        py: Python<'py>,
        key: String,
        value: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let val = convert::py_object_to_value(py, &value)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            guard.set(key, val);
            Ok(())
        })
    }

    /// Set multiple session-scoped parameters at once.
    fn set_all<'py>(
        &self,
        py: Python<'py>,
        params: HashMap<String, Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in params {
            let val = convert::py_object_to_value(py, &v)?;
            rust_params.insert(k, val);
        }
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            for (k, v) in rust_params {
                guard.set(k, v);
            }
            Ok(())
        })
    }

    /// Get a session-scoped parameter.
    fn get<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            match guard.get(&key) {
                Some(v) => {
                    let cloned = v.clone();
                    Python::attach(|py| {
                        let py_val = convert::value_to_py(py, &cloned)?;
                        Ok(Some(py_val))
                    })
                }
                None => Ok(None),
            }
        })
    }

    /// Execute a query with session variables.
    ///
    /// Returns a `QueryResult` with `.rows`, `.metrics`, `.warnings`, `.columns`.
    #[pyo3(signature = (cypher, params=None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = if let Some(p) = params {
            let mut map = HashMap::new();
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, val);
            }
            Some(map)
        } else {
            None
        };
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let result = if let Some(params) = rust_params {
                let mut builder = guard.query_with(&cypher);
                for (k, v) in params {
                    builder = builder.param(k, v);
                }
                builder
                    .fetch_all()
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            } else {
                guard
                    .query(&cypher)
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            };
            Python::attach(|py| convert::query_result_to_py_class(py, result))
        })
    }

    /// Create a new async transaction for multi-statement writes.
    #[pyo3(signature = (timeout=None))]
    fn tx<'py>(&self, py: Python<'py>, timeout: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = if let Some(secs) = timeout {
                guard
                    .tx_with()
                    .timeout(std::time::Duration::from_secs_f64(secs))
                    .start()
                    .await
            } else {
                guard.tx().await
            }
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(AsyncTransaction {
                inner: Arc::new(tokio::sync::Mutex::new(Some(tx))),
            })
        })
    }

    /// Create a transaction builder for configuring transaction options.
    fn tx_with(&self) -> AsyncTransactionBuilder {
        AsyncTransactionBuilder {
            session: self.inner.clone(),
            timeout_secs: None,
            isolation_level: None,
        }
    }

    /// Get the session ID.
    fn id<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            Ok(guard.id().to_string())
        })
    }

    /// Get session capabilities.
    fn capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let caps = guard.capabilities();
            let write_lease = caps.write_lease.map(|wl| format!("{:?}", wl));
            Ok(crate::types::PySessionCapabilities {
                can_write: caps.can_write,
                can_pin: caps.can_pin,
                isolation: caps.isolation.to_string(),
                has_notifications: caps.has_notifications,
                write_lease,
            })
        })
    }

    /// Evaluate a Locy program within this session (returns awaitable).
    #[pyo3(signature = (program, params=None))]
    fn locy<'py>(
        &self,
        py: Python<'py>,
        program: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = if let Some(p) = params {
            let mut map = HashMap::new();
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, val);
            }
            Some(map)
        } else {
            None
        };
        let inner = self.inner.clone();
        // Locy future is !Send — use spawn_blocking
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let guard = inner.lock().await;
                    if let Some(params) = rust_params {
                        let mut builder = guard.locy_with(&program);
                        for (k, v) in params {
                            builder = builder.param(&k, v);
                        }
                        builder.run().await
                    } else {
                        guard.locy(&program).await
                    }
                })
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| convert::locy_result_to_py_class(py, result))
        })
    }

    /// Register Locy rules for reuse across evaluations in this session.
    fn register_rules<'py>(&self, py: Python<'py>, program: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            guard
                .register_rules(&program)
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Prepare a Cypher query for repeated execution (returns awaitable).
    fn prepare<'py>(&self, py: Python<'py>, cypher: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let prepared = guard
                .prepare(&cypher)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyPreparedQuery {
                inner: std::sync::Mutex::new(prepared),
            })
        })
    }

    /// Explain a query plan without executing.
    ///
    /// Returns a typed `ExplainOutput`.
    fn explain<'py>(&self, py: Python<'py>, cypher: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let output = guard
                .explain(&cypher)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| convert::explain_output_to_py_class(py, output))
        })
    }

    /// Explain a Locy program's evaluation plan.
    ///
    /// Returns a typed `LocyExplainOutput`.
    fn explain_locy<'py>(&self, py: Python<'py>, program: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        // explain_locy is now sync (compile-only, no I/O) — use spawn_blocking
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let guard = inner.lock().await;
                    guard.explain_locy(&program)
                })
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(convert::locy_explain_to_py_class(result))
        })
    }

    /// Profile a query with operator-level statistics.
    ///
    /// Returns `(QueryResult, ProfileOutput)`.
    #[pyo3(signature = (cypher, params=None))]
    fn profile<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = if let Some(p) = params {
            let mut map = HashMap::new();
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, val);
            }
            Some(map)
        } else {
            None
        };
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let (results, profile) = if let Some(params) = rust_params {
                let mut builder = guard.profile_with(&cypher);
                for (k, v) in params {
                    builder = builder.param(k, v);
                }
                builder.run().await
            } else {
                guard.profile(&cypher).await
            }
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                let query_result = convert::query_result_to_py_class(py, results)?;
                let profile_output = convert::profile_output_to_py_class(py, profile)?;
                let tuple = (query_result, profile_output);
                tuple.into_py_any(py)
            })
        })
    }

    /// Clear all registered Locy rules.
    fn clear_rules<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            guard.clear_rules();
            Ok(())
        })
    }

    /// Compile a Locy program without executing it.
    fn compile_locy<'py>(&self, py: Python<'py>, program: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let compiled = guard
                .compile_locy(&program)
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyCompiledProgram { inner: compiled })
        })
    }

    /// Get session metrics.
    fn metrics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let m = guard.metrics();
            Ok(crate::types::PySessionMetrics {
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
            })
        })
    }

    /// Cancel in-progress operations on this session.
    fn cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            guard.cancel();
            Ok(())
        })
    }

    /// Pin this session to a specific snapshot version.
    fn pin_to_version<'py>(
        &self,
        py: Python<'py>,
        snapshot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            guard
                .pin_to_version(&snapshot_id)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Pin this session to a specific timestamp (seconds since epoch).
    fn pin_to_timestamp<'py>(
        &self,
        py: Python<'py>,
        epoch_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let ts = chrono::DateTime::from_timestamp(
                epoch_secs as i64,
                ((epoch_secs.fract()) * 1_000_000_000.0) as u32,
            )
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid timestamp"))?;
            let mut guard = inner.lock().await;
            guard
                .pin_to_timestamp(ts)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Refresh session to latest database version (unpins if pinned).
    fn refresh<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            guard
                .refresh()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Create a cursor-based query for streaming large result sets.
    #[pyo3(signature = (cypher, params=None))]
    fn query_cursor<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = if let Some(p) = params {
            let mut map = HashMap::new();
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, val);
            }
            Some(map)
        } else {
            None
        };
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let cursor = if let Some(params) = rust_params {
                let mut builder = guard.query_with(&cypher);
                for (k, v) in params {
                    builder = builder.param(k, v);
                }
                builder
                    .cursor()
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            } else {
                guard
                    .query_cursor(&cypher)
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            };
            let columns = cursor.columns().to_vec();
            Ok(AsyncQueryCursor {
                cursor: Arc::new(tokio::sync::Mutex::new(Some(cursor))),
                buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
                columns,
            })
        })
    }

    /// Check if this session is pinned to a specific version.
    fn is_pinned<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            Ok(guard.is_pinned())
        })
    }

    /// Prepare a Locy program for repeated execution.
    fn prepare_locy<'py>(&self, py: Python<'py>, program: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let prepared = guard
                .prepare_locy(&program)
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyPreparedLocy {
                inner: std::sync::Mutex::new(prepared),
            })
        })
    }

    /// Register a user-defined function callable from Cypher/Locy.
    fn register_function<'py>(
        &self,
        py: Python<'py>,
        name: String,
        func: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        struct PyUdfWrapper {
            py_obj: Py<PyAny>,
        }
        unsafe impl Send for PyUdfWrapper {}
        unsafe impl Sync for PyUdfWrapper {}

        let wrapper = PyUdfWrapper {
            py_obj: func.clone_ref(py),
        };
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            guard
                .register_function(&name, move |args: &[::uni_db::Value]| {
                    Python::attach(|py| {
                        let py_args: Vec<Py<PyAny>> = args
                            .iter()
                            .map(|v| crate::convert::value_to_py(py, v))
                            .collect::<pyo3::PyResult<Vec<_>>>()
                            .map_err(|e| {
                                uni_common::UniError::Internal(anyhow::anyhow!(
                                    "UDF arg conversion: {}",
                                    e
                                ))
                            })?;
                        let py_list = pyo3::types::PyList::new(py, &py_args).map_err(|e| {
                            uni_common::UniError::Internal(anyhow::anyhow!(
                                "UDF list creation: {}",
                                e
                            ))
                        })?;
                        let result = wrapper.py_obj.call1(py, (py_list,)).map_err(|e| {
                            uni_common::UniError::Internal(anyhow::anyhow!("UDF call: {}", e))
                        })?;
                        crate::convert::py_object_to_value(py, &result).map_err(|e| {
                            uni_common::UniError::Internal(anyhow::anyhow!(
                                "UDF result conversion: {}",
                                e
                            ))
                        })
                    })
                })
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(())
        })
    }

    /// Add a session hook (Python object with optional before_query/after_query/before_commit/after_commit methods).
    fn add_hook<'py>(&self, py: Python<'py>, hook: Py<PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            guard.add_hook(crate::builders::PySessionHook { py_obj: hook });
            Ok(())
        })
    }

    /// Watch for commit notifications (returns an async CommitStream).
    fn watch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let stream = guard.watch();
            Ok(AsyncCommitStream {
                inner: Arc::new(tokio::sync::Mutex::new(Some(stream))),
            })
        })
    }

    /// Create a WatchBuilder for configuring commit notification filters.
    fn watch_with<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let builder = guard.watch_with();
            Ok(crate::types::PyWatchBuilder {
                inner: Some(builder),
            })
        })
    }

    // ── Builder factories (per-operation timeout, cancellation, etc.) ──

    /// Create a query builder with parameter binding, timeout, max_memory, cancellation.
    fn query_with(&self, cypher: &str) -> AsyncSessionQueryBuilder {
        AsyncSessionQueryBuilder {
            inner: self.inner.clone(),
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_memory: None,
            cancellation_token: None,
        }
    }

    /// Create a Locy evaluation builder with parameters, timeout, max_iterations.
    fn locy_with(&self, program: &str) -> AsyncSessionLocyBuilder {
        AsyncSessionLocyBuilder {
            inner: self.inner.clone(),
            program: program.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_iterations: None,
            locy_config: None,
            cancellation_token: None,
        }
    }

    /// Create a profile builder with parameter binding.
    fn profile_with(&self, cypher: &str) -> AsyncSessionProfileBuilder {
        AsyncSessionProfileBuilder {
            inner: self.inner.clone(),
            cypher: cypher.to_string(),
            params: HashMap::new(),
        }
    }

    /// Get the session's cancellation token.
    fn cancellation_token<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            Ok(crate::types::PyCancellationToken {
                inner: guard.cancellation_token(),
            })
        })
    }

    /// Async context manager support.
    fn __aenter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj: Py<PyAny> = slf.into_any().unbind();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(obj) })
    }

    /// Async context manager exit — cancels in-progress operations.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            guard.cancel();
            Ok(false) // Don't suppress exceptions
        })
    }
}

// ============================================================================
// AsyncCommitStream (async iterator)
// ============================================================================

/// An async iterator over commit notifications.
#[pyclass(name = "AsyncCommitStream")]
pub struct AsyncCommitStream {
    pub(crate) inner: Arc<tokio::sync::Mutex<Option<::uni_db::CommitStream>>>,
}

#[pymethods]
impl AsyncCommitStream {
    fn __aiter__(slf: pyo3::PyRef<'_, Self>) -> pyo3::PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let stream = match guard.as_mut() {
                Some(s) => s,
                None => {
                    return Err(PyErr::new::<pyo3::exceptions::PyStopAsyncIteration, _>(""));
                }
            };
            match stream.next().await {
                Some(n) => Ok(crate::types::PyCommitNotification::from(n)),
                None => Err(PyErr::new::<pyo3::exceptions::PyStopAsyncIteration, _>("")),
            }
        })
    }

    /// Close the stream, releasing resources.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            guard.take();
            Ok(())
        })
    }
}

// ============================================================================
// AsyncTransactionBuilder
// ============================================================================

/// Builder for configuring async transaction options.
#[pyclass(name = "AsyncTransactionBuilder")]
pub struct AsyncTransactionBuilder {
    session: Arc<tokio::sync::Mutex<::uni_db::Session>>,
    timeout_secs: Option<f64>,
    isolation_level: Option<String>,
}

#[pymethods]
impl AsyncTransactionBuilder {
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
    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.session.clone();
        let timeout_secs = self.timeout_secs;
        let isolation_level = self.isolation_level.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = session.lock().await;
            let mut builder = guard.tx_with();
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            if let Some(ref level) = isolation_level {
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
            let tx = builder
                .start()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(AsyncTransaction {
                inner: Arc::new(tokio::sync::Mutex::new(Some(tx))),
            })
        })
    }
}

// ============================================================================
// AsyncBulkWriter (wrapping real Rust BulkWriter)
// ============================================================================

/// Async bulk writer for high-throughput data ingestion.
///
/// Uses `std::sync::Mutex` + `spawn_blocking` because `BulkWriter`'s progress
/// callback (`Box<dyn Fn + Send>`) is not `Sync`, so it can't be held across
/// `.await` points in a `tokio::sync::Mutex`.
#[pyclass]
pub struct AsyncBulkWriter {
    inner: Arc<std::sync::Mutex<Option<::uni_db::api::bulk::BulkWriter>>>,
}

#[pymethods]
impl AsyncBulkWriter {
    /// Insert vertices in bulk.
    fn insert_vertices<'py>(
        &self,
        py: Python<'py>,
        label: String,
        vertices: Vec<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_props: Vec<HashMap<String, ::uni_db::Value>> =
            Vec::with_capacity(vertices.len());
        for v in vertices {
            let mut map = HashMap::new();
            for (k, val) in v {
                map.insert(k, convert::py_object_to_value(py, &val)?);
            }
            rust_props.push(map);
        }
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // BulkWriter is !Sync (progress callback), use spawn_blocking
            let vids = tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                let writer = guard.as_mut().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "BulkWriter already completed",
                    )
                })?;
                let rt = tokio::runtime::Handle::current();
                rt.block_on(writer.insert_vertices(&label, rust_props))
                    .map_err(crate::exceptions::anyhow_to_pyerr)
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Ok(vids.into_iter().map(|v| v.as_u64()).collect::<Vec<_>>())
        })
    }

    /// Insert edges in bulk.
    fn insert_edges<'py>(
        &self,
        py: Python<'py>,
        edge_type: String,
        edges: Vec<(u64, u64, HashMap<String, Py<PyAny>>)>,
    ) -> PyResult<Bound<'py, PyAny>> {
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
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                let writer = guard.as_mut().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "BulkWriter already completed",
                    )
                })?;
                let rt = tokio::runtime::Handle::current();
                rt.block_on(writer.insert_edges(&edge_type, rust_edges))
                    .map_err(crate::exceptions::anyhow_to_pyerr)
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Ok(())
        })
    }

    /// Get current bulk load statistics.
    fn stats(&self) -> PyResult<BulkStats> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let writer = guard.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("BulkWriter already completed")
        })?;
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
    }

    /// Get labels that have been written to.
    fn touched_labels(&self) -> PyResult<Vec<String>> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let writer = guard.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("BulkWriter already completed")
        })?;
        Ok(writer.touched_labels())
    }

    /// Get edge types that have been written to.
    fn touched_edge_types(&self) -> PyResult<Vec<String>> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let writer = guard.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("BulkWriter already completed")
        })?;
        Ok(writer.touched_edge_types())
    }

    /// Commit all pending data and rebuild indexes.
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stats = tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                let writer = guard.take().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "BulkWriter already completed",
                    )
                })?;
                let rt = tokio::runtime::Handle::current();
                rt.block_on(writer.commit())
                    .map_err(crate::exceptions::anyhow_to_pyerr)
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Ok(BulkStats {
                vertices_inserted: stats.vertices_inserted,
                edges_inserted: stats.edges_inserted,
                indexes_rebuilt: stats.indexes_rebuilt,
                duration_secs: stats.duration.as_secs_f64(),
                index_build_duration_secs: stats.index_build_duration.as_secs_f64(),
                index_task_ids: stats.index_task_ids.clone(),
                indexes_pending: stats.indexes_pending,
            })
        })
    }

    /// Abort bulk loading and discard uncommitted changes.
    fn abort<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                if let Some(writer) = guard.take() {
                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(writer.abort())
                        .map_err(crate::exceptions::anyhow_to_pyerr)?;
                }
                Ok::<_, PyErr>(())
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Ok(())
        })
    }

    fn __aenter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj: Py<PyAny> = slf.into_any().unbind();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(obj) })
    }

    /// Auto-abort on exception if not committed.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().unwrap();
                if let Some(writer) = guard.take() {
                    let rt = tokio::runtime::Handle::current();
                    let _ = rt.block_on(writer.abort());
                }
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(false)
        })
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
                    .map_err(crate::exceptions::uni_error_to_pyerr)?;
            let columns = cursor.columns().to_vec();
            Ok(AsyncQueryCursor {
                cursor: Arc::new(tokio::sync::Mutex::new(Some(cursor))),
                buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
                columns,
            })
        })
    }

    /// Execute a mutation query and return affected row count (returns awaitable).
    fn execute<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }

        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::execute_with_params_core(&inner, &cypher, rust_params)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Execute the query and fetch all results as a `QueryResult` (returns awaitable).
    fn fetch_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
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
            let result =
                core::query_builder_core(&inner, &cypher, rust_params, timeout_secs, max_memory)
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| convert::query_result_to_py_class(py, result))
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
                    Err(e) => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e)),
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
                let remaining = cursor
                    .collect_remaining()
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?;
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
// Async Session-level Builders (query_with, execute_with, locy_with, profile_with)
// ============================================================================

/// Async builder for parameterized queries on a Session.
#[pyclass(name = "AsyncSessionQueryBuilder")]
pub struct AsyncSessionQueryBuilder {
    inner: Arc<tokio::sync::Mutex<::uni_db::Session>>,
    cypher: String,
    params: HashMap<String, Py<PyAny>>,
    timeout_secs: Option<f64>,
    max_memory: Option<usize>,
    cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl AsyncSessionQueryBuilder {
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

    /// Attach a cancellation token.
    fn cancellation_token(
        mut slf: PyRefMut<'_, Self>,
        token: crate::types::PyCancellationToken,
    ) -> PyRefMut<'_, Self> {
        slf.cancellation_token = Some(token);
        slf
    }

    /// Fetch all results (returns awaitable QueryResult).
    fn fetch_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        let max_memory = self.max_memory;
        let cancel_token = self.cancellation_token.as_ref().map(|ct| ct.inner.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let mut builder = guard.query_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            if let Some(m) = max_memory {
                builder = builder.max_memory(m);
            }
            if let Some(ct) = cancel_token {
                builder = builder.cancellation_token(ct);
            }
            let result = builder
                .fetch_all()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| convert::query_result_to_py_class(py, result))
        })
    }

    /// Fetch the first row or None (returns awaitable).
    fn fetch_one<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        let max_memory = self.max_memory;
        let cancel_token = self.cancellation_token.as_ref().map(|ct| ct.inner.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let mut builder = guard.query_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            if let Some(m) = max_memory {
                builder = builder.max_memory(m);
            }
            if let Some(ct) = cancel_token {
                builder = builder.cancellation_token(ct);
            }
            let result = builder
                .fetch_all()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                let py_result = convert::query_result_to_py_class(py, result)?;
                Ok(py_result.rows.into_iter().next())
            })
        })
    }

    /// Open a streaming cursor (returns awaitable AsyncQueryCursor).
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
        let cancel_token = self.cancellation_token.as_ref().map(|ct| ct.inner.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let mut builder = guard.query_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            if let Some(m) = max_memory {
                builder = builder.max_memory(m);
            }
            if let Some(ct) = cancel_token {
                builder = builder.cancellation_token(ct);
            }
            let cursor = builder
                .cursor()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            let columns = cursor.columns().to_vec();
            Ok(AsyncQueryCursor {
                cursor: Arc::new(tokio::sync::Mutex::new(Some(cursor))),
                buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
                columns,
            })
        })
    }
}

/// Async builder for Locy evaluation on a Session.
#[pyclass(name = "AsyncSessionLocyBuilder")]
pub struct AsyncSessionLocyBuilder {
    inner: Arc<tokio::sync::Mutex<::uni_db::Session>>,
    program: String,
    params: HashMap<String, Py<PyAny>>,
    timeout_secs: Option<f64>,
    max_iterations: Option<usize>,
    locy_config: Option<::uni_locy::LocyConfig>,
    cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl AsyncSessionLocyBuilder {
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

    /// Attach a cancellation token.
    fn cancellation_token(
        mut slf: PyRefMut<'_, Self>,
        token: crate::types::PyCancellationToken,
    ) -> PyRefMut<'_, Self> {
        slf.cancellation_token = Some(token);
        slf
    }

    /// Execute the Locy evaluation (returns awaitable LocyResult).
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }
        let inner = self.inner.clone();
        let program = self.program.clone();
        let timeout_secs = self.timeout_secs;
        let max_iterations = self.max_iterations;
        let locy_config = self.locy_config.clone();
        let cancel_token = self.cancellation_token.as_ref().map(|ct| ct.inner.clone());
        // Locy future is !Send — use spawn_blocking
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let guard = inner.lock().await;
                    let mut builder = guard.locy_with(&program);
                    for (k, v) in rust_params {
                        builder = builder.param(k, v);
                    }
                    if let Some(t) = timeout_secs {
                        builder = builder.timeout(std::time::Duration::from_secs_f64(t));
                    }
                    if let Some(n) = max_iterations {
                        builder = builder.max_iterations(n);
                    }
                    if let Some(c) = locy_config {
                        builder = builder.with_config(c);
                    }
                    if let Some(ct) = cancel_token {
                        builder = builder.cancellation_token(ct);
                    }
                    builder.run().await
                })
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| convert::locy_result_to_py_class(py, result))
        })
    }
}

/// Async builder for profiling on a Session.
#[pyclass(name = "AsyncSessionProfileBuilder")]
pub struct AsyncSessionProfileBuilder {
    inner: Arc<tokio::sync::Mutex<::uni_db::Session>>,
    cypher: String,
    params: HashMap<String, Py<PyAny>>,
}

#[pymethods]
impl AsyncSessionProfileBuilder {
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

    /// Execute and profile the query (returns awaitable (QueryResult, ProfileOutput)).
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let mut builder = guard.profile_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(k, v);
            }
            let (results, profile) = builder
                .run()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                let query_result = convert::query_result_to_py_class(py, results)?;
                let profile_output = convert::profile_output_to_py_class(py, profile)?;
                let tuple = pyo3::types::PyTuple::new(
                    py,
                    [
                        query_result.into_pyobject(py)?.into_any(),
                        profile_output.into_pyobject(py)?.into_any(),
                    ],
                )?;
                Ok(tuple.unbind().into_any())
            })
        })
    }
}

// ============================================================================
// Async Transaction-level Builders (query_with, execute_with, locy_with, apply_with)
// ============================================================================

/// Async builder for parameterized queries on a Transaction.
#[pyclass(name = "AsyncTxQueryBuilder")]
pub struct AsyncTxQueryBuilder {
    inner: Arc<tokio::sync::Mutex<Option<::uni_db::Transaction>>>,
    cypher: String,
    params: HashMap<String, Py<PyAny>>,
    timeout_secs: Option<f64>,
}

#[pymethods]
impl AsyncTxQueryBuilder {
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

    /// Fetch all results (returns awaitable QueryResult).
    fn fetch_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let mut builder = tx.query_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(&k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            let result = builder
                .fetch_all()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| convert::query_result_to_py_class(py, result))
        })
    }

    /// Fetch the first row or None (returns awaitable).
    fn fetch_one<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let mut builder = tx.query_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(&k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            let result = builder
                .fetch_all()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                let py_result = convert::query_result_to_py_class(py, result)?;
                Ok(py_result.rows.into_iter().next())
            })
        })
    }

    /// Execute as a mutation (returns awaitable ExecuteResult).
    fn execute<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let mut builder = tx.execute_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            let result = builder
                .run()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                let py_result = convert::execute_result_to_py(py, result)?;
                Ok(py_result.into_pyobject(py)?.into_any().unbind())
            })
        })
    }

    /// Open a streaming cursor (returns awaitable AsyncQueryCursor).
    fn cursor<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let mut builder = tx.query_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(&k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            let cursor = builder
                .cursor()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            let columns = cursor.columns().to_vec();
            Ok(AsyncQueryCursor {
                cursor: Arc::new(tokio::sync::Mutex::new(Some(cursor))),
                buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
                columns,
            })
        })
    }
}

/// Async builder for parameterized mutations on a Transaction.
#[pyclass(name = "AsyncTxExecuteBuilder")]
pub struct AsyncTxExecuteBuilder {
    inner: Arc<tokio::sync::Mutex<Option<::uni_db::Transaction>>>,
    cypher: String,
    params: HashMap<String, Py<PyAny>>,
    timeout_secs: Option<f64>,
}

#[pymethods]
impl AsyncTxExecuteBuilder {
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

    /// Execute the mutation (returns awaitable ExecuteResult).
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let mut builder = tx.execute_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            let result = builder
                .run()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                let py_result = convert::execute_result_to_py(py, result)?;
                Ok(py_result.into_pyobject(py)?.into_any().unbind())
            })
        })
    }
}

/// Async builder for Locy evaluation on a Transaction.
#[pyclass(name = "AsyncTxLocyBuilder")]
pub struct AsyncTxLocyBuilder {
    inner: Arc<tokio::sync::Mutex<Option<::uni_db::Transaction>>>,
    program: String,
    params: HashMap<String, Py<PyAny>>,
    timeout_secs: Option<f64>,
    max_iterations: Option<usize>,
    locy_config: Option<::uni_locy::LocyConfig>,
    cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl AsyncTxLocyBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
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

    /// Attach a cancellation token.
    fn cancellation_token(
        mut slf: PyRefMut<'_, Self>,
        token: crate::types::PyCancellationToken,
    ) -> PyRefMut<'_, Self> {
        slf.cancellation_token = Some(token);
        slf
    }

    /// Execute the Locy evaluation (returns awaitable LocyResult).
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }
        let inner = self.inner.clone();
        let program = self.program.clone();
        let timeout_secs = self.timeout_secs;
        let max_iterations = self.max_iterations;
        let locy_config = self.locy_config.clone();
        let cancel_token = self.cancellation_token.as_ref().map(|ct| ct.inner.clone());
        // Locy future is !Send — use spawn_blocking
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let guard = inner.lock().await;
                    let tx = guard.as_ref().ok_or_else(|| {
                        crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                            "Transaction already completed",
                        )
                    })?;
                    let mut builder = tx.locy_with(&program);
                    for (k, v) in rust_params {
                        builder = builder.param(k, v);
                    }
                    if let Some(t) = timeout_secs {
                        builder = builder.timeout(std::time::Duration::from_secs_f64(t));
                    }
                    if let Some(n) = max_iterations {
                        builder = builder.max_iterations(n);
                    }
                    if let Some(c) = locy_config {
                        builder = builder.with_config(c);
                    }
                    if let Some(ct) = cancel_token {
                        builder = builder.cancellation_token(ct);
                    }
                    builder
                        .run()
                        .await
                        .map_err(crate::exceptions::uni_error_to_pyerr)
                })
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Python::attach(|py| convert::locy_result_to_py_class(py, result))
        })
    }
}

/// Async builder for applying a DerivedFactSet with staleness controls.
#[pyclass(name = "AsyncApplyBuilder")]
pub struct AsyncApplyBuilder {
    inner: Arc<tokio::sync::Mutex<Option<::uni_db::Transaction>>>,
    derived: Option<uni_locy::DerivedFactSet>,
    require_fresh: bool,
    max_version_gap: Option<u64>,
}

#[pymethods]
impl AsyncApplyBuilder {
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

    /// Execute the apply (returns awaitable ApplyResult).
    fn run<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let dfs = self.derived.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("DerivedFactSet already consumed")
        })?;
        let require_fresh = self.require_fresh;
        let max_version_gap = self.max_version_gap;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = guard.as_ref().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let mut builder = tx.apply_with(dfs);
            if require_fresh {
                builder = builder.require_fresh();
            }
            if let Some(gap) = max_version_gap {
                builder = builder.max_version_gap(gap);
            }
            let result = builder
                .run()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyApplyResult {
                facts_applied: result.facts_applied,
                version_gap: result.version_gap,
            })
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
                .map_err(crate::exceptions::uni_error_to_pyerr)
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
    ///
    /// `index_type` can be a string or a dict with rich config (see `LabelBuilder.index()`).
    fn index<'py>(
        mut slf: PyRefMut<'py, Self>,
        property: String,
        index_type: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        let label = slf.name.clone();
        let idx = crate::builders::parse_index_config(py, &label, &property, &index_type)?;
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
