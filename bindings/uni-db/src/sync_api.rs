// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Synchronous Python API — `Database`, `Transaction`, and `LocyEngine`.

use crate::builders::{BulkWriterBuilder, SchemaBuilder, SessionBuilder};
use crate::convert;
use crate::core;
use crate::types::*;
use ::uni_db::Uni;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

// ============================================================================
// QueryCursor (synchronous)
// ============================================================================

/// Cursor-based result streaming for large query result sets.
///
/// Implements Python's iterator protocol (`__iter__`/`__next__`) and context
/// manager protocol (`__enter__`/`__exit__`).  Rows are yielded one at a time
/// from the underlying batch stream.
#[pyclass]
pub struct QueryCursor {
    pub(crate) cursor: std::sync::Mutex<Option<core::QueryCursor>>,
    pub(crate) buffer: std::sync::Mutex<VecDeque<core::Row>>,
    #[pyo3(get)]
    pub(crate) columns: Vec<String>,
}

impl QueryCursor {
    /// Pull the next single row, refilling from the batch stream as needed.
    fn next_row(&self) -> PyResult<Option<core::Row>> {
        let mut buf = self.buffer.lock().unwrap();
        if let Some(row) = buf.pop_front() {
            return Ok(Some(row));
        }
        // Buffer empty – fetch next batch from cursor.
        let mut guard = self.cursor.lock().unwrap();
        let cursor = match guard.as_mut() {
            Some(c) => c,
            None => return Ok(None), // closed
        };
        let batch = pyo3_async_runtimes::tokio::get_runtime().block_on(cursor.next_batch());
        match batch {
            Some(Ok(rows)) => {
                let mut iter = rows.into_iter();
                let first = iter.next();
                buf.extend(iter);
                Ok(first)
            }
            Some(Err(e)) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                e.to_string(),
            )),
            None => Ok(None),
        }
    }
}

#[pymethods]
impl QueryCursor {
    /// Fetch a single row, or `None` if exhausted.
    fn fetch_one(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        match self.next_row()? {
            Some(row) => {
                let dict = PyDict::new(py);
                for (col, val) in row.as_map() {
                    dict.set_item(col, convert::value_to_py(py, val)?)?;
                }
                Ok(Some(dict.into()))
            }
            None => Ok(None),
        }
    }

    /// Fetch up to `n` rows.
    #[pyo3(signature = (n))]
    fn fetch_many(&self, py: Python, n: usize) -> PyResult<Vec<Py<PyAny>>> {
        let mut result = Vec::with_capacity(n);
        for _ in 0..n {
            match self.next_row()? {
                Some(row) => {
                    let dict = PyDict::new(py);
                    for (col, val) in row.as_map() {
                        dict.set_item(col, convert::value_to_py(py, val)?)?;
                    }
                    result.push(dict.into());
                }
                None => break,
            }
        }
        Ok(result)
    }

    /// Fetch all remaining rows.
    fn fetch_all(&self, py: Python) -> PyResult<Vec<Py<PyAny>>> {
        // Drain buffer first, then collect remaining from cursor.
        let mut rows: Vec<core::Row> = {
            let mut buf = self.buffer.lock().unwrap();
            buf.drain(..).collect()
        };

        let cursor_opt = {
            let mut guard = self.cursor.lock().unwrap();
            guard.take()
        };
        if let Some(cursor) = cursor_opt {
            let remaining = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(cursor.collect_remaining())
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            rows.extend(remaining);
        }

        convert::rows_to_py(py, rows)
    }

    /// Close the cursor, releasing resources.
    fn close(&self) -> PyResult<()> {
        let _ = self.cursor.lock().unwrap().take();
        self.buffer.lock().unwrap().clear();
        Ok(())
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        self.fetch_one(py)
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
        self.close()?;
        Ok(false) // don't suppress exceptions
    }
}

// ============================================================================
// Transaction
// ============================================================================

/// A database transaction for atomic operations.
///
/// Wraps a Rust `Transaction` which provides ACID guarantees.
/// Use as a context manager for automatic rollback on error.
#[pyclass]
pub struct Transaction {
    pub(crate) inner: Option<::uni_db::Transaction>,
}

impl Transaction {
    fn check_active(&self) -> PyResult<&::uni_db::Transaction> {
        self.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })
    }
}

#[pymethods]
impl Transaction {
    /// Execute a read query within this transaction.
    ///
    /// Returns a `QueryResult` with `.rows`, `.metrics`, `.warnings`, `.columns`.
    #[pyo3(signature = (cypher, params=None))]
    fn query(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<crate::types::PyQueryResult> {
        let tx = self.check_active()?;
        let result = if let Some(p) = params {
            let mut builder = tx.query_with(cypher);
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                builder = builder.param(&k, val);
            }
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(builder.fetch_all())
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(tx.query(cypher))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        };
        convert::query_result_to_py_class(py, result)
    }

    /// Execute a mutation query within this transaction.
    #[pyo3(signature = (cypher, params=None))]
    fn execute(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<PyExecuteResult> {
        let tx = self.check_active()?;
        let result = if let Some(p) = params {
            let mut builder = tx.execute_with(cypher);
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                builder = builder.param(&k, val);
            }
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(builder.run())
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(tx.execute(cypher))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        };
        convert::execute_result_to_py(py, result)
    }

    /// Evaluate a Locy program within this transaction.
    #[pyo3(signature = (program, params=None))]
    fn locy(
        &self,
        py: Python,
        program: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<PyLocyResult> {
        let tx = self.check_active()?;
        let result = if let Some(p) = params {
            let mut builder = tx.locy_with(program);
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                builder = builder.param(&k, val);
            }
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(builder.run())
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(tx.locy(program))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        };
        convert::locy_result_to_py_class(py, result)
    }

    /// Apply a DerivedFactSet to this transaction.
    fn apply(&self, _py: Python, derived: &mut PyDerivedFactSet) -> PyResult<PyApplyResult> {
        let tx = self.check_active()?;
        let dfs = derived.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("DerivedFactSet already consumed")
        })?;
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(tx.apply(dfs))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(PyApplyResult {
            facts_applied: result.facts_applied,
            version_gap: result.version_gap,
        })
    }

    /// Register Locy rules for reuse within this transaction.
    fn register_rules(&self, program: &str) -> PyResult<()> {
        let tx = self.check_active()?;
        tx.register_rules(program)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Clear all registered Locy rules in this transaction.
    fn clear_rules(&self) -> PyResult<()> {
        let tx = self.check_active()?;
        tx.clear_rules();
        Ok(())
    }

    /// Cancel in-progress operations.
    fn cancel(&self) -> PyResult<()> {
        let tx = self.check_active()?;
        tx.cancel();
        Ok(())
    }

    /// Prepare a Cypher query for repeated execution within this transaction.
    fn prepare(&self, cypher: &str) -> PyResult<PyPreparedQuery> {
        let tx = self.check_active()?;
        let prepared = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(tx.prepare(cypher))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(PyPreparedQuery {
            inner: std::sync::Mutex::new(prepared),
        })
    }

    /// Prepare a Locy program for repeated execution within this transaction.
    fn prepare_locy(&self, program: &str) -> PyResult<PyPreparedLocy> {
        let tx = self.check_active()?;
        let prepared = tx
            .prepare_locy(program)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(PyPreparedLocy {
            inner: std::sync::Mutex::new(prepared),
        })
    }

    /// Create a query builder for this transaction.
    fn query_with(slf: Py<Self>, cypher: &str) -> crate::builders::PyTxQueryBuilder {
        crate::builders::PyTxQueryBuilder {
            tx: slf,
            cypher: cypher.to_string(),
            params: HashMap::new(),
        }
    }

    /// Create an execute builder for this transaction.
    fn execute_with(slf: Py<Self>, cypher: &str) -> crate::builders::PyTxExecuteBuilder {
        crate::builders::PyTxExecuteBuilder {
            tx: slf,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
        }
    }

    /// Create a Locy builder for this transaction.
    fn locy_with(slf: Py<Self>, program: &str) -> crate::builders::PyTxLocyBuilder {
        crate::builders::PyTxLocyBuilder {
            tx: slf,
            program: program.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_iterations: None,
        }
    }

    /// Create an apply builder for this transaction.
    fn apply_with(
        slf: Py<Self>,
        derived: &mut PyDerivedFactSet,
    ) -> PyResult<crate::builders::PyApplyBuilder> {
        let dfs = derived.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("DerivedFactSet already consumed")
        })?;
        Ok(crate::builders::PyApplyBuilder {
            tx: slf,
            derived: Some(dfs),
            require_fresh: false,
            max_version_gap: None,
        })
    }

    /// Commit the transaction, returning a CommitResult.
    fn commit(&mut self) -> PyResult<PyCommitResult> {
        let tx = self.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(tx.commit())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(PyCommitResult::from(result))
    }

    /// Rollback the transaction.
    fn rollback(&mut self) -> PyResult<()> {
        let tx = self.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        tx.rollback();
        Ok(())
    }

    /// Get the transaction ID.
    fn id(&self) -> PyResult<String> {
        Ok(self.check_active()?.id().to_string())
    }

    /// Check if the transaction has uncommitted changes.
    fn is_dirty(&self) -> PyResult<bool> {
        Ok(self.check_active()?.is_dirty())
    }

    /// Check if the transaction has been completed (committed or rolled back).
    fn is_completed(&self) -> bool {
        self.inner.is_none()
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager exit — rolls back if not committed.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        // If the transaction is still active, roll it back.
        // The Rust Drop impl will also auto-rollback, but explicit is clearer.
        if let Some(tx) = self.inner.take() {
            tx.rollback();
        }
        Ok(false) // don't suppress exceptions
    }
}

// ============================================================================
// Database (main entry point)
// ============================================================================

/// Main entry point for the Uni embedded graph database.
#[pyclass]
pub struct Database {
    pub(crate) inner: Arc<Uni>,
}

#[pymethods]
impl Database {
    // ========================================================================
    // Static Factory Methods
    // ========================================================================

    /// Open or create a database at the given path.
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let uni = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { Uni::open(path).build().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(Database {
            inner: Arc::new(uni),
        })
    }

    /// Create a temporary in-memory database.
    #[staticmethod]
    fn temporary() -> PyResult<Self> {
        let uni = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { Uni::temporary().build().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(Database {
            inner: Arc::new(uni),
        })
    }

    /// Create an in-memory database (alias for temporary).
    #[staticmethod]
    fn in_memory() -> PyResult<Self> {
        Self::temporary()
    }

    /// Create a new database. Fails if it already exists.
    #[staticmethod]
    fn create(path: &str) -> PyResult<Self> {
        let uni = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { Uni::create(path).build().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(Database {
            inner: Arc::new(uni),
        })
    }

    /// Open an existing database. Fails if it does not exist.
    #[staticmethod]
    fn open_existing(path: &str) -> PyResult<Self> {
        let uni = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { Uni::open_existing(path).build().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(Database {
            inner: Arc::new(uni),
        })
    }

    /// Return a DatabaseBuilder for advanced configuration.
    #[staticmethod]
    fn builder() -> crate::builders::DatabaseBuilder {
        crate::builders::DatabaseBuilder::temporary()
    }

    // ========================================================================
    // Context Manager
    // ========================================================================

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
        // Flush on exit to ensure data durability.
        let _ = pyo3_async_runtimes::tokio::get_runtime().block_on(core::flush_core(&self.inner));
        Ok(false)
    }

    fn __repr__(&self) -> String {
        "Database(open)".to_string()
    }

    /// Flush all uncommitted changes to persistent storage.
    fn flush(&self) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::flush_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }

    // ========================================================================
    // Schema Methods
    // ========================================================================

    /// Create a schema builder.
    fn schema(&self) -> SchemaBuilder {
        SchemaBuilder {
            inner: self.inner.clone(),
            pending_labels: Vec::new(),
            pending_edge_types: Vec::new(),
            pending_properties: Vec::new(),
            pending_indexes: Vec::new(),
        }
    }

    /// Create a label.
    fn create_label(&self, name: &str) -> PyResult<u16> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::create_label_core(&self.inner, name))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Create an edge type.
    #[pyo3(signature = (name, from_labels=None, to_labels=None))]
    fn create_edge_type(
        &self,
        name: &str,
        from_labels: Option<Vec<String>>,
        to_labels: Option<Vec<String>>,
    ) -> PyResult<u32> {
        let from = from_labels.unwrap_or_default();
        let to = to_labels.unwrap_or_default();
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::create_edge_type_core(&self.inner, name, from, to))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Add a property to a label or edge type.
    fn add_property(
        &self,
        label_or_type: &str,
        name: &str,
        data_type: &str,
        nullable: bool,
    ) -> PyResult<()> {
        let dt = core::parse_data_type(data_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::add_property_core(
                &self.inner,
                label_or_type,
                name,
                dt,
                nullable,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }

    /// Check if a label exists.
    fn label_exists(&self, name: &str) -> PyResult<bool> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::label_exists_core(&self.inner, name))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Check if an edge type exists.
    fn edge_type_exists(&self, name: &str) -> PyResult<bool> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::edge_type_exists_core(&self.inner, name))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Get all label names.
    fn list_labels(&self) -> PyResult<Vec<String>> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::list_labels_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Get all edge type names.
    fn list_edge_types(&self) -> PyResult<Vec<String>> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::list_edge_types_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Get detailed information about a label.
    fn get_label_info(&self, name: &str) -> PyResult<Option<LabelInfo>> {
        let info = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::get_label_info_core(&self.inner, name))
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
    }

    /// Get the full schema as a dictionary.
    fn get_schema(&self, py: Python) -> PyResult<Py<PyAny>> {
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
    fn load_schema(&self, path: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::load_schema_core(&self.inner, path))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Save schema to a JSON file.
    fn save_schema(&self, path: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::save_schema_core(&self.inner, path))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    // ========================================================================
    // Index Methods
    // ========================================================================

    /// Create a scalar index on a property.
    fn create_scalar_index(&self, label: &str, property: &str, index_type: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::create_scalar_index_core(
                &self.inner,
                label,
                property,
                index_type,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }

    /// Create a vector index on a property.
    fn create_vector_index(&self, label: &str, property: &str, metric: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::create_vector_index_core(
                &self.inner,
                label,
                property,
                metric,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }

    // ========================================================================
    // Session Methods
    // ========================================================================

    /// Create a new session.
    ///
    /// Sessions are the primary scope for reads and the factory for transactions.
    fn session(&self) -> crate::builders::Session {
        crate::builders::Session {
            inner: self.inner.session(),
        }
    }

    /// Create a session builder for setting variables before building.
    fn session_builder(&self) -> SessionBuilder {
        SessionBuilder {
            inner: self.inner.clone(),
            variables: HashMap::new(),
        }
    }

    /// Create a session template builder for pre-configured session factories.
    fn session_template(&self) -> crate::builders::SessionTemplateBuilder {
        crate::builders::SessionTemplateBuilder {
            inner: Some(self.inner.session_template()),
        }
    }

    // ========================================================================
    // Bulk Loading Methods
    // ========================================================================

    /// Create a bulk writer builder.
    fn bulk_writer(&self) -> BulkWriterBuilder {
        BulkWriterBuilder {
            inner: self.inner.clone(),
            defer_vector_indexes: true,
            defer_scalar_indexes: true,
            batch_size: None,
            async_indexes: false,
            validate_constraints: None,
            max_buffer_size_bytes: None,
        }
    }

    /// Register Locy rules at the database level.
    fn register_rules(&self, program: &str) -> PyResult<()> {
        self.inner
            .register_rules(program)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Clear all registered Locy rules at the database level.
    fn clear_rules(&self) {
        self.inner.clear_rules();
    }

    /// Get a Xervo facade for embedding and generation operations.
    fn xervo(&self) -> PyResult<Xervo> {
        Ok(Xervo {
            inner: self.inner.clone(),
        })
    }

    // -----------------------------------------------------------------------
    // Snapshot management
    // -----------------------------------------------------------------------

    /// Create a point-in-time snapshot. Returns the snapshot ID.
    #[pyo3(signature = (name=None))]
    fn create_snapshot(&self, name: Option<String>) -> PyResult<String> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::create_snapshot_core(&self.inner, name))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// List all available snapshots.
    fn list_snapshots(&self, py: Python) -> PyResult<Vec<crate::types::SnapshotInfo>> {
        let manifests = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::list_snapshots_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        manifests
            .into_iter()
            .map(|m| convert::snapshot_manifest_to_py(py, m))
            .collect()
    }

    /// Restore the database to a specific snapshot.
    fn restore_snapshot(&self, snapshot_id: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::restore_snapshot_core(&self.inner, snapshot_id))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    // -----------------------------------------------------------------------
    // Compaction
    // -----------------------------------------------------------------------

    /// Compact a label's storage files.
    fn compact_label(&self, label: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::compact_label_core(&self.inner, label))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Compact an edge type's storage files.
    fn compact_edge_type(&self, edge_type: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::compact_edge_type_core(&self.inner, edge_type))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Wait for any ongoing compaction to complete.
    fn wait_for_compaction(&self) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::wait_for_compaction_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    // -----------------------------------------------------------------------
    // Index administration
    // -----------------------------------------------------------------------

    /// Get status of background index rebuild tasks.
    fn index_rebuild_status(
        &self,
        py: Python,
    ) -> PyResult<Vec<crate::types::IndexRebuildTaskInfo>> {
        let tasks = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::index_rebuild_status_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        tasks
            .into_iter()
            .map(|t| convert::index_rebuild_task_to_py(py, t))
            .collect()
    }

    /// Retry failed index rebuild tasks. Returns task IDs scheduled for retry.
    fn retry_index_rebuilds(&self) -> PyResult<Vec<String>> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::retry_index_rebuilds_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Force rebuild indexes for a label. If background=true, returns a task ID.
    #[pyo3(signature = (label, background=false))]
    fn rebuild_indexes(&self, label: &str, background: bool) -> PyResult<Option<String>> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::rebuild_indexes_core(&self.inner, label, background))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Check if an index is currently being rebuilt for a label.
    fn is_index_building(&self, label: &str) -> PyResult<bool> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::is_index_building_core(&self.inner, label))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// List all indexes defined on a specific label.
    fn list_indexes(
        &self,
        py: Python,
        label: &str,
    ) -> PyResult<Vec<crate::types::IndexDefinitionInfo>> {
        core::list_indexes_core(&self.inner, label)
            .into_iter()
            .map(|i| convert::index_definition_to_py(py, i))
            .collect()
    }

    /// List all indexes in the database.
    fn list_all_indexes(&self, py: Python) -> PyResult<Vec<crate::types::IndexDefinitionInfo>> {
        core::list_all_indexes_core(&self.inner)
            .into_iter()
            .map(|i| convert::index_definition_to_py(py, i))
            .collect()
    }

    /// Flush data and prepare for shutdown.
    ///
    /// Calls `flush()` for data safety. The actual shutdown occurs when the
    /// last reference is dropped (Python GC triggers Rust `Drop`).
    fn shutdown(&self) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.flush())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
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
}

// ============================================================================
// Xervo (synchronous)
// ============================================================================

/// Synchronous facade for Uni-Xervo embedding and generation.
#[pyclass]
pub struct Xervo {
    inner: Arc<Uni>,
}

#[pymethods]
impl Xervo {
    /// Check if Xervo (embedding/generation) is available for this database.
    fn is_available(&self) -> bool {
        self.inner.xervo().is_available()
    }

    /// Embed texts using a configured model alias. Returns a list of float vectors.
    fn embed(&self, alias: &str, texts: Vec<String>) -> PyResult<Vec<Vec<f32>>> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::xervo_embed_core(&self.inner, alias, texts))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Generate text using structured messages. Returns a GenerationResult.
    ///
    /// Each message may be a `Message` instance or a dict with `"role"` and `"content"` keys.
    #[pyo3(signature = (alias, messages, max_tokens=None, temperature=None, top_p=None))]
    fn generate(
        &self,
        py: Python,
        alias: &str,
        messages: Vec<Py<PyAny>>,
        max_tokens: Option<usize>,
        temperature: Option<f32>,
        top_p: Option<f32>,
    ) -> PyResult<crate::types::PyGenerationResult> {
        let msg_pairs = convert::extract_messages(py, messages)?;
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::xervo_generate_core(
                &self.inner,
                alias,
                msg_pairs,
                max_tokens,
                temperature,
                top_p,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        convert::generation_result_to_py(py, result)
    }

    /// Generate text from a single user prompt. Convenience wrapper around `generate()`.
    #[pyo3(signature = (alias, prompt, max_tokens=None, temperature=None, top_p=None))]
    fn generate_text(
        &self,
        py: Python,
        alias: &str,
        prompt: String,
        max_tokens: Option<usize>,
        temperature: Option<f32>,
        top_p: Option<f32>,
    ) -> PyResult<crate::types::PyGenerationResult> {
        let msg_pairs = vec![("user".to_string(), prompt)];
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::xervo_generate_core(
                &self.inner,
                alias,
                msg_pairs,
                max_tokens,
                temperature,
                top_p,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        convert::generation_result_to_py(py, result)
    }
}
