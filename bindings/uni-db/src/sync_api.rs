// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Synchronous Python API — `Database`, `Transaction`, and `LocyEngine`.

use crate::builders::{BulkWriterBuilder, QueryBuilder, SchemaBuilder, SessionBuilder};
use crate::convert;
use crate::core;
use crate::types::*;
use ::uni_db::Uni;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
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
#[pyclass]
pub struct Transaction {
    pub(crate) inner: Arc<Uni>,
    pub(crate) completed: bool,
}

#[pymethods]
impl Transaction {
    /// Execute a read query within this transaction.
    #[pyo3(signature = (cypher, params=None))]
    fn query(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        let rust_params = convert::prepare_params(py, params)?;

        let rows = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::query_core(&self.inner, cypher, rust_params))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        convert::rows_to_py(py, rows.rows)
    }

    /// Execute a mutation query within this transaction.
    #[pyo3(signature = (cypher, params=None))]
    fn execute(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<usize> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        let rust_params = convert::prepare_params(py, params)?;
        if rust_params.is_empty() {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::execute_core(&self.inner, cypher))
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::execute_with_params_core(
                    &self.inner,
                    cypher,
                    rust_params,
                ))
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        }
    }

    /// Create a query builder for this transaction.
    fn query_with(&self, cypher: &str) -> PyResult<QueryBuilder> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        Ok(QueryBuilder {
            inner: self.inner.clone(),
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_memory: None,
        })
    }

    /// Commit the transaction.
    fn commit(&mut self) -> PyResult<()> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::commit_transaction_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        self.completed = true;
        Ok(())
    }

    /// Rollback the transaction.
    fn rollback(&mut self) -> PyResult<()> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::rollback_transaction_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        self.completed = true;
        Ok(())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager exit — rolls back on error.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        if !self.completed {
            self.completed = true;
            // Rollback if exception, already handled externally otherwise
            let _ = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::rollback_transaction_core(&self.inner));
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

    // ========================================================================
    // Query Methods
    // ========================================================================

    /// Execute a Cypher query and return results.
    #[pyo3(signature = (cypher, params=None, timeout=None))]
    fn query(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
        timeout: Option<f64>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let rust_params = convert::prepare_params(py, params)?;
        if timeout.is_some() {
            let rows = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::query_builder_core(
                    &self.inner,
                    cypher,
                    rust_params,
                    timeout,
                    None,
                ))
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            convert::rows_to_py(py, rows.rows)
        } else {
            let rows = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::query_core(&self.inner, cypher, rust_params))
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            convert::rows_to_py(py, rows.rows)
        }
    }

    /// Open a streaming cursor for a query.
    #[pyo3(signature = (cypher, params=None))]
    fn query_cursor(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<QueryCursor> {
        let rust_params = convert::prepare_params(py, params)?;
        let cursor = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::query_cursor_core(
                &self.inner,
                cypher,
                rust_params,
                None,
                None,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        let columns = cursor.columns().to_vec();
        Ok(QueryCursor {
            cursor: std::sync::Mutex::new(Some(cursor)),
            buffer: std::sync::Mutex::new(VecDeque::new()),
            columns,
        })
    }

    /// Create a query builder for parameterized queries.
    fn query_with(&self, cypher: &str) -> QueryBuilder {
        QueryBuilder {
            inner: self.inner.clone(),
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_memory: None,
        }
    }

    /// Create a query builder for mutation queries (alias for query_with).
    fn execute_with(&self, cypher: &str) -> QueryBuilder {
        self.query_with(cypher)
    }

    /// Execute a mutation query, returning affected row count.
    #[pyo3(signature = (cypher, params=None, timeout=None))]
    fn execute(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
        timeout: Option<f64>,
    ) -> PyResult<usize> {
        let rust_params = convert::prepare_params(py, params)?;
        if timeout.is_some() {
            // Use builder core for timeout support
            let rows = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::query_builder_core(
                    &self.inner,
                    cypher,
                    rust_params,
                    timeout,
                    None,
                ))
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Ok(rows.rows.len())
        } else if rust_params.is_empty() {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::execute_core(&self.inner, cypher))
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::execute_with_params_core(
                    &self.inner,
                    cypher,
                    rust_params,
                ))
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        }
    }

    /// Explain the query plan without executing.
    fn explain(&self, py: Python, cypher: &str) -> PyResult<Py<PyAny>> {
        let output = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::explain_core(&self.inner, cypher))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

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

        Ok(dict.into())
    }

    /// Profile query execution with operator-level statistics.
    fn profile(&self, py: Python, cypher: &str) -> PyResult<(Vec<Py<PyAny>>, Py<PyAny>)> {
        let (results, profile) = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::profile_core(&self.inner, cypher))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

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

        Ok((rows, profile_dict.into()))
    }

    // ========================================================================
    // Transaction Methods
    // ========================================================================

    /// Begin a new transaction.
    fn begin(&self) -> PyResult<Transaction> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::begin_transaction_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        Ok(Transaction {
            inner: self.inner.clone(),
            completed: false,
        })
    }

    /// Execute a closure within a transaction (auto-commit on success, auto-rollback on error).
    fn transaction(&self, py: Python, func: Py<PyAny>) -> PyResult<Py<PyAny>> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::begin_transaction_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        let tx = Transaction {
            inner: self.inner.clone(),
            completed: false,
        };
        let tx_obj = Py::new(py, tx)?;

        match func.call1(py, (tx_obj.clone_ref(py),)) {
            Ok(result) => {
                // Auto-commit on success
                let mut tx_ref = tx_obj.borrow_mut(py);
                if !tx_ref.completed {
                    tx_ref.completed = true;
                    pyo3_async_runtimes::tokio::get_runtime()
                        .block_on(core::commit_transaction_core(&self.inner))
                        .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
                }
                Ok(result)
            }
            Err(e) => {
                // Auto-rollback on error
                let mut tx_ref = tx_obj.borrow_mut(py);
                if !tx_ref.completed {
                    tx_ref.completed = true;
                    let _ = pyo3_async_runtimes::tokio::get_runtime()
                        .block_on(core::rollback_transaction_core(&self.inner));
                }
                Err(e)
            }
        }
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

    /// Create a session builder.
    fn session(&self) -> SessionBuilder {
        SessionBuilder {
            inner: self.inner.clone(),
            variables: HashMap::new(),
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
            batch_size: 10_000,
            async_indexes: false,
        }
    }

    // ========================================================================
    // Locy
    // ========================================================================

    /// Get the Locy evaluation engine.
    fn locy(&self) -> LocyEngine {
        LocyEngine {
            inner: self.inner.clone(),
        }
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
}

// ============================================================================
// LocyEngine (synchronous)
// ============================================================================

/// Synchronous Locy evaluation engine.
#[pyclass]
pub struct LocyEngine {
    pub(crate) inner: Arc<Uni>,
}

#[pymethods]
impl LocyEngine {
    /// Evaluate a Locy program with optional params and config.
    #[pyo3(signature = (program, params=None, config=None))]
    fn evaluate(
        &self,
        py: Python,
        program: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
        config: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<crate::types::PyLocyResult> {
        let result = if config.is_some() || params.is_some() {
            let mut locy_config = config
                .map(|cfg| convert::extract_locy_config(py, cfg))
                .transpose()?
                .unwrap_or_default();
            if let Some(p) = params {
                locy_config.params = convert::prepare_params(py, Some(p))?;
            }
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::locy_evaluate_with_config_core(
                    &self.inner,
                    program,
                    locy_config,
                ))
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::locy_evaluate_core(&self.inner, program))
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?
        };
        convert::locy_result_to_py_class(py, result)
    }

    /// Create a builder for complex Locy evaluation configuration.
    fn evaluate_with(&self, program: &str) -> crate::builders::LocyBuilder {
        crate::builders::LocyBuilder {
            inner: self.inner.clone(),
            program: program.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_iterations: None,
            config: None,
        }
    }

    /// Register Locy rules for reuse across multiple evaluate calls.
    fn register(&self, program: &str) -> PyResult<()> {
        self.inner
            .locy()
            .register(program)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Clear all registered Locy rules.
    fn clear_registry(&self) -> PyResult<()> {
        self.inner.locy().clear_registry();
        Ok(())
    }

    /// Compile a Locy program without executing it.
    fn compile_only(&self, program: &str) -> PyResult<crate::types::PyCompiledProgram> {
        let compiled = core::locy_compile_only_core(&self.inner, program)
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(crate::types::PyCompiledProgram { inner: compiled })
    }

    /// Explain a Locy program's evaluation plan.
    #[pyo3(signature = (program, params=None))]
    fn explain(
        &self,
        py: Python,
        program: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<crate::types::PyLocyResult> {
        let result = if let Some(p) = params {
            let config = ::uni_db::locy::LocyConfig {
                params: convert::prepare_params(py, Some(p))?,
                ..Default::default()
            };
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::locy_evaluate_with_config_core(
                    &self.inner,
                    program,
                    config,
                ))
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::locy_evaluate_core(&self.inner, program))
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?
        };
        convert::locy_result_to_py_class(py, result)
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
