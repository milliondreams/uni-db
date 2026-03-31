// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Synchronous Python API — `Database`, `Transaction`, and `LocyEngine`.

use crate::builders::SchemaBuilder;
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
            Some(Err(e)) => Err(crate::exceptions::uni_error_to_pyerr(e)),
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
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
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
            crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                "Transaction already completed",
            )
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
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(tx.query(cypher))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
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
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(tx.execute(cypher))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
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
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(tx.locy(program))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
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
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(PyApplyResult {
            facts_applied: result.facts_applied,
            version_gap: result.version_gap,
        })
    }

    /// Access the transaction-scoped rule registry.
    fn rules(&self) -> PyResult<PyRuleRegistry> {
        let tx = self.check_active()?;
        Ok(PyRuleRegistry {
            registry: tx.rules().clone_registry_arc(),
        })
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
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(PyPreparedQuery {
            inner: std::sync::Mutex::new(prepared),
        })
    }

    /// Prepare a Locy program for repeated execution within this transaction.
    fn prepare_locy(&self, program: &str) -> PyResult<PyPreparedLocy> {
        let tx = self.check_active()?;
        let prepared = tx
            .prepare_locy(program)
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
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
            timeout_secs: None,
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
            locy_config: None,
            cancellation_token: None,
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
            crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                "Transaction already completed",
            )
        })?;
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(tx.commit())
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(PyCommitResult::from(result))
    }

    /// Rollback the transaction.
    fn rollback(&mut self) -> PyResult<()> {
        let tx = self.inner.take().ok_or_else(|| {
            crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                "Transaction already completed",
            )
        })?;
        tx.rollback();
        Ok(())
    }

    /// Get the transaction ID.
    fn id(&self) -> PyResult<String> {
        Ok(self.check_active()?.id().to_string())
    }

    /// Database version when this transaction was started.
    fn started_at_version(&self) -> PyResult<u64> {
        Ok(self.check_active()?.started_at_version())
    }

    /// Check if the transaction has uncommitted changes.
    fn is_dirty(&self) -> PyResult<bool> {
        Ok(self.check_active()?.is_dirty())
    }

    /// Check if the transaction has been completed (committed or rolled back).
    fn is_completed(&self) -> bool {
        self.inner.is_none()
    }

    /// Get a cancellation token for this transaction.
    fn cancellation_token(&self) -> PyResult<crate::types::PyCancellationToken> {
        let tx = self.check_active()?;
        Ok(crate::types::PyCancellationToken {
            inner: tx.cancellation_token(),
        })
    }

    /// Create a bulk writer builder for high-throughput data ingestion.
    fn bulk_writer(slf: Py<Self>) -> TxBulkWriterBuilder {
        TxBulkWriterBuilder {
            tx: slf,
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
    fn appender(&self, label: &str) -> PyResult<crate::builders::StreamingAppender> {
        let tx = self.check_active()?;
        let builder = tx.appender(label);
        let appender = builder
            .build()
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(crate::builders::StreamingAppender {
            inner: std::sync::Mutex::new(Some(appender)),
        })
    }

    /// Create a configurable appender builder for the given label.
    fn appender_builder(slf: Py<Self>, label: &str) -> TxAppenderBuilder {
        TxAppenderBuilder {
            tx: slf,
            label: label.to_string(),
            batch_size: None,
            defer_vector_indexes: None,
            max_buffer_size_bytes: None,
        }
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
// Transaction Bulk Writer Builder
// ============================================================================

/// Builder for configuring bulk data loading within a transaction.
#[pyclass(name = "TxBulkWriterBuilder")]
pub struct TxBulkWriterBuilder {
    tx: Py<Transaction>,
    defer_vector_indexes: bool,
    defer_scalar_indexes: bool,
    batch_size: Option<usize>,
    async_indexes: bool,
    validate_constraints: Option<bool>,
    max_buffer_size_bytes: Option<usize>,
    on_progress: Option<Py<PyAny>>,
}

#[pymethods]
impl TxBulkWriterBuilder {
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

    fn build(&self, py: Python) -> PyResult<crate::builders::BulkWriter> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.check_active()?;
        let mut builder = tx.bulk_writer();
        builder = builder
            .defer_vector_indexes(self.defer_vector_indexes)
            .defer_scalar_indexes(self.defer_scalar_indexes)
            .async_indexes(self.async_indexes);
        if let Some(bs) = self.batch_size {
            builder = builder.batch_size(bs);
        }
        if let Some(vc) = self.validate_constraints {
            builder = builder.validate_constraints(vc);
        }
        if let Some(mbs) = self.max_buffer_size_bytes {
            builder = builder.max_buffer_size_bytes(mbs);
        }
        if let Some(ref callback) = self.on_progress {
            struct PyProgressWrapper {
                py_obj: Py<PyAny>,
            }
            unsafe impl Send for PyProgressWrapper {}

            let wrapper = PyProgressWrapper {
                py_obj: callback.clone_ref(py),
            };
            builder = builder.on_progress(move |progress: ::uni_db::api::bulk::BulkProgress| {
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
        Ok(crate::builders::BulkWriter {
            inner: std::sync::Mutex::new(Some(real_writer)),
        })
    }
}

// ============================================================================
// Transaction Appender Builder
// ============================================================================

/// Builder for configuring a StreamingAppender within a transaction.
#[pyclass(name = "TxAppenderBuilder")]
pub struct TxAppenderBuilder {
    tx: Py<Transaction>,
    label: String,
    batch_size: Option<usize>,
    defer_vector_indexes: Option<bool>,
    max_buffer_size_bytes: Option<usize>,
}

#[pymethods]
impl TxAppenderBuilder {
    fn batch_size(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.batch_size = Some(size);
        slf
    }

    fn defer_vector_indexes(mut slf: PyRefMut<'_, Self>, defer: bool) -> PyRefMut<'_, Self> {
        slf.defer_vector_indexes = Some(defer);
        slf
    }

    fn max_buffer_size_bytes(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.max_buffer_size_bytes = Some(size);
        slf
    }

    fn build(&self, py: Python) -> PyResult<crate::builders::StreamingAppender> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.check_active()?;
        let mut builder = tx.appender(&self.label);
        if let Some(bs) = self.batch_size {
            builder = builder.batch_size(bs);
        }
        if let Some(dvi) = self.defer_vector_indexes {
            builder = builder.defer_vector_indexes(dvi);
        }
        if let Some(mbs) = self.max_buffer_size_bytes {
            builder = builder.max_buffer_size_bytes(mbs);
        }
        let appender = builder
            .build()
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(crate::builders::StreamingAppender {
            inner: std::sync::Mutex::new(Some(appender)),
        })
    }
}

// ============================================================================
// Database (main entry point)
// ============================================================================

/// Main entry point for the Uni embedded graph database.
#[pyclass(name = "Uni")]
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
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(Database {
            inner: Arc::new(uni),
        })
    }

    /// Create a temporary in-memory database.
    #[staticmethod]
    fn temporary() -> PyResult<Self> {
        let uni = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { Uni::temporary().build().await })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
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
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(Database {
            inner: Arc::new(uni),
        })
    }

    /// Open an existing database. Fails if it does not exist.
    #[staticmethod]
    fn open_existing(path: &str) -> PyResult<Self> {
        let uni = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { Uni::open_existing(path).build().await })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
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
        // Shutdown on exit.
        let _ = self.shutdown();
        Ok(false)
    }

    fn __repr__(&self) -> String {
        "Uni(open)".to_string()
    }

    /// Flush all uncommitted changes to persistent storage.
    fn flush(&self) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::flush_core(&self.inner))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
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

    /// Check if a label exists.
    fn label_exists(&self, name: &str) -> PyResult<bool> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::label_exists_core(&self.inner, name))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Check if an edge type exists.
    fn edge_type_exists(&self, name: &str) -> PyResult<bool> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::edge_type_exists_core(&self.inner, name))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Get all label names.
    fn list_labels(&self) -> PyResult<Vec<String>> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::list_labels_core(&self.inner))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Get all edge type names.
    fn list_edge_types(&self) -> PyResult<Vec<String>> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::list_edge_types_core(&self.inner))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Get detailed information about a label.
    fn get_label_info(&self, name: &str) -> PyResult<Option<LabelInfo>> {
        let info = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::get_label_info_core(&self.inner, name))
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
    }

    /// Get detailed information about an edge type.
    fn get_edge_type_info(&self, name: &str) -> PyResult<Option<crate::types::EdgeTypeInfo>> {
        let info = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::get_edge_type_info_core(&self.inner, name))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;

        Ok(info.map(|i| crate::types::EdgeTypeInfo {
            name: i.name,
            count: i.count,
            source_labels: i.source_labels,
            target_labels: i.target_labels,
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

    /// Load schema from a JSON file.
    fn load_schema(&self, path: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::load_schema_core(&self.inner, path))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Save schema to a JSON file.
    fn save_schema(&self, path: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::save_schema_core(&self.inner, path))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    // ========================================================================
    // Index Methods
    // ========================================================================

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

    /// Create a session template builder for pre-configured session factories.
    fn session_template(&self) -> crate::builders::SessionTemplateBuilder {
        crate::builders::SessionTemplateBuilder {
            inner: Some(self.inner.session_template()),
        }
    }

    /// Access the rule registry for managing pre-compiled Locy rules.
    fn rules(&self) -> PyRuleRegistry {
        PyRuleRegistry {
            registry: self.inner.rules().clone_registry_arc(),
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
    fn create_snapshot(&self, name: &str) -> PyResult<String> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::create_snapshot_core(&self.inner, name))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// List all available snapshots.
    fn list_snapshots(&self, py: Python) -> PyResult<Vec<crate::types::SnapshotInfo>> {
        let manifests = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::list_snapshots_core(&self.inner))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        manifests
            .into_iter()
            .map(|m| convert::snapshot_manifest_to_py(py, m))
            .collect()
    }

    /// Restore the database to a specific snapshot.
    fn restore_snapshot(&self, snapshot_id: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::restore_snapshot_core(&self.inner, snapshot_id))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Access compaction operations.
    fn compaction(&self) -> PyCompaction {
        PyCompaction {
            inner: self.inner.clone(),
        }
    }

    /// Access index management operations.
    fn indexes(&self) -> PyIndexes {
        PyIndexes {
            inner: self.inner.clone(),
        }
    }

    /// Flush data and prepare for shutdown.
    ///
    /// Calls `flush()` for data safety. The actual shutdown occurs when the
    /// last reference is dropped (Python GC triggers Rust `Drop`).
    fn shutdown(&self) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(self.inner.flush())
            .map_err(crate::exceptions::uni_error_to_pyerr)
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
            .map_err(crate::exceptions::uni_error_to_pyerr)
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
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
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
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::generation_result_to_py(py, result)
    }
}

// ============================================================================
// Rule Registry (synchronous)
// ============================================================================

/// Facade for managing pre-compiled Locy rules.
///
/// Obtained via `db.rules()`, `session.rules()`, or `tx.rules()`.
#[pyclass(name = "RuleRegistry")]
pub struct PyRuleRegistry {
    pub(crate) registry: std::sync::Arc<std::sync::RwLock<uni_db::LocyRuleRegistry>>,
}

#[pymethods]
impl PyRuleRegistry {
    /// Register Locy rules from a program string.
    fn register(&self, program: &str) -> PyResult<()> {
        let facade = uni_db::RuleRegistry::new(&self.registry);
        facade
            .register(program)
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Remove a rule by name. Returns True if found and removed.
    fn remove(&self, name: &str) -> PyResult<bool> {
        let facade = uni_db::RuleRegistry::new(&self.registry);
        facade
            .remove(name)
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// List names of all registered rules.
    fn list(&self) -> Vec<String> {
        let facade = uni_db::RuleRegistry::new(&self.registry);
        facade.list()
    }

    /// Get metadata about a registered rule.
    fn get(&self, name: &str) -> Option<crate::types::PyRuleInfo> {
        let facade = uni_db::RuleRegistry::new(&self.registry);
        facade.get(name).map(|info| crate::types::PyRuleInfo {
            name: info.name,
            clause_count: info.clause_count,
            is_recursive: info.is_recursive,
        })
    }

    /// Clear all registered rules.
    fn clear(&self) {
        let facade = uni_db::RuleRegistry::new(&self.registry);
        facade.clear();
    }

    /// Get the number of registered rules.
    fn count(&self) -> usize {
        let facade = uni_db::RuleRegistry::new(&self.registry);
        facade.count()
    }
}

// ============================================================================
// Compaction (synchronous)
// ============================================================================

/// Facade for compaction operations.
///
/// Obtained via `db.compaction()`.
#[pyclass(name = "Compaction")]
pub struct PyCompaction {
    inner: Arc<Uni>,
}

#[pymethods]
impl PyCompaction {
    /// Compact data for a label or edge type.
    fn compact(&self, name: &str) -> PyResult<crate::types::PyCompactionStats> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::compact_core(&self.inner, name))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Wait for all background compaction to complete.
    fn wait(&self) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::wait_for_compaction_core(&self.inner))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }
}

// ============================================================================
// Indexes (synchronous)
// ============================================================================

/// Facade for index management operations.
///
/// Obtained via `db.indexes()`.
#[pyclass(name = "Indexes")]
pub struct PyIndexes {
    inner: Arc<Uni>,
}

#[pymethods]
impl PyIndexes {
    /// List index definitions, optionally filtered by label.
    #[pyo3(signature = (label=None))]
    fn list(
        &self,
        py: Python,
        label: Option<&str>,
    ) -> PyResult<Vec<crate::types::IndexDefinitionInfo>> {
        let indexes = match label {
            Some(l) => core::list_indexes_core(&self.inner, l),
            None => core::list_all_indexes_core(&self.inner),
        };
        indexes
            .into_iter()
            .map(|i| convert::index_definition_to_py(py, i))
            .collect()
    }

    /// Rebuild indexes for a label. If background=true, returns a task ID.
    #[pyo3(signature = (label, background=false))]
    fn rebuild(&self, label: &str, background: bool) -> PyResult<Option<String>> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::rebuild_indexes_core(&self.inner, label, background))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Get status of all rebuild tasks.
    fn rebuild_status(&self, py: Python) -> PyResult<Vec<crate::types::IndexRebuildTaskInfo>> {
        let tasks = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::index_rebuild_status_core(&self.inner))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        tasks
            .into_iter()
            .map(|t| convert::index_rebuild_task_to_py(py, t))
            .collect()
    }

    /// Retry failed rebuild tasks. Returns task IDs scheduled for retry.
    fn retry_failed(&self) -> PyResult<Vec<String>> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::retry_index_rebuilds_core(&self.inner))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }
}
