// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Python bindings for the Uni embedded graph database.
//!
//! This module provides PyO3-based Python bindings that expose the Uni graph
//! database API to Python applications. It includes both synchronous and
//! asynchronous database management, query execution, schema management,
//! and bulk loading.

pub mod async_api;
pub mod btic;
pub mod builders;
pub mod convert;
pub mod core;
pub mod exceptions;
pub mod sync_api;
pub mod types;

use pyo3::prelude::*;

/// Python module for the Uni embedded graph database.
#[pymodule]
fn _uni_db(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialise the tokio runtime used by pyo3-async-runtimes with an 8 MB
    // worker-thread stack.  The query executor builds deeply nested async state
    // machines that overflow the default 2 MB stack in debug builds.
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_stack_size(8 * 1024 * 1024);
    pyo3_async_runtimes::tokio::init(builder);

    // Sync main classes
    m.add_class::<sync_api::Database>()?;
    m.add_class::<sync_api::Xervo>()?;
    m.add_class::<sync_api::PyRuleRegistry>()?;
    m.add_class::<sync_api::PyCompaction>()?;
    m.add_class::<sync_api::PyIndexes>()?;
    m.add_class::<sync_api::PyParams>()?;
    m.add_class::<builders::DatabaseBuilder>()?;
    m.add_class::<sync_api::Transaction>()?;

    m.add_class::<sync_api::QueryCursor>()?;

    // Schema
    m.add_class::<builders::SchemaBuilder>()?;
    m.add_class::<builders::LabelBuilder>()?;
    m.add_class::<builders::EdgeTypeBuilder>()?;

    // Session
    m.add_class::<builders::Session>()?;

    // Session builders
    m.add_class::<builders::SessionQueryBuilder>()?;
    m.add_class::<builders::SessionLocyBuilder>()?;
    m.add_class::<builders::PyTransactionBuilder>()?;

    // Transaction builders
    m.add_class::<builders::PyTxQueryBuilder>()?;
    m.add_class::<builders::PyTxExecuteBuilder>()?;
    m.add_class::<builders::PyTxLocyBuilder>()?;
    m.add_class::<builders::PyApplyBuilder>()?;

    // Bulk loading
    m.add_class::<builders::BulkWriter>()?;

    // Async classes
    m.add_class::<async_api::AsyncDatabase>()?;
    m.add_class::<async_api::AsyncXervo>()?;
    m.add_class::<async_api::AsyncCompaction>()?;
    m.add_class::<async_api::AsyncIndexes>()?;
    m.add_class::<async_api::AsyncDatabaseBuilder>()?;
    m.add_class::<async_api::AsyncTransaction>()?;
    m.add_class::<async_api::AsyncSession>()?;
    m.add_class::<async_api::AsyncBulkWriter>()?;
    m.add_class::<async_api::AsyncTransactionBuilder>()?;
    m.add_class::<async_api::AsyncQueryBuilder>()?;
    m.add_class::<async_api::AsyncQueryCursor>()?;
    m.add_class::<async_api::AsyncSchemaBuilder>()?;
    m.add_class::<async_api::AsyncLabelBuilder>()?;
    m.add_class::<async_api::AsyncEdgeTypeBuilder>()?;

    // Data classes
    m.add_class::<types::LabelInfo>()?;
    m.add_class::<types::PropertyInfo>()?;
    m.add_class::<types::IndexInfo>()?;
    m.add_class::<types::ConstraintInfo>()?;
    m.add_class::<types::BulkStats>()?;
    m.add_class::<types::BulkProgress>()?;

    // Graph element types
    m.add_class::<types::PyNode>()?;
    m.add_class::<types::PyEdge>()?;
    m.add_class::<types::PyPath>()?;
    m.add_class::<types::LocyStats>()?;
    m.add_class::<types::PyLocyResult>()?;
    m.add_class::<types::PyCompiledProgram>()?;

    // Query result types (Phase 1)
    m.add_class::<types::PyQueryResult>()?;
    m.add_class::<types::PyQueryMetrics>()?;
    m.add_class::<types::PyQueryWarning>()?;
    m.add_class::<types::PyExplainOutput>()?;
    m.add_class::<types::PyProfileOutput>()?;
    m.add_class::<types::PyLocyExplainOutput>()?;

    // New result types
    m.add_class::<types::PyExecuteResult>()?;
    m.add_class::<types::PyApplyResult>()?;
    m.add_class::<types::PyDerivedFactSet>()?;
    m.add_class::<types::PySessionMetrics>()?;

    // Xervo types
    m.add_class::<types::PyMessage>()?;
    m.add_class::<types::PyTokenUsage>()?;
    m.add_class::<types::PyGenerationResult>()?;

    // Snapshot & index types
    m.add_class::<types::SnapshotInfo>()?;
    m.add_class::<types::IndexRebuildTaskInfo>()?;
    m.add_class::<types::IndexDefinitionInfo>()?;

    // Rules
    m.add_class::<types::PyRuleInfo>()?;

    // Edge type info
    m.add_class::<types::EdgeTypeInfo>()?;

    // Compaction
    m.add_class::<types::PyCompactionStats>()?;

    // Session/Transaction result types
    m.add_class::<types::PyRulePromotionError>()?;
    m.add_class::<types::PyCommitResult>()?;
    m.add_class::<types::PyCommitNotification>()?;
    m.add_class::<types::PySessionCapabilities>()?;
    m.add_class::<types::PyPreparedQuery>()?;
    m.add_class::<types::PyPreparedLocy>()?;
    m.add_class::<types::PyDatabaseMetrics>()?;
    m.add_class::<types::PyCommitStream>()?;
    m.add_class::<types::PyWatchBuilder>()?;

    // Async commit stream
    m.add_class::<async_api::AsyncCommitStream>()?;

    // Async session/transaction builders
    m.add_class::<async_api::AsyncSessionQueryBuilder>()?;
    m.add_class::<async_api::AsyncSessionLocyBuilder>()?;
    m.add_class::<async_api::AsyncTxQueryBuilder>()?;
    m.add_class::<async_api::AsyncTxExecuteBuilder>()?;
    m.add_class::<async_api::AsyncTxLocyBuilder>()?;
    m.add_class::<async_api::AsyncApplyBuilder>()?;

    // Session templates & streaming appender
    m.add_class::<builders::SessionTemplateBuilder>()?;
    m.add_class::<builders::SessionTemplate>()?;
    m.add_class::<builders::StreamingAppender>()?;

    // Phase B: New Python types
    m.add_class::<types::PyCancellationToken>()?;
    m.add_class::<types::PyPreparedQueryBinder>()?;
    m.add_class::<types::PyPreparedLocyBinder>()?;
    m.add_class::<types::PyWriteLease>()?;

    // Transaction bulk writer/appender builders
    m.add_class::<sync_api::TxBulkWriterBuilder>()?;
    m.add_class::<sync_api::TxAppenderBuilder>()?;
    m.add_class::<async_api::AsyncTxBulkWriterBuilder>()?;

    // Type system enums
    m.add_class::<types::PyDataType>()?;
    m.add_class::<types::PyCrdtType>()?;

    // ID types
    m.add_class::<types::PyVid>()?;
    m.add_class::<types::PyEid>()?;
    m.add_class::<types::PyUniId>()?;

    // BTIC temporal interval
    m.add_class::<btic::PyBtic>()?;

    // Value and Row types
    m.add_class::<types::PyValue>()?;
    m.add_class::<types::PyRow>()?;

    // LocyConfig and Schema
    m.add_class::<types::PyLocyConfig>()?;
    m.add_class::<types::PySchema>()?;

    // CommandResult classes
    m.add_class::<types::PyQueryCommandResult>()?;
    m.add_class::<types::PyAssumeCommandResult>()?;
    m.add_class::<types::PyExplainCommandResult>()?;
    m.add_class::<types::PyAbduceCommandResult>()?;
    m.add_class::<types::PyDeriveCommandResult>()?;
    m.add_class::<types::PyCypherCommandResult>()?;

    // Hook context types
    m.add_class::<types::PyHookContext>()?;
    m.add_class::<types::PyCommitHookContext>()?;
    m.add_class::<types::PyQueryType>()?;

    // Typed exception hierarchy
    exceptions::register_exceptions(_py, m)?;

    Ok(())
}
