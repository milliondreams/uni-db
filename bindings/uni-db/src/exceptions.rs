// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Typed Python exception hierarchy mirroring [`uni_common::UniError`] variants.
//!
//! Every `UniError` variant maps to a dedicated Python exception class rooted
//! under `UniError(Exception)`.  This lets Python callers write
//! `except uni_db.UniLabelNotFoundError` instead of matching on message strings.

use pyo3::prelude::*;
use pyo3::{create_exception, exceptions::PyException};

// ============================================================================
// Exception hierarchy
// ============================================================================

// Base
create_exception!(
    _uni_db,
    UniError,
    PyException,
    "Base exception for all Uni database errors."
);

// Database lifecycle
create_exception!(
    _uni_db,
    UniNotFoundError,
    UniError,
    "Database path does not exist."
);
create_exception!(
    _uni_db,
    UniDatabaseLockedError,
    UniError,
    "Database is locked by another process."
);

// Schema errors
create_exception!(
    _uni_db,
    UniSchemaError,
    UniError,
    "Schema definition or migration error."
);
create_exception!(
    _uni_db,
    UniLabelNotFoundError,
    UniError,
    "Label not found in schema."
);
create_exception!(
    _uni_db,
    UniEdgeTypeNotFoundError,
    UniError,
    "Edge type not found in schema."
);
create_exception!(
    _uni_db,
    UniPropertyNotFoundError,
    UniError,
    "Property not found on entity."
);
create_exception!(_uni_db, UniIndexNotFoundError, UniError, "Index not found.");
create_exception!(
    _uni_db,
    UniLabelAlreadyExistsError,
    UniError,
    "Label already exists in schema."
);
create_exception!(
    _uni_db,
    UniEdgeTypeAlreadyExistsError,
    UniError,
    "Edge type already exists in schema."
);
create_exception!(
    _uni_db,
    UniConstraintError,
    UniError,
    "Constraint violation."
);
create_exception!(
    _uni_db,
    UniInvalidIdentifierError,
    UniError,
    "Invalid identifier name."
);

// Query & parse errors
create_exception!(
    _uni_db,
    UniParseError,
    UniError,
    "Cypher or Locy parse error."
);
create_exception!(_uni_db, UniQueryError, UniError, "Query execution error.");
create_exception!(_uni_db, UniTypeError, UniError, "Type mismatch error.");

// Transaction errors
create_exception!(_uni_db, UniTransactionError, UniError, "Transaction error.");
create_exception!(
    _uni_db,
    UniTransactionConflictError,
    UniError,
    "Transaction serialization conflict."
);
create_exception!(
    _uni_db,
    UniTransactionAlreadyCompletedError,
    UniError,
    "Transaction has already been committed or rolled back."
);
create_exception!(
    _uni_db,
    UniTransactionExpiredError,
    UniError,
    "Transaction exceeded its deadline."
);
create_exception!(
    _uni_db,
    UniCommitTimeoutError,
    UniError,
    "Transaction commit timed out waiting for the writer lock."
);

// Resource limits
create_exception!(
    _uni_db,
    UniMemoryLimitExceededError,
    UniError,
    "Query exceeded its memory limit."
);
create_exception!(_uni_db, UniTimeoutError, UniError, "Operation timed out.");

// Access control
create_exception!(
    _uni_db,
    UniReadOnlyError,
    UniError,
    "Operation not supported on read-only database."
);
create_exception!(
    _uni_db,
    UniPermissionDeniedError,
    UniError,
    "Permission denied."
);

// Storage & I/O
create_exception!(_uni_db, UniStorageError, UniError, "Storage layer error.");
create_exception!(_uni_db, UniIOError, UniError, "I/O error.");
create_exception!(_uni_db, UniInternalError, UniError, "Internal error.");

// Snapshot
create_exception!(
    _uni_db,
    UniSnapshotNotFoundError,
    UniError,
    "Snapshot not found."
);

// Arguments
create_exception!(
    _uni_db,
    UniInvalidArgumentError,
    UniError,
    "Invalid argument."
);

// Concurrency
create_exception!(
    _uni_db,
    UniWriteContextAlreadyActiveError,
    UniError,
    "A write context is already active on the session."
);
create_exception!(
    _uni_db,
    UniCancelledError,
    UniError,
    "Operation was cancelled."
);

// Locy-specific
create_exception!(
    _uni_db,
    UniStaleDerivedFactsError,
    UniError,
    "Derived facts are stale relative to the current database version."
);
create_exception!(
    _uni_db,
    UniRuleConflictError,
    UniError,
    "Locy rule conflict during promotion."
);
create_exception!(
    _uni_db,
    UniHookRejectedError,
    UniError,
    "A session hook rejected the operation."
);
create_exception!(
    _uni_db,
    UniLocyCompileError,
    UniError,
    "Locy program compilation error."
);
create_exception!(
    _uni_db,
    UniLocyRuntimeError,
    UniError,
    "Locy program runtime error."
);

// ============================================================================
// Error conversion
// ============================================================================

/// Convert a [`uni_common::UniError`] into the matching typed Python exception.
pub fn uni_error_to_pyerr(e: uni_common::UniError) -> PyErr {
    use uni_common::UniError::*;
    let msg = e.to_string();
    match e {
        NotFound { .. } => UniNotFoundError::new_err(msg),
        Schema { .. } => UniSchemaError::new_err(msg),
        Parse { .. } => UniParseError::new_err(msg),
        Query { .. } => UniQueryError::new_err(msg),
        Transaction { .. } => UniTransactionError::new_err(msg),
        TransactionConflict { .. } => UniTransactionConflictError::new_err(msg),
        TransactionAlreadyCompleted => UniTransactionAlreadyCompletedError::new_err(msg),
        ReadOnly { .. } => UniReadOnlyError::new_err(msg),
        LabelNotFound { .. } => UniLabelNotFoundError::new_err(msg),
        EdgeTypeNotFound { .. } => UniEdgeTypeNotFoundError::new_err(msg),
        PropertyNotFound { .. } => UniPropertyNotFoundError::new_err(msg),
        IndexNotFound { .. } => UniIndexNotFoundError::new_err(msg),
        SnapshotNotFound { .. } => UniSnapshotNotFoundError::new_err(msg),
        MemoryLimitExceeded { .. } => UniMemoryLimitExceededError::new_err(msg),
        DatabaseLocked => UniDatabaseLockedError::new_err(msg),
        Timeout { .. } => UniTimeoutError::new_err(msg),
        Type { .. } => UniTypeError::new_err(msg),
        Constraint { .. } => UniConstraintError::new_err(msg),
        Storage { .. } => UniStorageError::new_err(msg),
        Io(_) => UniIOError::new_err(msg),
        Internal(_) => UniInternalError::new_err(msg),
        InvalidIdentifier { .. } => UniInvalidIdentifierError::new_err(msg),
        LabelAlreadyExists { .. } => UniLabelAlreadyExistsError::new_err(msg),
        EdgeTypeAlreadyExists { .. } => UniEdgeTypeAlreadyExistsError::new_err(msg),
        PermissionDenied { .. } => UniPermissionDeniedError::new_err(msg),
        InvalidArgument { .. } => UniInvalidArgumentError::new_err(msg),
        WriteContextAlreadyActive { .. } => UniWriteContextAlreadyActiveError::new_err(msg),
        CommitTimeout { .. } => UniCommitTimeoutError::new_err(msg),
        TransactionExpired { .. } => UniTransactionExpiredError::new_err(msg),
        Cancelled => UniCancelledError::new_err(msg),
        StaleDerivedFacts { .. } => UniStaleDerivedFactsError::new_err(msg),
        RuleConflict { .. } => UniRuleConflictError::new_err(msg),
        HookRejected { .. } => UniHookRejectedError::new_err(msg),
        // Catch-all for future variants (non_exhaustive)
        _ => UniError::new_err(msg),
    }
}

/// Convert an [`anyhow::Error`] into a Python exception.
///
/// Attempts to downcast to `UniError` first; falls back to `UniInternalError`.
pub fn anyhow_to_pyerr(e: anyhow::Error) -> PyErr {
    match e.downcast::<uni_common::UniError>() {
        Ok(uni_err) => uni_error_to_pyerr(uni_err),
        Err(e) => UniInternalError::new_err(e.to_string()),
    }
}

/// Convert a Locy compile error into a Python exception.
pub fn locy_compile_error_to_pyerr(e: uni_locy::LocyCompileError) -> PyErr {
    UniLocyCompileError::new_err(e.to_string())
}

/// Convert a Locy runtime error into a Python exception.
pub fn locy_runtime_error_to_pyerr(e: uni_locy::LocyError) -> PyErr {
    UniLocyRuntimeError::new_err(e.to_string())
}

// ============================================================================
// Module registration
// ============================================================================

/// Register all exception types on the Python module.
pub fn register_exceptions(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("UniError", py.get_type::<UniError>())?;

    // Database lifecycle
    m.add("UniNotFoundError", py.get_type::<UniNotFoundError>())?;
    m.add(
        "UniDatabaseLockedError",
        py.get_type::<UniDatabaseLockedError>(),
    )?;

    // Schema
    m.add("UniSchemaError", py.get_type::<UniSchemaError>())?;
    m.add(
        "UniLabelNotFoundError",
        py.get_type::<UniLabelNotFoundError>(),
    )?;
    m.add(
        "UniEdgeTypeNotFoundError",
        py.get_type::<UniEdgeTypeNotFoundError>(),
    )?;
    m.add(
        "UniPropertyNotFoundError",
        py.get_type::<UniPropertyNotFoundError>(),
    )?;
    m.add(
        "UniIndexNotFoundError",
        py.get_type::<UniIndexNotFoundError>(),
    )?;
    m.add(
        "UniLabelAlreadyExistsError",
        py.get_type::<UniLabelAlreadyExistsError>(),
    )?;
    m.add(
        "UniEdgeTypeAlreadyExistsError",
        py.get_type::<UniEdgeTypeAlreadyExistsError>(),
    )?;
    m.add("UniConstraintError", py.get_type::<UniConstraintError>())?;
    m.add(
        "UniInvalidIdentifierError",
        py.get_type::<UniInvalidIdentifierError>(),
    )?;

    // Query & parse
    m.add("UniParseError", py.get_type::<UniParseError>())?;
    m.add("UniQueryError", py.get_type::<UniQueryError>())?;
    m.add("UniTypeError", py.get_type::<UniTypeError>())?;

    // Transaction
    m.add("UniTransactionError", py.get_type::<UniTransactionError>())?;
    m.add(
        "UniTransactionConflictError",
        py.get_type::<UniTransactionConflictError>(),
    )?;
    m.add(
        "UniTransactionAlreadyCompletedError",
        py.get_type::<UniTransactionAlreadyCompletedError>(),
    )?;
    m.add(
        "UniTransactionExpiredError",
        py.get_type::<UniTransactionExpiredError>(),
    )?;
    m.add(
        "UniCommitTimeoutError",
        py.get_type::<UniCommitTimeoutError>(),
    )?;

    // Resource limits
    m.add(
        "UniMemoryLimitExceededError",
        py.get_type::<UniMemoryLimitExceededError>(),
    )?;
    m.add("UniTimeoutError", py.get_type::<UniTimeoutError>())?;

    // Access control
    m.add("UniReadOnlyError", py.get_type::<UniReadOnlyError>())?;
    m.add(
        "UniPermissionDeniedError",
        py.get_type::<UniPermissionDeniedError>(),
    )?;

    // Storage & I/O
    m.add("UniStorageError", py.get_type::<UniStorageError>())?;
    m.add("UniIOError", py.get_type::<UniIOError>())?;
    m.add("UniInternalError", py.get_type::<UniInternalError>())?;

    // Snapshot
    m.add(
        "UniSnapshotNotFoundError",
        py.get_type::<UniSnapshotNotFoundError>(),
    )?;

    // Arguments
    m.add(
        "UniInvalidArgumentError",
        py.get_type::<UniInvalidArgumentError>(),
    )?;

    // Concurrency
    m.add(
        "UniWriteContextAlreadyActiveError",
        py.get_type::<UniWriteContextAlreadyActiveError>(),
    )?;
    m.add("UniCancelledError", py.get_type::<UniCancelledError>())?;

    // Locy-specific
    m.add(
        "UniStaleDerivedFactsError",
        py.get_type::<UniStaleDerivedFactsError>(),
    )?;
    m.add(
        "UniRuleConflictError",
        py.get_type::<UniRuleConflictError>(),
    )?;
    m.add(
        "UniHookRejectedError",
        py.get_type::<UniHookRejectedError>(),
    )?;
    m.add("UniLocyCompileError", py.get_type::<UniLocyCompileError>())?;
    m.add("UniLocyRuntimeError", py.get_type::<UniLocyRuntimeError>())?;

    Ok(())
}
