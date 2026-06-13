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
create_exception!(
    _uni_db,
    UniConstraintConflictError,
    UniError,
    "Commit-time uniqueness race (e.g. concurrent MERGE on the same key). \
     Retriable — unlike UniConstraintError, which is a non-retriable \
     constraint violation."
);
create_exception!(
    _uni_db,
    UniLockTimeoutError,
    UniError,
    "Timed out waiting for a FOR UPDATE row lock. Retriable."
);

// Resource limits
create_exception!(
    _uni_db,
    UniMemoryLimitExceededError,
    UniError,
    "Query exceeded its memory limit."
);
create_exception!(_uni_db, UniTimeoutError, UniError, "Operation timed out.");
create_exception!(
    _uni_db,
    UniLocyIncompleteError,
    UniError,
    "Locy evaluation stopped before completing (timeout or iteration limit). \
     Carries `reason`, strata counts, and the `skipped_rules` / \
     `complement_rules_affected` attributes; re-run with `allow_partial=True` \
     to recover the partial result instead."
);

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
// Fork lifecycle (Phase 4b)
create_exception!(
    _uni_db,
    UniForkNotFoundError,
    UniError,
    "Fork with the given name does not exist."
);
create_exception!(
    _uni_db,
    UniForkAlreadyExistsError,
    UniError,
    "Session::fork(name).new_() called against an existing fork."
);
create_exception!(
    _uni_db,
    UniForkInUseError,
    UniError,
    "Drop refused because forked sessions are still alive on the fork. \
     Carries `holder_count: int` attribute."
);
create_exception!(
    _uni_db,
    UniForkInflightTxError,
    UniError,
    "Drop refused because a transaction has uncommitted mutations on the fork."
);
create_exception!(
    _uni_db,
    UniForkHasChildrenError,
    UniError,
    "drop_fork refused because nested children exist. Use drop_fork_cascade. \
     Carries `children: list[str]` attribute."
);
create_exception!(
    _uni_db,
    UniForkSubtreeInUseError,
    UniError,
    "drop_fork_cascade refused because the subtree has live sessions / open txes. \
     Carries `blockers: list[str]` attribute."
);
create_exception!(
    _uni_db,
    UniForkBudgetExceededError,
    UniError,
    "Fork budget cap reached. Carries `current: int` and `max: int` attributes."
);
create_exception!(
    _uni_db,
    UniForkCorruptRegistryError,
    UniError,
    "Fork registry on disk is malformed."
);
create_exception!(
    _uni_db,
    UniForkLifecycleError,
    UniError,
    "A 2PC step on a fork lifecycle operation failed. Carries `stage: str` attribute."
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
        // SSI/OCC conflicts: previously fell through to the generic
        // `UniError` catch-all, making retriable contention impossible to
        // catch distinctly from Python.
        SerializationConflict { .. } => UniTransactionConflictError::new_err(msg),
        ConstraintConflict { .. } => UniConstraintConflictError::new_err(msg),
        LockTimeout { .. } => UniLockTimeoutError::new_err(msg),
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
        LocyIncomplete { detail } => {
            fork_err_with(UniLocyIncompleteError::new_err(msg), |py, val| {
                val.setattr("reason", detail.reason.as_str())?;
                val.setattr("elapsed_ms", detail.elapsed_ms)?;
                val.setattr("limit_ms", detail.limit_ms)?;
                val.setattr("max_iterations", detail.max_iterations)?;
                val.setattr("completed_strata", detail.completed_strata)?;
                val.setattr("total_strata", detail.total_strata)?;
                val.setattr(
                    "incomplete_rules",
                    pyo3::types::PyList::new(py, &detail.incomplete_rules)?,
                )?;
                val.setattr(
                    "skipped_rules",
                    pyo3::types::PyList::new(py, &detail.skipped_rules)?,
                )?;
                val.setattr(
                    "complement_rules_affected",
                    pyo3::types::PyList::new(py, &detail.complement_rules_affected)?,
                )?;
                Ok(())
            })
        }
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

        // Fork lifecycle (Phase 4b). The four payload-bearing variants
        // get their fields attached as Python attributes so callers can
        // write `e.holder_count`, `e.children`, etc.
        ForkNotFound { name } => {
            fork_err_with_attrs::<UniForkNotFoundError>(msg, &[("name", name)])
        }
        ForkAlreadyExists { name } => {
            fork_err_with_attrs::<UniForkAlreadyExistsError>(msg, &[("name", name)])
        }
        ForkInUse { name, holder_count } => {
            fork_err_with(UniForkInUseError::new_err(msg), |_py, val| {
                val.setattr("name", name)?;
                val.setattr("holder_count", holder_count)?;
                Ok(())
            })
        }
        ForkInflightTx { name } => {
            fork_err_with_attrs::<UniForkInflightTxError>(msg, &[("name", name)])
        }
        ForkHasChildren { name, children } => {
            fork_err_with(UniForkHasChildrenError::new_err(msg), |py, val| {
                val.setattr("name", name)?;
                val.setattr("children", pyo3::types::PyList::new(py, &children)?)?;
                Ok(())
            })
        }
        ForkSubtreeInUse { blockers } => {
            fork_err_with(UniForkSubtreeInUseError::new_err(msg), |py, val| {
                val.setattr("blockers", pyo3::types::PyList::new(py, &blockers)?)?;
                Ok(())
            })
        }
        ForkBudgetExceeded { current, max } => {
            fork_err_with(UniForkBudgetExceededError::new_err(msg), |_py, val| {
                val.setattr("current", current)?;
                val.setattr("max", max)?;
                Ok(())
            })
        }
        ForkCorruptRegistry { .. } => UniForkCorruptRegistryError::new_err(msg),
        ForkLifecycle { name, stage, .. } => {
            fork_err_with(UniForkLifecycleError::new_err(msg), |_py, val| {
                val.setattr("name", name)?;
                val.setattr("stage", stage)?;
                Ok(())
            })
        }
        ForkWritesNotYetSupported => UniError::new_err(msg),
        // Catch-all for future variants (non_exhaustive)
        _ => UniError::new_err(msg),
    }
}

/// Helper: attach a single `name` attribute to a fork exception that
/// has no other payload. Reduces boilerplate for the simple variants.
fn fork_err_with_attrs<E: pyo3::PyTypeInfo>(msg: String, attrs: &[(&str, String)]) -> PyErr {
    let err = PyErr::new::<E, _>(msg);
    Python::attach(|py| {
        let val = err.value(py);
        for (name, v) in attrs {
            let _ = val.setattr(*name, v.clone());
        }
    });
    err
}

/// Helper: build a fork exception, then mutate its Python value to
/// attach typed payload attributes. Errors during setattr are
/// suppressed — they're impossible in practice (all attrs are simple
/// owned types) and panicking inside the conversion would be worse
/// than carrying a missing attribute.
fn fork_err_with<F>(err: PyErr, mutator: F) -> PyErr
where
    F: for<'py> FnOnce(Python<'py>, &Bound<'py, pyo3::exceptions::PyBaseException>) -> PyResult<()>,
{
    Python::attach(|py| {
        let val = err.value(py);
        let _ = mutator(py, val);
    });
    err
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
    m.add(
        "UniConstraintConflictError",
        py.get_type::<UniConstraintConflictError>(),
    )?;
    m.add("UniLockTimeoutError", py.get_type::<UniLockTimeoutError>())?;

    // Resource limits
    m.add(
        "UniMemoryLimitExceededError",
        py.get_type::<UniMemoryLimitExceededError>(),
    )?;
    m.add("UniTimeoutError", py.get_type::<UniTimeoutError>())?;
    m.add(
        "UniLocyIncompleteError",
        py.get_type::<UniLocyIncompleteError>(),
    )?;

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

    // Fork lifecycle (Phase 4b)
    m.add(
        "UniForkNotFoundError",
        py.get_type::<UniForkNotFoundError>(),
    )?;
    m.add(
        "UniForkAlreadyExistsError",
        py.get_type::<UniForkAlreadyExistsError>(),
    )?;
    m.add("UniForkInUseError", py.get_type::<UniForkInUseError>())?;
    m.add(
        "UniForkInflightTxError",
        py.get_type::<UniForkInflightTxError>(),
    )?;
    m.add(
        "UniForkHasChildrenError",
        py.get_type::<UniForkHasChildrenError>(),
    )?;
    m.add(
        "UniForkSubtreeInUseError",
        py.get_type::<UniForkSubtreeInUseError>(),
    )?;
    m.add(
        "UniForkBudgetExceededError",
        py.get_type::<UniForkBudgetExceededError>(),
    )?;
    m.add(
        "UniForkCorruptRegistryError",
        py.get_type::<UniForkCorruptRegistryError>(),
    )?;
    m.add(
        "UniForkLifecycleError",
        py.get_type::<UniForkLifecycleError>(),
    )?;

    Ok(())
}
