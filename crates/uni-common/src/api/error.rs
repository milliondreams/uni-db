// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum UniError {
    #[error("Database not found: {path}")]
    NotFound { path: PathBuf },

    #[error("Schema error: {message}")]
    Schema { message: String },

    #[error("Parse error: {message}")]
    Parse {
        message: String,
        position: Option<usize>,
        line: Option<usize>,
        column: Option<usize>,
        context: Option<String>,
    },

    #[error("Query error: {message}")]
    Query {
        message: String,
        query: Option<String>,
    },

    #[error("Transaction error: {message}")]
    Transaction { message: String },

    #[error("Transaction conflict: {message}")]
    TransactionConflict { message: String },

    #[error("Transaction already completed")]
    TransactionAlreadyCompleted,

    /// Operation not supported on read-only database
    #[error("Operation '{operation}' not supported on read-only database")]
    ReadOnly { operation: String },

    /// Label not found in schema
    #[error("Label '{label}' not found in schema")]
    LabelNotFound { label: String },

    /// Edge type not found in schema
    #[error("Edge type '{edge_type}' not found in schema")]
    EdgeTypeNotFound { edge_type: String },

    /// Property not found on node/edge
    #[error("Property '{property}' not found on {entity_type} with label '{label}'")]
    PropertyNotFound {
        property: String,
        entity_type: String, // "node" or "edge"
        label: String,
    },

    /// Index not found
    #[error("Index '{index}' not found")]
    IndexNotFound { index: String },

    /// Snapshot not found
    #[error("Snapshot '{snapshot_id}' not found")]
    SnapshotNotFound { snapshot_id: String },

    /// Query memory limit exceeded
    #[error("Query exceeded memory limit of {limit_bytes} bytes")]
    MemoryLimitExceeded { limit_bytes: usize },

    #[error("Database is locked by another process")]
    DatabaseLocked,

    #[error("Operation timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    #[error("Type error: expected {expected}, got {actual}")]
    Type { expected: String, actual: String },

    #[error("Constraint violation: {message}")]
    Constraint { message: String },

    #[error("Storage error: {message}")]
    Storage {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),

    #[error("Invalid identifier '{name}': {reason}")]
    InvalidIdentifier { name: String, reason: String },

    #[error("Label '{label}' already exists")]
    LabelAlreadyExists { label: String },

    #[error("Edge type '{edge_type}' already exists")]
    EdgeTypeAlreadyExists { edge_type: String },

    #[error("Permission denied: {action}")]
    PermissionDenied { action: String },

    #[error("Argument '{arg}' is invalid: {message}")]
    InvalidArgument { arg: String, message: String },

    /// Write context (transaction, bulk writer, or appender) is already active on session.
    #[error("A write context is already active on session '{session_id}'")]
    WriteContextAlreadyActive {
        session_id: String,
        hint: &'static str,
    },

    /// Transaction commit timed out waiting for the global writer lock.
    #[error("Transaction '{tx_id}' commit timed out")]
    CommitTimeout { tx_id: String, hint: &'static str },

    /// Transaction exceeded its deadline.
    #[error("Transaction '{tx_id}' expired")]
    TransactionExpired { tx_id: String, hint: &'static str },

    /// Operation was cancelled via a cancellation token.
    #[error("Operation cancelled")]
    Cancelled,

    /// Derived facts are stale relative to the current database version.
    #[error("Derived facts are stale: version gap is {version_gap}")]
    StaleDerivedFacts { version_gap: u64 },

    /// A Locy rule conflict was detected during transaction commit rule promotion.
    #[error("Rule conflict: rule '{rule_name}' conflicts during promotion")]
    RuleConflict { rule_name: String },

    /// A session hook rejected the operation.
    #[error("Hook rejected: {message}")]
    HookRejected { message: String },

    /// Fork with the given name does not exist in the registry.
    #[error("Fork '{name}' not found")]
    ForkNotFound { name: String },

    /// `session.fork(name).new_()` was called against an existing fork.
    #[error("Fork '{name}' already exists")]
    ForkAlreadyExists { name: String },

    /// Phase-1 gate: writes through `forked_session.tx()` are blocked
    /// until Phase 2 lands. Reads, `locy()`, and admin paths work.
    #[error(
        "Writes on a forked session are not yet supported (Phase 2); reads, locy, and admin paths work"
    )]
    ForkWritesNotYetSupported,

    /// Drop refused because forked sessions are still alive on the fork.
    #[error("Fork '{name}' is held by {holder_count} live session(s); drop refused")]
    ForkInUse { name: String, holder_count: usize },

    /// Drop refused because a transaction has uncommitted mutations on the
    /// fork. Commit or roll back the transaction first, then retry drop.
    #[error("Fork '{name}' has uncommitted transaction state; commit or rollback first")]
    ForkInflightTx { name: String },

    /// Registry on disk is malformed (corrupt JSON, missing required field, etc.).
    #[error("Fork registry is corrupt: {message}")]
    ForkCorruptRegistry { message: String },

    /// Drop refused because this fork has nested children. Use
    /// `drop_fork_cascade` to remove the whole subtree, or drop the
    /// children individually first.
    #[error("Fork '{name}' has nested children {children:?}; use drop_fork_cascade or drop them first")]
    ForkHasChildren { name: String, children: Vec<String> },

    /// `drop_fork_cascade` refused because at least one fork in the
    /// subtree has live sessions or in-flight transactions. No branch
    /// has been deleted yet — the cascade is atomic at the validation
    /// step. Resolve the blockers and retry.
    #[error("Fork subtree cannot be dropped: {blockers:?}")]
    ForkSubtreeInUse { blockers: Vec<String> },

    /// 2PC step on a fork lifecycle operation failed.
    ///
    /// `stage` names the step (`registry_pending`, `create_branch`,
    /// `registry_active`, `tombstone`, `delete_branch`, `registry_clear`,
    /// `backend_unsupported`, `recovery`) so recovery and humans can
    /// triage without parsing prose.
    #[error("Fork '{name}' lifecycle failed at stage '{stage}': {source}")]
    ForkLifecycle {
        name: String,
        stage: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T> = std::result::Result<T, UniError>;
