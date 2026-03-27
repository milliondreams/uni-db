// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Session hooks — before/after interception for queries and commits.
//!
//! Hooks allow cross-cutting concerns (audit logging, authorization, metrics)
//! to be injected into the query and commit lifecycle without modifying
//! individual query call sites.

use std::collections::HashMap;

use uni_common::{Result, Value};
use uni_query::QueryMetrics;

use crate::api::transaction::CommitResult;

/// The type of query being executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    /// A Cypher query (read or write).
    Cypher,
    /// A Locy program evaluation.
    Locy,
    /// An execute (mutation) statement.
    Execute,
}

/// Context passed to query hooks.
#[derive(Debug, Clone)]
pub struct HookContext {
    /// The session ID that initiated the query.
    pub session_id: String,
    /// The query text (Cypher or Locy program).
    pub query_text: String,
    /// The type of query.
    pub query_type: QueryType,
    /// Parameters bound to the query.
    pub params: HashMap<String, Value>,
}

/// Context passed to commit hooks.
#[derive(Debug, Clone)]
pub struct CommitHookContext {
    /// The session ID that owns the transaction.
    pub session_id: String,
    /// The transaction ID being committed.
    pub tx_id: String,
    /// Number of mutations in the transaction.
    pub mutation_count: usize,
}

/// Trait for session lifecycle hooks.
///
/// Implement this trait to intercept queries and commits at the session level.
/// Hooks are stored as `Arc<dyn SessionHook>` and can be shared across sessions
/// and templates.
///
/// # Failure Semantics
///
/// - `before_query`: Returning `Err` aborts the query with `HookRejected`.
/// - `after_query`: Infallible — panics are caught and logged.
/// - `before_commit`: Returning `Err` aborts the commit with `HookRejected`.
/// - `after_commit`: Infallible — panics are caught and logged.
pub trait SessionHook: Send + Sync {
    /// Called before a query is executed. Return `Err` to reject the query.
    fn before_query(&self, _ctx: &HookContext) -> Result<()> {
        Ok(())
    }

    /// Called after a query completes. Panics are caught and logged.
    fn after_query(&self, _ctx: &HookContext, _metrics: &QueryMetrics) {}

    /// Called before a transaction is committed. Return `Err` to reject the commit.
    fn before_commit(&self, _ctx: &CommitHookContext) -> Result<()> {
        Ok(())
    }

    /// Called after a transaction is successfully committed. Panics are caught and logged.
    fn after_commit(&self, _ctx: &CommitHookContext, _result: &CommitResult) {}
}
