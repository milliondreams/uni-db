// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Traits for native Locy command dispatch.
//!
//! `DerivedFactSource` provides read-only access to derived facts and the graph,
//! replacing `CypherExecutor` for read operations in the native path.
//!
//! `LocyExecutionContext` extends it with write operations (mutations, L0 fork/restore,
//! and strata re-evaluation) needed by ASSUME, DERIVE, and ABDUCE.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use uni_common::Value;
use uni_cypher::ast::{Expr, Pattern, Query};
use uni_locy::{CompiledProgram, FactRow, LocyConfig, LocyError};

use super::locy_delta::RowStore;

/// Read-only access to derived facts and graph data.
///
/// Replaces `CypherExecutor` for read operations in the native command dispatch path.
/// `lookup_derived` converts RecordBatch-based native facts to `FactRow` format internally.
#[async_trait(?Send)]
pub trait DerivedFactSource: Send + Sync {
    /// Look up all facts for a rule. Returns `FactRow`-based results.
    fn lookup_derived(&self, rule_name: &str) -> Result<Vec<FactRow>, LocyError>;

    /// Look up all facts for a rule as raw RecordBatches (zero-copy from native store).
    ///
    /// Default implementation returns an error. Override in native adapters to read
    /// directly from `DerivedStore` without converting to rows.
    fn lookup_derived_batches(&self, _rule_name: &str) -> Result<Vec<RecordBatch>, LocyError> {
        Err(LocyError::EvaluationError {
            message: "lookup_derived_batches not implemented for this adapter".into(),
        })
    }

    /// Execute a graph MATCH query with optional WHERE conditions.
    ///
    /// Returns raw `RecordBatch`es so the native path stays columnar.
    /// Callers that need `Vec<FactRow>` must convert via `record_batches_to_locy_rows`.
    ///
    /// Used by SLG clause resolution, EXPLAIN re-execution, and delta resolution.
    /// AST construction happens inside the adapter via `build_match_return_query`.
    async fn execute_pattern(
        &self,
        pattern: &Pattern,
        where_conditions: &[Expr],
    ) -> Result<Vec<RecordBatch>, LocyError>;
}

/// DB operations needed by ASSUME, DERIVE, and ABDUCE.
///
/// Extends `DerivedFactSource` with write operations.
#[async_trait(?Send)]
pub trait LocyExecutionContext: DerivedFactSource {
    /// Look up facts enriched with full node objects (for VID-based stores).
    ///
    /// Default implementation delegates to `lookup_derived()` (no enrichment needed
    /// for the orchestrator path which already returns full `Value::Node` objects).
    /// Override in native adapters to replace UInt64 VID values with full nodes so
    /// that commands like DERIVE can access node properties.
    async fn lookup_derived_enriched(&self, rule_name: &str) -> Result<Vec<FactRow>, LocyError> {
        self.lookup_derived(rule_name)
    }

    /// Execute a pre-compiled Cypher read query (e.g. from a `CompiledCommand::Cypher`).
    ///
    /// Used by ASSUME/ABDUCE body dispatch where a Query AST is already available.
    async fn execute_cypher_read(&self, ast: Query) -> Result<Vec<FactRow>, LocyError>;

    /// Execute a mutation (CREATE/MERGE/DELETE), returning affected row count.
    async fn execute_mutation(
        &self,
        ast: Query,
        params: HashMap<String, Value>,
    ) -> Result<usize, LocyError>;

    /// Fork the current Locy L0 buffer for hypothetical reasoning.
    ///
    /// Saves the current L0 state and replaces it with a clone.
    /// Mutations after fork are isolated to the clone.
    /// Call `restore_l0()` to undo all hypothetical changes.
    async fn fork_l0(&self) -> Result<(), LocyError>;

    /// Restore the Locy L0 buffer to its state before the last `fork_l0()`.
    ///
    /// Discards all mutations made since the fork.
    async fn restore_l0(&self) -> Result<(), LocyError>;

    /// Re-evaluate all strata in the current (possibly mutated) state.
    ///
    /// Used by ASSUME and ABDUCE `validate_modification` to check hypothetical states.
    /// Returns the row-based `RowStore` after convergence.
    async fn re_evaluate_strata(
        &self,
        program: &CompiledProgram,
        config: &LocyConfig,
    ) -> Result<RowStore, LocyError>;
}
