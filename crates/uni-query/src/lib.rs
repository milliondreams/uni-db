// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Query execution layer for the Uni graph database.
//!
//! This crate provides OpenCypher query parsing, logical planning, and
//! execution against Uni's object-store-backed property graph.
//!
//! # Modules
//!
//! - [`query`] — planner, executor, DataFusion integration, pushdown logic
//! - [`types`] — public value types (`Value`, `Node`, `Edge`, `Path`, etc.)
//!
//! # Quick Start
//!
//! ```rust,ignore
//! let executor = Executor::new(storage);
//! let planner = QueryPlanner::new(schema);
//! let plan = planner.plan(cypher_ast)?;
//! let result = executor.execute_plan(plan, &params).await?;
//! ```

#![recursion_limit = "256"]

pub mod query;
pub mod types;

pub use query::executor::core::{OperatorStats, ProfileOutput};
pub use query::executor::procedure::{
    ProcedureOutput, ProcedureParam, ProcedureRegistry, ProcedureValueType, RegisteredProcedure,
};
pub use query::executor::{CustomFunctionRegistry, CustomScalarFn, Executor, ResultNormalizer};
pub use query::planner::{CostEstimates, ExplainOutput, IndexUsage, LogicalPlan, QueryPlanner};
pub use types::{
    Edge, ExecuteResult, FromValue, Node, Path, QueryCursor, QueryMetrics, QueryResult,
    QueryWarning, Row, Value,
};
pub use uni_cypher::ast::{Query as CypherQuery, TimeTravelSpec};

/// Validate that a query AST contains only read clauses.
///
/// Rejects any query that contains CREATE, MERGE, DELETE, SET, REMOVE,
/// or schema commands.
///
/// # Errors
///
/// Returns `Err(message)` describing the first write clause found. Used to
/// enforce read-only access for time-travel queries (`VERSION AS OF` /
/// `TIMESTAMP AS OF`).
pub fn validate_read_only(query: &CypherQuery) -> Result<(), String> {
    use uni_cypher::ast::{Clause, Query, Statement};

    fn check_statement(stmt: &Statement) -> Result<(), String> {
        for clause in &stmt.clauses {
            match clause {
                Clause::Create(_)
                | Clause::Merge(_)
                | Clause::Delete(_)
                | Clause::Set(_)
                | Clause::Remove(_) => {
                    return Err(
                        "Write clauses (CREATE, MERGE, DELETE, SET, REMOVE) are not allowed \
                         with VERSION AS OF / TIMESTAMP AS OF"
                            .to_string(),
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn check_query(q: &Query) -> Result<(), String> {
        match q {
            Query::Single(stmt) => check_statement(stmt),
            Query::Union { left, right, .. } => {
                check_query(left)?;
                check_query(right)
            }
            Query::Explain(inner) => check_query(inner),
            Query::TimeTravel { query, .. } => check_query(query),
            Query::Schema(cmd) => {
                use uni_cypher::ast::SchemaCommand;
                match cmd.as_ref() {
                    // Read-only schema commands are allowed
                    SchemaCommand::ShowConstraints(_)
                    | SchemaCommand::ShowIndexes(_)
                    | SchemaCommand::ShowDatabase
                    | SchemaCommand::ShowConfig
                    | SchemaCommand::ShowStatistics => Ok(()),
                    // All other schema commands mutate state
                    _ => Err(
                        "Mutating schema commands are not allowed in read-only context".to_string(),
                    ),
                }
            }
        }
    }

    check_query(query)
}
