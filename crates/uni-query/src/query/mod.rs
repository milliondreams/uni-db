// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cypher query planning and execution engine.
//!
//! Contains the logical planner, executor, DataFusion integration,
//! predicate pushdown, rewrite rules, and supporting utilities.
//!
//! Leaf modules (df_udfs, datetime, spatial, df_expr, expr_eval,
//! cypher_type_coerce, function_props, fusion, pushdown, rewrite,
//! similar_to) live in the `uni-query-functions` crate and are
//! re-exported below so downstream callers can keep using
//! `uni_query::query::<name>::*`.

pub mod df_graph;
pub mod df_planner;
pub mod df_udaf_plugin;
pub mod df_udfs_plugin;
pub mod executor;
pub mod locy_planner;
pub mod planner;
pub mod planner_locy_types;

// Re-export leaves from uni-query-functions so external code keeps
// working without path changes.
pub use uni_query_functions::{
    cypher_type_coerce, datetime, df_expr, df_udfs, expr_eval, function_props, fusion, pushdown,
    rewrite, similar_to, spatial,
};

/// Supported window function names (uppercase).
/// Used by both planner and executor for consistency.
pub const WINDOW_FUNCTIONS: &[&str] = &[
    "ROW_NUMBER",
    "RANK",
    "DENSE_RANK",
    "LAG",
    "LEAD",
    "NTILE",
    "FIRST_VALUE",
    "LAST_VALUE",
    "NTH_VALUE",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "COUNT",
];
