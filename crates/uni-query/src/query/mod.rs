// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod cypher_type_coerce;
pub mod datetime;
pub mod df_expr;
pub mod df_graph;
pub mod df_planner;
pub mod df_udfs;
pub mod executor;
pub mod expr_eval;
pub mod function_props;
pub mod locy_planner;
pub mod planner;
pub mod planner_locy_types;
pub mod pushdown;
pub mod rewrite;
pub mod spatial;

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
