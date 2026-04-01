// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Backend-independent table naming conventions.
//!
//! All backends must use these naming conventions for graph tables.
//! Extracted from `LanceDbStore` for backend-agnostic use.

/// Main vertices table name.
pub fn main_vertex_table_name() -> &'static str {
    "vertices"
}

/// Main edges table name.
pub fn main_edge_table_name() -> &'static str {
    "edges"
}

/// Per-label vertex table name.
pub fn vertex_table_name(label: &str) -> String {
    format!("vertices_{}", label)
}

/// Delta table name for edge mutations.
pub fn delta_table_name(edge_type: &str, direction: &str) -> String {
    format!("deltas_{}_{}", edge_type, direction)
}

/// Adjacency table name.
pub fn adjacency_table_name(edge_type: &str, direction: &str) -> String {
    format!("adjacency_{}_{}", edge_type, direction)
}
