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

/// Reject a table name containing characters unsafe for on-disk dataset
/// paths and Lance branch names. (L6)
///
/// A label/edge-type with a path separator, whitespace, or control char
/// would otherwise reach Lance (which panics on an invalid dataset/branch
/// name) via a schemaless interning path that bypasses the schema-definition
/// validation. Guarding at table creation turns that panic into a clean
/// error on every write path. `.` is allowed (qualified names).
///
/// # Errors
/// Returns an error if `name` contains a control, whitespace, `/`, or `\`
/// character.
pub fn validate_table_name(name: &str) -> anyhow::Result<()> {
    if let Some(c) = name
        .chars()
        .find(|c| c.is_control() || c.is_whitespace() || matches!(c, '/' | '\\'))
    {
        anyhow::bail!("table name '{name}' contains an unsafe character ({c:?})");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the canonical adjacency name (`adjacency_{edge_type}_{direction}`)
    /// that both the fork branch registration (`fork.rs`) and the branch
    /// lookup key (`StorageManager::adjacency_dataset`) depend on. (L8)
    #[test]
    fn adjacency_table_name_is_canonical() {
        assert_eq!(adjacency_table_name("KNOWS", "fwd"), "adjacency_KNOWS_fwd");
        assert_eq!(adjacency_table_name("KNOWS", "bwd"), "adjacency_KNOWS_bwd");
    }
}
