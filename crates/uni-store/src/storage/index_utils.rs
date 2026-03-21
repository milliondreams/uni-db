// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Shared helpers for LanceDB index management.

use lancedb::Table;
use lancedb::index::Index as LanceDbIndex;
use lancedb::index::scalar::BTreeIndexBuilder;

/// Creates a BTree index on a column if one does not already exist.
///
/// Logs a warning on failure rather than propagating the error, since
/// index creation failure is non-fatal (queries still work via full scan).
pub(super) async fn ensure_btree_index(
    table: &Table,
    indices: &[lancedb::index::IndexConfig],
    column: &str,
    table_label: &str,
) {
    let already_exists = indices
        .iter()
        .any(|idx| idx.columns.iter().any(|c| c == column));
    if already_exists {
        return;
    }

    log::info!("Creating {} BTree index for {} table", column, table_label);
    if let Err(e) = table
        .create_index(&[column], LanceDbIndex::BTree(BTreeIndexBuilder::default()))
        .execute()
        .await
    {
        log::warn!(
            "Failed to create {} index for {}: {}",
            column,
            table_label,
            e
        );
    }
}
