// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Compaction facade for triggering and waiting on data compaction.

use uni_common::{Result, UniError};

use super::UniInner;

/// Facade for compaction operations.
///
/// Obtained via `db.compaction()`.
pub struct Compaction<'a> {
    pub(crate) inner: &'a UniInner,
}

impl Compaction<'_> {
    /// Compact data for a label or edge type.
    ///
    /// Automatically detects whether the name refers to a label or edge type
    /// by checking the schema.
    pub async fn compact(&self, name: &str) -> Result<uni_store::compaction::CompactionStats> {
        let schema = self.inner.schema.schema();
        if schema.labels.contains_key(name) {
            self.inner
                .storage
                .compact_label(name)
                .await
                .map_err(UniError::Internal)
        } else if schema.edge_types.contains_key(name) {
            self.inner
                .storage
                .compact_edge_type(name)
                .await
                .map_err(UniError::Internal)
        } else {
            Err(UniError::Internal(anyhow::anyhow!(
                "No label or edge type named '{}'",
                name
            )))
        }
    }

    /// Wait for all background compaction to complete.
    pub async fn wait(&self) -> Result<()> {
        self.inner
            .storage
            .wait_for_compaction()
            .await
            .map_err(UniError::Internal)
    }
}
