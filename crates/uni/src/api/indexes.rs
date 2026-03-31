// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Indexes facade for managing and rebuilding indexes.

use uni_common::{Result, UniError};

use super::UniInner;

/// Facade for index management operations.
///
/// Obtained via `db.indexes()`.
pub struct Indexes<'a> {
    pub(crate) inner: &'a UniInner,
}

impl Indexes<'_> {
    /// List index definitions, optionally filtered by label.
    pub fn list(&self, label: Option<&str>) -> Vec<uni_common::core::schema::IndexDefinition> {
        let schema = self.inner.schema.schema();
        match label {
            Some(l) => schema
                .indexes
                .iter()
                .filter(|i| i.label() == l)
                .cloned()
                .collect(),
            None => schema.indexes.clone(),
        }
    }

    /// Rebuild indexes for a label.
    ///
    /// When `background` is true, rebuilds asynchronously and returns a task ID.
    /// When `background` is false, blocks until complete and returns `None`.
    pub async fn rebuild(&self, label: &str, background: bool) -> Result<Option<String>> {
        if background {
            let manager = uni_store::storage::IndexRebuildManager::new(
                self.inner.storage.clone(),
                self.inner.schema.clone(),
                self.inner.config.index_rebuild.clone(),
            )
            .await
            .map_err(UniError::Internal)?;

            let task_ids = manager
                .schedule(vec![label.to_string()])
                .await
                .map_err(UniError::Internal)?;

            let manager = std::sync::Arc::new(manager);
            let handle = manager.start_background_worker(self.inner.shutdown_handle.subscribe());
            self.inner.shutdown_handle.track_task(handle);

            Ok(task_ids.into_iter().next())
        } else {
            let idx_mgr = uni_store::storage::IndexManager::new(
                self.inner.storage.base_path(),
                self.inner.schema.clone(),
                self.inner.storage.backend_arc(),
            );
            idx_mgr
                .rebuild_indexes_for_label(label)
                .await
                .map_err(UniError::Internal)?;
            Ok(None)
        }
    }

    /// Get status of all rebuild tasks.
    pub async fn rebuild_status(&self) -> Result<Vec<uni_store::storage::IndexRebuildTask>> {
        let manager = uni_store::storage::IndexRebuildManager::new(
            self.inner.storage.clone(),
            self.inner.schema.clone(),
            self.inner.config.index_rebuild.clone(),
        )
        .await
        .map_err(UniError::Internal)?;

        Ok(manager.status())
    }

    /// Retry failed rebuild tasks. Returns retried task IDs.
    pub async fn retry_failed(&self) -> Result<Vec<String>> {
        let manager = uni_store::storage::IndexRebuildManager::new(
            self.inner.storage.clone(),
            self.inner.schema.clone(),
            self.inner.config.index_rebuild.clone(),
        )
        .await
        .map_err(UniError::Internal)?;

        let retried = manager.retry_failed().await.map_err(UniError::Internal)?;

        if !retried.is_empty() {
            let manager = std::sync::Arc::new(manager);
            let handle = manager.start_background_worker(self.inner.shutdown_handle.subscribe());
            self.inner.shutdown_handle.track_task(handle);
        }

        Ok(retried)
    }
}
