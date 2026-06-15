// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::store_utils::{DEFAULT_TIMEOUT, get_with_timeout, list_with_timeout, put_with_timeout};
use anyhow::Result;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::instrument;
use uni_common::core::snapshot::SnapshotManifest;

pub struct SnapshotManager {
    store: Arc<dyn ObjectStore>,
}

impl SnapshotManager {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    fn manifest_path(&self, snapshot_id: &str) -> ObjectStorePath {
        ObjectStorePath::from(format!("catalog/manifests/{}.json", snapshot_id))
    }

    fn latest_ptr_path(&self) -> ObjectStorePath {
        ObjectStorePath::from("catalog/latest")
    }

    fn named_snapshots_path(&self) -> ObjectStorePath {
        ObjectStorePath::from("catalog/named_snapshots.json")
    }

    #[instrument(skip(self, manifest), fields(snapshot_id = %manifest.snapshot_id, size_bytes), level = "info")]
    pub async fn save_snapshot(&self, manifest: &SnapshotManifest) -> Result<()> {
        let path = self.manifest_path(&manifest.snapshot_id);
        let json = serde_json::to_string_pretty(manifest)?;
        tracing::Span::current().record("size_bytes", json.len());
        put_with_timeout(&self.store, &path, Bytes::from(json), DEFAULT_TIMEOUT).await?;
        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn load_snapshot(&self, snapshot_id: &str) -> Result<SnapshotManifest> {
        let path = self.manifest_path(snapshot_id);
        let result = get_with_timeout(&self.store, &path, DEFAULT_TIMEOUT).await?;
        let bytes = result.bytes().await?;
        let content = String::from_utf8(bytes.to_vec())?;
        let manifest: SnapshotManifest = serde_json::from_str(&content)?;
        Ok(manifest)
    }

    pub async fn list_snapshots(&self) -> Result<Vec<String>> {
        let prefix = ObjectStorePath::from("catalog/manifests");
        let metas = list_with_timeout(&self.store, Some(&prefix), DEFAULT_TIMEOUT).await?;
        let mut ids = Vec::new();

        for meta in metas {
            if let Some(filename) = meta.location.filename()
                && filename.ends_with(".json")
            {
                ids.push(filename.trim_end_matches(".json").to_string());
            }
        }
        Ok(ids)
    }

    /// Check if any snapshot manifests exist (for detecting database with lost manifest pointer).
    pub async fn has_any_manifests(&self) -> Result<bool> {
        let ids = self.list_snapshots().await?;
        Ok(!ids.is_empty())
    }

    pub async fn load_latest_snapshot(&self) -> Result<Option<SnapshotManifest>> {
        let latest_path = self.latest_ptr_path();
        match get_with_timeout(&self.store, &latest_path, DEFAULT_TIMEOUT).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(anyhow::Error::from)?;
                let snapshot_id = String::from_utf8(bytes.to_vec())?;
                let snapshot_id = snapshot_id.trim();
                if snapshot_id.is_empty() {
                    return Ok(None);
                }
                Ok(Some(self.load_snapshot(snapshot_id).await?))
            }
            Err(e) if e.to_string().contains("not found") => Ok(None),
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self), level = "info")]
    pub async fn set_latest_snapshot(&self, snapshot_id: &str) -> Result<()> {
        let path = self.latest_ptr_path();
        put_with_timeout(
            &self.store,
            &path,
            Bytes::from(snapshot_id.to_string()),
            DEFAULT_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    pub async fn load_named_snapshots(&self) -> Result<HashMap<String, String>> {
        let path = self.named_snapshots_path();
        match get_with_timeout(&self.store, &path, DEFAULT_TIMEOUT).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let content = String::from_utf8(bytes.to_vec())?;
                Ok(serde_json::from_str(&content)?)
            }
            Err(_) => Ok(HashMap::new()),
        }
    }

    pub async fn save_named_snapshot(&self, name: &str, snapshot_id: &str) -> Result<()> {
        let mut map = self.load_named_snapshots().await?;
        map.insert(name.to_string(), snapshot_id.to_string());

        let json = serde_json::to_string_pretty(&map)?;
        put_with_timeout(
            &self.store,
            &self.named_snapshots_path(),
            Bytes::from(json),
            DEFAULT_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    pub async fn get_named_snapshot(&self, name: &str) -> Result<Option<String>> {
        let map = self.load_named_snapshots().await?;
        Ok(map.get(name).cloned())
    }

    /// Find the most recent snapshot created at or before the given timestamp.
    pub async fn find_snapshot_at_time(
        &self,
        target: DateTime<Utc>,
    ) -> Result<Option<SnapshotManifest>> {
        let ids = self.list_snapshots().await?;
        let mut best: Option<SnapshotManifest> = None;

        for id in ids {
            // Fail closed: propagate a load error for a listed snapshot rather
            // than silently skipping it. Swallowing the error (the old `if let
            // Ok(m)`) let a corrupt/unreadable newer manifest fall through to an
            // older snapshot, answering a time-travel query from the wrong point
            // in time with no signal (review #3c).
            let m = self.load_snapshot(&id).await?;
            if m.created_at <= target && best.as_ref().is_none_or(|b| m.created_at > b.created_at) {
                best = Some(m);
            }
        }
        Ok(best)
    }
}

/// Make a just-published snapshot durable on local-filesystem stores by
/// fsync'ing the manifest body and the `catalog/latest` pointer (and their
/// parent directories) BEFORE the WAL — the only other durable copy of this
/// flush's data — is truncated (review C4).
///
/// `save_snapshot` / `set_latest_snapshot` write through the object store,
/// which does NOT fsync. Without this barrier a crash after WAL truncation but
/// before the OS flushed those writes would lose the snapshot: recovery could
/// not resolve `catalog/latest`.
///
/// A no-op when `local_root` is `None` (remote/object stores), which provide
/// their own durability on `put`. The two artifacts are fsync'd body-first then
/// pointer, matching the publish order, so a crash mid-barrier never leaves
/// `latest` pointing at a non-durable manifest. Paths mirror the private
/// `SnapshotManager::manifest_path` / `SnapshotManager::latest_ptr_path` helpers.
pub fn fsync_snapshot_pointer(
    local_root: Option<&std::path::Path>,
    snapshot_id: &str,
) -> std::io::Result<()> {
    let Some(root) = local_root else {
        return Ok(());
    };
    let manifest = root
        .join("catalog")
        .join("manifests")
        .join(format!("{snapshot_id}.json"));
    let latest = root.join("catalog").join("latest");
    crate::runtime::wal::sync_file_and_parent(&manifest)?;
    crate::runtime::wal::sync_file_and_parent(&latest)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Remote/object stores have no local root — the barrier is a no-op and
    /// must never error (durability is the backend's responsibility there).
    #[test]
    fn test_fsync_snapshot_pointer_noop_for_remote() {
        assert!(fsync_snapshot_pointer(None, "snap-1").is_ok());
    }

    /// On a local filesystem the manifest body and `catalog/latest` pointer are
    /// fsync'd in place (review C4).
    #[test]
    fn test_fsync_snapshot_pointer_syncs_local_artifacts() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path();
        let manifests = root.join("catalog").join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        std::fs::write(manifests.join("snap-1.json"), b"{}").unwrap();
        std::fs::write(root.join("catalog").join("latest"), b"snap-1").unwrap();

        fsync_snapshot_pointer(Some(root), "snap-1").unwrap();
    }
}
