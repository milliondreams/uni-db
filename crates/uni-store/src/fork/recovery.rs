// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Crash recovery for the fork registry.
//!
//! Invoked from `Uni::open` after schema load and before any session
//! handle is exposed. Walks the registry and tombstones to resume any
//! create that crashed in `Pending` or any drop that crashed
//! in/after the tombstone PUT.
//!
//! Phase 1 covers the synthetic-state path; Day 9 adds env-var-gated
//! fault injection in `lance_branch::create_branch` for end-to-end
//! crash tests.

// Rust guideline compliant

use std::sync::Arc;

use object_store::ObjectStore;
use tracing::{info, instrument, warn};
use uni_common::api::error::UniError;
use uni_common::core::fork::{ForkInfo, ForkStatus};

use super::registry::ForkRegistryHandle;

/// Resume any partial create/drop left behind by a crash.
///
/// Returns the number of registry entries reconciled, useful for tests
/// and observability.
///
/// # Errors
///
/// Returns the underlying [`UniError`] from the first unrecoverable
/// failure. Recovery is intentionally best-effort for individual
/// branches: a missing branch on a `Pending` rollback path is
/// success.
#[instrument(
    skip(registry, storage_store, candidate_datasets, dataset_uri_for),
    level = "info"
)]
pub async fn recover_forks<F>(
    registry: &ForkRegistryHandle,
    storage_store: &Arc<dyn ObjectStore>,
    candidate_datasets: &[String],
    mut dataset_uri_for: F,
) -> Result<usize, UniError>
where
    F: FnMut(&str) -> String,
{
    let mut reconciled = 0usize;

    // 1. Resume any `Pending` create — for Phase 1 we always roll back.
    //    Rolling forward (promote to Active) requires verifying that all
    //    expected branches were created, which the writer side may not
    //    have recorded yet at the point of crash. Conservative rollback
    //    is safe and simple.
    let snapshot = registry.snapshot().await;
    let pending: Vec<ForkInfo> = snapshot
        .forks
        .values()
        .filter(|f| f.status == ForkStatus::Pending)
        .cloned()
        .collect();

    for info in pending {
        info!(fork_name = %info.name, fork_id = %info.id, "rolling back Pending create");
        // Walk any partial branches and force-delete them.
        rollback_branches(&info, candidate_datasets, &mut dataset_uri_for).await;
        registry.rollback_create(&info.name).await?;
        reconciled += 1;
    }

    // 2. Resume any `Tombstoned` registry entry — finish the drop.
    let snapshot = registry.snapshot().await;
    let tombstoned: Vec<ForkInfo> = snapshot
        .forks
        .values()
        .filter(|f| f.status == ForkStatus::Tombstoned)
        .cloned()
        .collect();

    for info in tombstoned {
        info!(fork_name = %info.name, fork_id = %info.id, "completing tombstoned drop");
        delete_all_branches(&info, &mut dataset_uri_for).await;
        registry.finish_drop(&info).await?;
        super::delete_fork_artifacts(storage_store, &info.id).await;
        reconciled += 1;
    }

    // 3. Sweep any orphan tombstones (schema mismatches, etc.). These
    //    have no registry entry but a tombstone file on disk — finish
    //    the drop and remove the file.
    let orphans = registry.list_tombstones().await?;
    for info in orphans {
        info!(
            fork_name = %info.name,
            fork_id = %info.id,
            "sweeping orphan tombstone"
        );
        delete_all_branches(&info, &mut dataset_uri_for).await;
        registry.finish_drop(&info).await?;
        super::delete_fork_artifacts(storage_store, &info.id).await;
        reconciled += 1;
    }

    Ok(reconciled)
}

/// Best-effort: try to remove every branch in `info.datasets`. Errors
/// are logged at warn level and otherwise ignored, since the whole
/// point of force-delete is to mop up partial state.
async fn delete_all_branches<F>(info: &ForkInfo, dataset_uri_for: &mut F)
where
    F: FnMut(&str) -> String,
{
    #[cfg(feature = "lance-backend")]
    for (dataset, branch) in &info.datasets {
        let uri = dataset_uri_for(dataset);
        if let Err(e) = crate::backend::lance_branch::delete_branch(&uri, branch).await {
            warn!(
                dataset = %dataset,
                branch = %branch,
                "delete_branch during recovery failed: {e}"
            );
        }
    }

    #[cfg(not(feature = "lance-backend"))]
    {
        let _ = (info, dataset_uri_for);
    }
}

/// On Pending rollback, the registry's `datasets` map may be empty
/// (the writer hadn't recorded the branch names yet). Phase 1 takes
/// the conservative route: rely on `delete_all_branches` for any
/// names already recorded; un-recorded zombie branches are surfaced
/// in the spike binary's fault-injection scenario rather than
/// silently force-deleted, since we don't know what name to use.
async fn rollback_branches<F>(
    info: &ForkInfo,
    candidate_datasets: &[String],
    dataset_uri_for: &mut F,
) where
    F: FnMut(&str) -> String,
{
    // Recorded branches (the writer got far enough to persist `datasets`).
    if !info.datasets.is_empty() {
        delete_all_branches(info, dataset_uri_for).await;
    }

    // L3: a create that failed before recording any branch leaves a Pending
    // entry with an EMPTY `datasets` map plus `fork_{id}_{dataset}` branches
    // that no record references — zombies. Reconstruct the candidate branch
    // names from the schema-derived dataset list and force-delete them
    // (idempotent, so re-deleting an already-recorded branch is harmless and
    // an absent branch is a no-op). Caller passes the current schema's
    // candidate datasets; a branch whose dataset has since left the schema
    // is not reconstructable here (accepted: schema is persisted, so drift
    // between the crashed create and recovery is unlikely).
    #[cfg(feature = "lance-backend")]
    for dataset in candidate_datasets {
        let branch = format!("fork_{}_{}", info.id, dataset);
        let uri = dataset_uri_for(dataset);
        if let Err(e) = crate::backend::lance_branch::delete_branch(&uri, &branch).await {
            warn!(
                dataset = %dataset,
                branch = %branch,
                "zombie-branch reclamation during recovery failed: {e}"
            );
        }
    }

    #[cfg(not(feature = "lance-backend"))]
    let _ = candidate_datasets;
}

/// Convenience for tests: a `dataset_uri_for` closure that joins a
/// fixed base URI with each dataset name.
#[doc(hidden)]
pub fn join_uri_with(base_uri: String) -> impl FnMut(&str) -> String {
    move |dataset: &str| {
        if base_uri.ends_with('/') {
            format!("{base_uri}{dataset}.lance")
        } else {
            format!("{base_uri}/{dataset}.lance")
        }
    }
}
