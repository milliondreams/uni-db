// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Branch primitives for Lance datasets, used by the fork feature.
//!
//! `lancedb` 0.27.1 does not expose Lance's branch API, so this module
//! descends one layer to operate on `lance::Dataset` directly. It provides
//! free functions for create / delete / list / open-branch and version
//! lookup, used by the fork registry and recovery driver in
//! `crate::fork`.
//!
//! # Idempotence and recovery
//!
//! `lance::Dataset::create_branch` is a two-phase operation (shallow
//! clone of the dataset, then `BranchContents` write) and is **not
//! atomic**. A crash between the two phases leaves a "zombie" branch
//! dataset that prevents re-creating the same branch name. The recovery
//! driver must call [`force_delete_branch`] before retrying. See
//! `crates/uni-store/src/fork/recovery.rs` for the recovery logic.

// Rust guideline compliant

use anyhow::{Context, Result};
use lance::Dataset;

/// Open the dataset at `uri` on its main branch.
///
/// Used by Phase 1 fork-creation flows to discover the current version
/// before branching, and by recovery to enumerate live branches.
///
/// # Errors
///
/// Returns an error if the dataset does not exist or cannot be opened.
pub async fn open_dataset(uri: &str) -> Result<Dataset> {
    Dataset::open(uri)
        .await
        .with_context(|| format!("open lance dataset at {uri}"))
}

/// Open the dataset at `uri` on the given branch.
///
/// Returns a `Dataset` whose subsequent reads resolve through the branch
/// chain (`base_paths`) up to primary.
///
/// # Errors
///
/// Returns an error if the dataset or branch cannot be opened.
pub async fn open_branch(uri: &str, branch: &str) -> Result<Dataset> {
    let dataset = open_dataset(uri).await?;
    dataset
        .checkout_branch(branch)
        .await
        .with_context(|| format!("checkout branch {branch} on {uri}"))
}

/// Return the current version number of the dataset's main branch.
///
/// The returned u64 is the parent_version handed to [`create_branch`].
///
/// # Errors
///
/// Returns an error if the dataset cannot be opened.
pub async fn current_version(uri: &str) -> Result<u64> {
    let dataset = open_dataset(uri).await?;
    Ok(dataset.version().version)
}

/// Create a new branch off the dataset's main branch at `parent_version`.
///
/// Phase 1 of fork creation calls this once per Lance dataset that
/// already exists in the schema at fork point. The resulting branch
/// shares fragments with the parent until its first write.
///
/// **Not atomic.** A crash between the shallow-clone and BranchContents
/// phases leaves a zombie branch dataset. Recovery must use
/// [`force_delete_branch`] to clean up before retry.
///
/// # Errors
///
/// Returns an error if the branch already exists, the parent version
/// does not exist, or the underlying object store call fails.
///
/// # Fault injection
///
/// Setting `UNI_FORK_INJECT_FAIL_AFTER` to a non-negative integer N
/// causes the Nth and subsequent invocations (in this process) to
/// return an error before reaching `create_branch`. Used by the
/// recovery test suite to drive partial-create scenarios. The counter
/// is process-local; no real production path reads this variable.
pub async fn create_branch(uri: &str, branch: &str, parent_version: u64) -> Result<()> {
    inject_fault_create_branch()?;
    let mut dataset = open_dataset(uri).await?;
    dataset
        .create_branch(branch, parent_version, None)
        .await
        .with_context(|| format!("create branch {branch} on {uri} at v{parent_version}"))?;
    Ok(())
}

/// Return the current version number of the named branch (Phase 3).
///
/// Lance versions are per-branch — a branch's tip advances independently
/// of main once writes land on it. Nested-fork creation must read the
/// parent branch's tip, not main's, before calling
/// [`create_branch_from`].
///
/// # Errors
///
/// Returns an error if the dataset or branch cannot be opened.
pub async fn current_version_on_branch(uri: &str, branch: &str) -> Result<u64> {
    let dataset = open_branch(uri, branch).await?;
    Ok(dataset.version().version)
}

/// Create a new branch off another branch (Phase 3, nested forks).
///
/// `parent_branch` must already exist on the dataset. The new branch's
/// `base_paths` resolves: `new_branch → parent_branch → main`, so reads
/// against the new branch chain through both ancestors.
///
/// Same non-idempotence contract as [`create_branch`]: a crash between
/// the shallow-clone and `BranchContents` phases leaves a zombie; the
/// recovery driver must [`delete_branch`] (force-mode) before retry.
///
/// # Errors
///
/// - The dataset or parent branch cannot be opened.
/// - `new_branch` already exists.
/// - The underlying object store call fails.
pub async fn create_branch_from(
    uri: &str,
    new_branch: &str,
    parent_branch: &str,
    parent_version: u64,
) -> Result<()> {
    inject_fault_create_branch()?;
    let mut on_parent = open_branch(uri, parent_branch).await?;
    on_parent
        .create_branch(new_branch, parent_version, None)
        .await
        .with_context(|| {
            format!(
                "create branch {new_branch} off {parent_branch} on {uri} at v{parent_version}"
            )
        })?;
    Ok(())
}

/// Per-process counter for fault injection in `create_branch`. The
/// recovery test crate reads / writes via the helpers below.
#[doc(hidden)]
pub mod fault_injection {
    use std::sync::atomic::{AtomicI64, Ordering};

    pub(super) static CALL_COUNT: AtomicI64 = AtomicI64::new(0);
    pub(super) static DELETE_CALL_COUNT: AtomicI64 = AtomicI64::new(0);

    /// Reset the create-branch call counter. Tests that exercise
    /// `UNI_FORK_INJECT_FAIL_AFTER` should call this at the start to
    /// make assertions deterministic.
    pub fn reset() {
        CALL_COUNT.store(0, Ordering::SeqCst);
    }

    /// Read the current create-branch call count.
    pub fn calls_so_far() -> i64 {
        CALL_COUNT.load(Ordering::SeqCst)
    }

    /// Reset the delete-branch call counter (Phase 3 cascade-recovery
    /// test harness). Pairs with `UNI_FORK_INJECT_FAIL_DELETE_AFTER`.
    pub fn reset_delete() {
        DELETE_CALL_COUNT.store(0, Ordering::SeqCst);
    }

    /// Read the current delete-branch call count.
    pub fn delete_calls_so_far() -> i64 {
        DELETE_CALL_COUNT.load(Ordering::SeqCst)
    }
}

fn inject_fault_create_branch() -> Result<()> {
    let cur = fault_injection::CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let Some(threshold_str) = std::env::var_os("UNI_FORK_INJECT_FAIL_AFTER") else {
        return Ok(());
    };
    let Ok(threshold) = threshold_str
        .into_string()
        .map_err(|_| ())
        .and_then(|s| s.parse::<i64>().map_err(|_| ()))
    else {
        return Ok(());
    };
    if cur >= threshold {
        anyhow::bail!(
            "UNI_FORK_INJECT_FAIL_AFTER triggered at call #{cur} (threshold {threshold})"
        );
    }
    Ok(())
}

/// Phase 3 cascade-recovery fault hook for `delete_branch`. Reads
/// `UNI_FORK_INJECT_FAIL_DELETE_AFTER`; the Nth and subsequent calls
/// (in-process counter `DELETE_CALL_COUNT`) bail before the actual
/// delete runs. Independent from the create-side counter so the two
/// can be armed in the same test.
fn inject_fault_delete_branch() -> Result<()> {
    let cur = fault_injection::DELETE_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let Some(threshold_str) = std::env::var_os("UNI_FORK_INJECT_FAIL_DELETE_AFTER") else {
        return Ok(());
    };
    let Ok(threshold) = threshold_str
        .into_string()
        .map_err(|_| ())
        .and_then(|s| s.parse::<i64>().map_err(|_| ()))
    else {
        return Ok(());
    };
    if cur >= threshold {
        anyhow::bail!(
            "UNI_FORK_INJECT_FAIL_DELETE_AFTER triggered at call #{cur} (threshold {threshold})"
        );
    }
    Ok(())
}

/// Delete a branch (idempotent under the recovery driver — uses force).
///
/// Phase 1 fork-drop and recovery both call this. Force-delete handles
/// zombie branches left by a half-completed `create_branch`. Missing
/// branches are treated as success: the recovery driver may invoke
/// this even when nothing was created, and the spec's drop step 3 is
/// declared idempotent.
///
/// # Errors
///
/// Returns an error only on object-store failures unrelated to the
/// missing-branch case.
pub async fn delete_branch(uri: &str, branch: &str) -> Result<()> {
    inject_fault_delete_branch()?;
    let mut dataset = open_dataset(uri).await?;

    // Cheap pre-check: skip the force-delete if neither BranchContents
    // nor the shallow-clone tree exist. lance::Dataset::force_delete_branch
    // treats truly-missing branches as an error, but the recovery
    // contract requires idempotence — so we filter that case here.
    let branches = dataset
        .list_branches()
        .await
        .with_context(|| format!("list branches on {uri}"))?;
    if !branches.contains_key(branch) {
        // Probe the shallow-clone path to catch zombies that have no
        // BranchContents but still occupy `tree/{branch}`.
        let zombie_uri = format!("{uri}/tree/{branch}");
        if Dataset::open(&zombie_uri).await.is_err() {
            return Ok(());
        }
    }

    dataset
        .force_delete_branch(branch)
        .await
        .with_context(|| format!("force-delete branch {branch} on {uri}"))?;
    Ok(())
}

/// List branch names on the dataset.
///
/// Used by recovery to determine which datasets a partial fork has
/// already branched.
///
/// # Errors
///
/// Returns an error if the dataset cannot be opened.
pub async fn list_branches(uri: &str) -> Result<Vec<String>> {
    let dataset = open_dataset(uri).await?;
    let branches = dataset
        .list_branches()
        .await
        .with_context(|| format!("list branches on {uri}"))?;
    Ok(branches.into_keys().collect())
}

// ─────────────────────────────────────────────────────────────────────────
// Phase 2: branch-targeted write helpers
//
// These wrap `lance::Dataset` write paths against a specific branch,
// keeping the lancedb high-level API for primary while routing fork
// writes one layer below. lancedb 0.27.1 has no branch-write surface
// (`Table::as_branch` is `todo!()`), so descent is the only option.
// ─────────────────────────────────────────────────────────────────────────

/// Append `batches` to the dataset's named branch.
///
/// Opens the dataset on `branch`, then commits an Append. The on-disk
/// dataset path is unchanged; the branch's `BranchContents` advances
/// to a new version. Primary is unaffected.
///
/// # Errors
///
/// - The dataset or branch does not exist.
/// - The append fails (schema mismatch, IO, conflict on commit).
pub async fn write_to_branch<R>(uri: &str, branch: &str, batches: R) -> Result<()>
where
    R: arrow_array::RecordBatchReader + Send + 'static,
{
    let mut on_branch = open_branch(uri, branch)
        .await
        .with_context(|| format!("open branch {branch} on {uri} for append"))?;
    on_branch
        .append(batches, None)
        .await
        .with_context(|| format!("append to branch {branch} on {uri}"))?;
    Ok(())
}

/// Delete rows on the dataset's named branch by SQL predicate.
///
/// Opens the dataset on `branch`, then commits a Delete. Lance encodes
/// deletions as tombstones at the branch tip; primary's main branch
/// is untouched.
///
/// # Errors
///
/// - The dataset or branch does not exist.
/// - The predicate fails to parse / type-check.
pub async fn delete_from_branch(uri: &str, branch: &str, predicate: &str) -> Result<()> {
    let mut on_branch = open_branch(uri, branch)
        .await
        .with_context(|| format!("open branch {branch} on {uri} for delete"))?;
    on_branch
        .delete(predicate)
        .await
        .with_context(|| format!("delete on branch {branch} on {uri} with predicate `{predicate}`"))?;
    Ok(())
}

/// Replace the branch's tip with `batches` (overwrite semantics).
///
/// Used by the rare write-path that wants atomic table-replace on a
/// fork (e.g. compaction artifacts). Implementation: delete-all
/// followed by append, both committed against the branch. Two commits,
/// not one — Lance does not yet expose a single-commit overwrite at
/// the branch level.
///
/// # Errors
///
/// - The dataset or branch does not exist.
/// - The delete or append fails.
pub async fn replace_branch_tip<R>(uri: &str, branch: &str, batches: R) -> Result<()>
where
    R: arrow_array::RecordBatchReader + Send + 'static,
{
    let mut on_branch = open_branch(uri, branch)
        .await
        .with_context(|| format!("open branch {branch} on {uri} for replace"))?;
    on_branch
        .delete("true")
        .await
        .with_context(|| format!("delete-all on branch {branch} on {uri}"))?;
    on_branch
        .append(batches, None)
        .await
        .with_context(|| format!("append on branch {branch} on {uri} after delete-all"))?;
    Ok(())
}

/// Create a new Lance dataset and immediately branch off it.
///
/// Used when a fork's transaction introduces a label or edge type
/// whose Lance dataset doesn't yet exist on primary. The flow:
///
/// 1. `Dataset::write` to `uri` with `initial_batches` (empty or seed).
///    This produces `version=1` on the dataset's main branch.
/// 2. `create_branch(uri, branch_name, version=1)` — the fork's
///    branch starts from primary's empty version 1.
///
/// Phase 2's flush path appends data to the branch *after* this helper
/// returns. Primary's main branch stays at version 1 forever (or until
/// promotion) — its schema file does not list the label, so primary
/// queries can't see this dataset by name.
///
/// # Errors
///
/// - The dataset already exists at `uri`.
/// - Either step fails. On step-2 failure the dataset *is* created on
///   primary but the branch is not; Phase 2 callers compensate by
///   invoking [`delete_branch`] (force-mode) and then retrying the
///   whole flow, since `create_branch` itself is non-idempotent.
pub async fn create_dataset_then_branch<R>(
    uri: &str,
    branch: &str,
    initial_batches: R,
) -> Result<()>
where
    R: arrow_array::RecordBatchReader + Send + 'static,
{
    Dataset::write(initial_batches, uri, None)
        .await
        .with_context(|| format!("create lance dataset at {uri}"))?;
    let parent_v = current_version(uri).await?;
    create_branch(uri, branch, parent_v)
        .await
        .with_context(|| format!("branch {branch} off newly-created dataset at {uri}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn test_batch(ids: Vec<u64>, values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(UInt64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    /// Seed a fresh dataset with one batch and return its URI + temp dir guard.
    async fn seed_dataset() -> (TempDir, String) {
        let dir = TempDir::new().unwrap();
        let uri = format!("{}/ds.lance", dir.path().display());
        let batch = test_batch(vec![1, 2, 3], vec![100, 200, 300]);
        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)].into_iter(),
            test_schema(),
        );
        Dataset::write(reader, &uri, None).await.unwrap();
        (dir, uri)
    }

    #[tokio::test]
    async fn current_version_returns_positive_value() {
        let (_dir, uri) = seed_dataset().await;
        let v = current_version(&uri).await.unwrap();
        assert!(v >= 1, "fresh dataset should have version >= 1, got {v}");
    }

    #[tokio::test]
    async fn create_open_list_delete_roundtrip() {
        let (_dir, uri) = seed_dataset().await;
        let v = current_version(&uri).await.unwrap();

        // Create
        create_branch(&uri, "fork-a", v).await.unwrap();

        // List sees it
        let branches = list_branches(&uri).await.unwrap();
        assert!(
            branches.iter().any(|b| b == "fork-a"),
            "expected fork-a in {branches:?}"
        );

        // Open on the branch and confirm it reads parent rows via base_paths
        let branched = open_branch(&uri, "fork-a").await.unwrap();
        let count = branched.count_rows(None).await.unwrap();
        assert_eq!(count, 3, "branch reads should chain to parent");

        // Delete
        delete_branch(&uri, "fork-a").await.unwrap();
        let branches = list_branches(&uri).await.unwrap();
        assert!(!branches.iter().any(|b| b == "fork-a"));
    }

    #[tokio::test]
    async fn create_branch_not_idempotent_at_same_version() {
        // Documents the contract: re-creating an existing branch errors.
        // Recovery uses force_delete_branch + retry, not idempotent retry.
        let (_dir, uri) = seed_dataset().await;
        let v = current_version(&uri).await.unwrap();
        create_branch(&uri, "fork-x", v).await.unwrap();

        let second = create_branch(&uri, "fork-x", v).await;
        assert!(second.is_err(), "second create_branch should fail");

        // After force-delete, recreate succeeds.
        delete_branch(&uri, "fork-x").await.unwrap();
        create_branch(&uri, "fork-x", v).await.unwrap();
    }

    #[tokio::test]
    async fn delete_missing_branch_is_force_safe() {
        // force_delete_branch should not error on a name that never existed;
        // recovery relies on this to clean up partial state idempotently.
        let (_dir, uri) = seed_dataset().await;
        delete_branch(&uri, "never-existed").await.unwrap();
    }

    #[tokio::test]
    async fn current_version_on_branch_tracks_branch_tip() {
        // Phase 3: branch versions advance independently of main.
        let (_dir, uri) = seed_dataset().await;
        let v_main = current_version(&uri).await.unwrap();
        create_branch(&uri, "child", v_main).await.unwrap();

        // Initially the branch tip matches the parent version it was
        // forked from (Lance stamps a BranchContents commit on creation).
        let v_branch_initial = current_version_on_branch(&uri, "child").await.unwrap();
        assert!(v_branch_initial >= v_main);

        // Append on the branch — branch tip advances, main does not.
        let batch = test_batch(vec![10, 11], vec![1000, 1100]);
        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)].into_iter(),
            test_schema(),
        );
        write_to_branch(&uri, "child", reader).await.unwrap();

        let v_branch_after = current_version_on_branch(&uri, "child").await.unwrap();
        let v_main_after = current_version(&uri).await.unwrap();
        assert!(
            v_branch_after > v_branch_initial,
            "branch tip should advance after append"
        );
        assert_eq!(
            v_main_after, v_main,
            "main version must not move when branch is written"
        );
    }

    #[tokio::test]
    async fn create_branch_from_chains_through_parent() {
        // Phase 3: nested branch reads chain through parent branch and main.
        let (_dir, uri) = seed_dataset().await;
        let v_main = current_version(&uri).await.unwrap();
        create_branch(&uri, "level1", v_main).await.unwrap();

        // Append on level1 so we can verify level2 reads see those rows.
        let batch = test_batch(vec![10, 11], vec![1000, 1100]);
        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)].into_iter(),
            test_schema(),
        );
        write_to_branch(&uri, "level1", reader).await.unwrap();

        let v_l1 = current_version_on_branch(&uri, "level1").await.unwrap();
        create_branch_from(&uri, "level2", "level1", v_l1).await.unwrap();

        // level2 should see all 5 rows: 3 from main + 2 from level1.
        let on_l2 = open_branch(&uri, "level2").await.unwrap();
        assert_eq!(on_l2.count_rows(None).await.unwrap(), 5);

        // Writes on level1 *after* level2 was created must not appear on level2.
        let batch2 = test_batch(vec![20], vec![2000]);
        let reader2 = arrow_array::RecordBatchIterator::new(
            vec![Ok(batch2)].into_iter(),
            test_schema(),
        );
        write_to_branch(&uri, "level1", reader2).await.unwrap();

        let on_l2_again = open_branch(&uri, "level2").await.unwrap();
        assert_eq!(
            on_l2_again.count_rows(None).await.unwrap(),
            5,
            "level2 must not see writes to level1 that happened after its creation"
        );
    }

    #[tokio::test]
    async fn parent_writes_after_branch_invisible_to_branch() {
        // Spec §10: snapshot isolation at fork point.
        let (_dir, uri) = seed_dataset().await;
        let v = current_version(&uri).await.unwrap();
        create_branch(&uri, "snap", v).await.unwrap();

        // Append on primary
        let batch = test_batch(vec![4, 5], vec![400, 500]);
        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)].into_iter(),
            test_schema(),
        );
        let mut primary = Dataset::open(&uri).await.unwrap();
        primary.append(reader, None).await.unwrap();

        // Branch still sees fork-point state (3 rows, not 5)
        let branched = open_branch(&uri, "snap").await.unwrap();
        let count = branched.count_rows(None).await.unwrap();
        assert_eq!(count, 3, "branch must not see post-fork primary writes");

        // Primary saw the new rows
        let primary = Dataset::open(&uri).await.unwrap();
        assert_eq!(primary.count_rows(None).await.unwrap(), 5);
    }
}
