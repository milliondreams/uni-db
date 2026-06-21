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
//! driver must call `force_delete_branch` before retrying. See
//! `crates/uni-store/src/fork/recovery.rs` for the recovery logic.

// Rust guideline compliant

use std::sync::Arc;

use crate::backend::types::FilterExpr;
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
/// `force_delete_branch` to clean up before retry.
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
            format!("create branch {new_branch} off {parent_branch} on {uri} at v{parent_version}")
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

/// Create a scalar BTree index on a branch-checked-out dataset
/// (Phase 5a-impl). The Phase 5a spike confirmed Lance writes the
/// index file branch-locally — main sees zero indexes after this
/// call; only the branch sees the new one.
///
/// Used by `fork_index_builder::build_fork_local_index` to land
/// fork-local BTree / Sorted indexes without touching primary's
/// index set.
///
/// # Errors
///
/// - The dataset or branch cannot be opened.
/// - Lance rejects the column (e.g. unsupported type).
/// - Object-store IO failures.
pub async fn create_scalar_index_on_branch(
    uri: &str,
    branch: &str,
    column: &str,
    index_name: &str,
) -> Result<()> {
    use lance::index::DatasetIndexExt;
    use lance_index::{IndexType, scalar::ScalarIndexParams};
    let mut on_branch = open_branch(uri, branch).await?;
    on_branch
        .create_index_builder(&[column], IndexType::Scalar, &ScalarIndexParams::default())
        .name(index_name.to_string())
        .replace(true)
        .await
        .with_context(|| {
            format!("create_scalar_index_on_branch({uri}@{branch}, column={column})")
        })?;
    Ok(())
}

/// Phase 5b: vector kNN search against a branch-checked-out dataset.
/// Lance's `base_paths` chain on the branch surfaces both the
/// fork-local rows and the parent-inherited rows in one scan, so a
/// single nearest-K call returns the fused result set.
///
/// Used by `BranchedBackend::vector_search` when the fork has a
/// branch for the target dataset. When the fork has no branch
/// (label never written through the fork), the BranchedBackend
/// delegates to primary's vector_search directly.
///
/// # Errors
///
/// - The dataset or branch cannot be opened.
/// - Lance rejects the query (column type mismatch, dimension mismatch).
pub async fn vector_search_on_branch(
    uri: &str,
    branch: &str,
    column: &str,
    query: &[f32],
    k: usize,
    filter: &FilterExpr,
) -> Result<Vec<arrow_array::RecordBatch>> {
    use arrow_array::Float32Array;
    use futures::TryStreamExt;

    let on_branch = open_branch(uri, branch).await?;
    let key = Float32Array::from(query.to_vec());
    let mut scanner = on_branch.scan();
    scanner
        .nearest(column, &key, k)
        .map_err(|e| anyhow::anyhow!("vector_search_on_branch nearest({column}, k={k}): {e}"))?;
    // M6: honor the caller's filter (user predicate + `_deleted = false`
    // + version HWM pin). Prefilter so excluded rows never consume a
    // top-k slot — otherwise a soft-deleted or out-of-version row could
    // shadow a live match and shrink the result below k.
    if let FilterExpr::Sql(sql) = filter {
        scanner.prefilter(true);
        scanner
            .filter(sql)
            .map_err(|e| anyhow::anyhow!("vector_search_on_branch filter('{sql}'): {e}"))?;
    }
    let stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| anyhow::anyhow!("vector_search_on_branch stream: {e}"))?;
    stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::anyhow!("vector_search_on_branch collect: {e}"))
}

/// Phase 5b: BM25 full-text search against a branch-checked-out
/// dataset. Same `base_paths` semantics as `vector_search_on_branch`
/// — the chain gives a fused result set.
///
/// # Errors
///
/// - The dataset or branch cannot be opened.
/// - Lance rejects the query.
pub async fn full_text_search_on_branch(
    uri: &str,
    branch: &str,
    column: &str,
    query: &str,
    k: usize,
    filter: &FilterExpr,
) -> Result<Vec<arrow_array::RecordBatch>> {
    use futures::TryStreamExt;
    use lance_index::scalar::FullTextSearchQuery;
    use lance_index::scalar::inverted::query::MatchQuery;

    let on_branch = open_branch(uri, branch).await?;
    let match_query = MatchQuery::new(query.to_string()).with_column(Some(column.to_string()));
    let fts_query = FullTextSearchQuery {
        query: match_query.into(),
        limit: Some(k as i64),
        wand_factor: None,
    };
    let mut scanner = on_branch.scan();
    scanner
        .full_text_search(fts_query)
        .map_err(|e| anyhow::anyhow!("full_text_search_on_branch({column}, k={k}): {e}"))?;
    // M6: honor the caller's filter (user predicate + `_deleted = false`
    // + version HWM pin), prefiltered so excluded rows don't take a slot.
    if let FilterExpr::Sql(sql) = filter {
        scanner.prefilter(true);
        scanner
            .filter(sql)
            .map_err(|e| anyhow::anyhow!("full_text_search_on_branch filter('{sql}'): {e}"))?;
    }
    let stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| anyhow::anyhow!("full_text_search_on_branch stream: {e}"))?;
    stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::anyhow!("full_text_search_on_branch collect: {e}"))
}

/// Create a Lance vector index on a branch-checked-out dataset (Phase 5b).
/// The Phase 5b spike confirmed Lance writes the vector index file
/// branch-locally — main sees zero indexes after; only the branch
/// sees the new one. The fork's branch reads via `base_paths` then
/// produce results that include parent-inherited rows when relevant.
///
/// Defaults to a 1-partition IVF-Flat with L2 distance — the
/// simplest configuration that always builds. Phase 5b's MVP doesn't
/// expose the IVF/PQ knobs because the auto-build path is opt-in via
/// `Session::build_fork_local_index` and matches whatever the user
/// explicitly asked for.
///
/// # Errors
///
/// - The dataset or branch cannot be opened.
/// - Lance rejects the column (e.g. wrong type for vector index —
///   must be `FixedSizeList<Float32, dim>`).
/// - Object-store IO failures.
pub async fn create_vector_index_on_branch(
    uri: &str,
    branch: &str,
    column: &str,
    index_name: &str,
) -> Result<()> {
    use lance::index::DatasetIndexExt;
    use lance::index::vector::VectorIndexParams;
    use lance_index::IndexType;
    use lance_linalg::distance::MetricType;

    let mut on_branch = open_branch(uri, branch).await?;
    let params = VectorIndexParams::ivf_flat(1, MetricType::L2);
    on_branch
        .create_index(
            &[column],
            IndexType::Vector,
            Some(index_name.to_string()),
            &params,
            true,
        )
        .await
        .with_context(|| {
            format!("create_vector_index_on_branch({uri}@{branch}, column={column})")
        })?;
    Ok(())
}

/// Create a Lance native FTS / inverted index on a branch-checked-out
/// dataset (Phase 5b).
///
/// Same per-branch semantics as `create_vector_index_on_branch`.
/// Used by `fork_index_builder::build_fork_local_index` for the
/// `FullText` kind.
///
/// # Errors
///
/// - The dataset or branch cannot be opened.
/// - The column type is not text.
/// - Object-store IO failures.
pub async fn create_fts_index_on_branch(
    uri: &str,
    branch: &str,
    column: &str,
    index_name: &str,
) -> Result<()> {
    use lance::index::DatasetIndexExt;
    use lance_index::{IndexType, scalar::InvertedIndexParams};

    let mut on_branch = open_branch(uri, branch).await?;
    // Mirrors `IndexManager::create_fts_index`: uses
    // `IndexType::Inverted` (not Scalar) and `InvertedIndexParams`
    // which carries the required `base_tokenizer` config that
    // ScalarIndexParams::for_builtin(Inverted) doesn't set.
    let fts_params = InvertedIndexParams::default();
    on_branch
        .create_index_builder(&[column], IndexType::Inverted, &fts_params)
        .name(index_name.to_string())
        .replace(true)
        .await
        .with_context(|| format!("create_fts_index_on_branch({uri}@{branch}, column={column})"))?;
    Ok(())
}

/// Create a Lance tag pinning `branch`'s current tip (Phase 4a).
///
/// Tags are GC-exempt references — Lance's compaction retention sweep
/// preserves any version referenced by an active tag. This is what
/// lets `Uni::tag_fork` keep a fork's state on disk after the fork
/// itself is dropped (e.g. for audit / regulatory hold).
///
/// The reference resolved at creation time is the branch's current
/// version: subsequent writes on the branch do not "follow" the tag.
///
/// # Errors
///
/// - The dataset or `branch` cannot be opened.
/// - A tag named `tag` already exists (Lance returns `RefConflict`).
/// - Object-store IO fails.
pub async fn create_tag(uri: &str, tag: &str, branch: &str) -> Result<()> {
    let on_branch = open_branch(uri, branch).await?;
    let version = on_branch.version().version;
    let dataset = open_dataset(uri).await?;
    dataset
        .tags()
        .create(tag, (branch, version))
        .await
        .with_context(|| format!("create tag {tag} on {uri} -> {branch}@v{version}"))?;
    Ok(())
}

/// Delete a Lance tag (Phase 4a). Idempotent: a missing tag is treated
/// as success so callers don't have to special-case re-runs.
///
/// # Errors
///
/// Object-store IO failures unrelated to the missing-tag case.
pub async fn delete_tag(uri: &str, tag: &str) -> Result<()> {
    let dataset = open_dataset(uri).await?;
    let existing = dataset
        .tags()
        .list()
        .await
        .with_context(|| format!("list tags on {uri}"))?;
    if !existing.contains_key(tag) {
        return Ok(());
    }
    dataset
        .tags()
        .delete(tag)
        .await
        .with_context(|| format!("delete tag {tag} on {uri}"))
}

/// List all tags on the dataset, returning `(name, pinned_version)`
/// pairs (Phase 4a).
///
/// # Errors
///
/// Object-store IO failures.
pub async fn list_tags(uri: &str) -> Result<Vec<(String, u64)>> {
    let dataset = open_dataset(uri).await?;
    let map = dataset
        .tags()
        .list()
        .await
        .with_context(|| format!("list tags on {uri}"))?;
    Ok(map
        .into_iter()
        .map(|(name, contents)| (name, contents.version))
        .collect())
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
    on_branch.delete(predicate).await.with_context(|| {
        format!("delete on branch {branch} on {uri} with predicate `{predicate}`")
    })?;
    Ok(())
}

/// Merge-insert (update-only) `batches` into the dataset's named branch.
///
/// Opens the dataset on `branch` and runs Lance's `MergeInsertBuilder`
/// keyed on `on`, applying `WhenMatched::UpdateAll`. Like the primary
/// [`crate::backend::lance::LanceDbBackend::merge_insert`], it deliberately
/// does NOT insert unmatched source rows (CREATE goes through the Append
/// path); the partial/tombstone source only updates existing rows. The
/// commit lands on the branch tip; primary's main branch is untouched.
///
/// This is what lets a fork update or soft-delete an *inherited*
/// (base_paths) vertex: the flush emits a partial `_deleted`/`_version`
/// (or touched-column) batch keyed by `_vid`, which matches the inherited
/// row visible through the branch chain and shadows it on the branch.
///
/// # Errors
///
/// - The dataset or branch does not exist.
/// - The merge build/execute fails (schema mismatch, IO, commit conflict).
pub async fn merge_insert_on_branch<R>(
    uri: &str,
    branch: &str,
    on: &[&str],
    batches: R,
) -> Result<()>
where
    R: arrow_array::RecordBatchReader + Send + 'static,
{
    use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched};

    let on_branch = open_branch(uri, branch)
        .await
        .with_context(|| format!("open branch {branch} on {uri} for merge_insert"))?;
    let mut builder = MergeInsertBuilder::try_new(
        Arc::new(on_branch),
        on.iter().map(|s| (*s).to_string()).collect(),
    )
    .with_context(|| format!("merge_insert builder on branch {branch} of {uri}"))?;
    builder.when_matched(WhenMatched::UpdateAll);
    // Update-only: drop unmatched source rows. The flush source is a
    // partial-column batch (e.g. tombstone `_vid`/`_deleted`/`_version`),
    // so inserting unmatched rows would fail on non-nullable target columns
    // and is wrong anyway — CREATE goes through the Append path. The lance
    // `Dataset` builder defaults to InsertAll (unlike the lancedb `Table`),
    // so this must be set explicitly.
    builder.when_not_matched(WhenNotMatched::DoNothing);
    let job = builder
        .try_build()
        .with_context(|| format!("merge_insert build on branch {branch} of {uri}"))?;
    let boxed: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(batches);
    job.execute_reader(boxed)
        .await
        .with_context(|| format!("merge_insert execute on branch {branch} of {uri}"))?;
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
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
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
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
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
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
        write_to_branch(&uri, "level1", reader).await.unwrap();

        let v_l1 = current_version_on_branch(&uri, "level1").await.unwrap();
        create_branch_from(&uri, "level2", "level1", v_l1)
            .await
            .unwrap();

        // level2 should see all 5 rows: 3 from main + 2 from level1.
        let on_l2 = open_branch(&uri, "level2").await.unwrap();
        assert_eq!(on_l2.count_rows(None).await.unwrap(), 5);

        // Writes on level1 *after* level2 was created must not appear on level2.
        let batch2 = test_batch(vec![20], vec![2000]);
        let reader2 =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch2)].into_iter(), test_schema());
        write_to_branch(&uri, "level1", reader2).await.unwrap();

        let on_l2_again = open_branch(&uri, "level2").await.unwrap();
        assert_eq!(
            on_l2_again.count_rows(None).await.unwrap(),
            5,
            "level2 must not see writes to level1 that happened after its creation"
        );
    }

    #[tokio::test]
    async fn create_list_delete_tag_roundtrip() {
        let (_dir, uri) = seed_dataset().await;
        let v = current_version(&uri).await.unwrap();
        create_branch(&uri, "to-tag", v).await.unwrap();

        // Create
        create_tag(&uri, "audit-2026", "to-tag").await.unwrap();

        // List
        let tags = list_tags(&uri).await.unwrap();
        assert!(
            tags.iter().any(|(n, _)| n == "audit-2026"),
            "tags = {tags:?}"
        );

        // Delete idempotent
        delete_tag(&uri, "audit-2026").await.unwrap();
        let tags2 = list_tags(&uri).await.unwrap();
        assert!(!tags2.iter().any(|(n, _)| n == "audit-2026"));

        // Re-deleting a missing tag is a no-op.
        delete_tag(&uri, "audit-2026").await.unwrap();
    }

    #[tokio::test]
    async fn create_tag_pins_version_at_call_time() {
        // Phase 4a contract: tag captures the branch's tip at create
        // time and does not "follow" subsequent writes. This is what
        // makes a tagged-then-dropped fork safe to retain on disk.
        let (_dir, uri) = seed_dataset().await;
        let v = current_version(&uri).await.unwrap();
        create_branch(&uri, "snap-branch", v).await.unwrap();

        // Append a row before tagging.
        let batch = test_batch(vec![10], vec![1000]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
        write_to_branch(&uri, "snap-branch", reader).await.unwrap();

        create_tag(&uri, "v1", "snap-branch").await.unwrap();
        let tags = list_tags(&uri).await.unwrap();
        let (_, pinned_v) = tags.iter().find(|(n, _)| n == "v1").unwrap();

        // Append more after tagging; the tag's pinned version must not move.
        let batch2 = test_batch(vec![11], vec![1100]);
        let reader2 =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch2)].into_iter(), test_schema());
        write_to_branch(&uri, "snap-branch", reader2).await.unwrap();

        let tags_after = list_tags(&uri).await.unwrap();
        let (_, pinned_after) = tags_after.iter().find(|(n, _)| n == "v1").unwrap();
        assert_eq!(
            pinned_v, pinned_after,
            "tag must pin to fork-time version, not follow branch tip"
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
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
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

    /// Phase 5b spike: probe whether Lance's vector_search via
    /// `Scanner::nearest` traverses `base_paths` for indexes the same
    /// way scalar BTree reads do. If yes, fork-local vector indexes
    /// fuse with parent's automatically (no bespoke fusion needed).
    /// If no, Phase 5b needs a real `FusedVectorSearchExec`.
    #[tokio::test]
    #[ignore = "phase-5b spike: documents Lance per-branch vector index behavior; run with --run-ignored ignored-only"]
    async fn phase5b_spike_per_branch_vector() {
        use arrow_array::{Float32Array, RecordBatch as Batch, UInt64Array};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use lance::index::DatasetIndexExt;
        use lance::index::vector::VectorIndexParams;
        use lance_index::IndexType;
        use lance_linalg::distance::MetricType;

        let dir = TempDir::new().unwrap();
        let uri = format!("{}/vec_ds.lance", dir.path().display());

        // Schema: id u64 + vector FixedSizeList<Float32, 4>.
        let vec_field = Field::new("item", DataType::Float32, false);
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(vec_field.clone()), 4),
                false,
            ),
        ]));

        // Helper to build a batch with 4-d vectors.
        let make_batch = |ids: Vec<u64>, vecs: Vec<[f32; 4]>| -> Batch {
            let flat: Vec<f32> = vecs.into_iter().flatten().collect();
            let values = Float32Array::from(flat);
            let list = arrow_array::FixedSizeListArray::new(
                Arc::new(vec_field.clone()),
                4,
                Arc::new(values),
                None,
            );
            Batch::try_new(
                schema.clone(),
                vec![Arc::new(UInt64Array::from(ids)), Arc::new(list)],
            )
            .unwrap()
        };

        // Seed primary with 100 rows, all near origin.
        let primary_batch = {
            let ids: Vec<u64> = (0..100).collect();
            let vecs: Vec<[f32; 4]> = (0..100)
                .map(|i| [(i as f32) * 0.001, 0.0, 0.0, 0.0])
                .collect();
            make_batch(ids, vecs)
        };
        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(primary_batch)].into_iter(),
            schema.clone(),
        );
        Dataset::write(reader, &uri, None).await.unwrap();

        // Build a vector index on primary's main branch.
        let mut main_ds = Dataset::open(&uri).await.unwrap();
        let params = VectorIndexParams::ivf_flat(1, MetricType::L2);
        main_ds
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("primary_vec".into()),
                &params,
                true,
            )
            .await
            .unwrap();

        // Branch off main and append 5 fork-only rows clustered far
        // away (so they're easy to distinguish in nearest-N results).
        let v_main = current_version(&uri).await.unwrap();
        create_branch(&uri, "fork-vec", v_main).await.unwrap();
        let fork_batch = {
            let ids: Vec<u64> = (1000..1005).collect();
            let vecs: Vec<[f32; 4]> = (0..5)
                .map(|i| [100.0 + (i as f32), 0.0, 0.0, 0.0])
                .collect();
            make_batch(ids, vecs)
        };
        let reader2 =
            arrow_array::RecordBatchIterator::new(vec![Ok(fork_batch)].into_iter(), schema.clone());
        write_to_branch(&uri, "fork-vec", reader2).await.unwrap();

        // Probe 1: query near 100.0 on the FORK branch — does the
        // result include the fork-only rows?
        let on_branch = open_branch(&uri, "fork-vec").await.unwrap();
        let query = Float32Array::from(vec![100.5_f32, 0.0, 0.0, 0.0]);
        let mut scanner = on_branch.scan();
        scanner.nearest("vector", &query, 5).unwrap();
        let stream = scanner.try_into_stream().await.unwrap();
        let batches = futures::TryStreamExt::try_collect::<Vec<_>>(stream)
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        let mut ids: Vec<u64> = Vec::new();
        for b in &batches {
            let id_col = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                ids.push(id_col.value(i));
            }
        }
        eprintln!("SPIKE: branch nearest-5 to [100.5,0,0,0]: {total} rows, ids={ids:?}");
        let saw_fork_id = ids.iter().any(|i| *i >= 1000);
        let saw_primary_id = ids.iter().any(|i| *i < 1000);
        eprintln!(
            "SPIKE VERDICT: branch sees fork rows={saw_fork_id} parent rows={saw_primary_id}"
        );

        // Probe 2: try to build a vector index on the fork branch.
        let mut on_branch_mut = open_branch(&uri, "fork-vec").await.unwrap();
        let result = on_branch_mut
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("fork_vec".into()),
                &params,
                true,
            )
            .await;
        match result {
            Ok(_) => {
                let main_after = Dataset::open(&uri).await.unwrap();
                let main_indices = main_after.load_indices().await.unwrap();
                let branch_after = open_branch(&uri, "fork-vec").await.unwrap();
                let branch_indices = branch_after.load_indices().await.unwrap();
                let main_has_fork = main_indices
                    .iter()
                    .any(|i: &lance::table::format::IndexMetadata| i.name == "fork_vec");
                let branch_has_fork = branch_indices
                    .iter()
                    .any(|i: &lance::table::format::IndexMetadata| i.name == "fork_vec");
                eprintln!(
                    "SPIKE: vector index branch-local? {} leaked-to-main? {}",
                    branch_has_fork && !main_has_fork,
                    main_has_fork
                );
            }
            Err(e) => {
                eprintln!("SPIKE: vector index build on branch refused: {e}");
            }
        }
    }

    /// Phase 5a spike: probe whether `Dataset::create_index_builder`
    /// against a branch-checked-out dataset produces a branch-local
    /// index, leaks to main, or is rejected.
    ///
    /// This test isn't a behavior gate — it documents the observed
    /// Lance per-branch index semantics so the `fork_index_builder`
    /// implementation knows which path is real.
    #[tokio::test]
    #[ignore = "phase-5a spike: documents Lance per-branch index semantics; run with --run-ignored ignored-only"]
    async fn phase5a_spike_per_branch_index() {
        use lance::index::DatasetIndexExt;
        use lance_index::{IndexType, scalar::ScalarIndexParams};

        let (_dir, uri) = seed_dataset().await;
        let v_main = current_version(&uri).await.unwrap();
        create_branch(&uri, "fork-spike", v_main).await.unwrap();

        // Append fork-only rows on the branch.
        let batch = test_batch(vec![100, 101, 102], vec![1000, 1100, 1200]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
        write_to_branch(&uri, "fork-spike", reader).await.unwrap();

        // Probe 1: try to build a scalar BTree on `id` against the branch.
        let mut on_branch = open_branch(&uri, "fork-spike").await.unwrap();
        let scalar_params = ScalarIndexParams::default();
        let result = on_branch
            .create_index_builder(&["id"], IndexType::Scalar, &scalar_params)
            .name("phase5a_spike".to_string())
            .replace(true)
            .await;

        match result {
            Ok(metadata) => {
                eprintln!(
                    "SPIKE OUTCOME 1 OR 2: index created. name={} uuid={} dataset_version={}",
                    metadata.name, metadata.uuid, metadata.dataset_version
                );
                // Probe whether the index is visible from main vs only from the branch.
                let main_after = Dataset::open(&uri).await.unwrap();
                let main_indices = main_after.load_indices().await.unwrap();
                let branch_after = open_branch(&uri, "fork-spike").await.unwrap();
                let branch_indices = branch_after.load_indices().await.unwrap();
                eprintln!(
                    "main has {} index(es) after branch build; branch has {}",
                    main_indices.len(),
                    branch_indices.len()
                );
                for idx in main_indices.iter() {
                    eprintln!("  main index: name={} uuid={}", idx.name, idx.uuid);
                }
                for idx in branch_indices.iter() {
                    eprintln!("  branch index: name={} uuid={}", idx.name, idx.uuid);
                }
                let leaked_to_main = main_indices
                    .iter()
                    .any(|i: &lance::table::format::IndexMetadata| i.name == "phase5a_spike");
                let on_branch_only = branch_indices
                    .iter()
                    .any(|i: &lance::table::format::IndexMetadata| i.name == "phase5a_spike");
                eprintln!(
                    "SPIKE VERDICT: branch-local={} leaked-to-main={}",
                    on_branch_only && !leaked_to_main,
                    leaked_to_main
                );
            }
            Err(e) => {
                eprintln!("SPIKE OUTCOME 3: Lance refused per-branch index: {e}");
            }
        }
    }
}
