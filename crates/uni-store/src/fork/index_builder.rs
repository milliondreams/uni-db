// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 5a-impl — fork-local index build entry point.
//!
//! Builds a Lance scalar index on a forked session's branch. The
//! Phase 5a spike (`backend::lance_branch::tests::phase5a_spike_per_branch_index`)
//! confirmed that `Dataset::create_index_builder` against a
//! branch-checked-out dataset writes the index file branch-locally
//! and does not leak to main, so we can use Lance's native index path
//! routed through `BranchedBackend`'s branch resolution.
//!
//! The entry point is intentionally narrow: it covers the three
//! lossless Phase 5a fusion types (BTree, Sorted, VidUid). Vector ANN
//! and BM25 land in Phase 5b alongside their recall benchmarks.

// Rust guideline compliant

use anyhow::{Context, Result};
use tracing::{debug, info, instrument};

use super::scope::{ForkLocalIndexKind, ForkScope};

/// Build a fork-local index on the active fork's branch and register
/// it on `scope`.
///
/// The build path varies by `kind`:
///
/// - **`VidUid`** — no Lance index file is built. The fork's
///   branch is the index: a fork-first lookup scans the branch
///   directly via Lance's `base_paths` chain. The registry entry
///   alone tells the planner to emit `FusedVidUidLookupExec`.
/// - **`ScalarBtree`** — `lance_branch::create_scalar_index_on_branch`
///   builds a BTree on the fork's `vertices_{label}` branch via
///   `Dataset::create_index_builder` with `IndexType::Scalar`.
/// - **`Sorted`** — same Lance scalar index path, used as a sorted
///   range scan. Lance's BTree doubles as a sorted index for ORDER BY
///   queries on the indexed column.
///
/// On any kind, `scope.register_fork_local_index(label, column, kind)`
/// fires only after a successful build (or, for VidUid, immediately).
///
/// # Errors
///
/// - The fork's `vertices_{label}` branch cannot be opened.
/// - Lance's index builder rejects the column (e.g. unsupported type
///   for BTree).
/// - Object-store IO failures.
#[instrument(skip(scope, base_uri), fields(label = %label, column = %column, kind = ?kind))]
pub async fn build_fork_local_index(
    scope: &ForkScope,
    base_uri: &str,
    label: &str,
    column: &str,
    kind: ForkLocalIndexKind,
) -> Result<()> {
    // For every kind that needs an actual Lance index file, resolve
    // the fork's branch URI for the per-label dataset.
    let dataset_name = format!("vertices_{label}");
    let lookup_branch = || -> Result<String> {
        scope.branch_for(&dataset_name).ok_or_else(|| {
            anyhow::anyhow!(
                "fork-local index build for ({label}, {column}, {kind:?}): \
                 fork has no branch for {dataset_name}; write some rows to the fork first"
            )
        })
    };
    let dataset_uri = || {
        if base_uri.ends_with('/') {
            format!("{base_uri}{dataset_name}.lance")
        } else {
            format!("{base_uri}/{dataset_name}.lance")
        }
    };

    match kind {
        ForkLocalIndexKind::VidUid => {
            // No index file to build — Lance's `base_paths` chain
            // already gives us the fork's view of UIDs. The registry
            // entry below switches the planner to the fork-first
            // operator.
            debug!(
                fork_id = %scope.fork_id(),
                "VidUid fork-local 'index' is a planner marker; no Lance build needed"
            );
        }
        ForkLocalIndexKind::Sparse => {
            // No index file to build — Approach A serves fork sparse queries by a
            // brute-force branch scan re-scored via `sparse_dot`
            // (`StorageManager::sparse_search`). The registry entry below is a
            // planner/EXPLAIN marker that switches `uni.sparse.query` to the
            // `SparseDot` fused operator. A dedicated fork-local postings dataset
            // (Approach B) is deferred behind the M5 benchmark (issue #95).
            debug!(
                fork_id = %scope.fork_id(),
                "Sparse fork-local 'index' is a planner marker; retrieval is a brute-force branch scan"
            );
        }
        ForkLocalIndexKind::ScalarBtree | ForkLocalIndexKind::Sorted => {
            let branch = lookup_branch()?;
            let uri = dataset_uri();
            let index_name = format!("fork_{}_{column}_btree", scope.fork_id());
            crate::backend::lance_branch::create_scalar_index_on_branch(
                &uri,
                &branch,
                column,
                &index_name,
            )
            .await
            .with_context(|| {
                format!("build fork-local scalar index on {uri}@{branch} column={column}")
            })?;
            info!(
                fork_id = %scope.fork_id(),
                dataset = %dataset_name,
                branch = %branch,
                column = %column,
                "fork-local scalar/sorted index built"
            );
        }
        ForkLocalIndexKind::Vector => {
            let branch = lookup_branch()?;
            let uri = dataset_uri();
            let index_name = format!("fork_{}_{column}_vec", scope.fork_id());
            crate::backend::lance_branch::create_vector_index_on_branch(
                &uri,
                &branch,
                column,
                &index_name,
            )
            .await
            .with_context(|| {
                format!("build fork-local vector index on {uri}@{branch} column={column}")
            })?;
            info!(
                fork_id = %scope.fork_id(),
                dataset = %dataset_name,
                branch = %branch,
                column = %column,
                "fork-local vector index built (Phase 5b)"
            );
        }
        ForkLocalIndexKind::FullText => {
            let branch = lookup_branch()?;
            let uri = dataset_uri();
            let index_name = format!("fork_{}_{column}_fts", scope.fork_id());
            // The fork-local build path does not carry the persisted
            // `FullTextIndexConfig`, so we build with the default (standard)
            // analyzer. A custom analyzer configured on the main index is not
            // yet propagated to fork-local FTS indexes.
            debug!(
                fork_id = %scope.fork_id(),
                column = %column,
                "fork-local FTS index uses the default analyzer (custom analyzers are not \
                 propagated to fork-local indexes yet)"
            );
            let tokenizer = uni_common::core::schema::TokenizerConfig::Standard;
            crate::backend::lance_branch::create_fts_index_on_branch(
                &uri,
                &branch,
                column,
                &index_name,
                &tokenizer,
            )
            .await
            .with_context(|| {
                format!("build fork-local FTS index on {uri}@{branch} column={column}")
            })?;
            info!(
                fork_id = %scope.fork_id(),
                dataset = %dataset_name,
                branch = %branch,
                column = %column,
                "fork-local FTS index built (Phase 5b)"
            );
        }
    }
    scope.register_fork_local_index(label, column, kind);
    Ok(())
}
