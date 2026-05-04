// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Public `Session::fork(name)` API.
//!
//! The builder runs the open-or-create flow against the fork
//! registry, drives the create 2PC against Lance branches, constructs
//! a [`uni_store::fork::ForkScope`], and returns a forked [`Session`]
//! whose reads route through the fork's branches via `base_paths`
//! and whose writes (Phase 2 onwards) land on the fork's branches.

// Rust guideline compliant

use std::collections::BTreeMap;
use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::Arc;

use tracing::{debug, instrument};
use uni_common::api::error::{Result, UniError};
use uni_common::core::fork::{ForkId, ForkInfo, ForkStatus};
use uni_store::backend::lance_branch;
use uni_store::fork::{ForkRegistryHandle, ForkScope};

use super::session::Session;

/// Builder returned by [`Session::fork`].
///
/// Drive it with `.await` for the open-or-create flow, or `.new_().await`
/// to require fresh creation. Errors with [`UniError::ForkAlreadyExists`]
/// when `.new_()` is set and the fork already exists.
pub struct ForkBuilder<'a> {
    parent: &'a Session,
    name: String,
    must_create: bool,
}

impl<'a> ForkBuilder<'a> {
    pub(crate) fn new(parent: &'a Session, name: String) -> Self {
        Self {
            parent,
            name,
            must_create: false,
        }
    }

    /// Require that the fork is newly created.
    ///
    /// `session.fork("x").new_().await` errors with `ForkAlreadyExists`
    /// if a fork named `x` is already in the registry.
    #[must_use]
    pub fn new_(mut self) -> Self {
        self.must_create = true;
        self
    }

    /// Drive the open-or-create flow. Used by [`IntoFuture`] but kept
    /// `pub(crate)` for an explicit call site if ever needed.
    #[instrument(skip(self), fields(fork_name = %self.name, must_create = self.must_create))]
    pub(crate) async fn build(self) -> Result<Session> {
        let parent = self.parent;
        let registry = parent
            .db
            .fork_registry
            .clone();
        // Phase 1 forbids forks-of-forks: nested forks land in Phase 3.
        if parent.is_forked() {
            return Err(UniError::InvalidArgument {
                arg: "self".into(),
                message: "nested forks are not supported in Phase 1".into(),
            });
        }

        // Acquire per-name lock for the entire open-or-create flow.
        // This prevents two concurrent callers from both running create
        // 2PC against the same name.
        let name_lock = registry.name_lock(&self.name).await;
        let _name_guard = name_lock.lock().await;

        // Open-or-create dispatch.
        let info = match registry.get(&self.name).await {
            Ok(existing) => {
                if self.must_create {
                    return Err(UniError::ForkAlreadyExists { name: self.name });
                }
                match existing.status {
                    ForkStatus::Active => existing,
                    ForkStatus::Pending => {
                        return Err(UniError::ForkLifecycle {
                            name: self.name.clone(),
                            stage: "pending_open",
                            source: "fork is mid-creation; try again after recovery".into(),
                        });
                    }
                    ForkStatus::Tombstoned => {
                        return Err(UniError::ForkLifecycle {
                            name: self.name.clone(),
                            stage: "tombstoned_open",
                            source: "fork is being dropped; choose a different name".into(),
                        });
                    }
                    // ForkStatus is `#[non_exhaustive]`; future variants
                    // surface as a clear lifecycle error rather than
                    // silently opening.
                    other => {
                        return Err(UniError::ForkLifecycle {
                            name: self.name.clone(),
                            stage: "unknown_status",
                            source: format!("unknown fork status {other:?}").into(),
                        });
                    }
                }
            }
            Err(UniError::ForkNotFound { .. }) => {
                create_fork_2pc(parent, &registry, self.name.clone()).await?
            }
            Err(other) => return Err(other),
        };

        // Build the scope (per-session — its ForkHolderGuard increments
        // the registry's holder count, which `drop_fork` consults).
        let overlay = registry.load_schema_overlay(&info.id).await?;
        let scope = Arc::new(ForkScope::new(
            Arc::new(info.clone()),
            overlay,
            registry.clone(),
        ));

        // Phase 2 Day 8: same-fork sessions share an `Arc<UniInner>`
        // so commits from one session are immediately visible to
        // sibling sessions (they share the same Writer + L0). The
        // per-name `name_lock` held above serializes concurrent
        // `fork(name)` calls on the same name, so the check-then-insert
        // below is race-free.
        let forked_inner_arc = match parent
            .db
            .fork_inners
            .get(&info.id)
            .and_then(|w| w.upgrade())
        {
            Some(existing) => {
                debug!(fork_id = %info.id, fork_name = %info.name, "fork inner cache hit");
                existing
            }
            None => {
                let new_inner = Arc::new(parent.db.at_fork(scope.clone()).await?);
                parent
                    .db
                    .fork_inners
                    .insert(info.id, Arc::downgrade(&new_inner));
                debug!(fork_id = %info.id, fork_name = %info.name, "fork inner cache miss; constructed");
                new_inner
            }
        };

        Ok(Session::new_forked(forked_inner_arc, scope))
    }
}

impl<'a> IntoFuture for ForkBuilder<'a> {
    type Output = Result<Session>;
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.build())
    }
}

/// Run the 4-step create 2PC and return the now-`Active` ForkInfo.
async fn create_fork_2pc(
    parent: &Session,
    registry: &Arc<ForkRegistryHandle>,
    name: String,
) -> Result<ForkInfo> {
    // Capture parent state at fork-point: snapshot id and schema version.
    let snapshot_manager = parent.db.storage.snapshot_manager();
    let parent_snapshot_id = snapshot_manager
        .load_latest_snapshot()
        .await
        .map_err(UniError::Internal)?
        .map(|m| m.snapshot_id)
        .unwrap_or_else(|| "uninitialized".to_string());
    let schema_version = parent.db.schema.schema().schema_version;

    let fork_id = ForkId::new();
    let info = ForkInfo::new_pending(
        fork_id,
        name.clone(),
        parent_snapshot_id.clone(),
        schema_version,
    );

    // Step 2: persist the Pending entry.
    registry.begin_create(info).await?;

    // Phase 2 Day 7: bootstrap the fork's IdAllocator above primary's
    // current HWM. Without this, the fork starts at VID 0 and collides
    // with primary's pre-existing rows visible through the `base_paths`
    // chain — Lance read-merge then shadows the fork's writes. See
    // module rustdoc on `uni_store::fork::id_alloc` for the full
    // discussion. We read primary's HWM from in-memory state because
    // primary's allocator file may live on a different `ObjectStore`
    // than the fork's path resolves through.
    let (vid_hwm, eid_hwm) = if let Some(writer_lock) = &parent.db.writer {
        let writer = writer_lock.read().await;
        writer.allocator.current_hwm().await
    } else {
        (0, 0)
    };
    if let Err(e) = uni_store::fork::id_alloc::bootstrap_fork_from_primary_hwm(
        parent.db.storage.store(),
        &fork_id,
        vid_hwm,
        eid_hwm,
    )
    .await
    {
        if let Err(rb) = registry.rollback_create(&name).await {
            tracing::warn!("rollback after id-allocator bootstrap failed: {rb}");
        }
        return Err(UniError::Internal(e));
    }

    // Step 3: create one Lance branch per known dataset. Phase 1
    // collects the dataset list from the *current* schema's labels +
    // edge types. New labels created on a forked session land in
    // Phase 2 (on-the-fly dataset+branch creation).
    let datasets = match build_datasets_for_fork(parent, fork_id).await {
        Ok(ds) => ds,
        Err(e) => {
            // Recovery on next boot will roll back the Pending entry
            // and force-delete any partial branches. We also try a
            // best-effort rollback here so the state is clean if the
            // process keeps running.
            if let Err(rb) = registry.rollback_create(&name).await {
                tracing::warn!("rollback after partial create_branch failed: {rb}");
            }
            return Err(e);
        }
    };

    // Step 4: promote to Active with the full datasets map.
    let active = registry.finish_create(&name, datasets).await?;
    Ok(active)
}

/// Walk the schema's labels and edge types, calling `lance_branch::create_branch`
/// for each existing dataset. Returns the dataset → branch map.
///
/// Branches every dataset that exists on disk at fork-point, including:
/// - the main `vertices` and `edges` tables (label-agnostic; written by
///   `flush_to_l1`),
/// - per-label `vertices_{label}` tables,
/// - per-edge-type `deltas_{type}_{fwd,bwd}` tables,
/// - per-edge-type `adjacency_{type}_{fwd,bwd}` tables (if compaction
///   has produced them).
///
/// Datasets that don't exist on disk yet (e.g. a label declared in
/// schema but never written, or compaction-only adjacency tables on a
/// fresh DB) are skipped here. They get branched on-the-fly inside
/// [`crate::api::fork::ensure_branch_for`] the first time the fork's
/// writer touches them.
async fn build_datasets_for_fork(
    parent: &Session,
    fork_id: ForkId,
) -> Result<BTreeMap<String, String>> {
    let schema = parent.db.schema.schema();
    let storage_uri = parent.db.storage.base_uri().to_string();

    let mut branches: BTreeMap<String, String> = BTreeMap::new();
    let mut candidate_names: Vec<String> = Vec::new();

    // Main label-agnostic tables. `flush_to_l1` always writes here.
    candidate_names.push("vertices".to_string());
    candidate_names.push("edges".to_string());

    // Per-label vertex tables.
    for label in schema.labels.keys() {
        candidate_names.push(format!("vertices_{label}"));
    }

    // Per-edge-type delta + adjacency tables.
    for edge_type in schema.edge_types.keys() {
        candidate_names.push(format!("deltas_{edge_type}_fwd"));
        candidate_names.push(format!("deltas_{edge_type}_bwd"));
        candidate_names.push(format!("adjacency_{edge_type}_fwd"));
        candidate_names.push(format!("adjacency_{edge_type}_bwd"));
    }

    for dataset_name in candidate_names {
        let dataset_uri = join_uri(&storage_uri, &dataset_name);
        if !path_exists(&dataset_uri) {
            continue;
        }
        let parent_v = lance_branch::current_version(&dataset_uri)
            .await
            .map_err(|e| UniError::ForkLifecycle {
                name: format!("<fork:{fork_id}>"),
                stage: "current_version",
                source: e.into(),
            })?;
        let branch_name = format!("fork_{fork_id}_{dataset_name}");
        lance_branch::create_branch(&dataset_uri, &branch_name, parent_v)
            .await
            .map_err(|e| UniError::ForkLifecycle {
                name: format!("<fork:{fork_id}>"),
                stage: "create_branch",
                source: e.into(),
            })?;
        branches.insert(dataset_name, branch_name);
    }

    Ok(branches)
}

fn join_uri(base: &str, dataset: &str) -> String {
    if base.ends_with('/') {
        format!("{base}{dataset}.lance")
    } else {
        format!("{base}/{dataset}.lance")
    }
}

/// Cheap on-disk existence check. Used to skip branching for labels
/// that have no rows yet (no `.lance` directory). For non-local
/// stores this conservatively returns `true`, deferring the existence
/// check to `lance_branch::current_version` which surfaces the right
/// error.
fn path_exists(uri: &str) -> bool {
    // Local-fs heuristic: if it parses as a URL with a scheme, assume
    // remote. Otherwise check the path on disk.
    if uri.contains("://") {
        return true;
    }
    std::path::Path::new(uri).exists()
}
