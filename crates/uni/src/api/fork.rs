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
use std::time::Duration;

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
    ttl: Option<Duration>,
}

impl<'a> ForkBuilder<'a> {
    pub(crate) fn new(parent: &'a Session, name: String) -> Self {
        Self {
            parent,
            name,
            must_create: false,
            ttl: None,
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

    /// Phase 4a: set a wall-clock TTL on the fork. The background
    /// sweeper drops the fork (cascade) once `Utc::now()` passes
    /// `created_at + ttl`. Override of `UniConfig::fork_default_ttl`
    /// for this fork only.
    ///
    /// Has no effect when the fork already exists (open-or-create
    /// returns the existing entry, whose TTL was set at create time).
    #[must_use]
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Drive the open-or-create flow. Used by [`IntoFuture`] but kept
    /// `pub(crate)` for an explicit call site if ever needed.
    #[instrument(skip(self), fields(fork_name = %self.name, must_create = self.must_create))]
    pub(crate) async fn build(self) -> Result<Session> {
        // Validate the name before touching the registry / name-lock — it
        // becomes a registry key and is written into the on-disk catalog
        // (L5). Reject empty/whitespace/over-long/control-char names.
        validate_fork_name(&self.name)?;

        let parent = self.parent;
        let registry = parent.db.fork_registry.clone();

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
                // Resolve effective TTL: builder override > config default > None.
                let effective_ttl = self.ttl.or(parent.db.config.fork_default_ttl);
                create_fork_2pc(parent, &registry, self.name.clone(), effective_ttl).await?
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

        // Phase 4a: link the new forked session's cancellation to the
        // parent's so a parent cancel cascades to the child (spec §4.6).
        let parent_token = parent.cancellation_token();
        Ok(Session::new_forked(forked_inner_arc, scope, parent_token))
    }
}

impl<'a> IntoFuture for ForkBuilder<'a> {
    type Output = Result<Session>;
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.build())
    }
}

/// Maximum fork-name length. Generous; the cap is hygiene (the name is a
/// registry key and lands in the on-disk catalog), not a storage limit.
const MAX_FORK_NAME_LEN: usize = 255;

/// Validate a fork name before any registry/catalog state is created (L5).
///
/// Rejects empty/all-whitespace names, names over [`MAX_FORK_NAME_LEN`]
/// bytes, and names containing control characters (e.g. `\n`, `\0`).
fn validate_fork_name(name: &str) -> Result<()> {
    if name.is_empty() || name.chars().all(char::is_whitespace) {
        return Err(UniError::ForkNameInvalid {
            reason: "name must be non-empty and not all whitespace".to_string(),
        });
    }
    if name.len() > MAX_FORK_NAME_LEN {
        return Err(UniError::ForkNameInvalid {
            reason: format!("name exceeds {MAX_FORK_NAME_LEN} bytes"),
        });
    }
    if let Some(c) = name.chars().find(|c| c.is_control()) {
        return Err(UniError::ForkNameInvalid {
            reason: format!("name contains a control character ({c:?})"),
        });
    }
    Ok(())
}

/// Run the 4-step create 2PC and return the now-`Active` ForkInfo.
async fn create_fork_2pc(
    parent: &Session,
    registry: &Arc<ForkRegistryHandle>,
    name: String,
    ttl: Option<Duration>,
) -> Result<ForkInfo> {
    // Materialize the fork point. A fork branches off concrete Lance
    // dataset versions and resolves reads through `base_paths`; it never
    // consults the parent's in-memory L0 buffer. Any writes the parent
    // committed but has not flushed (L0) are therefore invisible to the
    // child unless we flush them to L1 (Lance) first. `flush_to_l1(None)`
    // is idempotent and a no-op when L0 is empty, so it is safe and cheap
    // for both primary and nested parents. It runs before we capture
    // `parent_snapshot_id` and the allocator HWM below, so branches are
    // created from the post-flush version.
    //
    // Snapshot isolation is unaffected: this only materializes writes the
    // parent had *already committed* at fork time. Writes committed after
    // the fork are not flushed here and stay invisible to the fork (see
    // `fork_read_only::fork_sees_fork_point_state_after_primary_writes`).
    //
    // Fixes #97: before this, a primary-parent fork (and especially an
    // in-memory DB, which never auto-flushes) branched off a stale/empty
    // Lance tip and read zero rows for data the parent read correctly.
    // The flush used to be gated on `parent.is_forked()`, leaving the
    // primary case to a `db.flush()`-before-fork convention that uni-db
    // neither requires nor recommends.
    // Materialize *and* atomically capture the fork point (M1). A single
    // `flush_and_capture_fork_point` call flushes the parent's
    // committed-but-unflushed L0 and, under the same held `flush_lock`,
    // reads the allocator HWM and every existing dataset's Lance version.
    // Capturing under the lock closes the window in which a concurrent
    // parent commit/flush could advance the allocator or a dataset tip
    // between the flush and the reads — otherwise the fork could branch
    // off post-fork-point rows or bootstrap a stale HWM and collide VIDs.
    // The captured per-dataset versions are passed into
    // `build_datasets_for_fork` so branches are cut at the fork-point
    // version rather than a re-read live tip.
    let candidate_datasets = fork_candidate_dataset_names(&parent.db.schema.schema());
    let fork_point = if let Some(writer_lock) = &parent.db.writer {
        let writer: &uni_store::Writer = writer_lock.as_ref();
        writer
            .flush_and_capture_fork_point(&candidate_datasets)
            .await
            .map_err(UniError::Internal)?
    } else {
        uni_store::ForkPoint::default()
    };

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
    let mut info = ForkInfo::new_pending(
        fork_id,
        name.clone(),
        parent_snapshot_id.clone(),
        schema_version,
    );
    // Bootstrap the fork's MVCC version floor to the parent's fork-point
    // version HWM so a fork transaction's `_version <= pin` read still sees
    // inherited (base_paths) rows. Without this the fork starts at version
    // 0 and in-tx reads filter out every inherited row.
    info.fork_point_version_hwm = fork_point.version_hwm;
    // Phase 3: record the parent fork id when this is a nested fork.
    // `parent_fork_id == None` ⇒ parent is primary.
    info.parent_fork_id = parent.fork_scope().map(|s| s.fork_id());
    // Phase 4a: stamp TTL expiry at create time. The sweeper reads
    // `ttl_expires_at` and drops once `Utc::now()` is past it.
    if let Some(d) = ttl
        && let Ok(chrono_d) = chrono::Duration::from_std(d)
    {
        info.ttl_expires_at = Some(chrono::Utc::now() + chrono_d);
    }

    // Step 2: persist the Pending entry.
    registry.begin_create(info).await?;

    // Phase 2 Day 7: bootstrap the fork's IdAllocator above primary's
    // current HWM. Without this, the fork starts at VID 0 and collides
    // with primary's pre-existing rows visible through the `base_paths`
    // chain — Lance read-merge then shadows the fork's writes. See
    // module rustdoc on `uni_store::fork::id_alloc` for the full
    // discussion. The HWM was captured atomically with the flush above
    // (under `flush_lock`), so it cannot have drifted from the dataset
    // versions the branches are cut at.
    if let Err(e) = uni_store::fork::id_alloc::bootstrap_fork_from_primary_hwm(
        parent.db.storage.store(),
        &fork_id,
        fork_point.vid_hwm,
        fork_point.eid_hwm,
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
    let datasets =
        match build_datasets_for_fork(parent, fork_id, &fork_point.dataset_versions).await {
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

/// Candidate Lance dataset names a fork may branch, derived from `schema`.
///
/// The single source of truth for fork dataset naming, shared by the
/// fork-point version capture ([`uni_store::Writer::flush_and_capture_fork_point`])
/// and [`build_datasets_for_fork`] so the captured versions key exactly
/// the datasets the fork later branches. Includes the main `vertices` /
/// `edges` tables, per-label `vertices_{label}`, and per-edge-type
/// `deltas_{type}_{dir}` / `adjacency_{type}_{dir}` tables. Membership is
/// schema-derived; on-disk existence is checked by the consumer.
fn fork_candidate_dataset_names(schema: &uni_common::core::schema::Schema) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();

    // Main label-agnostic tables. `flush_to_l1` always writes here.
    names.push("vertices".to_string());
    names.push("edges".to_string());

    // Per-label vertex tables.
    for label in schema.labels.keys() {
        names.push(format!("vertices_{label}"));
    }

    // Per-edge-type delta + adjacency tables.
    for edge_type in schema.edge_types.keys() {
        names.push(format!("deltas_{edge_type}_fwd"));
        names.push(format!("deltas_{edge_type}_bwd"));
        names.push(format!("adjacency_{edge_type}_fwd"));
        names.push(format!("adjacency_{edge_type}_bwd"));
    }

    names
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
    captured_versions: &BTreeMap<String, u64>,
) -> Result<BTreeMap<String, String>> {
    let schema = parent.db.schema.schema();
    let storage_uri = parent.db.storage.base_uri().to_string();

    let mut branches: BTreeMap<String, String> = BTreeMap::new();
    let candidate_names = fork_candidate_dataset_names(&schema);

    // Phase 3: when the parent is a forked session, route every Lance
    // `create_branch` call through the parent's branch so the child's
    // `base_paths` chain is `child_branch → parent_branch → main`.
    // When the parent is primary, the parent branch is implicitly main
    // and the legacy `current_version` / `create_branch` helpers apply.
    let parent_scope = parent.fork_scope();

    for dataset_name in candidate_names {
        let dataset_uri = join_uri(&storage_uri, &dataset_name);
        if !path_exists(&dataset_uri) {
            continue;
        }

        let branch_name = format!("fork_{fork_id}_{dataset_name}");
        match parent_scope
            .as_ref()
            .and_then(|s| s.branch_for(&dataset_name))
        {
            Some(parent_branch) => {
                // Nested fork: branch off the parent fork's branch tip.
                let parent_v =
                    lance_branch::current_version_on_branch(&dataset_uri, &parent_branch)
                        .await
                        .map_err(|e| UniError::ForkLifecycle {
                            name: format!("<fork:{fork_id}>"),
                            stage: "current_version_on_branch",
                            source: e.into(),
                        })?;
                lance_branch::create_branch_from(
                    &dataset_uri,
                    &branch_name,
                    &parent_branch,
                    parent_v,
                )
                .await
                .map_err(|e| UniError::ForkLifecycle {
                    name: format!("<fork:{fork_id}>"),
                    stage: "create_branch_from",
                    source: e.into(),
                })?;
            }
            None => {
                // Either parent is primary (no scope), or parent is a
                // fork that has no branch for this dataset yet. In both
                // cases the legacy "branch off main" path is correct:
                // - primary case: we want main as the child's ancestor.
                // - nested-but-unbranched case: we'd otherwise need to
                //   branch off "main with no parent fork branch", which
                //   means the parent fork never wrote this dataset, so
                //   main *is* the correct ancestor for the child as
                //   well. (The child can write through on-the-fly
                //   creation later; the registry record we build here
                //   reflects only the primary-known dataset.)
                if parent_scope.is_some() {
                    // No parent branch for this dataset — defer to
                    // on-the-fly creation when the child first writes,
                    // exactly like a primary-parent fork would for a
                    // brand-new label. Skip eager branching here.
                    continue;
                }
                // Primary-parent fork (M1): branch at the version captured
                // atomically with the flush, not a re-read of the live
                // tip. The captured map is built from the same candidate
                // list + existence check, so the fallback (a dataset that
                // somehow materialized after capture) should never fire,
                // but keep it for robustness.
                let parent_v = match captured_versions.get(&dataset_name) {
                    Some(v) => *v,
                    None => lance_branch::current_version(&dataset_uri)
                        .await
                        .map_err(|e| UniError::ForkLifecycle {
                            name: format!("<fork:{fork_id}>"),
                            stage: "current_version",
                            source: e.into(),
                        })?,
                };
                lance_branch::create_branch(&dataset_uri, &branch_name, parent_v)
                    .await
                    .map_err(|e| UniError::ForkLifecycle {
                        name: format!("<fork:{fork_id}>"),
                        stage: "create_branch",
                        source: e.into(),
                    })?;
            }
        }
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
