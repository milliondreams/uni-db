// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `ForkScope` — read-only state shared by every component of a forked
//! session.
//!
//! A `ForkScope` is owned by a forked `Session`'s `UniInner` and carries
//! everything `StorageManager` and `SchemaManager` need to resolve fork-
//! aware reads:
//!
//! - `fork_info` — registry record, including the dataset → branch map
//!   used to route Lance reads through the fork's branches.
//! - `overlay` — `SchemaDelta` merged on top of primary's schema by
//!   `UniInner::at_fork` at construction time.
//! - `registry` — back-reference for liveness queries; holders are
//!   tracked here so drop refuses while sessions are alive.
//! - `_holder` — RAII guard that decrements the holder count when the
//!   scope is dropped.
//!
//! `ForkScope` is intentionally a `Phase 1` read-only snapshot: both
//! `fork_info` and `overlay` are wrapped in `Arc` rather than `ArcSwap`.
//! Phase 2 will swap to `ArcSwap` when fork-local writes start mutating
//! the overlay; Phase 1 has nothing to mutate.

// Rust guideline compliant

use std::sync::Arc;

use dashmap::DashMap;
use uni_common::core::fork::{ForkId, ForkInfo, SchemaDelta};

use super::registry::{ForkHolderGuard, ForkRegistryHandle};

/// Read-only scope identifying a forked session.
///
/// Constructed by `Session::fork(name).build()` (Day 7) via
/// [`ForkScope::new`]. Once built, both `fork_info` and `overlay` are
/// immutable for the scope's lifetime — Phase 1 forks are read-only.
pub struct ForkScope {
    fork_id: ForkId,
    fork_info: Arc<ForkInfo>,
    overlay: Arc<SchemaDelta>,
    registry: Arc<ForkRegistryHandle>,
    /// Branches created after fork construction, e.g. by
    /// [`crate::backend::BranchedBackend`] when the fork's writer
    /// flushes to a label whose dataset wasn't branched at fork-point.
    /// Consulted alongside `fork_info.datasets` by [`Self::branch_for`]
    /// so reads on the same session see writes through the same
    /// branch that produced them. Persisted out-of-band via
    /// [`ForkRegistryHandle::register_dataset_branch`] so a restart
    /// recovers the same mapping.
    dynamic_branches: Arc<DashMap<String, String>>,
    /// RAII guard. Lifetime-tied to this `ForkScope`. Cloning the
    /// containing `Arc<ForkScope>` does *not* increment the holder
    /// count — only the constructor does, via `register_holder`.
    _holder: ForkHolderGuard,
}

impl std::fmt::Debug for ForkScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForkScope")
            .field("fork_id", &self.fork_id)
            .field("fork_name", &self.fork_info.name)
            .finish_non_exhaustive()
    }
}

impl ForkScope {
    /// Build a new fork scope, registering a holder on `registry`.
    ///
    /// `fork_info` must already be in `Active` status; callers should
    /// have walked the registry's open-or-create flow before invoking.
    /// `overlay` is the schema delta loaded from
    /// `catalog/fork_schemas/{fork_id}.json`.
    #[must_use]
    pub fn new(
        fork_info: Arc<ForkInfo>,
        overlay: Arc<SchemaDelta>,
        registry: Arc<ForkRegistryHandle>,
    ) -> Self {
        let holder = registry.register_holder(fork_info.id);
        Self {
            fork_id: fork_info.id,
            fork_info,
            overlay,
            registry,
            dynamic_branches: Arc::new(DashMap::new()),
            _holder: holder,
        }
    }

    /// Stable fork identifier.
    #[must_use]
    pub fn fork_id(&self) -> ForkId {
        self.fork_id
    }

    /// Fork registry record (cheap `Arc::clone`).
    #[must_use]
    pub fn fork_info(&self) -> Arc<ForkInfo> {
        self.fork_info.clone()
    }

    /// Schema delta to merge on top of primary's schema.
    #[must_use]
    pub fn overlay(&self) -> Arc<SchemaDelta> {
        self.overlay.clone()
    }

    /// Branch name for a given Lance dataset, if this fork has one.
    ///
    /// Used by `StorageManager` dataset factories to route reads.
    /// Consults both the immutable fork-point datasets map (set by
    /// `finish_create`) and the dynamic-branches map (populated by
    /// [`Self::register_dynamic_branch`] when a flush hits a dataset
    /// that wasn't branched at fork-point). Returns `None` only if no
    /// branch exists on either side — the BranchedBackend then either
    /// creates one on the fly or surfaces an error.
    #[must_use]
    pub fn branch_for(&self, dataset_name: &str) -> Option<String> {
        if let Some(b) = self.fork_info.datasets.get(dataset_name) {
            return Some(b.clone());
        }
        self.dynamic_branches
            .get(dataset_name)
            .map(|r| r.value().clone())
    }

    /// Record a branch created after fork-point (e.g. for a dataset
    /// that didn't exist on primary at fork creation, or for
    /// compaction-only adjacency tables).
    ///
    /// In-memory only; the caller is responsible for persisting via
    /// [`ForkRegistryHandle::register_dataset_branch`] so a restart
    /// recovers the same mapping. Idempotent — re-registering an
    /// existing entry is a no-op.
    pub fn register_dynamic_branch(&self, dataset: String, branch: String) {
        self.dynamic_branches.insert(dataset, branch);
    }

    /// Registry handle (used by admin paths to e.g. compute holder counts).
    #[must_use]
    pub fn registry(&self) -> Arc<ForkRegistryHandle> {
        self.registry.clone()
    }
}
