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
    /// Used by `StorageManager` dataset factories to route reads. Returns
    /// `None` when the fork doesn't own a branch on the named dataset
    /// (e.g. the dataset was created on primary after the fork-point and
    /// before any fork-local writes — Phase 2 territory).
    #[must_use]
    pub fn branch_for(&self, dataset_name: &str) -> Option<&str> {
        self.fork_info
            .datasets
            .get(dataset_name)
            .map(String::as_str)
    }

    /// Registry handle (used by admin paths to e.g. compute holder counts).
    #[must_use]
    pub fn registry(&self) -> Arc<ForkRegistryHandle> {
        self.registry.clone()
    }
}
