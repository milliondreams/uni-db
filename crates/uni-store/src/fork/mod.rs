// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fork persistence and lifecycle.
//!
//! - [`registry`] — `ForkRegistryHandle`: persists `catalog/fork_registry.json`
//!   and `catalog/fork_schemas/{fork_id}.json`; runs the create/drop 2PC.
//! - [`recovery`] — driver invoked from `Uni::open` that resumes any
//!   `Pending` create or `Tombstoned` drop left behind by a crash.
//!
//! The state machines and durability invariants are documented in
//! `docs/proposals/graph_fork_plan.md` §Phase 1.

// Rust guideline compliant

pub mod id_alloc;
pub mod index_builder;
pub mod recovery;
pub mod registry;
pub mod scope;
pub mod wal;
pub mod writer_factory;

pub use registry::{ForkHolderGuard, ForkRegistryHandle};
pub use scope::{ForkLocalIndexKind, ForkScope};
