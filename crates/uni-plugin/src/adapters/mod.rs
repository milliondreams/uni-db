// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Adapters that bridge one plugin surface to another.
//!
//! Adapters are pure plumbing — they take an `Arc<dyn TraitA>` and
//! expose it as a `dyn TraitB` so a host that already consumes B
//! doesn't need new plumbing to reach plugins authored against A.
//!
//! Adapters live here (not in `traits/`) because they are deliberately
//! cross-trait: putting `StorageCatalogTable` next to
//! [`crate::traits::storage::Storage`] would imply a `Storage` ↔
//! `CatalogTable` coupling that doesn't exist at the trait level.

// Rust guideline compliant

pub mod catalog_from_storage;

pub use catalog_from_storage::StorageCatalogTable;
