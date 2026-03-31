// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Pluggable storage backend abstraction.
//!
//! This module defines the [`StorageBackend`] trait that all storage backends
//! must implement, along with supporting types for queries, writes, and
//! index management.

pub mod capabilities;
#[cfg(feature = "lance-backend")]
pub mod lance;
pub mod table_names;
pub mod traits;
pub mod types;

pub use capabilities::{FullTextSearchCapability, ScalarIndexCapability, VectorSearchCapability};
pub use traits::StorageBackend;
pub use types::*;
