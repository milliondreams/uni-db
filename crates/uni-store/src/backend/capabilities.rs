// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Optional marker traits for compile-time capability guarantees.
//!
//! These are used when constructing backends directly (not through
//! `dyn StorageBackend`) to provide compile-time proof that a backend
//! supports a given capability.

use super::traits::StorageBackend;

/// Marker: this backend supports vector similarity search.
pub trait VectorSearchCapability: StorageBackend {}

/// Marker: this backend supports full-text search.
pub trait FullTextSearchCapability: StorageBackend {}

/// Marker: this backend supports scalar indexes.
pub trait ScalarIndexCapability: StorageBackend {}
