// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fork persistence and lifecycle.
//!
//! - [`registry`] — `ForkRegistryHandle`: persists `catalog/fork_registry.json`
//!   and `catalog/fork_schemas/{fork_id}.json`; runs the create/drop 2PC.
//! - [`recovery`] — driver invoked from `Uni::open` that resumes any
//!   `Pending` create or `Tombstoned` drop left behind by a crash.

pub mod id_alloc;
pub mod index_builder;
pub mod recovery;
pub mod registry;
pub mod scope;
pub mod wal;
pub mod writer_factory;

pub use registry::{ForkHolderGuard, ForkRegistryHandle};
pub use scope::{ForkLocalIndexKind, ForkScope};

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use uni_common::core::fork::ForkId;

use crate::store_utils::{DEFAULT_TIMEOUT, delete_with_timeout, list_with_timeout};

/// Best-effort, idempotent removal of every storage-side artifact a fork wrote.
///
/// Covers the fork's catalog namespace `catalog/forks/{id}/` (id allocator +
/// fork-scoped snapshot manifests + `latest`) and its WAL directory
/// `wal_forks/{id}/` (review H3). These live on the **storage** object store —
/// not the registry's metadata store — so both the drop path and crash recovery
/// call this with the storage store in hand. Errors are logged, not propagated:
/// the drop has already committed and missing objects are success.
pub async fn delete_fork_artifacts(store: &Arc<dyn ObjectStore>, fork_id: &ForkId) {
    let prefixes = [
        ObjectStorePath::from(format!("catalog/forks/{fork_id}")),
        wal::wal_prefix(fork_id),
    ];
    for prefix in prefixes {
        match list_with_timeout(store, Some(&prefix), DEFAULT_TIMEOUT).await {
            Ok(metas) => {
                for meta in metas {
                    if let Err(e) =
                        delete_with_timeout(store, &meta.location, DEFAULT_TIMEOUT).await
                    {
                        tracing::warn!(fork_id = %fork_id, path = %meta.location, "delete fork artifact returned {e}");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(fork_id = %fork_id, prefix = %prefix, "list fork artifacts returned {e}")
            }
        }
    }
}
