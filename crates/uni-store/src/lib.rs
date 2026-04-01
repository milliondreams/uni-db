// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod backend;
pub mod cloud;
pub mod compaction;
#[cfg(feature = "lance-backend")]
pub mod lancedb;
pub mod runtime;
pub mod storage;
pub mod store_utils;
pub mod snapshot {
    pub mod manager;
}

pub use backend::StorageBackend;
#[cfg(feature = "lance-backend")]
pub use backend::lance::LanceDbBackend;
pub use compaction::{CompactionStats, CompactionStatus};
pub use runtime::context::QueryContext;
pub use runtime::property_manager::PropertyManager;
pub use runtime::writer::Writer;
pub use snapshot::manager::SnapshotManager;
