// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod adjacency;
pub mod adjacency_manager;
pub mod adjacency_overlay;
pub mod arrow_convert;
pub mod compaction;
pub mod csr;
pub mod delta;
pub mod direction;
#[cfg(feature = "lance-backend")]
pub mod edge;
#[cfg(feature = "lance-backend")]
pub mod index;
pub mod index_manager;
pub mod index_rebuild;
#[cfg(feature = "lance-backend")]
pub mod inverted_index;
#[cfg(feature = "lance-backend")]
pub mod json_index;
pub mod main_edge;
pub mod main_vertex;
pub mod manager;
pub mod property_builder;
pub mod resilient_store;
pub mod shadow_csr;
pub mod value_codec;
pub mod vertex;
pub mod vid_labels;

pub use adjacency::AdjacencyDataset;
pub use adjacency_manager::AdjacencyManager;
pub use csr::CompressedSparseRow;
pub use delta::DeltaDataset;
pub use direction::Direction;
#[cfg(feature = "lance-backend")]
pub use edge::EdgeDataset;
#[cfg(feature = "lance-backend")]
pub use index::UidIndex;
pub use index_manager::{IndexManager, IndexRebuildStatus, IndexRebuildTask};
pub use index_rebuild::IndexRebuildManager;
#[cfg(feature = "lance-backend")]
pub use inverted_index::InvertedIndex;
pub use main_edge::MainEdgeDataset;
pub use main_vertex::MainVertexDataset;
pub use manager::StorageManager;
pub use resilient_store::ResilientObjectStore;
pub use vertex::VertexDataset;
pub use vid_labels::{EidTypeIndex, VidLabelsIndex};
