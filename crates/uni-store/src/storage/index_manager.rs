// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Index lifecycle management: creation, rebuild, and incremental updates for all index types.

#[cfg(feature = "lance-backend")]
use crate::storage::inverted_index::InvertedIndex;
use crate::storage::vertex::VertexDataset;
use anyhow::{Result, anyhow};
use arrow_array::UInt64Array;
use chrono::{DateTime, Utc};
#[cfg(feature = "lance-backend")]
use lance::index::vector::VectorIndexParams;
#[cfg(feature = "lance-backend")]
use lance_index::progress::IndexBuildProgress;
#[cfg(feature = "lance-backend")]
use lance_index::scalar::{BuiltinIndexType, InvertedIndexParams, ScalarIndexParams};
#[cfg(feature = "lance-backend")]
use lance_index::vector::bq::RQBuildParams;
#[cfg(feature = "lance-backend")]
use lance_index::vector::hnsw::builder::HnswBuildParams;
#[cfg(feature = "lance-backend")]
use lance_index::vector::ivf::IvfBuildParams;
#[cfg(feature = "lance-backend")]
use lance_index::vector::pq::PQBuildParams;
#[cfg(feature = "lance-backend")]
use lance_index::vector::sq::builder::SQBuildParams;
#[cfg(feature = "lance-backend")]
use lance_index::{DatasetIndexExt, IndexType};
#[cfg(feature = "lance-backend")]
use lance_linalg::distance::MetricType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
#[cfg(feature = "lance-backend")]
use std::collections::HashSet;
use std::sync::Arc;
#[cfg(feature = "lance-backend")]
use tracing::{debug, info, instrument, warn};
use uni_common::core::id::Vid;
#[cfg(feature = "lance-backend")]
use uni_common::core::schema::IndexDefinition;
use uni_common::core::schema::SchemaManager;
#[cfg(feature = "lance-backend")]
use uni_common::core::schema::{
    DistanceMetric, FullTextIndexConfig, InvertedIndexConfig, JsonFtsIndexConfig,
    ScalarIndexConfig, ScalarIndexType, VectorIndexConfig, VectorIndexType,
};

/// Validates that a column name contains only safe characters to prevent SQL injection.
///
/// Issue #8: Column names must be sanitized before interpolation in SQL queries.
/// Allows only alphanumeric characters and underscores.
fn is_valid_column_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Tracing-based progress reporter for Lance index builds.
///
/// Emits structured log events at each stage boundary, enabling
/// observability into index build duration and progress.
#[cfg(feature = "lance-backend")]
#[derive(Debug)]
pub struct TracingIndexProgress {
    index_name: String,
}

#[cfg(feature = "lance-backend")]
impl TracingIndexProgress {
    pub fn arc(index_name: &str) -> Arc<dyn IndexBuildProgress> {
        Arc::new(Self {
            index_name: index_name.to_string(),
        })
    }
}

#[cfg(feature = "lance-backend")]
#[async_trait::async_trait]
impl IndexBuildProgress for TracingIndexProgress {
    async fn stage_start(&self, stage: &str, total: Option<u64>, unit: &str) -> lance::Result<()> {
        info!(
            index = %self.index_name,
            stage,
            ?total,
            unit,
            "Index build stage started"
        );
        Ok(())
    }

    async fn stage_progress(&self, stage: &str, completed: u64) -> lance::Result<()> {
        debug!(
            index = %self.index_name,
            stage,
            completed,
            "Index build progress"
        );
        Ok(())
    }

    async fn stage_complete(&self, stage: &str) -> lance::Result<()> {
        info!(
            index = %self.index_name,
            stage,
            "Index build stage complete"
        );
        Ok(())
    }
}

/// Status of an index rebuild task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexRebuildStatus {
    /// Task is waiting to be processed.
    Pending,
    /// Task is currently being processed.
    InProgress,
    /// Task completed successfully.
    Completed,
    /// Task failed with an error.
    Failed,
}

/// A task representing an index rebuild operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRebuildTask {
    /// Unique identifier for this task.
    pub id: String,
    /// The label for which indexes are being rebuilt.
    pub label: String,
    /// Current status of the task.
    pub status: IndexRebuildStatus,
    /// When the task was created.
    pub created_at: DateTime<Utc>,
    /// When the task started processing.
    pub started_at: Option<DateTime<Utc>>,
    /// When the task completed (successfully or with failure).
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message if the task failed.
    pub error: Option<String>,
    /// Number of retry attempts.
    pub retry_count: u32,
}

/// Manages physical and logical indexes across all vertex datasets.
pub struct IndexManager {
    base_uri: String,
    schema_manager: Arc<SchemaManager>,
    backend: Arc<dyn crate::backend::StorageBackend>,
}

impl std::fmt::Debug for IndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexManager")
            .field("base_uri", &self.base_uri)
            .finish_non_exhaustive()
    }
}

impl IndexManager {
    /// Create a new `IndexManager` bound to `base_uri` and the given schema and backend.
    pub fn new(
        base_uri: &str,
        schema_manager: Arc<SchemaManager>,
        backend: Arc<dyn crate::backend::StorageBackend>,
    ) -> Self {
        Self {
            base_uri: base_uri.to_string(),
            schema_manager,
            backend,
        }
    }

    /// Build and persist an inverted index for set-membership queries.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn create_inverted_index(&self, config: InvertedIndexConfig) -> Result<()> {
        let label = &config.label;
        let property = &config.property;
        info!(
            "Creating Inverted Index '{}' on {}.{}",
            config.name, label, property
        );

        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        let mut index = InvertedIndex::new(&self.base_uri, config.clone()).await?;

        let ds = VertexDataset::new(&self.base_uri, label, label_meta.id);

        // Check if dataset exists
        if ds.open_raw().await.is_ok() {
            index
                .build_from_dataset(&ds, |n| info!("Indexed {} terms", n))
                .await?;
        } else {
            warn!(
                "Dataset for label '{}' not found, creating empty inverted index",
                label
            );
        }

        self.schema_manager
            .add_index(IndexDefinition::Inverted(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    /// Build and persist a vector (ANN) index on an embedding column.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn create_vector_index(&self, config: VectorIndexConfig) -> Result<()> {
        let label = &config.label;
        let property = &config.property;
        info!(
            "Creating vector index '{}' on {}.{}",
            config.name, label, property
        );

        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);

        match ds_wrapper.open_raw().await {
            Ok(mut lance_ds) => {
                let metric_type = match config.metric {
                    DistanceMetric::L2 => MetricType::L2,
                    DistanceMetric::Cosine => MetricType::Cosine,
                    DistanceMetric::Dot => MetricType::Dot,
                    _ => return Err(anyhow!("Unsupported metric: {:?}", config.metric)),
                };

                let params = match config.index_type {
                    VectorIndexType::Flat => {
                        let ivf = IvfBuildParams::new(1);
                        VectorIndexParams::with_ivf_flat_params(metric_type, ivf)
                    }
                    VectorIndexType::IvfFlat { num_partitions } => {
                        let ivf = IvfBuildParams::new(num_partitions as usize);
                        VectorIndexParams::with_ivf_flat_params(metric_type, ivf)
                    }
                    VectorIndexType::IvfPq {
                        num_partitions,
                        num_sub_vectors,
                        bits_per_subvector,
                    } => {
                        let ivf = IvfBuildParams::new(num_partitions as usize);
                        let pq = PQBuildParams::new(
                            num_sub_vectors as usize,
                            bits_per_subvector as usize,
                        );
                        VectorIndexParams::with_ivf_pq_params(metric_type, ivf, pq)
                    }
                    VectorIndexType::IvfSq { num_partitions } => {
                        let ivf = IvfBuildParams::new(num_partitions as usize);
                        let sq = SQBuildParams::default();
                        VectorIndexParams::with_ivf_sq_params(metric_type, ivf, sq)
                    }
                    VectorIndexType::IvfRq {
                        num_partitions,
                        num_bits,
                    } => {
                        let ivf = IvfBuildParams::new(num_partitions as usize);
                        let mut rq = RQBuildParams::default();
                        if let Some(bits) = num_bits {
                            rq.num_bits = bits;
                        }
                        VectorIndexParams::with_ivf_rq_params(metric_type, ivf, rq)
                    }
                    VectorIndexType::HnswFlat {
                        m,
                        ef_construction,
                        num_partitions,
                    } => {
                        let ivf = IvfBuildParams::new(num_partitions.unwrap_or(1) as usize);
                        let hnsw = HnswBuildParams::default()
                            .num_edges(m as usize)
                            .ef_construction(ef_construction as usize);
                        VectorIndexParams::ivf_hnsw(metric_type, ivf, hnsw)
                    }
                    VectorIndexType::HnswSq {
                        m,
                        ef_construction,
                        num_partitions,
                    } => {
                        let ivf = IvfBuildParams::new(num_partitions.unwrap_or(1) as usize);
                        let hnsw = HnswBuildParams::default()
                            .num_edges(m as usize)
                            .ef_construction(ef_construction as usize);
                        let sq = SQBuildParams::default();
                        VectorIndexParams::with_ivf_hnsw_sq_params(metric_type, ivf, hnsw, sq)
                    }
                    VectorIndexType::HnswPq {
                        m,
                        ef_construction,
                        num_sub_vectors,
                        num_partitions,
                    } => {
                        let ivf = IvfBuildParams::new(num_partitions.unwrap_or(1) as usize);
                        let hnsw = HnswBuildParams::default()
                            .num_edges(m as usize)
                            .ef_construction(ef_construction as usize);
                        let pq = PQBuildParams::new(num_sub_vectors as usize, 8);
                        VectorIndexParams::with_ivf_hnsw_pq_params(metric_type, ivf, hnsw, pq)
                    }
                    _ => {
                        return Err(anyhow!(
                            "Unsupported vector index type: {:?}",
                            config.index_type
                        ));
                    }
                };

                // Ignore errors during creation if dataset is empty or similar, but try
                let progress = TracingIndexProgress::arc(&config.name);
                match lance_ds
                    .create_index_builder(&[property], IndexType::Vector, &params)
                    .name(config.name.clone())
                    .replace(true)
                    .progress(progress)
                    .await
                {
                    Ok(metadata) => {
                        info!(
                            index_name = %metadata.name,
                            index_uuid = %metadata.uuid,
                            dataset_version = metadata.dataset_version,
                            "Vector index created"
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to create physical vector index (dataset might be empty): {}",
                            e
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Dataset not found for label '{}', skipping physical index creation but saving schema definition. Error: {}",
                    label, e
                );
            }
        }

        self.schema_manager
            .add_index(IndexDefinition::Vector(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    /// Build and persist a scalar (BTree) index for exact-match and range queries.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn create_scalar_index(&self, config: ScalarIndexConfig) -> Result<()> {
        let label = &config.label;
        let properties = &config.properties;
        info!(
            "Creating scalar index '{}' on {}.{:?}",
            config.name, label, properties
        );

        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);

        match ds_wrapper.open_raw().await {
            Ok(mut lance_ds) => {
                let columns: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();

                let progress = TracingIndexProgress::arc(&config.name);
                let scalar_params = match config.index_type {
                    ScalarIndexType::Bitmap => {
                        ScalarIndexParams::for_builtin(BuiltinIndexType::Bitmap)
                    }
                    ScalarIndexType::LabelList => {
                        ScalarIndexParams::for_builtin(BuiltinIndexType::LabelList)
                    }
                    _ => ScalarIndexParams::default(),
                };
                match lance_ds
                    .create_index_builder(&columns, IndexType::Scalar, &scalar_params)
                    .name(config.name.clone())
                    .replace(true)
                    .progress(progress)
                    .await
                {
                    Ok(metadata) => {
                        info!(
                            index_name = %metadata.name,
                            index_uuid = %metadata.uuid,
                            dataset_version = metadata.dataset_version,
                            "Scalar index created"
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to create physical scalar index (dataset might be empty): {}",
                            e
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Dataset not found for label '{}' (scalar index), skipping physical creation. Error: {}",
                    label, e
                );
            }
        }

        self.schema_manager
            .add_index(IndexDefinition::Scalar(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    /// Build and persist a full-text search (Lance inverted) index.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn create_fts_index(&self, config: FullTextIndexConfig) -> Result<()> {
        let label = &config.label;
        info!(
            "Creating FTS index '{}' on {}.{:?}",
            config.name, label, config.properties
        );

        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);

        match ds_wrapper.open_raw().await {
            Ok(mut lance_ds) => {
                let columns: Vec<&str> = config.properties.iter().map(|s| s.as_str()).collect();

                let fts_params =
                    InvertedIndexParams::default().with_position(config.with_positions);

                let progress = TracingIndexProgress::arc(&config.name);
                match lance_ds
                    .create_index_builder(&columns, IndexType::Inverted, &fts_params)
                    .name(config.name.clone())
                    .replace(true)
                    .progress(progress)
                    .await
                {
                    Ok(metadata) => {
                        info!(
                            index_name = %metadata.name,
                            index_uuid = %metadata.uuid,
                            dataset_version = metadata.dataset_version,
                            "FTS index created"
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to create physical FTS index (dataset might be empty): {}",
                            e
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Dataset not found for label '{}' (FTS index), skipping physical creation. Error: {}",
                    label, e
                );
            }
        }

        self.schema_manager
            .add_index(IndexDefinition::FullText(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    /// Creates a JSON Full-Text Search index on a column.
    ///
    /// This creates a Lance inverted index on the specified column,
    /// enabling BM25-based full-text search with optional phrase matching.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn create_json_fts_index(&self, config: JsonFtsIndexConfig) -> Result<()> {
        let label = &config.label;
        let column = &config.column;
        info!(
            "Creating JSON FTS index '{}' on {}.{}",
            config.name, label, column
        );

        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);

        match ds_wrapper.open_raw().await {
            Ok(mut lance_ds) => {
                let fts_params =
                    InvertedIndexParams::default().with_position(config.with_positions);

                let progress = TracingIndexProgress::arc(&config.name);
                match lance_ds
                    .create_index_builder(&[column.as_str()], IndexType::Inverted, &fts_params)
                    .name(config.name.clone())
                    .replace(true)
                    .progress(progress)
                    .await
                {
                    Ok(metadata) => {
                        info!(
                            index_name = %metadata.name,
                            index_uuid = %metadata.uuid,
                            dataset_version = metadata.dataset_version,
                            "JSON FTS index created"
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to create physical JSON FTS index (dataset might be empty): {}",
                            e
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Dataset not found for label '{}' (JSON FTS index), skipping physical creation. Error: {}",
                    label, e
                );
            }
        }

        self.schema_manager
            .add_index(IndexDefinition::JsonFullText(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    /// Remove an index both physically from the Lance dataset and from the schema.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn drop_index(&self, name: &str) -> Result<()> {
        info!("Dropping index '{}'", name);

        let idx_def = self
            .schema_manager
            .get_index(name)
            .ok_or_else(|| anyhow!("Index '{}' not found in schema", name))?;

        // Attempt physical index drop on the underlying Lance dataset.
        let label = idx_def.label();
        let schema = self.schema_manager.schema();
        if let Some(label_meta) = schema.labels.get(label) {
            let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);
            match ds_wrapper.open_raw().await {
                Ok(mut lance_ds) => {
                    if let Err(e) = lance_ds.drop_index(name).await {
                        // Log but don't fail — the index may never have been
                        // physically built (e.g. empty dataset at creation time).
                        warn!(
                            "Physical index drop for '{}' returned error (non-fatal): {}",
                            name, e
                        );
                    } else {
                        info!("Physical index '{}' dropped from Lance dataset", name);
                    }
                }
                Err(e) => {
                    debug!(
                        "Could not open dataset for label '{}' to drop physical index: {}",
                        label, e
                    );
                }
            }
        }

        self.schema_manager.remove_index(name)?;
        self.schema_manager.save().await?;
        Ok(())
    }

    /// Rebuild all indexes registered for `label` from scratch.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn rebuild_indexes_for_label(&self, label: &str) -> Result<()> {
        info!("Rebuilding all indexes for label '{}'", label);
        let schema = self.schema_manager.schema();

        // Clone and filter to avoid holding lock while async awaiting
        let indexes: Vec<_> = schema
            .indexes
            .iter()
            .filter(|idx| idx.label() == label)
            .cloned()
            .collect();

        for index in indexes {
            match index {
                IndexDefinition::Vector(cfg) => self.create_vector_index(cfg).await?,
                IndexDefinition::Scalar(cfg) => self.create_scalar_index(cfg).await?,
                IndexDefinition::FullText(cfg) => self.create_fts_index(cfg).await?,
                IndexDefinition::Inverted(cfg) => self.create_inverted_index(cfg).await?,
                IndexDefinition::JsonFullText(cfg) => self.create_json_fts_index(cfg).await?,
                _ => warn!("Unknown index type encountered during rebuild, skipping"),
            }
        }
        Ok(())
    }

    /// Create composite index for unique constraint
    #[cfg(feature = "lance-backend")]
    pub async fn create_composite_index(&self, label: &str, properties: &[String]) -> Result<()> {
        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        // Lance supports multi-column indexes
        let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);

        // We need to verify dataset exists
        if let Ok(mut ds) = ds_wrapper.open_raw().await {
            // Create composite BTree index
            let index_name = format!("{}_{}_composite", label, properties.join("_"));

            // Convert properties to slice of &str
            let columns: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();

            let progress = TracingIndexProgress::arc(&index_name);
            match ds
                .create_index_builder(&columns, IndexType::Scalar, &ScalarIndexParams::default())
                .name(index_name.clone())
                .replace(true)
                .progress(progress)
                .await
            {
                Ok(metadata) => {
                    info!(
                        index_name = %metadata.name,
                        index_uuid = %metadata.uuid,
                        dataset_version = metadata.dataset_version,
                        "Composite index created"
                    );
                }
                Err(e) => {
                    warn!("Failed to create physical composite index: {}", e);
                }
            }

            let config = ScalarIndexConfig {
                name: index_name,
                label: label.to_string(),
                properties: properties.to_vec(),
                index_type: uni_common::core::schema::ScalarIndexType::BTree,
                where_clause: None,
                metadata: Default::default(),
            };

            self.schema_manager
                .add_index(IndexDefinition::Scalar(config))?;
            self.schema_manager.save().await?;
        }

        Ok(())
    }

    /// Lookup by composite key
    pub async fn composite_lookup(
        &self,
        label: &str,
        key_values: &HashMap<String, Value>,
    ) -> Result<Option<Vid>> {
        use crate::backend::types::ScanRequest;

        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);
        let table_name = ds_wrapper.table_name();
        let backend = self.backend.as_ref();

        if !backend.table_exists(&table_name).await.unwrap_or(false) {
            return Ok(None);
        }

        // Build filter from key values
        let filter = key_values
            .iter()
            .map(|(k, v)| {
                // Issue #8: Validate column name to prevent SQL injection
                if !is_valid_column_name(k) {
                    anyhow::bail!("Invalid column name '{}': must contain only alphanumeric characters and underscores", k);
                }

                let val_str = match v {
                    Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "null".to_string(),
                    _ => v.to_string(),
                };
                // Quote column name for case sensitivity
                Ok(format!("\"{}\" = {}", k, val_str))
            })
            .collect::<Result<Vec<_>>>()?
            .join(" AND ");

        let request = ScanRequest::all(&table_name)
            .with_filter(filter)
            .with_limit(1)
            .with_columns(vec!["_vid".to_string()]);

        let batches = match backend.scan(request).await {
            Ok(b) => b,
            Err(_) => return Ok(None),
        };

        for batch in batches {
            if batch.num_rows() > 0 {
                let vid_col = batch
                    .column_by_name("_vid")
                    .ok_or_else(|| anyhow!("Missing _vid column"))?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("Invalid _vid column type"))?;

                let vid = vid_col.value(0);
                return Ok(Some(Vid::from(vid)));
            }
        }

        Ok(None)
    }

    /// Applies incremental updates to an inverted index.
    ///
    /// Instead of rebuilding the entire index, this method updates only the
    /// changed entries, making it much faster for small mutations.
    ///
    /// # Errors
    ///
    /// Returns an error if the index doesn't exist or the update fails.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self, added, removed), level = "info", fields(
        label = %config.label,
        property = %config.property
    ))]
    pub async fn update_inverted_index_incremental(
        &self,
        config: &InvertedIndexConfig,
        added: &HashMap<Vid, Vec<String>>,
        removed: &HashSet<Vid>,
    ) -> Result<()> {
        info!(
            added = added.len(),
            removed = removed.len(),
            "Incrementally updating inverted index"
        );

        let mut index = InvertedIndex::new(&self.base_uri, config.clone()).await?;
        index.apply_incremental_updates(added, removed).await
    }
}
