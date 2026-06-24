// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Index lifecycle management: creation, rebuild, and incremental updates for all index types.

use crate::backend::StorageBackend;
use crate::backend::table_names;
use crate::backend::types::ScanRequest;
#[cfg(feature = "lance-backend")]
use crate::storage::inverted_index::InvertedIndex;
use crate::storage::vertex::VertexDataset;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
#[cfg(feature = "lance-backend")]
use lance::index::DatasetIndexExt;
#[cfg(feature = "lance-backend")]
use lance::index::vector::VectorIndexParams;
#[cfg(feature = "lance-backend")]
use lance_index::IndexType;
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
use lance_linalg::distance::MetricType;
use serde::{Deserialize, Serialize};
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

/// Resolves the embedding dimension of a vector or multi-vector property type.
///
/// Recurses through `List(Vector{dim})` (multi-vector / ColBERT) to the inner
/// `Vector{dim}`; returns `None` for non-vector types.
fn resolve_vector_dim(t: &uni_common::DataType) -> Option<usize> {
    match t {
        uni_common::DataType::Vector { dimensions } => Some(*dimensions),
        uni_common::DataType::List(inner) => resolve_vector_dim(inner),
        _ => None,
    }
}

/// Manages physical and logical indexes across all vertex datasets.
pub struct IndexManager {
    base_uri: String,
    schema_manager: Arc<SchemaManager>,
    /// Storage backend, when available. Needed only for MUVERA FDE backfill (scan +
    /// `replace_table_atomic`); `None` callers (e.g. some rebuild paths) still build
    /// indexes over already-materialised columns.
    backend: Option<Arc<dyn StorageBackend>>,
}

impl std::fmt::Debug for IndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexManager")
            .field("base_uri", &self.base_uri)
            .finish_non_exhaustive()
    }
}

impl IndexManager {
    /// Create a new `IndexManager` bound to `base_uri` and the given schema, without a
    /// storage backend (MUVERA backfill over pre-existing rows is unavailable).
    pub fn new(base_uri: &str, schema_manager: Arc<SchemaManager>) -> Self {
        Self {
            base_uri: base_uri.to_string(),
            schema_manager,
            backend: None,
        }
    }

    /// Attach a storage backend, enabling MUVERA FDE backfill over already-flushed rows.
    pub fn with_backend(mut self, backend: Arc<dyn StorageBackend>) -> Self {
        self.backend = Some(backend);
        self
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

    /// Build and persist a vector (ANN) index on an embedding column. This is the SINGLE
    /// build path every creation surface converges on (Cypher DDL, the
    /// `uni.schema.createIndex` procedure, and the Rust/Python schema builders via
    /// `rebuild`), so dense, native-multivector, and MUVERA indexes behave identically.
    ///
    /// For a `Muvera` index it first prepares the derived FDE column ([`Self::prepare_muvera_fde`]:
    /// register + one-time backfill), then builds the physical single-vector ANN over that
    /// `__fde_*` column with the **Dot** metric (its inner product approximates MaxSim),
    /// while the persisted config stays the MUVERA one so query routing detects it.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn create_vector_index(&self, config: VectorIndexConfig) -> Result<()> {
        self.create_vector_index_inner(config, false).await
    }

    /// Like [`Self::create_vector_index`], but `force_backfill` re-materialises a MUVERA
    /// index's derived FDE column over ALL current rows even if it was already registered.
    ///
    /// Full rebuilds ([`Self::rebuild_indexes_for_label`], hence `db.indexes().rebuild()`
    /// and the bulk loader's sync index sync) set this. The flush-time FDE materializer
    /// (`Writer::materialize_fde_columns`) only runs on the tx write path, so after a BULK
    /// load — or any out-of-band table mutation — the newly written rows have no FDE. A
    /// plain create would hit the "already registered" guard and skip the backfill, leaving
    /// those rows invisible to the FDE ANN (silent empty results). A rebuild must therefore
    /// force a fresh backfill; `splice_fde_batch` recomputes every row's FDE deterministically
    /// and overwrites any stale column, so this is idempotent.
    #[cfg(feature = "lance-backend")]
    async fn create_vector_index_inner(
        &self,
        config: VectorIndexConfig,
        force_backfill: bool,
    ) -> Result<()> {
        if let VectorIndexType::Muvera { inner, .. } = &config.index_type {
            // Register + backfill the derived FDE column (forced on a full rebuild).
            self.prepare_muvera_fde(&config, force_backfill).await?;
            let inner_cfg = VectorIndexConfig {
                name: config.name.clone(),
                label: config.label.clone(),
                property: crate::storage::muvera_index::fde_derived_column(&config.name),
                index_type: (**inner).clone(),
                metric: DistanceMetric::Dot,
                embedding_config: None,
                metadata: config.metadata.clone(),
            };
            self.build_physical_vector_index(&inner_cfg).await?;
        } else {
            self.build_physical_vector_index(&config).await?;
        }
        self.schema_manager
            .add_index(IndexDefinition::Vector(config))?;
        self.schema_manager.save().await?;
        Ok(())
    }

    /// Prepare a MUVERA index's derived `__fde_*` column: register it as an internal
    /// schema property and, the FIRST time (when it was not already registered), backfill
    /// it over all already-flushed rows via a full table rewrite (scan → splice the FDE
    /// column into the `get_arrow_schema`-sorted position → `replace_table_atomic`),
    /// mirroring the inverted-index "scan all rows at create time" precedent.
    ///
    /// The "already registered" guard makes this cheap on incremental creates: a plain
    /// `create_vector_index` (e.g. when another index on the label is added, or on schema
    /// re-apply) skips the rewrite — on the tx write path the column is kept current by the
    /// flush-time materializer (`Writer::materialize_fde_columns`). `force_backfill` bypasses
    /// that guard for full rebuilds, where the materializer assumption does not hold (e.g.
    /// after a bulk load); see [`Self::create_vector_index_inner`]. No-op for a non-MUVERA
    /// config, an unresolved source dimension, a label with nothing flushed yet, or when no
    /// backend is attached.
    #[cfg(feature = "lance-backend")]
    async fn prepare_muvera_fde(
        &self,
        config: &VectorIndexConfig,
        force_backfill: bool,
    ) -> Result<()> {
        use crate::storage::muvera_index::fde_spec_for_config;

        let schema = self.schema_manager.schema();
        let Some(spec) = fde_spec_for_config(&schema, config) else {
            return Ok(());
        };
        spec.params.validate()?;

        // Register the derived column. `add_internal_property` is write-lock-guarded and
        // reports whether THIS call inserted it, so two concurrent creates of the same MUVERA
        // index can't both run the (expensive) full-table backfill — only the inserter does.
        let newly_added = self.schema_manager.add_internal_property(
            &spec.label,
            &spec.derived_col,
            uni_common::DataType::Vector {
                dimensions: spec.params.fde_dim(),
            },
            true,
        )?;

        // Backfill when we just registered the column, or when a full rebuild forces it (the
        // flush-time materializer doesn't cover bulk-loaded / out-of-band rows). A plain
        // re-create that finds the column already present relies on that materializer.
        if !newly_added && !force_backfill {
            return Ok(());
        }

        // Run the backfill; if it FAILS after we just added the column, roll the registration
        // back so the in-memory schema stays consistent with disk and a retry re-adds +
        // re-backfills. Otherwise the retry would see the column registered, skip the
        // backfill, and build the index over an unpopulated FDE column.
        //
        // Crash-window note: the on-disk order is backfill (`replace_table_atomic`) THEN
        // schema save (in `create_vector_index_inner`). A crash in between leaves an orphan
        // `__fde_*` column with no persisted schema entry, which the next create's idempotent
        // rewrite overwrites — self-healing. Persisting the schema first would be worse (a
        // registered column with no data errors reads), and a `Building` marker is not
        // auto-recovered (`labels_needing_rebuild` skips `Building`/`Failed`).
        if let Err(e) = self.backfill_fde_column(&spec).await {
            if newly_added {
                let _ = self
                    .schema_manager
                    .drop_property(&spec.label, &spec.derived_col);
            }
            return Err(e);
        }
        Ok(())
    }

    /// Materialize the MUVERA derived FDE column over all currently-flushed rows via a full
    /// table rewrite (scan → recompute each row's FDE → splice into the
    /// `get_arrow_schema`-sorted position → `replace_table_atomic`). No-op (kept registration)
    /// when no backend is attached or the label has nothing flushed yet — create-before-ingest,
    /// where the flush path materializes the column. The caller must have already registered
    /// the derived column in the schema.
    #[cfg(feature = "lance-backend")]
    async fn backfill_fde_column(
        &self,
        spec: &crate::storage::muvera_index::FdeSpec,
    ) -> Result<()> {
        use crate::storage::muvera_index::splice_fde_batch;

        let Some(backend) = self.backend.as_ref() else {
            return Ok(());
        };
        let table = table_names::vertex_table_name(&spec.label);
        if !backend.table_exists(&table).await.unwrap_or(false) {
            return Ok(());
        }

        let schema = self.schema_manager.schema();
        let label_id = schema
            .label_id_by_name(&spec.label)
            .ok_or_else(|| anyhow!("MUVERA: label '{}' not found", spec.label))?;
        // Schema already carries the FDE column (registered by the caller) so it's in the
        // arrow schema at the position future flush appends will use.
        let target_schema =
            VertexDataset::new(&self.base_uri, &spec.label, label_id).get_arrow_schema(&schema)?;
        let source_dt = schema
            .properties
            .get(&spec.label)
            .and_then(|p| p.get(&spec.source_prop))
            .map(|m| m.r#type.clone());
        let encoder = uni_common::muvera::FdeEncoder::new(&spec.params)?;

        let batches = backend.scan(ScanRequest::all(&table)).await?;
        let mut new_batches = Vec::with_capacity(batches.len());
        for batch in &batches {
            new_batches.push(splice_fde_batch(
                batch,
                &target_schema,
                spec,
                &encoder,
                source_dt.as_ref(),
            )?);
        }
        backend
            .replace_table_atomic(&table, new_batches, target_schema)
            .await?;
        Ok(())
    }

    /// Build the physical Lance ANN index described by `config` over `config.property`
    /// with `config.metric`. Does NOT persist the schema index definition — the caller
    /// does, possibly under a different logical config (see MUVERA in
    /// [`Self::create_vector_index`]).
    #[cfg(feature = "lance-backend")]
    async fn build_physical_vector_index(&self, config: &VectorIndexConfig) -> Result<()> {
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

        // Fail fast on an invalid PQ configuration before touching Lance (which
        // would otherwise error opaquely at build time). The embedding dimension
        // comes from the schema property type, recursing `List(Vector{dim})` for
        // multi-vector (ColBERT) columns.
        let prop_dim = schema
            .properties
            .get(label)
            .and_then(|props| props.get(property))
            .and_then(|meta| resolve_vector_dim(&meta.r#type));
        let pq_sub = match &config.index_type {
            VectorIndexType::IvfPq {
                num_sub_vectors, ..
            }
            | VectorIndexType::HnswPq {
                num_sub_vectors, ..
            } => Some(*num_sub_vectors as usize),
            _ => None,
        };
        // Only the realistic misconfiguration (sub-vectors that don't divide a
        // dimension at least as large) is rejected up front. The degenerate
        // `sub > dim` case (e.g. the default 16 on a dim-2 column) is left to Lance,
        // which clamps/defers it — notably so an index can be declared on an empty
        // table before any rows exist.
        if let (Some(dim), Some(sub)) = (prop_dim, pq_sub)
            && sub != 0
            && dim >= sub
            && dim % sub != 0
        {
            return Err(anyhow!(
                "Vector index '{}': PQ num_sub_vectors ({}) must divide the embedding dimension ({})",
                config.name,
                sub,
                dim
            ));
        }

        let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);

        match ds_wrapper.open_raw().await {
            Ok(mut lance_ds) => {
                let metric_type = match &config.metric {
                    DistanceMetric::L2 => MetricType::L2,
                    DistanceMetric::Cosine => MetricType::Cosine,
                    DistanceMetric::Dot => MetricType::Dot,
                    _ => return Err(anyhow!("Unsupported metric: {:?}", config.metric)),
                };

                let params = match config.index_type.clone() {
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
                // A full rebuild must force the MUVERA FDE backfill: bulk-loaded / reopened
                // rows aren't covered by the flush-time materializer (see
                // `create_vector_index_inner`).
                IndexDefinition::Vector(cfg) => self.create_vector_index_inner(cfg, true).await?,
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
