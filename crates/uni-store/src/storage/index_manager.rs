// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Index lifecycle management: creation, rebuild, and incremental updates for all index types.

use crate::backend::StorageBackend;
use crate::backend::table_names;
use crate::backend::types::ScanRequest;
#[cfg(feature = "lance-backend")]
use crate::storage::inverted_index::InvertedIndex;
#[cfg(feature = "lance-backend")]
use crate::storage::sparse_index::SparseVectorIndex;
use crate::storage::vertex::VertexDataset;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
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
    ScalarIndexConfig, ScalarIndexType, SparseVectorIndexConfig, VectorIndexConfig,
    VectorIndexType,
};

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

/// Maps a schema [`VectorIndexType`] to the backend's [`VectorIndexParams`].
///
/// The logical MUVERA type is resolved to its `inner` shape by the caller
/// (`create_vector_index_inner`) before reaching here, so a `Muvera` value is a
/// programming error. The `Option<num_partitions>` HNSW default of "auto" is
/// resolved to a single partition, matching the prior raw-`Dataset` mapping.
///
/// # Errors
/// Returns an error if a `Muvera` type reaches this physical-build mapping.
#[cfg(feature = "lance-backend")]
fn to_backend_vector_params(
    metric: DistanceMetric,
    index_type: &VectorIndexType,
) -> Result<crate::backend::types::VectorIndexParams> {
    use crate::backend::types::{VectorIndexKind, VectorIndexParams};
    // `DistanceMetric` here is the schema (uni_common) enum; the backend has its
    // own. Both are `#[non_exhaustive]`, so the catch-all arms are required.
    let backend_metric = match metric {
        DistanceMetric::L2 => crate::backend::types::DistanceMetric::L2,
        DistanceMetric::Cosine => crate::backend::types::DistanceMetric::Cosine,
        DistanceMetric::Dot => crate::backend::types::DistanceMetric::Dot,
        other => return Err(anyhow!("Unsupported vector index metric: {:?}", other)),
    };
    let kind = match index_type {
        VectorIndexType::Flat => VectorIndexKind::Flat,
        VectorIndexType::IvfFlat { num_partitions } => VectorIndexKind::IvfFlat {
            num_partitions: *num_partitions,
        },
        VectorIndexType::IvfPq {
            num_partitions,
            num_sub_vectors,
            bits_per_subvector,
        } => VectorIndexKind::IvfPq {
            num_partitions: *num_partitions,
            num_sub_vectors: *num_sub_vectors,
            num_bits: *bits_per_subvector,
        },
        VectorIndexType::IvfSq { num_partitions } => VectorIndexKind::IvfSq {
            num_partitions: *num_partitions,
        },
        VectorIndexType::IvfRq {
            num_partitions,
            num_bits,
        } => VectorIndexKind::IvfRq {
            num_partitions: *num_partitions,
            num_bits: *num_bits,
        },
        VectorIndexType::HnswFlat {
            m,
            ef_construction,
            num_partitions,
        } => VectorIndexKind::HnswFlat {
            m: *m,
            ef_construction: *ef_construction,
            num_partitions: num_partitions.unwrap_or(1),
        },
        VectorIndexType::HnswSq {
            m,
            ef_construction,
            num_partitions,
        } => VectorIndexKind::HnswSq {
            m: *m,
            ef_construction: *ef_construction,
            num_partitions: num_partitions.unwrap_or(1),
        },
        VectorIndexType::HnswPq {
            m,
            ef_construction,
            num_sub_vectors,
            num_partitions,
        } => VectorIndexKind::HnswPq {
            m: *m,
            ef_construction: *ef_construction,
            num_sub_vectors: *num_sub_vectors,
            num_partitions: num_partitions.unwrap_or(1),
        },
        VectorIndexType::Muvera { .. } => {
            return Err(anyhow!(
                "MUVERA must be resolved to its inner index type before the physical build"
            ));
        }
        other => return Err(anyhow!("Unsupported vector index type: {:?}", other)),
    };
    Ok(VectorIndexParams {
        metric: backend_metric,
        kind,
    })
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
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{}' not found", label));
        }

        let mut index = InvertedIndex::new(&self.base_uri, config.clone()).await?;

        // Backfill from the flushed vertex table via the storage backend. The
        // LanceDB-managed table is not at the raw `{base}/vertices_<label>`
        // path a `VertexDataset` open would target, and the backend read is
        // branch-aware. Mirrors the sparse-index backfill. A not-yet-flushed
        // table legitimately yields an empty index, populated on the next flush.
        let table = table_names::vertex_table_name(label);
        if let Some(backend) = self.backend.as_ref() {
            if backend.table_exists(&table).await? {
                let batches = backend.scan(ScanRequest::all(&table)).await?;
                index
                    .build_from_batches(&batches, |n| info!("Indexed {} terms", n))
                    .await?;
            } else {
                debug!(
                    "Table '{}' not flushed yet; creating empty inverted index (populated on flush)",
                    table
                );
            }
        } else {
            warn!(
                "No storage backend available; inverted index '{}' left empty (populated on flush)",
                config.name
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
    /// For a `Muvera` index it first prepares the derived FDE column (`prepare_muvera_fde`:
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
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{}' not found", label));
        }

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

        let params = to_backend_vector_params(config.metric.clone(), &config.index_type)?;
        let table = table_names::vertex_table_name(label);

        let Some(backend) = self.backend.as_ref() else {
            warn!(
                "No storage backend; physical vector index '{}' deferred until a flush",
                config.name
            );
            return Ok(());
        };

        // Build only once the table is flushed; create-before-flush is a no-op
        // here and is materialized by the next flush's rebuild. A build failure
        // on a tiny/degenerate column is tolerated (Lance may clamp or defer ANN
        // training) — the schema definition is still persisted by the caller.
        if backend.table_exists(&table).await? {
            info!(
                "Building physical vector index '{}' on '{}'",
                config.name, table
            );
            if let Err(e) = backend
                .create_vector_index(&table, property, &config.name, params)
                .await
            {
                warn!(
                    "Failed to build physical vector index '{}' (column may be empty): {}",
                    config.name, e
                );
            } else {
                info!("Vector index '{}' created", config.name);
            }
        } else {
            debug!(
                "Label '{}' not flushed yet; physical vector index '{}' built on next flush",
                label, config.name
            );
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
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{}' not found", label));
        }

        let columns: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();
        // Map the schema scalar type to the backend's; anything other than the
        // explicit Bitmap/LabelList falls back to BTree (matching the prior
        // `ScalarIndexParams::default()`).
        let backend_idx_type = match config.index_type {
            ScalarIndexType::Bitmap => crate::backend::types::ScalarIndexType::Bitmap,
            ScalarIndexType::LabelList => crate::backend::types::ScalarIndexType::LabelList,
            _ => crate::backend::types::ScalarIndexType::BTree,
        };
        let table = table_names::vertex_table_name(label);

        if let Some(backend) = self.backend.as_ref() {
            if backend.table_exists(&table).await? {
                info!(
                    "Building physical scalar index '{}' on '{}'",
                    config.name, table
                );
                if let Err(e) = backend
                    .create_scalar_index(&table, &columns, backend_idx_type, Some(&config.name))
                    .await
                {
                    warn!(
                        "Failed to build physical scalar index '{}' (table may be empty): {}",
                        config.name, e
                    );
                } else {
                    info!("Scalar index '{}' created", config.name);
                }
            } else {
                debug!(
                    "Label '{}' not flushed yet; physical scalar index '{}' built on next flush",
                    label, config.name
                );
            }
        } else {
            warn!(
                "No storage backend; physical scalar index '{}' deferred until a flush",
                config.name
            );
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
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{}' not found", label));
        }

        let columns: Vec<&str> = config.properties.iter().map(|s| s.as_str()).collect();
        let table = table_names::vertex_table_name(label);

        if let Some(backend) = self.backend.as_ref() {
            if backend.table_exists(&table).await? {
                info!(
                    "Building physical FTS index '{}' on '{}'",
                    config.name, table
                );
                if let Err(e) = backend
                    .create_fts_index(&table, &columns, Some(&config.name), config.with_positions)
                    .await
                {
                    warn!(
                        "Failed to build physical FTS index '{}' (table may be empty): {}",
                        config.name, e
                    );
                } else {
                    info!("FTS index '{}' created", config.name);
                }
            } else {
                debug!(
                    "Label '{}' not flushed yet; physical FTS index '{}' built on next flush",
                    label, config.name
                );
            }
        } else {
            warn!(
                "No storage backend; physical FTS index '{}' deferred until a flush",
                config.name
            );
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
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{}' not found", label));
        }

        let table = table_names::vertex_table_name(label);

        if let Some(backend) = self.backend.as_ref() {
            if backend.table_exists(&table).await? {
                info!(
                    "Building physical JSON FTS index '{}' on '{}'",
                    config.name, table
                );
                if let Err(e) = backend
                    .create_fts_index(
                        &table,
                        &[column.as_str()],
                        Some(&config.name),
                        config.with_positions,
                    )
                    .await
                {
                    warn!(
                        "Failed to build physical JSON FTS index '{}' (table may be empty): {}",
                        config.name, e
                    );
                } else {
                    info!("JSON FTS index '{}' created", config.name);
                }
            } else {
                debug!(
                    "Label '{}' not flushed yet; physical JSON FTS index '{}' built on next flush",
                    label, config.name
                );
            }
        } else {
            warn!(
                "No storage backend; physical JSON FTS index '{}' deferred until a flush",
                config.name
            );
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

        // Drop the physical index through the backend. Best-effort: the index
        // may never have been physically built (e.g. created before any flush),
        // so a failure here is non-fatal.
        let label = idx_def.label();
        let table = table_names::vertex_table_name(label);
        if let Some(backend) = self.backend.as_ref() {
            if let Err(e) = backend.drop_index(&table, name).await {
                warn!(
                    "Physical index drop for '{}' returned error (non-fatal): {}",
                    name, e
                );
            } else {
                info!("Physical index '{}' dropped from '{}'", name, table);
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
                IndexDefinition::Sparse(cfg) => self.create_sparse_vector_index(cfg).await?,
                _ => warn!("Unknown index type encountered during rebuild, skipping"),
            }
        }
        Ok(())
    }

    /// Create composite index for unique constraint
    #[cfg(feature = "lance-backend")]
    pub async fn create_composite_index(&self, label: &str, properties: &[String]) -> Result<()> {
        let schema = self.schema_manager.schema();
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{}' not found", label));
        }

        // Lance supports multi-column indexes.
        let index_name = format!("{}_{}_composite", label, properties.join("_"));
        let columns: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();
        let table = table_names::vertex_table_name(label);

        if let Some(backend) = self.backend.as_ref() {
            if backend.table_exists(&table).await? {
                info!("Building composite index '{}' on '{}'", index_name, table);
                if let Err(e) = backend
                    .create_scalar_index(
                        &table,
                        &columns,
                        crate::backend::types::ScalarIndexType::BTree,
                        Some(&index_name),
                    )
                    .await
                {
                    warn!(
                        "Failed to build composite index '{}' (table may be empty): {}",
                        index_name, e
                    );
                } else {
                    info!("Composite index '{}' created", index_name);
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
            } else {
                debug!(
                    "Label '{}' not flushed yet; composite index for {:?} built on next flush",
                    label, properties
                );
            }
        } else {
            warn!(
                "No storage backend; composite index for {:?} deferred until a flush",
                properties
            );
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

    /// Create (and backfill) a scored sparse-vector index. Mirrors
    /// `create_inverted_index`: build from the flushed vertex dataset if it
    /// exists, then register + persist the config.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn create_sparse_vector_index(&self, config: SparseVectorIndexConfig) -> Result<()> {
        let label = &config.label;
        let property = &config.property;
        info!(
            "Creating Sparse Vector Index '{}' on {}.{}",
            config.name, label, property
        );

        let schema = self.schema_manager.schema();
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{}' not found", label));
        }

        let mut index = SparseVectorIndex::new(&self.base_uri, config.clone()).await?;

        // Backfill from the flushed vertex table via the storage backend (the
        // LanceDB-managed table is not at the raw `{base}/vertices_<label>`
        // path a `VertexDataset::open` expects). Mirrors the MUVERA backfill.
        let table = table_names::vertex_table_name(label);
        if let Some(backend) = self.backend.as_ref() {
            if backend.table_exists(&table).await.unwrap_or(false) {
                let batches = backend.scan(ScanRequest::all(&table)).await?;
                index
                    .build_from_batches(&batches, |n| debug!("Indexed {} sparse docs", n))
                    .await?;
            } else {
                debug!(
                    "Table '{}' not flushed yet; creating empty sparse index (populated on flush)",
                    table
                );
            }
        } else {
            warn!(
                "No storage backend available; sparse index '{}' left empty (populated on flush)",
                config.name
            );
        }

        self.schema_manager
            .add_index(IndexDefinition::Sparse(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    /// Applies incremental updates to a sparse-vector index (load-modify-write,
    /// same semantics as the set-membership inverted index).
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self, added, removed), level = "info", fields(
        label = %config.label,
        property = %config.property
    ))]
    pub async fn update_sparse_vector_index_incremental(
        &self,
        config: &SparseVectorIndexConfig,
        added: &HashMap<Vid, Vec<(u32, f32)>>,
        removed: &HashSet<Vid>,
    ) -> Result<()> {
        info!(
            added = added.len(),
            removed = removed.len(),
            "Incrementally updating sparse vector index"
        );
        let mut index = SparseVectorIndex::new(&self.base_uri, config.clone()).await?;
        index.apply_incremental_updates(added, removed).await
    }

    /// Open a sparse-vector index for querying, given its label + property.
    /// Errors if no `IndexDefinition::Sparse` is registered for that pair.
    #[cfg(feature = "lance-backend")]
    pub async fn sparse_vector_index(
        &self,
        label: &str,
        property: &str,
    ) -> Result<SparseVectorIndex> {
        let schema = self.schema_manager.schema();
        let config = schema
            .indexes
            .iter()
            .find_map(|idx| match idx {
                IndexDefinition::Sparse(cfg) if cfg.label == label && cfg.property == property => {
                    Some(cfg.clone())
                }
                _ => None,
            })
            .ok_or_else(|| anyhow!("No sparse vector index found for {}.{}", label, property))?;
        SparseVectorIndex::new(&self.base_uri, config).await
    }
}
