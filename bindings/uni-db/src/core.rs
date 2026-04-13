// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Shared async core functions used by both sync and async APIs.
//!
//! These are pure async Rust functions with no Python dependencies,
//! making them callable from both `block_on()` (sync) and
//! `future_into_py()` (async) contexts.

use ::uni_db::{Uni, Value, Vid};
use std::collections::HashMap;
use std::time::Duration;
use uni_common::UniError;
use uni_common::core::schema::{
    DataType, DistanceMetric, EmbeddingConfig, FullTextIndexConfig, IndexDefinition,
    InvertedIndexConfig, ScalarIndexConfig, ScalarIndexType, TokenizerConfig, VectorIndexConfig,
    VectorIndexType,
};

// Re-export types used by the sync and async API modules.
pub use ::uni_db::api::schema::EdgeTypeInfo as UniEdgeTypeInfo;
pub use ::uni_db::api::schema::LabelInfo as UniLabelInfo;
pub use ::uni_db::query_crate::QueryCursor;
pub use ::uni_db::{ExplainOutput, ProfileOutput, QueryResult, Row};

// ============================================================================
// Query Core
// ============================================================================

/// Execute a query with parameters and return result rows.
pub async fn query_core(
    db: &Uni,
    cypher: &str,
    params: HashMap<String, Value>,
) -> Result<QueryResult, UniError> {
    let session = db.session();
    let mut builder = session.query_with(cypher);
    for (k, v) in params {
        builder = builder.param(&k, v);
    }
    builder.fetch_all().await
}

/// Execute a query with parameters, timeout, and memory limit.
pub async fn query_builder_core(
    db: &Uni,
    cypher: &str,
    params: HashMap<String, Value>,
    timeout_secs: Option<f64>,
    max_memory: Option<usize>,
) -> Result<QueryResult, UniError> {
    let session = db.session();
    let mut builder = session.query_with(cypher);
    for (k, v) in params {
        builder = builder.param(&k, v);
    }
    if let Some(t) = timeout_secs {
        builder = builder.timeout(Duration::from_secs_f64(t));
    }
    if let Some(m) = max_memory {
        builder = builder.max_memory(m);
    }
    builder.fetch_all().await
}

/// Open a streaming cursor for a query with parameters, timeout, and memory limit.
pub async fn query_cursor_core(
    db: &Uni,
    cypher: &str,
    params: HashMap<String, Value>,
    timeout_secs: Option<f64>,
    max_memory: Option<usize>,
) -> Result<QueryCursor, UniError> {
    let session = db.session();
    let mut builder = session.query_with(cypher);
    for (k, v) in params {
        builder = builder.param(&k, v);
    }
    if let Some(t) = timeout_secs {
        builder = builder.timeout(Duration::from_secs_f64(t));
    }
    if let Some(m) = max_memory {
        builder = builder.max_memory(m);
    }
    builder.cursor().await
}

/// Execute a mutation query, returning affected row count.
pub async fn execute_core(db: &Uni, cypher: &str) -> Result<usize, UniError> {
    let session = db.session();
    let tx = session.tx().await?;
    let result = tx.execute(cypher).await?;
    let affected = result.affected_rows();
    tx.commit().await?;
    Ok(affected)
}

/// Execute a mutation query with parameters, returning affected row count.
pub async fn execute_with_params_core(
    db: &Uni,
    cypher: &str,
    params: HashMap<String, Value>,
) -> Result<usize, UniError> {
    let session = db.session();
    let tx = session.tx().await?;
    let mut builder = tx.execute_with(cypher);
    for (k, v) in params {
        builder = builder.param(&k, v);
    }
    let result = builder.run().await?;
    let affected = result.affected_rows();
    tx.commit().await?;
    Ok(affected)
}

/// Flush all uncommitted changes to persistent storage.
pub async fn flush_core(db: &Uni) -> Result<(), UniError> {
    db.flush().await
}

// ============================================================================
// Schema Core
// ============================================================================

/// Check if a label exists.
pub async fn label_exists_core(db: &Uni, name: &str) -> Result<bool, UniError> {
    db.label_exists(name).await
}

/// Check if an edge type exists.
pub async fn edge_type_exists_core(db: &Uni, name: &str) -> Result<bool, UniError> {
    db.edge_type_exists(name).await
}

/// List all label names.
pub async fn list_labels_core(db: &Uni) -> Result<Vec<String>, UniError> {
    db.list_labels().await
}

/// List all edge type names.
pub async fn list_edge_types_core(db: &Uni) -> Result<Vec<String>, UniError> {
    db.list_edge_types().await
}

/// Get detailed information about a label.
pub async fn get_label_info_core(db: &Uni, name: &str) -> Result<Option<UniLabelInfo>, UniError> {
    db.get_label_info(name).await
}

/// Get detailed information about an edge type.
pub async fn get_edge_type_info_core(
    db: &Uni,
    name: &str,
) -> Result<Option<UniEdgeTypeInfo>, UniError> {
    db.get_edge_type_info(name).await
}

/// Load schema from a JSON file.
pub async fn load_schema_core(db: &Uni, path: &str) -> Result<(), UniError> {
    db.load_schema(path).await
}

/// Save schema to a JSON file.
pub async fn save_schema_core(db: &Uni, path: &str) -> Result<(), UniError> {
    db.save_schema(path).await
}

/// Apply pending schema changes.
///
/// This is additive/idempotent: labels and edge types that already exist are
/// silently skipped so that the schema builder can be used to add properties
/// or indexes to existing schema elements.
pub async fn apply_schema_core(
    db: &Uni,
    pending_labels: &[String],
    pending_edge_types: &[(String, Vec<String>, Vec<String>)],
    pending_properties: &[(String, String, DataType, bool)],
    pending_indexes: &[IndexDefinition],
) -> Result<(), UniError> {
    use ::uni_db::api::schema::SchemaChange;

    let mut changes = Vec::new();

    for name in pending_labels {
        changes.push(SchemaChange::AddLabel { name: name.clone() });
    }

    for (name, from, to) in pending_edge_types {
        changes.push(SchemaChange::AddEdgeType {
            name: name.clone(),
            from_labels: from.clone(),
            to_labels: to.clone(),
        });
    }

    for (label_or_type, prop_name, data_type, nullable) in pending_properties {
        changes.push(SchemaChange::AddProperty {
            label_or_type: label_or_type.clone(),
            name: prop_name.clone(),
            data_type: data_type.clone(),
            nullable: *nullable,
        });
    }

    for idx in pending_indexes {
        changes.push(SchemaChange::AddIndex(idx.clone()));
    }

    db.schema().with_changes(changes).apply().await
}

// ============================================================================
// Index Core
// ============================================================================

// ============================================================================
// Bulk Loading Core
// ============================================================================

/// Bulk insert vertices.
pub async fn bulk_insert_vertices_core(
    db: &Uni,
    label: &str,
    rust_props: Vec<HashMap<String, serde_json::Value>>,
) -> Result<Vec<Vid>, UniError> {
    let uni_props: Vec<uni_common::Properties> = rust_props
        .into_iter()
        .map(|m| m.into_iter().map(|(k, v)| (k, v.into())).collect())
        .collect();
    let session = db.session();
    let tx = session.tx().await?;
    let vids = tx.bulk_insert_vertices(label, uni_props).await?;
    tx.commit().await?;
    Ok(vids)
}

/// Bulk insert edges.
pub async fn bulk_insert_edges_core(
    db: &Uni,
    edge_type: &str,
    edges: Vec<(Vid, Vid, HashMap<String, serde_json::Value>)>,
) -> Result<(), UniError> {
    let uni_edges: Vec<(Vid, Vid, uni_common::Properties)> = edges
        .into_iter()
        .map(|(s, d, m)| (s, d, m.into_iter().map(|(k, v)| (k, v.into())).collect()))
        .collect();
    let session = db.session();
    let tx = session.tx().await?;
    tx.bulk_insert_edges(edge_type, uni_edges).await?;
    tx.commit().await?;
    Ok(())
}

// ============================================================================
// Xervo Core
// ============================================================================

/// Embed texts using a configured Xervo model alias.
pub async fn xervo_embed_core(
    db: &Uni,
    alias: &str,
    texts: Vec<String>,
) -> Result<Vec<Vec<f32>>, UniError> {
    let xervo = db.xervo();
    let text_refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
    xervo.embed(alias, &text_refs).await
}

/// Generate text using structured messages via a configured Xervo model alias.
pub async fn xervo_generate_core(
    db: &Uni,
    alias: &str,
    messages: Vec<(String, String)>,
    max_tokens: Option<usize>,
    temperature: Option<f32>,
    top_p: Option<f32>,
) -> Result<::uni_db::api::xervo::GenerationResult, UniError> {
    use ::uni_db::api::xervo::{GenerationOptions, Message};
    let xervo = db.xervo();
    let rust_messages: Vec<Message> = messages
        .into_iter()
        .map(|(role, content)| match role.as_str() {
            "user" => Message::user(content),
            "assistant" => Message::assistant(content),
            "system" => Message::system(content),
            _ => Message::user(content),
        })
        .collect();
    let opts = GenerationOptions {
        max_tokens,
        temperature,
        top_p,
        width: None,
        height: None,
    };
    xervo.generate(alias, &rust_messages, opts).await
}

// ============================================================================
// Snapshot Core
// ============================================================================

/// Create a point-in-time snapshot of the database.
pub async fn create_snapshot_core(db: &Uni, name: &str) -> Result<String, UniError> {
    db.create_snapshot(name).await
}

/// List all available snapshots.
pub async fn list_snapshots_core(
    db: &Uni,
) -> Result<Vec<uni_common::core::snapshot::SnapshotManifest>, UniError> {
    db.list_snapshots().await
}

/// Restore the database to a specific snapshot.
pub async fn restore_snapshot_core(db: &Uni, snapshot_id: &str) -> Result<(), UniError> {
    db.restore_snapshot(snapshot_id).await
}

// ============================================================================
// Compaction Core
// ============================================================================

/// Compact a label or edge type's storage files.
pub async fn compact_core(
    db: &Uni,
    name: &str,
) -> Result<crate::types::PyCompactionStats, UniError> {
    let stats = db.compaction().compact(name).await?;
    Ok(crate::types::PyCompactionStats {
        files_compacted: stats.files_compacted,
        bytes_before: stats.bytes_before,
        bytes_after: stats.bytes_after,
        duration_secs: stats.duration.as_secs_f64(),
        crdt_merges: stats.crdt_merges,
    })
}

/// Wait for any ongoing compaction to complete.
pub async fn wait_for_compaction_core(db: &Uni) -> Result<(), UniError> {
    db.compaction().wait().await
}

// ============================================================================
// Index Admin Core
// ============================================================================

/// Get status of background index rebuild tasks.
pub async fn index_rebuild_status_core(
    db: &Uni,
) -> Result<Vec<uni_store::storage::IndexRebuildTask>, UniError> {
    db.indexes().rebuild_status().await
}

/// Retry failed index rebuild tasks.
pub async fn retry_index_rebuilds_core(db: &Uni) -> Result<Vec<String>, UniError> {
    db.indexes().retry_failed().await
}

/// Force rebuild indexes for a label (background = true runs in background).
pub async fn rebuild_indexes_core(
    db: &Uni,
    label: &str,
    background: bool,
) -> Result<Option<String>, UniError> {
    db.indexes().rebuild(label, background).await
}

/// List all indexes defined on a specific label.
pub fn list_indexes_core(db: &Uni, label: &str) -> Vec<uni_common::core::schema::IndexDefinition> {
    db.indexes().list(Some(label))
}

/// List all indexes in the database.
pub fn list_all_indexes_core(db: &Uni) -> Vec<uni_common::core::schema::IndexDefinition> {
    db.indexes().list(None)
}

// ============================================================================
// Locy Core
// ============================================================================

/// Evaluate a Locy program with default configuration.
pub async fn locy_evaluate_core(
    db: &Uni,
    program: &str,
) -> Result<::uni_db::locy::LocyResult, UniError> {
    db.session().locy(program).await
}

/// Evaluate a Locy program with custom configuration.
pub async fn locy_evaluate_with_config_core(
    db: &Uni,
    program: &str,
    config: ::uni_db::locy::LocyConfig,
) -> Result<::uni_db::locy::LocyResult, UniError> {
    db.session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await
}

/// Compile a Locy program without executing it.
pub fn locy_compile_only_core(
    db: &Uni,
    program: &str,
) -> Result<::uni_locy::CompiledProgram, UniError> {
    db.session().compile_locy(program)
}

// ============================================================================
// Database Builder Core
// ============================================================================

/// Mode for opening database.
#[derive(Debug, Clone, Copy)]
pub enum OpenMode {
    Open,
    OpenExisting,
    Create,
    Temporary,
}

/// Build and open a Uni database.
#[allow(clippy::too_many_arguments)]
pub async fn build_database_core(
    uri: &str,
    mode: OpenMode,
    hybrid_local: Option<&str>,
    hybrid_remote: Option<&str>,
    cache_size: Option<usize>,
    parallelism: Option<usize>,
    schema_file: Option<&str>,
    xervo_catalog_json: Option<&str>,
    xervo_catalog_file: Option<&str>,
    cloud_config: Option<uni_common::CloudStorageConfig>,
    uni_config: Option<uni_common::UniConfig>,
    read_only: bool,
    write_lease: Option<::uni_db::api::multi_agent::WriteLease>,
) -> Result<Uni, UniError> {
    let mut builder = match mode {
        OpenMode::Open => Uni::open(uri),
        OpenMode::OpenExisting => Uni::open_existing(uri),
        OpenMode::Create => Uni::create(uri),
        OpenMode::Temporary => Uni::temporary(),
    };

    // Apply uni_config first so cache_size/parallelism overrides below take effect
    let mut config = uni_config.unwrap_or_default();

    if let Some(size) = cache_size {
        config.cache_size = size;
    }

    if let Some(n) = parallelism {
        config.parallelism = n;
    }

    builder = builder.config(config);

    if let Some(path) = schema_file {
        builder = builder.schema_file(path);
    }

    if let Some(json) = xervo_catalog_json {
        let catalog = ::uni_db::xervo_catalog_from_str(json)
            .map_err(|e| UniError::Internal(anyhow::anyhow!(e.to_string())))?;
        builder = builder.xervo_catalog(catalog);
    } else if let Some(path) = xervo_catalog_file {
        let catalog = ::uni_db::xervo_catalog_from_file(path)
            .map_err(|e| UniError::Internal(anyhow::anyhow!(e.to_string())))?;
        builder = builder.xervo_catalog(catalog);
    }

    if let (Some(_local), Some(remote)) = (hybrid_local, hybrid_remote) {
        if let Some(cc) = cloud_config {
            builder = builder.remote_storage(remote, cc);
        } else {
            // remote_storage requires config; create a minimal one from the URL scheme
            return Err(UniError::Internal(anyhow::anyhow!(
                "remote_storage requires a CloudStorageConfig"
            )));
        }
    }

    if read_only {
        builder = builder.read_only();
    }

    if let Some(wl) = write_lease {
        builder = builder.write_lease(wl);
    }

    builder.build().await
}

// ============================================================================
// Helpers
// ============================================================================

/// Parse a data type string into a DataType enum.
pub fn parse_data_type(data_type: &str) -> Result<DataType, String> {
    if data_type.starts_with("vector:") {
        let dims = data_type
            .split(':')
            .nth(1)
            .ok_or_else(|| "Vector type must specify dimensions, e.g., 'vector:128'".to_string())?
            .parse::<usize>()
            .map_err(|_| "Invalid dimensions for vector type".to_string())?;
        Ok(DataType::Vector { dimensions: dims })
    } else if data_type.starts_with("list:") {
        let elem_type = data_type.split(':').nth(1).ok_or_else(|| {
            "List type must specify element type, e.g., 'list:string'".to_string()
        })?;
        let inner = parse_data_type(elem_type)?;
        Ok(DataType::List(Box::new(inner)))
    } else {
        match data_type.to_lowercase().as_str() {
            "string" => Ok(DataType::String),
            "int64" | "int" => Ok(DataType::Int64),
            "int32" => Ok(DataType::Int32),
            "float64" | "float" => Ok(DataType::Float64),
            "float32" => Ok(DataType::Float32),
            "bool" => Ok(DataType::Bool),
            "datetime" | "timestamp" => Ok(DataType::DateTime),
            "date" => Ok(DataType::Date),
            "time" => Ok(DataType::Time),
            "duration" => Ok(DataType::Duration),
            "json" => Ok(DataType::CypherValue),
            "btic" => Ok(DataType::Btic),
            "bytes" => Ok(DataType::String),
            _ => Err(format!("Unknown data type: {}", data_type)),
        }
    }
}

/// Create an index definition from parameters.
pub fn create_index_definition(
    label: &str,
    property: &str,
    index_type: &str,
) -> Result<IndexDefinition, String> {
    match index_type.to_lowercase().as_str() {
        "btree" | "scalar" => Ok(IndexDefinition::Scalar(ScalarIndexConfig {
            name: format!("idx_{}_{}", label, property),
            label: label.to_string(),
            properties: vec![property.to_string()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
            metadata: Default::default(),
        })),
        "hash" => Ok(IndexDefinition::Scalar(ScalarIndexConfig {
            name: format!("idx_{}_{}", label, property),
            label: label.to_string(),
            properties: vec![property.to_string()],
            index_type: ScalarIndexType::Hash,
            where_clause: None,
            metadata: Default::default(),
        })),
        "vector" => Ok(IndexDefinition::Vector(VectorIndexConfig {
            name: format!("idx_{}_{}_vec", label, property),
            label: label.to_string(),
            property: property.to_string(),
            index_type: VectorIndexType::Hnsw {
                m: 16,
                ef_construction: 200,
                ef_search: 50,
            },
            metric: DistanceMetric::Cosine,
            embedding_config: None,
            metadata: Default::default(),
        })),
        "inverted" => Ok(IndexDefinition::Inverted(InvertedIndexConfig {
            name: format!("idx_{}_{}_inv", label, property),
            label: label.to_string(),
            property: property.to_string(),
            normalize: true,
            max_terms_per_doc: 1024,
            metadata: Default::default(),
        })),
        "fulltext" => Ok(IndexDefinition::FullText(FullTextIndexConfig {
            name: format!("idx_{}_{}_ft", label, property),
            label: label.to_string(),
            properties: vec![property.to_string()],
            tokenizer: TokenizerConfig::Standard,
            with_positions: true,
            metadata: Default::default(),
        })),
        _ => Err(format!("Unknown index type: {}", index_type)),
    }
}

/// Create an index definition from a rich configuration dict.
///
/// The config dict must have a `"type"` key. Supported types:
/// - `"vector"`: optional `algorithm`, `metric`, `m`, `ef_construction`, `embedding`
/// - `"fulltext"`: optional `tokenizer`, `ngram_min`, `ngram_max`
/// - `"inverted"`: no extra options
/// - `"btree"`, `"hash"`, `"scalar"`: no extra options
pub fn create_index_definition_from_config(
    label: &str,
    property: &str,
    config: &HashMap<String, serde_json::Value>,
) -> Result<IndexDefinition, String> {
    let idx_type = config
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Index config must contain a 'type' key".to_string())?;

    match idx_type.to_lowercase().as_str() {
        "btree" | "scalar" => create_index_definition(label, property, "btree"),
        "hash" => create_index_definition(label, property, "hash"),
        "inverted" => create_index_definition(label, property, "inverted"),

        "vector" => {
            let algorithm = config
                .get("algorithm")
                .and_then(|v| v.as_str())
                .unwrap_or("hnsw");
            let metric = match config
                .get("metric")
                .and_then(|v| v.as_str())
                .unwrap_or("cosine")
                .to_lowercase()
                .as_str()
            {
                "l2" => DistanceMetric::L2,
                "dot" => DistanceMetric::Dot,
                _ => DistanceMetric::Cosine,
            };

            let index_type = match algorithm.to_lowercase().as_str() {
                "ivf_pq" | "ivfpq" => VectorIndexType::IvfPq {
                    num_partitions: config
                        .get("partitions")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(256) as u32,
                    num_sub_vectors: config
                        .get("sub_vectors")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(16) as u32,
                    bits_per_subvector: config
                        .get("bits_per_subvector")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(8) as u8,
                },
                "flat" => VectorIndexType::Flat,
                _ => VectorIndexType::Hnsw {
                    m: config.get("m").and_then(|v| v.as_u64()).unwrap_or(16) as u32,
                    ef_construction: config
                        .get("ef_construction")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(200) as u32,
                    ef_search: config
                        .get("ef_search")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(50) as u32,
                },
            };

            let embedding_config = config.get("embedding").and_then(|v| {
                let obj = v.as_object()?;
                Some(EmbeddingConfig {
                    alias: obj.get("alias")?.as_str()?.to_string(),
                    source_properties: obj
                        .get("source_properties")?
                        .as_array()?
                        .iter()
                        .filter_map(|s| s.as_str().map(|s| s.to_string()))
                        .collect(),
                    batch_size: obj
                        .get("batch_size")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(100) as usize,
                })
            });

            Ok(IndexDefinition::Vector(VectorIndexConfig {
                name: format!("idx_{}_{}_vec", label, property),
                label: label.to_string(),
                property: property.to_string(),
                index_type,
                metric,
                embedding_config,
                metadata: Default::default(),
            }))
        }

        "fulltext" => {
            let tokenizer = match config
                .get("tokenizer")
                .and_then(|v| v.as_str())
                .unwrap_or("standard")
                .to_lowercase()
                .as_str()
            {
                "whitespace" => TokenizerConfig::Whitespace,
                "ngram" => {
                    let min = config
                        .get("ngram_min")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(2) as u8;
                    let max = config
                        .get("ngram_max")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(4) as u8;
                    TokenizerConfig::Ngram { min, max }
                }
                "custom" => {
                    let name = config
                        .get("custom_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("default")
                        .to_string();
                    TokenizerConfig::Custom { name }
                }
                _ => TokenizerConfig::Standard,
            };

            Ok(IndexDefinition::FullText(FullTextIndexConfig {
                name: format!("idx_{}_{}_ft", label, property),
                label: label.to_string(),
                properties: vec![property.to_string()],
                tokenizer,
                with_positions: config
                    .get("with_positions")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true),
                metadata: Default::default(),
            }))
        }

        _ => Err(format!("Unknown index type in config: {}", idx_type)),
    }
}
