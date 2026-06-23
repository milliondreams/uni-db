// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Vector / FTS / hybrid search procedure bodies, relocated from
//! `procedure_call.rs` during the M4 vector/fts/search port.
//!
//! Each `run_*` function takes a [`QueryProcedureHost`] (the snapshot
//! wrapper) rather than a `&GraphExecutionContext`, so the
//! `procedures_plugin/{vector,fts,search}.rs` impls can call them from
//! `ProcedurePlugin::invoke` after downcasting `ctx.host`. The legacy
//! match arms in `procedure_call.rs::execute_procedure` are deleted
//! once the plugin registrations land.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    Float32Builder, Float64Builder, Int64Builder, StringBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::common::Result as DFResult;
use uni_common::Value;
use uni_common::core::id::Vid;
use uni_common::core::schema::DistanceMetric;

use crate::query::df_graph::common::{arrow_err, calculate_score};
use crate::query::df_graph::procedure_call::{
    create_empty_batch, extract_optional_filter, map_yield_to_canonical, require_string_arg,
};
use crate::query::df_graph::scan::resolve_property_type;
use crate::query::executor::procedure_host::QueryProcedureHost;

// Rust guideline compliant

// ---------------------------------------------------------------------------
// Argument helpers (kept here because vector/fts/search are the only callers)
// ---------------------------------------------------------------------------

pub(crate) fn extract_optional_threshold(args: &[Value], index: usize) -> Option<f64> {
    args.get(index)
        .and_then(|v| if v.is_null() { None } else { v.as_f64() })
}

pub(crate) fn require_int_arg(args: &[Value], index: usize, description: &str) -> DFResult<usize> {
    args.get(index)
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "{description} must be an integer"
            ))
        })
}

/// Extract a vector from a `Value` (either `Value::Vector` or a list of
/// numbers).
pub(crate) fn extract_vector(val: &Value) -> DFResult<Vec<f32>> {
    match val {
        Value::Vector(vec) => Ok(vec.clone()),
        Value::List(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                if let Some(f) = v.as_f64() {
                    out.push(f as f32);
                } else {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Query vector must contain numbers".to_string(),
                    ));
                }
            }
            Ok(out)
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Query vector must be a list or vector".to_string(),
        )),
    }
}

/// Extract a multi-vector (a list of token vectors) from a `Value`, for
/// late-interaction (ColBERT / MaxSim) queries and document properties.
///
/// Accepts a `Value::List` whose elements are each a vector (`Value::Vector`
/// or a list of numbers, via [`extract_vector`]).
pub(crate) fn extract_vector_list(val: &Value) -> DFResult<Vec<Vec<f32>>> {
    match val {
        Value::List(arr) => arr.iter().map(extract_vector).collect(),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Multi-vector query must be a list of vectors".to_string(),
        )),
    }
}

/// Parse a distance-metric name (case-insensitive); defaults to `Cosine`.
fn parse_distance_metric(s: &str) -> DistanceMetric {
    match s.to_ascii_lowercase().as_str() {
        "dot" => DistanceMetric::Dot,
        "l2" | "euclidean" => DistanceMetric::L2,
        _ => DistanceMetric::Cosine,
    }
}

// ---------------------------------------------------------------------------
// Reranker configuration + per-call reranker context
// ---------------------------------------------------------------------------

/// Sentinel reranker alias selecting in-process MaxSim (late-interaction /
/// ColBERT) scoring instead of a neural cross-encoder model.
pub(super) const MAXSIM_RERANKER: &str = "maxsim";

/// Configuration for an optional reranking stage.
///
/// When `alias` equals [`MAXSIM_RERANKER`], scoring is in-process MaxSim over a
/// stored multi-vector property (`maxsim_query` is the per-token query and
/// `metric` its similarity metric); otherwise it is a neural cross-encoder.
pub(super) struct RerankerConfig {
    pub alias: String,
    pub property: String,
    pub k: usize,
    pub query_override: Option<String>,
    /// MaxSim query multi-vector; `Some` only for the [`MAXSIM_RERANKER`] alias.
    pub maxsim_query: Option<Vec<Vec<f32>>>,
    /// Similarity metric for MaxSim scoring (default `Cosine`).
    pub metric: DistanceMetric,
}

pub(super) fn parse_reranker_options(
    options_map: Option<&HashMap<String, Value>>,
    k: usize,
    default_text_property: Option<&str>,
) -> Option<RerankerConfig> {
    let map = options_map?;
    let alias = map.get("reranker")?.as_str()?.to_string();
    let property = map
        .get("reranker_property")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| default_text_property.map(String::from))
        .unwrap_or_default();
    let reranker_k = map
        .get("reranker_k")
        .and_then(|v| v.as_u64())
        .map(|v| (v as usize).clamp(k, 1000))
        .unwrap_or((k * 3).min(1000));
    let query_override = map
        .get("reranker_query")
        .and_then(|v| v.as_str())
        .map(String::from);
    // MaxSim mode: parse the per-token query multi-vector and metric. The query
    // is parsed best-effort here; a missing/malformed query is reported as a
    // hard error at rerank time so a requested maxsim never silently no-ops.
    let (maxsim_query, metric) = if alias == MAXSIM_RERANKER {
        let q = map
            .get("maxsim_query")
            .and_then(|v| extract_vector_list(v).ok());
        let m = map
            .get("maxsim_metric")
            .and_then(|v| v.as_str())
            .map(parse_distance_metric)
            .unwrap_or(DistanceMetric::Cosine);
        (q, m)
    } else {
        (None, DistanceMetric::Cosine)
    };
    Some(RerankerConfig {
        alias,
        property,
        k: reranker_k,
        query_override,
        maxsim_query,
        metric,
    })
}

fn sigmoid(x: f32) -> f32 {
    1.0 / (1.0 + (-x).exp())
}

/// Context from a cross-encoder reranking stage.
pub(super) struct RerankContext {
    pub scores: HashMap<Vid, f32>,
    pub props: HashMap<Vid, uni_common::Properties>,
}

async fn rerank_candidates(
    host: &QueryProcedureHost,
    candidates: Vec<(Vid, f32)>,
    label: &str,
    query_text: &str,
    config: &RerankerConfig,
    k: usize,
) -> DFResult<(Vec<(Vid, f32)>, RerankContext)> {
    let vids: Vec<Vid> = candidates.iter().map(|(v, _)| *v).collect();

    let property_manager = host.property_manager().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Cannot rerank: property manager not available on host".to_string(),
        )
    })?;
    let query_ctx = host.query_context();
    let props_map = property_manager
        .get_batch_vertex_props_for_label(&vids, label, Some(&query_ctx))
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    // MaxSim (late-interaction / ColBERT) rerank: no neural model — score each
    // candidate's stored multi-vector property against the query multi-vector
    // in-process. Pure CPU; `reranker_k` (clamped <= 1000) bounds the work.
    if config.alias == MAXSIM_RERANKER {
        let query = config.maxsim_query.as_ref().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "maxsim reranker requires a valid 'maxsim_query' option (a list of vectors)"
                    .to_string(),
            )
        })?;
        let mut scored: Vec<(Vid, f32)> = Vec::with_capacity(vids.len());
        for vid in &vids {
            // Missing/empty multi-vector property -> no document tokens -> score 0.
            let doc_tokens = props_map
                .get(vid)
                .and_then(|p| p.get(&config.property))
                .map(extract_vector_list)
                .transpose()?
                .unwrap_or_default();
            let score = uni_query_functions::similar_to::maxsim(query, &doc_tokens, &config.metric)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
            scored.push((*vid, score));
        }
        let rerank_map: HashMap<Vid, f32> = scored.iter().copied().collect();
        let mut reranked = scored;
        reranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        reranked.truncate(k);
        return Ok((
            reranked,
            RerankContext {
                scores: rerank_map,
                props: props_map,
            },
        ));
    }

    let doc_texts: Vec<String> = vids
        .iter()
        .map(|vid| {
            props_map
                .get(vid)
                .and_then(|p| p.get(&config.property))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();

    let runtime = host.xervo_runtime().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Cannot rerank: Uni-Xervo runtime not configured".to_string(),
        )
    })?;
    let reranker = runtime
        .reranker(&config.alias)
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    let doc_refs: Vec<&str> = doc_texts.iter().map(|s| s.as_str()).collect();
    let scored = reranker.rerank(query_text, &doc_refs).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!("Reranker inference failed: {e}"))
    })?;

    let mut reranked: Vec<(Vid, f32)> = scored
        .iter()
        .map(|sd| (vids[sd.index], sigmoid(sd.score)))
        .collect();
    reranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    reranked.truncate(k);

    let rerank_map: HashMap<Vid, f32> = scored
        .iter()
        .map(|sd| (vids[sd.index], sigmoid(sd.score)))
        .collect();

    Ok((
        reranked,
        RerankContext {
            scores: rerank_map,
            props: props_map,
        },
    ))
}

// ---------------------------------------------------------------------------
// Auto-embed
// ---------------------------------------------------------------------------

async fn auto_embed_text(
    host: &QueryProcedureHost,
    label: &str,
    property: &str,
    query_text: &str,
) -> DFResult<Vec<f32>> {
    let storage = host.storage();
    let uni_schema = storage.schema_manager().schema();
    let index_config = uni_schema.vector_index_for_property(label, property);

    let embedding_config = index_config
        .and_then(|cfg| cfg.embedding_config.as_ref())
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Cannot auto-embed: vector index for {label}.{property} has no embedding_config. \
                 Either provide a pre-computed vector or create the index with embedding options."
            ))
        })?;

    let runtime = host.xervo_runtime().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Cannot auto-embed: Uni-Xervo runtime not configured".to_string(),
        )
    })?;

    let embedder = runtime
        .embedding(&embedding_config.alias)
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    let prefixed_query = match &embedding_config.query_prefix {
        Some(prefix) => format!("{prefix}{query_text}"),
        None => query_text.to_string(),
    };

    let embeddings = embedder
        .embed(vec![prefixed_query.as_str()])
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    embeddings.into_iter().next().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Embedding service returned no results".to_string(),
        )
    })
}

// ---------------------------------------------------------------------------
// Batch builders
// ---------------------------------------------------------------------------

pub(super) struct HybridScoreContext<'a> {
    pub vec_score_map: &'a HashMap<Vid, f32>,
    pub fts_score_map: &'a HashMap<Vid, f32>,
    pub fts_max: f32,
    pub metric: &'a DistanceMetric,
}

pub(super) struct BatchBuildCtx<'a> {
    pub yield_items: &'a [(String, Option<String>)],
    pub target_properties: &'a HashMap<String, Vec<String>>,
    pub host: &'a QueryProcedureHost,
    pub schema: &'a SchemaRef,
    pub rerank_ctx: Option<&'a RerankContext>,
}

fn build_node_yield_columns(
    vids: &[Vid],
    label: &str,
    output_name: &str,
    target_properties: &HashMap<String, Vec<String>>,
    props_map: &HashMap<Vid, uni_common::Properties>,
    label_props: Option<&std::collections::HashMap<String, uni_common::core::schema::PropertyMeta>>,
) -> DFResult<Vec<ArrayRef>> {
    let num_rows = vids.len();
    let mut columns = Vec::new();

    let mut vid_builder = UInt64Builder::with_capacity(num_rows);
    for vid in vids {
        vid_builder.append_value(vid.as_u64());
    }
    columns.push(Arc::new(vid_builder.finish()) as ArrayRef);

    let mut var_builder = StringBuilder::with_capacity(num_rows, num_rows * 20);
    for vid in vids {
        var_builder.append_value(vid.to_string());
    }
    columns.push(Arc::new(var_builder.finish()) as ArrayRef);

    let mut labels_builder = arrow_array::builder::ListBuilder::new(StringBuilder::new());
    for _ in 0..num_rows {
        labels_builder.values().append_value(label);
        labels_builder.append(true);
    }
    columns.push(Arc::new(labels_builder.finish()) as ArrayRef);

    if let Some(props) = target_properties.get(output_name) {
        for prop_name in props {
            let data_type = resolve_property_type(prop_name, label_props);
            let column = crate::query::df_graph::scan::build_property_column_static(
                vids, props_map, prop_name, &data_type,
            )?;
            columns.push(column);
        }
    }

    Ok(columns)
}

async fn build_search_result_batch(
    results: &[(Vid, f32)],
    label: &str,
    metric: &DistanceMetric,
    batch_ctx: &BatchBuildCtx<'_>,
) -> DFResult<Option<RecordBatch>> {
    let num_rows = results.len();
    let vids: Vec<Vid> = results.iter().map(|(vid, _)| *vid).collect();
    let distances: Vec<f32> = results.iter().map(|(_, d)| *d).collect();

    let retrieval_scores: Vec<f32> = distances
        .iter()
        .map(|dist| calculate_score(*dist, metric))
        .collect();

    let query_ctx = batch_ctx.host.query_context();
    let uni_schema = batch_ctx.host.storage().schema_manager().schema();
    let label_props = uni_schema.properties.get(label);

    let has_node_yield = batch_ctx
        .yield_items
        .iter()
        .any(|(name, _)| map_yield_to_canonical(name) == "node");

    let owned_props;
    let props_map = if let Some(rctx) = batch_ctx.rerank_ctx {
        &rctx.props
    } else if has_node_yield {
        let property_manager = batch_ctx.host.property_manager().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "Cannot materialise node properties: property manager not available on host"
                    .to_string(),
            )
        })?;
        owned_props = property_manager
            .get_batch_vertex_props_for_label(&vids, label, Some(&query_ctx))
            .await
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        &owned_props
    } else {
        owned_props = HashMap::new();
        &owned_props
    };

    let mut columns: Vec<ArrayRef> = Vec::new();
    for (name, alias) in batch_ctx.yield_items {
        let output_name = alias.as_ref().unwrap_or(name);
        let canonical = map_yield_to_canonical(name);

        match canonical {
            "node" => {
                columns.extend(build_node_yield_columns(
                    &vids,
                    label,
                    output_name,
                    batch_ctx.target_properties,
                    props_map,
                    label_props,
                )?);
            }
            "distance" => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for dist in &distances {
                    builder.append_value(*dist as f64);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for (i, vid) in vids.iter().enumerate() {
                    let score = batch_ctx
                        .rerank_ctx
                        .and_then(|rctx| rctx.scores.get(vid).copied())
                        .unwrap_or(retrieval_scores[i]);
                    builder.append_value(score);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "rerank_score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for vid in &vids {
                    match batch_ctx.rerank_ctx.and_then(|rctx| rctx.scores.get(vid)) {
                        Some(&s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "vid" => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for vid in &vids {
                    builder.append_value(vid.as_u64() as i64);
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(num_rows, 0);
                for _ in 0..num_rows {
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    let batch = RecordBatch::try_new(batch_ctx.schema.clone(), columns).map_err(arrow_err)?;
    Ok(Some(batch))
}

async fn build_hybrid_search_batch(
    results: &[(Vid, f32)],
    scores: &HybridScoreContext<'_>,
    label: &str,
    batch_ctx: &BatchBuildCtx<'_>,
) -> DFResult<Option<RecordBatch>> {
    let num_rows = results.len();
    let vids: Vec<Vid> = results.iter().map(|(vid, _)| *vid).collect();
    let fused_scores: Vec<f32> = results.iter().map(|(_, s)| *s).collect();

    let query_ctx = batch_ctx.host.query_context();
    let uni_schema = batch_ctx.host.storage().schema_manager().schema();
    let label_props = uni_schema.properties.get(label);

    let has_node_yield = batch_ctx
        .yield_items
        .iter()
        .any(|(name, _)| map_yield_to_canonical(name) == "node");

    let owned_props;
    let props_map = if let Some(rctx) = batch_ctx.rerank_ctx {
        &rctx.props
    } else if has_node_yield {
        let property_manager = batch_ctx.host.property_manager().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "Cannot materialise node properties: property manager not available on host"
                    .to_string(),
            )
        })?;
        owned_props = property_manager
            .get_batch_vertex_props_for_label(&vids, label, Some(&query_ctx))
            .await
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        &owned_props
    } else {
        owned_props = HashMap::new();
        &owned_props
    };

    let mut columns: Vec<ArrayRef> = Vec::new();
    for (name, alias) in batch_ctx.yield_items {
        let output_name = alias.as_ref().unwrap_or(name);
        let canonical = map_yield_to_canonical(name);

        match canonical {
            "node" => {
                columns.extend(build_node_yield_columns(
                    &vids,
                    label,
                    output_name,
                    batch_ctx.target_properties,
                    props_map,
                    label_props,
                )?);
            }
            "vid" => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for vid in &vids {
                    builder.append_value(vid.as_u64() as i64);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for (i, vid) in vids.iter().enumerate() {
                    let score = batch_ctx
                        .rerank_ctx
                        .and_then(|rctx| rctx.scores.get(vid).copied())
                        .unwrap_or(fused_scores[i]);
                    builder.append_value(score);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "rerank_score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for vid in &vids {
                    match batch_ctx.rerank_ctx.and_then(|rctx| rctx.scores.get(vid)) {
                        Some(&s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "vector_score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for vid in &vids {
                    if let Some(&dist) = scores.vec_score_map.get(vid) {
                        let score = calculate_score(dist, scores.metric);
                        builder.append_value(score);
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "fts_score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for vid in &vids {
                    if let Some(&raw_score) = scores.fts_score_map.get(vid) {
                        let norm = if scores.fts_max > 0.0 {
                            raw_score / scores.fts_max
                        } else {
                            0.0
                        };
                        builder.append_value(norm);
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "distance" => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for vid in &vids {
                    if let Some(&dist) = scores.vec_score_map.get(vid) {
                        builder.append_value(dist as f64);
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(num_rows, 0);
                for _ in 0..num_rows {
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    let batch = RecordBatch::try_new(batch_ctx.schema.clone(), columns).map_err(arrow_err)?;
    Ok(Some(batch))
}

// ---------------------------------------------------------------------------
// Public entry points (called by procedures_plugin)
// ---------------------------------------------------------------------------

/// `uni.vector.query(label, property, query, k, filter?, threshold?, options?)`.
pub(crate) async fn run_vector_query(
    host: &QueryProcedureHost,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let label = require_string_arg(args, 0, "uni.vector.query: first argument (label)")?;
    let property = require_string_arg(args, 1, "uni.vector.query: second argument (property)")?;

    let query_val = args.get(2).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "uni.vector.query: third argument (query) is required".to_string(),
        )
    })?;

    let storage = host.storage();
    let query_text_from_arg = query_val.as_str().map(String::from);
    let query_vector: Vec<f32> = if let Some(ref query_text) = query_text_from_arg {
        auto_embed_text(host, &label, &property, query_text).await?
    } else {
        extract_vector(query_val)?
    };

    let k = require_int_arg(args, 3, "uni.vector.query: fourth argument (k)")?;
    let filter = extract_optional_filter(args, 4);
    let threshold = extract_optional_threshold(args, 5);
    let options_val = args.get(6);
    let options_map = options_val.and_then(|v| if v.is_null() { None } else { v.as_object() });
    let reranker_config = parse_reranker_options(options_map, k, None);

    if let Some(ref rcfg) = reranker_config {
        // MaxSim scores against `maxsim_query` (a multi-vector), not the text
        // query, so it is exempt from the cross-encoder's reranker_query rule.
        if rcfg.alias != MAXSIM_RERANKER
            && query_text_from_arg.is_none()
            && rcfg.query_override.is_none()
        {
            return Err(datafusion::error::DataFusionError::Execution(
                "Cannot rerank: query is a pre-computed vector. \
                 Provide reranker_query in options."
                    .to_string(),
            ));
        }
        if rcfg.property.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "reranker_property is required when using reranker with uni.vector.query"
                    .to_string(),
            ));
        }
    }

    let retrieval_k = reranker_config.as_ref().map_or(k, |rc| rc.k);
    let query_ctx = host.query_context();
    let mut results = storage
        .vector_search(
            &label,
            &property,
            &query_vector,
            retrieval_k,
            filter.as_deref(),
            Some(&query_ctx),
        )
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    if let Some(max_dist) = threshold {
        results.retain(|(_, dist)| *dist <= max_dist as f32);
    }

    if results.is_empty() {
        return Ok(Some(create_empty_batch(schema.clone())?));
    }

    let schema_manager = storage.schema_manager();
    let uni_schema = schema_manager.schema();
    let metric = uni_schema
        .vector_index_for_property(&label, &property)
        .map(|config| config.metric.clone())
        .unwrap_or(DistanceMetric::L2);

    let (results, rerank_ctx) = if let Some(ref rcfg) = reranker_config {
        let reranker_query = rcfg
            .query_override
            .as_deref()
            .or(query_text_from_arg.as_deref())
            .unwrap_or("");
        let (reranked, ctx) =
            rerank_candidates(host, results, &label, reranker_query, rcfg, k).await?;
        (reranked, Some(ctx))
    } else {
        (results, None)
    };

    let batch_ctx = BatchBuildCtx {
        yield_items,
        target_properties,
        host,
        schema,
        rerank_ctx: rerank_ctx.as_ref(),
    };
    build_search_result_batch(&results, &label, &metric, &batch_ctx).await
}

/// `uni.fts.query(label, property, search_term, k, filter?, threshold?, options?)`.
pub(crate) async fn run_fts_query(
    host: &QueryProcedureHost,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let label = require_string_arg(args, 0, "uni.fts.query: first argument (label)")?;
    let property = require_string_arg(args, 1, "uni.fts.query: second argument (property)")?;
    let search_term = require_string_arg(args, 2, "uni.fts.query: third argument (search_term)")?;
    let k = require_int_arg(args, 3, "uni.fts.query: fourth argument (k)")?;
    let filter = extract_optional_filter(args, 4);
    let threshold = extract_optional_threshold(args, 5);
    let options_val = args.get(6);
    let options_map = options_val.and_then(|v| if v.is_null() { None } else { v.as_object() });
    let reranker_config = parse_reranker_options(options_map, k, Some(&property));

    let retrieval_k = reranker_config.as_ref().map_or(k, |rc| rc.k);
    let storage = host.storage();
    let query_ctx = host.query_context();

    let mut results = storage
        .fts_search(
            &label,
            &property,
            &search_term,
            retrieval_k,
            filter.as_deref(),
            Some(&query_ctx),
        )
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    if let Some(min_score) = threshold {
        results.retain(|(_, score)| *score as f64 >= min_score);
    }

    if results.is_empty() {
        return Ok(Some(create_empty_batch(schema.clone())?));
    }

    let (results, rerank_ctx) = if let Some(ref rcfg) = reranker_config {
        let reranker_query = rcfg.query_override.as_deref().unwrap_or(&search_term);
        let (reranked, ctx) =
            rerank_candidates(host, results, &label, reranker_query, rcfg, k).await?;
        (reranked, Some(ctx))
    } else {
        (results, None)
    };

    let batch_ctx = BatchBuildCtx {
        yield_items,
        target_properties,
        host,
        schema,
        rerank_ctx: rerank_ctx.as_ref(),
    };
    build_search_result_batch(&results, &label, &DistanceMetric::L2, &batch_ctx).await
}

/// `uni.search(label, properties, query_text, query_vector?, k, filter?, options?)`.
pub(crate) async fn run_hybrid_search(
    host: &QueryProcedureHost,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let label = require_string_arg(args, 0, "uni.search: first argument (label)")?;

    let properties_val = args.get(1).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "uni.search: second argument (properties) is required".to_string(),
        )
    })?;

    let (vector_prop, fts_prop) = if let Some(obj) = properties_val.as_object() {
        let vec_prop = obj
            .get("vector")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let fts_prop = obj
            .get("fts")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        (vec_prop, fts_prop)
    } else if let Some(prop) = properties_val.as_str() {
        (Some(prop.to_string()), Some(prop.to_string()))
    } else {
        return Err(datafusion::error::DataFusionError::Execution(
            "Properties must be an object {vector: '...', fts: '...'} or a string".to_string(),
        ));
    };

    let query_text = require_string_arg(args, 2, "uni.search: third argument (query_text)")?;

    let query_vector: Option<Vec<f32>> = args.get(3).and_then(|v| {
        if v.is_null() {
            return None;
        }
        v.as_array().map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_f64().map(|f| f as f32))
                .collect()
        })
    });

    let k = require_int_arg(args, 4, "uni.search: fifth argument (k)")?;
    let filter = extract_optional_filter(args, 5);

    let options_val = args.get(6);
    let options_map = options_val.and_then(|v| v.as_object());
    let fusion_method = options_map
        .and_then(|m| m.get("method"))
        .and_then(|v| v.as_str())
        .unwrap_or("rrf")
        .to_string();
    let alpha = options_map
        .and_then(|m| m.get("alpha"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.5) as f32;
    let over_fetch_factor = options_map
        .and_then(|m| m.get("over_fetch"))
        .and_then(|v| v.as_f64())
        .unwrap_or(2.0) as f32;
    let rrf_k = options_map
        .and_then(|m| m.get("rrf_k"))
        .and_then(|v| v.as_u64())
        .unwrap_or(60) as usize;

    let reranker_config = parse_reranker_options(options_map, k, fts_prop.as_deref());

    let over_fetch_k = (k as f32 * over_fetch_factor).ceil() as usize;
    let effective_retrieval_k = reranker_config
        .as_ref()
        .map_or(over_fetch_k, |rc| rc.k.max(over_fetch_k));

    let storage = host.storage();
    let query_ctx = host.query_context();

    let mut vector_results: Vec<(Vid, f32)> = Vec::new();
    if let Some(ref vec_prop) = vector_prop {
        let qvec = if let Some(ref v) = query_vector {
            v.clone()
        } else {
            auto_embed_text(host, &label, vec_prop, &query_text)
                .await
                .unwrap_or_default()
        };

        if !qvec.is_empty() {
            vector_results = storage
                .vector_search(
                    &label,
                    vec_prop,
                    &qvec,
                    effective_retrieval_k,
                    filter.as_deref(),
                    Some(&query_ctx),
                )
                .await
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        }
    }

    let mut fts_results: Vec<(Vid, f32)> = Vec::new();
    if let Some(ref fts_prop) = fts_prop {
        fts_results = storage
            .fts_search(
                &label,
                fts_prop,
                &query_text,
                effective_retrieval_k,
                filter.as_deref(),
                Some(&query_ctx),
            )
            .await
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    }

    let fused_results = match fusion_method.as_str() {
        "weighted" => crate::query::fusion::fuse_weighted(&vector_results, &fts_results, alpha),
        _ => crate::query::fusion::fuse_rrf(&vector_results, &fts_results, rrf_k),
    };

    let (final_results, rerank_ctx) = if let Some(ref rcfg) = reranker_config {
        let candidates: Vec<_> = fused_results.into_iter().take(rcfg.k).collect();
        if candidates.is_empty() {
            return Ok(Some(create_empty_batch(schema.clone())?));
        }
        let reranker_query = rcfg.query_override.as_deref().unwrap_or(&query_text);
        let (reranked, ctx) =
            rerank_candidates(host, candidates, &label, reranker_query, rcfg, k).await?;
        (reranked, Some(ctx))
    } else {
        let results: Vec<_> = fused_results.into_iter().take(k).collect();
        (results, None)
    };

    if final_results.is_empty() {
        return Ok(Some(create_empty_batch(schema.clone())?));
    }

    let vec_score_map: HashMap<Vid, f32> = vector_results.iter().cloned().collect();
    let fts_score_map: HashMap<Vid, f32> = fts_results.iter().cloned().collect();
    let fts_max = fts_results.iter().map(|(_, s)| *s).fold(0.0f32, f32::max);

    let uni_schema = storage.schema_manager().schema();
    let metric = vector_prop
        .as_ref()
        .and_then(|vp| {
            uni_schema
                .vector_index_for_property(&label, vp)
                .map(|config| config.metric.clone())
        })
        .unwrap_or(DistanceMetric::L2);

    let score_ctx = HybridScoreContext {
        vec_score_map: &vec_score_map,
        fts_score_map: &fts_score_map,
        fts_max,
        metric: &metric,
    };

    let batch_ctx = BatchBuildCtx {
        yield_items,
        target_properties,
        host,
        schema,
        rerank_ctx: rerank_ctx.as_ref(),
    };
    build_hybrid_search_batch(&final_results, &score_ctx, &label, &batch_ctx).await
}
