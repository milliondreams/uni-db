// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `similar_to()` expression function — unified similarity scoring.
//!
//! Dispatches to vector cosine similarity, BM25 full-text scoring, or
//! hybrid fusion based on schema types. Returns a float in `[0, 1]`
//! where higher means more similar.
//!
//! This is a **point computation** (score one bound node), not a search
//! (top-K index scan). It works in WHERE, RETURN, WITH, ORDER BY, and
//! Locy rule bodies.

use anyhow::Result;
use uni_common::Value;
use uni_common::core::schema::{DistanceMetric, Schema};

use crate::query::df_graph::common::calculate_score;
use crate::query::fusion;

/// Named error types for `similar_to()` validation failures.
#[derive(Debug, thiserror::Error)]
pub enum SimilarToError {
    #[error("similar_to: property '{label}.{property}' has no vector or full-text index")]
    NoIndex { label: String, property: String },

    #[error(
        "similar_to: source {source_index} is FTS-indexed but query is a vector (FTS cannot score against vectors)"
    )]
    TypeMismatch { source_index: usize },

    #[error(
        "similar_to: source {source_index} is a vector property but query is a string, and the index has no embedding config for auto-embedding"
    )]
    NoEmbeddingConfig { source_index: usize },

    #[error("similar_to: weights length ({weights_len}) != sources length ({sources_len})")]
    WeightsLengthMismatch {
        weights_len: usize,
        sources_len: usize,
    },

    #[error("similar_to: weights must sum to 1.0 (got {sum})")]
    WeightsNotNormalized { sum: f32 },

    #[error("similar_to: unknown method '{method}', expected 'rrf' or 'weighted'")]
    InvalidMethod { method: String },

    #[error("similar_to: {message}")]
    InvalidOption { message: String },

    #[error("similar_to: vector dimensions mismatch: {a} vs {b}")]
    DimensionMismatch { a: usize, b: usize },

    #[error("similar_to: expected vector or list of numbers, got {actual}")]
    InvalidVectorValue { actual: String },

    #[error("similar_to: weighted fusion requires 'weights' option")]
    WeightsRequired,

    #[error("similar_to takes 2 or 3 arguments (sources, queries [, options]), got {count}")]
    InvalidArity { count: usize },

    #[error("similar_to requires GraphExecutionContext")]
    NoGraphContext,
}

/// Fusion method for multi-source scoring.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum FusionMethod {
    /// Reciprocal Rank Fusion (default). Falls back to equal-weight
    /// fusion in point-computation context.
    #[default]
    Rrf,
    /// Weighted sum of per-source scores.
    Weighted,
}

/// Options for `similar_to()` controlling fusion and scoring behavior.
#[derive(Debug, Clone)]
pub struct SimilarToOptions {
    /// Fusion algorithm when multiple sources are present.
    pub method: FusionMethod,
    /// Per-source weights for weighted fusion. Must sum to 1.0.
    pub weights: Option<Vec<f32>>,
    /// RRF constant k (default 60).
    pub k: usize,
    /// BM25 saturation constant for FTS normalization (default 1.0).
    pub fts_k: f32,
}

impl Default for SimilarToOptions {
    fn default() -> Self {
        SimilarToOptions {
            method: FusionMethod::Rrf,
            weights: None,
            k: 60,
            fts_k: 1.0,
        }
    }
}

/// Parse options from a `Value::Map`.
pub fn parse_options(value: &Value) -> Result<SimilarToOptions, SimilarToError> {
    let map = match value {
        Value::Map(m) => m,
        Value::Null => return Ok(SimilarToOptions::default()),
        _ => {
            return Err(SimilarToError::InvalidOption {
                message: format!("options must be a map, got {:?}", value),
            });
        }
    };

    let mut opts = SimilarToOptions::default();

    if let Some(method_val) = map.get("method") {
        match method_val.as_str() {
            Some("rrf") => opts.method = FusionMethod::Rrf,
            Some("weighted") => opts.method = FusionMethod::Weighted,
            Some(other) => {
                return Err(SimilarToError::InvalidMethod {
                    method: other.to_string(),
                });
            }
            None => {
                return Err(SimilarToError::InvalidOption {
                    message: "'method' must be a string ('rrf' or 'weighted')".to_string(),
                });
            }
        }
    }

    if let Some(weights_val) = map.get("weights") {
        match weights_val {
            Value::List(list) => {
                let weights: Result<Vec<f32>, SimilarToError> = list
                    .iter()
                    .map(|v| {
                        v.as_f64()
                            .map(|f| f as f32)
                            .ok_or_else(|| SimilarToError::InvalidOption {
                                message: "weight must be a number".to_string(),
                            })
                    })
                    .collect();
                opts.weights = Some(weights?);
            }
            _ => {
                return Err(SimilarToError::InvalidOption {
                    message: "'weights' must be a list of numbers".to_string(),
                });
            }
        }
    }

    if let Some(k_val) = map.get("k") {
        opts.k = k_val
            .as_i64()
            .ok_or_else(|| SimilarToError::InvalidOption {
                message: "'k' must be an integer".to_string(),
            })? as usize;
    }

    if let Some(fts_k_val) = map.get("fts_k") {
        opts.fts_k = fts_k_val
            .as_f64()
            .ok_or_else(|| SimilarToError::InvalidOption {
                message: "'fts_k' must be a number".to_string(),
            })? as f32;
    }

    Ok(opts)
}

/// What type of source a property represents.
#[derive(Debug, Clone)]
pub enum SourceType {
    /// Vector property with a vector index.
    Vector {
        metric: DistanceMetric,
        has_embedding_config: bool,
    },
    /// String property with a full-text index.
    Fts,
}

/// Resolve the source type for a property given the schema.
pub fn resolve_source_type(
    schema: &Schema,
    label: &str,
    property: &str,
) -> Result<SourceType, SimilarToError> {
    // Check vector index first
    if let Some(vec_config) = schema.vector_index_for_property(label, property) {
        return Ok(SourceType::Vector {
            metric: vec_config.metric.clone(),
            has_embedding_config: vec_config.embedding_config.is_some(),
        });
    }

    // Check full-text index
    if schema
        .fulltext_index_for_property(label, property)
        .is_some()
    {
        return Ok(SourceType::Fts);
    }

    Err(SimilarToError::NoIndex {
        label: label.to_string(),
        property: property.to_string(),
    })
}

/// Compute cosine similarity between two vectors, returning a score in [0, 1].
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> Result<f32, SimilarToError> {
    if a.len() != b.len() {
        return Err(SimilarToError::DimensionMismatch {
            a: a.len(),
            b: b.len(),
        });
    }

    let mut dot = 0.0f64;
    let mut mag1 = 0.0f64;
    let mut mag2 = 0.0f64;
    for (x, y) in a.iter().zip(b.iter()) {
        let x = *x as f64;
        let y = *y as f64;
        dot += x * y;
        mag1 += x * x;
        mag2 += y * y;
    }
    let mag1 = mag1.sqrt();
    let mag2 = mag2.sqrt();

    if mag1 == 0.0 || mag2 == 0.0 {
        return Ok(0.0);
    }

    // Cosine similarity in [-1, 1], map to [0, 1]
    let sim = (dot / (mag1 * mag2)) as f32;
    Ok(sim.clamp(-1.0, 1.0))
}

/// Score two vectors using the specified distance metric, returning a similarity
/// score where higher means more similar.
///
/// - **Cosine**: raw cosine similarity in \[-1, 1\] (delegates to [`cosine_similarity`]).
/// - **L2**: `1 / (1 + d²)` where d² is squared Euclidean distance; range (0, 1\].
/// - **Dot**: raw dot product (for normalised vectors equals cosine similarity).
pub fn score_vectors(a: &[f32], b: &[f32], metric: &DistanceMetric) -> Result<f32, SimilarToError> {
    if a.len() != b.len() {
        return Err(SimilarToError::DimensionMismatch {
            a: a.len(),
            b: b.len(),
        });
    }
    match metric {
        DistanceMetric::Cosine => cosine_similarity(a, b),
        DistanceMetric::L2 => {
            let distance = metric.compute_distance(a, b);
            Ok(calculate_score(distance, metric))
        }
        DistanceMetric::Dot => {
            // compute_distance returns -dot (LanceDB convention: lower = more similar).
            // Negate to recover the actual dot product as a similarity score.
            let distance = metric.compute_distance(a, b);
            Ok(-distance)
        }
        // DistanceMetric is #[non_exhaustive]; fall back to L2-style normalisation.
        _ => {
            let distance = metric.compute_distance(a, b);
            Ok(calculate_score(distance, metric))
        }
    }
}

/// Normalize a BM25 score to [0, 1] using a saturation function.
///
/// `normalized = score / (score + fts_k)` where `fts_k` defaults to 1.0.
pub fn normalize_bm25(score: f32, fts_k: f32) -> f32 {
    if score <= 0.0 {
        return 0.0;
    }
    score / (score + fts_k)
}

/// Compute pure vector-vs-vector similarity (no storage access needed).
///
/// Both values must be `Value::List` of numbers or `Value::Vector`.
pub fn eval_similar_to_pure(v1: &Value, v2: &Value) -> Result<Value> {
    let vec1 = value_to_f32_vec(v1)?;
    let vec2 = value_to_f32_vec(v2)?;
    let sim = cosine_similarity(&vec1, &vec2)?;
    Ok(Value::Float(sim as f64))
}

/// Convert a Value to a `Vec<f32>` for vector operations.
pub fn value_to_f32_vec(v: &Value) -> Result<Vec<f32>, SimilarToError> {
    match v {
        Value::Vector(vec) => Ok(vec.clone()),
        Value::List(list) => list
            .iter()
            .map(|v| {
                v.as_f64()
                    .map(|f| f as f32)
                    .ok_or_else(|| SimilarToError::InvalidOption {
                        message: "vector element must be a number".to_string(),
                    })
            })
            .collect(),
        _ => Err(SimilarToError::InvalidVectorValue {
            actual: format!("{:?}", v),
        }),
    }
}

/// Validate options against the number of sources.
pub fn validate_options(opts: &SimilarToOptions, num_sources: usize) -> Result<(), SimilarToError> {
    if let Some(ref weights) = opts.weights {
        if weights.len() != num_sources {
            return Err(SimilarToError::WeightsLengthMismatch {
                weights_len: weights.len(),
                sources_len: num_sources,
            });
        }
        let sum: f32 = weights.iter().sum();
        if (sum - 1.0).abs() > 0.01 {
            return Err(SimilarToError::WeightsNotNormalized { sum });
        }
    }
    Ok(())
}

/// Validate per-pair type compatibility.
///
/// Returns an error if a Vector query is paired with an FTS source,
/// or a String query is paired with a Vector source that has no embedding config.
pub fn validate_pair(
    source_type: &SourceType,
    query_is_vector: bool,
    query_is_string: bool,
    source_index: usize,
) -> Result<(), SimilarToError> {
    match source_type {
        SourceType::Fts if query_is_vector => Err(SimilarToError::TypeMismatch { source_index }),
        SourceType::Vector {
            has_embedding_config: false,
            ..
        } if query_is_string => Err(SimilarToError::NoEmbeddingConfig { source_index }),
        _ => Ok(()),
    }
}

/// Fuse multiple per-source scores into a single score.
pub fn fuse_scores(scores: &[f32], opts: &SimilarToOptions) -> Result<f32, SimilarToError> {
    if scores.len() == 1 {
        return Ok(scores[0]);
    }

    match opts.method {
        FusionMethod::Weighted => {
            let weights = opts
                .weights
                .as_ref()
                .ok_or(SimilarToError::WeightsRequired)?;
            Ok(fusion::fuse_weighted_multi(scores, weights))
        }
        FusionMethod::Rrf => {
            // In point-computation context, RRF degenerates.
            // Fall back to equal-weight fusion.
            let (score, _used_fallback) = fusion::fuse_rrf_point(scores);
            // TODO: emit WarningCode::RrfInPointContext when used_fallback is true
            Ok(score)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_parse_options_default() {
        let opts = parse_options(&Value::Null).unwrap();
        assert_eq!(opts.method, FusionMethod::Rrf);
        assert_eq!(opts.k, 60);
        assert!((opts.fts_k - 1.0).abs() < 1e-6);
        assert!(opts.weights.is_none());
    }

    #[test]
    fn test_parse_options_weighted() {
        let mut map = HashMap::new();
        map.insert("method".to_string(), Value::String("weighted".to_string()));
        map.insert(
            "weights".to_string(),
            Value::List(vec![Value::Float(0.7), Value::Float(0.3)]),
        );
        let opts = parse_options(&Value::Map(map)).unwrap();
        assert_eq!(opts.method, FusionMethod::Weighted);
        let weights = opts.weights.unwrap();
        assert!((weights[0] - 0.7).abs() < 1e-6);
        assert!((weights[1] - 0.3).abs() < 1e-6);
    }

    #[test]
    fn test_parse_options_rrf_with_k() {
        let mut map = HashMap::new();
        map.insert("method".to_string(), Value::String("rrf".to_string()));
        map.insert("k".to_string(), Value::Int(30));
        let opts = parse_options(&Value::Map(map)).unwrap();
        assert_eq!(opts.method, FusionMethod::Rrf);
        assert_eq!(opts.k, 30);
    }

    #[test]
    fn test_parse_options_fts_k() {
        let mut map = HashMap::new();
        map.insert("fts_k".to_string(), Value::Float(2.0));
        let opts = parse_options(&Value::Map(map)).unwrap();
        assert!((opts.fts_k - 2.0).abs() < 1e-6);
    }

    #[test]
    fn test_parse_options_invalid_method() {
        let mut map = HashMap::new();
        map.insert("method".to_string(), Value::String("invalid".to_string()));
        assert!(parse_options(&Value::Map(map)).is_err());
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let v = vec![1.0, 0.0, 0.0];
        let sim = cosine_similarity(&v, &v).unwrap();
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let sim = cosine_similarity(&a, &b).unwrap();
        assert!((sim - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_opposite() {
        let a = vec![1.0, 0.0];
        let b = vec![-1.0, 0.0];
        let sim = cosine_similarity(&a, &b).unwrap();
        assert!((sim - (-1.0)).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_dimension_mismatch() {
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!(cosine_similarity(&a, &b).is_err());
    }

    #[test]
    fn test_normalize_bm25() {
        assert!((normalize_bm25(0.0, 1.0) - 0.0).abs() < 1e-6);
        assert!((normalize_bm25(1.0, 1.0) - 0.5).abs() < 1e-6);
        assert!((normalize_bm25(9.0, 1.0) - 0.9).abs() < 1e-6);
        assert!((normalize_bm25(99.0, 1.0) - 0.99).abs() < 1e-4);
    }

    #[test]
    fn test_normalize_bm25_custom_k() {
        assert!((normalize_bm25(2.0, 2.0) - 0.5).abs() < 1e-6);
    }

    #[test]
    fn test_eval_similar_to_pure() {
        let v1 = Value::List(vec![Value::Float(1.0), Value::Float(0.0)]);
        let v2 = Value::List(vec![Value::Float(1.0), Value::Float(0.0)]);
        let result = eval_similar_to_pure(&v1, &v2).unwrap();
        match result {
            Value::Float(f) => assert!((f - 1.0).abs() < 1e-6),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_similar_to_pure_vector_type() {
        let v1 = Value::Vector(vec![1.0, 0.0]);
        let v2 = Value::Vector(vec![0.0, 1.0]);
        let result = eval_similar_to_pure(&v1, &v2).unwrap();
        match result {
            Value::Float(f) => assert!((f - 0.0).abs() < 1e-6),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_validate_options_weights_length() {
        let opts = SimilarToOptions {
            weights: Some(vec![0.5]),
            ..Default::default()
        };
        assert!(validate_options(&opts, 2).is_err());
    }

    #[test]
    fn test_validate_options_weights_sum() {
        let opts = SimilarToOptions {
            weights: Some(vec![0.5, 0.3]),
            ..Default::default()
        };
        assert!(validate_options(&opts, 2).is_err());
    }

    #[test]
    fn test_validate_options_ok() {
        let opts = SimilarToOptions {
            weights: Some(vec![0.7, 0.3]),
            ..Default::default()
        };
        assert!(validate_options(&opts, 2).is_ok());
    }

    #[test]
    fn test_validate_pair_fts_vector_query() {
        assert!(validate_pair(&SourceType::Fts, true, false, 0).is_err());
    }

    #[test]
    fn test_validate_pair_vector_string_no_embed() {
        let st = SourceType::Vector {
            metric: DistanceMetric::Cosine,
            has_embedding_config: false,
        };
        assert!(validate_pair(&st, false, true, 0).is_err());
    }

    #[test]
    fn test_validate_pair_vector_string_with_embed() {
        let st = SourceType::Vector {
            metric: DistanceMetric::Cosine,
            has_embedding_config: true,
        };
        assert!(validate_pair(&st, false, true, 0).is_ok());
    }

    #[test]
    fn test_validate_pair_vector_vector() {
        let st = SourceType::Vector {
            metric: DistanceMetric::Cosine,
            has_embedding_config: false,
        };
        assert!(validate_pair(&st, true, false, 0).is_ok());
    }

    #[test]
    fn test_validate_pair_fts_string() {
        assert!(validate_pair(&SourceType::Fts, false, true, 0).is_ok());
    }

    #[test]
    fn test_fuse_scores_single() {
        let opts = SimilarToOptions::default();
        let score = fuse_scores(&[0.8], &opts).unwrap();
        assert!((score - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_fuse_scores_weighted() {
        let opts = SimilarToOptions {
            method: FusionMethod::Weighted,
            weights: Some(vec![0.7, 0.3]),
            ..Default::default()
        };
        let score = fuse_scores(&[0.8, 0.6], &opts).unwrap();
        assert!((score - 0.74).abs() < 1e-6);
    }

    #[test]
    fn test_fuse_scores_rrf_fallback() {
        let opts = SimilarToOptions::default();
        let score = fuse_scores(&[0.8, 0.6], &opts).unwrap();
        // RRF in point context falls back to equal weights: (0.8 + 0.6) / 2 = 0.7
        assert!((score - 0.7).abs() < 1e-6);
    }

    // -----------------------------------------------------------------------
    // score_vectors() tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_score_vectors_cosine_identical() {
        let v = vec![1.0, 0.0, 0.0];
        let score = score_vectors(&v, &v, &DistanceMetric::Cosine).unwrap();
        assert!((score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_score_vectors_cosine_matches_raw() {
        // score_vectors with Cosine delegates to cosine_similarity
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.8, 0.6, 0.0];
        let raw = cosine_similarity(&a, &b).unwrap();
        let scored = score_vectors(&a, &b, &DistanceMetric::Cosine).unwrap();
        assert!((raw - scored).abs() < 1e-6);
    }

    #[test]
    fn test_score_vectors_l2() {
        // [1,0,0] vs [0,1,0]: L2 squared distance = 2, score = 1/(1+2) ≈ 0.333
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let score = score_vectors(&a, &b, &DistanceMetric::L2).unwrap();
        assert!((score - 1.0 / 3.0).abs() < 1e-5);
    }

    #[test]
    fn test_score_vectors_l2_identical() {
        let v = vec![1.0, 0.0, 0.0];
        let score = score_vectors(&v, &v, &DistanceMetric::L2).unwrap();
        assert!((score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_score_vectors_dot() {
        // [1,0,0] dot [0.8,0.6,0] = 0.8
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.8, 0.6, 0.0];
        let score = score_vectors(&a, &b, &DistanceMetric::Dot).unwrap();
        assert!((score - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_score_vectors_dot_identical() {
        let v = vec![1.0, 0.0, 0.0];
        let score = score_vectors(&v, &v, &DistanceMetric::Dot).unwrap();
        assert!((score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_score_vectors_dimension_mismatch() {
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!(score_vectors(&a, &b, &DistanceMetric::Cosine).is_err());
    }
}
