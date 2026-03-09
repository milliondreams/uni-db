// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Score fusion algorithms for combining results from multiple search sources.
//!
//! Extracted from procedure_call.rs for reuse by `similar_to` and hybrid search procedures.

use std::collections::{HashMap, HashSet};
use uni_common::Vid;

/// Reciprocal Rank Fusion (RRF) for combining ranked result lists.
///
/// RRF score = sum(1 / (k + rank + 1)) for each result list.
/// Results are sorted by fused score descending.
pub fn fuse_rrf(
    vec_results: &[(Vid, f32)],
    fts_results: &[(Vid, f32)],
    k: usize,
) -> Vec<(Vid, f32)> {
    let mut scores: HashMap<Vid, f32> = HashMap::new();

    for (rank, (vid, _)) in vec_results.iter().enumerate() {
        let rrf_score = 1.0 / (k as f32 + rank as f32 + 1.0);
        *scores.entry(*vid).or_default() += rrf_score;
    }

    for (rank, (vid, _)) in fts_results.iter().enumerate() {
        let rrf_score = 1.0 / (k as f32 + rank as f32 + 1.0);
        *scores.entry(*vid).or_default() += rrf_score;
    }

    let mut results: Vec<_> = scores.into_iter().collect();
    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    results
}

/// Weighted fusion: alpha * vec_score + (1 - alpha) * fts_score.
///
/// Both score sets are normalized to [0, 1] range before fusion.
/// Vector scores are assumed to be distances (lower = more similar)
/// and are inverted. FTS scores are normalized by max.
pub fn fuse_weighted(
    vec_results: &[(Vid, f32)],
    fts_results: &[(Vid, f32)],
    alpha: f32,
) -> Vec<(Vid, f32)> {
    // Normalize vector scores (distance -> similarity)
    let vec_max = vec_results.iter().map(|(_, s)| *s).fold(f32::MIN, f32::max);
    let vec_min = vec_results.iter().map(|(_, s)| *s).fold(f32::MAX, f32::min);
    let vec_range = if vec_max > vec_min {
        vec_max - vec_min
    } else {
        1.0
    };

    let fts_max = fts_results.iter().map(|(_, s)| *s).fold(0.0f32, f32::max);

    let vec_scores: HashMap<Vid, f32> = vec_results
        .iter()
        .map(|(vid, dist)| {
            let norm = 1.0 - (dist - vec_min) / vec_range;
            (*vid, norm)
        })
        .collect();

    let fts_scores: HashMap<Vid, f32> = fts_results
        .iter()
        .map(|(vid, score)| {
            let norm = if fts_max > 0.0 { score / fts_max } else { 0.0 };
            (*vid, norm)
        })
        .collect();

    let all_vids: HashSet<Vid> = vec_scores
        .keys()
        .chain(fts_scores.keys())
        .cloned()
        .collect();

    let mut results: Vec<(Vid, f32)> = all_vids
        .into_iter()
        .map(|vid| {
            let vec_score = *vec_scores.get(&vid).unwrap_or(&0.0);
            let fts_score = *fts_scores.get(&vid).unwrap_or(&0.0);
            let fused = alpha * vec_score + (1.0 - alpha) * fts_score;
            (vid, fused)
        })
        .collect();

    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    results
}

/// Multi-source weighted fusion for `similar_to`.
///
/// Unlike the two-source `fuse_weighted`, this operates on pre-normalized
/// `[0, 1]` scores and supports an arbitrary number of sources.
pub fn fuse_weighted_multi(scores: &[f32], weights: &[f32]) -> f32 {
    debug_assert_eq!(scores.len(), weights.len());
    scores.iter().zip(weights.iter()).map(|(s, w)| s * w).sum()
}

/// Multi-source RRF fusion for point computation context.
///
/// In point computation (single node evaluation), there is no global ranking
/// context. We fall back to weighted fusion with equal weights and emit a warning.
///
/// Returns `(fused_score, used_fallback)` where `used_fallback` is true
/// when RRF was requested but we fell back to equal-weight fusion.
pub fn fuse_rrf_point(scores: &[f32]) -> (f32, bool) {
    if scores.is_empty() {
        return (0.0, false);
    }
    let weight = 1.0 / scores.len() as f32;
    let fused: f32 = scores.iter().map(|s| s * weight).sum();
    (fused, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fuse_weighted_multi() {
        let scores = vec![0.8, 0.6];
        let weights = vec![0.7, 0.3];
        let result = fuse_weighted_multi(&scores, &weights);
        assert!((result - 0.74).abs() < 1e-6);
    }

    #[test]
    fn test_fuse_weighted_multi_equal() {
        let scores = vec![0.5, 0.5, 0.5];
        let weights = vec![1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0];
        let result = fuse_weighted_multi(&scores, &weights);
        assert!((result - 0.5).abs() < 1e-6);
    }

    #[test]
    fn test_fuse_rrf_point_fallback() {
        let scores = vec![0.8, 0.6];
        let (result, used_fallback) = fuse_rrf_point(&scores);
        assert!(used_fallback);
        assert!((result - 0.7).abs() < 1e-6);
    }

    #[test]
    fn test_fuse_rrf_point_empty() {
        let (result, used_fallback) = fuse_rrf_point(&[]);
        assert!(!used_fallback);
        assert!((result - 0.0).abs() < 1e-6);
    }
}
