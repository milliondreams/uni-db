// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Score fusion algorithms for combining results from multiple search sources.
//!
//! Extracted from procedure_call.rs for reuse by `similar_to` and hybrid search procedures.

use std::collections::{HashMap, HashSet};
use uni_common::Vid;

/// Reciprocal Rank Fusion (RRF) over an arbitrary number of ranked lists.
///
/// RRF score = sum over every list of `1 / (k + rank + 1)`; results are sorted
/// by fused score descending. An empty list iterates zero times and therefore
/// contributes nothing, so passing a source with no hits is a no-op — a two-way
/// fusion stays identical when a third (e.g. sparse) source is absent.
pub fn fuse_rrf_multi(ranked_lists: &[&[(Vid, f32)]], k: usize) -> Vec<(Vid, f32)> {
    let mut scores: HashMap<Vid, f32> = HashMap::new();

    for ranked_list in ranked_lists {
        for (rank, (vid, _)) in ranked_list.iter().enumerate() {
            let rrf_score = 1.0 / (k as f32 + rank as f32 + 1.0);
            *scores.entry(*vid).or_default() += rrf_score;
        }
    }

    let mut results: Vec<_> = scores.into_iter().collect();
    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    results
}

/// Reciprocal Rank Fusion (RRF) for combining two ranked result lists.
///
/// Thin two-source shim over [`fuse_rrf_multi`]; preserved so existing callers
/// remain unchanged.
pub fn fuse_rrf(
    vec_results: &[(Vid, f32)],
    fts_results: &[(Vid, f32)],
    k: usize,
) -> Vec<(Vid, f32)> {
    fuse_rrf_multi(&[vec_results, fts_results], k)
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

/// How a source's raw scores map onto the `[0, 1]` fusion range.
///
/// Distances (lower is more similar) and scores (higher is more similar) need
/// opposite normalization; this tags which a source uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormKind {
    /// Lower is better (e.g. vector distance); min-max inverted to a similarity.
    DistanceToSim,
    /// Higher is better (e.g. FTS relevance or sparse dot); divided by the max.
    ScoreByMax,
}

/// One input to [`fuse_weighted_sources`]: a ranked `(vid, raw_score)` list, the
/// source's fusion weight, and how its raw scores normalize onto `[0, 1]`.
pub type WeightedSource<'a> = (&'a [(Vid, f32)], f32, NormKind);

/// Weighted fusion across an arbitrary number of per-source-normalized lists.
///
/// Each source carries its ranked `(vid, raw_score)` list, a fusion weight, and
/// a [`NormKind`] describing how its raw scores normalize onto `[0, 1]` before
/// the weighted sum. Results are sorted by fused score descending. A vid present
/// in only some sources contributes only from those sources (others count zero).
///
/// This generalizes [`fuse_weighted`] to three or more sources (e.g.
/// dense + text + sparse), reproducing the per-source normalization the
/// two-source path applies (`DistanceToSim` for vectors, `ScoreByMax` for FTS).
pub fn fuse_weighted_sources(sources: &[WeightedSource<'_>]) -> Vec<(Vid, f32)> {
    let mut fused: HashMap<Vid, f32> = HashMap::new();

    for (results, weight, norm) in sources {
        let normalized: HashMap<Vid, f32> = match norm {
            NormKind::DistanceToSim => {
                let max = results.iter().map(|(_, s)| *s).fold(f32::MIN, f32::max);
                let min = results.iter().map(|(_, s)| *s).fold(f32::MAX, f32::min);
                let range = if max > min { max - min } else { 1.0 };
                results
                    .iter()
                    .map(|(vid, dist)| (*vid, 1.0 - (dist - min) / range))
                    .collect()
            }
            NormKind::ScoreByMax => {
                let max = results.iter().map(|(_, s)| *s).fold(0.0f32, f32::max);
                results
                    .iter()
                    .map(|(vid, score)| {
                        let norm = if max > 0.0 { score / max } else { 0.0 };
                        (*vid, norm)
                    })
                    .collect()
            }
        };

        for (vid, norm_score) in normalized {
            *fused.entry(vid).or_default() += weight * norm_score;
        }
    }

    let mut results: Vec<(Vid, f32)> = fused.into_iter().collect();
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

    #[test]
    fn test_fuse_rrf_disjoint_lists() {
        let vec_results = vec![(Vid::from(1u64), 0.9), (Vid::from(2u64), 0.7)];
        let fts_results = vec![(Vid::from(3u64), 0.8), (Vid::from(4u64), 0.6)];
        let fused = fuse_rrf(&vec_results, &fts_results, 60);

        // All 4 VIDs should appear (disjoint union)
        assert_eq!(fused.len(), 4);
        let vids: HashSet<Vid> = fused.iter().map(|(v, _)| *v).collect();
        assert!(vids.contains(&Vid::from(1u64)));
        assert!(vids.contains(&Vid::from(2u64)));
        assert!(vids.contains(&Vid::from(3u64)));
        assert!(vids.contains(&Vid::from(4u64)));
    }

    #[test]
    fn test_fuse_rrf_overlapping_lists() {
        let vec_results = vec![(Vid::from(1u64), 0.9), (Vid::from(2u64), 0.7)];
        let fts_results = vec![(Vid::from(1u64), 0.8), (Vid::from(3u64), 0.6)];
        let fused = fuse_rrf(&vec_results, &fts_results, 60);

        // VID 1 appears in both lists → should have highest fused score
        assert_eq!(fused.len(), 3);
        assert_eq!(
            fused[0].0,
            Vid::from(1u64),
            "Overlapping VID should rank first"
        );
    }

    #[test]
    fn test_fuse_rrf_empty_lists() {
        let fused = fuse_rrf(&[], &[], 60);
        assert!(fused.is_empty());
    }

    #[test]
    fn test_fuse_rrf_multi_three_sources_overlap_wins() {
        let vec_results = vec![(Vid::from(1u64), 0.9), (Vid::from(2u64), 0.7)];
        let fts_results = vec![(Vid::from(1u64), 0.8), (Vid::from(3u64), 0.6)];
        let sparse_results = vec![(Vid::from(1u64), 5.0), (Vid::from(4u64), 1.0)];
        let fused = fuse_rrf_multi(&[&vec_results, &fts_results, &sparse_results], 60);

        // VID 1 is the only id in all three lists → must rank first.
        assert_eq!(fused.len(), 4);
        assert_eq!(fused[0].0, Vid::from(1u64));
    }

    #[test]
    fn test_fuse_rrf_multi_empty_third_source_is_noop() {
        let vec_results = vec![(Vid::from(1u64), 0.9), (Vid::from(2u64), 0.7)];
        let fts_results = vec![(Vid::from(1u64), 0.8), (Vid::from(3u64), 0.6)];

        // Compare as score maps: an empty source adds nothing, so the fused
        // scores are identical. (Tie ordering among equal scores follows HashMap
        // iteration order and is not stable — the original `fuse_rrf` is the same.)
        let two_way: HashMap<Vid, f32> = fuse_rrf(&vec_results, &fts_results, 60)
            .into_iter()
            .collect();
        let three_way: HashMap<Vid, f32> = fuse_rrf_multi(&[&vec_results, &fts_results, &[]], 60)
            .into_iter()
            .collect();

        assert_eq!(two_way, three_way, "absent sparse source must be a no-op");
    }

    #[test]
    fn test_fuse_weighted_sources_normalizes_per_source() {
        // Vector scores are distances (lower better); sparse are dot (higher better).
        let vec_results = vec![(Vid::from(1u64), 0.0), (Vid::from(2u64), 1.0)];
        let sparse_results = vec![(Vid::from(1u64), 2.0), (Vid::from(2u64), 4.0)];
        let fused = fuse_weighted_sources(&[
            (&vec_results, 0.5, NormKind::DistanceToSim),
            (&sparse_results, 0.5, NormKind::ScoreByMax),
        ]);

        // VID1: 0.5*1.0 (closest) + 0.5*(2/4)=0.25 → 0.75.
        // VID2: 0.5*0.0 (farthest) + 0.5*(4/4)=0.5  → 0.50.
        let v1 = fused.iter().find(|(v, _)| *v == Vid::from(1u64)).unwrap().1;
        let v2 = fused.iter().find(|(v, _)| *v == Vid::from(2u64)).unwrap().1;
        assert!((v1 - 0.75).abs() < 1e-6);
        assert!((v2 - 0.50).abs() < 1e-6);
        assert_eq!(fused[0].0, Vid::from(1u64));
    }

    #[test]
    fn test_fuse_weighted_sources_zero_max_sparse() {
        // All-zero sparse scores must normalize to 0, not divide by zero.
        let vec_results = vec![(Vid::from(1u64), 0.0), (Vid::from(2u64), 1.0)];
        let sparse_results = vec![(Vid::from(1u64), 0.0), (Vid::from(2u64), 0.0)];
        let fused = fuse_weighted_sources(&[
            (&vec_results, 0.5, NormKind::DistanceToSim),
            (&sparse_results, 0.5, NormKind::ScoreByMax),
        ]);

        let v1 = fused.iter().find(|(v, _)| *v == Vid::from(1u64)).unwrap().1;
        assert!(
            (v1 - 0.5).abs() < 1e-6,
            "sparse contributes 0 when all zero"
        );
    }
}
