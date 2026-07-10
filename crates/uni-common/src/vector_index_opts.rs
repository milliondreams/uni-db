// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Single source of truth for parsing vector-index options into a
//! [`VectorIndexType`] + [`DistanceMetric`].
//!
//! ALL index-creation entry points use these helpers so dense vectors, native
//! multi-vectors, and MUVERA behave **identically** regardless of path:
//! - the Cypher DDL `CREATE VECTOR INDEX ... OPTIONS {type:'...', ...}` (`planner.rs`),
//! - the `uni.schema.createIndex(...)` procedure (`executor::ddl_procedures`), and
//! - the Python binding config map (`bindings/uni-db/src/core.rs`).
//!
//! (The typed Rust builder `VectorAlgo` in the `uni` crate maps directly to the same
//! `VectorIndexType`.) Lives in `uni-common` — the only crate every surface depends on.
//! Keeping the mapping here prevents the paths from drifting (they previously had
//! different default ANN types: `ivf_pq` vs `hnsw`).

use anyhow::Result;

use crate::core::schema::{DistanceMetric, VectorIndexType};
use crate::muvera::DEFAULT_FDE_SEED;

/// Raw, already-typed vector-index options collected from either entry point. Each
/// field is the user-supplied value or `None` (→ the canonical default below).
#[derive(Debug, Default, Clone)]
pub struct VectorIndexOpts<'a> {
    /// The ANN/index subtype name (`flat`, `ivf_pq`, `hnsw_sq`, `muvera`, …). For the
    /// DDL path this is `OPTIONS.type`; for the procedure it is the `algorithm` field.
    pub type_name: Option<&'a str>,
    pub partitions: Option<u32>,
    pub m: Option<u32>,
    pub ef_construction: Option<u32>,
    pub sub_vectors: Option<u32>,
    pub num_bits: Option<u8>,
    // MUVERA-only knobs.
    pub k_sim: Option<u32>,
    pub reps: Option<u32>,
    pub d_proj: Option<u32>,
    pub seed: Option<u64>,
    /// The single-vector ANN type built over the MUVERA FDE column.
    pub inner: Option<&'a str>,
}

/// Map a single-vector ANN type name to a [`VectorIndexType`], defaulting to `IvfPq`.
/// Shared by the outer index type and the MUVERA `inner` type.
fn ann_type(o: &VectorIndexOpts, t: Option<&str>) -> VectorIndexType {
    match t {
        Some("flat") => VectorIndexType::Flat,
        Some("ivf_flat") => VectorIndexType::IvfFlat {
            num_partitions: o.partitions.unwrap_or(256),
        },
        Some("ivf_sq") => VectorIndexType::IvfSq {
            num_partitions: o.partitions.unwrap_or(256),
        },
        Some("ivf_rq") => VectorIndexType::IvfRq {
            num_partitions: o.partitions.unwrap_or(256),
            num_bits: o.num_bits,
        },
        Some("hnsw_flat") => VectorIndexType::HnswFlat {
            m: o.m.unwrap_or(16),
            ef_construction: o.ef_construction.unwrap_or(200),
            num_partitions: o.partitions,
        },
        Some("hnsw" | "hnsw_sq") => VectorIndexType::HnswSq {
            m: o.m.unwrap_or(16),
            ef_construction: o.ef_construction.unwrap_or(200),
            num_partitions: o.partitions,
        },
        Some("hnsw_pq") => VectorIndexType::HnswPq {
            m: o.m.unwrap_or(16),
            ef_construction: o.ef_construction.unwrap_or(200),
            num_sub_vectors: o.sub_vectors.unwrap_or(16),
            num_partitions: o.partitions,
        },
        // None / unknown → IVF_PQ (the canonical default for BOTH paths).
        _ => VectorIndexType::IvfPq {
            num_partitions: o.partitions.unwrap_or(256),
            num_sub_vectors: o.sub_vectors.unwrap_or(16),
            bits_per_subvector: o.num_bits.unwrap_or(8),
        },
    }
}

/// Build a [`VectorIndexType`] from raw options. `type:'muvera'` produces a MUVERA index
/// whose `inner` ANN (over the derived FDE column) is itself parsed via the private
/// `ann_type` helper.
///
/// NOTE: the MUVERA defaults below (`k_sim=4, reps=20, d_proj=16`) are reasonable starting
/// points, NOT values validated for recall on a specific corpus. FDE recall is
/// corpus-dependent; tune these per corpus and confirm recall@k with the bench harness
/// `crates/uni-store/examples/multivec_recall_real.rs` (real ColBERT corpus) before relying
/// on the first-stage retrieval quality.
pub fn build_vector_index_type(o: &VectorIndexOpts) -> VectorIndexType {
    match o.type_name {
        Some("muvera") => VectorIndexType::Muvera {
            k_sim: o.k_sim.unwrap_or(4),
            reps: o.reps.unwrap_or(20),
            d_proj: o.d_proj.unwrap_or(16),
            seed: o.seed.unwrap_or(DEFAULT_FDE_SEED),
            inner: Box::new(ann_type(o, o.inner)),
        },
        other => ann_type(o, other),
    }
}

/// Parse a vector distance-metric name; errors on an unknown value. `None` → `Cosine`
/// (the ColBERT/vector default). Shared by both paths so the error text matches.
pub fn parse_vector_metric(s: Option<&str>) -> Result<DistanceMetric> {
    match s.map(|m| m.to_ascii_lowercase()).as_deref() {
        Some("l2" | "euclidean") => Ok(DistanceMetric::L2),
        Some("dot") => Ok(DistanceMetric::Dot),
        Some("l1" | "manhattan") => Ok(DistanceMetric::L1),
        Some("cosine") | None => Ok(DistanceMetric::Cosine),
        Some(other) => Err(anyhow::anyhow!(
            "Unknown vector index metric '{other}' (expected cosine, l2, dot, or l1)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(type_name: Option<&str>) -> VectorIndexOpts<'_> {
        VectorIndexOpts {
            type_name,
            ..Default::default()
        }
    }

    #[test]
    fn default_is_ivf_pq_for_both_paths() {
        // None and unknown names both default to IVF_PQ (the canonical default).
        assert!(matches!(
            build_vector_index_type(&opts(None)),
            VectorIndexType::IvfPq { .. }
        ));
        assert!(matches!(
            build_vector_index_type(&opts(Some("nonsense"))),
            VectorIndexType::IvfPq { .. }
        ));
    }

    #[test]
    fn named_types_map() {
        assert!(matches!(
            build_vector_index_type(&opts(Some("flat"))),
            VectorIndexType::Flat
        ));
        assert!(matches!(
            build_vector_index_type(&opts(Some("hnsw"))),
            VectorIndexType::HnswSq { .. }
        ));
    }

    #[test]
    fn muvera_defaults_and_inner() {
        let o = VectorIndexOpts {
            type_name: Some("muvera"),
            inner: Some("flat"),
            ..Default::default()
        };
        match build_vector_index_type(&o) {
            VectorIndexType::Muvera {
                k_sim,
                reps,
                d_proj,
                seed,
                inner,
            } => {
                assert_eq!((k_sim, reps, d_proj), (4, 20, 16));
                assert_eq!(seed, DEFAULT_FDE_SEED);
                assert!(matches!(*inner, VectorIndexType::Flat));
            }
            other => panic!("expected Muvera, got {other:?}"),
        }
        // Default inner is IVF_PQ.
        assert!(matches!(
            build_vector_index_type(&opts(Some("muvera"))),
            VectorIndexType::Muvera { inner, .. } if matches!(*inner, VectorIndexType::IvfPq { .. })
        ));
    }

    #[test]
    fn metric_parsing() {
        assert_eq!(parse_vector_metric(None).unwrap(), DistanceMetric::Cosine);
        assert_eq!(parse_vector_metric(Some("L2")).unwrap(), DistanceMetric::L2);
        assert_eq!(
            parse_vector_metric(Some("dot")).unwrap(),
            DistanceMetric::Dot
        );
        assert!(parse_vector_metric(Some("hamming")).is_err());
    }
}
