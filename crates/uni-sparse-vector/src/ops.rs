//! Pure scoring/transform kernels over [`SparseVector`]. No graph/runtime
//! dependencies — this is the analogue of `uni-btic`'s interval math, kept in
//! the type crate so every layer (index, rerank, brute-force oracle) calls one
//! canonical implementation.

use crate::sparse::SparseVector;

/// Dot product of two sparse vectors via a linear merge-join over their
/// (ascending) term ids. This is the SPLADE/learned-sparse scoring primitive
/// and the exact ground truth a brute-force oracle uses.
///
/// O(|a| + |b|). Relies on the [`SparseVector`] sorted-index invariant.
pub fn sparse_dot(a: &SparseVector, b: &SparseVector) -> f32 {
    let (ai, av) = (a.indices(), a.values());
    let (bi, bv) = (b.indices(), b.values());
    let mut i = 0;
    let mut j = 0;
    let mut acc = 0.0f32;
    while i < ai.len() && j < bi.len() {
        match ai[i].cmp(&bi[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                acc += av[i] * bv[j];
                i += 1;
                j += 1;
            }
        }
    }
    acc
}

/// Euclidean (L2) norm of the weights.
pub fn l2_norm(v: &SparseVector) -> f32 {
    v.values().iter().map(|w| w * w).sum::<f32>().sqrt()
}

/// Keep only the `k` terms with the largest absolute weight, preserving the
/// ascending-index invariant. This is the universal query-side latency lever
/// for learned-sparse retrieval — high-DF / low-weight query terms dominate the
/// posting-scan cost, so dropping them trades a little recall for large speedups.
///
/// Returns the input unchanged when `k >= len`. Ties are broken by keeping the
/// lower term id (deterministic).
pub fn prune_top_k(v: &SparseVector, k: usize) -> SparseVector {
    if k >= v.len() {
        return v.clone();
    }
    if k == 0 {
        return SparseVector::new(Vec::new(), Vec::new()).expect("empty vector is always valid");
    }

    // Rank positions by descending |weight|, tie-break by ascending term id.
    let mut order: Vec<usize> = (0..v.len()).collect();
    let values = v.values();
    let indices = v.indices();
    order.sort_by(|&x, &y| {
        values[y]
            .abs()
            .partial_cmp(&values[x].abs())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(indices[x].cmp(&indices[y]))
    });
    order.truncate(k);
    // Re-sort the kept positions by term id to restore the invariant.
    order.sort_unstable();

    let kept_indices = order.iter().map(|&p| indices[p]).collect();
    let kept_values = order.iter().map(|&p| values[p]).collect();
    SparseVector::new(kept_indices, kept_values).expect("subset of a valid vector is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dot_disjoint_is_zero() {
        let a = SparseVector::new(vec![1, 3], vec![1.0, 1.0]).unwrap();
        let b = SparseVector::new(vec![2, 4], vec![1.0, 1.0]).unwrap();
        assert_eq!(sparse_dot(&a, &b), 0.0);
    }

    #[test]
    fn dot_full_overlap() {
        let a = SparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]).unwrap();
        let b = SparseVector::new(vec![1, 2, 3], vec![4.0, 5.0, 6.0]).unwrap();
        // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
        assert_eq!(sparse_dot(&a, &b), 32.0);
    }

    #[test]
    fn dot_partial_overlap() {
        let a = SparseVector::new(vec![1, 5, 9], vec![2.0, 3.0, 4.0]).unwrap();
        let b = SparseVector::new(vec![5, 9, 13], vec![10.0, 0.5, 1.0]).unwrap();
        // overlap on 5 (3*10=30) and 9 (4*0.5=2) => 32
        assert_eq!(sparse_dot(&a, &b), 32.0);
    }

    #[test]
    fn dot_is_commutative() {
        let a = SparseVector::new(vec![1, 4, 7], vec![1.5, -2.0, 3.0]).unwrap();
        let b = SparseVector::new(vec![4, 7, 8], vec![2.0, 1.0, 9.0]).unwrap();
        assert_eq!(sparse_dot(&a, &b), sparse_dot(&b, &a));
    }

    #[test]
    fn dot_with_empty_is_zero() {
        let a = SparseVector::new(vec![1, 2], vec![1.0, 1.0]).unwrap();
        let empty = SparseVector::new(vec![], vec![]).unwrap();
        assert_eq!(sparse_dot(&a, &empty), 0.0);
    }

    #[test]
    fn l2_norm_basic() {
        let v = SparseVector::new(vec![1, 2], vec![3.0, 4.0]).unwrap();
        assert_eq!(l2_norm(&v), 5.0);
    }

    #[test]
    fn prune_keeps_largest_magnitude() {
        let v = SparseVector::new(vec![1, 2, 3, 4], vec![0.1, -5.0, 0.2, 3.0]).unwrap();
        let pruned = prune_top_k(&v, 2);
        // largest |w|: term 2 (5.0) and term 4 (3.0); re-sorted by index
        assert_eq!(pruned.indices(), &[2, 4]);
        assert_eq!(pruned.values(), &[-5.0, 3.0]);
    }

    #[test]
    fn prune_k_ge_len_is_identity() {
        let v = SparseVector::new(vec![1, 2], vec![1.0, 2.0]).unwrap();
        assert_eq!(prune_top_k(&v, 5), v);
        assert_eq!(prune_top_k(&v, 2), v);
    }

    #[test]
    fn prune_k_zero_is_empty() {
        let v = SparseVector::new(vec![1, 2], vec![1.0, 2.0]).unwrap();
        assert!(prune_top_k(&v, 0).is_empty());
    }

    #[test]
    fn prune_preserves_dot_on_kept_terms() {
        // Pruning the query to its top terms can only drop contributions from
        // the removed terms; on a doc sharing only kept terms the score is exact.
        let q = SparseVector::new(vec![1, 2, 3], vec![10.0, 0.01, 9.0]).unwrap();
        let doc = SparseVector::new(vec![1, 3], vec![2.0, 2.0]).unwrap();
        let pruned = prune_top_k(&q, 2);
        assert_eq!(sparse_dot(&pruned, &doc), sparse_dot(&q, &doc));
    }
}
