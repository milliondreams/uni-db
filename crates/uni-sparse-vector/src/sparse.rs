use crate::error::SparseError;
use serde::{Deserialize, Serialize};

/// A learned-sparse vector: `{term_id -> weight}` over a high-cardinality
/// vocabulary (e.g. SPLADE-v3 / BGE-M3 sparse head over a ~30k-term BERT vocab).
///
/// Stored as two parallel arrays — `indices` (term ids) and `values` (weights) —
/// with `indices` kept **strictly ascending** so that the canonical scoring
/// kernel ([`crate::ops::sparse_dot`]) is a linear merge-join. The invariant is
/// enforced at construction by [`SparseVector::new`].
///
/// The in-memory and binary forms keep weights as lossless `f32`. Weight
/// quantization (8-bit, etc.) is a storage-engine concern applied at the index
/// postings boundary, never in this type, so a brute-force scorer over
/// `SparseVector` is always an exact ground truth.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SparseVector {
    indices: Vec<u32>,
    values: Vec<f32>,
}

impl SparseVector {
    /// Construct a sparse vector, validating its invariants:
    /// - `indices.len() == values.len()` (SV-1),
    /// - `indices` strictly ascending — sorted and unique (SV-2),
    /// - every weight finite — no NaN / ±inf (SV-3).
    ///
    /// Use [`SparseVector::from_pairs`] for unsorted input with duplicate
    /// term ids (the typical embedding-producer shape).
    pub fn new(indices: Vec<u32>, values: Vec<f32>) -> Result<Self, SparseError> {
        if indices.len() != values.len() {
            return Err(SparseError::LengthMismatch {
                indices: indices.len(),
                values: values.len(),
            });
        }
        for i in 1..indices.len() {
            if indices[i] <= indices[i - 1] {
                return Err(SparseError::UnsortedIndices {
                    position: i,
                    prev: indices[i - 1],
                    curr: indices[i],
                });
            }
        }
        for (position, &value) in values.iter().enumerate() {
            if !value.is_finite() {
                return Err(SparseError::NonFiniteWeight { position, value });
            }
        }
        Ok(Self { indices, values })
    }

    /// Build from arbitrary `(term_id, weight)` pairs: sorts by term id and
    /// sums the weights of duplicate term ids, then validates. This is the
    /// ingestion-friendly constructor for embedding-model output and for
    /// `dict[int, float]` from the Python surface.
    ///
    /// Non-finite input weights are still rejected (after summation).
    pub fn from_pairs(mut pairs: Vec<(u32, f32)>) -> Result<Self, SparseError> {
        pairs.sort_by_key(|&(term, _)| term);
        let mut indices = Vec::with_capacity(pairs.len());
        let mut values = Vec::with_capacity(pairs.len());
        for (term, weight) in pairs {
            if indices.last() == Some(&term) {
                *values
                    .last_mut()
                    .expect("indices non-empty implies values non-empty") += weight;
            } else {
                indices.push(term);
                values.push(weight);
            }
        }
        Self::new(indices, values)
    }

    /// The (strictly ascending) term ids.
    #[inline]
    pub fn indices(&self) -> &[u32] {
        &self.indices
    }

    /// The weights, parallel to [`SparseVector::indices`].
    #[inline]
    pub fn values(&self) -> &[f32] {
        &self.values
    }

    /// Number of non-zero terms.
    #[inline]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Whether the vector has no non-zero terms.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Iterate over `(term_id, weight)` pairs in ascending term-id order.
    pub fn iter(&self) -> impl Iterator<Item = (u32, f32)> + '_ {
        self.indices
            .iter()
            .copied()
            .zip(self.values.iter().copied())
    }

    /// Consume the vector into its parallel arrays.
    pub fn into_parts(self) -> (Vec<u32>, Vec<f32>) {
        (self.indices, self.values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_accepts_sorted_finite() {
        let v = SparseVector::new(vec![1, 5, 9], vec![0.5, -1.0, 2.0]).unwrap();
        assert_eq!(v.len(), 3);
        assert!(!v.is_empty());
    }

    #[test]
    fn new_accepts_empty() {
        let v = SparseVector::new(vec![], vec![]).unwrap();
        assert!(v.is_empty());
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn new_rejects_length_mismatch() {
        let err = SparseVector::new(vec![1, 2], vec![0.5]).unwrap_err();
        assert!(matches!(err, SparseError::LengthMismatch { .. }));
    }

    #[test]
    fn new_rejects_unsorted() {
        let err = SparseVector::new(vec![5, 1], vec![0.5, 0.5]).unwrap_err();
        assert!(matches!(err, SparseError::UnsortedIndices { .. }));
    }

    #[test]
    fn new_rejects_duplicate_indices() {
        let err = SparseVector::new(vec![3, 3], vec![0.5, 0.5]).unwrap_err();
        assert!(matches!(err, SparseError::UnsortedIndices { .. }));
    }

    #[test]
    fn new_rejects_nan_and_inf() {
        assert!(matches!(
            SparseVector::new(vec![1], vec![f32::NAN]).unwrap_err(),
            SparseError::NonFiniteWeight { .. }
        ));
        assert!(matches!(
            SparseVector::new(vec![1], vec![f32::INFINITY]).unwrap_err(),
            SparseError::NonFiniteWeight { .. }
        ));
    }

    #[test]
    fn from_pairs_sorts_and_sums_duplicates() {
        let v = SparseVector::from_pairs(vec![(9, 1.0), (1, 2.0), (9, 0.5), (1, -0.5)]).unwrap();
        assert_eq!(v.indices(), &[1, 9]);
        assert_eq!(v.values(), &[1.5, 1.5]);
    }

    #[test]
    fn from_pairs_empty() {
        let v = SparseVector::from_pairs(vec![]).unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn iter_yields_pairs_in_order() {
        let v = SparseVector::new(vec![2, 4], vec![1.0, 2.0]).unwrap();
        let pairs: Vec<_> = v.iter().collect();
        assert_eq!(pairs, vec![(2, 1.0), (4, 2.0)]);
    }
}
