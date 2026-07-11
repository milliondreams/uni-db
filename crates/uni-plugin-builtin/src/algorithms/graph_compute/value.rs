// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Value types carried behind GraphCompute handles: sets, tensors, scalars.
//!
//! Guest kernels see only opaque handles; behind each handle lives one of these
//! host-owned values. A [`VertexSet`] is a frontier or visited mask over vertex
//! slots; a [`Tensor`] is a shaped, Arrow-backed per-vertex value map. Both are
//! deterministic-by-construction — set iteration is ascending-slot order and
//! tensor reductions run in fixed slot order (proposal §5.3).
//!
//! # Phase 1 dtype note
//! The [`DType`] taxonomy is reserved in full for forward-compatibility
//! (embeddings, weight matrices — proposal §4.2), but v1 *compute* runs a single
//! `f64` path: a [`Tensor`] is always backed by an Arrow [`Float64Array`]. Slot
//! ids fit exactly in the f64 mantissa below 2⁵³, so label/parent/score maps are
//! all representable. Native i64/u32 compute paths (path-counting) are a
//! documented follow-up; the [`DType`] tag is retained on the surface so they
//! land additively.
//
// Rust guideline compliant

use arrow_array::{Array, Float64Array, Int64Array};

/// A logical element type for a [`Tensor`].
///
/// v1 computes in `f64` regardless of tag (see module docs); the tag is retained
/// so the integer/boolean compute paths land additively later (proposal §4.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DType {
    /// 32-bit float (reserved for embeddings).
    F32,
    /// 64-bit float — the v1 compute type.
    F64,
    /// 64-bit signed integer (reserved for path counting).
    I64,
    /// 32-bit unsigned integer (reserved for slot ids).
    U32,
    /// Boolean (reserved for reachability masks stored as tensors).
    Bool,
}

/// The shape of a tensor handle.
///
/// v1 uses only [`Shape::V`] (the per-vertex scalar map); `Vd`/`D`/`Dd` are
/// reserved for embeddings and free weight matrices (proposal §4.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// A `[V]` per-vertex scalar map — the only v1 shape.
    V,
    /// A `[V, d]` per-vertex embedding matrix (reserved).
    Vd(u32),
    /// A `[d]` free vector (reserved).
    D(u32),
    /// A `[d, d']` free weight matrix (reserved).
    Dd(u32, u32),
}

/// A scalar value crossing the kernel boundary (a reduction result or an input).
///
/// A tagged union over [`DType`]; v1 kernels produce and consume `F64` scalars.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Scalar {
    /// A 64-bit float scalar.
    F64(f64),
    /// A 64-bit signed integer scalar.
    I64(i64),
    /// A boolean scalar.
    Bool(bool),
}

impl Scalar {
    /// Coerces the scalar to `f64` for the v1 single-path compute model.
    #[must_use]
    pub fn as_f64(self) -> f64 {
        match self {
            Scalar::F64(x) => x,
            #[expect(
                clippy::cast_precision_loss,
                reason = "v1 compute is f64; i64 scalars are small in practice"
            )]
            Scalar::I64(x) => x as f64,
            Scalar::Bool(b) => {
                if b {
                    1.0
                } else {
                    0.0
                }
            }
        }
    }

    /// Coerces the scalar to `i64` for the exact integer compute path.
    ///
    /// A whole-valued `f64` round-trips exactly; a fractional `f64` truncates
    /// toward zero (a caller scattering an integer into an `i64` map passes a
    /// whole value).
    #[must_use]
    pub fn as_i64(self) -> i64 {
        match self {
            Scalar::I64(x) => x,
            #[expect(
                clippy::cast_possible_truncation,
                reason = "i64 maps are seeded with whole values"
            )]
            Scalar::F64(x) => x as i64,
            Scalar::Bool(b) => i64::from(b),
        }
    }
}

/// A set of vertex slots, stored as a fixed-capacity bitset.
///
/// Backs frontier and visited-mask handles. Set operations are word-wise and
/// iteration yields set slots in ascending order, so any algorithm built on it
/// is deterministic (proposal §5.3). Capacity is the projection's vertex count;
/// out-of-range slots are a programming error.
///
/// # Examples
/// ```
/// use uni_plugin_builtin::algorithms::graph_compute::value::VertexSet;
///
/// let mut s = VertexSet::with_capacity(10);
/// s.insert(3);
/// s.insert(7);
/// assert_eq!(s.len(), 2);
/// assert_eq!(s.iter().collect::<Vec<_>>(), vec![3, 7]);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VertexSet {
    words: Vec<u64>,
    capacity: usize,
}

impl VertexSet {
    /// Creates an empty set able to hold slots `0..capacity`.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            words: vec![0; capacity.div_ceil(64)],
            capacity,
        }
    }

    /// Returns the slot capacity (the projection vertex count).
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Inserts `slot` into the set.
    ///
    /// # Panics
    /// Panics if `slot >= capacity` — a host-side programming error, since guest
    /// slots are always derived from the projection's own vertex count.
    pub fn insert(&mut self, slot: u32) {
        let s = slot as usize;
        assert!(s < self.capacity, "vertex slot out of range");
        self.words[s / 64] |= 1u64 << (s % 64);
    }

    /// Returns `true` if `slot` is in the set (and in range).
    #[must_use]
    pub fn contains(&self, slot: u32) -> bool {
        let s = slot as usize;
        s < self.capacity && (self.words[s / 64] >> (s % 64)) & 1 == 1
    }

    /// Returns the number of slots in the set.
    #[must_use]
    pub fn len(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Returns `true` if the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|&w| w == 0)
    }

    /// Iterates set slots in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.words.iter().enumerate().flat_map(|(wi, &word)| {
            (0..64).filter_map(move |b| {
                if (word >> b) & 1 == 1 {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "slot index is bounded by capacity which fits u32"
                    )]
                    Some((wi * 64 + b) as u32)
                } else {
                    None
                }
            })
        })
    }

    /// Returns the union of `self` and `other` (capacities must match).
    #[must_use]
    pub fn union(&self, other: &VertexSet) -> VertexSet {
        self.zip_with(other, |a, b| a | b)
    }

    /// Returns the intersection of `self` and `other`.
    #[must_use]
    pub fn intersect(&self, other: &VertexSet) -> VertexSet {
        self.zip_with(other, |a, b| a & b)
    }

    /// Returns the set difference `self \ other`.
    #[must_use]
    pub fn difference(&self, other: &VertexSet) -> VertexSet {
        self.zip_with(other, |a, b| a & !b)
    }

    /// Applies `op` word-wise against `other`, producing a new set.
    ///
    /// # Panics
    /// Panics on a capacity mismatch — sets from the same session always share
    /// the projection vertex count, so a mismatch is a host programming error.
    fn zip_with(&self, other: &VertexSet, op: impl Fn(u64, u64) -> u64) -> VertexSet {
        assert_eq!(
            self.capacity, other.capacity,
            "vertex-set capacity mismatch"
        );
        VertexSet {
            words: self
                .words
                .iter()
                .zip(other.words.iter())
                .map(|(&a, &b)| op(a, b))
                .collect(),
            capacity: self.capacity,
        }
    }

    /// Returns the number of bytes this set holds live, for arena accounting.
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        self.words.len() * std::mem::size_of::<u64>()
    }
}

/// The physical Arrow buffer behind a [`Tensor`].
///
/// The `f64` path is the v1 default; the `i64` path backs exact integer
/// path-counting (proposal §4.2 / F-9), where accumulated counts can exceed the
/// `2⁵³` exact-integer range of `f64`.
#[derive(Clone, Debug)]
enum TensorBuf {
    F64(Float64Array),
    I64(Int64Array),
}

/// A shaped, Arrow-backed per-vertex value map.
///
/// v1 is [`Shape::V`] backed by a [`Float64Array`] by default; the exact
/// integer path-counting kernels (F-9) back it with an [`Int64Array`] instead.
/// The Arrow backing satisfies the invariant that a `[V]` map *is* a DataFusion
/// column (proposal §4.1 / D6), so a future columnar bridge is a zero-copy view.
///
/// # Examples
/// ```
/// use uni_plugin_builtin::algorithms::graph_compute::value::{DType, Tensor};
///
/// let t = Tensor::from_f64(vec![1.0, 2.0, 3.0]);
/// assert_eq!(t.len(), 3);
/// assert_eq!(t.dtype(), DType::F64);
/// assert_eq!(t.values(), &[1.0, 2.0, 3.0]);
/// ```
#[derive(Clone, Debug)]
pub struct Tensor {
    shape: Shape,
    dtype: DType,
    buf: TensorBuf,
}

impl Tensor {
    /// Builds a `[V]` `f64` tensor from a value vector.
    #[must_use]
    pub fn from_f64(values: Vec<f64>) -> Self {
        Self {
            shape: Shape::V,
            dtype: DType::F64,
            buf: TensorBuf::F64(Float64Array::from(values)),
        }
    }

    /// Builds a `[V]` tensor from values, tagging it with a logical `dtype`.
    ///
    /// The buffer is still `f64`; `dtype` records the caller's logical intent
    /// (e.g. `U32` slot ids from `vertex_ids`). Integer *compute* uses
    /// [`from_i64`](Self::from_i64), which backs the buffer with an `Int64Array`.
    #[must_use]
    pub fn from_f64_typed(values: Vec<f64>, dtype: DType) -> Self {
        Self {
            shape: Shape::V,
            dtype,
            buf: TensorBuf::F64(Float64Array::from(values)),
        }
    }

    /// Builds a `[V]` exact-integer (`i64`) tensor from a value vector.
    ///
    /// Backs the buffer with an `Int64Array` so path counts beyond `2⁵³` stay
    /// exact (proposal §4.2 / F-9); `f64`-only kernels reject it with `0x862`.
    #[must_use]
    pub fn from_i64(values: Vec<i64>) -> Self {
        Self {
            shape: Shape::V,
            dtype: DType::I64,
            buf: TensorBuf::I64(Int64Array::from(values)),
        }
    }

    /// Returns the tensor shape (always [`Shape::V`] in v1).
    #[must_use]
    pub fn shape(&self) -> Shape {
        self.shape
    }

    /// Returns the logical element type.
    #[must_use]
    pub fn dtype(&self) -> DType {
        self.dtype
    }

    /// Returns `true` if the tensor is backed by an exact `i64` buffer.
    #[must_use]
    pub fn is_i64(&self) -> bool {
        matches!(self.buf, TensorBuf::I64(_))
    }

    /// Returns the element count (`V`).
    #[must_use]
    pub fn len(&self) -> usize {
        match &self.buf {
            TensorBuf::F64(b) => b.len(),
            TensorBuf::I64(b) => b.len(),
        }
    }

    /// Returns `true` if the tensor has no elements.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the values as a zero-copy `f64` slice.
    ///
    /// # Panics
    /// Panics if the tensor is `i64`-backed (callers must dtype-dispatch first)
    /// or the backing array contains nulls, which the constructors never produce.
    #[must_use]
    pub fn values(&self) -> &[f64] {
        match &self.buf {
            TensorBuf::F64(b) => {
                assert_eq!(b.null_count(), 0, "Tensor buffer must be non-null");
                b.values()
            }
            TensorBuf::I64(_) => panic!("values(): tensor is i64-backed; use values_i64()"),
        }
    }

    /// Returns the values as a zero-copy `i64` slice, or `None` if `f64`-backed.
    #[must_use]
    pub fn values_i64(&self) -> Option<&[i64]> {
        match &self.buf {
            TensorBuf::I64(b) => {
                assert_eq!(b.null_count(), 0, "Tensor buffer must be non-null");
                Some(b.values())
            }
            TensorBuf::F64(_) => None,
        }
    }

    /// Returns a copy of the values as `f64`, regardless of the backing type.
    ///
    /// The `f64`-backed case is a plain copy; an `i64` buffer is widened (exact
    /// for counts within `±2⁵³`). Used by `emit`, whose result sink is `f64`.
    #[must_use]
    pub fn as_f64_vec(&self) -> Vec<f64> {
        match &self.buf {
            TensorBuf::F64(b) => b.values().to_vec(),
            #[expect(
                clippy::cast_precision_loss,
                reason = "emit sink is f64; exact for counts within 2^53"
            )]
            TensorBuf::I64(b) => b.values().iter().map(|&v| v as f64).collect(),
        }
    }

    /// Returns the underlying `f64` Arrow array (the columnar-bridge anchor).
    ///
    /// # Panics
    /// Panics if the tensor is `i64`-backed.
    #[must_use]
    pub fn arrow(&self) -> &Float64Array {
        match &self.buf {
            TensorBuf::F64(b) => b,
            TensorBuf::I64(_) => panic!("arrow(): tensor is i64-backed"),
        }
    }

    /// Returns the number of bytes this tensor holds live, for arena accounting.
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        // Both element types are 8 bytes wide.
        self.len() * std::mem::size_of::<i64>()
    }
}

/// A batch of sampled random walks, behind a [`HandleKind::Walks`] handle.
///
/// Each inner vector is one walk as a sequence of vertex *slots* (dense u32);
/// walks may be shorter than the requested length when a walk hits a dead end.
/// Produced by `random_walks` and consumed by reductions like
/// `walk_visit_counts` (proposal §8, F-8). Slots (not external `Vid`s) are stored
/// so a reduction can index per-vertex arrays directly.
#[derive(Clone, Debug)]
pub struct WalkMatrix {
    walks: Vec<Vec<u32>>,
}

impl WalkMatrix {
    /// Wraps a batch of slot-sequence walks.
    #[must_use]
    pub fn new(walks: Vec<Vec<u32>>) -> Self {
        Self { walks }
    }

    /// Returns the walks as slot sequences.
    #[must_use]
    pub fn walks(&self) -> &[Vec<u32>] {
        &self.walks
    }

    /// Returns the number of walks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.walks.len()
    }

    /// Returns `true` if there are no walks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.walks.is_empty()
    }

    /// Returns the number of bytes held live, for arena accounting.
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        self.walks
            .iter()
            .map(|w| w.len() * std::mem::size_of::<u32>())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vertex_set_ops_and_iteration() {
        let mut a = VertexSet::with_capacity(200);
        for s in [3, 7, 130, 199] {
            a.insert(s);
        }
        assert_eq!(a.len(), 4);
        assert_eq!(a.iter().collect::<Vec<_>>(), vec![3, 7, 130, 199]);

        let mut b = VertexSet::with_capacity(200);
        for s in [7, 8, 199] {
            b.insert(s);
        }
        assert_eq!(a.union(&b).len(), 5);
        assert_eq!(a.intersect(&b).iter().collect::<Vec<_>>(), vec![7, 199]);
        assert_eq!(a.difference(&b).iter().collect::<Vec<_>>(), vec![3, 130]);
    }

    #[test]
    fn vertex_set_empty_and_contains() {
        let mut s = VertexSet::with_capacity(64);
        assert!(s.is_empty());
        s.insert(0);
        s.insert(63);
        assert!(!s.is_empty());
        assert!(s.contains(0));
        assert!(s.contains(63));
        assert!(!s.contains(1));
        assert!(!s.contains(100));
    }

    #[test]
    fn tensor_arrow_backing_is_zero_copy_slice() {
        let t = Tensor::from_f64(vec![10.0, 20.0, 30.0]);
        assert_eq!(t.values(), &[10.0, 20.0, 30.0]);
        // The columnar-bridge invariant: the buffer is a real Arrow array.
        assert_eq!(t.arrow().len(), 3);
        assert_eq!(t.dtype(), DType::F64);
    }

    #[test]
    fn scalar_coercion() {
        assert!((Scalar::F64(2.5).as_f64() - 2.5).abs() < f64::EPSILON);
        assert!((Scalar::I64(4).as_f64() - 4.0).abs() < f64::EPSILON);
        assert!((Scalar::Bool(true).as_f64() - 1.0).abs() < f64::EPSILON);
    }
}
