// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Determinism-owning float reduction (the plugin-compute proposal §6/§8
//! accumulator contract).
//!
//! DataFusion's partitioned float `SUM` is **not bit-reproducible**: the
//! per-partition partial sums combine in a nondeterministic order, so the same
//! data reduced under `target_partitions = 1` vs `8` can differ in the low bits
//! (DF #9554/#9680/#9804 and the float-associativity literature). A reproducible
//! study number (grid-reliability §7, any Monte-Carlo estimate) cannot come from
//! that stock reduction.
//!
//! Compensated summation alone (Kahan/Neumaier) *reduces* rounding error but is
//! still **order-dependent** — the running compensation depends on the order the
//! addends arrive, so two permutations of the same multiset can still differ in
//! the last bit. The only reductions that are bitwise-identical across arbitrary
//! permutations and partition counts are the ones that **canonicalize order**
//! before summing. [`deterministic_sum`] does exactly that: it sorts the addends
//! into a canonical order (`f64::total_cmp`, so `NaN`/`-0.0` are ordered too)
//! and then runs a Neumaier-compensated sum. The result is a pure function of
//! the input *multiset*, independent of how it was chunked or ordered.
//
// Rust guideline compliant

/// Returns a bitwise-reproducible sum of `values`, independent of their order.
///
/// Canonicalizes the addend order via [`f64::total_cmp`] and then accumulates
/// with Neumaier compensation, so any permutation or partitioning of the same
/// multiset yields byte-identical bits (proposal §6/§8, test DF-6). This is the
/// determinism-owning accumulator a reducing stage must use in place of stock
/// partitioned `SUM` when the output feeds a reproducible number.
///
/// The cost is `O(n log n)` for the canonicalizing sort — acceptable for a
/// reducing stage whose determinism is the point; a hot inner loop that does not
/// need cross-partition reproducibility should keep using a plain fold.
///
/// # Examples
/// ```
/// use uni_algo::algo::reduce::deterministic_sum;
///
/// let forward = [1e16, 1.0, -1e16, 2.0];
/// let mut shuffled = forward;
/// shuffled.reverse();
/// // Order-independent to the bit — and the small addends are not lost.
/// assert_eq!(
///     deterministic_sum(&forward).to_bits(),
///     deterministic_sum(&shuffled).to_bits()
/// );
/// assert_eq!(deterministic_sum(&forward), 3.0);
/// ```
#[must_use]
pub fn deterministic_sum(values: &[f64]) -> f64 {
    let mut sorted: Vec<f64> = values.to_vec();
    // Canonical total order over all f64 bit patterns (handles NaN and signed
    // zero), so the addend sequence depends only on the input multiset.
    sorted.sort_unstable_by(|a, b| a.total_cmp(b));
    neumaier_sum(&sorted)
}

/// Neumaier-compensated (improved Kahan) sum over `values` in the given order.
///
/// Tracks a running compensation term that recovers the low-order bits lost when
/// a large accumulator absorbs a small addend (and vice-versa). *Order-dependent*
/// on its own — [`deterministic_sum`] canonicalizes the order first to make the
/// whole reduction order-independent.
///
/// # Examples
/// ```
/// use uni_algo::algo::reduce::neumaier_sum;
///
/// // The naive left fold loses the `1.0`; Neumaier keeps it.
/// assert_eq!(neumaier_sum(&[1e16, 1.0, -1e16]), 1.0);
/// ```
#[must_use]
pub fn neumaier_sum(values: &[f64]) -> f64 {
    let mut sum = 0.0f64;
    let mut c = 0.0f64; // running compensation
    for &v in values {
        let t = sum + v;
        if sum.abs() >= v.abs() {
            c += (sum - t) + v;
        } else {
            c += (v - t) + sum;
        }
        sum = t;
    }
    sum + c
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::rng::counter_hash;

    /// A deterministic pseudo-random f64 in a wide dynamic range from `i`.
    fn pseudo(i: u64) -> f64 {
        // Mantissa from the hash, exponent spread so magnitudes vary widely —
        // the regime where naive summation is most order-sensitive.
        let h = counter_hash(0xD1FF, 0, i);
        let mantissa = (h >> 12) as f64 / (1u64 << 52) as f64; // [0,1)
        let sign = if h & 1 == 0 { 1.0 } else { -1.0 };
        let exp = ((h >> 1) % 40) as i32 - 20; // 2^-20 .. 2^19
        sign * (1.0 + mantissa) * 2f64.powi(exp)
    }

    #[test]
    fn deterministic_sum_is_bitwise_permutation_invariant() {
        // DF-6 core: the sum's bits depend only on the multiset, not the order —
        // so target_partitions ∈ {1, 8} and any batch permutation agree.
        let base: Vec<f64> = (0..5000).map(pseudo).collect();
        let canonical = deterministic_sum(&base).to_bits();

        // Several independent shuffles (deterministic Fisher-Yates via the hash).
        for round in 1..8u64 {
            let mut v = base.clone();
            for i in (1..v.len()).rev() {
                let j = (counter_hash(0x5EED, round, i as u64) as usize) % (i + 1);
                v.swap(i, j);
            }
            assert_eq!(
                deterministic_sum(&v).to_bits(),
                canonical,
                "round {round}: permuted sum differed in bits"
            );
        }
    }

    #[test]
    fn deterministic_sum_is_invariant_to_partition_order_when_gathered() {
        // The determinism-contract route for a partitioned reduce (proposal §6):
        // gather every partition's values into the single reducing stage and take
        // ONE flat `deterministic_sum`. However the partitions are split and
        // however they arrive, the gathered flat sum is byte-identical — because
        // the canonicalizing sort erases both the split and the arrival order.
        //
        // (A two-level "partial per partition, then combine" is deliberately NOT
        // asserted equal: compensated summation is not associative, so two-level
        // ≠ flat in the low bit. That is precisely why the determinism contract
        // routes the reduce through a single gathering stage rather than a
        // partitioned partial-combine.)
        let base: Vec<f64> = (0..4096).map(|i| pseudo(i + 100)).collect();
        let whole = deterministic_sum(&base).to_bits();
        for p in [1usize, 2, 3, 8, 16] {
            let chunk = base.len().div_ceil(p).max(1);
            // Reorder the partitions (chunks) but gather all their values flat.
            let mut chunks: Vec<&[f64]> = base.chunks(chunk).collect();
            chunks.reverse(); // a different inter-partition arrival order
            let gathered: Vec<f64> = chunks.into_iter().flatten().copied().collect();
            assert_eq!(
                deterministic_sum(&gathered).to_bits(),
                whole,
                "partition count {p}: gathered flat sum differed from the whole"
            );
        }
    }

    #[test]
    fn neumaier_recovers_lost_low_bits() {
        // The compensation is real: a naive fold drops the small addends.
        let naive: f64 = [1e16, 1.0, 1.0, 1.0, -1e16].iter().sum();
        let compensated = neumaier_sum(&[1e16, 1.0, 1.0, 1.0, -1e16]);
        assert_eq!(compensated, 3.0);
        assert_ne!(naive, 3.0, "the naive fold is expected to lose the ones");
    }

    #[test]
    fn deterministic_sum_matches_exact_on_small_integers() {
        // Where the exact answer is representable, the reduction is exact.
        let vals: Vec<f64> = (1..=1000).map(|i| i as f64).collect();
        assert_eq!(deterministic_sum(&vals), 500_500.0);
    }
}
