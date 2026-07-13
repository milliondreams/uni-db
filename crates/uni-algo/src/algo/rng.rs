// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Reproducible counter-hash RNG shared across seeded graph algorithms.
//!
//! A *counter-hash* derives each pseudo-random draw purely from its coordinates
//! `(seed, iter, elem)` rather than from a mutable stream position, so the draw
//! for a given element is independent of iteration order, partition count, and
//! thread schedule (the plugin-compute proposal §8 determinism contract). This
//! is what lets a masked/sampled kernel run in parallel and still be bitwise
//! reproducible: element `e` always sees `counter_hash(seed, iter, e)` no matter
//! who computes it or when.
//!
//! The core is the [SplitMix64] finalizer (avalanche mixing). Random walks were
//! the first user of this pattern (`random_walk::walk_seed`); this module hoists
//! the finalizer so the walk seeding and the promoted `sample` primitive share
//! one hash, guaranteeing walk streams are unchanged by the promotion (test
//! S-6). The domain is *extended* here to carry an explicit `iter` counter,
//! which walks (being non-iterative) never needed.
//!
//! [SplitMix64]: https://prng.di.unimi.it/splitmix64.c
//
// Rust guideline compliant

/// Odd multiplier applied to the `iter` coordinate before finalizing.
const ITER_MULT: u64 = 0xD1B5_4A32_D192_ED03;

/// Odd multiplier applied to the `elem` coordinate before finalizing.
const ELEM_MULT: u64 = 0x9E37_79B9_7F4A_7C15;

/// Applies the SplitMix64 finalizer (avalanche step) to `x`.
///
/// A pure, invertible bit-mixing function that turns a weakly-varying counter
/// into a well-distributed 64-bit value. Shared by every seeded stream so the
/// mixing is identical across the walk seeding and the `sample` primitive.
///
/// # Examples
/// ```
/// use uni_algo::algo::rng::splitmix64_finalize;
///
/// // Distinct inputs avalanche to unrelated outputs, and it is deterministic.
/// assert_eq!(splitmix64_finalize(0), splitmix64_finalize(0));
/// assert_ne!(splitmix64_finalize(0), splitmix64_finalize(1));
/// ```
#[inline]
#[must_use]
pub fn splitmix64_finalize(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Returns a reproducible 64-bit hash for the coordinate `(seed, iter, elem)`.
///
/// The stream is *stateless*: the value depends only on its three coordinates,
/// so it is order-, partition-, and thread-independent by construction (proposal
/// §8). Mix `iter` in for a fresh, decorrelated mask per iteration; mix `elem`
/// in for a per-element draw within one iteration. Distinct `(iter, elem)`
/// coordinates do not collide because each is scaled by a distinct odd
/// multiplier before the finalizer avalanches the sum.
///
/// # Examples
/// ```
/// use uni_algo::algo::rng::counter_hash;
///
/// // Deterministic, and decorrelated across both iter and elem.
/// assert_eq!(counter_hash(7, 0, 0), counter_hash(7, 0, 0));
/// assert_ne!(counter_hash(7, 0, 0), counter_hash(7, 1, 0));
/// assert_ne!(counter_hash(7, 0, 0), counter_hash(7, 0, 1));
/// ```
#[inline]
#[must_use]
pub fn counter_hash(seed: u64, iter: u64, elem: u64) -> u64 {
    let mixed = seed
        .wrapping_add(iter.wrapping_mul(ITER_MULT))
        .wrapping_add(elem.wrapping_mul(ELEM_MULT));
    splitmix64_finalize(mixed)
}

/// Maps a 64-bit hash to a uniform `f64` in `[0, 1)`.
///
/// Uses the high 53 bits so every representable double in the unit interval is
/// reachable with equal spacing (`2⁻⁵³` granularity). Comparing this against a
/// probability `p` yields a `Bernoulli(p)` draw.
///
/// # Examples
/// ```
/// use uni_algo::algo::rng::{counter_hash, hash_to_unit_f64};
///
/// let u = hash_to_unit_f64(counter_hash(1, 0, 0));
/// assert!((0.0..1.0).contains(&u));
/// ```
#[inline]
#[must_use]
pub fn hash_to_unit_f64(hash: u64) -> f64 {
    // 53-bit mantissa: take the top 53 bits and scale by 2^-53.
    const SCALE: f64 = 1.0 / ((1u64 << 53) as f64);
    #[expect(
        clippy::cast_precision_loss,
        reason = "value is a 53-bit integer, exactly representable in f64"
    )]
    let numer = (hash >> 11) as f64;
    numer * SCALE
}

/// Returns a `Bernoulli(prob)` draw for the coordinate `(seed, iter, elem)`.
///
/// `true` with probability `prob`, drawn from the reproducible counter-hash
/// stream. A `prob <= 0.0` never fires and a `prob >= 1.0` always fires, so a
/// degenerate probability is exact rather than off-by-one. This is the core of
/// the `sample` kernel: draw membership for element `elem` at iteration `iter`.
///
/// # Examples
/// ```
/// use uni_algo::algo::rng::sample_bernoulli;
///
/// assert!(!sample_bernoulli(0.0, 1, 0, 0), "p=0 never fires");
/// assert!(sample_bernoulli(1.0, 1, 0, 0), "p=1 always fires");
/// ```
#[inline]
#[must_use]
pub fn sample_bernoulli(prob: f64, seed: u64, iter: u64, elem: u64) -> bool {
    if prob <= 0.0 {
        return false;
    }
    if prob >= 1.0 {
        return true;
    }
    hash_to_unit_f64(counter_hash(seed, iter, elem)) < prob
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_hash_is_deterministic_and_order_independent() {
        // The stream depends only on coordinates: computing element draws in any
        // order yields the same set of hashes (S-1/S-2 substrate).
        let seed = 0xABCD_1234;
        let forward: Vec<u64> = (0..1000).map(|e| counter_hash(seed, 3, e)).collect();
        let reverse: Vec<u64> = (0..1000).rev().map(|e| counter_hash(seed, 3, e)).collect();
        let mut reverse_reordered = reverse;
        reverse_reordered.reverse();
        assert_eq!(forward, reverse_reordered);
    }

    #[test]
    fn distinct_iters_decorrelate() {
        // S-3 substrate: masks at different iterations share few members.
        let seed = 99;
        let a: Vec<bool> = (0..2000)
            .map(|e| sample_bernoulli(0.5, seed, 0, e))
            .collect();
        let b: Vec<bool> = (0..2000)
            .map(|e| sample_bernoulli(0.5, seed, 1, e))
            .collect();
        let agree = a.iter().zip(&b).filter(|(x, y)| x == y).count();
        // Independent fair coins agree ~50% of the time; assert it is far from
        // the degenerate 100% (identical) or 0% (perfectly anti-correlated).
        let frac = agree as f64 / a.len() as f64;
        assert!((0.4..0.6).contains(&frac), "iters not decorrelated: {frac}");
    }

    #[test]
    fn bernoulli_marginal_is_close_to_p() {
        // S-4 substrate: the empirical rate matches p over many elements.
        for &p in &[0.1, 0.5, 0.9] {
            let n = 20_000u64;
            let fired = (0..n).filter(|&e| sample_bernoulli(p, 7, 0, e)).count();
            let rate = fired as f64 / n as f64;
            assert!((rate - p).abs() < 0.02, "p={p}: empirical {rate}");
        }
    }

    #[test]
    fn degenerate_probabilities_are_exact() {
        for e in 0..100 {
            assert!(!sample_bernoulli(0.0, 1, 0, e));
            assert!(!sample_bernoulli(-1.0, 1, 0, e));
            assert!(sample_bernoulli(1.0, 1, 0, e));
            assert!(sample_bernoulli(2.0, 1, 0, e));
        }
    }
}
