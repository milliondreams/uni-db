use std::cmp::Ordering;

use crate::btic::{Btic, NEG_INF, POS_INF};
use crate::certainty::Certainty;
use crate::granularity::Granularity;

/// Compute the intersection of two intervals.
///
/// Returns `[max(a.lo, b.lo), min(a.hi, b.hi))`, or `None` if the intervals
/// are disjoint (the result would be empty).
///
/// Granularity and certainty are inherited per spec §14.3:
/// - Each result bound inherits metadata from whichever input provided that bound.
/// - When both inputs have equal bound values, the finer granularity and least
///   certain certainty are used.
pub fn intersection(a: &Btic, b: &Btic) -> Option<Btic> {
    let lo = a.lo().max(b.lo());
    let hi = a.hi().min(b.hi());

    // Disjoint: no intersection
    if lo >= hi {
        return None;
    }

    let (lo_gran, lo_cert) = pick_bound_meta(a, b, BoundSide::Lo, Ordering::Greater);
    let (hi_gran, hi_cert) = pick_bound_meta(a, b, BoundSide::Hi, Ordering::Less);

    let meta = build_result_meta(lo, hi, lo_gran, hi_gran, lo_cert, hi_cert);
    Btic::new(lo, hi, meta).ok()
}

/// Compute the span (bounding interval) of two intervals.
///
/// Returns `[min(a.lo, b.lo), max(a.hi, b.hi))`. Always valid.
///
/// Granularity and certainty are inherited per spec §14.3.
pub fn span(a: &Btic, b: &Btic) -> Btic {
    let lo = a.lo().min(b.lo());
    let hi = a.hi().max(b.hi());

    let (lo_gran, lo_cert) = pick_bound_meta(a, b, BoundSide::Lo, Ordering::Less);
    let (hi_gran, hi_cert) = pick_bound_meta(a, b, BoundSide::Hi, Ordering::Greater);

    let meta = build_result_meta(lo, hi, lo_gran, hi_gran, lo_cert, hi_cert);
    Btic::new(lo, hi, meta).expect("span of two valid intervals must be valid")
}

/// Compute the gap between two disjoint intervals.
///
/// Returns `[min(a.hi, b.hi), max(a.lo, b.lo))` if the intervals are disjoint
/// and non-adjacent. Returns `None` if they overlap or are adjacent.
///
/// Granularity/certainty: The gap's lo bound comes from whichever interval's `hi`
/// is smaller; the gap's hi bound comes from whichever interval's `lo` is larger.
pub fn gap(a: &Btic, b: &Btic) -> Option<Btic> {
    let gap_lo = a.hi().min(b.hi());
    let gap_hi = a.lo().max(b.lo());

    // If intervals overlap or are adjacent, there is no gap
    if gap_lo >= gap_hi {
        return None;
    }

    // Gap's lo comes from min(a.hi, b.hi) — the hi side of whichever interval ends first
    let (lo_gran, lo_cert) = pick_bound_meta(a, b, BoundSide::Hi, Ordering::Less);
    // Gap's hi comes from max(a.lo, b.lo) — the lo side of whichever interval starts later
    let (hi_gran, hi_cert) = pick_bound_meta(a, b, BoundSide::Lo, Ordering::Greater);

    let meta = build_result_meta(gap_lo, gap_hi, lo_gran, hi_gran, lo_cert, hi_cert);
    Btic::new(gap_lo, gap_hi, meta).ok()
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum BoundSide {
    Lo,
    Hi,
}

/// Extract (granularity, certainty) for a given bound side from a Btic.
fn bound_meta(btic: &Btic, side: BoundSide) -> (Granularity, Certainty) {
    match side {
        BoundSide::Lo => (btic.lo_granularity(), btic.lo_certainty()),
        BoundSide::Hi => (btic.hi_granularity(), btic.hi_certainty()),
    }
}

/// Extract the raw bound value for a given side.
fn bound_val(btic: &Btic, side: BoundSide) -> i64 {
    match side {
        BoundSide::Lo => btic.lo(),
        BoundSide::Hi => btic.hi(),
    }
}

/// Pick metadata for the result bound that comes from the extremal value.
///
/// When `pick` is `Greater`, selects metadata from `max(a.bound, b.bound)`.
/// When `pick` is `Less`, selects metadata from `min(a.bound, b.bound)`.
/// When both bounds are equal, uses the finer granularity and least certainty.
fn pick_bound_meta(
    a: &Btic,
    b: &Btic,
    side: BoundSide,
    pick: std::cmp::Ordering,
) -> (Granularity, Certainty) {
    let va = bound_val(a, side);
    let vb = bound_val(b, side);
    let (ga, ca) = bound_meta(a, side);
    let (gb, cb) = bound_meta(b, side);

    match va.cmp(&vb) {
        ord if ord == pick => (ga, ca),
        std::cmp::Ordering::Equal => (ga.finer(gb), ca.least_certain(cb)),
        _ => (gb, cb),
    }
}

/// Build a meta word for a result, respecting INV-6 (sentinel bounds must have zeroed meta).
fn build_result_meta(
    lo: i64,
    hi: i64,
    lo_gran: Granularity,
    hi_gran: Granularity,
    lo_cert: Certainty,
    hi_cert: Certainty,
) -> u64 {
    let (lg, lc) = if lo == NEG_INF {
        (Granularity::Millisecond, Certainty::Definite)
    } else {
        (lo_gran, lo_cert)
    };
    let (hg, hc) = if hi == POS_INF {
        (Granularity::Millisecond, Certainty::Definite)
    } else {
        (hi_gran, hi_cert)
    };
    Btic::build_meta(lg, hg, lc, hc)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make(lo: i64, hi: i64) -> Btic {
        let meta = Btic::build_meta(
            Granularity::Millisecond,
            Granularity::Millisecond,
            Certainty::Definite,
            Certainty::Definite,
        );
        Btic::new(lo, hi, meta).unwrap()
    }

    fn make_with_gran(lo: i64, hi: i64, lg: Granularity, hg: Granularity) -> Btic {
        let meta = Btic::build_meta(lg, hg, Certainty::Definite, Certainty::Definite);
        Btic::new(lo, hi, meta).unwrap()
    }

    fn make_with_cert(lo: i64, hi: i64, lc: Certainty, hc: Certainty) -> Btic {
        let meta = Btic::build_meta(Granularity::Millisecond, Granularity::Millisecond, lc, hc);
        Btic::new(lo, hi, meta).unwrap()
    }

    // -- intersection --

    #[test]
    fn intersection_overlapping() {
        let a = make(100, 300);
        let b = make(200, 400);
        let r = intersection(&a, &b).unwrap();
        assert_eq!(r.lo(), 200);
        assert_eq!(r.hi(), 300);
    }

    #[test]
    fn intersection_contained() {
        let a = make(100, 400);
        let b = make(200, 300);
        let r = intersection(&a, &b).unwrap();
        assert_eq!(r.lo(), 200);
        assert_eq!(r.hi(), 300);
    }

    #[test]
    fn intersection_identical() {
        let a = make(100, 200);
        let r = intersection(&a, &a).unwrap();
        assert_eq!(r.lo(), 100);
        assert_eq!(r.hi(), 200);
    }

    #[test]
    fn intersection_disjoint() {
        let a = make(100, 200);
        let b = make(300, 400);
        assert!(intersection(&a, &b).is_none());
    }

    #[test]
    fn intersection_adjacent_is_none() {
        let a = make(100, 200);
        let b = make(200, 300);
        assert!(intersection(&a, &b).is_none()); // lo >= hi
    }

    #[test]
    fn intersection_granularity_inherited() {
        let a = make_with_gran(100, 300, Granularity::Month, Granularity::Year);
        let b = make_with_gran(200, 400, Granularity::Day, Granularity::Day);
        let r = intersection(&a, &b).unwrap();
        // lo=200 comes from b (larger), so lo_gran = b's lo_gran = Day
        assert_eq!(r.lo_granularity(), Granularity::Day);
        // hi=300 comes from a (smaller), so hi_gran = a's hi_gran = Year
        assert_eq!(r.hi_granularity(), Granularity::Year);
    }

    #[test]
    fn intersection_equal_bounds_finer_granularity() {
        let a = make_with_gran(100, 300, Granularity::Year, Granularity::Year);
        let b = make_with_gran(100, 300, Granularity::Day, Granularity::Day);
        let r = intersection(&a, &b).unwrap();
        // Equal lo: finer = Day (lower code)
        assert_eq!(r.lo_granularity(), Granularity::Day);
        assert_eq!(r.hi_granularity(), Granularity::Day);
    }

    #[test]
    fn intersection_certainty_inherited() {
        let a = make_with_cert(100, 300, Certainty::Definite, Certainty::Approximate);
        let b = make_with_cert(200, 400, Certainty::Uncertain, Certainty::Definite);
        let r = intersection(&a, &b).unwrap();
        // lo=200 from b → b's lo_cert = Uncertain
        assert_eq!(r.lo_certainty(), Certainty::Uncertain);
        // hi=300 from a → a's hi_cert = Approximate
        assert_eq!(r.hi_certainty(), Certainty::Approximate);
    }

    #[test]
    fn intersection_with_sentinel() {
        let a = Btic::new(
            NEG_INF,
            300,
            Btic::build_meta(
                Granularity::Millisecond,
                Granularity::Day,
                Certainty::Definite,
                Certainty::Definite,
            ),
        )
        .unwrap();
        let b = make(100, 400);
        let r = intersection(&a, &b).unwrap();
        assert_eq!(r.lo(), 100);
        assert_eq!(r.hi(), 300);
    }

    // -- span --

    #[test]
    fn span_overlapping() {
        let a = make(100, 300);
        let b = make(200, 400);
        let r = span(&a, &b);
        assert_eq!(r.lo(), 100);
        assert_eq!(r.hi(), 400);
    }

    #[test]
    fn span_disjoint() {
        let a = make(100, 200);
        let b = make(300, 400);
        let r = span(&a, &b);
        assert_eq!(r.lo(), 100);
        assert_eq!(r.hi(), 400);
    }

    #[test]
    fn span_contained() {
        let a = make(100, 400);
        let b = make(200, 300);
        let r = span(&a, &b);
        assert_eq!(r.lo(), 100);
        assert_eq!(r.hi(), 400);
    }

    #[test]
    fn span_identical() {
        let a = make(100, 200);
        let r = span(&a, &a);
        assert_eq!(r.lo(), 100);
        assert_eq!(r.hi(), 200);
    }

    #[test]
    fn span_granularity_inherited() {
        let a = make_with_gran(100, 300, Granularity::Month, Granularity::Year);
        let b = make_with_gran(200, 400, Granularity::Day, Granularity::Day);
        let r = span(&a, &b);
        // lo=100 comes from a (smaller), so lo_gran = a's lo_gran = Month
        assert_eq!(r.lo_granularity(), Granularity::Month);
        // hi=400 comes from b (larger), so hi_gran = b's hi_gran = Day
        assert_eq!(r.hi_granularity(), Granularity::Day);
    }

    #[test]
    fn span_with_sentinel() {
        let a = Btic::new(
            NEG_INF,
            200,
            Btic::build_meta(
                Granularity::Millisecond,
                Granularity::Day,
                Certainty::Definite,
                Certainty::Definite,
            ),
        )
        .unwrap();
        let b = make(100, 400);
        let r = span(&a, &b);
        assert_eq!(r.lo(), NEG_INF);
        assert_eq!(r.hi(), 400);
        // Sentinel lo: granularity/certainty must be zeroed per INV-6
        assert_eq!(r.lo_granularity(), Granularity::Millisecond);
        assert_eq!(r.lo_certainty(), Certainty::Definite);
    }

    // -- gap --

    #[test]
    fn gap_disjoint_with_space() {
        let a = make(100, 200);
        let b = make(300, 400);
        let r = gap(&a, &b).unwrap();
        assert_eq!(r.lo(), 200); // min(a.hi, b.hi) = 200
        assert_eq!(r.hi(), 300); // max(a.lo, b.lo) = 300
    }

    #[test]
    fn gap_disjoint_reversed() {
        let a = make(300, 400);
        let b = make(100, 200);
        let r = gap(&a, &b).unwrap();
        assert_eq!(r.lo(), 200);
        assert_eq!(r.hi(), 300);
    }

    #[test]
    fn gap_overlapping_returns_none() {
        let a = make(100, 300);
        let b = make(200, 400);
        assert!(gap(&a, &b).is_none());
    }

    #[test]
    fn gap_adjacent_returns_none() {
        let a = make(100, 200);
        let b = make(200, 300);
        assert!(gap(&a, &b).is_none());
    }

    #[test]
    fn gap_contained_returns_none() {
        let a = make(100, 400);
        let b = make(200, 300);
        assert!(gap(&a, &b).is_none());
    }

    #[test]
    fn gap_granularity_inherited() {
        let a = make_with_gran(100, 200, Granularity::Month, Granularity::Year);
        let b = make_with_gran(300, 400, Granularity::Day, Granularity::Day);
        let r = gap(&a, &b).unwrap();
        // gap lo=200 comes from min(a.hi=200, b.hi=400) = a.hi → a's hi_gran = Year
        assert_eq!(r.lo_granularity(), Granularity::Year);
        // gap hi=300 comes from max(a.lo=100, b.lo=300) = b.lo → b's lo_gran = Day
        assert_eq!(r.hi_granularity(), Granularity::Day);
    }
}
