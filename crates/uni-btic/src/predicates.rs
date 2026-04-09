use crate::btic::Btic;

/// Returns true if `interval` contains the point (in ms since epoch).
///
/// Uses half-open semantics: `interval.lo <= point < interval.hi`.
pub fn contains_point(interval: &Btic, point: i64) -> bool {
    interval.lo() <= point && point < interval.hi()
}

/// Returns true if intervals `a` and `b` overlap (share at least one tick).
///
/// Two half-open intervals overlap iff `a.lo < b.hi AND b.lo < a.hi`.
pub fn overlaps(a: &Btic, b: &Btic) -> bool {
    a.lo() < b.hi() && b.lo() < a.hi()
}

/// Returns true if `a` fully contains `b` (including equal bounds).
///
/// Covers Allen relations: during-inverse, starts-inverse, finishes-inverse, equals.
pub fn contains(a: &Btic, b: &Btic) -> bool {
    a.lo() <= b.lo() && b.hi() <= a.hi()
}

/// Returns true if `a` ends before or exactly when `b` starts.
///
/// This is `before ∪ meets` in Allen's terminology.
pub fn before(a: &Btic, b: &Btic) -> bool {
    a.hi() <= b.lo()
}

/// Returns true if `a` starts after or exactly when `b` ends.
///
/// Inverse of `before`.
pub fn after(a: &Btic, b: &Btic) -> bool {
    b.hi() <= a.lo()
}

/// Returns true if `a` ends exactly where `b` starts (no gap, no overlap).
pub fn meets(a: &Btic, b: &Btic) -> bool {
    a.hi() == b.lo()
}

/// Returns true if the intervals are adjacent (meets or met-by).
pub fn adjacent(a: &Btic, b: &Btic) -> bool {
    a.hi() == b.lo() || b.hi() == a.lo()
}

/// Returns true if the intervals share no ticks.
pub fn disjoint(a: &Btic, b: &Btic) -> bool {
    a.hi() <= b.lo() || b.hi() <= a.lo()
}

/// Temporal equivalence: same `lo` and `hi`, ignoring metadata.
///
/// This differs from `==` (bytewise equality) which also compares granularity/certainty.
pub fn btic_equals(a: &Btic, b: &Btic) -> bool {
    a.lo() == b.lo() && a.hi() == b.hi()
}

/// Returns true if `a` starts at the same point as `b` but ends earlier.
pub fn starts(a: &Btic, b: &Btic) -> bool {
    a.lo() == b.lo() && a.hi() < b.hi()
}

/// Returns true if `a` is strictly contained within `b` (both bounds strictly inside).
pub fn during(a: &Btic, b: &Btic) -> bool {
    b.lo() < a.lo() && a.hi() < b.hi()
}

/// Returns true if `a` ends at the same point as `b` but starts later.
pub fn finishes(a: &Btic, b: &Btic) -> bool {
    a.hi() == b.hi() && b.lo() < a.lo()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::certainty::Certainty;
    use crate::granularity::Granularity;

    fn make_btic(lo: i64, hi: i64) -> Btic {
        let meta = Btic::build_meta(
            Granularity::Millisecond,
            Granularity::Millisecond,
            Certainty::Definite,
            Certainty::Definite,
        );
        Btic::new(lo, hi, meta).unwrap()
    }

    #[test]
    fn contains_point_basic() {
        let interval = make_btic(100, 200);
        assert!(contains_point(&interval, 100)); // lo inclusive
        assert!(contains_point(&interval, 150));
        assert!(!contains_point(&interval, 200)); // hi exclusive
        assert!(!contains_point(&interval, 99));
        assert!(!contains_point(&interval, 201));
    }

    #[test]
    fn contains_point_instant() {
        let interval = make_btic(100, 101);
        assert!(contains_point(&interval, 100));
        assert!(!contains_point(&interval, 101));
    }

    #[test]
    fn overlaps_basic() {
        let a = make_btic(100, 200);
        let b = make_btic(150, 250);
        assert!(overlaps(&a, &b));
        assert!(overlaps(&b, &a));
    }

    #[test]
    fn overlaps_adjacent_not_overlapping() {
        let a = make_btic(100, 200);
        let b = make_btic(200, 300);
        assert!(!overlaps(&a, &b)); // [100,200) and [200,300) share no ticks
        assert!(!overlaps(&b, &a));
    }

    #[test]
    fn overlaps_contained() {
        let outer = make_btic(100, 300);
        let inner = make_btic(150, 250);
        assert!(overlaps(&outer, &inner));
        assert!(overlaps(&inner, &outer));
    }

    #[test]
    fn overlaps_disjoint() {
        let a = make_btic(100, 200);
        let b = make_btic(300, 400);
        assert!(!overlaps(&a, &b));
        assert!(!overlaps(&b, &a));
    }

    #[test]
    fn overlaps_same_interval() {
        let a = make_btic(100, 200);
        assert!(overlaps(&a, &a));
    }

    // -- contains --

    #[test]
    fn contains_strict() {
        let outer = make_btic(100, 300);
        let inner = make_btic(150, 250);
        assert!(contains(&outer, &inner));
        assert!(!contains(&inner, &outer));
    }

    #[test]
    fn contains_equal() {
        let a = make_btic(100, 200);
        assert!(contains(&a, &a)); // equal bounds → contains
    }

    #[test]
    fn contains_shared_lo() {
        let a = make_btic(100, 300);
        let b = make_btic(100, 200);
        assert!(contains(&a, &b));
        assert!(!contains(&b, &a));
    }

    #[test]
    fn contains_shared_hi() {
        let a = make_btic(100, 300);
        let b = make_btic(200, 300);
        assert!(contains(&a, &b));
        assert!(!contains(&b, &a));
    }

    // -- before / after --

    #[test]
    fn before_with_gap() {
        let a = make_btic(100, 200);
        let b = make_btic(300, 400);
        assert!(before(&a, &b));
        assert!(!before(&b, &a));
    }

    #[test]
    fn before_adjacent() {
        let a = make_btic(100, 200);
        let b = make_btic(200, 300);
        assert!(before(&a, &b)); // a.hi <= b.lo → true (includes meets)
    }

    #[test]
    fn before_overlapping() {
        let a = make_btic(100, 250);
        let b = make_btic(200, 300);
        assert!(!before(&a, &b));
    }

    #[test]
    fn after_basic() {
        let a = make_btic(300, 400);
        let b = make_btic(100, 200);
        assert!(after(&a, &b));
        assert!(!after(&b, &a));
    }

    // -- meets --

    #[test]
    fn meets_true() {
        let a = make_btic(100, 200);
        let b = make_btic(200, 300);
        assert!(meets(&a, &b));
        assert!(!meets(&b, &a)); // b does not meet a (b.hi=300 != a.lo=100)
    }

    #[test]
    fn meets_gap() {
        let a = make_btic(100, 200);
        let b = make_btic(201, 300);
        assert!(!meets(&a, &b));
    }

    #[test]
    fn meets_overlap() {
        let a = make_btic(100, 250);
        let b = make_btic(200, 300);
        assert!(!meets(&a, &b));
    }

    // -- adjacent --

    #[test]
    fn adjacent_forward() {
        let a = make_btic(100, 200);
        let b = make_btic(200, 300);
        assert!(adjacent(&a, &b));
        assert!(adjacent(&b, &a)); // symmetric
    }

    #[test]
    fn adjacent_gap() {
        let a = make_btic(100, 200);
        let b = make_btic(201, 300);
        assert!(!adjacent(&a, &b));
    }

    // -- disjoint --

    #[test]
    fn disjoint_with_gap() {
        let a = make_btic(100, 200);
        let b = make_btic(300, 400);
        assert!(disjoint(&a, &b));
        assert!(disjoint(&b, &a));
    }

    #[test]
    fn disjoint_adjacent() {
        let a = make_btic(100, 200);
        let b = make_btic(200, 300);
        assert!(disjoint(&a, &b)); // adjacent intervals are disjoint (no shared ticks)
    }

    #[test]
    fn disjoint_overlapping() {
        let a = make_btic(100, 250);
        let b = make_btic(200, 300);
        assert!(!disjoint(&a, &b));
    }

    // -- btic_equals --

    #[test]
    fn btic_equals_same() {
        let a = make_btic(100, 200);
        assert!(btic_equals(&a, &a));
    }

    #[test]
    fn btic_equals_different_meta() {
        let meta_day = Btic::build_meta(
            Granularity::Day,
            Granularity::Day,
            Certainty::Definite,
            Certainty::Definite,
        );
        let meta_year = Btic::build_meta(
            Granularity::Year,
            Granularity::Year,
            Certainty::Definite,
            Certainty::Definite,
        );
        let a = Btic::new(100, 200, meta_day).unwrap();
        let b = Btic::new(100, 200, meta_year).unwrap();
        assert!(btic_equals(&a, &b)); // same lo/hi → temporally equal
        assert_ne!(a, b); // different meta → not bytewise equal
    }

    #[test]
    fn btic_equals_different_bounds() {
        let a = make_btic(100, 200);
        let b = make_btic(100, 300);
        assert!(!btic_equals(&a, &b));
    }

    // -- starts / during / finishes --

    #[test]
    fn starts_true() {
        let a = make_btic(100, 200);
        let b = make_btic(100, 300);
        assert!(starts(&a, &b)); // a starts b: same lo, a.hi < b.hi
        assert!(!starts(&b, &a));
    }

    #[test]
    fn starts_equal_not_starts() {
        let a = make_btic(100, 200);
        assert!(!starts(&a, &a)); // equal intervals: a.hi == b.hi, not <
    }

    #[test]
    fn during_true() {
        let a = make_btic(150, 250);
        let b = make_btic(100, 300);
        assert!(during(&a, &b)); // a during b: strictly inside
        assert!(!during(&b, &a));
    }

    #[test]
    fn during_shared_bound_not_during() {
        let a = make_btic(100, 250);
        let b = make_btic(100, 300);
        assert!(!during(&a, &b)); // shared lo → not strictly during
    }

    #[test]
    fn finishes_true() {
        let a = make_btic(200, 300);
        let b = make_btic(100, 300);
        assert!(finishes(&a, &b)); // a finishes b: same hi, b.lo < a.lo
        assert!(!finishes(&b, &a));
    }

    #[test]
    fn finishes_equal_not_finishes() {
        let a = make_btic(100, 200);
        assert!(!finishes(&a, &a)); // equal: b.lo not < a.lo
    }
}
