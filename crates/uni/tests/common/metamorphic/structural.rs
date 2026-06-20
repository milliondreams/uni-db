//! Structural metamorphic checks (the plan's B2 "cheap structural checks").
//!
//! Three reference-free invariants a correct engine must satisfy for any query
//! `Q`, each targeting a bug class the TLP/NoREC oracles don't directly cover:
//!
//! 1. **ORDER BY is a permutation:** `bag(Q ORDER BY x) == bag(Q)` — sorting may
//!    only reorder rows, never add, drop, or alter them.
//! 2. **LIMIT is a sub-bag:** `bag(Q LIMIT n) ⊆ bag(Q)` — a limited result may
//!    only ever be a sub-multiset of the unlimited one (no phantom rows).
//! 3. **count equals enumeration:** `count(Q) == |rows(Q)|` — the aggregation
//!    engine must agree with the scan on cardinality.
//!
//! All three run against the shared read-only seed db (see `metamorphic::drive`)
//! and reuse `arb_case` + the `diff` bag comparator.

use uni_db::Uni;

use super::{drive, run_bag, run_scalar, smoke_cases, soak_cases};
use crate::diff::{bag_eq, bag_is_subset};
use crate::querygen::render::render;
use crate::querygen::{Case, arb_case};

/// Fixed `LIMIT` values exercised per case.
///
/// Straddle the seed's cardinality (≤12 Person rows / ≤10 edges): `0` and `1`
/// hit the small boundary, and `100` exceeds any base result so the limited bag
/// must equal the whole.
const LIMITS: &[u32] = &[0, 1, 5, 100];

/// Tolerance for the integer count comparison (counts are exact in `f64`).
const COUNT_EPSILON: f64 = 1e-9;

/// Runs all three structural laws for one case against the shared seed db.
///
/// # Errors
///
/// Returns a descriptive error — naming the law and including the rendered
/// queries plus the bag difference — on the first violation.
async fn check_structural(db: &Uni, case: &Case) -> anyhow::Result<()> {
    let base = run_bag(db, &case.base_query()).await?;

    // 1. ORDER BY is a permutation of the unordered result.
    let ordered = run_bag(db, &case.ordered_query()).await?;
    if let Err(diff) = bag_eq(&base, &ordered) {
        anyhow::bail!(
            "ORDER BY changed the row bag.\n  base:    {}\n  ordered: {}\n{diff}",
            render(&case.base_query()),
            render(&case.ordered_query()),
        );
    }

    // 2. Every LIMIT n result is a sub-bag of the whole.
    for &n in LIMITS {
        let limited = run_bag(db, &case.limited_query(n)).await?;
        anyhow::ensure!(
            limited.total <= base.total && limited.total <= n as usize,
            "LIMIT {n} returned {} rows (base has {}).\n  {}",
            limited.total,
            base.total,
            render(&case.limited_query(n)),
        );
        if let Err(diff) = bag_is_subset(&limited, &base) {
            anyhow::bail!(
                "LIMIT {n} produced rows absent from the base result.\n  base:    {}\n  limited: {}\n{diff}",
                render(&case.base_query()),
                render(&case.limited_query(n)),
            );
        }
    }

    // 3. count(*) equals the actual row count.
    let counted = run_scalar(db, &case.count_query()).await?;
    anyhow::ensure!(
        (counted - base.total as f64).abs() < COUNT_EPSILON,
        "count(*)={counted} disagrees with enumeration |rows|={}.\n  count: {}\n  base:  {}",
        base.total,
        render(&case.count_query()),
        render(&case.base_query()),
    );

    Ok(())
}

/// PR smoke gate for the structural laws.
#[test]
fn structural_laws_smoke() {
    drive(smoke_cases(), arb_case(), |db, case| {
        Box::pin(check_structural(db, case))
    });
}

/// Nightly soak for the structural laws; volume from `METAMORPHIC_CASES`.
#[test]
#[ignore = "soak: high-volume nightly run; volume set by METAMORPHIC_CASES"]
fn structural_laws_soak() {
    drive(soak_cases(), arb_case(), |db, case| {
        Box::pin(check_structural(db, case))
    });
}

#[cfg(test)]
mod targeted {
    use super::*;
    use crate::diff::bag;
    use crate::metamorphic::seed::build_seed;

    /// ORDER BY preserves the bag and LIMIT/count behave, on a concrete query —
    /// the structural oracle's teeth check.
    #[tokio::test]
    async fn structural_laws_hold_on_a_concrete_query() -> anyhow::Result<()> {
        let db = build_seed().await?;
        let s = db.session();
        let base = bag(&s.query("MATCH (a:Person) RETURN a.name AS c0").await?);
        let ordered = bag(&s
            .query("MATCH (a:Person) RETURN a.name AS c0 ORDER BY a.name")
            .await?);
        assert!(
            bag_eq(&base, &ordered).is_ok(),
            "ORDER BY must not change the bag"
        );

        let limited = bag(&s
            .query("MATCH (a:Person) RETURN a.name AS c0 LIMIT 3")
            .await?);
        assert_eq!(limited.total, 3, "LIMIT 3 yields 3 of 12 rows");
        assert!(bag_is_subset(&limited, &base).is_ok(), "LIMIT 3 ⊆ base");
        assert!(
            bag_is_subset(&base, &limited).is_err(),
            "base is NOT a subset of LIMIT 3 — the check has teeth"
        );

        let counted: i64 = s
            .query("MATCH (a:Person) RETURN count(*) AS c")
            .await?
            .rows()[0]
            .get::<i64>("c")?;
        assert_eq!(counted as usize, base.total, "count(*) == |rows|");
        Ok(())
    }
}
