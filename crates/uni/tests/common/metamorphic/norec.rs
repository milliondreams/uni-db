//! No-Optimization Reference Comparison (NoREC) oracle.
//!
//! A predicate must produce the same answer whether or not the optimizer can
//! push it into the scan. We compare two semantically identical queries:
//!
//! ```text
//! optimized:    MATCH (a:Person) WHERE p RETURN …
//! unoptimized:  MATCH (a:Person) WITH a, (p) AS keep WHERE keep RETURN …
//! ```
//!
//! In the unoptimized form `keep` is a projected column, so the optimizer cannot
//! push it back into the `(a:Person)` scan — the predicate is forced to evaluate
//! post-scan. A correct engine returns the same bag either way; a divergence
//! exposes a pushdown bug (the bug class behind several `bugs/` repros).
//!
//! The plain `(p) AS keep` form preserves three-valued logic exactly: a NULL
//! `keep` is dropped by `WHERE keep`, identical to `WHERE p`. (A `CASE … ELSE
//! false` barrier would instead collapse NULL→false, so it is deliberately not
//! used here.)

use uni_db::Uni;

use super::{drive, run_bag, smoke_cases, soak_cases};
use crate::diff::bag_eq;
use crate::querygen::render::render;
use crate::querygen::{Case, arb_case};

/// Checks that the optimized and barrier forms agree for one case.
///
/// # Errors
///
/// Returns a descriptive error — rendered queries plus the bag symmetric
/// difference — when the two forms disagree.
async fn check_norec(db: &Uni, case: &Case) -> anyhow::Result<()> {
    let optimized = run_bag(db, &case.norec_optimized()).await?;
    let unoptimized = run_bag(db, &case.norec_unoptimized()).await?;
    if let Err(diff) = bag_eq(&optimized, &unoptimized) {
        anyhow::bail!(
            "NoREC mismatch.\n  optimized:   {}\n  unoptimized: {}\n{diff}",
            render(&case.norec_optimized()),
            render(&case.norec_unoptimized()),
        );
    }
    Ok(())
}

/// PR smoke gate: optimized vs `WITH`-barrier forms must agree.
#[test]
fn norec_law_smoke() {
    drive(smoke_cases(), arb_case(), |db, case| {
        Box::pin(check_norec(db, case))
    });
}

/// Nightly soak for the NoREC identity; volume from `METAMORPHIC_CASES`.
#[test]
#[ignore = "soak: high-volume nightly run; volume set by METAMORPHIC_CASES"]
fn norec_law_soak() {
    drive(soak_cases(), arb_case(), |db, case| {
        Box::pin(check_norec(db, case))
    });
}

#[cfg(test)]
mod targeted {
    use super::*;
    use crate::diff::bag;
    use crate::metamorphic::seed::build_seed;

    /// The optimized and barrier forms agree on a concrete predicate, and the
    /// barrier form really does filter (its result is a strict subset of all
    /// persons) — proof the comparison has teeth.
    #[tokio::test]
    async fn barrier_matches_optimized_and_filters() -> anyhow::Result<()> {
        let db = build_seed().await?;
        let s = db.session();
        let optimized = bag(&s
            .query("MATCH (a:Person) WHERE a.age > 30 RETURN a.name AS name")
            .await?);
        let barrier = bag(&s
            .query("MATCH (a:Person) WITH a, (a.age > 30) AS keep WHERE keep RETURN a.name AS name")
            .await?);
        assert!(
            bag_eq(&optimized, &barrier).is_ok(),
            "optimized and WITH-barrier forms must agree"
        );
        let all = bag(&s.query("MATCH (a:Person) RETURN a.name AS name").await?);
        assert!(
            barrier.total < all.total,
            "the barrier form must actually filter (else the test is vacuous)"
        );
        Ok(())
    }
}
