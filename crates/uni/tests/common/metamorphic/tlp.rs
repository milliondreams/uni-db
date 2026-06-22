//! Ternary Logic Partitioning (TLP) oracle.
//!
//! For any query `Q = MATCH … [WHERE B] RETURN …` and any predicate `p` over
//! `Q`'s bound variables, a correct engine must satisfy the multiset identity
//!
//! ```text
//! bag(Q) == bag(Q WHERE p) ⊎ bag(Q WHERE NOT p) ⊎ bag(Q WHERE p IS NULL)
//! ```
//!
//! Every matched row falls in exactly one partition — `p` evaluates to TRUE,
//! FALSE, or (three-valued-logic) NULL — so the three filtered results partition
//! `Q`'s rows with no overlap and no loss. A mismatch is a real bug: dropped
//! rows from pushdown, mishandled NULLs, or a broken `WHERE`.
//!
//! The law holds only for pure filter+projection queries. The generator
//! (`querygen::arb_case`) is structurally unable to emit the constructs that
//! would break it — DISTINCT, LIMIT/SKIP, aggregation, OPTIONAL MATCH — so the
//! oracle never sees a law-breaking shape. The base predicate `B` composes
//! safely: it is applied identically in all four queries, so it only restricts
//! the common candidate set.
//!
//! # Aggregate variant
//!
//! For `count(*)` / `sum(prop)` the partitions reconcile additively:
//! `agg(Q) == agg(p) + agg(NOT p) + agg(p IS NULL)`. `sum` skips NULL operands
//! identically in every partition, so the identity still holds.

use uni_db::Uni;

use super::{drive, run_bag, run_scalar, smoke_cases, soak_cases};
use crate::diff::{bag_eq, bag_union};
use crate::querygen::render::render;
use crate::querygen::{Case, Partition, arb_agg_case, arb_case};

/// Absolute tolerance for the aggregate-`sum` reconciliation.
///
/// Sums over the seed are small (ages in the hundreds, scores ≤ ~12), so any
/// difference above this is a real discrepancy, not float drift.
const SUM_EPSILON: f64 = 1e-9;

/// Checks the row-level TLP identity for one case against the shared seed db.
///
/// # Errors
///
/// Returns a descriptive error — including the rendered queries and the bag
/// symmetric difference — when the identity fails.
async fn check_tlp_row(db: &Uni, case: &Case) -> anyhow::Result<()> {
    let base = run_bag(db, &case.base_query()).await?;
    let t = run_bag(db, &case.partition_query(Partition::True)).await?;
    let f = run_bag(db, &case.partition_query(Partition::False)).await?;
    let n = run_bag(db, &case.partition_query(Partition::Null)).await?;
    let combined = bag_union(&[t, f, n]);
    if let Err(diff) = bag_eq(&base, &combined) {
        anyhow::bail!(
            "TLP row law violated.\n  base:    {}\n  p:       {}\n  NOT p:   {}\n  p ISNULL:{}\n{diff}",
            render(&case.base_query()),
            render(&case.partition_query(Partition::True)),
            render(&case.partition_query(Partition::False)),
            render(&case.partition_query(Partition::Null)),
        );
    }
    Ok(())
}

/// Checks the additive aggregate-TLP identity for one case.
///
/// # Errors
///
/// Returns a descriptive error when the whole-query aggregate differs from the
/// sum of its three partition aggregates by more than [`SUM_EPSILON`].
async fn check_tlp_agg(db: &Uni, case: &Case) -> anyhow::Result<()> {
    let whole = run_scalar(db, &case.base_query()).await?;
    let t = run_scalar(db, &case.partition_query(Partition::True)).await?;
    let f = run_scalar(db, &case.partition_query(Partition::False)).await?;
    let n = run_scalar(db, &case.partition_query(Partition::Null)).await?;
    let parts = t + f + n;
    anyhow::ensure!(
        (whole - parts).abs() <= SUM_EPSILON,
        "TLP aggregate law violated: whole={whole} != p+NOTp+ISNULL={parts} \
         ({t} + {f} + {n}).\n  base: {}",
        render(&case.base_query()),
    );
    Ok(())
}

/// PR smoke gate for the row-level TLP identity.
#[test]
fn tlp_row_law_smoke() {
    drive(smoke_cases(), arb_case(), |db, case| {
        Box::pin(check_tlp_row(db, case))
    });
}

/// PR smoke gate for the aggregate-TLP identity.
#[test]
fn tlp_agg_law_smoke() {
    drive(smoke_cases(), arb_agg_case(), |db, case| {
        Box::pin(check_tlp_agg(db, case))
    });
}

/// Nightly soak for the row-level TLP identity; volume from `METAMORPHIC_CASES`.
///
/// Non-vacuity of the `IS NULL` partition is proved deterministically by the
/// targeted test [`targeted::null_partition_is_nonvacuous`], so the soak needs
/// no per-run accumulator.
#[test]
#[ignore = "soak: high-volume nightly run; volume set by METAMORPHIC_CASES"]
fn tlp_row_law_soak() {
    drive(soak_cases(), arb_case(), |db, case| {
        Box::pin(check_tlp_row(db, case))
    });
}

/// Nightly soak for the aggregate-TLP identity; volume from `METAMORPHIC_CASES`.
#[test]
#[ignore = "soak: high-volume nightly run; volume set by METAMORPHIC_CASES"]
fn tlp_agg_law_soak() {
    drive(soak_cases(), arb_agg_case(), |db, case| {
        Box::pin(check_tlp_agg(db, case))
    });
}

#[cfg(test)]
mod targeted {
    use super::*;
    use crate::diff::bag;
    use crate::metamorphic::seed::build_seed;

    /// The `IS NULL` partition is non-empty for a nullable-prop predicate —
    /// proof the three-valued-logic branch has teeth (else the law is vacuous).
    #[tokio::test]
    async fn null_partition_is_nonvacuous() -> anyhow::Result<()> {
        let db = build_seed().await?;
        let n = bag(&db
            .session()
            .query("MATCH (a:Person) WHERE (a.age > 30) IS NULL RETURN a.name AS name")
            .await?);
        assert_eq!(
            n.total, 2,
            "the two age-NULL persons land in the IS NULL partition"
        );
        Ok(())
    }

    /// Dropping the `IS NULL` partition must break the identity whenever the
    /// predicate has NULL rows — the oracle's teeth check.
    #[tokio::test]
    async fn oracle_detects_missing_null_partition() -> anyhow::Result<()> {
        let db = build_seed().await?;
        let s = db.session();
        let base = bag(&s.query("MATCH (a:Person) RETURN a.name AS name").await?);
        let t = bag(&s
            .query("MATCH (a:Person) WHERE a.age > 30 RETURN a.name AS name")
            .await?);
        let f = bag(&s
            .query("MATCH (a:Person) WHERE NOT (a.age > 30) RETURN a.name AS name")
            .await?);
        // Deliberately omit the IS NULL partition: the two age-NULL persons are
        // in `base` but in neither `t` nor `f`.
        let broken = bag_union(&[t, f]);
        let diff = bag_eq(&base, &broken).expect_err("must detect the missing NULL partition");
        assert_eq!(diff.left_total, 12);
        assert_eq!(diff.right_total, 10);
        Ok(())
    }
}
