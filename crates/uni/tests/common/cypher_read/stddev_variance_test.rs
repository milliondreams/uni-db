// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! REQ-3 regression: native `stDev` / `stDevP` / `variance` / `varianceP`
//! aggregates in Cypher.
//!
//! Previously `stDev`/`stDevP` were recognised as aggregate names but had no
//! planner implementation (they fell through to the plugin-aggregate path and
//! errored "Unsupported aggregate function"), and there was no `variance`. The
//! fix maps them to DataFusion's `stddev`/`stddev_pop`/`var_sample`/`var_pop`
//! UDAFs, which use Welford's online algorithm — numerically stable, unlike the
//! `sqrt(avg(x*x) - avg(x)^2)` identity callers previously used (which cancels
//! catastrophically for large means). Inputs are coerced to Float64 the same way
//! `avg` is, so raw schemaless integer properties work without `toFloat(...)`.

use anyhow::Result;
use uni_db::Uni;

/// Seed the classic stats sample [2,4,4,4,5,5,7,9] (mean 5, Σdev² = 32).
async fn sample_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:X {v: 2}), (:X {v: 4}), (:X {v: 4}), (:X {v: 4}),
                (:X {v: 5}), (:X {v: 5}), (:X {v: 7}), (:X {v: 9})",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// REQ-3: sample vs population standard deviation and variance, on a raw
/// (uncoerced) integer property.
#[tokio::test]
async fn stdev_and_variance_sample_and_population() -> Result<()> {
    let db = sample_db().await?;
    let s = db.session();

    // Sample stddev = sqrt(32/7) ≈ 2.138090.
    let r = s.query("MATCH (n:X) RETURN stDev(n.v) AS x").await?;
    assert!((r.rows()[0].get::<f64>("x")? - 2.138_089_9).abs() < 1e-5);

    // Population stddev = sqrt(32/8) = 2.0.
    let r = s.query("MATCH (n:X) RETURN stDevP(n.v) AS x").await?;
    assert!((r.rows()[0].get::<f64>("x")? - 2.0).abs() < 1e-9);

    // Sample variance = 32/7 ≈ 4.571429.
    let r = s.query("MATCH (n:X) RETURN variance(n.v) AS x").await?;
    assert!((r.rows()[0].get::<f64>("x")? - 4.571_428_6).abs() < 1e-5);

    // Population variance = 32/8 = 4.0.
    let r = s.query("MATCH (n:X) RETURN varianceP(n.v) AS x").await?;
    assert!((r.rows()[0].get::<f64>("x")? - 4.0).abs() < 1e-9);
    Ok(())
}

/// REQ-3 completeness: `GROUP BY` computes the aggregate independently per group.
#[tokio::test]
async fn stdev_with_group_by() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    // 'a': [2,4,6] sample stddev = 2.0 ; 'b': [10,20] sample stddev ≈ 7.0711
    tx.execute(
        "CREATE (:Y {g: 'a', v: 2}), (:Y {g: 'a', v: 4}), (:Y {g: 'a', v: 6}),
                (:Y {g: 'b', v: 10}), (:Y {g: 'b', v: 20})",
    )
    .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Y) RETURN n.g AS g, stDev(n.v) AS x ORDER BY g")
        .await?;
    assert_eq!(r.rows().len(), 2);
    assert!((r.rows()[0].get::<f64>("x")? - 2.0).abs() < 1e-9);
    assert!((r.rows()[1].get::<f64>("x")? - 7.071_067_8).abs() < 1e-5);
    Ok(())
}

/// REQ-3 numerical stability: for large means the naive
/// `sqrt(avg(x*x) - avg(x)^2)` identity cancels catastrophically; the native
/// (Welford) aggregate stays accurate. Sample [1e8 .. 1e8+4] has sample
/// stddev = sqrt(10/4) ≈ 1.5811388.
#[tokio::test]
async fn stdev_is_numerically_stable_for_large_means() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Big {v: 100000000}), (:Big {v: 100000001}), (:Big {v: 100000002}),
                (:Big {v: 100000003}), (:Big {v: 100000004})",
    )
    .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Big) RETURN stDev(n.v) AS x")
        .await?;
    assert!(
        (r.rows()[0].get::<f64>("x")? - 1.581_138_8).abs() < 1e-3,
        "native stDev should stay accurate for large means; got {}",
        r.rows()[0].get::<f64>("x")?
    );
    Ok(())
}

/// REQ-3 edge case: population stddev of a single value is 0.
#[tokio::test]
async fn population_stdev_of_single_value_is_zero() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:S {v: 42})").await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:S) RETURN stDevP(n.v) AS x")
        .await?;
    assert!((r.rows()[0].get::<f64>("x")? - 0.0).abs() < 1e-12);
    Ok(())
}

/// REQ-3 robustness: non-numeric values in the column are coerced to NULL and
/// skipped (matching `avg`), rather than crashing the query. Mixed [2,4,'x',6]
/// ⇒ sample stddev over {2,4,6} = 2.0.
#[tokio::test]
async fn stdev_skips_non_numeric_values() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Z {v: 2}), (:Z {v: 4}), (:Z {v: 'x'}), (:Z {v: 6})")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Z) RETURN stDev(n.v) AS x")
        .await?;
    assert!(
        (r.rows()[0].get::<f64>("x")? - 2.0).abs() < 1e-9,
        "stDev should skip the non-numeric value and compute over {{2,4,6}}; got {}",
        r.rows()[0].get::<f64>("x")?
    );
    Ok(())
}
