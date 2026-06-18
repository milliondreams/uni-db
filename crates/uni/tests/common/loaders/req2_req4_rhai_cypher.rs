// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! REQ-2 / REQ-4 regression tests (uniscape × uni-db gap report).
//!
//! # REQ-2 — Rhai aggregate UDFs callable in Cypher `GROUP BY`
//!
//! A Rhai plugin aggregate (`*_init`/`*_accumulate`/`*_merge`/`*_finalize`)
//! registered successfully but the Cypher planner rejected the call with
//! `UDF '<name>' is not registered`: the dynamic loaders never published the
//! aggregate's Cypher routing hint (`register_plugin_aggregate`), so the planner
//! classified `RETURN ns.agg(x)` as a scalar UDF. The fix records staged
//! aggregate qnames on the `PluginRegistrar` and publishes them centrally in
//! `with_loading_registrar`, covering every dynamic loader at once.
//!
//! # REQ-4 — plugin scalar arg coercion from a raw property
//!
//! A raw integer/float node property reaches a plugin scalar as a `LargeBinary`
//! CypherValue array, so a scalar declaring `Primitive(Int64)` failed its
//! downcast unless the caller wrote `toInteger(...)`. The fix auto-coerces the
//! `LargeBinary` transport to the declared numeric type before invoke, and emits
//! a precise error for genuinely non-numeric values.

#![cfg(feature = "rhai-plugins")]

use anyhow::Result;
use uni_plugin::{Capability, CapabilitySet};

/// A Rhai aggregate computing the *sample* standard deviation via Welford-style
/// sums. Plugin id is single-segment (`mcagg`) so the Cypher planner's
/// first-dot qname split (`mcagg.sstddev`) resolves correctly.
const SSTDDEV_PLUGIN: &str = r#"
    fn uni_manifest() {
        #{
            id: "mcagg",
            version: "0.1.0",
            determinism: "pure",
            aggregate_fns: [
                #{ name: "sstddev", args: ["float"], returns: "float", state: "map" },
            ],
        }
    }
    fn sstddev_init() { #{ n: 0, sum: 0.0, sum_sq: 0.0 } }
    fn sstddev_accumulate(state, x) {
        state.n += 1;
        state.sum += x;
        state.sum_sq += x * x;
        state
    }
    fn sstddev_merge(a, b) {
        #{ n: a.n + b.n, sum: a.sum + b.sum, sum_sq: a.sum_sq + b.sum_sq }
    }
    fn sstddev_finalize(s) {
        if s.n < 2 { return (); }
        let mean = s.sum / s.n;
        let variance = (s.sum_sq - s.sum * mean) / (s.n - 1);
        variance.sqrt()
    }
"#;

/// REQ-2: a Rhai aggregate is callable in a Cypher aggregation and computes the
/// correct value. The classic sample `[2,4,4,4,5,5,7,9]` has sample stddev
/// ≈ 2.13809 (mean 5, Σdev² = 32, /7).
#[tokio::test]
async fn rhai_aggregate_callable_in_cypher() -> Result<()> {
    let db = uni_db::Uni::in_memory().build().await?;
    let loader = uni_plugin_rhai::RhaiLoader::new();
    let caps = CapabilitySet::from_iter_of([Capability::AggregateFn]);
    let outcome = db.load_rhai_plugin(&loader, SSTDDEV_PLUGIN, &caps)?;
    assert_eq!(
        outcome.aggregates_registered.len(),
        1,
        "expected the aggregate to register"
    );

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:X {v: 2}), (:X {v: 4}), (:X {v: 4}), (:X {v: 4}),
                (:X {v: 5}), (:X {v: 5}), (:X {v: 7}), (:X {v: 9})",
    )
    .await?;
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (n:X) RETURN mcagg.sstddev(toFloat(n.v)) AS sd")
        .await?;
    let sd = res.rows()[0].get::<f64>("sd")?;
    assert!(
        (sd - 2.138_089_9).abs() < 1e-5,
        "rhai aggregate in Cypher returned {sd}, expected ~2.13809"
    );
    Ok(())
}

/// REQ-2 completeness: the same aggregate works under `GROUP BY`, computed
/// independently per group.
#[tokio::test]
async fn rhai_aggregate_works_with_group_by() -> Result<()> {
    let db = uni_db::Uni::in_memory().build().await?;
    let loader = uni_plugin_rhai::RhaiLoader::new();
    let caps = CapabilitySet::from_iter_of([Capability::AggregateFn]);
    db.load_rhai_plugin(&loader, SSTDDEV_PLUGIN, &caps)?;

    let tx = db.session().tx().await?;
    // group 'a': [2,4,6] sample stddev = 2.0 ; group 'b': [10,20] sample stddev ≈ 7.0711
    tx.execute(
        "CREATE (:Y {g: 'a', v: 2}), (:Y {g: 'a', v: 4}), (:Y {g: 'a', v: 6}),
                (:Y {g: 'b', v: 10}), (:Y {g: 'b', v: 20})",
    )
    .await?;
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (n:Y) RETURN n.g AS g, mcagg.sstddev(toFloat(n.v)) AS sd ORDER BY g")
        .await?;
    assert_eq!(res.rows().len(), 2);
    assert_eq!(res.rows()[0].get::<String>("g")?, "a");
    assert!((res.rows()[0].get::<f64>("sd")? - 2.0).abs() < 1e-9);
    assert_eq!(res.rows()[1].get::<String>("g")?, "b");
    assert!((res.rows()[1].get::<f64>("sd")? - 7.071_067_8).abs() < 1e-5);
    Ok(())
}

/// REQ-2 failure scenario: calling an *unregistered* aggregate name still
/// produces a clear error rather than silently succeeding.
#[tokio::test]
async fn unregistered_aggregate_errors_clearly() -> Result<()> {
    let db = uni_db::Uni::in_memory().build().await?;
    let loader = uni_plugin_rhai::RhaiLoader::new();
    let caps = CapabilitySet::from_iter_of([Capability::AggregateFn]);
    db.load_rhai_plugin(&loader, SSTDDEV_PLUGIN, &caps)?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:X {v: 1})").await?;
    tx.commit().await?;

    let err = db
        .session()
        .query("MATCH (n:X) RETURN mcagg.nope(toFloat(n.v)) AS sd")
        .await
        .expect_err("unknown aggregate must error");
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("nope") || msg.to_lowercase().contains("aggregate"),
        "error should name the missing aggregate; got: {msg}"
    );
    Ok(())
}

/// A Rhai scalar declaring an `int` argument. `idf(x) = x + 1`.
const IDF_PLUGIN: &str = r#"
    fn uni_manifest() {
        #{
            id: "mcscale",
            version: "0.1.0",
            determinism: "pure",
            scalar_fns: [
                #{ name: "idf", args: ["int"], returns: "int" },
            ],
        }
    }
    fn idf(x) { x + 1 }
"#;

/// REQ-4: a raw integer property passed straight to a plugin scalar (no
/// `toInteger(...)` wrapper) is auto-coerced from the LargeBinary transport to
/// the declared `Int64` and the call succeeds.
#[tokio::test]
async fn plugin_scalar_accepts_raw_int_property() -> Result<()> {
    let db = uni_db::Uni::in_memory().build().await?;
    let loader = uni_plugin_rhai::RhaiLoader::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    db.load_rhai_plugin(&loader, IDF_PLUGIN, &caps)?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {freq: 5})").await?;
    tx.commit().await?;

    // No toInteger(...) wrapper — the property arrives as LargeBinary and must
    // be coerced to the declared Int64 automatically.
    let res = db
        .session()
        .query("MATCH (d:Doc) RETURN mcscale.idf(d.freq) AS y")
        .await?;
    assert_eq!(
        res.rows()[0].get::<i64>("y")?,
        6,
        "raw int property should be coerced and idf(5) = 6"
    );
    Ok(())
}

/// REQ-4 completeness: the explicit `toInteger(...)` form must keep working
/// (we did not break the path callers already use).
#[tokio::test]
async fn plugin_scalar_still_accepts_coerced_int_property() -> Result<()> {
    let db = uni_db::Uni::in_memory().build().await?;
    let loader = uni_plugin_rhai::RhaiLoader::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    db.load_rhai_plugin(&loader, IDF_PLUGIN, &caps)?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {freq: 41})").await?;
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc) RETURN mcscale.idf(toInteger(d.freq)) AS y")
        .await?;
    assert_eq!(res.rows()[0].get::<i64>("y")?, 42);
    Ok(())
}

/// REQ-4 failure scenario: a genuinely non-numeric property (a string) where an
/// integer is declared yields a precise, actionable error — not an opaque
/// downcast panic — naming the toInteger() hint.
#[tokio::test]
async fn plugin_scalar_non_numeric_property_errors_clearly() -> Result<()> {
    let db = uni_db::Uni::in_memory().build().await?;
    let loader = uni_plugin_rhai::RhaiLoader::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    db.load_rhai_plugin(&loader, IDF_PLUGIN, &caps)?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {tag: 'hello'})").await?;
    tx.commit().await?;

    let err = db
        .session()
        .query("MATCH (d:Doc) RETURN mcscale.idf(d.tag) AS y")
        .await
        .expect_err("string arg for an int-declared scalar must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("toInteger"),
        "error should hint at toInteger(); got: {msg}"
    );
    Ok(())
}
