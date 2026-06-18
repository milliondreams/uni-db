// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression: a dynamic-loader plugin whose id itself contains dots (a
//! reverse-DNS id like `ai.example.geo`) must resolve its scalars, aggregates,
//! AND procedures when referenced by qualified name in Cypher.
//!
//! # Bug
//!
//! Dynamic loaders register `QName(namespace = full plugin id, local = name)`,
//! so `ai.example.geo` + `gsum` ⇒ `QName("ai.example.geo", "gsum")`. But the
//! Cypher aggregate planner and the procedure resolver split the *user* name on
//! the FIRST dot (`split_once('.')`), producing `("ai", "example.geo.gsum")` —
//! which never matches the registered qname. Scalars were unaffected (they
//! register under the full `qname.to_string()` and resolve by exact match).
//!
//! # Fix
//!
//! Resolution now tries every namespace/local split via
//! `QName::candidate_splits` (first-dot → last-dot), so the last-dot split
//! `("ai.example.geo", "gsum")` is found. The first-dot M9/builtin/apoc
//! convention (single-segment namespace + dotted local) still resolves because
//! its split is also among the candidates.

#![cfg(feature = "rhai-plugins")]

use anyhow::Result;
use uni_plugin::{Capability, CapabilitySet};

/// A plugin with a DOTTED id exporting one of each callable kind.
const GEO_PLUGIN: &str = r#"
    fn uni_manifest() {
        #{
            id: "ai.example.geo",
            version: "0.1.0",
            determinism: "pure",
            scalar_fns: [
                #{ name: "dbl", args: ["float"], returns: "float" },
            ],
            aggregate_fns: [
                #{ name: "gsum", args: ["float"], returns: "float", state: "map" },
            ],
            procedures: [
                #{ name: "answer", args: [], yields: ["int"], mode: "read" },
            ],
        }
    }
    fn dbl(x) { x * 2.0 }
    fn gsum_init() { 0.0 }
    fn gsum_accumulate(s, x) { s + x }
    fn gsum_merge(a, b) { a + b }
    fn gsum_finalize(s) { s }
    fn answer() { [ #{ col0: 42 } ] }
"#;

async fn db_with_geo_plugin() -> Result<uni_db::Uni> {
    let db = uni_db::Uni::in_memory().build().await?;
    let loader = uni_plugin_rhai::RhaiLoader::new();
    let caps = CapabilitySet::from_iter_of([
        Capability::ScalarFn,
        Capability::AggregateFn,
        Capability::Procedure,
    ]);
    let outcome = db.load_rhai_plugin(&loader, GEO_PLUGIN, &caps)?;
    assert_eq!(outcome.plugin_id.as_str(), "ai.example.geo");
    Ok(db)
}

/// SCALAR under a dotted id (already worked — guards against regression).
#[tokio::test]
async fn dotted_id_scalar_resolves_in_cypher() -> Result<()> {
    let db = db_with_geo_plugin().await?;
    let res = db
        .session()
        .query("RETURN ai.example.geo.dbl(2.5) AS d")
        .await?;
    assert!((res.rows()[0].get::<f64>("d")? - 5.0).abs() < 1e-9);
    Ok(())
}

/// AGGREGATE under a dotted id — the core fix. Before, this errored
/// `Unsupported aggregate function: ai.example.geo.gsum`.
#[tokio::test]
async fn dotted_id_aggregate_resolves_in_cypher() -> Result<()> {
    let db = db_with_geo_plugin().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:X {v: 1}), (:X {v: 2}), (:X {v: 3})")
        .await?;
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (n:X) RETURN ai.example.geo.gsum(toFloat(n.v)) AS s")
        .await?;
    assert!(
        (res.rows()[0].get::<f64>("s")? - 6.0).abs() < 1e-9,
        "dotted-id aggregate did not resolve/compute; got {}",
        res.rows()[0].get::<f64>("s")?
    );
    Ok(())
}

/// PROCEDURE under a dotted id — the other half of the fix. Before, the
/// procedure resolver's first-dot split failed and the CALL was unknown.
#[tokio::test]
async fn dotted_id_procedure_resolves_in_cypher() -> Result<()> {
    let db = db_with_geo_plugin().await?;
    let res = db
        .session()
        .query("CALL ai.example.geo.answer() YIELD col0 RETURN col0 AS x")
        .await?;
    assert_eq!(
        res.rows()[0].get::<i64>("x")?,
        42,
        "dotted-id procedure did not resolve"
    );
    Ok(())
}

/// Regression: a SINGLE-SEGMENT plugin id still resolves its aggregate (the
/// first-dot candidate is tried first, unchanged behavior).
#[tokio::test]
async fn single_segment_id_aggregate_still_resolves() -> Result<()> {
    let db = uni_db::Uni::in_memory().build().await?;
    let loader = uni_plugin_rhai::RhaiLoader::new();
    let caps = CapabilitySet::from_iter_of([
        Capability::ScalarFn,
        Capability::AggregateFn,
        Capability::Procedure,
    ]);
    let script = GEO_PLUGIN.replace("ai.example.geo", "geo");
    db.load_rhai_plugin(&loader, &script, &caps)?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:X {v: 10}), (:X {v: 20})").await?;
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (n:X) RETURN geo.gsum(toFloat(n.v)) AS s")
        .await?;
    assert!((res.rows()[0].get::<f64>("s")? - 30.0).abs() < 1e-9);
    Ok(())
}
