// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Coverage gaps in the planner's `Expr` walkers, and what they cost.
//!
//! `contains_aggregate_recursive` and `collect_non_aggregate_refs` each
//! hand-rolled their `Expr` traversal and ended in `_ => {}`, so the newer
//! `MapProjection`, `ValidAt`, `LabelCheck` (and for the latter also `Map`,
//! `ArrayIndex`, `ArraySlice`) variants were never descended into. A map
//! projection therefore yielded no aggregate detection and no group keys.

use uni_db::{DataType, Uni};

async fn seeded() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("P")
        .property("name", DataType::String)
        .apply()
        .await
        .unwrap();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.execute("CREATE (:P {name: 'a'}), (:P {name: 'b'})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

/// A map projection with no aggregate is unaffected.
#[tokio::test]
async fn plain_map_projection_still_works() {
    let db = seeded().await;
    let result = db
        .session()
        .query("MATCH (n:P) RETURN n{.name}")
        .await
        .expect("plain map projection must plan");
    assert_eq!(result.len(), 2);
}

/// Mixing an aggregate into a map projection is not supported, but it must fail
/// as a *Cypher* error rather than leaking the physical planner's internals.
///
/// Before the walkers were fixed this produced
/// `DataFusion planning failed: Schema error: No field named "n.name"` — the
/// aggregate went undetected, so no grouping was built and the projection then
/// referenced a column the aggregate output did not carry. It now reports
/// `AmbiguousAggregationExpression`, which is actionable.
///
/// KNOWN LIMITATION: the equivalent flat form `RETURN n.name, count(*)` does
/// group and succeed (asserted below), so the two spellings still disagree.
/// Closing that needs the aggregate planner to treat a map projection's
/// non-aggregate entries as implicit group keys — a planner change, not a
/// walker change.
#[tokio::test]
async fn map_projection_with_aggregate_fails_as_a_cypher_error() {
    let db = seeded().await;
    let err = db
        .session()
        .query("MATCH (n:P) RETURN n{.name, c: count(*)}")
        .await
        .expect_err("mixing an aggregate into a map projection is not supported");
    let msg = err.to_string();
    assert!(
        msg.contains("AmbiguousAggregationExpression"),
        "expected a Cypher aggregation error, got: {msg}"
    );
    assert!(
        !msg.contains("DataFusion") && !msg.contains("No field named"),
        "physical-planner internals must not leak to the user: {msg}"
    );
}

/// The flat spelling groups by the non-aggregate reference, as Cypher requires.
#[tokio::test]
async fn flat_aggregate_projection_groups() {
    let db = seeded().await;
    let result = db
        .session()
        .query("MATCH (n:P) RETURN n.name, count(*)")
        .await
        .expect("flat aggregate projection must group");
    assert_eq!(result.len(), 2);
}

/// The narrowing that must survive the migration: an aggregate inside a
/// comprehension body is scoped to the comprehension, so it must NOT turn the
/// enclosing query into an aggregation.
#[tokio::test]
async fn aggregate_inside_comprehension_does_not_aggregate_outer_query() {
    let db = seeded().await;
    let result = db
        .session()
        .query("MATCH (n:P) RETURN n.name AS name, size([x IN [1,2,3] | x]) AS k ORDER BY name")
        .await
        .expect("comprehension body must not turn the outer query into an aggregation");
    assert_eq!(result.len(), 2, "must stay one row per matched node");
}
