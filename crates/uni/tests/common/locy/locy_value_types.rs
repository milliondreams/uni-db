// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A Locy rule keeps its values' types.
//!
//! The planner typed every `FOLD` output except `COUNT` as Float64, and every
//! property it could not place on a label (an unlabelled node, any
//! relationship) as Float64 too, casting the rule body's values before they
//! reached the aggregate. So `MIN(b.id)` over integers returned `1.0`,
//! `COLLECT(b.id)` a list of floats, `b.id AS bid` on an unlabelled `b` and
//! `r.w AS w` returned floats — and above 2^53 the value itself was wrong.
//! `MIN`/`MAX` (and `MMIN`/`MMAX`) now take their argument's type, `COLLECT`'s
//! input is not cast, and a property whose every schema declaration agrees
//! takes that type.
//!
//! Found by the `locy_fold` relation of `metamorphic::dqp::topo` (W4).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(locy_value_types)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Above 2^53, where an `f64` cannot hold every integer.
const BIG: i64 = 9_007_199_254_740_993;

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("id", DataType::Int)
        .property("big", DataType::Int)
        .edge_type("K", &["P"], &["P"])
        .property("w", DataType::Int)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "CREATE (a:P {{id: 1, big: 0}}), (b:P {{id: 2, big: {BIG}}}), (c:P {{id: 3, big: 5}}), \
         (a)-[:K {{w: 7}}]->(b), (a)-[:K {{w: 4}}]->(c), (b)-[:K {{w: 1}}]->(c)"
    ))
    .await?;
    tx.commit().await?;
    Ok(db)
}

async fn query_row(db: &Uni, program: &str) -> Result<Vec<Value>> {
    let result = db.session().locy(program).await?;
    let rows = result
        .command_results()
        .iter()
        .find_map(|c| c.as_query())
        .expect("program has a QUERY");
    assert_eq!(rows.len(), 1, "{program}");
    let mut columns: Vec<_> = rows[0].iter().collect();
    columns.sort_by_key(|(name, _)| (*name).clone());
    Ok(columns.into_iter().map(|(_, v)| v.clone()).collect())
}

#[tokio::test]
async fn locy_value_types_fold_min_max_collect() -> Result<()> {
    let db = open().await?;
    // Labelled and unlabelled targets alike.
    for target in ["(b:P)", "(b)"] {
        let program = format!(
            "CREATE RULE f AS MATCH (a:P)-[:K]->{target} \
             FOLD lo = MIN(b.id), hi = MAX(b.big), c = COLLECT(b.id) YIELD KEY a, lo, hi, c \
             QUERY f WHERE a.id = 1 RETURN lo AS c0, hi AS c1, c AS c2"
        );
        let row = query_row(&db, &program).await?;
        assert_eq!(row[0], Value::Int(2), "{program}");
        assert_eq!(row[1], Value::Int(BIG), "{program}");
        let Value::List(mut ids) = row[2].clone() else {
            panic!("COLLECT returned {:?}", row[2]);
        };
        ids.sort_by_key(|v| format!("{v:?}"));
        assert_eq!(ids, vec![Value::Int(2), Value::Int(3)], "{program}");
    }
    Ok(())
}

#[tokio::test]
async fn locy_value_types_yielded_properties() -> Result<()> {
    let db = open().await?;
    // An unlabelled node's property and a relationship's, without any FOLD.
    let row = query_row(
        &db,
        "CREATE RULE f AS MATCH (a:P)-[r:K]->(b) YIELD KEY a, KEY b, b.big AS big, r.w AS w \
         QUERY f WHERE b.id = 2 RETURN big AS c0, w AS c1",
    )
    .await?;
    assert_eq!(row, vec![Value::Int(BIG), Value::Int(7)]);
    let row = query_row(
        &db,
        "CREATE RULE f AS MATCH (a:P)-[r:K]->(b:P) FOLD hi = MAX(r.w) YIELD KEY a, hi \
         QUERY f WHERE a.id = 1 RETURN hi AS c0",
    )
    .await?;
    assert_eq!(row, vec![Value::Int(7)]);
    Ok(())
}

/// A recursive rule's monotone `MMAX`, over the reach of a node.
#[tokio::test]
async fn locy_value_types_recursive_max() -> Result<()> {
    let db = open().await?;
    let row = query_row(
        &db,
        "CREATE RULE reach AS MATCH (a)-[:K]->(b) YIELD KEY a, KEY b \
         CREATE RULE reach AS MATCH (a)-[:K]->(m) WHERE m IS reach TO b YIELD KEY a, KEY b \
         CREATE RULE far AS MATCH (a:P) WHERE a IS reach TO b FOLD hi = MAX(b.big) YIELD KEY a, hi \
         QUERY far WHERE a.id = 1 RETURN hi AS c0",
    )
    .await?;
    assert_eq!(row, vec![Value::Int(BIG)]);
    Ok(())
}
