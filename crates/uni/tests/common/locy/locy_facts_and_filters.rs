// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A Locy rule's facts are rows, its `QUERY` filter is three-valued, and a sum
//! of nothing is 0.
//!
//! * A derived fact is a row (reference §6.2): two derivations of the same row
//!   are one fact. The recursive fixpoint deduplicated through its delta
//!   computation; a non-recursive rule, evaluated in one pass, did not, so three
//!   parallel edges derived `(a, b)` three times.
//! * `QUERY ... WHERE` runs in Locy's in-memory evaluator, where `NULL = x` was
//!   false and `NULL <> x` true, and `AND`/`OR` with a NULL side were NULL even
//!   when the other side decides them. So `QUERY r WHERE a.age <> 0` kept rows
//!   whose `age` is NULL, which the same filter in a rule body (and in Cypher)
//!   rejects.
//! * `SUM` / `MSUM` over a group with no non-null value is 0.0, as Cypher's
//!   `sum` is; it was NULL.
//!
//! Found by the `locy_rule`, `locy_reach` and `locy_fold` relations of
//! `metamorphic::dqp::topo` (W4).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(locy_facts_and_filters)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("id", DataType::Int)
        .property_nullable("age", DataType::Int)
        .edge_type("K", &["P"], &["P"])
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:P {id: 1, age: 30}), (b:P {id: 2}), \
         (a)-[:K]->(b), (a)-[:K]->(b), (a)-[:K]->(b), (a)-[:K]->(a)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

async fn rows(db: &Uni, program: &str) -> Result<Vec<Vec<Value>>> {
    let result = db.session().locy(program).await?;
    let rows = result
        .command_results()
        .iter()
        .find_map(|c| c.as_query())
        .expect("program has a QUERY");
    let mut out: Vec<Vec<Value>> = rows
        .iter()
        .map(|row| {
            let mut columns: Vec<_> = row.iter().collect();
            columns.sort_by_key(|(name, _)| (*name).clone());
            columns.into_iter().map(|(_, v)| v.clone()).collect()
        })
        .collect();
    out.sort_by_key(|r| format!("{r:?}"));
    Ok(out)
}

#[tokio::test]
async fn locy_facts_and_filters_a_fact_is_a_row() -> Result<()> {
    let db = open().await?;
    let program = "CREATE RULE r AS MATCH (a:P)-[:K]->(b:P) YIELD KEY a, KEY b \
                   QUERY r RETURN a.id AS c0, b.id AS c1";
    assert_eq!(
        rows(&db, program).await?,
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(1), Value::Int(2)],
        ]
    );
    let result = db.session().locy(program).await?;
    assert_eq!(result.derived_facts("r").map(Vec::len), Some(2));
    // A FOLD still counts every derivation.
    assert_eq!(
        rows(
            &db,
            "CREATE RULE n AS MATCH (a:P)-[:K]->(b:P) WHERE b.id = 2 FOLD c = COUNT(*) \
             YIELD KEY a, c QUERY n RETURN c AS c0"
        )
        .await?,
        vec![vec![Value::Int(3)]]
    );
    Ok(())
}

#[tokio::test]
async fn locy_facts_and_filters_query_where_is_three_valued() -> Result<()> {
    let db = open().await?;
    let base = "CREATE RULE r AS MATCH (a:P) YIELD KEY a ";
    let one = vec![vec![Value::Int(1)]];
    for (filter, want) in [
        ("a.age <> 0", one.clone()),
        ("NOT (a.age = 99)", one.clone()),
        ("(a.age <> 0) AND (a.id > 0)", one.clone()),
        ("a.age = 30 OR a.age = 31", one.clone()),
        // `false AND NULL` is false and `true OR NULL` is true.
        (
            "NOT (a.id = 1 AND a.age = 0)",
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        ),
        (
            "a.id = 2 OR a.age = 30",
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        ),
        ("a.age IS NULL", vec![vec![Value::Int(2)]]),
    ] {
        let program = format!("{base}QUERY r WHERE {filter} RETURN a.id AS c0");
        assert_eq!(rows(&db, &program).await?, want, "{filter}");
    }
    Ok(())
}

#[tokio::test]
async fn locy_facts_and_filters_sum_of_nothing_is_zero() -> Result<()> {
    let db = open().await?;
    for agg in ["SUM", "MSUM"] {
        let program = format!(
            "CREATE RULE s AS MATCH (a:P)-[:K]->(b:P) WHERE b.id = 2 FOLD t = {agg}(b.age) \
             YIELD KEY a, t QUERY s RETURN t AS c0"
        );
        assert_eq!(
            rows(&db, &program).await?,
            vec![vec![Value::Float(0.0)]],
            "{agg}"
        );
    }
    Ok(())
}
