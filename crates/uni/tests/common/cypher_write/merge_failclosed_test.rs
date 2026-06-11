// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Regression for the 2026-06-10 review bug #3a: the single-node MERGE fast path
// diverged from the general path in ways that silently created duplicate nodes
// where it should have matched an existing one.
//
//   * Label case: the fast path keyed L0 / the persisted scan on the *raw*
//     label, while the general path matches labels case-insensitively. So
//     `MERGE (:person …)` against an existing `:Person` row missed and created a
//     duplicate.
//   * Numeric key type: match keys used derived `Value` equality, so `Int(1)`
//     did not match a node stored with `Float(1.0)` (and vice versa), creating a
//     duplicate.
//
// MERGE's contract is match-or-create: a second MERGE with an equivalent key
// must match the first node, never duplicate it.
// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

async fn node_count(db: &Uni, label: &str) -> Result<i64> {
    let cypher = format!("MATCH (n:{label}) RETURN count(n) AS c");
    let r = db.session().query(&cypher).await?;
    Ok(r.rows()[0].get::<i64>("c").unwrap())
}

/// `MERGE (:person {...})` must match a previously-created `:Person` node — the
/// fast path now resolves the label to its schema-canonical case.
#[tokio::test]
async fn merge_matches_across_label_case() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'alice'})").await?;
    tx.commit().await?;

    // Different label case — must MATCH the existing node, not create a new one.
    let tx = db.session().tx().await?;
    let res = tx.execute("MERGE (:person {name: 'alice'})").await?;
    tx.commit().await?;
    assert_eq!(
        res.nodes_created(),
        0,
        "MERGE with a different-case label must match, not create"
    );

    assert_eq!(
        node_count(&db, "Person").await?,
        1,
        "exactly one Person node must exist (no case-divergent duplicate)"
    );
    Ok(())
}

/// `MERGE (:T {v: 1})` (an `Int` key) must match a node created with the
/// equivalent `Float(1.0)` — numeric match keys are canonicalized.
#[tokio::test]
async fn merge_matches_across_int_float_key() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("T")
        .property_nullable("v", DataType::Float64)
        .done()
        .apply()
        .await?;

    // Create with a float-valued key.
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:T {v: $v})")
        .param("v", Value::Float(1.0))
        .run()
        .await?;
    tx.commit().await?;

    // MERGE with an integer-valued key for the same numeric value.
    let tx = db.session().tx().await?;
    let res = tx
        .execute_with("MERGE (:T {v: $v})")
        .param("v", Value::Int(1))
        .run()
        .await?;
    tx.commit().await?;
    assert_eq!(
        res.nodes_created(),
        0,
        "MERGE with an Int key must match a node stored with the equal Float value"
    );

    assert_eq!(
        node_count(&db, "T").await?,
        1,
        "exactly one T node must exist (no Int/Float-divergent duplicate)"
    );
    Ok(())
}
