// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression coverage for temporal type-loss in the property-map read path.
//!
//! A temporal stored in a node/edge property (`date(...)`, `datetime(...)`,
//! `duration(...)`) must read back as a `Value::Temporal`, not a stringified
//! `Value::String`, when it flows through the `_all_props` property-map path —
//! i.e. `RETURN n`, `RETURN properties(n)`, an array property, and edge maps.
//!
//! Root cause (RED today): the `_all_props` builders in
//! `uni-query/src/query/df_graph/{scan,traverse}.rs` rebuild property maps
//! through `serde_json::Value`, and `From<Value> for serde_json::Value`
//! (`uni-common/src/value.rs`) collapses `Value::Temporal(t)` to
//! `serde_json::Value::String(t.to_string())`. The storage codec and decode
//! side already preserve every temporal variant, so the loss is purely on the
//! encode side. The openCypher TCK cannot gate this (its comparison renders
//! temporals to strings), hence this direct engine repro.
//!
//! The matrix is checked in BOTH storage tiers: pre-`flush()` (the committed
//! main-L0 buffer) and post-`flush()` (durable L1), since the stringification
//! happens at map-build time regardless of tier.

use anyhow::Result;
use std::collections::HashMap;
use uni_common::Value;
use uni_common::value::TemporalValue;
use uni_db::{DataType, Uni};

fn assert_date(props: &HashMap<String, Value>, key: &str, ctx: &str) {
    match props.get(key) {
        Some(Value::Temporal(TemporalValue::Date { .. })) => {}
        other => panic!("{ctx}: property `{key}` must be Value::Temporal(Date), got {other:?}"),
    }
}

fn assert_datetime(props: &HashMap<String, Value>, key: &str, ctx: &str) {
    match props.get(key) {
        Some(Value::Temporal(TemporalValue::DateTime { .. })) => {}
        other => {
            panic!("{ctx}: property `{key}` must be Value::Temporal(DateTime), got {other:?}")
        }
    }
}

fn assert_duration(props: &HashMap<String, Value>, key: &str, ctx: &str) {
    match props.get(key) {
        Some(Value::Temporal(TemporalValue::Duration { .. })) => {}
        other => {
            panic!("{ctx}: property `{key}` must be Value::Temporal(Duration), got {other:?}")
        }
    }
}

/// Assert every read surface returns typed temporals for the graph created by
/// [`seed`]. `tier` labels which storage tier (L0 / L1) is being exercised.
async fn check_typed(db: &Uni, tier: &str) -> Result<()> {
    // 1. Embedded node map: `RETURN n` → Value::Node with a typed property map.
    let res = db
        .session()
        .query("MATCH (n:E {name: 'x'}) RETURN n")
        .await?;
    let node = match res.rows()[0].value("n").unwrap() {
        Value::Node(n) => n,
        other => panic!("{tier}: expected Value::Node, got {other:?}"),
    };
    assert_date(&node.properties, "d", &format!("{tier} RETURN n"));
    assert_datetime(&node.properties, "ts", &format!("{tier} RETURN n"));
    assert_duration(&node.properties, "dur", &format!("{tier} RETURN n"));
    // 1b. Array property: a List of typed Dates (overflow / CypherValue path).
    match node.properties.get("tags") {
        Some(Value::List(items)) => {
            assert_eq!(items.len(), 2, "{tier}: tags length");
            for (i, item) in items.iter().enumerate() {
                assert!(
                    matches!(item, Value::Temporal(TemporalValue::Date { .. })),
                    "{tier}: tags[{i}] must be Value::Temporal(Date), got {item:?}",
                );
            }
        }
        other => panic!("{tier}: property `tags` must be a List of Dates, got {other:?}"),
    }

    // 2. `properties(n)` UDF → Value::Map with typed values.
    let res = db
        .session()
        .query("MATCH (n:E {name: 'x'}) RETURN properties(n) AS p")
        .await?;
    let map = match res.rows()[0].value("p").unwrap() {
        Value::Map(m) => m,
        other => panic!("{tier}: properties(n) must be a Map, got {other:?}"),
    };
    assert_date(map, "d", &format!("{tier} properties(n)"));
    assert_datetime(map, "ts", &format!("{tier} properties(n)"));
    assert_duration(map, "dur", &format!("{tier} properties(n)"));

    // 3. Single-property projection `RETURN n.d`.
    let res = db
        .session()
        .query("MATCH (n:E {name: 'x'}) RETURN n.d AS d")
        .await?;
    assert!(
        matches!(
            res.rows()[0].value("d").unwrap(),
            Value::Temporal(TemporalValue::Date { .. })
        ),
        "{tier}: n.d must be Value::Temporal(Date), got {:?}",
        res.rows()[0].value("d"),
    );

    // 4. Edge property map: `RETURN r` → Value::Edge with a typed property map.
    let res = db
        .session()
        .query("MATCH (:E {name: 'x'})-[r:R]->(:E {name: 'y'}) RETURN r")
        .await?;
    let edge = match res.rows()[0].value("r").unwrap() {
        Value::Edge(e) => e,
        other => panic!("{tier}: expected Value::Edge, got {other:?}"),
    };
    assert_datetime(&edge.properties, "at", &format!("{tier} RETURN r"));

    Ok(())
}

async fn seed(db: &Uni) -> Result<()> {
    db.schema()
        .label("E")
        .property("name", DataType::String)
        .property_nullable("d", DataType::Date)
        .property_nullable("ts", DataType::DateTime)
        .property_nullable("dur", DataType::Duration)
        .edge_type("R", &["E"], &["E"])
        .property("at", DataType::DateTime)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:E {name: 'x', d: date('1984-10-11'), \
         ts: datetime('1984-10-11T12:00:00Z'), dur: duration('PT1H'), \
         tags: [date('2001-01-01'), date('2002-02-02')]})",
    )
    .await?;
    tx.execute("CREATE (:E {name: 'y'})").await?;
    tx.execute(
        "MATCH (a:E {name: 'x'}), (b:E {name: 'y'}) \
         CREATE (a)-[:R {at: datetime('1999-12-31T23:59:59Z')}]->(b)",
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Temporals in property maps stay typed across both storage tiers.
#[tokio::test]
async fn temporal_node_and_edge_properties_preserve_type() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(&db).await?;

    // L0: committed data still resident in the main L0 buffer.
    check_typed(&db, "L0").await?;

    // L1: force the data out of L0 into durable storage, then re-check.
    db.flush().await?;
    check_typed(&db, "L1").await?;

    Ok(())
}
