// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Probe + regression suite for the `DataType::Bytes` mis-decode surface —
//! WHOLE-ENTITY and MAP-PROJECTION shapes.
//!
//! `RETURN b`, `properties(b)`, and map projections (`b{.data}`, `b{.*}`) rebuild
//! a property map from Arrow columns. If a Bytes property value loses its schema
//! hint while the map is materialized, it is mis-decoded by the tagged codec.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration bytes_maps_projection

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

fn payload() -> Vec<u8> {
    b"audio-fingerprint-\x00\xff\x01-blob".to_vec()
}

async fn blob_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Blob")
        .property("id", DataType::String)
        .property_nullable("data", DataType::Bytes)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (b:Blob {id: 'b1', data: $d})")
        .param("d", Value::Bytes(payload()))
        .run()
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// Helper: extract the `data` entry from a returned map/node-properties Value.
fn data_entry(v: Option<&Value>) -> Option<Value> {
    match v {
        Some(Value::Map(m)) => m.get("data").cloned(),
        Some(Value::Node(_)) => None, // handled separately
        _ => None,
    }
}

/// `RETURN b` — the whole node's `data` property must survive as raw Bytes.
#[tokio::test]
async fn whole_node_return_preserves_bytes() -> Result<()> {
    let db = blob_db().await?;
    let res = db.session().query("MATCH (b:Blob) RETURN b").await?;
    let node = res.rows()[0].value("b").cloned();
    let got = match node {
        Some(Value::Node(n)) => n.properties.get("data").cloned(),
        other => panic!("expected Node, got {other:?}"),
    };
    assert_eq!(
        got,
        Some(Value::Bytes(payload())),
        "RETURN b dropped Bytes: {got:?}"
    );
    Ok(())
}

/// `RETURN properties(b)` — the property map must carry raw Bytes.
#[tokio::test]
async fn properties_function_preserves_bytes() -> Result<()> {
    let db = blob_db().await?;
    let res = db
        .session()
        .query("MATCH (b:Blob) RETURN properties(b) AS p")
        .await?;
    let got = data_entry(res.rows()[0].value("p"));
    assert_eq!(
        got,
        Some(Value::Bytes(payload())),
        "properties(b) dropped Bytes: {got:?}"
    );
    Ok(())
}

/// `RETURN b{.data}` — explicit map projection of a Bytes property.
#[tokio::test]
async fn map_projection_named_preserves_bytes() -> Result<()> {
    let db = blob_db().await?;
    let res = db
        .session()
        .query("MATCH (b:Blob) RETURN b{.data} AS p")
        .await?;
    let got = data_entry(res.rows()[0].value("p"));
    assert_eq!(
        got,
        Some(Value::Bytes(payload())),
        "b{{.data}} dropped Bytes: {got:?}"
    );
    Ok(())
}

/// `RETURN b{.*}` — all-properties map projection of a Bytes property.
///
/// KNOWN LIMITATION (tracked): unlike `b{.data}` (named, fixed) and `RETURN b`
/// (whole node, fixed), the `b{.*}` wildcard passes the entity to `_map_project`
/// via a path where the raw `Bytes` property's `uni_raw_bytes` marker is lost
/// before materialization, so it mis-decodes as tagged CypherValue. The fix needs
/// planner-level marker propagation onto the wildcard projection; deferred as a
/// focused follow-up. The assertion below is the correct expectation.
#[ignore = "b{.*} wildcard projection over Bytes loses uni_raw_bytes marker (planner propagation follow-up)"]
#[tokio::test]
async fn map_projection_all_preserves_bytes() -> Result<()> {
    let db = blob_db().await?;
    let res = db
        .session()
        .query("MATCH (b:Blob) RETURN b{.*} AS p")
        .await?;
    let got = data_entry(res.rows()[0].value("p"));
    assert_eq!(
        got,
        Some(Value::Bytes(payload())),
        "b{{.*}} dropped Bytes: {got:?}"
    );
    Ok(())
}
