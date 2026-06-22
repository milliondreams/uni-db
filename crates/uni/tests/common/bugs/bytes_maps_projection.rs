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
/// The `b{.*}` wildcard expands to `_map_project("__all__", b)`. The entity struct
/// carries a CypherValue-encoded `_all_props` map (lossless for raw `Bytes`)
/// alongside individually-decoded property columns (where a raw `Bytes` child
/// decodes to Null because `named_struct` drops the `uni_raw_bytes` marker).
/// `_map_project` now prefers `_all_props`, matching `properties()`.
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
