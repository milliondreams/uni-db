// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Probe + regression suite for the `DataType::Bytes` mis-decode surface —
//! AGGREGATE shapes (residual family of #93/#100).
//!
//! `Bytes`/`CypherValue`/`Duration` all map to Arrow `LargeBinary`; any aggregate
//! that re-materializes a `LargeBinary` element without the schema-type hint runs
//! raw `Bytes` through the tagged `cypher_value_codec`, which reads `byte[0]` as a
//! type tag and drops/corrupts the value. `collect()` was fixed in #100; this file
//! covers the remaining aggregate paths (`min`/`max`, `collect(DISTINCT)`,
//! `head`/`last` over a collected list).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration bytes_aggregates

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// First byte `0x61` ('a') is an unmapped codec tag; embedded NUL/0xFF make it
/// genuinely binary and must survive verbatim.
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
    Ok(db)
}

async fn insert_blob(db: &Uni, id: &str, data: &[u8]) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (b:Blob {id: $id, data: $d})")
        .param("id", Value::String(id.to_string()))
        .param("d", Value::Bytes(data.to_vec()))
        .run()
        .await?;
    tx.commit().await?;
    Ok(())
}

/// `min(b.data)` / `max(b.data)` over a single row must return that exact payload
/// (isolates the decode bug from any ordering ambiguity).
#[tokio::test]
async fn min_max_over_single_bytes_row() -> Result<()> {
    let db = blob_db().await?;
    insert_blob(&db, "b1", &payload()).await?;

    let res = db
        .session()
        .query("MATCH (b:Blob) RETURN min(b.data) AS lo, max(b.data) AS hi")
        .await?;
    let lo = res.rows()[0].value("lo").cloned();
    let hi = res.rows()[0].value("hi").cloned();
    assert_eq!(
        lo,
        Some(Value::Bytes(payload())),
        "min(b.data) corrupted: {lo:?}"
    );
    assert_eq!(
        hi,
        Some(Value::Bytes(payload())),
        "max(b.data) corrupted: {hi:?}"
    );
    Ok(())
}

/// `collect(DISTINCT b.data)` must round-trip the Bytes values (dedup uses string
/// repr; the elements must still be raw `Bytes`).
#[tokio::test]
async fn collect_distinct_over_bytes() -> Result<()> {
    let db = blob_db().await?;
    insert_blob(&db, "b1", &payload()).await?;
    insert_blob(&db, "b2", &payload()).await?; // duplicate payload

    let res = db
        .session()
        .query("MATCH (b:Blob) RETURN collect(DISTINCT b.data) AS items")
        .await?;
    let got = res.rows()[0].value("items").cloned();
    assert_eq!(
        got,
        Some(Value::List(vec![Value::Bytes(payload())])),
        "collect(DISTINCT) over Bytes corrupted: {got:?}"
    );
    Ok(())
}

/// `head(collect(b.data))` and `last(collect(b.data))` over the collected list.
#[tokio::test]
async fn head_last_over_collected_bytes() -> Result<()> {
    let db = blob_db().await?;
    insert_blob(&db, "b1", &payload()).await?;

    let res = db
        .session()
        .query("MATCH (b:Blob) WITH collect(b.data) AS l RETURN head(l) AS h, last(l) AS t")
        .await?;
    let h = res.rows()[0].value("h").cloned();
    let t = res.rows()[0].value("t").cloned();
    assert_eq!(
        h,
        Some(Value::Bytes(payload())),
        "head(collect) corrupted: {h:?}"
    );
    assert_eq!(
        t,
        Some(Value::Bytes(payload())),
        "last(collect) corrupted: {t:?}"
    );
    Ok(())
}
