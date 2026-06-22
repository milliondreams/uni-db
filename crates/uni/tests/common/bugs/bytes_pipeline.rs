// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Probe + regression suite for the `DataType::Bytes` mis-decode surface —
//! PIPELINE shapes (`WITH` aliasing, `UNWIND`, `reduce`, list comprehension,
//! `GROUP BY` key, `ORDER BY`).
//!
//! These exercise the value as it flows across projection boundaries and through
//! the fallback interpreter, to confirm the schema hint survives each hop.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration bytes_pipeline

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

/// Multi-hop `WITH ... AS` aliasing must carry the Bytes value through.
#[tokio::test]
async fn multi_hop_with_aliasing() -> Result<()> {
    let db = blob_db().await?;
    let res = db
        .session()
        .query("MATCH (b:Blob) WITH b.data AS d WITH d AS e RETURN e")
        .await?;
    let got = res.rows()[0].value("e").cloned();
    assert_eq!(
        got,
        Some(Value::Bytes(payload())),
        "multi-hop WITH dropped Bytes: {got:?}"
    );
    Ok(())
}

/// `UNWIND` over a collected Bytes list yields each Bytes value back.
#[tokio::test]
async fn unwind_collected_bytes() -> Result<()> {
    let db = blob_db().await?;
    let res = db
        .session()
        .query("MATCH (b:Blob) WITH collect(b.data) AS l UNWIND l AS d RETURN d")
        .await?;
    let got = res.rows()[0].value("d").cloned();
    assert_eq!(
        got,
        Some(Value::Bytes(payload())),
        "UNWIND of collected Bytes dropped value: {got:?}"
    );
    Ok(())
}

/// List comprehension over a collected Bytes list.
///
/// The comprehension list-materialization path (`cv_array_to_large_list` /
/// `large_list_of_cv_to_cv_array`) stays in `Value`-space rather than detouring
/// through `serde_json` (which base64-stringifies raw `Bytes`). Only the
/// `LargeBinary`-element branches were converted; the typed primitive and Struct
/// branches keep their builders, so VLP/pattern-comprehension projection is intact.
#[tokio::test]
async fn list_comprehension_over_bytes() -> Result<()> {
    let db = blob_db().await?;
    let res = db
        .session()
        .query("MATCH (b:Blob) WITH collect(b.data) AS l RETURN [x IN l | x] AS out")
        .await?;
    let got = res.rows()[0].value("out").cloned();
    assert_eq!(
        got,
        Some(Value::List(vec![Value::Bytes(payload())])),
        "list comprehension over Bytes corrupted: {got:?}"
    );
    Ok(())
}

/// `GROUP BY` a Bytes key: the returned grouping key must be the raw Bytes.
#[tokio::test]
async fn group_by_bytes_key() -> Result<()> {
    let db = blob_db().await?;
    let res = db
        .session()
        .query("MATCH (b:Blob) RETURN b.data AS k, count(*) AS n")
        .await?;
    let k = res.rows()[0].value("k").cloned();
    let n = res.rows()[0].value("n").cloned();
    assert_eq!(
        k,
        Some(Value::Bytes(payload())),
        "GROUP BY Bytes key corrupted: {k:?}"
    );
    assert_eq!(n, Some(Value::Int(1)), "count wrong: {n:?}");
    Ok(())
}

/// `ORDER BY b.data` then return it — value intact after sort.
#[tokio::test]
async fn order_by_bytes() -> Result<()> {
    let db = blob_db().await?;
    let res = db
        .session()
        .query("MATCH (b:Blob) RETURN b.data AS d ORDER BY b.data")
        .await?;
    let got = res.rows()[0].value("d").cloned();
    assert_eq!(
        got,
        Some(Value::Bytes(payload())),
        "ORDER BY dropped Bytes: {got:?}"
    );
    Ok(())
}
