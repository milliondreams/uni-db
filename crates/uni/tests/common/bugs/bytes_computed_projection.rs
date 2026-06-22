// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression suite for the general projection-output `uni_raw_bytes` propagation —
//! a raw `DataType::Bytes` value flowing through a COMPUTED projection expression.
//!
//! `Bytes`/`CypherValue`/`Duration` all serialize to Arrow `LargeBinary` and are
//! disambiguated only by the `uni_raw_bytes` field-metadata marker. A plain column
//! passthrough keeps its marker, but a computed expression (`coalesce`/`CASE` →
//! DataFusion `CaseExpr`, list literal → `make_array`, `head`/`last`/`index` UDFs)
//! produces an output field with no marker, so raw bytes were mis-decoded. The marker
//! is now propagated: scalar raw-bytes outputs (coalesce/CASE) are marked at the
//! projection, and raw-bytes list literals get a marked child at compile time (fixing
//! both direct returns and the list-consumer UDFs that decode them internally).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration bytes_computed_projection

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

fn payload() -> Vec<u8> {
    b"audio-fingerprint-\x00\xff\x01-blob".to_vec()
}
fn payload2() -> Vec<u8> {
    vec![0x00, 0xDE, 0xAD, 0xBE, 0xEF] // byte[0] = TAG_NULL (silent-corruption case)
}

async fn blob_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Blob")
        .property("id", DataType::String)
        .property_nullable("data", DataType::Bytes)
        .property_nullable("data2", DataType::Bytes)
        .property_nullable("name", DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (b:Blob {id: 'b1', data: $d, data2: $d2, name: 'n1'})")
        .param("d", Value::Bytes(payload()))
        .param("d2", Value::Bytes(payload2()))
        .run()
        .await?;
    tx.commit().await?;
    Ok(db)
}

async fn scalar_bytes(db: &Uni, query: &str, col: &str) -> Option<Value> {
    let res = db.session().query(query).await.unwrap();
    res.rows()[0].value(col).cloned()
}

// ---------------------------------------------------------------------------
// coalesce / CASE (scalar raw-bytes output → marked at the projection)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coalesce_missing_then_bytes() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(
        &db,
        "MATCH (b:Blob) RETURN coalesce(b.missing, b.data) AS v",
        "v",
    )
    .await;
    assert_eq!(got, Some(Value::Bytes(payload())), "coalesce(missing,data): {got:?}");
    Ok(())
}

#[tokio::test]
async fn coalesce_null_then_bytes() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN coalesce(NULL, b.data) AS v", "v").await;
    assert_eq!(got, Some(Value::Bytes(payload())), "coalesce(NULL,data): {got:?}");
    Ok(())
}

#[tokio::test]
async fn coalesce_bytes_then_bytes() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN coalesce(b.data, b.data2) AS v", "v").await;
    assert_eq!(got, Some(Value::Bytes(payload())), "coalesce(data,data2): {got:?}");
    Ok(())
}

#[tokio::test]
async fn coalesce_two_missing_then_bytes() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(
        &db,
        "MATCH (b:Blob) RETURN coalesce(b.missing, b.missing2, b.data) AS v",
        "v",
    )
    .await;
    assert_eq!(got, Some(Value::Bytes(payload())), "coalesce(missing,missing2,data): {got:?}");
    Ok(())
}

#[tokio::test]
async fn case_then_bytes() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(
        &db,
        "MATCH (b:Blob) RETURN CASE WHEN b.name = 'n1' THEN b.data ELSE b.data2 END AS v",
        "v",
    )
    .await;
    assert_eq!(got, Some(Value::Bytes(payload())), "CASE then-branch: {got:?}");
    Ok(())
}

#[tokio::test]
async fn case_else_bytes() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(
        &db,
        "MATCH (b:Blob) RETURN CASE WHEN b.name = 'zzz' THEN b.data ELSE b.data2 END AS v",
        "v",
    )
    .await;
    assert_eq!(got, Some(Value::Bytes(payload2())), "CASE else-branch: {got:?}");
    Ok(())
}

// ---------------------------------------------------------------------------
// list literal + list-extractor UDFs (make_array child marked at compile time)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn head_of_list_literal() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN head([b.data]) AS v", "v").await;
    assert_eq!(got, Some(Value::Bytes(payload())), "head([b.data]): {got:?}");
    Ok(())
}

#[tokio::test]
async fn last_of_list_literal() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN last([b.data]) AS v", "v").await;
    assert_eq!(got, Some(Value::Bytes(payload())), "last([b.data]): {got:?}");
    Ok(())
}

#[tokio::test]
async fn index_into_list_literal() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN [b.data][0] AS v", "v").await;
    assert_eq!(got, Some(Value::Bytes(payload())), "[b.data][0]: {got:?}");
    Ok(())
}

#[tokio::test]
async fn index_into_two_element_list_literal() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN [b.data, b.data2][1] AS v", "v").await;
    assert_eq!(got, Some(Value::Bytes(payload2())), "[b.data,b.data2][1]: {got:?}");
    Ok(())
}

#[tokio::test]
async fn head_of_two_element_list_literal() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN head([b.data, b.data2]) AS v", "v").await;
    assert_eq!(got, Some(Value::Bytes(payload())), "head([b.data,b.data2]): {got:?}");
    Ok(())
}

#[tokio::test]
async fn direct_list_literal_return() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN [b.data] AS v", "v").await;
    assert_eq!(
        got,
        Some(Value::List(vec![Value::Bytes(payload())])),
        "RETURN [b.data]: {got:?}"
    );
    Ok(())
}

#[tokio::test]
async fn direct_two_element_list_literal_return() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN [b.data, b.data2] AS v", "v").await;
    assert_eq!(
        got,
        Some(Value::List(vec![Value::Bytes(payload()), Value::Bytes(payload2())])),
        "RETURN [b.data,b.data2]: {got:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// WITH-chaining (marker survives intermediate projections via Column passthrough)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn with_chain_coalesce_then_passthrough() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(
        &db,
        "MATCH (b:Blob) WITH coalesce(b.missing, b.data) AS v RETURN v",
        "v",
    )
    .await;
    assert_eq!(got, Some(Value::Bytes(payload())), "WITH coalesce..AS v RETURN v: {got:?}");
    Ok(())
}

#[tokio::test]
async fn with_chain_passthrough_then_coalesce() -> Result<()> {
    let db = blob_db().await?;
    // `v` (a WITH-bound raw-Bytes column) flows into a coalesce; the marker must
    // survive the WITH projection and the coalesce output must round-trip.
    let got = scalar_bytes(
        &db,
        "MATCH (b:Blob) WITH b.data AS v RETURN coalesce(NULL, v) AS w",
        "w",
    )
    .await;
    assert_eq!(got, Some(Value::Bytes(payload())), "WITH b.data AS v RETURN coalesce(NULL,v): {got:?}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Negative / consistency: no false marking, correct round-trip
// ---------------------------------------------------------------------------

/// Mixed-type coalesce (`Bytes` + `String`): the `b.data` branch wins and must come
/// back as `Bytes` without corruption (Part D makes the unified column CV-decodable).
#[tokio::test]
async fn coalesce_bytes_then_string_consistent() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN coalesce(b.data, 'fallback') AS v", "v").await;
    assert_eq!(got, Some(Value::Bytes(payload())), "coalesce(data,'str'): {got:?}");
    Ok(())
}

/// `coalesce(String, Bytes)` where the string wins → must return the String (no
/// false bytes-marking of a mixed column).
#[tokio::test]
async fn coalesce_string_then_bytes_returns_string() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN coalesce(b.name, b.data) AS v", "v").await;
    assert_eq!(got, Some(Value::String("n1".to_string())), "coalesce(name,data): {got:?}");
    Ok(())
}

/// Pure non-bytes coalesce is unaffected.
#[tokio::test]
async fn coalesce_pure_string() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN coalesce(b.missing, b.name) AS v", "v").await;
    assert_eq!(got, Some(Value::String("n1".to_string())), "coalesce(missing,name): {got:?}");
    Ok(())
}

/// Plain passthrough still works (sanity).
#[tokio::test]
async fn plain_passthrough_sanity() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN b.data AS v", "v").await;
    assert_eq!(got, Some(Value::Bytes(payload())), "RETURN b.data: {got:?}");
    Ok(())
}

/// `size` over a bytes list literal returns an Int (value never decoded).
#[tokio::test]
async fn size_of_list_literal() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN size([b.data, b.data2]) AS n", "n").await;
    assert_eq!(got, Some(Value::Int(2)), "size([b.data,b.data2]): {got:?}");
    Ok(())
}

// ---------------------------------------------------------------------------
// List-mutator UDFs over list literals (fixed for free by the make_array child mark)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reverse_of_list_literal() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN reverse([b.data, b.data2]) AS v", "v").await;
    assert_eq!(
        got,
        Some(Value::List(vec![Value::Bytes(payload2()), Value::Bytes(payload())])),
        "reverse([b.data,b.data2]): {got:?}"
    );
    Ok(())
}

#[tokio::test]
async fn tail_of_list_literal() -> Result<()> {
    let db = blob_db().await?;
    let got = scalar_bytes(&db, "MATCH (b:Blob) RETURN tail([b.data, b.data2]) AS v", "v").await;
    assert_eq!(
        got,
        Some(Value::List(vec![Value::Bytes(payload2())])),
        "tail([b.data,b.data2]): {got:?}"
    );
    Ok(())
}
