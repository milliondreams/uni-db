// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Probe + regression suite for the `DataType::Bytes` mis-decode surface —
//! SCALAR/LIST-FUNCTION shapes over Bytes values.
//!
//! Cypher scalar UDFs receive their arguments as Arrow columns via
//! `get_value_from_array`, which decodes `LargeBinary` through the tagged codec
//! with no schema-type hint. These probes exercise functions that pass a Bytes
//! value (or a list/element containing one) through, to find which mis-decode.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration bytes_list_functions

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

/// `coalesce(NULL, b.data)` must return the raw Bytes value.
///
/// Fixed by the general projection-output `uni_raw_bytes` propagation
/// (`df_graph::raw_bytes_marker`): a uniformly raw-or-null coalesce keeps its raw
/// output and is marked at the projection, while a marker-mixed coalesce is rewritten
/// to a CypherValue-encoded CASE. See `bytes_computed_projection.rs` for the full
/// surface.
#[tokio::test]
async fn coalesce_returns_bytes() -> Result<()> {
    let db = blob_db().await?;
    let res = db
        .session()
        .query("MATCH (b:Blob) RETURN coalesce(b.missing, b.data) AS v")
        .await?;
    let got = res.rows()[0].value("v").cloned();
    assert_eq!(
        got,
        Some(Value::Bytes(payload())),
        "coalesce dropped Bytes: {got:?}"
    );
    Ok(())
}

/// `head([b.data, ...])` over a list literal containing a Bytes scalar.
///
/// Fixed by the general projection-output propagation: a raw-`Bytes` list literal's
/// `make_array` child is marked at compile time (`df_graph::raw_bytes_marker`), so
/// `head` decodes the element as raw `Bytes` (and re-encodes it as a CypherValue,
/// which the read recovers via the codec).
#[tokio::test]
async fn head_of_bytes_list_literal() -> Result<()> {
    let db = blob_db().await?;
    let res = db
        .session()
        .query("MATCH (b:Blob) RETURN head([b.data]) AS h")
        .await?;
    let got = res.rows()[0].value("h").cloned();
    assert_eq!(
        got,
        Some(Value::Bytes(payload())),
        "head([bytes]) corrupted: {got:?}"
    );
    Ok(())
}

/// List indexing `[b.data][0]` over a Bytes element.
///
/// Fixed by the general projection-output propagation (same mechanism as
/// [`head_of_bytes_list_literal`]): the `make_array` child is marked, so `index`
/// decodes the element as raw `Bytes`.
#[tokio::test]
async fn index_into_bytes_list_literal() -> Result<()> {
    let db = blob_db().await?;
    let res = db
        .session()
        .query("MATCH (b:Blob) RETURN [b.data][0] AS v")
        .await?;
    let got = res.rows()[0].value("v").cloned();
    assert_eq!(
        got,
        Some(Value::Bytes(payload())),
        "[bytes][0] corrupted: {got:?}"
    );
    Ok(())
}

/// `size()` of a collected Bytes list — sanity (returns Int, value never decoded).
#[tokio::test]
async fn size_of_collected_bytes() -> Result<()> {
    let db = blob_db().await?;
    let res = db
        .session()
        .query("MATCH (b:Blob) RETURN size(collect(b.data)) AS n")
        .await?;
    let got = res.rows()[0].value("n").cloned();
    assert_eq!(
        got,
        Some(Value::Int(1)),
        "size(collect(bytes)) wrong: {got:?}"
    );
    Ok(())
}

/// `reverse()` over a two-element collected Bytes list reverses order, elements intact.
#[tokio::test]
async fn reverse_collected_bytes_list() -> Result<()> {
    let db = blob_db().await?;
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (b:Blob {id: 'b2', data: $d})")
        .param("d", Value::Bytes(payload2()))
        .run()
        .await?;
    tx.commit().await?;

    let res = db
        .session()
        .query(
            "MATCH (b:Blob) WITH b ORDER BY b.id \
             WITH collect(b.data) AS l RETURN reverse(l) AS r",
        )
        .await?;
    let got = res.rows()[0].value("r").cloned();
    assert_eq!(
        got,
        Some(Value::List(vec![
            Value::Bytes(payload2()),
            Value::Bytes(payload())
        ])),
        "reverse(collect(bytes)) corrupted: {got:?}"
    );
    Ok(())
}
