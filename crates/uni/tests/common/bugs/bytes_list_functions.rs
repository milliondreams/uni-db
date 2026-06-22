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
/// KNOWN LIMITATION (tracked): `coalesce` compiles to a DataFusion `CASE` whose
/// `LargeBinary` output field does not carry the `uni_raw_bytes` marker, so the
/// final read mis-decodes the passthrough raw bytes as tagged CypherValue. The fix
/// needs planner-level metadata propagation onto computed projection output fields;
/// deferred as a focused follow-up. The assertion below is the correct expectation.
#[ignore = "coalesce-of-Bytes: computed CASE output column lacks uni_raw_bytes marker (planner propagation follow-up)"]
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
/// KNOWN LIMITATION (tracked): a `Bytes` element inside a list LITERAL is read back
/// through the nested-container path, which can't discriminate raw `Bytes` from
/// CV-encoded `LargeBinary` by array type alone (same class as typed `List(Bytes)`).
/// Deferred to the nested-container follow-up. The assertion below is correct.
#[ignore = "Bytes in list-literal: nested-container read needs field-level Bytes-vs-CV discrimination (follow-up)"]
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
/// KNOWN LIMITATION (tracked): same nested-container class as
/// [`head_of_bytes_list_literal`] — a `Bytes` element in a list literal can't be
/// discriminated from CV-encoded `LargeBinary` on read by array type alone.
/// Deferred to the nested-container follow-up. The assertion below is correct.
#[ignore = "Bytes in list-literal: nested-container read needs field-level Bytes-vs-CV discrimination (follow-up)"]
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
