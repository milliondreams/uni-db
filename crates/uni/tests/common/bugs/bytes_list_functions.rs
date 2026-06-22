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
/// final read mis-decodes the passthrough raw bytes as tagged CypherValue.
///
/// Investigated for a targeted fix and deferred: replacing the native `CASE` with a
/// custom UDF would round-trip every coalesce arg through `Value` (lossy for the
/// Arrow types the type-agnostic `CASE` handles freely, and changes the output
/// column type), and a metadata-only wrapper can't safely decide when to mark —
/// `coalesce(b.missing, b.data)` only marks under an "any arg is Bytes" rule, which
/// mis-marks mixed `Bytes`+`String` coalesce (unified column holds both raw and
/// CV-encoded blobs row-by-row). This is the same computed-output-field
/// metadata-propagation class as the list-literal cases ([`head_of_bytes_list_literal`]);
/// deferred to a single general projection-output propagation follow-up. The
/// assertion below is the correct expectation.
#[ignore = "coalesce-of-Bytes: no safe targeted fix; general computed-output metadata-propagation follow-up (with list-literals)"]
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
/// KNOWN LIMITATION (tracked): a list LITERAL `[b.data]` is built by DataFusion's
/// `make_array`, producing a `List(LargeBinary)` whose child field carries NO
/// `uni_raw_bytes` marker — the marker can't flow from `b.data`'s column field
/// through a logical-plan transform. This is the same computed-output-field
/// metadata-propagation class as `coalesce` (see [`coalesce_returns_bytes`]):
/// deferred to the general projection-propagation follow-up. Unlike typed
/// `List(Bytes)` (schema-driven, now fixed), there is no schema or targetable UDF
/// here. The assertion below is correct.
#[ignore = "Bytes in list-literal: make_array output field lacks uni_raw_bytes; general computed-output propagation follow-up (with coalesce)"]
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
/// KNOWN LIMITATION (tracked): same class as [`head_of_bytes_list_literal`] — the
/// list literal `[b.data]` is built by `make_array` with no `uni_raw_bytes` marker
/// on its child field. Deferred to the general computed-output metadata-propagation
/// follow-up (with `coalesce`). The assertion below is correct.
#[ignore = "Bytes in list-literal: make_array output field lacks uni_raw_bytes; general computed-output propagation follow-up (with coalesce)"]
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
