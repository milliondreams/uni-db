// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression tests for GitHub issue #93: a `DataType::Bytes` property cannot be
//! read back through a Cypher `RETURN`.
//!
//! `DataType::Bytes`, `DataType::CypherValue`, and `DataType::Duration` all map to
//! Arrow `LargeBinary`, differing only in payload encoding — `Bytes` is raw, the
//! others are tagged-MessagePack via the `cypher_value_codec`. The scan output
//! schema dropped the distinction, so the projection read fed raw `Bytes` to the
//! CypherValue codec, treating `byte[0]` as a type tag:
//!
//!   * first byte is an unmapped tag (e.g. `0x23`) → decode error, value lost as Null;
//!   * first byte is a valid tag (e.g. `0x00` = `TAG_NULL`) → silent corruption.
//!
//! The fix stamps raw-`Bytes` scan columns with `uni_raw_bytes=true` field metadata
//! so `record_batches_to_rows` routes them to the raw-bytes branch of
//! `arrow_to_value`. These tests fail before that fix and pass after.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration issue_93_bytes_round_trip

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// A node `Bytes` property whose first byte (`0x23` = '#') is an unmapped codec tag
/// round-trips through `RETURN b.data`.
#[tokio::test]
async fn bytes_round_trip_via_return() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Blob")
        .property("id", DataType::String)
        .property_nullable("data", DataType::Bytes)
        .apply()
        .await?;

    // First byte '#' = 0x23 = 35 — not a valid CypherValue tag.
    let payload = b"# Spec\n\n- requirement one".to_vec();

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute_with("CREATE (b:Blob {id: 'b1', data: $data})")
        .param("data", Value::Bytes(payload.clone()))
        .run()
        .await?;
    tx.commit().await?;

    let result = session
        .query("MATCH (b:Blob {id: 'b1'}) RETURN b.data AS data")
        .await?;
    let got = result.rows()[0].value("data").cloned();
    assert_eq!(
        got,
        Some(Value::Bytes(payload)),
        "raw Bytes property must round-trip through Cypher RETURN, got {got:?}"
    );
    Ok(())
}

/// The worse, silent-corruption case: first byte `0x00` (`TAG_NULL`) makes the
/// CypherValue codec "succeed" and discard the real bytes. Must still round-trip.
#[tokio::test]
async fn bytes_with_tag_valued_first_byte_silently_corrupts() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Blob")
        .property("id", DataType::String)
        .property_nullable("data", DataType::Bytes)
        .apply()
        .await?;

    let payload = vec![0x00, 0xDE, 0xAD, 0xBE, 0xEF]; // byte[0] = 0x00 = TAG_NULL

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute_with("CREATE (b:Blob {id: 'b2', data: $data})")
        .param("data", Value::Bytes(payload.clone()))
        .run()
        .await?;
    tx.commit().await?;

    let result = session
        .query("MATCH (b:Blob {id: 'b2'}) RETURN b.data AS data")
        .await?;
    let got = result.rows()[0].value("data").cloned();
    assert_eq!(
        got,
        Some(Value::Bytes(payload)),
        "tag-valued first byte must not silently corrupt the payload, got {got:?}"
    );
    Ok(())
}

/// The same defect applies to edge properties (a separate scan-schema path), so
/// guard a `Bytes` property on an edge type read via `MATCH ()-[r]->() RETURN r.payload`.
#[tokio::test]
async fn edge_bytes_property_round_trip() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Blob")
        .property("id", DataType::String)
        .done()
        .edge_type("LINK", &["Blob"], &["Blob"])
        .property_nullable("payload", DataType::Bytes)
        .apply()
        .await?;

    let payload = vec![0x00, 0x01, 0x02, 0xFE, 0xFF];

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Blob {id: 'a'})").await?;
    tx.execute("CREATE (:Blob {id: 'b'})").await?;
    tx.execute_with(
        "MATCH (a:Blob {id: 'a'}), (b:Blob {id: 'b'}) \
         CREATE (a)-[:LINK {payload: $payload}]->(b)",
    )
    .param("payload", Value::Bytes(payload.clone()))
    .run()
    .await?;
    tx.commit().await?;

    let result = session
        .query("MATCH ()-[r:LINK]->() RETURN r.payload AS payload")
        .await?;
    let got = result.rows()[0].value("payload").cloned();
    assert_eq!(
        got,
        Some(Value::Bytes(payload)),
        "raw Bytes edge property must round-trip through Cypher RETURN, got {got:?}"
    );
    Ok(())
}
