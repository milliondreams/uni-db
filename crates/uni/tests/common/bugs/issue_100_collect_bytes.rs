// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression tests for GitHub issue #100: `collect()` over a `DataType::Bytes`
//! column drops the value (residual of #93).
//!
//! #93 fixed the *scalar* projection path so `RETURN b.data` round-trips a raw
//! `Bytes` property. The `collect()` aggregation path was a separate code path:
//! its `CypherCollectAccumulator` re-materialized each `LargeBinary` element
//! through the tagged CypherValue codec, which reads `byte[0]` as a type tag and
//! drops/corrupts the value — so `RETURN collect(b.data)` came back as `[]`, often
//! with a stderr `unknown CypherValue tag: {byte0}`.
//!
//! The fix threads the `uni_raw_bytes=true` field metadata (stamped at scan time,
//! same marker as #93) into the accumulator via `AccumulatorArgs::expr_fields`, so
//! raw `Bytes` elements are read verbatim. These tests fail before that fix and
//! pass after.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration issue_100_collect_bytes

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// `payload()` is genuinely binary: first byte `'a'` (0x61 = 97) is an unmapped
/// codec tag, and the embedded NUL/0xFF bytes must survive verbatim.
fn payload() -> Vec<u8> {
    b"audio-fingerprint-\x00\xff\x01-blob".to_vec()
}

async fn db_with_one_blob() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Blob")
        .property("id", DataType::String)
        .property_nullable("data", DataType::Bytes)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute_with("CREATE (b:Blob {id: 'b1', data: $d})")
        .param("d", Value::Bytes(payload()))
        .run()
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// CONTROL — the scalar path fixed by #93 must keep round-tripping.
#[tokio::test]
async fn scalar_return_of_bytes_column_round_trips() -> Result<()> {
    let db = db_with_one_blob().await?;
    let result = db
        .session()
        .query("MATCH (b:Blob {id:'b1'}) RETURN b.data AS data")
        .await?;
    let got = result.rows()[0].value("data").cloned();
    assert_eq!(
        got,
        Some(Value::Bytes(payload())),
        "scalar Bytes projection must round-trip (#93), got {got:?}"
    );
    Ok(())
}

/// BUG — `collect()` must round-trip the raw `Bytes` value instead of dropping it.
#[tokio::test]
async fn collect_over_bytes_column_round_trips() -> Result<()> {
    let db = db_with_one_blob().await?;
    let result = db
        .session()
        .query("MATCH (b:Blob {id:'b1'}) RETURN collect(b.data) AS items")
        .await?;
    let got = result.rows()[0].value("items").cloned();
    assert_eq!(
        got,
        Some(Value::List(vec![Value::Bytes(payload())])),
        "collect() must round-trip the raw Bytes value, got {got:?}"
    );
    Ok(())
}

/// `collect()` over several `Bytes` rows, including the silent-corruption case
/// where `byte[0]` is a *valid* tag (`0x00` = `TAG_NULL`) — every payload must
/// survive verbatim and in order.
#[tokio::test]
async fn collect_over_multiple_bytes_rows_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Blob")
        .property("id", DataType::String)
        .property("seq", DataType::Int)
        .property_nullable("data", DataType::Bytes)
        .apply()
        .await?;

    let payloads = [
        vec![0x00, 0xDE, 0xAD, 0xBE, 0xEF], // byte[0] = 0x00 = TAG_NULL (silent-corruption case)
        b"# Spec\n\n- item".to_vec(),       // byte[0] = 0x23 (unmapped tag)
        vec![0xFF, 0xFE, 0x00, 0x01],
    ];

    let session = db.session();
    let tx = session.tx().await?;
    for (i, p) in payloads.iter().enumerate() {
        tx.execute_with("CREATE (b:Blob {id: $id, seq: $seq, data: $d})")
            .param("id", Value::String(format!("b{i}")))
            .param("seq", Value::Int(i as i64))
            .param("d", Value::Bytes(p.clone()))
            .run()
            .await?;
    }
    tx.commit().await?;

    let result = session
        .query("MATCH (b:Blob) WITH b ORDER BY b.seq RETURN collect(b.data) AS items")
        .await?;
    let got = result.rows()[0].value("items").cloned();
    let expected = Value::List(payloads.iter().cloned().map(Value::Bytes).collect());
    assert_eq!(
        got,
        Some(expected),
        "collect() must preserve every raw Bytes payload in order, got {got:?}"
    );
    Ok(())
}
