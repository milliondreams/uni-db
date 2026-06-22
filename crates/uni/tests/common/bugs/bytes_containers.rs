// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Probe + regression suite for the `DataType::Bytes` mis-decode surface —
//! TYPED CONTAINER properties (`List(Bytes)`, `Map(String, Bytes)`).
//!
//! A typed `List(Bytes)` or map-with-Bytes-values property must store and read
//! back each element verbatim. On read, nested elements are materialized via
//! `array_to_value_list` / the map path in `arrow_convert`, which pass `None` for
//! the element schema type and thus mis-decode raw Bytes. On write, the list/map
//! column builders historically support only String/Int/Float elements.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration bytes_containers

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

fn b1() -> Vec<u8> {
    b"audio-\x00\xff-one".to_vec()
}
fn b2() -> Vec<u8> {
    vec![0x00, 0xDE, 0xAD, 0xBE, 0xEF] // byte[0] = TAG_NULL
}

/// A typed `List(Bytes)` property round-trips every element.
///
/// KNOWN LIMITATION (tracked): reading a typed `List(Bytes)` back is ambiguous from
/// the Arrow array alone — a `LargeBinary` list child can be either raw `Bytes` or
/// CV-encoded values, and discriminating by array type alone corrupts CV-encoded
/// containers (regresses pattern-comprehension/VLP edge property maps). A correct fix
/// needs field-level `uni_raw_bytes` discrimination threaded through nested-container
/// decode; deferred. The assertion below is the correct expectation.
#[ignore = "typed List(Bytes) read-back needs field-level Bytes-vs-CV discrimination (nested-container follow-up)"]
#[tokio::test]
async fn list_of_bytes_property_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property("id", DataType::String)
        .property_nullable("chunks", DataType::List(Box::new(DataType::Bytes)))
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (d:Doc {id: 'd1', chunks: $c})")
        .param(
            "c",
            Value::List(vec![Value::Bytes(b1()), Value::Bytes(b2())]),
        )
        .run()
        .await?;
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc {id:'d1'}) RETURN d.chunks AS chunks")
        .await?;
    let got = res.rows()[0].value("chunks").cloned();
    assert_eq!(
        got,
        Some(Value::List(vec![Value::Bytes(b1()), Value::Bytes(b2())])),
        "List(Bytes) property corrupted: {got:?}"
    );
    Ok(())
}

/// A typed `Map(String, Bytes)` property round-trips every value.
///
/// KNOWN LIMITATION (tracked): same as the list case — reading a typed
/// `Map(String, Bytes)` back can't be discriminated from a CV-encoded map by array
/// type alone without corrupting CV-encoded maps. Needs field-level `uni_raw_bytes`
/// discrimination through nested-container decode; deferred. The assertion below is
/// the correct expectation.
#[ignore = "typed Map(String,Bytes) read-back needs field-level Bytes-vs-CV discrimination (nested-container follow-up)"]
#[tokio::test]
async fn map_of_bytes_property_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property("id", DataType::String)
        .property_nullable(
            "meta",
            DataType::Map(Box::new(DataType::String), Box::new(DataType::Bytes)),
        )
        .apply()
        .await?;

    let mut m = HashMap::new();
    m.insert("k1".to_string(), Value::Bytes(b1()));
    m.insert("k2".to_string(), Value::Bytes(b2()));

    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (d:Doc {id: 'd1', meta: $m})")
        .param("m", Value::Map(m.clone()))
        .run()
        .await?;
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc {id:'d1'}) RETURN d.meta AS meta")
        .await?;
    let got = res.rows()[0].value("meta").cloned();
    assert_eq!(
        got,
        Some(Value::Map(m)),
        "Map(String,Bytes) property corrupted: {got:?}"
    );
    Ok(())
}
