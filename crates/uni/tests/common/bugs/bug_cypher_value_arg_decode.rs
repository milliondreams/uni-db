// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! An expression must answer the same way whichever encoding its entity
//! arrived in.
//!
//! A vertex bound by a `MATCH` carries typed property columns. The same vertex
//! round-tripped through `collect()` + `UNWIND` arrives as one `LargeBinary`
//! CypherValue, and a property read off it is `LargeBinary` too. This project's
//! own UDFs take that; DataFusion's built-ins do not, so six valid openCypher
//! expressions failed as hard errors on the second encoding only — the type of
//! defect where the query is fine and the plan is fine and the answer depends
//! on how the row got there.
//!
//! Two mechanisms, fixed separately:
//!
//! * `coalesce` and `CASE` resolved their branches to `Utf8` and then *cast*
//!   msgpack-tagged bytes to it — "Encountered non UTF-8 data".
//! * `toUpper` / `trim` / `replace` / `left` rejected `LargeBinary` at planning
//!   instead, because a DataFusion built-in's signature does not admit it.
//!
//! The test is a differential oracle rather than a list of expected values: the
//! two arms are each other's control, so neither can be quietly wrong in the
//! same direction. Values are compared, not just success — a silently corrupted
//! string is exactly what the old cast produced.

use uni_db::Uni;

/// Expressions that must agree. The last group are controls that were already
/// correct and must stay so — a fix that changed them would be trading one
/// asymmetry for another.
const EXPRS: &[&str] = &[
    // The six that failed.
    "coalesce(n.name,'x')",
    "toUpper(n.name)",
    "trim(n.name)",
    "replace(n.name,'a','z')",
    "left(n.name,1)",
    "CASE WHEN n.age>1 THEN n.name ELSE 'z' END",
    // Controls.
    "n.name",
    "n.age",
    "coalesce(n.age,0)",
    "toString(n.name)",
    "toUpper(toString(n.name))",
    "size(n.name)",
    "n.age + 1",
    "n.name = 'ada'",
    "id(n)",
    "labels(n)",
];

async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING, age INT)")
        .await
        .unwrap();
    // A non-ASCII value is deliberate: it is what a raw LargeBinary→Utf8 cast
    // corrupts rather than rejecting, so it distinguishes a decode from a cast.
    tx.execute("CREATE (:P {name:'ada', age:30}), (:P {name:'Ünïcödé', age:41})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

async fn rows_for(db: &Uni, query: &str) -> Result<Vec<String>, String> {
    match db.session().query(query).await {
        Ok(r) => {
            let mut out: Vec<String> = r
                .rows()
                .iter()
                .map(|row| format!("{:?}", row.value("v")))
                .collect();
            out.sort();
            Ok(out)
        }
        Err(e) => Err(e.to_string()),
    }
}

#[tokio::test]
async fn an_expression_agrees_across_both_entity_encodings() {
    let db = fixture().await;

    for expr in EXPRS {
        let native = rows_for(&db, &format!("MATCH (n:P) RETURN {expr} AS v")).await;
        let unwound = rows_for(
            &db,
            &format!("MATCH (m:P) WITH collect(m) AS ms UNWIND ms AS n RETURN {expr} AS v"),
        )
        .await;

        assert!(
            native.is_ok(),
            "control: `{expr}` fails even on the natively-bound path, so the \
             comparison below would not mean what it claims: {native:?}"
        );
        assert_eq!(
            unwound, native,
            "`{expr}` answers differently depending on how the entity arrived"
        );
    }
}

/// A raw `Bytes` property must not be decoded. It is `LargeBinary` like a
/// CypherValue and is told apart only by field metadata; stringifying one would
/// corrupt it, silently, which is worse than the bug this all exists to fix.
#[tokio::test]
async fn a_raw_bytes_property_is_not_decoded_as_a_cypher_value() {
    // Invalid UTF-8 on purpose: if this were ever routed through the CypherValue
    // decode, it would be stringified or rejected instead of round-tripping.
    let payload = vec![0xffu8, 0x00, 0xfe, 0x01];

    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("B")
        .property_nullable("blob", uni_db::DataType::Bytes)
        .property_nullable("tag", uni_db::DataType::String)
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute_with("CREATE (:B {blob: $d, tag: 'keep'})")
        .param("d", uni_db::Value::Bytes(payload.clone()))
        .run()
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let got = db
        .session()
        .query("MATCH (b:B) RETURN b.blob AS v")
        .await
        .expect("a bytes property must still read back");
    assert_eq!(
        got.rows()[0].value("v"),
        Some(&uni_db::Value::Bytes(payload)),
        "a raw-bytes property came back changed"
    );
}
