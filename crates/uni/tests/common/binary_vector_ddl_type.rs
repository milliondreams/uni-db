// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end tests for the `BINARY_VECTOR(N)` property type declared via Cypher
//! DDL (deferred P2 item D1). Covers the new grammar rule (`type_binary_vector`),
//! the `parse_data_type` arm, a full ingest → flush → `RETURN d.bits` round-trip
//! through the typed `FixedSizeList<UInt8>` column, and exact `VECTOR_DISTANCE`
//! with the `hamming` / `jaccard` metrics (which have no ANN index).

use uni_db::{Uni, Value};

async fn db_with_ddl(decl: &str) -> anyhow::Result<Uni> {
    let db = Uni::temporary().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(decl).await?;
    tx.commit().await?;
    Ok(db)
}

fn assert_bits(row_val: Option<&Value>, expected: &[u8]) {
    match row_val {
        Some(Value::BinaryVector(b)) => {
            assert_eq!(b.as_slice(), expected, "binary vector round-trip")
        }
        other => panic!("expected BinaryVector, got {other:?}"),
    }
}

#[tokio::test]
async fn binary_vector_ddl_type_roundtrips() -> anyhow::Result<()> {
    // The DDL grammar must accept `BINARY_VECTOR(N)` and map it to
    // `DataType::BinaryVector { dimensions: N }`.
    let db = db_with_ddl("CREATE LABEL Doc (title STRING, bits BINARY_VECTOR(4))").await?;

    let bits = vec![0x00u8, 0xFF, 0xA5, 0x3C];
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: 'a', bits: $bits})")
        .param("bits", Value::BinaryVector(bits.clone()))
        .run()
        .await?;
    tx.commit().await?;

    // Read from L0 (no flush yet) …
    let l0 = db
        .session()
        .query("MATCH (d:Doc {title: 'a'}) RETURN d.bits AS bits")
        .await?;
    assert_bits(l0.rows()[0].value("bits"), &bits);

    // … then from flushed storage.
    db.flush().await?;
    let flushed = db
        .session()
        .query("MATCH (d:Doc {title: 'a'}) RETURN d.bits AS bits")
        .await?;
    assert_bits(flushed.rows()[0].value("bits"), &bits);
    Ok(())
}

#[tokio::test]
async fn binary_vector_ddl_lowercase_and_list_input() -> anyhow::Result<()> {
    // Case-insensitive keyword + a list-of-byte-ints literal input form, coerced
    // to the binary column at write time.
    let db = db_with_ddl("CREATE LABEL Doc (bits binary_vector(3))").await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {bits: [1, 2, 255]})").await?;
    tx.commit().await?;
    db.flush().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc) RETURN d.bits AS bits")
        .await?;
    assert_bits(res.rows()[0].value("bits"), &[1, 2, 255]);
    Ok(())
}

#[tokio::test]
async fn binary_vector_distance_hamming_jaccard() -> anyhow::Result<()> {
    // Exact Hamming/Jaccard via `VECTOR_DISTANCE` over stored binary vectors.
    let db = db_with_ddl("CREATE LABEL Doc (name STRING, bits BINARY_VECTOR(2))").await?;
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {name: 'a', bits: $a})")
        .param("a", Value::BinaryVector(vec![0x00, 0xA5]))
        .run()
        .await?;
    tx.execute_with("CREATE (:Doc {name: 'b', bits: $b})")
        .param("b", Value::BinaryVector(vec![0xFF, 0xA5]))
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Hamming(a, b): 0x00^0xFF = 8 bits differ, 0xA5^0xA5 = 0 → 8.
    let res = db
        .session()
        .query(
            "MATCH (a:Doc {name: 'a'}), (b:Doc {name: 'b'}) \
             RETURN VECTOR_DISTANCE(a.bits, b.bits, 'hamming') AS h, \
                    VECTOR_DISTANCE(a.bits, b.bits, 'jaccard') AS j",
        )
        .await?;
    let h = res.rows()[0].value("h").and_then(|v| v.as_f64()).unwrap();
    assert!((h - 8.0).abs() < 1e-9, "hamming = 8, got {h}");
    // Jaccard: inter = popcount(0x00 & 0xFF)=0 + popcount(0xA5&0xA5)=4 = 4;
    // union = popcount(0xFF)=8 + popcount(0xA5)=4 = 12 → 1 − 4/12 = 2/3.
    let j = res.rows()[0].value("j").and_then(|v| v.as_f64()).unwrap();
    assert!((j - 2.0 / 3.0).abs() < 1e-9, "jaccard = 2/3, got {j}");
    Ok(())
}
