// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end tests for the `SPARSE_VECTOR(N)` property type declared via Cypher
//! DDL (issue #95). Covers the new grammar rule (`type_sparse_vector`), the
//! `parse_data_type` arm, and a full ingest → flush → `RETURN d.emb` round-trip
//! through the typed sparse column.

use uni_db::{Uni, Value};

async fn db_with_ddl(decl: &str) -> anyhow::Result<Uni> {
    let db = Uni::temporary().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(decl).await?;
    tx.commit().await?;
    Ok(db)
}

fn sv(indices: Vec<u32>, values: Vec<f32>) -> Value {
    Value::SparseVector { indices, values }
}

/// Read `d.emb` back and assert it equals the given (indices, values).
fn assert_emb(row_val: Option<&Value>, qi: &[u32], qv: &[f32]) {
    match row_val {
        Some(Value::SparseVector { indices, values }) => {
            assert_eq!(indices.as_slice(), qi, "sparse indices round-trip");
            assert_eq!(values.as_slice(), qv, "sparse values round-trip");
        }
        other => panic!("expected SparseVector, got {other:?}"),
    }
}

#[tokio::test]
async fn sparse_vector_ddl_type_roundtrips() -> anyhow::Result<()> {
    // The DDL grammar must accept `SPARSE_VECTOR(N)` and map it to
    // `DataType::SparseVector { dimensions: N }`.
    let db = db_with_ddl("CREATE LABEL Doc (title STRING, emb SPARSE_VECTOR(1000))").await?;

    let qi = vec![1u32, 5, 9, 42];
    let qv = vec![1.0f32, 2.0, 3.0, 0.5];
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: 'a', emb: $emb})")
        .param("emb", sv(qi.clone(), qv.clone()))
        .run()
        .await?;
    tx.commit().await?;

    // Read from L0 (no flush yet) …
    let l0 = db
        .session()
        .query("MATCH (d:Doc {title: 'a'}) RETURN d.emb AS emb")
        .await?;
    assert_emb(l0.rows()[0].value("emb"), &qi, &qv);

    // … then from flushed storage.
    db.flush().await?;
    let flushed = db
        .session()
        .query("MATCH (d:Doc {title: 'a'}) RETURN d.emb AS emb")
        .await?;
    assert_emb(flushed.rows()[0].value("emb"), &qi, &qv);
    Ok(())
}

#[tokio::test]
async fn sparse_vector_ddl_lowercase_and_empty() -> anyhow::Result<()> {
    // Case-insensitive keyword + an empty sparse vector (valid: no terms).
    let db = db_with_ddl("CREATE LABEL Doc (emb sparse_vector(50))").await?;
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {emb: $emb})")
        .param("emb", sv(vec![], vec![]))
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc) RETURN d.emb AS emb")
        .await?;
    assert_emb(res.rows()[0].value("emb"), &[], &[]);
    Ok(())
}
