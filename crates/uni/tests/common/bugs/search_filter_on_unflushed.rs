// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A search's `filter` argument applies to unflushed rows as well as flushed
//! ones.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(search_filter_on_unflushed)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, Value};

async fn open() -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("id", DataType::Int)
        .property("emb", DataType::Vector { dimensions: 2 })
        .property("body", DataType::String)
        .apply()
        .await?;
    Ok(db)
}

async fn run(db: &Uni, stmt: &str) -> Result<()> {
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

async fn ids(db: &Uni, call: &str) -> Result<Vec<i64>> {
    let result = db
        .session()
        .query(&format!("{call} YIELD node RETURN node.id AS id"))
        .await?;
    let mut out: Vec<i64> = result
        .rows()
        .iter()
        .map(|r| match &r.values()[0] {
            Value::Int(i) => *i,
            other => panic!("id: {other:?}"),
        })
        .collect();
    out.sort_unstable();
    Ok(out)
}

const SEED: &str = "CREATE (:Doc {id: 1, emb: [1.0, 0.0], body: 'foo a'}), \
                    (:Doc {id: 2, emb: [0.9, 0.1], body: 'foo b'}), \
                    (:Doc {id: 3, emb: [0.0, 1.0], body: 'foo c'})";
const MORE: &str = "CREATE (:Doc {id: 4, emb: [1.0, 0.1], body: 'foo d'}), \
                    (:Doc {id: 5, emb: [0.8, 0.2], body: 'foo e'})";

async fn check(db: &Uni, stage: &str, filter: &str, want: &[i64]) -> Result<()> {
    let vector = format!("CALL uni.vector.query('Doc', 'emb', [1.0, 0.0], 10, '{filter}')");
    let text = format!("CALL uni.fts.query('Doc', 'body', 'foo', 10, '{filter}')");
    assert_eq!(ids(db, &vector).await?, want, "{stage}: vector, {filter}");
    assert_eq!(ids(db, &text).await?, want, "{stage}: text, {filter}");
    Ok(())
}

#[tokio::test]
async fn search_filter_on_unflushed_rows() -> Result<()> {
    let db = open().await?;
    run(&db, SEED).await?;
    check(&db, "unflushed", "id = 2", &[2]).await?;
    db.flush().await?;
    check(&db, "flushed", "id = 2", &[2]).await?;
    run(&db, MORE).await?;
    check(&db, "mixed", "id >= 2 AND id <= 4", &[2, 3, 4]).await?;
    Ok(())
}

/// An unflushed update that takes a flushed row out of the filter removes it.
#[tokio::test]
async fn search_filter_on_unflushed_updates() -> Result<()> {
    let db = open().await?;
    run(&db, SEED).await?;
    db.flush().await?;
    run(&db, "MATCH (d:Doc {id: 2}) SET d.id = 20").await?;
    check(&db, "updated", "id = 2", &[]).await?;
    Ok(())
}
