// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A full-text search ranks rows the same before and after they are flushed.
//!
//! Flushed rows were scored by Lance's BM25 and unflushed ones by the share
//! of query words their text holds, split on whitespace. The two scales do not
//! compare, so the ranking, and which rows made the top `k`, changed at the
//! flush: an exact two-word match scored 0.5 unflushed and ranked below a
//! long text that mentions each word once. Unflushed rows are now scored by
//! the backend, as its search will score them once stored.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(fts_rank_survives_flush)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, Value};

async fn run(db: &Uni, stmt: &str) -> Result<()> {
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

async fn ranked(db: &Uni, k: usize) -> Result<Vec<(i64, f64)>> {
    let result = db
        .session()
        .query(&format!(
            "CALL uni.fts.query('Doc', 'body', 'graph database', {k}) \
             YIELD node, score RETURN node.id, score"
        ))
        .await?;
    Ok(result
        .rows()
        .iter()
        .map(|r| match (&r.values()[0], &r.values()[1]) {
            (Value::Int(i), Value::Float(f)) => (*i, *f),
            other => panic!("row: {other:?}"),
        })
        .collect())
}

async fn open(indexed: bool) -> Result<Uni> {
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
        .property("body", DataType::String)
        .apply()
        .await?;
    if indexed {
        run(
            &db,
            "CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON EACH [d.body]",
        )
        .await?;
    }
    let mut rows: Vec<String> = (0..20)
        .map(|i| {
            format!(
                "(:Doc {{id: {}, body: 'unrelated filler text number {i}'}})",
                100 + i
            )
        })
        .collect();
    rows.push("(:Doc {id: 1, body: 'graph graph graph database'})".to_string());
    rows.push(
        "(:Doc {id: 2, body: 'a long text that mentions graph once and database once \
         among many other words here'})"
            .to_string(),
    );
    run(&db, &format!("CREATE {}", rows.join(", "))).await?;
    db.flush().await?;
    run(
        &db,
        "CREATE (:Doc {id: 3, body: 'graph database'}), (:Doc {id: 4, body: 'database'})",
    )
    .await?;
    Ok(db)
}

fn assert_same(before: &[(i64, f64)], after: &[(i64, f64)], what: &str) {
    let ids = |r: &[(i64, f64)]| r.iter().map(|(i, _)| *i).collect::<Vec<_>>();
    assert_eq!(
        ids(before),
        ids(after),
        "{what}: ranking {before:?} vs {after:?}"
    );
    for ((id, b), (_, a)) in before.iter().zip(after) {
        assert!((b - a).abs() < 1e-6, "{what}: id {id} scored {b} then {a}");
    }
}

#[tokio::test]
async fn fts_rank_survives_flush_with_an_index() -> Result<()> {
    let db = open(true).await?;
    let before = ranked(&db, 10).await?;
    let top2 = ranked(&db, 2).await?;
    db.flush().await?;
    assert_same(&before, &ranked(&db, 10).await?, "k = 10");
    assert_same(&top2, &ranked(&db, 2).await?, "k = 2");
    Ok(())
}

#[tokio::test]
async fn fts_rank_survives_flush_without_an_index() -> Result<()> {
    let db = open(false).await?;
    let before = ranked(&db, 10).await?;
    let top2 = ranked(&db, 2).await?;
    db.flush().await?;
    assert_same(&before, &ranked(&db, 10).await?, "k = 10");
    assert_same(&top2, &ranked(&db, 2).await?, "k = 2");
    Ok(())
}
