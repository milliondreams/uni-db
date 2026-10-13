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
//! With an index, Lance scores the rows it covers on the index's statistics
//! and the rows written since (a flush does not index them) on the index's
//! plus theirs. Re-scoring the covered rows with the unflushed ones counted
//! moved their scores at the flush (row 1: 0.835 before, 0.859 after), and
//! counting the unflushed rows without the uncovered flushed ones moved the
//! uncovered rows'. The second stage below runs with uncovered flushed rows
//! in place.
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

/// The top `k` for 'graph database', as (id, score), filtered by `filter`.
async fn ranked_where(db: &Uni, k: usize, filter: &str) -> Result<Vec<(i64, f64)>> {
    let result = db
        .session()
        .query(&format!(
            "CALL uni.fts.query('Doc', 'body', 'graph database', {k}, '{filter}') \
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

/// Every query a stage compares: the top 10, the top 2, and a filtered top 10.
async fn snapshot(db: &Uni) -> Result<Vec<Vec<(i64, f64)>>> {
    Ok(vec![
        ranked_where(db, 10, "id >= 0").await?,
        ranked_where(db, 2, "id >= 0").await?,
        ranked_where(db, 10, "id <> 2").await?,
    ])
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
    let built = db
        .indexes()
        .list(Some("Doc"))
        .into_iter()
        .any(|i| i.name() == "doc_fts" && i.metadata().last_built_at.is_some());
    assert_eq!(built, indexed, "the index must exist exactly when declared");
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

/// Each query ranks and scores the same before and after a flush, first
/// with every flushed row indexed, then with flushed rows the index does not
/// cover beside the unflushed ones.
async fn check(indexed: bool) -> Result<()> {
    let db = open(indexed).await?;
    let names = ["k = 10", "k = 2", "filtered"];
    for stage in ["first flush", "second flush"] {
        let before = snapshot(&db).await?;
        db.flush().await?;
        let after = snapshot(&db).await?;
        for ((b, a), name) in before.iter().zip(&after).zip(names) {
            assert_same(b, a, &format!("{stage}, {name}"));
        }
        run(
            &db,
            "CREATE (:Doc {id: 5, body: 'graph database graph'}), \
             (:Doc {id: 6, body: 'database graph and more'})",
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn fts_rank_survives_flush_with_an_index() -> Result<()> {
    check(true).await
}

#[tokio::test]
async fn fts_rank_survives_flush_without_an_index() -> Result<()> {
    check(false).await
}
