// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Vector and full-text search find schemaless rows after a flush.
//!
//! A search read flushed rows only from the column named after the property
//! in the label's own table. A schemaless value has no such column: an
//! undeclared label is flushed to the shared vertex table only, and an
//! undeclared property of a declared label is flushed into the label's
//! overflow column. The same query that found the rows while they were
//! unflushed returned nothing once they were flushed, with no error.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(schemaless_search_after_flush)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, Value};

/// Where a schemaless value lives once flushed.
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// A label the schema does not know: flushed to the shared vertex table.
    UndeclaredLabel,
    /// An undeclared property of a declared label: flushed to its overflow column.
    UndeclaredProperty,
}

impl Shape {
    fn label(self) -> &'static str {
        match self {
            Self::UndeclaredLabel => "Free",
            Self::UndeclaredProperty => "Doc",
        }
    }
}

async fn open(shape: Shape) -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    if let Shape::UndeclaredProperty = shape {
        db.schema()
            .label("Doc")
            .property("id", DataType::Int)
            .apply()
            .await?;
    }
    Ok(db)
}

async fn run(db: &Uni, stmt: &str) -> Result<()> {
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

/// `(id, score)` pairs in the order the search returned them.
async fn search(db: &Uni, call: &str) -> Result<Vec<(i64, f64)>> {
    let result = db
        .session()
        .query(&format!(
            "{call} YIELD node, score RETURN node.id AS id, score"
        ))
        .await?;
    Ok(result
        .rows()
        .iter()
        .map(|r| {
            let id = match &r.values()[0] {
                Value::Int(i) => *i,
                other => panic!("id: {other:?}"),
            };
            let score = match &r.values()[1] {
                Value::Float(f) => *f,
                other => panic!("score: {other:?}"),
            };
            (id, score)
        })
        .collect())
}

async fn vector_hits(db: &Uni, shape: Shape, k: usize) -> Result<Vec<(i64, f64)>> {
    let label = shape.label();
    search(
        db,
        &format!("CALL uni.vector.query('{label}', 'emb', [1.0, 0.0], {k})"),
    )
    .await
}

async fn text_hits(db: &Uni, shape: Shape, term: &str) -> Result<Vec<(i64, f64)>> {
    let label = shape.label();
    search(
        db,
        &format!("CALL uni.fts.query('{label}', 'body', '{term}', 10)"),
    )
    .await
}

fn ids(hits: &[(i64, f64)]) -> Vec<i64> {
    hits.iter().map(|(id, _)| *id).collect()
}

async fn seed(db: &Uni, shape: Shape) -> Result<()> {
    let label = shape.label();
    run(
        db,
        &format!(
            "CREATE (:{label} {{id: 1, emb: [1.0, 0.0], body: 'foo apple'}}), \
             (:{label} {{id: 2, emb: [0.0, 1.0], body: 'foo cherry pie'}}), \
             (:{label} {{id: 3, emb: [0.9, 0.1], body: 'other text'}}), \
             (:{label} {{id: 4, emb: [0.5, 0.5], body: 'foo pie'}})"
        ),
    )
    .await
}

/// A flushed row is found, in the same order and with the same score as
/// before the flush.
async fn finds_flushed_vectors(shape: Shape) -> Result<()> {
    let db = open(shape).await?;
    seed(&db, shape).await?;
    let unflushed = vector_hits(&db, shape, 10).await?;
    assert_eq!(ids(&unflushed), vec![1, 3, 4, 2], "{shape:?} unflushed");
    db.flush().await?;
    assert_eq!(vector_hits(&db, shape, 10).await?, unflushed, "{shape:?}");
    assert_eq!(
        ids(&vector_hits(&db, shape, 2).await?),
        vec![1, 3],
        "{shape:?}: k keeps the nearest"
    );
    Ok(())
}

async fn finds_flushed_text(shape: Shape) -> Result<()> {
    let db = open(shape).await?;
    seed(&db, shape).await?;
    let unflushed = text_hits(&db, shape, "foo pie").await?;
    // 2 and 4 hold both terms and tie; 1 holds one.
    assert_eq!(ids(&unflushed)[2], 1, "{shape:?} unflushed: {unflushed:?}");
    assert_eq!(unflushed.len(), 3, "{shape:?} unflushed: {unflushed:?}");
    db.flush().await?;
    let flushed = text_hits(&db, shape, "foo pie").await?;
    let mut a = unflushed.clone();
    let mut b = flushed.clone();
    a.sort_by_key(|(id, _)| *id);
    b.sort_by_key(|(id, _)| *id);
    assert_eq!(b, a, "{shape:?}");
    assert!(
        text_hits(&db, shape, "absent").await?.is_empty(),
        "{shape:?}"
    );
    Ok(())
}

/// Only a row's latest flushed version is searched.
async fn ignores_superseded_versions(shape: Shape) -> Result<()> {
    let label = shape.label();
    let db = open(shape).await?;
    seed(&db, shape).await?;
    db.flush().await?;
    run(
        &db,
        &format!("MATCH (d:{label} {{id: 1}}) SET d.emb = [0.0, 1.0], d.body = 'bar'"),
    )
    .await?;
    db.flush().await?;
    assert_eq!(
        ids(&vector_hits(&db, shape, 2).await?),
        vec![3, 4],
        "{shape:?}: the old vector no longer matches"
    );
    assert_eq!(
        ids(&text_hits(&db, shape, "apple").await?),
        Vec::<i64>::new(),
        "{shape:?}: the old text no longer matches"
    );
    Ok(())
}

/// A deleted row is not found, whether the delete is flushed or not.
async fn ignores_deleted_rows(shape: Shape) -> Result<()> {
    let label = shape.label();
    let db = open(shape).await?;
    seed(&db, shape).await?;
    db.flush().await?;
    run(&db, &format!("MATCH (d:{label} {{id: 1}}) DELETE d")).await?;
    assert_eq!(
        ids(&vector_hits(&db, shape, 1).await?),
        vec![3],
        "{shape:?}: unflushed delete"
    );
    db.flush().await?;
    assert_eq!(
        ids(&vector_hits(&db, shape, 1).await?),
        vec![3],
        "{shape:?}: flushed delete"
    );
    assert_eq!(
        ids(&text_hits(&db, shape, "apple").await?),
        Vec::<i64>::new(),
        "{shape:?}: flushed delete"
    );
    Ok(())
}

/// A vertex relabeled away from the label after the flush is no longer one
/// of its rows, though its older flushed row still carries the label.
async fn ignores_relabeled_rows(shape: Shape) -> Result<()> {
    let label = shape.label();
    let db = open(shape).await?;
    seed(&db, shape).await?;
    db.flush().await?;
    run(
        &db,
        &format!("MATCH (d:{label} {{id: 1}}) REMOVE d:{label} SET d:Gone"),
    )
    .await?;
    db.flush().await?;
    assert_eq!(ids(&vector_hits(&db, shape, 1).await?), vec![3]);
    assert_eq!(
        ids(&text_hits(&db, shape, "apple").await?),
        Vec::<i64>::new()
    );
    Ok(())
}

/// A flushed update to another property keeps the searched value.
async fn keeps_rows_after_other_updates(shape: Shape) -> Result<()> {
    let label = shape.label();
    let db = open(shape).await?;
    seed(&db, shape).await?;
    db.flush().await?;
    run(
        &db,
        &format!("MATCH (d:{label} {{id: 1}}) SET d.note = 'x'"),
    )
    .await?;
    db.flush().await?;
    assert_eq!(ids(&vector_hits(&db, shape, 1).await?), vec![1]);
    assert_eq!(ids(&text_hits(&db, shape, "apple").await?), vec![1]);
    Ok(())
}

/// A filter names columns the schemaless rows do not have; it is refused
/// rather than ignored.
async fn refuses_a_filter(shape: Shape) -> Result<()> {
    let label = shape.label();
    let db = open(shape).await?;
    seed(&db, shape).await?;
    db.flush().await?;
    for call in [
        format!("CALL uni.vector.query('{label}', 'emb', [1.0, 0.0], 10, 'id = 2')"),
        format!("CALL uni.fts.query('{label}', 'body', 'foo', 10, 'id = 2')"),
    ] {
        let err = search(&db, &call)
            .await
            .expect_err("a filter over a schemaless search must not be ignored");
        assert!(err.to_string().contains("not supported"), "{call}: {err}");
    }
    Ok(())
}

/// A fork finds its own flushed rows and its parent's; the parent finds only
/// its own.
async fn searches_a_fork(shape: Shape) -> Result<()> {
    let label = shape.label();
    let db = open(shape).await?;
    seed(&db, shape).await?;
    db.flush().await?;
    let fork = db.session().fork("schemaless_search").await?;
    let tx = fork.tx().await?;
    tx.execute(&format!(
        "CREATE (:{label} {{id: 5, emb: [0.8, 0.2], body: 'forked apple'}})"
    ))
    .await?;
    tx.commit().await?;
    fork.flush().await?;
    let fork_ids = |hits: Vec<(i64, f64)>| {
        let mut v = ids(&hits);
        v.sort_unstable();
        v
    };
    let vector = format!("CALL uni.vector.query('{label}', 'emb', [1.0, 0.0], 3)");
    let text = format!("CALL uni.fts.query('{label}', 'body', 'apple', 10)");
    let on_fork = |call: String| {
        let fork = &fork;
        async move {
            let result = fork
                .query(&format!("{call} YIELD node RETURN node.id AS id"))
                .await?;
            let mut v: Vec<i64> = result
                .rows()
                .iter()
                .map(|r| match &r.values()[0] {
                    Value::Int(i) => *i,
                    other => panic!("id: {other:?}"),
                })
                .collect();
            v.sort_unstable();
            anyhow::Ok(v)
        }
    };
    assert_eq!(
        on_fork(vector.clone()).await?,
        vec![1, 3, 5],
        "fork, vector"
    );
    assert_eq!(on_fork(text.clone()).await?, vec![1, 5], "fork, text");
    assert_eq!(
        fork_ids(search(&db, &vector).await?),
        vec![1, 3, 4],
        "parent, vector"
    );
    assert_eq!(fork_ids(search(&db, &text).await?), vec![1], "parent, text");
    Ok(())
}

macro_rules! per_shape {
    ($($case:ident),* $(,)?) => {
        mod undeclared_label {
            $(
                #[tokio::test]
                async fn $case() -> anyhow::Result<()> {
                    super::$case(super::Shape::UndeclaredLabel).await
                }
            )*
        }
        mod undeclared_property {
            $(
                #[tokio::test]
                async fn $case() -> anyhow::Result<()> {
                    super::$case(super::Shape::UndeclaredProperty).await
                }
            )*
        }
    };
}

per_shape!(
    finds_flushed_vectors,
    finds_flushed_text,
    ignores_superseded_versions,
    ignores_deleted_rows,
    ignores_relabeled_rows,
    keeps_rows_after_other_updates,
    refuses_a_filter,
    searches_a_fork,
);
