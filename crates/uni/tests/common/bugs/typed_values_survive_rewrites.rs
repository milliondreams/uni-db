// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A stored value keeps its type through every path that rewrites its row:
//! a flush, a `SET` of another property, compaction (`VACUUM`), and a reopen.
//!
//! Several readers decoded a stored value through `serde_json::Value` and
//! back. That turned a temporal into a string and a vector into a list, and
//! the paths that read a row, change one property, and write the row back
//! then stored the converted value: the property's type was lost for good,
//! or it became NULL where the column could not hold the string.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(typed_values_survive_rewrites)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, Value};

const DUR: &str = "duration('P1DT2H')";
const DT: &str = "datetime('2024-01-02T03:04:05Z')";
const DATE: &str = "date('2024-01-02')";
const TIME: &str = "time('03:04:05Z')";

fn config() -> UniConfig {
    let mut config = UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..Default::default()
    };
    config.compaction.enabled = false;
    config
}

async fn open(uri: &str) -> Result<Uni> {
    Ok(Uni::open(uri).config(config()).build().await?)
}

async fn run(db: &Uni, stmt: &str) -> Result<()> {
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

async fn row(db: &Uni, query: &str) -> Result<Vec<Value>> {
    let result = db.session().query(query).await?;
    assert_eq!(result.len(), 1, "{query}: {:?}", result.rows());
    Ok(result.rows()[0].values().to_vec())
}

const VERTEX: [&str; 13] = [
    "d", "ts", "lst", "cv", "emb", "x", "lb", "li", "lf", "ld", "ltm", "ldur", "lstr",
];
const EDGE: [&str; 2] = ["d", "cv"];

/// The values every read must return, in `VERTEX` / `EDGE` order.
async fn expected(db: &Uni) -> Result<(Vec<Value>, Vec<Value>)> {
    let lit = row(
        db,
        &format!("RETURN {DUR}, {DT}, [{DT}], {DT}, {DT}, [{DATE}], [{TIME}], [{DUR}]"),
    )
    .await?;
    let vertex = vec![
        lit[0].clone(),
        lit[1].clone(),
        lit[2].clone(),
        lit[3].clone(),
        Value::Vector(vec![1.0, 2.0]),
        lit[4].clone(),
        Value::List(vec![Value::Bool(true), Value::Bool(false)]),
        Value::List(vec![Value::Int(1), Value::Int(2)]),
        Value::List(vec![Value::Float(1.5)]),
        lit[5].clone(),
        lit[6].clone(),
        lit[7].clone(),
        Value::List(vec![Value::String("a".to_string())]),
    ];
    let edge = vec![lit[0].clone(), lit[1].clone()];
    Ok((vertex, edge))
}

async fn check(db: &Uni, stage: &str, errs: &mut Vec<String>) -> Result<()> {
    let (vertex, edge) = expected(db).await?;
    let reads = VERTEX
        .iter()
        .zip(&vertex)
        .map(|(p, want)| {
            (
                format!("MATCH (t:T {{k: 1}}) RETURN t.{p}"),
                format!("t.{p}"),
                want,
            )
        })
        .chain(EDGE.iter().zip(&edge).map(|(p, want)| {
            (
                format!("MATCH (:T {{k: 1}})-[r:R]->() RETURN r.{p}"),
                format!("r.{p}"),
                want,
            )
        }));
    for (query, name, want) in reads {
        match row(db, &query).await {
            Ok(got) if got[0] == *want => {}
            Ok(got) => errs.push(format!("{stage}: {name}: got {:?}, want {want:?}", got[0])),
            Err(e) => errs.push(format!("{stage}: {name}: error {e}")),
        }
    }
    Ok(())
}

#[tokio::test]
async fn typed_values_survive_rewrites() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().to_str().unwrap().to_string();
    let mut errs = Vec::new();
    {
        let db = open(&uri).await?;
        db.schema()
            .label("T")
            .property("k", DataType::Int)
            .property_nullable("n", DataType::Int)
            .property_nullable("d", DataType::Duration)
            .property_nullable("ts", DataType::DateTime)
            .property_nullable("lst", DataType::List(Box::new(DataType::DateTime)))
            .property_nullable("cv", DataType::CypherValue)
            .property_nullable("emb", DataType::Vector { dimensions: 2 })
            .property_nullable("lb", DataType::List(Box::new(DataType::Bool)))
            .property_nullable("li", DataType::List(Box::new(DataType::Int32)))
            .property_nullable("lf", DataType::List(Box::new(DataType::Float32)))
            .property_nullable("ld", DataType::List(Box::new(DataType::Date)))
            .property_nullable("ltm", DataType::List(Box::new(DataType::Time)))
            .property_nullable("ldur", DataType::List(Box::new(DataType::Duration)))
            .property_nullable("lstr", DataType::List(Box::new(DataType::String)))
            .edge_type("R", &["T"], &["T"])
            .property_nullable("d", DataType::Duration)
            .property_nullable("cv", DataType::CypherValue)
            .property_nullable("w", DataType::Int)
            .apply()
            .await?;
        run(
            &db,
            &format!(
                "CREATE (a:T {{k: 1, n: 1, d: {DUR}, ts: {DT}, lst: [{DT}], cv: {DT}, \
                 emb: [1.0, 2.0], x: {DT}, lb: [true, false], li: [1, 2], lf: [1.5], \
                 ld: [{DATE}], ltm: [{TIME}], ldur: [{DUR}], lstr: ['a']}})-[:R {{d: {DUR}, cv: {DT}, w: 1}}]->(:T {{k: 2}})"
            ),
        )
        .await?;
        check(&db, "unflushed", &mut errs).await?;
        db.flush().await?;
        check(&db, "flushed", &mut errs).await?;
        run(&db, "MATCH (t:T {k: 1}) SET t.n = 2").await?;
        run(&db, "MATCH (:T {k: 1})-[r:R]->() SET r.w = 2").await?;
        check(&db, "after SET, unflushed", &mut errs).await?;
        db.flush().await?;
        check(&db, "after SET, flushed", &mut errs).await?;
        run(&db, "VACUUM").await?;
        check(&db, "after VACUUM", &mut errs).await?;
        db.shutdown().await?;
    }
    let db = open(&uri).await?;
    check(&db, "reopened", &mut errs).await?;
    assert!(
        errs.is_empty(),
        "{} wrong values:\n{}",
        errs.len(),
        errs.join("\n")
    );
    Ok(())
}
