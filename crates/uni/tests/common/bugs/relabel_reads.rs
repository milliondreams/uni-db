// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A relabeled vertex is read under its new labels, with its properties,
//! before and after a flush and after a reopen.
//!
//! Two reads disagreed with the relabel:
//! - Before the flush, `MATCH (n:New)` found a flushed vertex relabeled to
//!   `New` with every property NULL: L0 held its new labels but no
//!   properties, and the flushed ones were not read.
//! - After the flush, `MATCH (n:Old)` on an undeclared `Old` still found the
//!   vertex: the scan filtered rows on the label before picking each
//!   vertex's latest row, so the older row, which carried `Old`, matched.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(relabel_reads)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, Value};

/// Which of the two labels the schema declares.
#[derive(Clone, Copy, Debug)]
struct Decl {
    old: bool,
    new: bool,
}

const ALL: [Decl; 4] = [
    Decl {
        old: false,
        new: false,
    },
    Decl {
        old: true,
        new: false,
    },
    Decl {
        old: false,
        new: true,
    },
    Decl {
        old: true,
        new: true,
    },
];

fn config() -> UniConfig {
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..Default::default()
    }
}

async fn open(uri: &str) -> Result<Uni> {
    Ok(Uni::open(uri).config(config()).build().await?)
}

async fn declare(db: &Uni, decl: Decl) -> Result<()> {
    for (label, declared) in [("Old", decl.old), ("New", decl.new)] {
        if declared {
            db.schema()
                .label(label)
                .property("id", DataType::Int)
                .property_nullable("body", DataType::String)
                .property_nullable("at", DataType::DateTime)
                .apply()
                .await?;
        }
    }
    Ok(())
}

async fn run(db: &Uni, stmt: &str) -> Result<()> {
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

async fn rows(db: &Uni, query: &str) -> Result<Vec<Vec<Value>>> {
    Ok(db
        .session()
        .query(query)
        .await?
        .rows()
        .iter()
        .map(|r| r.values().to_vec())
        .collect())
}

fn sorted_labels(v: &Value) -> Vec<String> {
    let Value::List(items) = v else {
        panic!("labels: {v:?}")
    };
    let mut out: Vec<String> = items
        .iter()
        .map(|l| match l {
            Value::String(s) => s.clone(),
            other => panic!("label: {other:?}"),
        })
        .collect();
    out.sort();
    out
}

/// Records a mismatch instead of stopping, so one run shows every failing read.
macro_rules! expect_eq {
    ($errs:expr, $got:expr, $want:expr, $($ctx:tt)+) => {{
        let (got, want) = (&$got, &$want);
        if got != want {
            $errs.push(format!("{}: got {:?}, want {:?}", format!($($ctx)+), got, want));
        }
    }};
}

const AT: &str = "datetime('2024-01-02T03:04:05Z')";

/// Every read of vertex 1 agrees with `labels`, and it keeps its properties
/// and their types.
async fn check(db: &Uni, labels: &[&str], stage: &str, errs: &mut Vec<String>) -> Result<()> {
    let want: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
    let at = rows(db, &format!("RETURN {AT}")).await?.remove(0).remove(0);
    let row = vec![Value::Int(1), Value::String("kept".to_string()), at.clone()];
    let vid = rows(db, "MATCH (n) WHERE n.id = 1 RETURN id(n)").await?;
    for label in ["Old", "New"] {
        let found = rows(
            db,
            &format!("MATCH (n:{label}) WHERE n.id = 1 RETURN n.id, n.body, n.at"),
        )
        .await?;
        let expect = if labels.contains(&label) {
            vec![row.clone()]
        } else {
            Vec::new()
        };
        expect_eq!(errs, found, expect, "{stage}: MATCH (n:{label})");
        if let Some(Value::Int(v)) = vid.first().and_then(|r| r.first()) {
            let by_id = rows(
                db,
                &format!("MATCH (n:{label}) WHERE id(n) = {v} RETURN n.id, n.body, n.at"),
            )
            .await?;
            expect_eq!(errs, by_id, expect, "{stage}: MATCH (n:{label}) by id");
        } else {
            errs.push(format!("{stage}: id(n): {vid:?}"));
        }
        let all = rows(db, &format!("MATCH (n:{label}) RETURN n.id ORDER BY n.id")).await?;
        let mut expect_all = vec![vec![Value::Int(2)]];
        if labels.contains(&label) {
            expect_all.insert(0, vec![Value::Int(1)]);
        }
        if label == "New" {
            expect_all.retain(|r| r[0] != Value::Int(2));
        }
        expect_eq!(errs, all, expect_all, "{stage}: every :{label}");
    }
    let found = rows(
        db,
        "MATCH (n) WHERE n.id = 1 RETURN labels(n), n.body, n.at",
    )
    .await?;
    if found.len() != 1 {
        errs.push(format!("{stage}: unlabeled match: {found:?}"));
        return Ok(());
    }
    expect_eq!(
        errs,
        sorted_labels(&found[0][0]),
        want,
        "{stage}: labels(n)"
    );
    expect_eq!(
        errs,
        found[0][1],
        Value::String("kept".to_string()),
        "{stage}: n.body"
    );
    expect_eq!(errs, found[0][2], at, "{stage}: n.at");
    Ok(())
}

/// Vertex 1 is created as `:Old`, flushed, then relabeled by `stmt`;
/// vertex 2 stays `:Old` throughout.
async fn relabel(
    decl: Decl,
    setup: &[&str],
    stmt: &str,
    labels: &[&str],
    errs: &mut Vec<String>,
) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().to_str().unwrap().to_string();
    {
        let db = open(&uri).await?;
        declare(&db, decl).await?;
        run(
            &db,
            &format!(
                "CREATE (:Old {{id: 1, body: 'kept', at: {AT}}}), \
                 (:Old {{id: 2, body: 'other'}})"
            ),
        )
        .await?;
        db.flush().await?;
        for step in setup {
            run(&db, step).await?;
            db.flush().await?;
        }
        run(&db, stmt).await?;
        check(&db, labels, &format!("{decl:?} unflushed"), errs).await?;
        db.flush().await?;
        check(&db, labels, &format!("{decl:?} flushed"), errs).await?;
        db.shutdown().await?;
    }
    let db = open(&uri).await?;
    check(&db, labels, &format!("{decl:?} reopened"), errs).await
}

#[tokio::test]
async fn relabel_reads_after_a_move() -> Result<()> {
    let mut errs = Vec::new();
    for decl in ALL {
        relabel(
            decl,
            &[],
            "MATCH (n:Old {id: 1}) REMOVE n:Old SET n:New",
            &["New"],
            &mut errs,
        )
        .await?;
    }
    assert!(
        errs.is_empty(),
        "{} wrong reads:\n{}",
        errs.len(),
        errs.join("\n")
    );
    Ok(())
}

#[tokio::test]
async fn relabel_reads_after_an_added_label() -> Result<()> {
    let mut errs = Vec::new();
    for decl in ALL {
        relabel(
            decl,
            &[],
            "MATCH (n:Old {id: 1}) SET n:New",
            &["New", "Old"],
            &mut errs,
        )
        .await?;
    }
    assert!(
        errs.is_empty(),
        "{} wrong reads:\n{}",
        errs.len(),
        errs.join("\n")
    );
    Ok(())
}

/// A label removed in one flush window and added back in the next.
#[tokio::test]
async fn relabel_reads_after_a_label_comes_back() -> Result<()> {
    let mut errs = Vec::new();
    for decl in ALL {
        relabel(
            decl,
            &[
                "MATCH (n:Old {id: 1}) SET n:New",
                "MATCH (n:Old {id: 1}) REMOVE n:New",
            ],
            "MATCH (n:Old {id: 1}) SET n:New",
            &["New", "Old"],
            &mut errs,
        )
        .await?;
    }
    assert!(
        errs.is_empty(),
        "{} wrong reads:\n{}",
        errs.len(),
        errs.join("\n")
    );
    Ok(())
}
