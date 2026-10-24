// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #310: `uni.fts.query` scored every unindexed hit 0.5 in 4.1.1, so
//! ranking changed once the rows were flushed / the store reopened.
//!
//! Mirrors the report: on-disk store, a declared label whose `terms` carries
//! an `IndexType::FullText` index declared through the schema builder, about
//! 60 rows written in one transaction, then the same query (1) before any
//! flush, (2) after an explicit flush, (3) after shutdown and reopen. Each
//! stage must return the same ranking and scores, and the scores must not
//! collapse to a constant.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(issue_310)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::api::schema::IndexType;
use uni_db::{DataType, Uni, Value};

const QUERY: &str = "CALL uni.fts.query('ElementVersion', 'terms', 'supplier', 50) \
                     YIELD node, score RETURN node.name AS name, score \
                     ORDER BY score DESC, name";

async fn ranked(db: &Uni) -> Result<Vec<(String, f64)>> {
    let result = db.session().query(QUERY).await?;
    Ok(result
        .rows()
        .iter()
        .map(|r| match (&r.values()[0], &r.values()[1]) {
            (Value::String(n), Value::Float(f)) => (n.clone(), *f),
            other => panic!("row: {other:?}"),
        })
        .collect())
}

fn index_built(db: &Uni) -> bool {
    db.indexes()
        .list(Some("ElementVersion"))
        .into_iter()
        .any(|i| i.metadata().last_built_at.is_some())
}

/// Rows shaped like the report: names that mention `supplier` in different
/// term positions and frequencies, plus unrelated filler.
fn rows() -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = [
        ("SupplierSite", "supplier site"),
        ("SupplierSite.code", "supplier site code"),
        ("SupplierSite#identity", "supplier site identity"),
        ("SupplierSite.supplier", "supplier site supplier"),
        ("SupplierName", "supplier name"),
        ("QualifiedSupplier", "qualified supplier"),
        (
            "Invoice.supplierRef",
            "invoice supplier ref amount currency due date",
        ),
        ("Supplier", "supplier"),
    ]
    .iter()
    .map(|(n, t)| (n.to_string(), t.to_string()))
    .collect();
    for i in 0..52 {
        out.push((
            format!("Filler{i}"),
            format!("customer order line item {i}"),
        ));
    }
    out
}

async fn declare(db: &Uni) -> Result<()> {
    db.schema()
        .label("ElementVersion")
        .property("uid", DataType::String)
        .property("sid", DataType::String)
        .property("kind", DataType::String)
        .property("name", DataType::String)
        .property("terms", DataType::String)
        .property("json", DataType::String)
        .index("terms", IndexType::FullText)
        .apply()
        .await?;
    Ok(())
}

fn print_stage(stage: &str, built: bool, hits: &[(String, f64)]) {
    println!("--- {stage} (index built: {built}) ---");
    for (rank, (name, score)) in hits.iter().enumerate() {
        println!("  {:>2} {name:<24} {score:.6}", rank + 1);
    }
}

fn assert_same(a: &[(String, f64)], b: &[(String, f64)], what: &str) {
    let names = |r: &[(String, f64)]| r.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>();
    assert_eq!(names(a), names(b), "{what}: ranking moved");
    for ((n, x), (_, y)) in a.iter().zip(b) {
        assert!((x - y).abs() < 1e-6, "{what}: {n} scored {x} then {y}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn issue_310_fts_scores_stable_across_flush_and_reopen() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap().to_string();

    let (unflushed, flushed) = {
        let db = Uni::open(&path).build().await?;
        declare(&db).await?;
        let tx = db.session().tx().await?;
        for (i, (name, terms)) in rows().into_iter().enumerate() {
            tx.execute(&format!(
                "CREATE (:ElementVersion {{uid: 'u{i}', sid: 's{i}', kind: 'k', \
                 name: '{name}', terms: '{terms}', json: '{{}}'}})"
            ))
            .await?;
        }
        tx.commit().await?;

        // The issue is about rows the index does not cover yet, so the first
        // stage must really run before any build.
        let built_before = index_built(&db);
        let unflushed = ranked(&db).await?;
        print_stage("unflushed (L0)", built_before, &unflushed);
        assert!(
            !built_before,
            "the full-text index was built before the flush"
        );

        db.flush().await?;
        let flushed = ranked(&db).await?;
        print_stage("after flush", index_built(&db), &flushed);

        db.shutdown().await?;
        (unflushed, flushed)
    };

    let db = Uni::open(&path).build().await?;
    let reopened = ranked(&db).await?;
    print_stage("after reopen", index_built(&db), &reopened);

    assert_eq!(unflushed.len(), 8, "every supplier row must hit");
    let distinct: std::collections::BTreeSet<u64> =
        unflushed.iter().map(|(_, s)| (s * 1e6) as u64).collect();
    assert!(
        distinct.len() > 1,
        "unflushed scores collapsed to a constant: {unflushed:?}"
    );
    assert!(
        unflushed
            .iter()
            .filter(|(_, s)| (s - 0.5).abs() < 1e-9)
            .count()
            < 2,
        "unflushed hits scored the 0.5 placeholder: {unflushed:?}"
    );
    assert_same(&unflushed, &flushed, "unflushed vs flushed");
    assert_same(&flushed, &reopened, "flushed vs reopened");
    Ok(())
}

/// Same rows, but `ElementVersion` is never declared, so `terms` is a
/// schemaless property. `StorageManager::fts_search` scores such a property
/// by word overlap on both sides of a flush (no BM25 statistics), so every
/// single-word hit scores `normalize_bm25(1.0, 1.0) = 0.5`.
///
/// This is the open residual of #310 (P3): it fails on `main` and is the
/// specification for the fix (real BM25 statistics for a schemaless property,
/// or a result marked unranked). Registered in `ON_DEMAND` in
/// `scripts/ci/ignored_tests.py` so the nightly sweep does not run a test
/// known to fail.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "#310 residual: schemaless FTS scores are still a constant 0.5 (word overlap, no BM25 statistics)"]
async fn issue_310_schemaless_fts_scores_are_constant() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).build().await?;
    let tx = db.session().tx().await?;
    for (i, (name, terms)) in rows().into_iter().enumerate() {
        tx.execute(&format!(
            "CREATE (:ElementVersion {{uid: 'u{i}', name: '{name}', terms: '{terms}'}})"
        ))
        .await?;
    }
    tx.commit().await?;
    let unflushed = ranked(&db).await?;
    print_stage("schemaless unflushed", index_built(&db), &unflushed);
    db.flush().await?;
    let flushed = ranked(&db).await?;
    print_stage("schemaless after flush", index_built(&db), &flushed);
    assert_eq!(unflushed.len(), 8, "every supplier row must hit");
    let distinct: std::collections::BTreeSet<u64> =
        flushed.iter().map(|(_, s)| (s * 1e6) as u64).collect();
    assert!(
        distinct.len() > 1,
        "schemaless scores collapsed to a constant: {flushed:?}"
    );
    Ok(())
}
