// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression: traversal `_labels` columns must survive a flush to Lance.
//!
//! Several traversal output builders resolved a target vertex's `_labels`
//! column from the in-memory L0 buffers only. Once a vertex is flushed to
//! Lance (the auto-flush, an explicit `db.flush()`, or fork branching, which
//! flushes all data before branching), its labels live only in the persisted
//! `VidLabelsIndex`, so an L0-only read returned an EMPTY label set. The fix
//! routes every output builder through `resolve_vertex_labels`, which consults
//! the L0 chain and then the persisted index.
//!
//! These tests exercise the three affected code paths end-to-end through the
//! public Cypher API, each after forcing the target out of L0:
//! - single-hop relationship scan (sync fast-path),
//! - schema-aware variable-length path,
//! - schemaless variable-length path,
//! plus fork, multi-label, mixed-provenance, and under-load scenarios.
//!
//! Each test uses an UNLABELED (or extra-labeled) traversal target so the
//! assertion depends on index resolution, not on the scope-label fallback —
//! otherwise a still-broken build could pass by echoing the `(m:Label)` filter.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Schema with an edge `R` from `A` to `B`/`C`/`D` (all label-only except `A`).
async fn db_with_schema() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("name", DataType::String)
        .label("B")
        .label("C")
        .label("D")
        .edge_type("R", &["A"], &["B", "C", "D"])
        .apply()
        .await?;
    Ok(db)
}

/// Read a `Vec<String>` label column from the single result row.
fn labels_of(result: &uni_db::QueryResult, col: &str) -> Vec<String> {
    result.rows()[0].get::<Vec<String>>(col).unwrap_or_default()
}

/// T1 — single-hop sync fast-path resolves labels after a flush.
///
/// `MATCH (a:A)-[:R]->(m) RETURN labels(m)` is property-less, so it routes to
/// the synchronous output builder. The target `(m)` is unlabeled, so `["B"]`
/// can only come from the persisted index.
#[tokio::test]
async fn t1_sync_single_hop_labels_after_flush() -> Result<()> {
    let db = db_with_schema().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (a:A {name: 'a'})-[:R]->(b:B)").await?;
    tx.commit().await?;
    db.flush().await?;

    let result = db
        .session()
        .query("MATCH (a:A)-[:R]->(m) RETURN labels(m) AS labels")
        .await?;
    assert_eq!(result.len(), 1, "expected one relationship match");
    let labels = labels_of(&result, "labels");
    assert_eq!(
        labels,
        vec!["B".to_string()],
        "sync path must resolve target labels from the persisted index after flush"
    );

    db.shutdown().await?;
    Ok(())
}

/// T2 — schema-aware variable-length path keeps persisted labels after a flush.
///
/// The target carries an extra label (`B:X`) beyond the scoping label; before
/// the fix the schema-aware VLP builder fell back to the scope label and
/// dropped `X` for a Lance-only vertex.
#[tokio::test]
async fn t2_schema_aware_vlp_labels_after_flush() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("name", DataType::String)
        .label("B")
        .label("X")
        .edge_type("R", &["A"], &["B", "X"])
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (a:A {name: 'a'})-[:R]->(b:B:X)").await?;
    tx.commit().await?;
    db.flush().await?;

    let result = db
        .session()
        .query("MATCH (a:A)-[:R*1..2]->(m) RETURN labels(m) AS labels")
        .await?;
    assert_eq!(result.len(), 1, "expected one variable-length match");
    let labels = labels_of(&result, "labels");
    assert!(
        labels.contains(&"B".to_string()) && labels.contains(&"X".to_string()),
        "schema-aware VLP must keep all persisted labels after flush; got {labels:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// T3 — schemaless variable-length path resolves labels after a flush.
///
/// No schema is declared, so the edge type `R` is schemaless and the traversal
/// uses the schemaless Main-VLP builder, which previously read L0-only labels
/// with no fallback at all — the worst case, returning an empty label set
/// after a flush. `labels(m)` does not plan over schemaless VLP output, so the
/// target's labels are observed by reconstructing the endpoint node `m`
/// (assembled from its `_labels` column), which is exactly what this fix
/// populates.
#[tokio::test]
async fn t3_schemaless_vlp_labels_after_flush() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (a:A)-[:R]->(b:B)").await?;
    tx.commit().await?;
    db.flush().await?;

    let result = db
        .session()
        .query("MATCH (a:A)-[:R*1..2]->(m) RETURN m")
        .await?;
    assert_eq!(result.len(), 1, "expected one variable-length match");
    let labels = match result.rows()[0].value("m") {
        Some(Value::Node(node)) => node.labels.clone(),
        other => panic!("expected a Node for m, got {other:?}"),
    };
    assert!(
        labels.contains(&"B".to_string()),
        "schemaless VLP must resolve endpoint labels from the persisted index after flush; got {labels:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// T4 — labels resolve on a fork (deterministic: forks flush to Lance).
///
/// A fork branches after all data is flushed to Lance, so the target's labels
/// live only in the persisted index — no auto-flush timing needed to reproduce.
#[tokio::test]
async fn t4_fork_single_hop_labels() -> Result<()> {
    let db = db_with_schema().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (a:A {name: 'a'})-[:R]->(b:B)").await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = session.fork("labels_fork").await?;
    let result = forked
        .query("MATCH (a:A)-[:R]->(m) RETURN labels(m) AS labels")
        .await?;
    assert_eq!(result.len(), 1, "fork should see the flushed relationship");
    let labels = labels_of(&result, "labels");
    assert_eq!(
        labels,
        vec!["B".to_string()],
        "fork traversal must resolve target labels from the persisted index"
    );

    db.shutdown().await?;
    Ok(())
}

/// T5 — a multi-label target returns all its labels after a flush.
#[tokio::test]
async fn t5_multi_label_target_after_flush() -> Result<()> {
    let db = db_with_schema().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (a:A {name: 'a'})-[:R]->(b:B:C)").await?;
    tx.commit().await?;
    db.flush().await?;

    let result = db
        .session()
        .query("MATCH (a:A)-[:R]->(m) RETURN labels(m) AS labels")
        .await?;
    let labels = labels_of(&result, "labels");
    assert!(
        labels.contains(&"B".to_string()) && labels.contains(&"C".to_string()),
        "multi-label target must return both labels after flush; got {labels:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// T6 — mixed provenance in one batch: flushed and L0-only targets align.
///
/// Two targets are flushed to Lance; a third is created afterward and lives
/// only in L0. Every row must carry its own correct, non-empty label.
#[tokio::test]
async fn t6_mixed_provenance_row_alignment() -> Result<()> {
    let db = db_with_schema().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (a:A {name: 'p1'})-[:R]->(b:B)").await?;
    tx.execute("CREATE (a:A {name: 'p2'})-[:R]->(c:C)").await?;
    tx.commit().await?;
    db.flush().await?; // p1->B and p2->C now live only in Lance.

    let tx = session.tx().await?;
    tx.execute("CREATE (a:A {name: 'p3'})-[:R]->(d:D)").await?;
    tx.commit().await?; // p3->D stays in L0 (unflushed).

    let result = db
        .session()
        .query("MATCH (a:A)-[:R]->(m) RETURN a.name AS src, labels(m) AS labels")
        .await?;
    assert_eq!(result.len(), 3, "expected three relationship matches");

    let mut seen = std::collections::HashMap::new();
    for row in result.rows() {
        let src: String = row.get("src")?;
        let labels: Vec<String> = row.get("labels").unwrap_or_default();
        seen.insert(src, labels);
    }
    assert_eq!(
        seen.get("p1"),
        Some(&vec!["B".to_string()]),
        "p1 -> B (flushed)"
    );
    assert_eq!(
        seen.get("p2"),
        Some(&vec!["C".to_string()]),
        "p2 -> C (flushed)"
    );
    assert_eq!(
        seen.get("p3"),
        Some(&vec!["D".to_string()]),
        "p3 -> D (L0-only)"
    );

    db.shutdown().await?;
    Ok(())
}

/// T7 — labels stay correct under repeated flush churn ("under load").
///
/// Each iteration commits a new edge and forces an explicit flush, so every
/// target is pushed to Lance (out of L0) before the traversal reads it — the
/// bug's precondition. As rows accumulate the scan spans many flushed targets;
/// none may resolve to an empty label set. Turns the "intermittent under
/// parallel load" symptom into a deterministic assertion.
#[tokio::test]
async fn t7_repeated_flush_stress_labels_never_empty() -> Result<()> {
    let db = db_with_schema().await?;
    let session = db.session();

    for i in 0..8 {
        let tx = session.tx().await?;
        tx.execute(&format!("CREATE (a:A {{name: 'a{i}'}})-[:R]->(b:B)"))
            .await?;
        tx.commit().await?;
        // Force the just-created target out of L0 into Lance before reading.
        db.flush().await?;

        let result = db
            .session()
            .query("MATCH (a:A)-[:R]->(m) RETURN labels(m) AS labels")
            .await?;
        assert_eq!(
            result.len(),
            i + 1,
            "iteration {i}: one row per created edge"
        );
        for row in result.rows() {
            let labels: Vec<String> = row.get("labels").unwrap_or_default();
            assert!(
                labels.contains(&"B".to_string()),
                "iteration {i}: every flushed target must resolve label B, got {labels:?}"
            );
        }
    }

    db.shutdown().await?;
    Ok(())
}
