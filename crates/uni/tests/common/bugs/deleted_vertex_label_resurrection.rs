// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression (#140): traversal label resolution must not resurrect the labels
//! of a flushed-then-deleted vertex from the stale persisted `VidLabelsIndex`.
//!
//! `GraphExecutionContext::resolve_vertex_labels` chained the L0 label lookup
//! and then the persisted `VidLabelsIndex`, consulting neither `vertex_tombstones`.
//! A vertex flushed with `["Person"]` and then deleted in the current (unflushed)
//! L0 has its tombstone recorded and is dropped from the L0 label index, but its
//! persisted index entry survives until the *next* flush — so `resolve_vertex_labels`
//! could return the stale `["Person"]` for a deleted vertex.
//!
//! These probes exercise the candidate reach paths end-to-end (traversal output
//! `_labels`, `EXISTS { }`, pattern comprehension) after flushing then deleting a
//! labeled vertex without re-flushing, asserting the deleted vertex is never
//! observed. They pass both before and after the fix: every one of these paths
//! feeds `resolve_vertex_labels` a vid produced by edge traversal, and deleting a
//! vertex cascade-tombstones its incident edges (correctly filtered), so the
//! deleted vertex is never reached to have its labels resolved. They therefore
//! guard that masking (cascade tombstoning + edge-tombstone filtering) rather
//! than the resurrection itself. The direct regression for the guard is the
//! white-box `test_resolve_labels_does_not_resurrect_deleted_vertex` in
//! `uni-query`'s `df_graph::traverse` tests, which calls `resolve_vertex_labels`
//! on a tombstoned vid and is red without the guard.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

/// Build a graph `(a:A)-[:R]->(p:Person)`, flush it, then `DETACH DELETE` the
/// `Person` in a committed-but-unflushed transaction. Returns the open db.
async fn flushed_then_deleted_person() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("name", DataType::String)
        .label("Person")
        .property("name", DataType::String)
        .edge_type("R", &["A"], &["Person"])
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (a:A {name: 'a'})-[:R]->(p:Person {name: 'p'})")
        .await?;
    tx.commit().await?;
    db.flush().await?; // a, p, and edge now live in Lance; index holds p -> ["Person"].

    let tx = session.tx().await?;
    // DETACH DELETE tombstones p and cascade-tombstones the incident edge, all
    // in the current L0 — deliberately NOT re-flushed, so the persisted index
    // entry for p survives.
    tx.execute("MATCH (p:Person {name: 'p'}) DETACH DELETE p")
        .await?;
    tx.commit().await?;

    Ok(db)
}

/// Traversal output `_labels`: the deleted `Person` must not surface via a
/// single-hop labeled traversal.
#[tokio::test]
async fn deleted_person_not_seen_via_traversal() -> Result<()> {
    let db = flushed_then_deleted_person().await?;

    let result = db
        .session()
        .query("MATCH (a:A)-[:R]->(m:Person) RETURN labels(m) AS labels")
        .await?;
    assert_eq!(
        result.len(),
        0,
        "deleted Person must not be reached by traversal; got {} rows",
        result.len()
    );

    db.shutdown().await?;
    Ok(())
}

/// `EXISTS { }` (pattern_exists): the predicate must be false once the only
/// `:Person` neighbor has been deleted.
#[tokio::test]
async fn deleted_person_not_seen_via_exists() -> Result<()> {
    let db = flushed_then_deleted_person().await?;

    let result = db
        .session()
        .query("MATCH (a:A) WHERE EXISTS { MATCH (a)-[:R]->(m:Person) } RETURN a.name AS name")
        .await?;
    let names: Vec<String> = result
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert!(
        names.is_empty(),
        "EXISTS must be false after the :Person neighbor is deleted; got {names:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// Pattern comprehension: collecting labels of `:Person` neighbors must yield
/// nothing for the deleted vertex.
#[tokio::test]
async fn deleted_person_not_seen_via_pattern_comprehension() -> Result<()> {
    let db = flushed_then_deleted_person().await?;

    let result = db
        .session()
        .query("MATCH (a:A) RETURN [(a)-[:R]->(m:Person) | m.name] AS ms")
        .await?;
    for row in result.rows() {
        let ms: Vec<String> = row.get("ms").unwrap_or_default();
        assert!(
            ms.is_empty(),
            "pattern comprehension must not resurrect the deleted Person; got {ms:?}"
        );
    }

    db.shutdown().await?;
    Ok(())
}
