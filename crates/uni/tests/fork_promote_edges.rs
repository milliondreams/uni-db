// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 6b — edge promotion via `PromotePattern::edge_type`.
//!
//! Phase 6 MVP did not promote edges; it counted them and warned.
//! Phase 6b adds explicit edge patterns with `(src_uid, dst_uid, type)`
//! dedup. Tests:
//! 1. Basic happy path: vertex + edge pattern in one call lands both
//!    endpoints and the edge on primary.
//! 2. Skip-no-endpoint: edge whose source isn't on primary is counted
//!    in `edges_skipped_no_endpoint`.
//! 3. Dedup: re-running an edge promote against an already-promoted
//!    edge counts it in `edges_skipped_duplicate` rather than
//!    inserting a parallel edge.

use anyhow::Result;
use uni_db::api::fork_diff::PromotePattern;
use uni_db::{DataType, Uni};

async fn build_test_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Int64)
        .apply()
        .await?;
    Ok(db)
}

#[tokio::test]
async fn promote_edges_lands_both_endpoints_and_edge() -> Result<()> {
    let db = build_test_db().await?;
    let session = db.session();

    // Seed primary with anchor row so the fork has something to inherit.
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Anchor'})").await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let fork = session.fork("rel_drop").await?;
        let tx = fork.tx().await?;
        tx.execute(
            "CREATE (:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(:Person {name: 'Bob'})",
        )
        .await?;
        tx.commit().await?;
    }

    let report = db
        .promote_from_fork(
            "rel_drop",
            &[
                PromotePattern::label("Person"),
                PromotePattern::edge_type("KNOWS"),
            ],
        )
        .await?;

    assert!(
        report.vertices_inserted >= 2,
        "expected Alice and Bob promoted, got {:?}",
        report
    );
    assert_eq!(
        report.edges_inserted, 1,
        "expected exactly one KNOWS edge promoted, got {:?}",
        report
    );
    assert_eq!(report.edges_skipped_no_endpoint, 0);
    assert_eq!(report.edges_skipped_duplicate, 0);

    // Confirm primary now sees the edge.
    let rs = session
        .query("MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN r.since AS since")
        .await?;
    assert_eq!(rs.rows().len(), 1);
    let since: i64 = rs.rows()[0].get("since")?;
    assert_eq!(since, 2020);

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn promote_edges_skips_when_endpoint_absent_on_primary() -> Result<()> {
    let db = build_test_db().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Anchor'})").await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let fork = session.fork("orphan_edge").await?;
        let tx = fork.tx().await?;
        tx.execute(
            "CREATE (:Person {name: 'Orphan-Src'})-[:KNOWS {since: 1}]->(:Person {name: 'Orphan-Dst'})",
        )
        .await?;
        tx.commit().await?;
    }

    // Promote ONLY the edge — endpoints stay fork-local.
    let report = db
        .promote_from_fork(
            "orphan_edge",
            &[PromotePattern::edge_type("KNOWS")],
        )
        .await?;
    assert_eq!(report.edges_inserted, 0);
    assert_eq!(
        report.edges_skipped_no_endpoint, 1,
        "expected the orphan edge to be counted: {:?}",
        report
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn promote_edges_dedupes_existing_edge() -> Result<()> {
    let db = build_test_db().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(
        "CREATE (:Person {name: 'Alice'})-[:KNOWS {since: 1}]->(:Person {name: 'Bob'})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    {
        // Fork creates the same shape — endpoints have the same UID
        // (same content), and the edge already exists on primary.
        let fork = session.fork("dup_edge").await?;
        let tx = fork.tx().await?;
        tx.execute(
            "CREATE (:Person {name: 'Alice'})-[:KNOWS {since: 1}]->(:Person {name: 'Bob'})",
        )
        .await?;
        tx.execute("CREATE (:Person {name: 'NewKid'})").await?;
        tx.commit().await?;
    }

    let report = db
        .promote_from_fork(
            "dup_edge",
            &[
                PromotePattern::label("Person"),
                PromotePattern::edge_type("KNOWS"),
            ],
        )
        .await?;

    assert_eq!(
        report.edges_inserted, 0,
        "edge already exists on primary, should not insert: {:?}",
        report
    );
    assert!(
        report.edges_skipped_duplicate >= 1,
        "duplicate dedup counter should fire: {:?}",
        report
    );

    // Primary still has exactly one Alice→Bob KNOWS edge.
    let rs = session
        .query("MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN count(r) AS c")
        .await?;
    let c: i64 = rs.rows()[0].get("c")?;
    assert_eq!(c, 1, "no parallel edge should be inserted");

    db.shutdown().await?;
    Ok(())
}
