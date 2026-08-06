// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Coverage for **Cypher relationship uniqueness** (relationship isomorphism):
//! within a single `MATCH`, no relationship may be traversed more than once.
//!
//! Added because an audit of the fail-open surface found that the four
//! traversal operators all resolved their edge-ID columns with
//! `filter_map(... downcast::<UInt64Array>)` — silently dropping any column
//! they could not read, which disables the uniqueness check for that column
//! and lets a path reuse an edge. That degradation turned out to be
//! unreachable today (every `_eid` producer declares `UInt64`, and the column
//! names are harvested from the operator's own input plan), so it was converted
//! to a hard error rather than left as a latent hazard.
//!
//! The more surprising finding was that **the invariant itself had no
//! end-to-end test** — only schema-shape unit tests on the operators. A
//! regression would have been caught by nothing. This file closes that gap; it
//! is not a repro for a live bug.
//!
//! The undirected and variable-length arms matter most: `-[r]-` over a single
//! edge is the classic way to get a spurious `A->B->A`, and variable-length
//! paths are where uniqueness is enforced by a different mechanism
//! (per-path `edge_path.contains(eid)`) than the cross-element check.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(relationship_uniqueness)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

/// Two nodes joined by exactly ONE edge: `a -[:R]-> b`.
///
/// One edge is the sharpest fixture — any result that traverses two
/// relationships must have reused the only one that exists.
async fn setup_single_edge() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .done()
        .edge_type("R", &["N"], &["N"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:N {name: 'a'})").await?;
    tx.execute("CREATE (:N {name: 'b'})").await?;
    tx.execute("MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) CREATE (a)-[:R]->(b)")
        .await?;
    tx.commit().await?;
    Ok(db)
}

async fn count(db: &Uni, cypher: &str) -> Result<i64> {
    let res = db.session().query(cypher).await?;
    Ok(res.rows()[0].get::<i64>("c")?)
}

/// Two undirected hops over a graph with a single edge must yield nothing:
/// `a -r- b -r2- ?` can only continue by re-traversing `r`, which Cypher
/// forbids.
///
/// Without uniqueness this returns 2 (`a->b->a` and `b->a->b`).
#[tokio::test]
async fn relationship_uniqueness_undirected_two_hops_cannot_reuse_edge() -> Result<()> {
    let db = setup_single_edge().await?;
    let c = count(
        &db,
        "MATCH (x:N)-[r1:R]-(y:N)-[r2:R]-(z:N) RETURN count(*) AS c",
    )
    .await?;
    assert_eq!(
        c, 0,
        "a two-hop pattern over a single edge must reuse that edge, which \
         relationship uniqueness forbids"
    );
    Ok(())
}

/// The same shape with two *distinct* edges must still return results — this
/// pins that the check excludes only edge REUSE, not legitimate two-hop paths.
/// Without it, a fix that simply returned nothing would look correct above.
#[tokio::test]
async fn relationship_uniqueness_control_distinct_edges_still_match() -> Result<()> {
    let db = setup_single_edge().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:N {name: 'c'})").await?;
    tx.execute("MATCH (b:N {name: 'b'}), (c:N {name: 'c'}) CREATE (b)-[:R]->(c)")
        .await?;
    tx.commit().await?;

    // a-b-c and c-b-a: two distinct edges, traversed undirected from either end.
    let c = count(
        &db,
        "MATCH (x:N)-[r1:R]-(y:N)-[r2:R]-(z:N) RETURN count(*) AS c",
    )
    .await?;
    assert_eq!(
        c, 2,
        "two distinct edges form a legitimate two-hop path in both directions"
    );
    Ok(())
}

/// Cross-pattern uniqueness: two relationship elements in the SAME `MATCH`
/// must not bind the same edge, even when written as separate comma-separated
/// patterns rather than a chain.
#[tokio::test]
async fn relationship_uniqueness_across_comma_separated_patterns() -> Result<()> {
    let db = setup_single_edge().await?;
    let c = count(
        &db,
        "MATCH (x:N)-[r1:R]->(y:N), (p:N)-[r2:R]->(q:N) RETURN count(*) AS c",
    )
    .await?;
    assert_eq!(
        c, 0,
        "r1 and r2 are distinct elements of one MATCH and cannot bind the same edge"
    );
    Ok(())
}

/// Variable-length paths enforce uniqueness per path via a different mechanism
/// than the cross-element check, so they need their own arm.
///
/// Over a single edge, a 2-hop VLP would have to walk back over it.
#[tokio::test]
async fn relationship_uniqueness_variable_length_path_cannot_reuse_edge() -> Result<()> {
    let db = setup_single_edge().await?;
    let c = count(&db, "MATCH (x:N)-[:R*2..2]-(z:N) RETURN count(*) AS c").await?;
    assert_eq!(
        c, 0,
        "a 2-hop variable-length path over one edge must re-traverse it"
    );
    Ok(())
}

/// A cycle is the case where reuse is most tempting: `a -> b -> a` exists as a
/// genuine 2-cycle here, so a 2-hop VLP legitimately returns results. This
/// guards against "fixing" uniqueness by over-pruning real cycles.
#[tokio::test]
async fn relationship_uniqueness_control_genuine_cycle_is_not_pruned() -> Result<()> {
    let db = setup_single_edge().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) CREATE (b)-[:R]->(a)")
        .await?;
    tx.commit().await?;

    // Two distinct directed edges a->b and b->a form a real 2-cycle.
    let c = count(&db, "MATCH (x:N)-[:R*2..2]->(z:N) RETURN count(*) AS c").await?;
    assert!(
        c > 0,
        "a genuine 2-cycle uses two DISTINCT edges and must not be pruned; got {c}"
    );
    Ok(())
}
