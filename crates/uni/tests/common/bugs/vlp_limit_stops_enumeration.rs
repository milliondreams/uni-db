// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression (#285): a `LIMIT` over an unbounded variable-length pattern must
//! stop the path enumeration, not merely truncate its result.
//!
//! `GraphVariableLengthTraverseExec` used to enumerate every path a source
//! vertex owns inside a single `poll_next`, so `LIMIT 5` cost exactly what no
//! limit cost. On a cyclic graph that set is exponential in the hop bound, and
//! the query died on the memory budget or the deadline with five rows asked for
//! and none delivered. Measured on a 852-entity / 1013-edge sanctions-shaped
//! graph: `LIMIT 5` timed out at 30s before, and answers in ~4s after -- the
//! residue being the breadth-first search itself, which has to run before an
//! accepting endpoint is known.
//!
//! The fix made the enumeration resumable (`pred_dag::PathEnumerator`) and gave
//! the operator somewhere to park a half-drained row, so an unfilled `LIMIT` is
//! ordinary back-pressure: the operator emits one batch, nothing pulls again,
//! and the remaining paths are never walked.
//!
//! These tests assert the *observable* contract -- the right rows come back --
//! rather than a wall-clock figure, which would be a machine-speed assertion.
//! The "did it actually stop early" half is covered at the cursor level by
//! `pred_dag::tests::a_cursor_stopped_early_leaves_the_rest_unproduced`.

// Rust guideline compliant

use anyhow::Result;
use std::collections::HashSet;
use uni_db::{DataType, Uni};

/// Build a directed cycle of `n` nodes plus every chord, which is the shape
/// that makes an unbounded pattern explode: every node reaches every other at
/// unboundedly many depths.
async fn mesh(n: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .edge_type("R", &["N"], &["N"])
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    for i in 0..n {
        tx.execute(&format!("CREATE (:N {{name: 'n{i}'}})")).await?;
    }
    for i in 0..n {
        for j in 0..n {
            if i == j {
                continue;
            }
            tx.execute(&format!(
                "MATCH (a:N {{name: 'n{i}'}}), (b:N {{name: 'n{j}'}}) CREATE (a)-[:R]->(b)"
            ))
            .await?;
        }
    }
    tx.commit().await?;
    Ok(db)
}

/// An unbounded pattern with a `LIMIT` returns exactly that many rows.
///
/// Without the resumable enumeration this does not return a short answer -- it
/// does not return at all within the budget.
#[tokio::test]
async fn unbounded_vlp_with_limit_returns_that_many_rows() -> Result<()> {
    let db = mesh(6).await?;
    for limit in [1usize, 5, 17] {
        let rows = db
            .session()
            .query(&format!(
                "MATCH p = (a:N {{name: 'n0'}})-[:R*]->(b:N) RETURN length(p) AS hops LIMIT {limit}"
            ))
            .await?;
        let rows = rows.rows();
        assert_eq!(
            rows.len(),
            limit,
            "unbounded pattern with LIMIT {limit} returned {} rows",
            rows.len()
        );
    }
    Ok(())
}

/// Pausing the enumeration must not change which paths exist.
///
/// A limit above the full count has to return the whole set, and the set has to
/// match what a bounded pattern covering the same depths returns. This is what
/// catches a cursor that drops a path at a pause boundary or hands the same one
/// over twice -- neither of which a row *count* would show.
#[tokio::test]
async fn paused_enumeration_yields_the_same_path_set() -> Result<()> {
    // Three nodes keeps the trail-bounded set small enough to compare whole.
    let db = mesh(3).await?;

    let bounded = db
        .session()
        .query(
            "MATCH p = (a:N {name: 'n0'})-[:R*1..6]->(b:N) \
             RETURN [x IN nodes(p) | x.name] AS names ORDER BY names",
        )
        .await?;
    // `*` is bounded by the same default hop cap the bounded form spells out,
    // so over this fixture the two must agree exactly.
    let unbounded = db
        .session()
        .query(
            "MATCH p = (a:N {name: 'n0'})-[:R*]->(b:N) \
             RETURN [x IN nodes(p) | x.name] AS names ORDER BY names",
        )
        .await?;

    let bounded = bounded.rows();
    let unbounded = unbounded.rows();
    assert_eq!(
        bounded.len(),
        unbounded.len(),
        "bounded and unbounded forms disagree on path count"
    );

    // Every prefix taken under a LIMIT must be a subset of the full set, with
    // no duplicates introduced by a resumed cursor.
    let full: HashSet<String> = unbounded
        .iter()
        .map(|row| format!("{:?}", row.value("names")))
        .collect();

    let limited = db
        .session()
        .query(
            "MATCH p = (a:N {name: 'n0'})-[:R*]->(b:N) \
             RETURN [x IN nodes(p) | x.name] AS names LIMIT 20",
        )
        .await?;
    let limited = limited.rows();
    assert_eq!(limited.len(), 20, "LIMIT 20 did not fill");
    let seen: Vec<String> = limited
        .iter()
        .map(|row| format!("{:?}", row.value("names")))
        .collect();
    for path in &seen {
        assert!(full.contains(path), "limited run produced an unknown path");
    }
    assert_eq!(
        seen.iter().collect::<HashSet<_>>().len(),
        seen.len(),
        "a resumed cursor handed the same path over twice"
    );
    Ok(())
}

/// A limit that the pattern cannot fill still returns everything it has, and
/// terminates.
#[tokio::test]
async fn limit_above_the_path_count_returns_the_whole_set() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .edge_type("R", &["N"], &["N"])
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    // A simple chain: n0 -> n1 -> n2, so `*` yields exactly two paths from n0.
    tx.execute("CREATE (a:N {name: 'n0'})-[:R]->(b:N {name: 'n1'}), (b)-[:R]->(c:N {name: 'n2'})")
        .await?;
    tx.commit().await?;

    let rows = db
        .session()
        .query("MATCH p = (a:N {name: 'n0'})-[:R*]->(b:N) RETURN length(p) AS hops LIMIT 100")
        .await?;
    assert_eq!(rows.rows().len(), 2, "chain from n0 has exactly two paths");
    Ok(())
}
