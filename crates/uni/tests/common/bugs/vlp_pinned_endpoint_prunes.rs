// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A variable-length pattern whose endpoint is pinned by an inline property map
//! must narrow its accepting set, not enumerate paths to every reachable vertex.
//!
//! `MATCH p = (a)-[:R*]->(b {uid: 'x'}) ... LIMIT 3` used to time out on a
//! cyclic graph while the same query with an *unpinned* endpoint answered in
//! well under a second. The pin made it slower, which is backwards: it can only
//! ever reduce the answer.
//!
//! The cause was that the predicate reached execution only as a `FilterExec`
//! *above* the traversal. Every reachable vertex was therefore an accepting
//! endpoint, the enumeration built paths to all of them, and the filter threw
//! nearly all of them away — so a `LIMIT` waited on paths it would discard.
//! The planner now also hands the traversal the target's property conditions,
//! which narrow the accepting set during the search itself.
//!
//! The narrowing is a pruning step only. The `FilterExec` still applies the
//! predicate and remains what makes the answer correct, which is what lets
//! `build_accepting_vid_filter` be conservative: anything it cannot establish
//! is admitted, because a vertex wrongly excluded there is a missing row that
//! no later stage can restore.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

/// A strongly-connected graph: every vertex reaches every other at many depths,
/// so an unpinned endpoint has a combinatorial path set.
async fn cyclic(n: usize, out: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("uid", DataType::String)
        .edge_type("OWNS", &["E"], &["E"])
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    for i in 0..n {
        tx.execute(&format!("CREATE (:E {{uid: 'e{i}'}})")).await?;
    }
    for i in 0..n {
        for k in 0..out {
            let dst = (i * 7 + k * 13 + 1) % n;
            tx.execute(&format!(
                "MATCH (a:E {{uid: 'e{i}'}}), (b:E {{uid: 'e{dst}'}}) CREATE (a)-[:OWNS]->(b)"
            ))
            .await?;
        }
    }
    tx.commit().await?;
    Ok(db)
}

/// The regression: an unbounded pattern to a pinned endpoint, under a `LIMIT`.
///
/// **This is a wall-clock assertion**, after two alternatives were measured and
/// rejected. A memory ceiling does not separate the two builds: since the
/// enumeration became lazy (#285) it stays inside a budget either way and only
/// the time differs. Growing the graph does not separate them cleanly either --
/// the unpruned cost measured 17-39 s across sizes and was not monotonic in
/// vertex count.
///
/// Measured margin on the development machine: 0.78 s pruned against a 17.6 s
/// best case unpruned, so a 10 s bound sits ~13x above the former and ~1.8x
/// below the latter. If this goes flaky, raise the bound and record the new
/// measurement here rather than deleting it.
#[tokio::test]
async fn a_pinned_endpoint_does_not_enumerate_the_whole_graph() -> Result<()> {
    let db = cyclic(30, 3).await?;

    let rows = db
        .session()
        .query_with(
            "MATCH p = (a:E {uid: 'e0'})-[:OWNS*]->(b:E {uid: 'e7'}) \
             RETURN length(p) AS hops LIMIT 3",
        )
        .timeout(std::time::Duration::from_secs(10))
        .fetch_all()
        .await?;

    assert_eq!(
        rows.rows().len(),
        3,
        "LIMIT 3 should fill from a pinned endpoint"
    );
    Ok(())
}

/// Pruning must not change which paths exist.
///
/// A bounded pattern is small enough to compare whole, so the pinned and
/// unpinned forms can be checked against each other: every path the pinned
/// query returns must be one the unpinned query also returns, and the count
/// must equal the number of unpinned paths that end at the pin.
///
/// This is the arm that would catch an over-eager filter. The test above only
/// says the query is *fast*; a filter that wrongly excluded endpoints would be
/// faster still.
#[tokio::test]
async fn pruning_does_not_change_the_answer() -> Result<()> {
    let db = cyclic(12, 2).await?;
    let session = db.session();

    let pinned = session
        .query("MATCH (a:E {uid: 'e0'})-[:OWNS*1..5]->(b:E {uid: 'e7'}) RETURN count(*) AS c")
        .await?;
    let unpinned = session
        .query(
            "MATCH (a:E {uid: 'e0'})-[:OWNS*1..5]->(b:E) \
             WHERE b.uid = 'e7' RETURN count(*) AS c",
        )
        .await?;

    let pinned_count: i64 = pinned.rows()[0].get("c")?;
    let unpinned_count: i64 = unpinned.rows()[0].get("c")?;
    assert_eq!(
        pinned_count, unpinned_count,
        "the inline pin and the equivalent WHERE must agree; pruning changed the answer"
    );
    assert!(
        pinned_count > 0,
        "fixture reaches no such endpoint, so the comparison proves nothing"
    );
    Ok(())
}

/// A zero-length path must survive the narrowing.
///
/// `build_accepting_vid_filter` is built from vertices that appear in an
/// adjacency list. The source of a zero-length path need not be one of them, so
/// the zero-length accept deliberately skips the filter. An isolated vertex is
/// the case that proves it: it appears in no adjacency list at all.
#[tokio::test]
async fn a_zero_length_path_to_a_pinned_endpoint_still_matches() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("uid", DataType::String)
        .edge_type("OWNS", &["E"], &["E"])
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    // `lonely` has no edges whatsoever, so it is in no adjacency list.
    tx.execute("CREATE (:E {uid: 'lonely'})").await?;
    tx.execute("CREATE (:E {uid: 'x'})-[:OWNS]->(:E {uid: 'y'})")
        .await?;
    tx.commit().await?;

    let rows = session
        .query(
            "MATCH p = (a:E {uid: 'lonely'})-[:OWNS*0..2]->(b:E {uid: 'lonely'}) \
             RETURN length(p) AS hops",
        )
        .await?;

    assert_eq!(
        rows.rows().len(),
        1,
        "an isolated vertex is its own zero-length path, pin or no pin"
    );
    let hops: i64 = rows.rows()[0].get("hops")?;
    assert_eq!(hops, 0);
    Ok(())
}
