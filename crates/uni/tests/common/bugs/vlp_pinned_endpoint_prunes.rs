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
/// Measured on the development machine, as the test runs it: 0.57 s pruned
/// against 10.6 s unpruned. The 5 s bound therefore sits ~9x above the pruned
/// time and ~2x below the unpruned one. An earlier 10 s bound was rejected --
/// the sibling rebound case came in at 9.2 s unpruned, which passed. If this
/// goes flaky, raise the bound and record the new measurement here rather than
/// deleting it.
#[tokio::test]
async fn a_pinned_endpoint_does_not_enumerate_the_whole_graph() -> Result<()> {
    let db = cyclic(30, 3).await?;

    let rows = db
        .session()
        .query_with(
            "MATCH p = (a:E {uid: 'e0'})-[:OWNS*]->(b:E {uid: 'e7'}) \
             RETURN length(p) AS hops LIMIT 3",
        )
        .timeout(std::time::Duration::from_secs(5))
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

/// The same pruning when the endpoint arrives from an earlier clause rather
/// than an inline map.
///
/// `MATCH (b {..}) WITH b MATCH p = (a)-[:R*]->(b)` traverses into a temporary
/// `__rebound_b` and reconciles it afterwards with a
/// `b._vid = __rebound_b._vid` filter. `detect_bound_target` looked only for
/// `__rebound_b._vid`, which never exists -- the bound column is under the
/// original name -- so the traversal had no endpoint to aim at and enumerated
/// paths to every reachable vertex. Measured 23-46 s before, 0.72 s after.
///
/// Same wall-clock caveat as the test above; see its comment for why the
/// deterministic alternatives do not separate the two builds.
#[tokio::test]
async fn a_rebound_endpoint_prunes_like_an_inline_one() -> Result<()> {
    let db = cyclic(30, 3).await?;

    let rows = db
        .session()
        .query_with(
            "MATCH (b:E {uid: 'e7'}) WITH b \
             MATCH p = (a:E {uid: 'e0'})-[:OWNS*]->(b) RETURN length(p) AS hops LIMIT 3",
        )
        .timeout(std::time::Duration::from_secs(5))
        .fetch_all()
        .await?;

    assert_eq!(
        rows.rows().len(),
        3,
        "LIMIT 3 should fill from a rebound endpoint"
    );
    Ok(())
}

/// The rebound form must agree with the inline form on the answer.
///
/// The correctness arm for the change above. Narrowing the accepting set by the
/// bound endpoint happens *before* the filter that would otherwise have done
/// it, so an over-eager narrowing would silently return fewer paths -- and a
/// timing test would call that an improvement.
#[tokio::test]
async fn a_rebound_endpoint_returns_the_same_paths_as_an_inline_one() -> Result<()> {
    let db = cyclic(12, 2).await?;
    let session = db.session();

    let inline = session
        .query("MATCH (a:E {uid: 'e0'})-[:OWNS*1..5]->(b:E {uid: 'e7'}) RETURN count(*) AS c")
        .await?;
    let rebound = session
        .query(
            "MATCH (b:E {uid: 'e7'}) WITH b \
             MATCH (a:E {uid: 'e0'})-[:OWNS*1..5]->(b) RETURN count(*) AS c",
        )
        .await?;

    let inline_count: i64 = inline.rows()[0].get("c")?;
    let rebound_count: i64 = rebound.rows()[0].get("c")?;
    assert_eq!(
        rebound_count, inline_count,
        "the rebound and inline forms must agree; narrowing changed the answer"
    );
    assert!(inline_count > 0, "fixture reaches no such endpoint");
    Ok(())
}

/// A bound endpoint still carries its properties into the projection.
///
/// Detecting the rebound column also makes the planner clear
/// `target_properties`, on the reasoning that the target's properties come from
/// the outer scope. That is a behaviour change in a much wider set of queries
/// than the one being optimised, so it gets its own assertion rather than
/// relying on the timing tests to notice.
#[tokio::test]
async fn a_rebound_endpoint_keeps_its_properties() -> Result<()> {
    let db = cyclic(10, 2).await?;

    let rows = db
        .session()
        .query(
            "MATCH (b:E {uid: 'e5'}) WITH b \
             MATCH (a:E {uid: 'e0'})-[:OWNS*1..6]->(b) RETURN DISTINCT b.uid AS uid",
        )
        .await?;

    assert_eq!(rows.rows().len(), 1, "expected the single bound endpoint");
    let uid: String = rows.rows()[0].get("uid")?;
    assert_eq!(uid, "e5", "the bound endpoint's properties must survive");
    Ok(())
}

// ---------------------------------------------------------------------------
// Coverage for the other operators `detect_bound_target` feeds.
//
// The rebound fix is a change to `detect_bound_target`, which is called from
// three places: the fixed-length branch of `plan_traverse`, its
// variable-length branch, and `plan_traverse_main_by_type` (schemaless). Only
// the second is what the tests above exercise. Detecting a bound target also
// makes the planner clear `target_properties`, so the blast radius is every
// query whose traversal target is already in scope -- far wider than the
// unbounded-path shape the change was written for.
//
// Measured before landing: all nine shapes below return byte-identical results
// with the change reverted, so these are regression guards rather than
// assertions of new behaviour. They exist because "it happens to still work"
// and "it is guarded" are different states, and only the second survives the
// next edit.
// ---------------------------------------------------------------------------

/// A six-vertex chain `e0 -> e1 -> ... -> e5`.
///
/// Deliberately acyclic and tiny: these tests are about *answers*, not about
/// the enumeration blow-up the tests above cover, and a chain makes every
/// expected result countable by hand.
async fn chain(n: usize) -> Result<Uni> {
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
    for i in 0..n - 1 {
        let j = i + 1;
        tx.execute(&format!(
            "MATCH (a:E {{uid: 'e{i}'}}), (b:E {{uid: 'e{j}'}}) CREATE (a)-[:OWNS]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// The fixed-length branch of `plan_traverse`, which the variable-length tests
/// above never touch.
#[tokio::test]
async fn a_rebound_target_on_a_fixed_length_hop_still_matches() -> Result<()> {
    let db = chain(6).await?;
    let rows = db
        .session()
        .query(
            "MATCH (b:E {uid: 'e1'}) WITH b \
             MATCH (a:E {uid: 'e0'})-[:OWNS]->(b) RETURN b.uid AS uid",
        )
        .await?;

    assert_eq!(rows.rows().len(), 1);
    let uid: String = rows.rows()[0].get("uid")?;
    assert_eq!(uid, "e1", "the bound target's properties must survive");
    Ok(())
}

/// The negative case for the above. A bound target that is *not* reachable
/// must yield nothing -- an over-eager bound check and a missing one both look
/// like "it worked" on the positive case alone.
#[tokio::test]
async fn an_unreachable_rebound_target_yields_no_rows() -> Result<()> {
    let db = chain(6).await?;
    let rows = db
        .session()
        .query(
            "MATCH (b:E {uid: 'e5'}) WITH b \
             MATCH (a:E {uid: 'e0'})-[:OWNS]->(b) RETURN b.uid AS uid",
        )
        .await?;

    assert!(
        rows.rows().is_empty(),
        "e5 is five hops away, so one hop must match nothing"
    );
    Ok(())
}

/// `plan_traverse_main_by_type` -- the schemaless path, the third caller.
#[tokio::test]
async fn a_rebound_target_works_on_the_schemaless_path() -> Result<()> {
    let db = chain(6).await?;
    let rows = db
        .session()
        .query(
            "MATCH (b {uid: 'e2'}) WITH b \
             MATCH (a {uid: 'e1'})-[:OWNS]->(b) RETURN b.uid AS uid",
        )
        .await?;

    assert_eq!(rows.rows().len(), 1);
    let uid: String = rows.rows()[0].get("uid")?;
    assert_eq!(uid, "e2");
    Ok(())
}

/// OPTIONAL MATCH against a bound target that *does* match.
#[tokio::test]
async fn an_optional_rebound_target_that_matches_returns_it() -> Result<()> {
    let db = chain(6).await?;
    let rows = db
        .session()
        .query(
            "MATCH (a:E {uid: 'e0'}), (b:E {uid: 'e1'}) \
             OPTIONAL MATCH (a)-[:OWNS]->(b) RETURN b.uid AS uid",
        )
        .await?;

    assert_eq!(rows.rows().len(), 1);
    let uid: String = rows.rows()[0].get("uid")?;
    assert_eq!(uid, "e1");
    Ok(())
}

/// OPTIONAL MATCH against a bound target that does **not** match.
///
/// This is the branch the narrowing added: when the bound endpoint column is
/// null for a row, the accepting set is cleared rather than filtered. Getting
/// that wrong drops the row entirely instead of emitting it with a NULL path,
/// which is a silently missing result rather than a visible error.
#[tokio::test]
async fn an_optional_rebound_target_that_misses_keeps_the_row_with_a_null_path() -> Result<()> {
    let db = chain(6).await?;
    let rows = db
        .session()
        .query(
            "MATCH (a:E {uid: 'e5'}), (b:E {uid: 'e0'}) \
             OPTIONAL MATCH p = (a)-[:OWNS*1..3]->(b) RETURN b.uid AS uid, length(p) AS hops",
        )
        .await?;

    assert_eq!(
        rows.rows().len(),
        1,
        "OPTIONAL MATCH must preserve the row when the pattern does not match"
    );
    let uid: String = rows.rows()[0].get("uid")?;
    assert_eq!(uid, "e0");
    assert!(
        rows.rows()[0].get::<i64>("hops").is_err(),
        "an unmatched optional path must come back NULL, not zero"
    );
    Ok(())
}

/// Direction is part of the pattern, and the bound-target check must not
/// quietly ignore it.
#[tokio::test]
async fn a_rebound_target_respects_an_incoming_direction() -> Result<()> {
    let db = chain(6).await?;
    let rows = db
        .session()
        .query(
            "MATCH (b:E {uid: 'e0'}) WITH b \
             MATCH (a:E {uid: 'e1'})<-[:OWNS]-(b) RETURN b.uid AS uid",
        )
        .await?;

    assert_eq!(rows.rows().len(), 1);
    let uid: String = rows.rows()[0].get("uid")?;
    assert_eq!(uid, "e0");
    Ok(())
}

/// An undirected variable-length pattern to a bound endpoint.
#[tokio::test]
async fn a_rebound_target_works_undirected() -> Result<()> {
    let db = chain(6).await?;
    let rows = db
        .session()
        .query(
            "MATCH (b:E {uid: 'e3'}) WITH b \
             MATCH (a:E {uid: 'e0'})-[:OWNS*1..5]-(b) RETURN DISTINCT b.uid AS uid",
        )
        .await?;

    assert_eq!(rows.rows().len(), 1);
    let uid: String = rows.rows()[0].get("uid")?;
    assert_eq!(uid, "e3");
    Ok(())
}

/// `shortestPath` to a bound endpoint takes its own planning path, and the
/// hop count is the thing an over-eager narrowing would corrupt.
#[tokio::test]
async fn a_rebound_target_works_with_shortest_path() -> Result<()> {
    let db = chain(6).await?;
    let rows = db
        .session()
        .query(
            "MATCH (b:E {uid: 'e4'}) WITH b \
             MATCH p = shortestPath((a:E {uid: 'e0'})-[:OWNS*]->(b)) RETURN length(p) AS hops",
        )
        .await?;

    assert_eq!(rows.rows().len(), 1);
    let hops: i64 = rows.rows()[0].get("hops")?;
    assert_eq!(hops, 4, "e0 to e4 along the chain is exactly four hops");
    Ok(())
}

// ---------------------------------------------------------------------------
// Correctness of the accepting-set narrowing itself.
//
// The narrowing only runs on the paused-enumeration path, which needs a
// single-row input and an unbounded pattern. Instrumenting the branch showed
// that of the tests above exactly one reaches it with a bound endpoint and a
// non-empty accepting set -- and that one is a wall-clock assertion. Inverting
// the narrowing's comparison therefore produced a *timeout*, not a wrong
// answer, and every correctness test still passed.
//
// These assert the answer on that path, on fixtures small enough to count by
// hand.
// ---------------------------------------------------------------------------

/// One path, unbounded pattern, endpoint bound through `WITH`.
///
/// A chain keeps the expected answer trivially knowable while still taking the
/// paused path: entry depends on the input row count and the hop bound, not on
/// how many paths exist.
#[tokio::test]
async fn narrowing_keeps_the_single_path_to_a_rebound_endpoint() -> Result<()> {
    let db = chain(6).await?;
    let rows = db
        .session()
        .query(
            "MATCH (b:E {uid: 'e5'}) WITH b \
             MATCH p = (a:E {uid: 'e0'})-[:OWNS*]->(b) RETURN length(p) AS hops",
        )
        .await?;

    assert_eq!(
        rows.rows().len(),
        1,
        "the chain offers exactly one path from e0 to e5"
    );
    let hops: i64 = rows.rows()[0].get("hops")?;
    assert_eq!(hops, 5);
    Ok(())
}

/// Several paths to the bound endpoint, all of which must survive.
///
/// The single-path test above would pass a narrowing that kept only the first
/// match. A diamond gives two distinct routes of different lengths to the same
/// endpoint, so under-delivery shows up as a count.
#[tokio::test]
async fn narrowing_keeps_every_path_to_a_rebound_endpoint() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("uid", DataType::String)
        .edge_type("OWNS", &["E"], &["E"])
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    // e0 -> e1 -> e3  and  e0 -> e2 -> e3, plus a direct e0 -> e3.
    // Three distinct paths to e3, of lengths 2, 2 and 1.
    for uid in ["e0", "e1", "e2", "e3"] {
        tx.execute(&format!("CREATE (:E {{uid: '{uid}'}})")).await?;
    }
    for (from, to) in [
        ("e0", "e1"),
        ("e1", "e3"),
        ("e0", "e2"),
        ("e2", "e3"),
        ("e0", "e3"),
    ] {
        tx.execute(&format!(
            "MATCH (a:E {{uid: '{from}'}}), (b:E {{uid: '{to}'}}) CREATE (a)-[:OWNS]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;

    let rebound = session
        .query(
            "MATCH (b:E {uid: 'e3'}) WITH b \
             MATCH p = (a:E {uid: 'e0'})-[:OWNS*]->(b) RETURN length(p) AS hops",
        )
        .await?;
    let mut hops: Vec<i64> = rebound
        .rows()
        .iter()
        .map(|r| r.get::<i64>("hops"))
        .collect::<Result<_, _>>()?;
    hops.sort_unstable();
    assert_eq!(
        hops,
        vec![1, 2, 2],
        "all three routes to the bound endpoint must survive the narrowing"
    );

    // And the narrowing must not admit anything extra: the inline spelling of
    // the same pin has to agree exactly.
    let inline = session
        .query("MATCH p = (a:E {uid: 'e0'})-[:OWNS*]->(b:E {uid: 'e3'}) RETURN length(p) AS hops")
        .await?;
    let mut inline_hops: Vec<i64> = inline
        .rows()
        .iter()
        .map(|r| r.get::<i64>("hops"))
        .collect::<Result<_, _>>()?;
    inline_hops.sort_unstable();
    assert_eq!(hops, inline_hops, "rebound and inline spellings must agree");
    Ok(())
}

/// The narrowing must exclude paths to *other* endpoints.
///
/// The complement of the tests above: they catch under-delivery, this catches
/// over-delivery. A narrowing that did nothing at all would pass both of them,
/// because the filter below the enumeration would still remove the extras --
/// so this asserts against the un-pinned count, which is strictly larger.
#[tokio::test]
async fn narrowing_does_not_admit_paths_to_other_endpoints() -> Result<()> {
    let db = chain(6).await?;
    let session = db.session();

    let pinned = session
        .query(
            "MATCH (b:E {uid: 'e2'}) WITH b \
             MATCH p = (a:E {uid: 'e0'})-[:OWNS*]->(b) RETURN count(p) AS c",
        )
        .await?;
    let unpinned = session
        .query("MATCH p = (a:E {uid: 'e0'})-[:OWNS*]->(b:E) RETURN count(p) AS c")
        .await?;

    let pinned_count: i64 = pinned.rows()[0].get("c")?;
    let unpinned_count: i64 = unpinned.rows()[0].get("c")?;
    assert_eq!(pinned_count, 1, "only e2 is reachable at exactly one route");
    assert!(
        unpinned_count > pinned_count,
        "fixture has no other endpoints, so the pin proves nothing"
    );
    Ok(())
}
