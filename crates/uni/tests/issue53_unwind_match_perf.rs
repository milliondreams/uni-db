// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Regression test for issue #53: UNWIND + MATCH WHERE id(a) = e.field is
// catastrophically slow because the equi-join predicate cannot be pushed to
// the scan (the right side is a column ref from UNWIND, not a literal/param),
// so the planner produces a CrossJoin of unfiltered scans + post-Filter.
//
// The fix converts Filter(CrossJoin(...)) with equi-join predicates into a
// HashJoinExec at the physical-planning layer, eliminating the cartesian
// blowup. The same fix benefits multi-MATCH joins, WITH-then-MATCH, and
// CALL-YIELD-then-MATCH patterns.

use anyhow::Result;
use std::collections::HashMap;
use std::time::Instant;
use uni_db::{DataType, Uni, Value};

/// Correctness: UNWIND a list of `{src, dst}` maps and CREATE edges via
/// `MATCH WHERE id(a) = e.src AND id(b) = e.dst`. Verify each requested edge
/// exists exactly once.
#[tokio::test]
async fn unwind_match_id_correctness() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Node")
        .property("idx", DataType::Int64)
        .edge_type("LINK", &["Node"], &["Node"])
        .apply()
        .await?;

    // Create 5 nodes and collect their VIDs in idx order.
    let tx = db.session().tx().await?;
    for i in 0..5 {
        tx.execute_with("CREATE (:Node {idx: $i})")
            .param("i", i as i64)
            .run()
            .await?;
    }
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (n:Node) RETURN id(n) AS vid, n.idx AS idx ORDER BY n.idx")
        .await?;
    let vids: Vec<i64> = (0..5).map(|i| result.rows()[i].get::<i64>("vid").unwrap()).collect();

    // Build edge list: 0→1, 1→2, 2→3, 3→4. UNWIND a list of {src, dst} maps.
    let edges: Vec<Value> = (0..4)
        .map(|i| {
            let mut m = HashMap::new();
            m.insert("src".to_string(), Value::Int(vids[i]));
            m.insert("dst".to_string(), Value::Int(vids[i + 1]));
            Value::Map(m)
        })
        .collect();

    let tx = db.session().tx().await?;
    let res = tx
        .execute_with(
            "UNWIND $edges AS e \
             MATCH (a:Node), (b:Node) WHERE id(a) = e.src AND id(b) = e.dst \
             CREATE (a)-[r:LINK]->(b) RETURN id(r) AS eid",
        )
        .param("edges", Value::List(edges))
        .run()
        .await?;
    tx.commit().await?;

    assert_eq!(
        res.affected_rows(),
        4,
        "should have created exactly 4 edges, got {}",
        res.affected_rows()
    );

    // Verify edges are exactly the requested chain.
    let edge_count = db
        .session()
        .query("MATCH ()-[r:LINK]->() RETURN count(r) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    assert_eq!(edge_count, 4);

    let chain = db
        .session()
        .query("MATCH (a:Node)-[:LINK]->(b:Node) RETURN a.idx AS sa, b.idx AS db ORDER BY sa")
        .await?;
    for i in 0..4 {
        assert_eq!(chain.rows()[i].get::<i64>("sa")?, i as i64);
        assert_eq!(chain.rows()[i].get::<i64>("db")?, (i + 1) as i64);
    }

    Ok(())
}

/// Correctness for the broader pattern: `MATCH (a) MATCH (b) WHERE a.k = b.k`.
/// Same root cause (column-ref equi-join across CrossJoin). Verifies the fix
/// doesn't regress this case while making it efficient.
#[tokio::test]
async fn multi_match_equi_join_correctness() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Left")
        .property("k", DataType::Int64)
        .label("Right")
        .property("k", DataType::Int64)
        .property("v", DataType::String)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..5 {
        tx.execute_with("CREATE (:Left {k: $k})")
            .param("k", i as i64)
            .run()
            .await?;
        tx.execute_with("CREATE (:Right {k: $k, v: $v})")
            .param("k", i as i64)
            .param("v", format!("v{i}"))
            .run()
            .await?;
    }
    tx.commit().await?;

    let result = db
        .session()
        .query(
            "MATCH (a:Left) MATCH (b:Right) WHERE a.k = b.k \
             RETURN a.k AS k, b.v AS v ORDER BY k",
        )
        .await?;
    assert_eq!(result.rows().len(), 5);
    for i in 0..5 {
        assert_eq!(result.rows()[i].get::<i64>("k")?, i as i64);
        assert_eq!(result.rows()[i].get::<String>("v")?, format!("v{i}"));
    }

    Ok(())
}

/// Performance regression guard: 600 edges via UNWIND must complete in well
/// under the per-test timeout. The bug had this at ~138 seconds; the fix
/// should bring it to ~1-2 seconds.
///
/// Marked `#[ignore]` so it doesn't run by default — invoke explicitly with
/// `cargo nextest run --test issue53_unwind_match_perf -- --ignored` or
/// `--run-ignored all`.
#[tokio::test]
#[ignore = "perf regression guard, run explicitly"]
async fn unwind_match_id_perf() -> Result<()> {
    const N: i64 = 600;

    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Obs")
        .property("idx", DataType::Int64)
        .label("Topic")
        .property("name", DataType::String)
        .edge_type("ABOUT", &["Obs"], &["Topic"])
        .apply()
        .await?;

    // Create N observations + 1 topic.
    let tx = db.session().tx().await?;
    for i in 0..N {
        tx.execute_with("CREATE (:Obs {idx: $i})")
            .param("i", i)
            .run()
            .await?;
    }
    tx.execute("CREATE (:Topic {name: 'main'})").await?;
    tx.commit().await?;

    let obs_vids: Vec<i64> = db
        .session()
        .query("MATCH (n:Obs) RETURN id(n) AS vid ORDER BY n.idx")
        .await?
        .rows()
        .iter()
        .map(|r| r.get::<i64>("vid").unwrap())
        .collect();
    let topic_vid: i64 = db
        .session()
        .query("MATCH (t:Topic) RETURN id(t) AS vid")
        .await?
        .rows()[0]
        .get("vid")?;

    let edges: Vec<Value> = obs_vids
        .iter()
        .map(|&src| {
            let mut m = HashMap::new();
            m.insert("src".to_string(), Value::Int(src));
            m.insert("dst".to_string(), Value::Int(topic_vid));
            Value::Map(m)
        })
        .collect();

    let tx = db.session().tx().await?;
    let start = Instant::now();
    let res = tx
        .execute_with(
            "UNWIND $edges AS e \
             MATCH (a), (b) WHERE id(a) = e.src AND id(b) = e.dst \
             CREATE (a)-[r:ABOUT]->(b) RETURN id(r) AS eid",
        )
        .param("edges", Value::List(edges))
        .run()
        .await?;
    let elapsed = start.elapsed();
    tx.commit().await?;

    eprintln!("UNWIND of {N} edges took {:?}", elapsed);
    assert_eq!(res.affected_rows() as i64, N);

    // Pre-fix: ~138s. Post-fix: should be a few seconds at most.
    // 10s leaves generous headroom for CI variance while still catching the regression.
    assert!(
        elapsed.as_secs() < 10,
        "UNWIND+MATCH for {N} edges took {:?}, expected < 10s — perf regression",
        elapsed
    );

    Ok(())
}
