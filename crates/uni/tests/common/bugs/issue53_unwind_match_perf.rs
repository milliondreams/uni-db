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
    let vids: Vec<i64> = (0..5)
        .map(|i| result.rows()[i].get::<i64>("vid").unwrap())
        .collect();

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

// ============================================================================
// Issue #54 follow-ups: string-key equi-joins, OPTIONAL MATCH, IN-pushdown.
// ============================================================================

/// Issue #54 part 1: equi-join on string properties must use HashJoin
/// (previously fell back to FilterExec+CrossJoinExec because
/// `wrap_in_to_integer` rejected string dtypes).
#[tokio::test]
async fn string_key_match_correctness() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("city", DataType::String)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    let people = [
        ("alice", "NY"),
        ("bob", "SF"),
        ("carol", "NY"),
        ("dave", "SF"),
        ("eve", "LA"),
    ];
    for (n, c) in &people {
        tx.execute_with("CREATE (:Person {name: $n, city: $c})")
            .param("n", *n)
            .param("c", *c)
            .run()
            .await?;
    }
    tx.commit().await?;

    // Self-join on string `city`. Each person joins with everyone in same city
    // (including themselves). Counts: NY 2x2=4, SF 2x2=4, LA 1x1=1 → 9 rows.
    let result = db
        .session()
        .query(
            "MATCH (a:Person) MATCH (b:Person) WHERE a.city = b.city \
             RETURN a.name AS an, b.name AS bn ORDER BY an, bn",
        )
        .await?;
    assert_eq!(result.rows().len(), 9, "expected 9 same-city pairs");

    // Spot-check: NY pairs are alice-alice, alice-carol, carol-alice, carol-carol.
    let ny_pairs: Vec<(String, String)> = result
        .rows()
        .iter()
        .map(|r| {
            (
                r.get::<String>("an").unwrap(),
                r.get::<String>("bn").unwrap(),
            )
        })
        .filter(|(a, _)| a == "alice" || a == "carol")
        .collect();
    assert_eq!(ny_pairs.len(), 4);

    Ok(())
}

/// Issue #54 part 1 perf: `UNWIND $names AS n MATCH (p:Person) WHERE p.name = n`
/// over a large person set must complete quickly. Without HashJoin on strings
/// the runtime is N×M with a per-row Filter eval; HashJoin makes it N+M.
#[tokio::test]
#[ignore = "perf regression guard, run explicitly"]
async fn unwind_match_string_property_perf() -> Result<()> {
    const N: i64 = 600;

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..N {
        tx.execute_with("CREATE (:Person {name: $n})")
            .param("n", format!("person_{i}"))
            .run()
            .await?;
    }
    tx.commit().await?;

    // Pick every 3rd name to look up.
    let names: Vec<Value> = (0..N)
        .step_by(3)
        .map(|i| Value::String(format!("person_{i}")))
        .collect();
    let expected = names.len();

    let start = Instant::now();
    let result = db
        .session()
        .query_with("UNWIND $names AS n MATCH (p:Person) WHERE p.name = n RETURN p.name AS m")
        .param("names", Value::List(names))
        .fetch_all()
        .await?;
    let elapsed = start.elapsed();

    eprintln!("UNWIND $names of {expected} took {:?}", elapsed);
    assert_eq!(result.rows().len(), expected);
    assert!(
        elapsed.as_secs() < 10,
        "string-key UNWIND join took {:?}, expected < 10s",
        elapsed
    );
    Ok(())
}

/// Issue #54 part 2: OPTIONAL MATCH equi-join must preserve NULL rows for
/// the optional side when no match exists. Previously this hit
/// FilterExec+CrossJoin via OptionalFilterExec; now uses LeftOuter HashJoin
/// for the pure-equi-join case, and must produce identical results.
#[tokio::test]
async fn optional_match_equi_join_correctness() -> Result<()> {
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
    // Left: k = 0..5. Right: only k = 0, 2, 4.
    for i in 0..5 {
        tx.execute_with("CREATE (:Left {k: $k})")
            .param("k", i as i64)
            .run()
            .await?;
    }
    for &k in &[0i64, 2, 4] {
        tx.execute_with("CREATE (:Right {k: $k, v: $v})")
            .param("k", k)
            .param("v", format!("v{k}"))
            .run()
            .await?;
    }
    tx.commit().await?;

    // OPTIONAL MATCH equi-join: every Left row appears; b is NULL for k = 1, 3.
    let result = db
        .session()
        .query(
            "MATCH (a:Left) OPTIONAL MATCH (b:Right) WHERE a.k = b.k \
             RETURN a.k AS k, b.v AS v ORDER BY k",
        )
        .await?;

    assert_eq!(result.rows().len(), 5, "expected one row per Left");

    let expected = [
        (0i64, Some("v0")),
        (1, None),
        (2, Some("v2")),
        (3, None),
        (4, Some("v4")),
    ];
    for (i, (exp_k, exp_v)) in expected.iter().enumerate() {
        let row = &result.rows()[i];
        assert_eq!(row.get::<i64>("k")?, *exp_k);
        let v: Option<String> = row.get("v").ok();
        assert_eq!(v.as_deref(), *exp_v, "row {i} mismatch");
    }
    Ok(())
}

/// Issue #54 part 3: when UNWIND source is a literal/parameter list and the
/// equi-join is on a scan-side property, the scan must be filtered by an
/// IN-list before the HashJoin. We can't easily inspect the physical plan
/// from here, so we validate via correctness on a query that would otherwise
/// require a full scan + per-row eval.
#[tokio::test]
async fn unwind_in_pushdown_correctness() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..20 {
        tx.execute_with("CREATE (:Person {name: $n})")
            .param("n", format!("p{i}"))
            .run()
            .await?;
    }
    tx.commit().await?;

    let lookups: Vec<Value> = ["p3", "p7", "p11", "p99"]
        .iter()
        .map(|s| Value::String((*s).to_string()))
        .collect();

    let result = db
        .session()
        .query_with(
            "UNWIND $names AS n MATCH (p:Person) WHERE p.name = n RETURN p.name AS m ORDER BY m",
        )
        .param("names", Value::List(lookups))
        .fetch_all()
        .await?;

    let mut names: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("m").unwrap())
        .collect();
    names.sort();
    // p99 doesn't exist — must be absent. Other 3 must be present.
    assert_eq!(
        names,
        vec!["p11".to_string(), "p3".to_string(), "p7".to_string()]
    );
    Ok(())
}
