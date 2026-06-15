// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Verification guard for the slow-pattern-in-WHERE residual (uniko
//! `bugs/unidb-slow-pattern-in-where/`). VERDICT: does **not** reproduce against
//! uni HEAD. The uniko repro reported an edge-pattern-scoped `similar_to()` query
//! 10–24x slower than unscoped at ~300 messages; the catastrophic case was fixed
//! under issue #41 (vectorized CSR evaluation in `pattern_exists.rs`). Re-measured
//! here at the same ≈300-message scale with FullText + scalar indexes, the scoped
//! query is only ~1.6x the unscoped one — comfortably fine.
//!
//! This is a **green guard**, not a red repro: the issue does not reproduce. It
//! complements `issue_41_pattern_exists_perf.rs::pattern_in_where_should_not_be_slow`
//! (#41's own guard, 100 messages, 10x bar) by pinning the behaviour at 3x the
//! scale and a tighter 6x bar. The deeper architectural note — that there is no
//! index/vector pushdown *fusion* (`pushdown.rs`), so the pattern filter and the
//! vector scan still run as independent steps — remains true but does not cause a
//! perf problem at this scale.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration bug_rc_pattern_where_index_pushdown

use std::time::Instant;

use anyhow::Result;
use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

/// ~300 Message nodes with FullText + scalar indexes, MENTIONS/SENT_BY edges to
/// two Participants/Entities (mirrors the issue #41 / uniko shape, scaled up).
async fn setup_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Message")
        .property("content", DataType::String)
        .index("content", IndexType::FullText)
        .done()
        .label("Entity")
        .property("name", DataType::String)
        .index("name", IndexType::Scalar(ScalarType::Hash))
        .done()
        .label("Participant")
        .property("name", DataType::String)
        .index("name", IndexType::Scalar(ScalarType::Hash))
        .done()
        .edge_type("MENTIONS", &["Message"], &["Entity"])
        .done()
        .edge_type("SENT_BY", &["Message"], &["Participant"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Entity {name: 'Jon'})").await?;
    tx.execute("CREATE (:Entity {name: 'Gina'})").await?;
    tx.execute("CREATE (:Participant {name: 'Jon'})").await?;
    tx.execute("CREATE (:Participant {name: 'Gina'})").await?;
    tx.commit().await?;

    for i in 0..300 {
        let speaker = if i % 2 == 0 { "Jon" } else { "Gina" };
        let other = if i % 2 == 0 { "Gina" } else { "Jon" };
        let content = format!("Message {i} from {speaker} about business and dance");
        let tx = session.tx().await?;
        tx.execute(&format!(
            "CREATE (m:Message {{content: '{content}'}}) \
             WITH m MATCH (p:Participant {{name: '{speaker}'}}) CREATE (m)-[:SENT_BY]->(p) \
             WITH m MATCH (e:Entity {{name: '{other}'}}) CREATE (m)-[:MENTIONS]->(e)"
        ))
        .await?;
        tx.commit().await?;
    }
    db.flush().await?;
    Ok(db)
}

/// Median latency of a query over `iters` runs.
async fn query_us(db: &Uni, cypher: &str, params: &[(&str, Value)], iters: u32) -> Result<u128> {
    let session = db.session();
    let run = || async {
        let mut q = session.query_with(cypher);
        for (k, v) in params {
            q = q.param(*k, v.clone());
        }
        q.fetch_all().await
    };
    let _ = run().await?; // warm-up
    let mut samples = Vec::with_capacity(iters as usize);
    for _ in 0..iters {
        let start = Instant::now();
        let _ = run().await?;
        samples.push(start.elapsed().as_micros());
    }
    samples.sort_unstable();
    Ok(samples[samples.len() / 2])
}

/// VERIFIED: scoping a `similar_to` query by an edge pattern stays well within a
/// small factor of the unscoped query (~1.6x measured), so the catalog's 10–24x
/// slowdown does not reproduce. Guards against a regression of the #41 fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_pattern_with_similar_to_stays_fast() -> Result<()> {
    let db = setup_db().await?;

    let unscoped_us = query_us(
        &db,
        "MATCH (m:Message) \
         RETURN m.content AS content, similar_to(m.content, $q) AS score \
         ORDER BY score DESC LIMIT 15",
        &[("q", Value::String("business dance".into()))],
        5,
    )
    .await?;

    let scoped_us = query_us(
        &db,
        "MATCH (m:Message) \
         WHERE (m)-[:SENT_BY]->(:Participant {name: $ename}) \
            OR (m)-[:MENTIONS]->(:Entity {name: $ename}) \
         RETURN m.content AS content, similar_to(m.content, $q) AS score \
         ORDER BY score DESC LIMIT 15",
        &[
            ("ename", Value::String("Jon".into())),
            ("q", Value::String("business dance".into())),
        ],
        5,
    )
    .await?;

    eprintln!(
        "slow-WHERE residual: unscoped={unscoped_us}us, scoped={scoped_us}us, \
         ratio={:.1}x",
        scoped_us as f64 / unscoped_us.max(1) as f64
    );

    assert!(
        scoped_us < unscoped_us * 6,
        "scoped pattern+similar_to ({scoped_us}us) > 6x unscoped ({unscoped_us}us) — \
         the #41 vectorized-CSR fix appears to have regressed"
    );
    Ok(())
}
