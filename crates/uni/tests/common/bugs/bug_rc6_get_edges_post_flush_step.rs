// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Verification guard for RC6 (uniko `UNI_DB_WORKAROUNDS.md`). VERDICT: the
//! `get_edges` *scaling* bug (#55) is fixed (`adjacency_manager.rs` short-circuits
//! frozen CSR segments with no entries for the queried `(edge_type, direction)`),
//! and the catalog's residual "constant post-flush latency step" from the
//! per-read tombstone `retain()` does **not** reproduce as a material cost against
//! uni HEAD.
//!
//! This is a **green guard**, not a red repro: the issue does not reproduce. The
//! test pins that. It compares two hubs with the *same* live out-degree after a
//! flush — one clean, one whose frozen segment also holds 4000 tombstones (a 10:1
//! tombstone:live ratio). Measured: the tombstoned hub is only ~1.0–1.3x the
//! clean one (the `retain()` pass scales linearly but with a tiny constant), so
//! there is no practical post-flush step. The generous 4x bar guards against a
//! regression to a genuinely expensive per-read tombstone pass (which at 10:1
//! would be >10x), not against the benign residual.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration bug_rc6_get_edges_post_flush_step

use std::time::Instant;

use anyhow::Result;
use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

/// Median 1-hop fan-out latency for the named hub over `iters` runs.
async fn traverse_us(db: &Uni, id: &str, iters: u32) -> Result<u128> {
    let session = db.session();
    // Warm up (plan cache, page-in).
    for _ in 0..3 {
        let _ = session
            .query_with("MATCH (h:Hub {id: $id})-[:REL]->(x) RETURN count(x) AS c")
            .param("id", Value::String(id.into()))
            .fetch_all()
            .await?;
    }
    let mut samples = Vec::with_capacity(iters as usize);
    for _ in 0..iters {
        let start = Instant::now();
        let _ = session
            .query_with("MATCH (h:Hub {id: $id})-[:REL]->(x) RETURN count(x) AS c")
            .param("id", Value::String(id.into()))
            .fetch_all()
            .await?;
        samples.push(start.elapsed().as_micros());
    }
    samples.sort_unstable();
    Ok(samples[samples.len() / 2])
}

/// VERIFIED: a tombstoned adjacency segment costs about the same to traverse as a
/// clean one with equal live degree — the per-read tombstone `retain()` adds no
/// material post-flush step. Guards against a regression to an expensive per-read
/// tombstone pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tombstoned_segment_adds_no_material_per_read_cost() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Hub")
        .property("id", DataType::String)
        .index("id", IndexType::Scalar(ScalarType::Hash))
        .done()
        .label("Leaf")
        .property("k", DataType::Int)
        .index("k", IndexType::Scalar(ScalarType::Hash))
        .done()
        .edge_type("REL", &["Hub"], &["Leaf"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    // Two hubs; 4800 leaves (0..399 for the clean hub, 400..4799 for the other).
    tx.execute("CREATE (:Hub {id: 'clean'})").await?;
    tx.execute("CREATE (:Hub {id: 'tomb'})").await?;
    tx.execute_with("UNWIND range(0, $hi) AS k CREATE (:Leaf {k: k})")
        .param("hi", Value::Int(4799))
        .run()
        .await?;
    // clean hub: 400 live edges, no deletes.
    tx.execute("MATCH (h:Hub {id: 'clean'}), (x:Leaf) WHERE x.k <= 399 CREATE (h)-[:REL]->(x)")
        .await?;
    // tomb hub: 4400 edges, then delete the upper 4000 → 400 live + 4000 tombstones.
    tx.execute(
        "MATCH (h:Hub {id: 'tomb'}), (x:Leaf) WHERE x.k >= 400 AND x.k <= 4799 \
         CREATE (h)-[:REL]->(x)",
    )
    .await?;
    tx.commit().await?;

    let tx = session.tx().await?;
    tx.execute("MATCH (h:Hub {id: 'tomb'})-[r:REL]->(x) WHERE x.k >= 800 DELETE r")
        .await?;
    tx.commit().await?;

    // Freeze the adjacency into a CSR segment carrying the tombstones.
    db.flush().await?;

    // Sanity: both hubs now have exactly 400 live out-edges.
    for id in ["clean", "tomb"] {
        let r = session
            .query_with("MATCH (h:Hub {id: $id})-[:REL]->(x) RETURN count(x) AS c")
            .param("id", Value::String(id.into()))
            .fetch_all()
            .await?;
        assert_eq!(r.rows()[0].get::<i64>("c")?, 400, "{id} hub live degree");
    }

    let clean_us = traverse_us(&db, "clean", 200).await?;
    let tomb_us = traverse_us(&db, "tomb", 200).await?;
    eprintln!(
        "RC6 post-flush traversal: clean={clean_us}us, tombstoned={tomb_us}us, \
         ratio={:.2}x",
        tomb_us as f64 / clean_us.max(1) as f64
    );

    assert!(
        tomb_us < clean_us * 4,
        "tombstoned segment ({tomb_us}us) costs >4x the clean segment ({clean_us}us) \
         per read — the per-read tombstone retain() pass has become expensive (RC6 regression)"
    );
    Ok(())
}
