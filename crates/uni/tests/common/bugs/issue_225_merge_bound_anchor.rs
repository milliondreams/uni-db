// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! MERGE must anchor on what the input row binds, not on what was written
//! first (#225).
//!
//! `execute_merge_match` carries its own copy of `plan_path`'s left-to-right
//! element walk, so neither #219's anchoring fix nor `dc232cd2a`'s ranking of
//! it reached MERGE. Boundness was consulted only for the leftmost node, which
//! made `MERGE (a:L)-[:R]->(b)` with `b` bound scan the whole of `L` — once per
//! input row, since the walk runs per row.
//!
//! # Why this compares two spellings rather than a counter or a growth curve
//!
//! **Not `rows_scanned`.** It cannot see this. MERGE's per-row plans never
//! reach the counter, so every spelling reports only what the *outer* MATCH
//! examined: the probe measured 395 for a sixteen-second arm and 395 for a
//! sub-second one. A test asserting on it would pass in both directions.
//!
//! **Not a growth curve either**, though that is what settled the diagnosis. A
//! per-row scan of a label costs in proportion to that label, so holding the
//! batch fixed and growing the label separates a scan from an anchored lookup —
//! `examples/merge_anchor_probe` does exactly that, and shows 5.4s → 16.2s
//! before and 0.70s → 0.76s after. It does not survive being shrunk into a
//! test: over an `Uni::in_memory()` fixture the defect measures **0.83x** for a
//! 4x larger label, the larger graph coming out *faster*. The probe's fixture
//! is on disk and flushed, and the L0-resident one does not scale the same way.
//! A growth assertion here would have been a test that cannot fail.
//!
//! **So: the same link, written two ways, over one fixture in one process.**
//! Machine speed divides out of the ratio, and the two spellings are asserted
//! to create the same relationships before any time is compared. Verified to
//! discriminate: 4.12x with the anchor reverted, 1.10x with it in place.
//!
//! **Parallelism:** the numbers are wall-clock, but both sides run in the same
//! process on the same data, so load slows them together. A failure close to
//! the guard should be re-run with `cargo nextest run -j1` before being read as
//! noise.

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{Uni, Value};

/// A 4x span, at sizes the effect actually clears.
///
/// An earlier version used 6 000 nodes and a 100-row batch, and **passed with
/// the fix reverted**: the per-row scan at that size is smaller than MERGE's
/// fixed per-row overhead and disappears into it. At 20 000 the ratio is 4.12x
/// reverted against 1.10x fixed. A guard is only worth having at a size where
/// the thing it guards is visible.
const LARGE: usize = 20_000;
const BATCH: usize = 200;

async fn graph(nodes: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (uid STRING)").await?;
    tx.execute("CREATE EDGE TYPE OWNS FROM Entity TO Entity")
        .await?;
    tx.execute("CREATE INDEX idx_uid FOR (e:Entity) ON (e.uid)")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let vertices: Vec<HashMap<String, Value>> = (0..nodes)
        .map(|i| HashMap::from([("uid".to_string(), Value::String(format!("e{i}")))]))
        .collect();
    bulk.insert_vertices("Entity", vertices).await?;
    bulk.commit().await?;
    tx.commit().await?;
    Ok(db)
}

/// `BATCH` deterministic src/dst pairs drawn from `nodes`.
fn batch(nodes: usize) -> Value {
    let mut state = 11u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((state >> 11) % nodes as u64) as usize
    };
    Value::List(
        (0..BATCH)
            .map(|_| {
                Value::Map(HashMap::from([
                    ("src".to_string(), Value::String(format!("e{}", next()))),
                    ("dst".to_string(), Value::String(format!("e{}", next()))),
                ]))
            })
            .collect(),
    )
}

/// Returns (seconds, relationships created), against an existing graph.
///
/// The transaction is rolled back rather than committed so a later run cannot
/// see edges an earlier one created — which would turn a MERGE insert into a
/// MERGE match and measure something else.
async fn run_on(db: &Uni, nodes: usize, query: &str) -> Result<(f64, usize)> {
    let tx = db.session().tx().await?;
    let started = std::time::Instant::now();
    let res = tx
        .execute_with(query)
        .param("batch", batch(nodes))
        .run()
        .await?;
    let secs = started.elapsed().as_secs_f64();
    let created = res.relationships_created();
    tx.rollback();
    Ok((secs, created))
}

/// `b` is bound by the preceding MATCH but written at the *tail* of the MERGE
/// pattern, so the walk's first element is the unbound `a`.
const BOUND_LAST: &str = "UNWIND $batch AS r \
                          MATCH (b:Entity {uid: r.dst}) \
                          MERGE (a:Entity {uid: r.src})-[e:OWNS]->(b)";

/// The same link, written so the bound node comes first.
const BOUND_FIRST: &str = "UNWIND $batch AS r \
                           MATCH (a:Entity {uid: r.src}) \
                           MERGE (a)-[e:OWNS]->(b:Entity {uid: r.dst})";

/// The two spellings describe the same link and should cost about the same.
///
/// Separate from the growth test because the two catch different regressions: a
/// change that made *both* spellings scan would keep the ratio here at 1 while
/// the growth test fails, and one that slowed only the reversal path would do
/// the reverse.
#[tokio::test]
async fn issue_225_both_spellings_of_the_same_link_cost_alike() -> Result<()> {
    let db = graph(LARGE).await?;
    let (last, last_created) = run_on(&db, LARGE, BOUND_LAST).await?;
    let (first, first_created) = run_on(&db, LARGE, BOUND_FIRST).await?;

    assert_eq!(
        last_created, first_created,
        "the two spellings created {last_created} and {first_created} \
         relationships; they are not describing the same link"
    );
    assert_eq!(last_created, BATCH, "fixture linked {last_created} pairs");

    let ratio = last / first.max(1e-9);
    eprintln!("bound-last {last:.3}s vs bound-first {first:.3}s = {ratio:.2}x");
    assert!(
        ratio < 1.8,
        "writing the bound node last cost {ratio:.2}x writing it first \
         ({last:.3}s vs {first:.3}s) for the same link — MERGE is not anchoring \
         on the binding (#225)"
    );
    Ok(())
}
