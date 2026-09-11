// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Execution counters never reached MERGE's per-row plans, so `rows_scanned`
//! read the same for a sixteen-second MERGE as for a sub-second one.

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{Uni, Value};

const N: usize = 4_000;

async fn graph() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (uid STRING)").await?;
    tx.execute("CREATE EDGE TYPE OWNS FROM Entity TO Entity")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let vertices: Vec<HashMap<String, Value>> = (0..N)
        .map(|i| HashMap::from([("uid".to_string(), Value::String(format!("e{i}")))]))
        .collect();
    bulk.insert_vertices("Entity", vertices).await?;
    bulk.commit().await?;
    tx.commit().await?;
    Ok(db)
}

/// A MERGE whose per-row plans scan a label must report those scans.
///
/// The counters live on an `Arc<QueryCounters>` that `Executor::clone`
/// deliberately makes fresh — the write path clones a cached template, and a
/// shared handle would spill one query's counts into the next one's result. But
/// `MutationContext` also holds a clone, so every scan a mutation performed
/// counted into a set nobody harvests.
///
/// The shape below is the one #225 measured: an unbound, unlabelled-key first
/// element forces a per-row scan of the whole label, which cannot be anchored
/// away. With `N` rows over a label of `N`, the per-row scans dominate the outer
/// MATCH by orders of magnitude — so a counter that only sees the outer MATCH is
/// unmistakable.
#[tokio::test]
async fn merge_reports_the_rows_its_per_row_plans_scanned() -> Result<()> {
    let db = graph().await?;
    let batch = Value::List(
        (0..20)
            .map(|i| {
                Value::Map(HashMap::from([(
                    "dst".to_string(),
                    Value::String(format!("e{i}")),
                )]))
            })
            .collect(),
    );

    let tx = db.session().tx().await?;
    let res = tx
        .execute_with(
            "UNWIND $batch AS r \
             MATCH (b:Entity {uid: r.dst}) \
             MERGE (a:Entity)-[:OWNS]->(b)",
        )
        .param("batch", batch)
        .run()
        .await?;
    let scanned = res.metrics().rows_scanned;
    tx.rollback();

    eprintln!("rows_scanned = {scanned} over a label of {N} with 20 rows");
    assert!(
        scanned > N,
        "rows_scanned = {scanned}, which is below the {N}-row label this MERGE \
         rescans once per input row. The counters are not reaching the \
         mutation executor, so a MERGE's own scans are invisible and any \
         assertion on this counter passes for free."
    );
    Ok(())
}
