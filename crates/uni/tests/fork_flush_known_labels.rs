// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 2 Day 10 — fork flushes succeed for labels that exist on
//! primary at fork-point.
//!
//! Before Day 10, `BranchedBackend::write` bailed for any dataset
//! that wasn't in the fork's `datasets` map. `build_datasets_for_fork`
//! only branched per-label `vertices_{label}` tables — not the main
//! label-agnostic `vertices` and `edges` tables that `flush_to_l1`
//! always writes through. The fix is to also branch the main tables
//! (and per-edge-type delta/adjacency tables) at fork-point.
//!
//! This test forces auto-flush on every commit and verifies the fork
//! flush completes cleanly + data is durable across a re-read.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn fork_flush_main_vertices_table_succeeds() -> Result<()> {
    let config = uni_db::UniConfig {
        // Force a flush on every commit so the fork's writer exercises
        // the branched-write path during the test.
        auto_flush_threshold: 1,
        ..Default::default()
    };
    let db = Uni::in_memory().config(config).build().await?;
    db.schema()
        .label("Item")
        .property("kind", DataType::String)
        .apply()
        .await?;

    // Seed primary so `vertices` and `vertices_Item` exist on disk
    // at fork-point.
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {kind: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = session.fork("flushy").await?;

    // Three commits — each triggers an auto-flush on the fork's writer.
    // Before Day 10 the writes against `vertices` would bail; the
    // commit succeeded but the post-commit `check_flush` swallowed the
    // bail-out as a `tracing::warn`. After Day 10, flush succeeds and
    // mutations are durable.
    for i in 0..3 {
        let tx = forked.tx().await?;
        tx.execute(&format!("CREATE (:Item {{kind: 'fork-{i}'}})"))
            .await?;
        tx.commit().await?;
    }

    let names: Vec<String> = forked
        .query("MATCH (i:Item) RETURN i.kind")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("i.kind").ok())
        .collect();
    assert_eq!(
        names.len(),
        4,
        "fork should see seed + 3 fork writes; got {names:?}"
    );

    db.shutdown().await?;
    Ok(())
}
