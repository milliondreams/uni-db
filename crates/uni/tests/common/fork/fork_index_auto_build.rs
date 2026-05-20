// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 5a-impl Step 7 — background fork index builder.
//!
//! Verifies that with the auto-builder enabled and a tight polling
//! interval, a fork that crosses `fork_index_build_threshold` will
//! eventually have a `ScalarBtree` fork-local index built and
//! registered without an explicit `Session::build_fork_local_index`
//! call.

// Rust guideline compliant

use std::time::Duration;

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::Uni;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_builder_schedules_scalar_fork_index() -> Result<()> {
    // Tight thresholds + interval so the test doesn't wait long.
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        disable_fork_index_builder: false,
        fork_index_builder_interval: Duration::from_millis(100),
        fork_index_build_threshold: 5, // tiny so a few inserts trigger
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Person")
        .property("email", DataType::String)
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    // Primary scalar index is required — auto-builder only schedules
    // builds for columns primary has indexed. Created via Cypher DDL,
    // matching the existing index test pattern.
    let tx = primary.tx().await?;
    tx.execute("CREATE INDEX person_email FOR (p:Person) ON (p.email)")
        .await?;
    tx.execute("CREATE (:Person {email: 'seed@x.com', name: 'seed'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = primary.fork("scenario").await?;
    let tx = forked.tx().await?;
    for i in 0..10 {
        tx.execute(&format!(
            "CREATE (:Person {{email: 'f{i}@x.com', name: 'fork-{i}'}})"
        ))
        .await?;
    }
    tx.commit().await?;
    forked.flush().await?;

    // Wait for the background builder to fire. Polling interval is
    // 100ms; allow a few cycles plus build time.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut saw_fusion = false;
    while std::time::Instant::now() < deadline {
        let plan = forked
            .query_with("MATCH (p:Person {email: 'seed@x.com'}) RETURN p.name AS name")
            .explain()
            .await?;
        if plan.plan_text.contains("FusedIndexScan") {
            saw_fusion = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert!(
        saw_fusion,
        "background builder did not register fork-local scalar index within 5s"
    );

    db.shutdown().await?;
    Ok(())
}
