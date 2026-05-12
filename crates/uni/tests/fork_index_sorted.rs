// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 5a-impl Step 6 — sorted fork-local fusion.
//!
//! Tests that:
//! 1. After registering a `Sorted` fork-local index for `(label, column)`,
//!    a `MATCH (n:label) RETURN n ORDER BY n.column` query has its
//!    underlying Scan rewritten to `FusedIndexScan { kind: SortedKWayMerge }`.
//! 2. Without the registry entry, the planner emits a regular Sort
//!    over Scan.
//! 3. Result correctness on the fork is preserved through Lance's
//!    `base_paths` chain — primary + fork rows return in the
//!    requested order.

// Rust guideline compliant

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::Uni;
use uni_store::fork::ForkLocalIndexKind;

#[tokio::test]
async fn fork_local_sorted_index_is_observed_by_planner() -> Result<()> {
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Item")
        .property("score", DataType::Int64)
        .property("name", DataType::String)
        .apply()
        .await?;

    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Item {score: 10, name: 'a'})").await?;
    tx.execute("CREATE (:Item {score: 30, name: 'b'})").await?;
    tx.execute("CREATE (:Item {score: 50, name: 'c'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = primary.fork("scenario").await?;
    let tx = forked.tx().await?;
    tx.execute("CREATE (:Item {score: 20, name: 'fork-d'})")
        .await?;
    tx.execute("CREATE (:Item {score: 40, name: 'fork-e'})")
        .await?;
    tx.commit().await?;

    // Pre-registration: regular Sort over Scan.
    let pre = forked
        .query_with("MATCH (i:Item) RETURN i.name AS name ORDER BY i.score")
        .explain()
        .await?;
    assert!(
        pre.plan_text.contains("Sort"),
        "expected Sort in plan; got {}",
        pre.plan_text
    );
    assert!(
        !pre.plan_text.contains("FusedIndexScan"),
        "FusedIndexScan emitted before sorted index was registered: {}",
        pre.plan_text
    );

    forked
        .build_fork_local_index("Item", "score", ForkLocalIndexKind::Sorted)
        .await?;

    // Post-registration: Sort wraps FusedIndexScan { SortedKWayMerge }.
    let post = forked
        .query_with("MATCH (i:Item) RETURN i.name AS name ORDER BY i.score")
        .explain()
        .await?;
    assert!(
        post.plan_text.contains("FusedIndexScan"),
        "expected FusedIndexScan after Sorted registration; got {}",
        post.plan_text
    );
    assert!(
        post.plan_text.contains("SortedKWayMerge"),
        "expected SortedKWayMerge fusion kind; got {}",
        post.plan_text
    );

    // Result correctness: ORDER BY produces the global sort over
    // primary + fork rows.
    let rows = forked
        .query("MATCH (i:Item) RETURN i.name AS name ORDER BY i.score")
        .await?;
    let names: Vec<String> = rows
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert_eq!(
        names,
        vec!["a", "fork-d", "b", "fork-e", "c"],
        "ORDER BY i.score should interleave primary + fork rows by score"
    );

    db.shutdown().await?;
    Ok(())
}
