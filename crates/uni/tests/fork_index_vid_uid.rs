// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 5a-impl Step 4 — VidUid fork-first fusion.
//!
//! Demonstrates that:
//! 1. After `Session::build_fork_local_index(label, column, VidUid)`
//!    the planner observes the registry entry and rewrites
//!    `LogicalPlan::Scan` → `LogicalPlan::FusedIndexScan` for
//!    matching equality predicates.
//! 2. Without registering the index, the planner emits the regular
//!    `Scan` (no fusion).
//! 3. The rewrite is correctness-preserving in Step 4 — the physical
//!    planner decays `FusedIndexScan` back to a regular Scan for
//!    Phase 5a-impl Steps 4–6, so query results are identical to
//!    today. The "operator was picked" assertion runs against the
//!    explain plan, not runtime stats.
//!
//! Steps 5 and 6 will replace the physical decay with type-specific
//! fused operators (`FusedBtreeScanExec`, `FusedSortedScanExec`); for
//! VidUid the decay is the *intended* implementation because Lance's
//! `base_paths` chain already provides fork-first semantics for free.

// Rust guideline compliant

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::Uni;
use uni_store::fork::ForkLocalIndexKind;

#[tokio::test]
async fn fork_local_vid_uid_index_is_observed_by_planner() -> Result<()> {
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Person")
        .property("uid", DataType::String)
        .property("name", DataType::String)
        .apply()
        .await?;

    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {uid: 'X', name: 'Primary-Alice'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = primary.fork("scenario").await?;
    let tx = forked.tx().await?;
    tx.execute("CREATE (:Person {uid: 'X', name: 'Fork-Alice'})")
        .await?;
    tx.commit().await?;

    // Before registering the fork-local index: planner emits Scan.
    let pre = forked
        .query_with("MATCH (p:Person {uid: 'X'}) RETURN p.name AS name")
        .explain()
        .await?;
    assert!(
        pre.plan_text.contains("Scan"),
        "expected Scan in plan; got {}",
        pre.plan_text
    );
    assert!(
        !pre.plan_text.contains("FusedIndexScan"),
        "FusedIndexScan emitted before fork-local index was registered: {}",
        pre.plan_text
    );

    // Register the fork-local VidUid index marker. For VidUid this is
    // a no-op build (no Lance index file written) — Lance's
    // `base_paths` chain on the fork's branch already gives us
    // fork-first lookup semantics. The registry entry alone is the
    // signal the planner needs.
    forked
        .build_fork_local_index("Person", "uid", ForkLocalIndexKind::VidUid)
        .await?;

    // After registering: planner emits FusedIndexScan.
    let post = forked
        .query_with("MATCH (p:Person {uid: 'X'}) RETURN p.name AS name")
        .explain()
        .await?;
    assert!(
        post.plan_text.contains("FusedIndexScan"),
        "expected FusedIndexScan in plan after registering fork-local index; got {}",
        post.plan_text
    );

    // Primary's planner remains unchanged.
    let primary_plan = primary
        .query_with("MATCH (p:Person {uid: 'X'}) RETURN p.name AS name")
        .explain()
        .await?;
    assert!(
        !primary_plan.plan_text.contains("FusedIndexScan"),
        "primary should not see fork-local index registry; got {}",
        primary_plan.plan_text
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn build_fork_local_index_on_primary_session_errors() -> Result<()> {
    let db = Uni::in_memory()
        .config(UniConfig {
            disable_fork_sweeper: true,
            ..UniConfig::default()
        })
        .build()
        .await?;
    db.schema()
        .label("Person")
        .property("uid", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let result = primary
        .build_fork_local_index("Person", "uid", ForkLocalIndexKind::VidUid)
        .await;
    assert!(
        matches!(
            result,
            Err(uni_common::api::error::UniError::InvalidArgument { .. })
        ),
        "expected InvalidArgument on primary session; got {result:?}"
    );
    db.shutdown().await?;
    Ok(())
}
