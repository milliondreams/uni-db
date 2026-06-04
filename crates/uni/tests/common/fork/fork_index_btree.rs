// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 5a-impl Step 5 — scalar BTree fork-local fusion.
//!
//! Once `Session::build_fork_local_index(label, column, ScalarBtree)`
//! has run, the planner observes the registry entry and rewrites
//! `Scan` → `FusedIndexScan { kind: BtreeUnion }` for matching
//! equality predicates. Step 5 reuses the planner rewrite shipped in
//! Step 4 — only the registered index `kind` differs.
//!
//! Result correctness on a forked session is preserved end-to-end:
//! Lance's `base_paths` chain reads through both branches, and the
//! existing `Scan` returns the union of primary + fork rows.
//! Step 5 adds the planner-side observability that fusion *would*
//! engage; the physical decay to a regular Scan keeps results
//! identical to the pre-fusion baseline.

// Rust guideline compliant

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::Uni;
use uni_store::fork::ForkLocalIndexKind;

#[tokio::test]
async fn fork_local_btree_index_is_observed_by_planner() -> Result<()> {
    let cfg = UniConfig {
        disable_fork_sweeper: true,
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
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {email: 'a@x.com', name: 'Alice'})")
        .await?;
    tx.execute("CREATE (:Person {email: 'b@x.com', name: 'Bob'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = primary.fork("scenario").await?;
    let tx = forked.tx().await?;
    tx.execute("CREATE (:Person {email: 'c@x.com', name: 'Carol-on-fork'})")
        .await?;
    tx.commit().await?;

    // Pre-registration: regular Scan.
    let pre = forked
        .query_with("MATCH (p:Person {email: 'a@x.com'}) RETURN p.name AS name")
        .explain()
        .await?;
    assert!(!pre.plan_text.contains("FusedIndexScan"));

    forked
        .build_fork_local_index("Person", "email", ForkLocalIndexKind::ScalarBtree)
        .await?;

    // Post-registration: FusedIndexScan with BtreeUnion kind.
    let post = forked
        .query_with("MATCH (p:Person {email: 'a@x.com'}) RETURN p.name AS name")
        .explain()
        .await?;
    assert!(
        post.plan_text.contains("FusedIndexScan"),
        "expected FusedIndexScan after BTree registration; got {}",
        post.plan_text
    );
    assert!(
        post.plan_text.contains("BtreeUnion"),
        "expected BtreeUnion fusion kind; got {}",
        post.plan_text
    );

    // Result correctness on the fork: queries on primary-only, fork-only, and
    // a non-existent email all behave correctly through `base_paths` reads.
    let primary_only = forked
        .query("MATCH (p:Person {email: 'a@x.com'}) RETURN p.name AS name")
        .await?;
    assert_eq!(primary_only.rows().len(), 1);

    let fork_only = forked
        .query("MATCH (p:Person {email: 'c@x.com'}) RETURN p.name AS name")
        .await?;
    assert_eq!(fork_only.rows().len(), 1);

    let missing = forked
        .query("MATCH (p:Person {email: 'nope@x.com'}) RETURN p.name AS name")
        .await?;
    assert_eq!(missing.rows().len(), 0);

    db.shutdown().await?;
    Ok(())
}
