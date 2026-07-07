#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for crates/uni/src/api/fork_maintenance.rs — fork-local index-kind
//! collision (correctness-scan Wave 4, the sole surviving unverified finding).
//!
//! Before the fix `ForkScope::fork_local_indexes` was a
//! `DashMap<(label, column), ForkLocalIndexKind>` — a SINGLE kind per (label,
//! column). Two distinct fork-buildable index kinds on the same column could
//! not coexist: registering the second overwrote the first, so only the
//! last-built kind was visible to the planner (`has_fork_index` saw just the
//! surviving kind), and the auto-builder ping-ponged between the two forever.
//!
//! We drive this through the public `Session::build_fork_local_index` API:
//! build a `ScalarBtree` on (Person, email) — the equality query fuses to a
//! `FusedIndexScan`/`BtreeUnion`. Then build a `FullText` index on the SAME
//! column. After the fix both kinds coexist, so the scalar-equality query STILL
//! fuses to `BtreeUnion` alongside the full-text index. Before the fix the
//! `FullText` registration clobbered the scalar kind and the equality query
//! fell back to a plain `Scan`.

use uni_db::{DataType, Uni};
use uni_store::fork::ForkLocalIndexKind;

#[tokio::test]
async fn two_fork_index_kinds_on_one_column_coexist() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
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
    tx.execute("CREATE (:Person {email: 'c@x.com', name: 'Carol'})")
        .await?;
    tx.commit().await?;

    // Build a ScalarBtree fork-local index on (Person, email).
    forked
        .build_fork_local_index("Person", "email", ForkLocalIndexKind::ScalarBtree)
        .await?;

    // The scalar-equality query now fuses to a FusedIndexScan (BtreeUnion).
    let with_btree = forked
        .query_with("MATCH (p:Person {email: 'a@x.com'}) RETURN p.name AS name")
        .explain()
        .await?;
    assert!(
        with_btree.plan_text.contains("FusedIndexScan")
            && with_btree.plan_text.contains("BtreeUnion"),
        "sanity: ScalarBtree fork-local index must fuse the equality query; got {}",
        with_btree.plan_text
    );

    // Build a FullText fork-local index on the SAME (Person, email) column.
    // Before the fix this overwrote the ScalarBtree entry in the single-value
    // DashMap; now the two kinds coexist in a set.
    forked
        .build_fork_local_index("Person", "email", ForkLocalIndexKind::FullText)
        .await?;

    // Re-run the scalar-equality query. The ScalarBtree fusion must SURVIVE
    // alongside the FullText index (still FusedIndexScan/BtreeUnion). Before
    // the fix this regressed to a plain Scan because the FullText build
    // clobbered the scalar kind in the (label, column)-keyed DashMap.
    let after_fulltext = forked
        .query_with("MATCH (p:Person {email: 'a@x.com'}) RETURN p.name AS name")
        .explain()
        .await?;
    assert!(
        after_fulltext.plan_text.contains("FusedIndexScan")
            && after_fulltext.plan_text.contains("BtreeUnion"),
        "BUG: ScalarBtree fusion was lost after building FullText on the same \
         column — the two kinds must coexist; plan = {}",
        after_fulltext.plan_text
    );

    // Retrieval is correct in both cases.
    let row = forked
        .query("MATCH (p:Person {email: 'a@x.com'}) RETURN p.name AS name")
        .await?;
    assert_eq!(row.rows().len(), 1);

    db.shutdown().await?;
    Ok(())
}
