// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 5b — fork-local vector ANN fusion.
//!
//! Confirms that:
//! 1. `Session::build_fork_local_index(label, column, Vector)` builds
//!    a Lance vector index on the fork's branch (no leak to primary).
//! 2. `CALL uni.vector.query(...)` on a forked session returns results
//!    that include both primary-inherited and fork-local vectors.
//! 3. The ranking is correct — vectors closer to the query are
//!    returned first regardless of which side they live on.
//!
//! Phase 5b's MVP relies on Lance's native per-branch index +
//! `base_paths` chain to deliver fused results — no bespoke
//! `FusedVectorSearchExec` operator (deferred to a 5b-followup if
//! recall benchmarks warrant it). The tests here assert *result
//! correctness*, not picked operator name.

// Rust guideline compliant

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::Uni;
use uni_store::fork::ForkLocalIndexKind;

#[tokio::test]
async fn fork_local_vector_index_returns_fused_results() -> Result<()> {
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Doc")
        .property("name", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 3 })
        .apply()
        .await?;

    // Primary holds 3 docs near the X axis.
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Doc {name: 'P-x', embedding: [1.0, 0.0, 0.0]})")
        .await?;
    tx.execute("CREATE (:Doc {name: 'P-x2', embedding: [0.95, 0.0, 0.0]})")
        .await?;
    tx.execute("CREATE (:Doc {name: 'P-z', embedding: [0.0, 0.0, 1.0]})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Fork inserts 2 docs near the Y axis (the query target below).
    let forked = primary.fork("scenario").await?;
    let tx = forked.tx().await?;
    tx.execute("CREATE (:Doc {name: 'F-y', embedding: [0.0, 1.0, 0.0]})")
        .await?;
    tx.execute("CREATE (:Doc {name: 'F-y2', embedding: [0.0, 0.95, 0.0]})")
        .await?;
    tx.commit().await?;
    forked.flush().await?;

    // Build fork-local vector index. The build path mirrors 5a-impl:
    // Lance writes the index file under the fork's branch directory.
    forked
        .build_fork_local_index("Doc", "embedding", ForkLocalIndexKind::Vector)
        .await?;

    // Phase 5b followup: planner emission for vector. Explain
    // should now show FusedIndexScanWrapped { kind: AnnRerank }
    // wrapping the original VectorKnn node.
    let plan = forked
        .query_with(
            "CALL uni.vector.query('Doc', 'embedding', [0.0, 1.0, 0.0], 5)
             YIELD node, score
             RETURN node.name AS name",
        )
        .explain()
        .await?;
    assert!(
        plan.plan_text.contains("FusedIndexScanWrapped"),
        "expected FusedIndexScanWrapped after Vector registration; got {}",
        plan.plan_text
    );
    assert!(
        plan.plan_text.contains("AnnRerank"),
        "expected AnnRerank fusion kind; got {}",
        plan.plan_text
    );

    // Query near the Y axis on the fork — fork-local vectors should
    // dominate the top of the ranking, primary-inherited Z/X should
    // appear lower with worse scores. The exact recall depends on
    // Lance's IVF parameters but the ordering is exact since Phase 5b
    // uses brute-force fallback for tiny datasets.
    let res = forked
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [0.0, 1.0, 0.0], 5)
             YIELD node, score
             RETURN node.name AS name, score
             ORDER BY score DESC",
        )
        .await?;

    assert!(
        res.rows().len() >= 2,
        "fork should see both primary-inherited and fork-local vectors; got {} rows",
        res.rows().len()
    );

    // Top result should be a fork-local Y-axis vector.
    let top_name: String = res.rows()[0].get("name")?;
    assert!(
        top_name == "F-y" || top_name == "F-y2",
        "expected fork-local Y-axis vector at top of ranking; got {top_name}"
    );

    // The result set should contain the names from both sides.
    let names: Vec<String> = res
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    let saw_fork = names.iter().any(|n| n.starts_with("F-"));
    let saw_primary = names.iter().any(|n| n.starts_with("P-"));
    assert!(
        saw_fork,
        "result set should include fork-local docs; got {names:?}"
    );
    assert!(
        saw_primary,
        "result set should include primary-inherited docs (top-K=5 vs 5 total); got {names:?}"
    );

    // Primary's vector search is unaffected — fork's index doesn't
    // leak.
    let primary_res = primary
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [0.0, 1.0, 0.0], 5)
             YIELD node, score
             RETURN node.name AS name, score
             ORDER BY score DESC",
        )
        .await?;
    let primary_names: Vec<String> = primary_res
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert!(
        !primary_names.iter().any(|n| n.starts_with("F-")),
        "primary vector search should not see fork-local docs; got {primary_names:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// Review M6: a fork vector query over a branch-local index must honor the
/// `_deleted = false` filter. Before the fix the branch search dropped the
/// filter, so a vertex soft-deleted on the parent before forking (inherited
/// via `base_paths`) leaked back into fork ANN results.
#[tokio::test]
async fn fork_local_vector_honors_deleted_filter() -> Result<()> {
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Doc")
        .property("name", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 3 })
        .apply()
        .await?;

    // Primary holds two near-Y vectors; one is the closest to the query.
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Doc {name: 'P-y-keep', embedding: [0.0, 0.9, 0.0]})")
        .await?;
    tx.execute("CREATE (:Doc {name: 'P-y-gone', embedding: [0.0, 1.0, 0.0]})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Soft-delete the closest match on PRIMARY before forking, then flush.
    let tx = primary.tx().await?;
    tx.execute("MATCH (d:Doc {name: 'P-y-gone'}) DETACH DELETE d")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = primary.fork("m6_vec").await?;
    let tx = forked.tx().await?;
    tx.execute("CREATE (:Doc {name: 'F-y', embedding: [0.0, 0.8, 0.0]})")
        .await?;
    tx.commit().await?;
    forked.flush().await?;

    forked
        .build_fork_local_index("Doc", "embedding", ForkLocalIndexKind::Vector)
        .await?;

    let res = forked
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [0.0, 1.0, 0.0], 10)
             YIELD node, score
             RETURN node.name AS name",
        )
        .await?;
    let names: Vec<String> = res
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();

    assert!(
        !names.iter().any(|n| n == "P-y-gone"),
        "soft-deleted (inherited) vector leaked into fork ANN results (M6); got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "P-y-keep"),
        "live inherited vector should still be returned; got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "F-y"),
        "fork-local vector should be returned; got {names:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// Nested (2-level) fork single-vector `vector.query` fuses results across the whole
/// ancestry (grandchild → child → parent → main). This is the single-vector counterpart to
/// `fork_index_multivector::nested_fork_multivector_resolves_through_ancestors`: it
/// previously failed identically because a filtered branch scan engaged scalar-index
/// pushdown whose `_vid` BTree `page_lookup.lance` is unresolvable across a >1-level fork
/// chain. Fixed by disabling scalar-index pushdown on branch scans (#106).
#[tokio::test]
async fn nested_fork_vector_resolves_through_ancestors() -> Result<()> {
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Doc")
        .property("name", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 3 })
        .apply()
        .await?;

    // Root (main): a doc on the X axis.
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Doc {name: 'P-x', embedding: [1.0, 0.0, 0.0]})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Level-1 fork: a doc on the Y axis (the query target).
    let a = primary.fork("vec_parent_fork").await?;
    let tx = a.tx().await?;
    tx.execute("CREATE (:Doc {name: 'A-y', embedding: [0.0, 1.0, 0.0]})")
        .await?;
    tx.commit().await?;
    a.flush().await?;

    // Level-2 fork (fork of a fork): a doc on the Z axis.
    let b = a.fork("vec_child_fork").await?;
    let tx = b.tx().await?;
    tx.execute("CREATE (:Doc {name: 'B-z', embedding: [0.0, 0.0, 1.0]})")
        .await?;
    tx.commit().await?;
    b.flush().await?;

    // Query the grandchild near the Y axis: must fuse all three ancestry levels (this
    // errored on the `_vid` scalar index before #106) with A-y (closest) on top.
    let res = b
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [0.0, 1.0, 0.0], 5)
             YIELD node, score
             RETURN node.name AS name, score
             ORDER BY score DESC",
        )
        .await?;
    let names: Vec<String> = res
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();

    assert_eq!(
        res.rows()[0].get::<String>("name")?,
        "A-y",
        "grandchild query: closest (parent-level) vector ranks first; got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "P-x"),
        "root-inherited vector must be visible across 2 fork levels; got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "B-z"),
        "child-local vector must be present; got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "A-y"),
        "parent-level vector must be present; got {names:?}"
    );

    db.shutdown().await?;
    Ok(())
}
