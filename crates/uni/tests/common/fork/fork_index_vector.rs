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
