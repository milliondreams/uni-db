// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 5b — fork-local BM25 / FTS fusion.
//!
//! Confirms `Session::build_fork_local_index(label, column, FullText)`
//! builds a Lance native FTS (Inverted) index on the fork's branch,
//! and that FTS queries on the forked session return results
//! including both primary-inherited and fork-local rows via Lance's
//! `base_paths` chain.

// Rust guideline compliant

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::Uni;
use uni_store::fork::ForkLocalIndexKind;

#[tokio::test]
async fn fork_local_fts_index_returns_fused_results() -> Result<()> {
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("body", DataType::String)
        .apply()
        .await?;

    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute(
        "CREATE (:Doc {title: 'P-zebra', body: 'a primary article about zebras'})",
    )
    .await?;
    tx.execute(
        "CREATE (:Doc {title: 'P-other', body: 'a primary article about something else'})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = primary.fork("scenario").await?;
    let tx = forked.tx().await?;
    tx.execute(
        "CREATE (:Doc {title: 'F-zebra', body: 'a fork-only zebra story'})",
    )
    .await?;
    tx.commit().await?;
    forked.flush().await?;

    forked
        .build_fork_local_index("Doc", "body", ForkLocalIndexKind::FullText)
        .await?;

    // Phase 5b followup: planner emission for FTS. Explain should
    // show FusedIndexScanWrapped { kind: Bm25Rrf } wrapping the
    // original InvertedIndexLookup node.
    let plan = forked
        .query_with(
            "CALL uni.fts.query('Doc', 'body', 'zebra', 10)
             YIELD node, score
             RETURN node.title AS title",
        )
        .explain()
        .await?;
    assert!(
        plan.plan_text.contains("FusedIndexScanWrapped"),
        "expected FusedIndexScanWrapped after FullText registration; got {}",
        plan.plan_text
    );
    assert!(
        plan.plan_text.contains("Bm25Rrf"),
        "expected Bm25Rrf fusion kind; got {}",
        plan.plan_text
    );

    // Use the existing CALL-style FTS surface. For Phase 5b the
    // fusion comes from BranchedBackend routing through the fork's
    // branch — the surface is unchanged.
    let res = forked
        .query(
            "CALL uni.fts.query('Doc', 'body', 'zebra', 10)
             YIELD node, score
             RETURN node.title AS title, score
             ORDER BY score DESC",
        )
        .await?;

    let titles: Vec<String> = res
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("title").ok())
        .collect();

    assert!(
        titles.iter().any(|t| t == "F-zebra"),
        "fork-local zebra doc should be in results; got {titles:?}"
    );
    assert!(
        titles.iter().any(|t| t == "P-zebra"),
        "primary-inherited zebra doc should be in results; got {titles:?}"
    );

    db.shutdown().await?;
    Ok(())
}
