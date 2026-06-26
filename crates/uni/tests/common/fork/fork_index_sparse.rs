// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fork-local learned-sparse (SPLADE) search on forks/branches (issue #95 Task #4).
//!
//! `uni.sparse.query` over a `SparseVector` column used to return empty on a
//! forked session (`StorageManager::sparse_search` bailed on a branch). The fix
//! (Approach A) enumerates the branch's candidate vids via a branch-aware scan
//! and re-scores by exact in-process `sparse_dot`, fusing fork-local L0 +
//! fork-flushed branch + parent-inherited rows — exactly the path the
//! multivector feature uses. `ForkLocalIndexKind::Sparse` is a planner/EXPLAIN
//! marker that switches the call to the `SparseDot` fused operator.
//!
//! These tests confirm: a fork sees inherited + fork-local sparse docs fused and
//! correctly ranked; the parent is isolated from fork writes (and vice versa);
//! an inherited doc tombstoned before the fork point does not leak back; nested
//! forks resolve through ancestors; and the background builder auto-registers the
//! fork-local `Sparse` marker (observable as `SparseDot` in EXPLAIN).
//!
//! Corpus convention: the query vector is `query_vec()`; a doc with `emb ==
//! query` is the unique dot maximizer (16.75). Zero-overlap docs score 0 and are
//! dropped, so every doc asserted "present" carries query-overlapping terms.

// Rust guideline compliant

use std::time::Duration;

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::{IndexType, Session, Uni, Value};
use uni_store::fork::ForkLocalIndexKind;

const VOCAB: usize = 1000;

/// A sparse vector as parallel sorted-unique `(indices, values)`.
type Sparse = (Vec<u32>, Vec<f32>);

/// The fixed query sparse vector; a doc with `emb == query` is the dot maximizer.
fn query_vec() -> Sparse {
    (vec![1, 5, 9, 42, 77], vec![1.0, 2.0, 3.0, 0.5, 1.5])
}

fn sv_value((indices, values): &Sparse) -> Value {
    Value::SparseVector {
        indices: indices.clone(),
        values: values.clone(),
    }
}

/// In-memory DB with a `Doc.emb` sparse column + sparse index; fork sweeper and
/// auto index-builder disabled so tests drive builds explicitly.
async fn mk_db() -> Result<Uni> {
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::SparseVector { dimensions: VOCAB })
        .index("emb", IndexType::Sparse { dimensions: VOCAB })
        .apply()
        .await?;
    Ok(db)
}

/// Write `(title, emb)` docs through `session` in one transaction.
async fn write(session: &Session, docs: &[(&str, Sparse)]) -> Result<()> {
    let tx = session.tx().await?;
    for (title, sparse) in docs {
        tx.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
            .param("title", Value::String((*title).to_string()))
            .param("emb", sv_value(sparse))
            .run()
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Result titles (in descending score order) of a `uni.sparse.query` over `emb`.
async fn query_titles(session: &Session, k: usize) -> Result<Vec<String>> {
    let q = query_vec();
    let res = session
        .query_with(
            "CALL uni.sparse.query('Doc', 'emb', $q, $k, null, null, {}) \
             YIELD node, score RETURN node.title AS title, score ORDER BY score DESC",
        )
        .param("q", sv_value(&q))
        .param("k", Value::Int(k as i64))
        .fetch_all()
        .await?;
    Ok(res
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("title").ok())
        .collect())
}

/// EXPLAIN `plan_text` for a fork `uni.sparse.query` — carries the fusion kind.
async fn query_plan_text(session: &Session, k: usize) -> Result<String> {
    let q = query_vec();
    let plan = session
        .query_with(
            "CALL uni.sparse.query('Doc', 'emb', $q, $k, null, null, {}) \
             YIELD node, score RETURN node.title AS title",
        )
        .param("q", sv_value(&q))
        .param("k", Value::Int(k as i64))
        .explain()
        .await?;
    Ok(plan.plan_text)
}

/// A fork sparse query returns both fork-local and parent-inherited docs fused
/// and dot-ranked; after registering the fork-local marker, EXPLAIN shows the
/// `SparseDot` fused operator.
#[tokio::test]
async fn sparse_fork_local_returns_fused_results() -> Result<()> {
    let db = mk_db().await?;
    let primary = db.session();
    write(
        &primary,
        &[
            ("P-zebra", query_vec()),                  // maximizer (16.75)
            ("P-other", (vec![2, 3], vec![1.0, 1.0])), // zero overlap → dropped
        ],
    )
    .await?;
    db.flush().await?;

    let forked = primary.fork("scenario").await?;
    write(&forked, &[("F-zebra", query_vec())]).await?;
    forked.flush().await?;

    forked
        .build_fork_local_index("Doc", "emb", ForkLocalIndexKind::Sparse)
        .await?;

    // The registered fork-local Sparse marker switches the call to the fused
    // operator: EXPLAIN wraps the procedure in FusedIndexScanWrapped { SparseDot }.
    let plan_text = query_plan_text(&forked, 10).await?;
    assert!(
        plan_text.contains("FusedIndexScanWrapped"),
        "expected FusedIndexScanWrapped after Sparse registration; got {plan_text}"
    );
    assert!(
        plan_text.contains("SparseDot"),
        "expected SparseDot fusion kind; got {plan_text}"
    );

    let titles = query_titles(&forked, 10).await?;
    assert!(
        titles.iter().any(|t| t == "F-zebra"),
        "fork-local doc should be in results; got {titles:?}"
    );
    assert!(
        titles.iter().any(|t| t == "P-zebra"),
        "parent-inherited doc should be in results; got {titles:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// Fork writes are invisible to the parent's sparse query, and a parent write
/// after the fork point is invisible to the fork.
#[tokio::test]
async fn sparse_fork_isolation_both_ways() -> Result<()> {
    let db = mk_db().await?;
    let primary = db.session();
    write(&primary, &[("P-orig", query_vec())]).await?;
    db.flush().await?;

    let forked = primary.fork("isolation").await?;
    write(&forked, &[("F-target", query_vec())]).await?;
    forked.flush().await?;

    // Parent write AFTER the fork point.
    write(&primary, &[("P-after", query_vec())]).await?;
    db.flush().await?;

    let parent = query_titles(&primary, 10).await?;
    assert!(
        !parent.iter().any(|t| t == "F-target"),
        "parent must not see fork's writes: {parent:?}"
    );
    assert!(
        parent.iter().any(|t| t == "P-after"),
        "parent sees its own post-fork write: {parent:?}"
    );

    let fork = query_titles(&forked, 10).await?;
    assert!(
        !fork.iter().any(|t| t == "P-after"),
        "fork must not see parent writes after the fork point: {fork:?}"
    );
    assert!(
        fork.iter().any(|t| t == "F-target"),
        "fork sees its own write: {fork:?}"
    );
    assert!(
        fork.iter().any(|t| t == "P-orig"),
        "fork sees the parent-inherited doc: {fork:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// A vertex soft-deleted on the parent *before* forking (inherited via
/// `base_paths`) must not leak back into fork sparse results — the branch scan
/// honors the `_deleted = false` prefilter.
#[tokio::test]
async fn sparse_fork_honors_deleted_filter() -> Result<()> {
    let db = mk_db().await?;
    let primary = db.session();
    write(&primary, &[("keep", query_vec()), ("gone", query_vec())]).await?;
    db.flush().await?;

    // Soft-delete one doc on PRIMARY before forking, then flush so the tombstone
    // lands in the dataset the fork will branch from.
    let tx = primary.tx().await?;
    tx.execute("MATCH (d:Doc {title: 'gone'}) DETACH DELETE d")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = primary.fork("deleted_filter").await?;
    write(&forked, &[("fork", query_vec())]).await?;
    forked.flush().await?;

    forked
        .build_fork_local_index("Doc", "emb", ForkLocalIndexKind::Sparse)
        .await?;

    let titles = query_titles(&forked, 10).await?;
    assert!(
        !titles.iter().any(|t| t == "gone"),
        "soft-deleted (inherited) doc leaked into fork sparse results; got {titles:?}"
    );
    assert!(
        titles.iter().any(|t| t == "keep"),
        "live inherited doc should still be returned; got {titles:?}"
    );
    assert!(
        titles.iter().any(|t| t == "fork"),
        "fork-local doc should be returned; got {titles:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// A grandchild fork sees docs through the full ancestor chain: the parent
/// fork's dot maximizer ranks first, with both the root-inherited and the
/// child-local docs present.
#[tokio::test]
async fn nested_fork_sparse_resolves_through_ancestors() -> Result<()> {
    let db = mk_db().await?;
    let primary = db.session();
    write(&primary, &[("P-root", (vec![9], vec![1.0]))]).await?; // dot 3.0
    db.flush().await?;

    let a = primary.fork("parent_fork").await?;
    write(&a, &[("A-target", query_vec())]).await?; // dot 16.75 (max)
    a.flush().await?;

    let b = a.fork("child_fork").await?;
    write(&b, &[("B-rival", (vec![1, 5], vec![1.0, 1.0]))]).await?; // dot 3.0
    b.flush().await?;

    let order = query_titles(&b, 10).await?;
    assert_eq!(
        order.first().map(String::as_str),
        Some("A-target"),
        "grandchild sees the ancestor's maximizer first: {order:?}"
    );
    assert!(
        order.iter().any(|t| t == "B-rival"),
        "child-local doc present: {order:?}"
    );
    assert!(
        order.iter().any(|t| t == "P-root"),
        "root-inherited doc present: {order:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// The background index builder auto-registers the fork-local `Sparse` marker for
/// fork rows written after branch-creation. Results are already correct via the
/// brute-force branch scan, so the observable transition is EXPLAIN flipping to
/// the `SparseDot` fused operator.
#[tokio::test]
async fn sparse_fork_auto_built_for_new_rows() -> Result<()> {
    // Index builder ENABLED; short interval + threshold 1 so a single fork
    // fragment triggers an auto-build promptly.
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        fork_index_builder_interval: Duration::from_millis(100),
        fork_index_build_threshold: 1,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::SparseVector { dimensions: VOCAB })
        .index("emb", IndexType::Sparse { dimensions: VOCAB })
        .apply()
        .await?;

    let primary = db.session();
    write(&primary, &[("P-zebra", query_vec())]).await?;
    db.flush().await?;

    let forked = primary.fork("auto_build").await?;
    write(&forked, &[("F-zebra", query_vec())]).await?;
    forked.flush().await?;

    // Do NOT manually build. Poll until the background builder registers the
    // fork-local Sparse marker — observable as SparseDot in the EXPLAIN plan.
    let mut saw_marker = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if query_plan_text(&forked, 10).await?.contains("SparseDot") {
            saw_marker = true;
            break;
        }
    }
    assert!(
        saw_marker,
        "fork-local Sparse marker was not auto-registered; EXPLAIN never showed SparseDot"
    );

    // Brute-force fusion makes both docs matchable regardless of the marker.
    let titles = query_titles(&forked, 10).await?;
    assert!(
        titles.iter().any(|t| t == "F-zebra"),
        "fork-local doc should be matchable: {titles:?}"
    );
    assert!(
        titles.iter().any(|t| t == "P-zebra"),
        "parent-inherited doc should be matchable: {titles:?}"
    );

    db.shutdown().await?;
    Ok(())
}
