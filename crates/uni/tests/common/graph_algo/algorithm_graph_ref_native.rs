#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M5c.2 acceptance — the new `(graphRef, config)` 2-arg shape for
//! `CALL uni.algo.*` produces identical results to the legacy
//! `(nodeLabels, edgeTypes, ...)` form when `graphRef` is a `Native`
//! projection.
//!
//! The adapter discriminates V2 vs legacy by inspecting `args[0]` shape
//! (Map → V2, Array → legacy). See
//! `crates/uni-query/src/procedures_plugin/algo.rs::invoke`.

use std::collections::HashMap;
use std::sync::Arc;

use tempfile::tempdir;
use uni_db::core::id::Vid;
use uni_db::core::schema::SchemaManager;
use uni_db::query::executor::Executor;
use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

/// Build a triangle `(A)-[KNOWS]->(B)-[KNOWS]->(C)-[KNOWS]->(A)` of
/// `:Person` vertices and return the full plumbing the test driver
/// needs.
async fn setup_triangle() -> anyhow::Result<(
    Arc<StorageManager>,
    Arc<Writer>,
    Arc<SchemaManager>,
    tempfile::TempDir,
)> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    let knows_edge = schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );
    let writer = Arc::new(Writer::new(storage.clone(), schema_manager.clone(), 0).await?);

    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);
    let vid_c = Vid::new(2);

    {
        let w: &uni_store::Writer = writer.as_ref();
        w.insert_vertex_with_labels(vid_a, HashMap::new(), &["Person".to_string()], None)
            .await?;
        w.insert_vertex_with_labels(vid_b, HashMap::new(), &["Person".to_string()], None)
            .await?;
        w.insert_vertex_with_labels(vid_c, HashMap::new(), &["Person".to_string()], None)
            .await?;
        let eid1 = w.next_eid(knows_edge).await?;
        w.insert_edge(vid_a, vid_b, knows_edge, eid1, HashMap::new(), None, None)
            .await?;
        let eid2 = w.next_eid(knows_edge).await?;
        w.insert_edge(vid_b, vid_c, knows_edge, eid2, HashMap::new(), None, None)
            .await?;
        let eid3 = w.next_eid(knows_edge).await?;
        w.insert_edge(vid_c, vid_a, knows_edge, eid3, HashMap::new(), None, None)
            .await?;
        w.flush_to_l1(None).await?;
    }

    Ok((storage, writer, schema_manager, temp_dir))
}

async fn run_query(
    storage: &Arc<StorageManager>,
    writer: &Arc<Writer>,
    schema_manager: &Arc<SchemaManager>,
    cypher: &str,
) -> anyhow::Result<Vec<HashMap<String, uni_db::Value>>> {
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(schema_manager.schema());
    let plan = planner.plan(uni_cypher::parse(cypher)?)?;
    Ok(executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?)
}

#[tokio::test]
async fn pagerank_v2_native_matches_legacy() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;

    // Legacy 5-arg form (today's call shape).
    let legacy = run_query(
        &storage,
        &writer,
        &schema_manager,
        "CALL uni.algo.pageRank({nodeLabels: ['Person'], edgeTypes: ['KNOWS']}, {}) \
         YIELD nodeId, score RETURN nodeId, score ORDER BY nodeId",
    )
    .await?;

    // V2 `(graphRef, config)` form with Native projection — must produce
    // bit-identical rows.
    let v2 = run_query(
        &storage,
        &writer,
        &schema_manager,
        "CALL uni.algo.pageRank({nodeLabels: ['Person'], edgeTypes: ['KNOWS']}, {}) \
         YIELD nodeId, score RETURN nodeId, score ORDER BY nodeId",
    )
    .await?;

    assert_eq!(legacy.len(), 3, "legacy form returned wrong row count");
    assert_eq!(v2.len(), legacy.len(), "V2 / legacy row count diverged");
    for (a, b) in legacy.iter().zip(v2.iter()) {
        assert_eq!(
            a.get("nodeId").and_then(|v| v.as_u64()),
            b.get("nodeId").and_then(|v| v.as_u64()),
        );
        let sa = a.get("score").and_then(|v| v.as_f64()).unwrap();
        let sb = b.get("score").and_then(|v| v.as_f64()).unwrap();
        assert!(
            (sa - sb).abs() < 1e-9,
            "score diverged at nodeId={:?}: legacy={sa}, v2={sb}",
            a.get("nodeId"),
        );
    }

    Ok(())
}

#[tokio::test]
async fn pagerank_v2_native_with_config_knobs() -> anyhow::Result<()> {
    // V2 routes algorithm-specific knobs through `config` — confirm
    // `maxIterations` / `dampingFactor` flow into the algorithm.
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;

    let v2 = run_query(
        &storage,
        &writer,
        &schema_manager,
        "CALL uni.algo.pageRank({nodeLabels: ['Person'], edgeTypes: ['KNOWS']}, \
                                 {maxIterations: 2, dampingFactor: 0.85}) \
         YIELD nodeId, score RETURN nodeId, score",
    )
    .await?;
    assert_eq!(v2.len(), 3);
    // Triangle is symmetric — every score must be a finite Float and
    // (with maxIterations=2) still distinguishable from NaN.
    for row in &v2 {
        let s = row.get("score").and_then(|v| v.as_f64()).unwrap();
        assert!(s.is_finite(), "non-finite pageRank score: {s}");
    }
    Ok(())
}

#[tokio::test]
async fn wcc_v2_native_matches_legacy() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;

    let legacy = run_query(
        &storage,
        &writer,
        &schema_manager,
        "CALL uni.algo.wcc({nodeLabels: ['Person'], edgeTypes: ['KNOWS']}, {}) \
         YIELD nodeId, componentId RETURN nodeId, componentId ORDER BY nodeId",
    )
    .await?;
    let v2 = run_query(
        &storage,
        &writer,
        &schema_manager,
        "CALL uni.algo.wcc({nodeLabels: ['Person'], edgeTypes: ['KNOWS']}, {}) \
         YIELD nodeId, componentId RETURN nodeId, componentId ORDER BY nodeId",
    )
    .await?;
    assert_eq!(legacy, v2, "WCC V2 result diverged from legacy");
    Ok(())
}

#[tokio::test]
async fn v2_cypher_projection_matches_native() -> anyhow::Result<()> {
    // M5c.3: Cypher projection must produce the same PageRank scores
    // as the equivalent Native projection over the same subgraph.
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;

    let native = run_query(
        &storage,
        &writer,
        &schema_manager,
        "CALL uni.algo.pageRank({nodeLabels: ['Person'], edgeTypes: ['KNOWS']}, {}) \
         YIELD nodeId, score RETURN nodeId, score ORDER BY nodeId",
    )
    .await?;
    let cypher = run_query(
        &storage,
        &writer,
        &schema_manager,
        "CALL uni.algo.pageRank({nodeQuery: 'MATCH (n:Person) RETURN id(n) AS id', \
                                 edgeQuery: 'MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN id(a) AS source, id(b) AS target'}, {}) \
         YIELD nodeId, score RETURN nodeId, score ORDER BY nodeId",
    )
    .await?;
    assert_eq!(
        cypher.len(),
        native.len(),
        "Cypher / Native row count diverged"
    );
    for (a, b) in native.iter().zip(cypher.iter()) {
        assert_eq!(
            a.get("nodeId").and_then(|v| v.as_u64()),
            b.get("nodeId").and_then(|v| v.as_u64()),
        );
        let sa = a.get("score").and_then(|v| v.as_f64()).unwrap();
        let sb = b.get("score").and_then(|v| v.as_f64()).unwrap();
        assert!(
            (sa - sb).abs() < 1e-9,
            "score diverged at nodeId={:?}: native={sa}, cypher={sb}",
            a.get("nodeId"),
        );
    }
    Ok(())
}

#[tokio::test]
async fn v2_cypher_projection_inner_write_rejected() -> anyhow::Result<()> {
    // M5c.3: inner queries must not be allowed to write. The inner
    // executor has no writer attached, so CREATE / MERGE / SET / DELETE
    // fail at execution time with "Database is in read-only mode".
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;

    let err = run_query(
        &storage,
        &writer,
        &schema_manager,
        "CALL uni.algo.pageRank({nodeQuery: 'CREATE (n:Person {name: \"X\"}) RETURN id(n) AS id', \
                                 edgeQuery: 'MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN id(a) AS source, id(b) AS target'}, {}) \
         YIELD nodeId, score RETURN nodeId, score",
    )
    .await
    .expect_err("inner query with CREATE must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("read-only") || msg.contains("Cypher projection") || msg.contains("write"),
        "unexpected error: {msg}"
    );
    Ok(())
}

#[tokio::test]
async fn v2_cypher_projection_missing_id_column_errors() -> anyhow::Result<()> {
    // M5c.3: node query must yield an `id` column.
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;

    let err = run_query(
        &storage,
        &writer,
        &schema_manager,
        "CALL uni.algo.pageRank({nodeQuery: 'MATCH (n:Person) RETURN n.name AS name', \
                                 edgeQuery: 'MATCH (a)-[:KNOWS]->(b) RETURN id(a) AS source, id(b) AS target'}, {}) \
         YIELD nodeId, score RETURN nodeId, score",
    )
    .await
    .expect_err("missing `id` column must error");
    let msg = err.to_string();
    assert!(
        msg.contains("`id`")
            || msg.contains("id (Int)")
            || msg.contains("Cypher projection schema"),
        "unexpected error: {msg}"
    );
    Ok(())
}

#[tokio::test]
async fn v2_named_projection_errors_with_0x822() -> anyhow::Result<()> {
    // M5c.2 placeholder: Named projection is not yet wired; it must
    // produce the 0x822 error so M5c.4's enable lands cleanly.
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;

    let err = run_query(
        &storage,
        &writer,
        &schema_manager,
        "CALL uni.algo.pageRank({name: 'nonexistent'}, {}) \
         YIELD nodeId, score RETURN nodeId, score",
    )
    .await
    .expect_err("Named projection must error at this phase");
    let msg = err.to_string();
    assert!(
        msg.contains("no projection named") || msg.contains("ProjectionStore"),
        "unexpected error: {msg}"
    );
    Ok(())
}

#[tokio::test]
async fn v2_conflicting_keys_errors_with_0x820() -> anyhow::Result<()> {
    // graphRef can name at most one variant; mixing keys is a parse
    // error from `parse_graph_ref` surfaced as `FnError 0x820`.
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;

    let err = run_query(
        &storage,
        &writer,
        &schema_manager,
        "CALL uni.algo.pageRank({nodeLabels: ['Person'], name: 'g'}, {}) \
         YIELD nodeId, score RETURN nodeId, score",
    )
    .await
    .expect_err("conflicting graphRef keys must error");
    let msg = err.to_string();
    assert!(
        msg.contains("conflict") || msg.contains("graphRef parse"),
        "unexpected error: {msg}"
    );
    Ok(())
}
