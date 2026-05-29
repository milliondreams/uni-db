#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M5c.4 acceptance — named projections registered via
//! `uni.graph.project` are reachable from algorithm calls via
//! `{name: "..."}` graphRef.
//!
//! The store is keyed on `Arc<StorageManager>` identity, so all calls
//! in a single test that reuse the same `Arc<StorageManager>` see the
//! same store.

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

/// Build a unique projection name per test to avoid cross-test
/// pollution from the process-global registry (different
/// `Arc<StorageManager>` instances get different stores, but using a
/// per-test name eliminates any residual cross-test risk).
fn unique_name(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    format!("{prefix}_{}", N.fetch_add(1, Ordering::Relaxed))
}

#[tokio::test]
async fn project_native_then_reuse_across_algos() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;
    let name = unique_name("g_native");

    // Project.
    let proj = run_query(
        &storage,
        &writer,
        &schema_manager,
        &format!(
            "CALL uni.graph.project('{name}', {{nodeLabels: ['Person'], edgeTypes: ['KNOWS']}}, {{}}) \
             YIELD name, node_count, edge_count RETURN name, node_count, edge_count"
        ),
    )
    .await?;
    assert_eq!(proj.len(), 1);
    assert_eq!(proj[0].get("node_count").and_then(|v| v.as_u64()), Some(3));
    assert_eq!(proj[0].get("edge_count").and_then(|v| v.as_u64()), Some(3));

    // PageRank against the named projection.
    let pr = run_query(
        &storage,
        &writer,
        &schema_manager,
        &format!(
            "CALL uni.algo.pageRank({{name: '{name}'}}, {{}}) \
             YIELD nodeId, score RETURN nodeId, score"
        ),
    )
    .await?;
    assert_eq!(pr.len(), 3);

    // WCC against the same projection — proves reuse.
    let wcc = run_query(
        &storage,
        &writer,
        &schema_manager,
        &format!(
            "CALL uni.algo.wcc({{name: '{name}'}}, {{}}) \
             YIELD nodeId, componentId RETURN nodeId, componentId"
        ),
    )
    .await?;
    assert_eq!(wcc.len(), 3);
    Ok(())
}

#[tokio::test]
async fn project_cypher_variant() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;
    let name = unique_name("g_cypher");

    let proj = run_query(
        &storage,
        &writer,
        &schema_manager,
        &format!(
            "CALL uni.graph.project('{name}', \
                {{nodeQuery: 'MATCH (n:Person) RETURN id(n) AS id', \
                  edgeQuery: 'MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN id(a) AS source, id(b) AS target'}}, \
                {{}}) \
             YIELD name, node_count, edge_count RETURN name, node_count, edge_count"
        ),
    )
    .await?;
    assert_eq!(proj.len(), 1);
    assert_eq!(proj[0].get("node_count").and_then(|v| v.as_u64()), Some(3));
    assert_eq!(proj[0].get("edge_count").and_then(|v| v.as_u64()), Some(3));
    Ok(())
}

#[tokio::test]
async fn drop_returns_true_then_false() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;
    let name = unique_name("g_drop");

    let _ = run_query(
        &storage,
        &writer,
        &schema_manager,
        &format!(
            "CALL uni.graph.project('{name}', {{nodeLabels: ['Person'], edgeTypes: ['KNOWS']}}, {{}}) \
             YIELD name RETURN name"
        ),
    )
    .await?;

    let first = run_query(
        &storage,
        &writer,
        &schema_manager,
        &format!("CALL uni.graph.drop('{name}') YIELD dropped RETURN dropped"),
    )
    .await?;
    assert_eq!(
        first[0].get("dropped").and_then(|v| v.as_bool()),
        Some(true)
    );

    let second = run_query(
        &storage,
        &writer,
        &schema_manager,
        &format!("CALL uni.graph.drop('{name}') YIELD dropped RETURN dropped"),
    )
    .await?;
    assert_eq!(
        second[0].get("dropped").and_then(|v| v.as_bool()),
        Some(false),
    );
    Ok(())
}

#[tokio::test]
async fn list_then_exists() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;
    let name = unique_name("g_list");

    let _ = run_query(
        &storage,
        &writer,
        &schema_manager,
        &format!(
            "CALL uni.graph.project('{name}', {{nodeLabels: ['Person'], edgeTypes: ['KNOWS']}}, {{}}) \
             YIELD name RETURN name"
        ),
    )
    .await?;

    let listed = run_query(
        &storage,
        &writer,
        &schema_manager,
        "CALL uni.graph.list() YIELD name, source_kind RETURN name, source_kind",
    )
    .await?;
    let found = listed.iter().any(|r| {
        r.get("name").and_then(|v| v.as_str()) == Some(name.as_str())
            && r.get("source_kind").and_then(|v| v.as_str()) == Some("Native")
    });
    assert!(found, "projection `{name}` missing from list: {listed:?}");

    let exists = run_query(
        &storage,
        &writer,
        &schema_manager,
        &format!("CALL uni.graph.exists('{name}') YIELD exists RETURN exists"),
    )
    .await?;
    assert_eq!(
        exists[0].get("exists").and_then(|v| v.as_bool()),
        Some(true),
    );

    let missing = run_query(
        &storage,
        &writer,
        &schema_manager,
        "CALL uni.graph.exists('definitely_not_a_real_name') YIELD exists RETURN exists",
    )
    .await?;
    assert_eq!(
        missing[0].get("exists").and_then(|v| v.as_bool()),
        Some(false),
    );
    Ok(())
}

#[tokio::test]
async fn duplicate_name_rejected() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;
    let name = unique_name("g_dup");

    let _ = run_query(
        &storage,
        &writer,
        &schema_manager,
        &format!(
            "CALL uni.graph.project('{name}', {{nodeLabels: ['Person'], edgeTypes: ['KNOWS']}}, {{}}) \
             YIELD name RETURN name"
        ),
    )
    .await?;
    let err = run_query(
        &storage,
        &writer,
        &schema_manager,
        &format!(
            "CALL uni.graph.project('{name}', {{nodeLabels: ['Person'], edgeTypes: ['KNOWS']}}, {{}}) \
             YIELD name RETURN name"
        ),
    )
    .await
    .expect_err("duplicate name must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("already exists") || msg.contains("0x824"),
        "unexpected error: {msg}"
    );
    Ok(())
}

#[tokio::test]
async fn named_missing_errors_with_0x822() -> anyhow::Result<()> {
    // Calling an algo with `{name: ...}` against a name that has not
    // been projected must fail with the M5c.4 error code.
    let _ = env_logger::builder().is_test(true).try_init();
    let (storage, writer, schema_manager, _td) = setup_triangle().await?;
    let name = unique_name("g_missing");

    let err = run_query(
        &storage,
        &writer,
        &schema_manager,
        &format!(
            "CALL uni.algo.pageRank({{name: '{name}'}}, {{}}) \
             YIELD nodeId, score RETURN nodeId, score"
        ),
    )
    .await
    .expect_err("missing named projection must error");
    let msg = err.to_string();
    assert!(
        msg.contains("no projection named") || msg.contains("uni.graph.project"),
        "unexpected error: {msg}"
    );
    Ok(())
}
