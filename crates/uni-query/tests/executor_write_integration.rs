// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for write operations (CREATE, SET, DELETE, MERGE, REMOVE, FOREACH).

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_common::Value;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_query::query::executor::Executor;
use uni_query::query::planner::QueryPlanner;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

async fn setup_graph_executor(
    path: &std::path::Path,
) -> (
    Executor,
    Arc<PropertyManager>,
    Arc<SchemaManager>,
    QueryPlanner,
) {
    let schema_manager = SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();

    schema_manager.add_label("Person").unwrap();
    schema_manager
        .add_property("Person", "name", DataType::String, true)
        .unwrap();
    schema_manager
        .add_property("Person", "age", DataType::Int32, true)
        .unwrap();
    schema_manager.add_label("Company").unwrap();
    schema_manager
        .add_property("Company", "name", DataType::String, true)
        .unwrap();
    schema_manager
        .add_edge_type("KNOWS", vec!["Person".into()], vec!["Person".into()])
        .unwrap();
    schema_manager
        .add_edge_type("WORKS_AT", vec!["Person".into()], vec!["Company".into()])
        .unwrap();
    schema_manager.save().await.unwrap();

    let planner = QueryPlanner::new(schema_manager.schema());
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await
        .unwrap(),
    );

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    let prop_manager = Arc::new(PropertyManager::new(
        storage.clone(),
        schema_manager.clone(),
        100,
    ));
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());

    (executor, prop_manager, schema_manager, planner)
}

async fn execute_cypher(
    executor: &Executor,
    planner: &QueryPlanner,
    prop_manager: &PropertyManager,
    cypher: &str,
) -> Vec<HashMap<String, Value>> {
    let query = uni_cypher::parse(cypher).unwrap();
    let plan = planner.plan(query).unwrap();
    executor
        .execute(plan, prop_manager, &HashMap::new())
        .await
        .unwrap()
}

// ── CREATE tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_single_vertex() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "CREATE (n:Person {name: 'Alice', age: 30}) RETURN n.name AS name",
    )
    .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("name"),
        Some(&Value::String("Alice".to_string()))
    );
}

#[tokio::test]
async fn test_create_vertex_and_edge() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a.name AS a, b.name AS b",
    )
    .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("a"), Some(&Value::String("Alice".into())));
    assert_eq!(rows[0].get("b"), Some(&Value::String("Bob".into())));
}

#[tokio::test]
async fn test_create_then_match_count() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    // Create 3 persons
    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "CREATE (:Person {name: 'A'}), (:Person {name: 'B'}), (:Person {name: 'C'})",
    )
    .await;

    // Match and count
    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person) RETURN count(n) AS cnt",
    )
    .await;

    assert_eq!(rows.len(), 1);
    let cnt = rows[0].get("cnt").unwrap();
    assert_eq!(cnt, &Value::Int(3));
}

// ── SET tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_set_property() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "CREATE (:Person {name: 'Alice', age: 30})",
    )
    .await;

    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person {name: 'Alice'}) SET n.age = 35",
    )
    .await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person {name: 'Alice'}) RETURN n.age AS age",
    )
    .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("age"), Some(&Value::Int(35)));
}

// ── DELETE tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_delete_isolated_vertex() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "CREATE (:Person {name: 'ToDelete'})",
    )
    .await;

    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person {name: 'ToDelete'}) DELETE n",
    )
    .await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person {name: 'ToDelete'}) RETURN n",
    )
    .await;

    assert_eq!(rows.len(), 0, "Deleted vertex should not appear in MATCH");
}

#[tokio::test]
async fn test_detach_delete_with_edges() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})",
    )
    .await;

    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person {name: 'Alice'}) DETACH DELETE n",
    )
    .await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person {name: 'Alice'}) RETURN n",
    )
    .await;
    assert_eq!(rows.len(), 0, "Detach-deleted vertex should be gone");
}

// ── MERGE tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_merge_create_when_missing() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MERGE (n:Person {name: 'Alice'}) RETURN n.name AS name",
    )
    .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("name"),
        Some(&Value::String("Alice".into())),
        "MERGE should create when not exists"
    );
}

#[tokio::test]
async fn test_merge_match_when_exists() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "CREATE (:Person {name: 'Alice'})",
    )
    .await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MERGE (n:Person {name: 'Alice'}) RETURN n.name AS name",
    )
    .await;

    assert_eq!(rows.len(), 1);

    // Should NOT have created a second Alice
    let count_rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person {name: 'Alice'}) RETURN count(n) AS cnt",
    )
    .await;
    assert_eq!(count_rows[0].get("cnt"), Some(&Value::Int(1)));
}

// ── REMOVE tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_remove_property() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "CREATE (:Person {name: 'Alice', age: 30})",
    )
    .await;

    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person {name: 'Alice'}) REMOVE n.age",
    )
    .await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person {name: 'Alice'}) RETURN n.age AS age",
    )
    .await;

    assert_eq!(rows.len(), 1);
    let age_val = rows[0].get("age");
    assert!(
        age_val == Some(&Value::Null) || age_val.is_none(),
        "Removed property should be null"
    );
}

// ── UNWIND test ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_unwind_list() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "UNWIND [1, 2, 3] AS x RETURN x",
    )
    .await;

    assert_eq!(rows.len(), 3);
}

// ── SET replace/merge tests ──────────────────────────────────────────

#[tokio::test]
async fn test_set_merge_properties() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "CREATE (:Person {name: 'Alice', age: 30})",
    )
    .await;

    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person {name: 'Alice'}) SET n += {city: 'NYC'}",
    )
    .await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person {name: 'Alice'}) RETURN n.age AS age, n.city AS city",
    )
    .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("age"),
        Some(&Value::Int(30)),
        "Merge mode should preserve existing age"
    );
    assert_eq!(
        rows[0].get("city"),
        Some(&Value::String("NYC".into())),
        "Merge mode should add new city"
    );
}

// ── MERGE ON CREATE SET test ─────────────────────────────────────────

#[tokio::test]
async fn test_merge_on_create_set() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.age = 30 RETURN n.name AS name, n.age AS age",
    )
    .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("name"), Some(&Value::String("Alice".into())));
    assert_eq!(
        rows[0].get("age"),
        Some(&Value::Int(30)),
        "ON CREATE SET should set age on new node"
    );
}

#[tokio::test]
async fn test_unwind_empty() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "UNWIND [] AS x RETURN x",
    )
    .await;

    assert_eq!(rows.len(), 0);
}
