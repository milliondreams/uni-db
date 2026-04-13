// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for read operations (MATCH, aggregation, OPTIONAL MATCH,
//! DISTINCT, UNION, CASE, window functions).

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_common::Value;
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

/// Seed the graph with test data: 3 persons with a KNOWS chain.
async fn seed_test_data(
    executor: &Executor,
    planner: &QueryPlanner,
    prop_manager: &PropertyManager,
) {
    execute_cypher(
        executor,
        planner,
        prop_manager,
        "CREATE (a:Person {name: 'Alice', age: 30})-[:KNOWS]->(b:Person {name: 'Bob', age: 25})-[:KNOWS]->(c:Person {name: 'Charlie', age: 35})",
    )
    .await;
}

// ── Scan tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_scan_with_label_filter() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;
    seed_test_data(&executor, &planner, &prop_manager).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person) RETURN n.name AS name",
    )
    .await;

    assert_eq!(rows.len(), 3, "Should find 3 Person vertices");
}

#[tokio::test]
async fn test_scan_with_where_filter() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;
    seed_test_data(&executor, &planner, &prop_manager).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person) WHERE n.age > 28 RETURN n.name AS name ORDER BY name",
    )
    .await;

    assert_eq!(rows.len(), 2, "Alice(30) and Charlie(35) match age > 28");
}

// ── Traversal tests ──────────────────────────────────────────────────

#[tokio::test]
async fn test_traverse_outgoing() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;
    seed_test_data(&executor, &planner, &prop_manager).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS dst ORDER BY src",
    )
    .await;

    assert_eq!(rows.len(), 2, "Alice->Bob and Bob->Charlie");
}

// ── Variable-length path test ────────────────────────────────────────

#[tokio::test]
async fn test_traverse_variable_length() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;
    seed_test_data(&executor, &planner, &prop_manager).await;

    // Alice->Bob->Charlie: variable-length 1..2 from Alice should reach both Bob and Charlie
    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person) RETURN b.name AS name ORDER BY name",
    )
    .await;

    assert!(
        rows.len() >= 2,
        "Variable-length path should reach at least Bob and Charlie, got {} rows",
        rows.len()
    );
}

// ── Aggregation tests ────────────────────────────────────────────────

#[tokio::test]
async fn test_aggregation_count() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;
    seed_test_data(&executor, &planner, &prop_manager).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person) RETURN count(n) AS cnt",
    )
    .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("cnt"), Some(&Value::Int(3)));
}

#[tokio::test]
async fn test_aggregation_sum_avg() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;
    seed_test_data(&executor, &planner, &prop_manager).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person) RETURN sum(n.age) AS total, avg(n.age) AS average",
    )
    .await;

    assert_eq!(rows.len(), 1);
    // 30 + 25 + 35 = 90
    let total = rows[0].get("total").unwrap();
    assert!(
        total == &Value::Int(90) || total == &Value::Float(90.0),
        "Sum should be 90, got {:?}",
        total
    );
    // Average = 30.0
    let avg = rows[0].get("average").unwrap();
    if let Value::Float(f) = avg {
        assert!((f - 30.0).abs() < 0.01, "Average should be 30.0, got {}", f);
    } else {
        panic!("Average should be Float, got {:?}", avg);
    }
}

#[tokio::test]
async fn test_aggregation_collect() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;
    seed_test_data(&executor, &planner, &prop_manager).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person) RETURN collect(n.name) AS names",
    )
    .await;

    assert_eq!(rows.len(), 1);
    if let Some(Value::List(names)) = rows[0].get("names") {
        assert_eq!(names.len(), 3, "Should collect 3 names");
    } else {
        panic!(
            "Expected list for collect(), got {:?}",
            rows[0].get("names")
        );
    }
}

// ── GROUP BY test ────────────────────────────────────────────────────

#[tokio::test]
async fn test_aggregation_group_by() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    // Create persons with overlapping ages
    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "CREATE (:Person {name: 'A', age: 30}), (:Person {name: 'B', age: 25}), (:Person {name: 'C', age: 30})",
    )
    .await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person) RETURN n.age AS age, count(n) AS cnt ORDER BY age",
    )
    .await;

    assert_eq!(rows.len(), 2, "Two distinct age groups");
    // age 25: 1 person, age 30: 2 persons
    assert_eq!(rows[0].get("age"), Some(&Value::Int(25)));
    assert_eq!(rows[0].get("cnt"), Some(&Value::Int(1)));
    assert_eq!(rows[1].get("age"), Some(&Value::Int(30)));
    assert_eq!(rows[1].get("cnt"), Some(&Value::Int(2)));
}

// ── OPTIONAL MATCH tests ─────────────────────────────────────────────

#[tokio::test]
async fn test_optional_match_null() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;
    seed_test_data(&executor, &planner, &prop_manager).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person {name: 'Charlie'}) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n.name AS n, m.name AS m",
    )
    .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("n"), Some(&Value::String("Charlie".into())));
    // Charlie has no outgoing KNOWS → m should be null
    let m_val = rows[0].get("m");
    assert!(
        m_val == Some(&Value::Null) || m_val.is_none(),
        "OPTIONAL MATCH with no match should produce null"
    );
}

// ── DISTINCT test ────────────────────────────────────────────────────

#[tokio::test]
async fn test_distinct() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    // Create two persons with same age
    execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "CREATE (:Person {name: 'A', age: 30}), (:Person {name: 'B', age: 30}), (:Person {name: 'C', age: 25})",
    )
    .await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person) RETURN DISTINCT n.age AS age ORDER BY age",
    )
    .await;

    assert_eq!(rows.len(), 2, "DISTINCT should collapse duplicate ages");
}

// ── UNION tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_union_all() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "RETURN 1 AS x UNION ALL RETURN 1 AS x",
    )
    .await;

    assert_eq!(rows.len(), 2, "UNION ALL should keep duplicates");
}

#[tokio::test]
async fn test_union_dedup() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "RETURN 1 AS x UNION RETURN 1 AS x",
    )
    .await;

    assert_eq!(rows.len(), 1, "UNION should deduplicate");
}

// ── CASE expression test ─────────────────────────────────────────────

#[tokio::test]
async fn test_case_expression() {
    let dir = tempdir().unwrap();
    let (executor, prop_manager, _schema, planner) = setup_graph_executor(dir.path()).await;
    seed_test_data(&executor, &planner, &prop_manager).await;

    let rows = execute_cypher(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (n:Person) RETURN n.name AS name, CASE WHEN n.age >= 30 THEN 'senior' ELSE 'junior' END AS category ORDER BY name",
    )
    .await;

    assert_eq!(rows.len(), 3);
    // Alice(30) = senior, Bob(25) = junior, Charlie(35) = senior
    for row in &rows {
        let name = row.get("name").unwrap().as_str().unwrap();
        let cat = row.get("category").unwrap().as_str().unwrap();
        match name {
            "Alice" | "Charlie" => assert_eq!(cat, "senior"),
            "Bob" => assert_eq!(cat, "junior"),
            _ => panic!("Unexpected name: {}", name),
        }
    }
}

// ── Advanced execution tests ─────────────────────────────────────────
// NOTE: Procedure calls (CALL db.labels()) and time-travel queries require
// additional setup (ProcedureRegistry, snapshot pinning) that goes beyond
// the basic executor setup. These are covered by the TCK test suite.
