// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for passing MATCH-bound variables as procedure call arguments.
//!
//! Verifies fix for GitHub issue #35: `uni.algo.shortestPath` rejects
//! Cypher variable arguments.

use anyhow::Result;
use uni_db::{Uni, Value};

/// Create a temporary database with a linked graph: A -> B -> C -> D
async fn setup_linked_graph() -> Result<Uni> {
    let db = Uni::temporary().build().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Node (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE LINK () FROM Node TO Node")
        .await?;

    tx.execute("CREATE (:Node {name: 'A'})").await?;
    tx.execute("CREATE (:Node {name: 'B'})").await?;
    tx.execute("CREATE (:Node {name: 'C'})").await?;
    tx.execute("CREATE (:Node {name: 'D'})").await?;

    tx.execute("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:LINK]->(b)")
        .await?;
    tx.execute("MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'}) CREATE (b)-[:LINK]->(c)")
        .await?;
    tx.execute("MATCH (c:Node {name: 'C'}), (d:Node {name: 'D'}) CREATE (c)-[:LINK]->(d)")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    Ok(db)
}

#[tokio::test]
async fn test_shortest_path_with_variable_args() -> Result<()> {
    let db = setup_linked_graph().await?;

    let results = db
        .session()
        .query(
            "MATCH (a:Node {name: 'A'}), (d:Node {name: 'D'})
             CALL uni.algo.shortestPath(a, d, ['LINK'])
             YIELD length
             RETURN length",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let length = results.rows()[0].value("length").unwrap();
    // Algo yields flow through Arrow Utf8 columns, so length arrives as String
    assert!(
        length == &Value::Int(3) || length == &Value::String("3".to_string()),
        "Expected length 3, got {:?}",
        length
    );

    Ok(())
}

#[tokio::test]
async fn test_shortest_path_with_where_filter() -> Result<()> {
    let db = setup_linked_graph().await?;

    let results = db
        .session()
        .query(
            "MATCH (start:Node), (end:Node)
             WHERE start.name = 'A' AND end.name = 'C'
             CALL uni.algo.shortestPath(start, end, ['LINK'])
             YIELD length
             RETURN length",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let length = results.rows()[0].value("length").unwrap();
    assert!(
        length == &Value::Int(2) || length == &Value::String("2".to_string()),
        "Expected length 2, got {:?}",
        length
    );

    Ok(())
}

#[tokio::test]
async fn test_shortest_path_no_path_found() -> Result<()> {
    let db = setup_linked_graph().await?;

    // D->A has no path (edges are directed A->B->C->D)
    let results = db
        .session()
        .query(
            "MATCH (d:Node {name: 'D'}), (a:Node {name: 'A'})
             CALL uni.algo.shortestPath(d, a, ['LINK'])
             YIELD length
             RETURN length",
        )
        .await?;

    // No path → zero result rows (inner join semantics in Apply)
    assert_eq!(results.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_shortest_path_multiple_pairs() -> Result<()> {
    let db = setup_linked_graph().await?;

    // Find shortest path from A to every other reachable node
    let results = db
        .session()
        .query(
            "MATCH (a:Node {name: 'A'}), (target:Node)
             WHERE target.name <> 'A'
             CALL uni.algo.shortestPath(a, target, ['LINK'])
             YIELD length
             RETURN target.name AS target, length
             ORDER BY length",
        )
        .await?;

    // A->B (1), A->B->C (2), A->B->C->D (3)
    assert_eq!(results.len(), 3);

    let lengths: Vec<i64> = results
        .rows()
        .iter()
        .map(|r| {
            let v = r.value("length").unwrap();
            match v {
                Value::Int(i) => *i,
                Value::String(s) => s.parse::<i64>().unwrap(),
                other => panic!("Expected int or string, got {:?}", other),
            }
        })
        .collect();
    assert_eq!(lengths, vec![1, 2, 3]);

    Ok(())
}

#[tokio::test]
async fn test_shortest_path_yield_node_ids() -> Result<()> {
    let db = setup_linked_graph().await?;

    let results = db
        .session()
        .query(
            "MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'})
             CALL uni.algo.shortestPath(a, c, ['LINK'])
             YIELD nodeIds, length
             RETURN nodeIds, length",
        )
        .await?;

    assert_eq!(results.len(), 1);

    let length = results.rows()[0].value("length").unwrap();
    assert!(
        length == &Value::Int(2) || length == &Value::String("2".to_string()),
        "Expected length 2, got {:?}",
        length
    );

    // nodeIds should be present (may come back as list or string depending on serialization)
    let node_ids = results.rows()[0].value("nodeIds").unwrap();
    match node_ids {
        Value::List(ids) => assert_eq!(ids.len(), 3, "Expected 3 node IDs: A, B, C"),
        Value::String(s) => {
            // May be JSON-serialized list
            assert!(
                s.contains(',') || s.contains('['),
                "Expected list-like, got: {}",
                s
            );
        }
        other => panic!("Expected nodeIds to be a list or string, got {:?}", other),
    }

    Ok(())
}
