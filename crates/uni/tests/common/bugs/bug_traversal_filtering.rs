// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_traversal_label_filtering_bug() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // 1. Setup Schema
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .label("Robot")
        .property("model", DataType::String)
        .edge_type("OWNS", &["Person"], &["Robot"])
        .apply()
        .await?;

    // 2. Insert Data
    // Create a Person 'Human' and a Robot 'Beep'
    let tx = db.session().tx().await?;
    tx.execute("CREATE (p:Person {name: 'Human'})").await?;
    tx.execute("CREATE (r:Robot {model: 'Beep'})").await?;

    // Connect them: Person -> OWNS -> Robot
    tx.execute(
        "MATCH (p:Person {name: 'Human'}), (r:Robot {model: 'Beep'}) CREATE (p)-[:OWNS]->(r)",
    )
    .await?;
    tx.commit().await?;

    // 3. Test Cases

    // Case A: MATCH (p:Person)-[:OWNS]->(x:Person)
    // Expectation: 0 rows, because the neighbor is a Robot, not a Person.
    let results_wrong_label = db
        .session()
        .query("MATCH (p:Person)-[:OWNS]->(x:Person) RETURN x")
        .await?;

    // If the bug exists, this might return 1 row (the Robot), ignoring the :Person label on x.
    if !results_wrong_label.is_empty() {
        let row = results_wrong_label.rows()[0].clone();
        let val = row.get::<String>("x.model"); // Try to access a property that only exists on Robot
        println!(
            "Bug Reproduced! Expected 0 rows, got {}. First row: {:?}",
            results_wrong_label.len(),
            row
        );
        if let Ok(model) = val {
            println!(
                "Returned node appears to be the Robot with model: {}",
                model
            );
        }
    }
    assert_eq!(
        results_wrong_label.len(),
        0,
        "Expected 0 results for mismatched label traversal, but got {}",
        results_wrong_label.len()
    );

    // Case B: MATCH (p:Person)-[:OWNS]->(x:Robot)
    // Expectation: 1 row.
    let results_correct_label = db
        .session()
        .query("MATCH (p:Person)-[:OWNS]->(x:Robot) RETURN x")
        .await?;
    assert_eq!(
        results_correct_label.len(),
        1,
        "Expected 1 result for correct label traversal"
    );

    // Case C: Variable Length Traversal
    // MATCH (p:Person)-[:OWNS*1..2]->(x:Person)
    // Expectation: 0 rows, because neighbor is Robot.
    let results_var_len = db
        .session()
        .query("MATCH (p:Person)-[:OWNS*1..2]->(x:Person) RETURN x")
        .await?;
    assert_eq!(
        results_var_len.len(),
        0,
        "Expected 0 results for mismatched label var-len traversal"
    );

    // Case D: Variable Length Traversal (Correct Label)
    let results_var_len_correct = db
        .session()
        .query("MATCH (p:Person)-[:OWNS*1..2]->(x:Robot) RETURN x")
        .await?;
    assert_eq!(
        results_var_len_correct.len(),
        1,
        "Expected 1 result for correct label var-len traversal"
    );

    Ok(())
}

/// H4: a typed variable-length path with an inline edge-property condition
/// (`[r:KNOWS*1..2 {year: 1988}]`) must filter *flushed* (CSR/Lance) edges by
/// that condition, not just L0 in-memory edges. Before the fix the flushed-edge
/// branch in `expand_neighbors` set `passes = true` and deferred to a
/// hardcoded `EidFilter::AllAllowed`, so the predicate was ignored once edges
/// left L0 → over-matching.
#[tokio::test]
async fn test_vlp_edge_property_filter_on_flushed_edges() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    let db = Uni::open(path).build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property("year", DataType::Int)
        .apply()
        .await?;

    // a -KNOWS{year:1988}-> b -KNOWS{year:1999}-> c
    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:Person {name: 'a'}), (b:Person {name: 'b'}), (c:Person {name: 'c'})")
        .await?;
    tx.execute(
        "MATCH (a:Person {name: 'a'}), (b:Person {name: 'b'}) \
         CREATE (a)-[:KNOWS {year: 1988}]->(b)",
    )
    .await?;
    tx.execute(
        "MATCH (b:Person {name: 'b'}), (c:Person {name: 'c'}) \
         CREATE (b)-[:KNOWS {year: 1999}]->(c)",
    )
    .await?;
    tx.commit().await?;

    // Push the edges out of L0 into CSR/Lance — this is where the bug lived.
    db.flush().await?;

    // From 'a', following only year=1988 edges, only 'b' is reachable: the
    // b->c edge is year=1999 and must be excluded. With the bug, the flushed
    // b->c edge passes unfiltered and 'c' is wrongly returned too.
    let filtered = db
        .session()
        .query(
            "MATCH (a:Person {name: 'a'})-[r:KNOWS*1..2 {year: 1988}]->(x) \
             RETURN x.name AS name ORDER BY name",
        )
        .await?;
    let names: Vec<String> = filtered
        .rows()
        .iter()
        .map(|r| r.get::<String>("name").unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["b".to_string()],
        "VLP edge-property filter must exclude the flushed year=1999 edge; got {names:?}"
    );

    // Sanity: without the filter both b and c are reachable, proving the graph
    // (and the flush) is intact and the filter — not a missing edge — is what
    // dropped 'c'.
    let unfiltered = db
        .session()
        .query(
            "MATCH (a:Person {name: 'a'})-[r:KNOWS*1..2]->(x) \
             RETURN x.name AS name ORDER BY name",
        )
        .await?;
    let all_names: Vec<String> = unfiltered
        .rows()
        .iter()
        .map(|r| r.get::<String>("name").unwrap())
        .collect();
    assert_eq!(
        all_names,
        vec!["b".to_string(), "c".to_string()],
        "unfiltered VLP should reach both b and c"
    );

    Ok(())
}
