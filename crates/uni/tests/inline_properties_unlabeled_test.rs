// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_inline_property_unlabeled() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create unlabeled nodes (no schema needed)
    db.session().execute("CREATE ({name: 'bar'})").await?;
    println!("Created node 1");

    // Test immediately after first CREATE (should be in L0)
    let immediate = db.session().query("MATCH (n) RETURN n").await?;
    println!("Immediately after first CREATE: {} nodes", immediate.len());

    db.session().execute("CREATE ({name: 'monkey'})").await?;
    db.session().execute("CREATE ({firstname: 'bar'})").await?;

    println!("Created 3 unlabeled nodes total");

    // Try to query ALL nodes to see if they exist
    let all_nodes = db.session().query("MATCH (n) RETURN n").await?;
    println!("Total nodes found (before flush): {}", all_nodes.len());

    // Try with WHERE instead of inline
    let with_where = db
        .session()
        .query("MATCH (n) WHERE n.name = 'bar' RETURN n")
        .await?;
    println!("With WHERE clause: {}", with_where.len());

    // Test inline property matching on unlabeled nodes
    let result = db
        .session()
        .query("MATCH (n {name: 'bar'}) RETURN n")
        .await?;
    println!("With inline property: {}", result.len());

    assert_eq!(
        result.len(),
        1,
        "Should match exactly one node with name='bar'"
    );

    Ok(())
}
