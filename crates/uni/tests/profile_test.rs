// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use tempfile::tempdir;
use uni_db::UniBuilder;

#[tokio::test]
async fn test_profile_basic() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let db = UniBuilder::new(path.to_str().unwrap().to_string())
        .build()
        .await?;

    // Create schema
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING, age INT)")
        .await?;
    tx.execute("CREATE LABEL City (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE LIVES_IN () FROM Person TO City")
        .await?;
    tx.commit().await?;

    // Insert data
    let tx = db.session().tx().await?;
    tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        .await?;
    tx.execute("CREATE (c:City {name: 'London'})").await?;
    tx.execute("MATCH (p:Person), (c:City) WHERE p.name = 'Alice' AND c.name = 'London' CREATE (p)-[:LIVES_IN]->(c)").await?;
    tx.commit().await?;

    // Profile query — the CLI strips "PROFILE" before calling profile()
    let clean_query = "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name";
    let (result, profile) = db.session().query_with(clean_query).profile().await?;

    println!("Profile Stats: {:#?}", profile.runtime_stats);

    assert_eq!(result.len(), 1);

    // Granular per-operator stats: must have more than a single summary entry
    assert!(
        profile.runtime_stats.len() > 1,
        "Expected granular per-operator stats, got {} entries: {:?}",
        profile.runtime_stats.len(),
        profile.runtime_stats
    );

    let operators: Vec<String> = profile
        .runtime_stats
        .iter()
        .map(|s| s.operator.clone())
        .collect();
    println!("Operators: {:?}", operators);

    // Expect graph scan and traverse operators from the custom DataFusion exec nodes
    assert!(
        operators.iter().any(|op| op.contains("GraphScanExec")),
        "Expected a GraphScanExec operator, got: {:?}",
        operators
    );
    assert!(
        operators.iter().any(|op| op.contains("Traverse")),
        "Expected a Traverse operator, got: {:?}",
        operators
    );

    // Check total time is present
    let _ = profile.total_time_ms;

    // The scan operator should report rows > 0
    let scan = profile
        .runtime_stats
        .iter()
        .find(|s| s.operator.contains("GraphScanExec"))
        .unwrap();
    assert!(
        scan.actual_rows > 0,
        "GraphScanExec should report rows, got {}",
        scan.actual_rows
    );

    Ok(())
}
