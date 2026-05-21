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

/// Profile a transaction WRITE via `tx.execute_with(cypher).profile()`.
/// Asserts that the returned `(ExecuteResult, ProfileOutput)` carries both
/// (a) mutation counters from the tx's private L0 and (b) profile stats.
#[tokio::test]
async fn test_tx_profile_create_returns_execute_result_and_profile_output() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db = UniBuilder::new(temp_dir.path().to_str().unwrap().to_string())
        .build()
        .await?;

    // Schema
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING, age INT)")
        .await?;
    tx.commit().await?;

    // Profile a CREATE inside a transaction.
    let tx = db.session().tx().await?;
    let (res, prof) = tx
        .execute_with("CREATE (p:Person {name: 'Alice', age: 30}) RETURN p")
        .profile()
        .await?;
    tx.commit().await?;

    assert_eq!(
        res.nodes_created(),
        1,
        "expected 1 node created, got {}",
        res.nodes_created()
    );
    assert_eq!(res.properties_set(), 2);
    assert!(
        !prof.runtime_stats.is_empty(),
        "expected runtime_stats to be populated, got empty"
    );
    let _ = prof.total_time_ms;

    Ok(())
}

/// Profile a parametrised transaction write via `.param(...).profile()`.
#[tokio::test]
async fn test_tx_profile_with_params() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db = UniBuilder::new(temp_dir.path().to_str().unwrap().to_string())
        .build()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Item (sku STRING, qty INT)")
        .await?;
    tx.execute("CREATE (:Item {sku: 'A', qty: 1})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let (res, prof) = tx
        .execute_with("MATCH (i:Item {sku: $sku}) SET i.qty = $qty RETURN i")
        .param("sku", "A")
        .param("qty", 42i64)
        .profile()
        .await?;
    tx.commit().await?;

    assert!(
        res.properties_set() >= 1,
        "expected at least 1 property set"
    );
    assert!(
        !prof.runtime_stats.is_empty(),
        "expected runtime_stats to be populated, got empty"
    );

    Ok(())
}

/// Regression for GitHub issue #72 (item 3): `MutationSetExec` and other
/// custom DataFusion operators must report non-zero `actual_rows` and
/// `time_ms` in `ProfileOutput`. Before the fix, `MutationSetExec` reported
/// `rows=0 time=0 ms` (no metrics wiring); `GraphScanExec` and `UnwindExec`
/// reported rows but `time=0` (Timer never started).
#[tokio::test]
async fn test_profile_metrics_populated_for_mutation_scan_unwind() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db = UniBuilder::new(temp_dir.path().to_str().unwrap().to_string())
        .build()
        .await?;

    // Schema and seed data
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (entity_id STRING NOT NULL, frequency INT)")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    for i in 0..10 {
        tx.execute_with("CREATE (:Entity {entity_id: $id, frequency: 1})")
            .param("id", format!("e:{i}"))
            .run()
            .await?;
    }
    tx.commit().await?;

    // Collect node ids
    let res = db
        .session()
        .query("MATCH (n:Entity) RETURN id(n) AS nid ORDER BY n.entity_id")
        .await?;
    let vids: Vec<i64> = res
        .into_iter()
        .map(|row| row.get::<i64>("nid").unwrap())
        .collect();
    assert_eq!(vids.len(), 10);

    // Build the issue's UNWIND ... MATCH WHERE id(n)=u.nid SET ... shape.
    let updates: Vec<uni_db::Value> = vids
        .iter()
        .enumerate()
        .map(|(i, &vid)| {
            let mut m = std::collections::HashMap::new();
            m.insert("nid".to_string(), uni_db::Value::Int(vid));
            m.insert("new_frequency".to_string(), uni_db::Value::Int((i as i64) + 2));
            uni_db::Value::Map(m)
        })
        .collect();

    let tx = db.session().tx().await?;
    let (_res, prof) = tx
        .execute_with(
            "UNWIND $updates AS u \
             MATCH (n:Entity) WHERE id(n) = u.nid \
             SET n.frequency = u.new_frequency",
        )
        .param("updates", uni_db::Value::List(updates))
        .profile()
        .await?;
    tx.commit().await?;

    let stats = &prof.runtime_stats;
    let op_names: Vec<String> = stats.iter().map(|s| s.operator.clone()).collect();
    println!("Operators: {op_names:?}");
    for s in stats {
        println!(
            "  {:<24} rows={:>4} time={:>8.3} ms",
            s.operator, s.actual_rows, s.time_ms
        );
    }

    // MutationSetExec must now report rows AND time.
    let mutation = stats
        .iter()
        .find(|s| s.operator.contains("MutationSetExec"))
        .unwrap_or_else(|| panic!("MutationSetExec not found in: {op_names:?}"));
    assert!(
        mutation.actual_rows > 0,
        "MutationSetExec.actual_rows should be > 0, got {}",
        mutation.actual_rows
    );
    assert!(
        mutation.time_ms > 0.0,
        "MutationSetExec.time_ms should be > 0 (was {}); metrics wiring regressed",
        mutation.time_ms
    );

    // GraphScanExec previously had time=0 (rows-only wiring).
    let scan = stats
        .iter()
        .find(|s| s.operator.contains("GraphScanExec"))
        .unwrap_or_else(|| panic!("GraphScanExec not found in: {op_names:?}"));
    assert!(
        scan.time_ms > 0.0,
        "GraphScanExec.time_ms should be > 0 (was {}); Timer wiring regressed",
        scan.time_ms
    );

    // GraphUnwindExec previously had time=0.
    let unwind = stats
        .iter()
        .find(|s| s.operator.contains("UnwindExec"))
        .unwrap_or_else(|| panic!("UnwindExec not found in: {op_names:?}"));
    assert!(
        unwind.time_ms > 0.0,
        "UnwindExec.time_ms should be > 0 (was {}); Timer wiring regressed",
        unwind.time_ms
    );

    Ok(())
}
