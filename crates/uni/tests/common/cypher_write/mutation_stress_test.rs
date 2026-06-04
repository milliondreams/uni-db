// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Stress tests for mutation operations at scale.
//!
//! All tests are `#[ignore]`d because they are slow (10k+ operations).
//! Run with: `cargo nextest run --run-ignored all -E 'test(stress)'`

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
#[ignore]
async fn test_stress_create_10k_nodes() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    for i in 0..10_000 {
        let tx = db.session().tx().await?;
        tx.execute(&format!("CREATE (n:StressNode {{idx: {i}}})"))
            .await?;
        tx.commit().await?;
    }

    let result = db
        .session()
        .query("MATCH (n:StressNode) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(result.rows().len(), 1);
    let count = result.rows()[0].get::<i64>("cnt")?;
    assert_eq!(count, 10_000);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_stress_set_10k_nodes() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Seed 10k nodes
    for i in 0..10_000 {
        let tx = db.session().tx().await?;
        tx.execute(&format!("CREATE (n:StressNode {{idx: {i}}})"))
            .await?;
        tx.commit().await?;
    }

    // Bulk SET via single MATCH
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:StressNode) SET n.updated = true")
        .await?;
    tx.commit().await?;

    // Verify all nodes were updated
    let result = db
        .session()
        .query("MATCH (n:StressNode) WHERE n.updated = true RETURN count(n) AS cnt")
        .await?;
    assert_eq!(result.rows().len(), 1);
    let count = result.rows()[0].get::<i64>("cnt")?;
    assert_eq!(count, 10_000);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_stress_delete_10k_nodes() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Seed 10k nodes
    for i in 0..10_000 {
        let tx = db.session().tx().await?;
        tx.execute(&format!("CREATE (n:StressNode {{idx: {i}}})"))
            .await?;
        tx.commit().await?;
    }

    // Verify seed
    let result = db
        .session()
        .query("MATCH (n:StressNode) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("cnt")?, 10_000);

    // Bulk DETACH DELETE
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:StressNode) DETACH DELETE n").await?;
    tx.commit().await?;

    // Verify empty
    let result = db
        .session()
        .query("MATCH (n:StressNode) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("cnt")?, 0);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_stress_mixed_mutations_10k() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // CREATE 5k nodes
    for i in 0..5_000 {
        let tx = db.session().tx().await?;
        tx.execute(&format!("CREATE (n:StressNode {{idx: {i}}})"))
            .await?;
        tx.commit().await?;
    }
    let result = db
        .session()
        .query("MATCH (n:StressNode) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("cnt")?, 5_000);

    // SET all
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:StressNode) SET n.phase = 'updated'")
        .await?;
    tx.commit().await?;

    // DELETE half (idx < 2500)
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:StressNode) WHERE n.idx < 2500 DETACH DELETE n")
        .await?;
    tx.commit().await?;
    let result = db
        .session()
        .query("MATCH (n:StressNode) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("cnt")?, 2_500);

    // CREATE 5k more (idx 5000..9999)
    for i in 5_000..10_000 {
        let tx = db.session().tx().await?;
        tx.execute(&format!("CREATE (n:StressNode {{idx: {i}}})"))
            .await?;
        tx.commit().await?;
    }

    // Verify final count: 2500 (surviving) + 5000 (new) = 7500
    let result = db
        .session()
        .query("MATCH (n:StressNode) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("cnt")?, 7_500);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_stress_merge_10k_ops() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // 5k MERGE creates (all new)
    for i in 0..5_000 {
        let tx = db.session().tx().await?;
        tx.execute(&format!("MERGE (n:StressNode {{idx: {i}}})"))
            .await?;
        tx.commit().await?;
    }
    let result = db
        .session()
        .query("MATCH (n:StressNode) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("cnt")?, 5_000);

    // 10k MERGE: first 5k match existing, next 5k create new
    for i in 0..10_000 {
        let tx = db.session().tx().await?;
        tx.execute(&format!("MERGE (n:StressNode {{idx: {i}}})"))
            .await?;
        tx.commit().await?;
    }

    // Verify 10k total (5k original matched + 5k new created)
    let result = db
        .session()
        .query("MATCH (n:StressNode) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("cnt")?, 10_000);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_stress_create_edges_5k() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create a chain of 5001 nodes with NEXT edges (5000 edges)
    let tx = db.session().tx().await?;
    tx.execute("CREATE (n:ChainNode {idx: 0})").await?;
    tx.commit().await?;
    for i in 1..=5_000 {
        let tx = db.session().tx().await?;
        tx.execute(&format!(
            "MATCH (a:ChainNode {{idx: {prev}}}) CREATE (b:ChainNode {{idx: {i}}}), (a)-[:NEXT]->(b)",
            prev = i - 1,
        ))
        .await?;
        tx.commit().await?;
    }

    // Verify node count
    let result = db
        .session()
        .query("MATCH (n:ChainNode) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("cnt")?, 5_001);

    // Verify edge count
    let result = db
        .session()
        .query("MATCH ()-[r:NEXT]->() RETURN count(r) AS cnt")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("cnt")?, 5_000);

    Ok(())
}
