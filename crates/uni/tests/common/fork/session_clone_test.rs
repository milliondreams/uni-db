// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for Session Clone (REQ-2) and DateTime isolation (Q-1).

use anyhow::Result;
use uni_db::Uni;

/// Cloned sessions share the plan cache but operate independently.
#[tokio::test]
async fn test_session_clone_shares_plan_cache() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Seed data
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'})")
        .await?;
    tx.commit().await?;

    let session1 = db.session();

    // Prime the cache via session1
    let result1 = session1
        .query("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
        .await?;
    assert_eq!(result1.len(), 2);

    // Clone session1 — should share the plan cache
    let session2 = session1.clone();

    // Query via session2 — should hit the shared cache
    let result2 = session2
        .query("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
        .await?;
    assert_eq!(result2.len(), 2);

    // Verify cache metrics show a hit (session2 reused session1's cached plan)
    let metrics = session2.metrics();
    // The shared cache should have at least 1 hit from session2's query
    assert!(
        metrics.plan_cache_hits >= 1,
        "Cloned session should share plan cache, hits={}",
        metrics.plan_cache_hits
    );

    Ok(())
}

/// Cloned sessions get independent write guards.
#[tokio::test]
async fn test_session_clone_independent_write_guard() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session1 = db.session();
    let session2 = session1.clone();

    // Both sessions should be able to create independent transactions
    let tx1 = session1.tx().await?;
    let tx2 = session2.tx().await?;

    tx1.execute("CREATE (:Node {val: 1})").await?;
    tx2.execute("CREATE (:Node {val: 2})").await?;

    tx1.commit().await?;
    tx2.commit().await?;

    let result = db
        .session()
        .query("MATCH (n:Node) RETURN n.val AS val ORDER BY val")
        .await?;
    assert_eq!(result.len(), 2);

    Ok(())
}

/// Q-1 confirmation: DateTime and scalar SET in the same query work correctly.
#[tokio::test]
async fn test_datetime_and_scalar_set_in_same_query() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create node with both scalar and datetime properties
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Event {name: 'initial', count: 0, ts: datetime('2025-01-01T00:00:00Z')})")
        .await?;
    tx.commit().await?;

    // Update BOTH scalar and DateTime in a single SET clause
    let tx = db.session().tx().await?;
    tx.execute(
        "MATCH (e:Event) SET e.name = 'updated', e.count = 42, e.ts = datetime('2026-06-15T12:00:00Z')",
    )
    .await?;
    tx.commit().await?;

    // Verify both were updated
    let result = db
        .session()
        .query("MATCH (e:Event) RETURN e.name AS name, e.count AS count, e.ts AS ts")
        .await?;
    assert_eq!(result.len(), 1);

    let name: String = result.rows()[0].get("name")?;
    assert_eq!(name, "updated", "Scalar string should be updated");

    let count: i64 = result.rows()[0].get("count")?;
    assert_eq!(count, 42, "Scalar int should be updated");

    // DateTime should also be updated (not the original value)
    let ts: String = result.rows()[0].get("ts")?;
    assert!(
        ts.contains("2026"),
        "DateTime should be updated to 2026, got: {ts}"
    );

    Ok(())
}
