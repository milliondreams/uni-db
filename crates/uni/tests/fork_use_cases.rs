// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 7 — end-to-end fork use-case aggregator.
//!
//! Each test below walks one named user scenario top to bottom in a
//! single file, so a reader can see "what does this feature *do*?"
//! without piecing together five separate test files. These tests
//! intentionally duplicate coverage that exists elsewhere — they're
//! the *contract* surface, not the *implementation* surface, and
//! they're where someone learning the system should land first.
//!
//! Scenarios (numbered to match the original fork spec's §3.x use
//! cases the plan calls out):
//!
//!   - 3.1 Rule-developer iterative loop
//!         (fork → write → query → diff → drop)
//!   - 3.3 Write-audit-publish
//!         (fork → write → diff → promote → drop)
//!   - 3.4 Side-by-side scenario diff
//!         (two forks → independent writes → diff → drop)
//!   - 3.5 Nested forks
//!         (parent fork → child fork → cascade drop)
//!   - 3.6 Fork-local schema overlay
//!         (fork → CREATE new label → primary schema unchanged)

use anyhow::Result;
use uni_db::api::fork_diff::PromotePattern;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn use_case_3_1_rule_developer_loop() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'Anchor'})").await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let fork = primary.fork("rule_dev").await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Person {name: 'Hypothesis-A'})").await?;
        tx.commit().await?;

        // The rule developer queries their hypothesis without touching primary.
        let result = fork
            .query("MATCH (p:Person) RETURN p.name AS name")
            .await?;
        let names: Vec<String> = result
            .rows()
            .iter()
            .filter_map(|r| r.get::<String>("name").ok())
            .collect();
        assert_eq!(names.len(), 2, "fork sees Anchor + Hypothesis-A");

        // Audit the delta before deciding whether to promote.
        let diff = db.diff_fork_primary("rule_dev").await?;
        assert_eq!(diff.vertices.added.len(), 1);
        assert!(diff.vertices.deleted.is_empty());
    }
    // Decision: don't promote, just drop the fork.
    db.drop_fork("rule_dev").await?;
    let primary_rows = primary
        .query("MATCH (p:Person) RETURN p.name")
        .await?
        .rows()
        .len();
    assert_eq!(primary_rows, 1, "primary is untouched");
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn use_case_3_3_write_audit_publish() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let staging = primary.fork("publish_q2").await?;
        let tx = staging.tx().await?;
        tx.execute("CREATE (:Person {name: 'Bob'})").await?;
        tx.execute("CREATE (:Person {name: 'Carol'})").await?;
        tx.commit().await?;
    }

    // Audit before publish.
    let diff = db.diff_fork_primary("publish_q2").await?;
    assert_eq!(diff.vertices.added.len(), 2, "two staged adds");

    // Publish.
    let report = db
        .promote_from_fork("publish_q2", &[PromotePattern::label("Person")])
        .await?;
    assert!(report.vertices_inserted >= 2);

    db.drop_fork("publish_q2").await?;
    let final_rows = primary
        .query("MATCH (p:Person) RETURN p.name")
        .await?
        .rows()
        .len();
    assert_eq!(final_rows, 3, "primary now has Alice + Bob + Carol");
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn use_case_3_4_side_by_side_scenarios() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .property("price", DataType::Int64)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Item {name: 'baseline', price: 100})").await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let scenario_a = primary.fork("low_price").await?;
        let tx = scenario_a.tx().await?;
        tx.execute("CREATE (:Item {name: 'A', price: 50})").await?;
        tx.commit().await?;
    }
    {
        let scenario_b = primary.fork("high_price").await?;
        let tx = scenario_b.tx().await?;
        tx.execute("CREATE (:Item {name: 'B', price: 500})").await?;
        tx.commit().await?;
    }

    // Compare the two scenarios.
    let diff = db.diff_forks("low_price", "high_price").await?;
    // Each side has exactly one fork-only row.
    assert_eq!(diff.vertices.added.len(), 1, "high_price-only row");
    assert_eq!(diff.vertices.deleted.len(), 1, "low_price-only row");

    // Inversion is structural.
    let reverse = db.diff_forks("high_price", "low_price").await?;
    assert_eq!(reverse.vertices.added.len(), diff.vertices.deleted.len());
    assert_eq!(reverse.vertices.deleted.len(), diff.vertices.added.len());

    db.drop_fork("low_price").await?;
    db.drop_fork("high_price").await?;
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn use_case_3_5_nested_forks() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'P-Root'})").await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let parent_fork = primary.fork("parent").await?;
        let tx = parent_fork.tx().await?;
        tx.execute("CREATE (:Person {name: 'P-Child-Anchor'})").await?;
        tx.commit().await?;

        let child_fork = parent_fork.fork("child").await?;
        let tx = child_fork.tx().await?;
        tx.execute("CREATE (:Person {name: 'P-Grandchild'})").await?;
        tx.commit().await?;

        // Child sees its own writes + parent's + primary's.
        let names: Vec<String> = child_fork
            .query("MATCH (p:Person) RETURN p.name AS name")
            .await?
            .rows()
            .iter()
            .filter_map(|r| r.get::<String>("name").ok())
            .collect();
        assert_eq!(names.len(), 3, "child sees three layers: {:?}", names);
    }

    db.drop_fork_cascade("parent").await?;
    let primary_rows = primary
        .query("MATCH (p:Person) RETURN p.name")
        .await?
        .rows()
        .len();
    assert_eq!(primary_rows, 1, "cascade drop leaves primary untouched");
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn use_case_3_6_fork_local_schema_overlay() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();

    {
        let fork = primary.fork("overlay").await?;
        // The fork registers a label that doesn't exist on primary;
        // property addition on existing labels is out of scope for
        // fork-local overlay (Phase 3 documented limit).
        fork.fork_schema().label("Hypothesis").apply().await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Hypothesis {statement: 'maybe'})")
            .await?;
        tx.commit().await?;

        // Fork sees both labels (via Cypher).
        let counts = fork
            .query("MATCH (n) WHERE labels(n)[0] IN ['Person', 'Hypothesis'] RETURN count(n) AS c")
            .await?;
        let c: i64 = counts.rows()[0].get("c")?;
        assert!(c >= 1, "fork sees its fork-local hypothesis row");
    }

    // Primary's schema is unchanged — no Hypothesis label.
    let primary_labels = db.list_labels().await?;
    assert!(
        !primary_labels.iter().any(|l| l == "Hypothesis"),
        "primary schema must NOT contain fork-only label: {:?}",
        primary_labels
    );

    db.drop_fork("overlay").await?;
    db.shutdown().await?;
    Ok(())
}
