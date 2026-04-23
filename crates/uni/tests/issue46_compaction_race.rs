// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Reproduction test for issue #46: Edge compaction panic from flush/compaction race.
//
// Creates 300 Message nodes with 2 edges each on a persistent KB,
// triggering multiple flush + compaction cycles that previously raced.
//
// Run with:
//   cargo nextest run -p uni-db --test issue46_compaction_race --run-ignored all --no-capture

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig};

const NUM_INSERTS: usize = 300;

#[tokio::test]
#[ignore]
async fn issue46_edge_compaction_no_panic() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();

    // Use a persistent KB with aggressive flush to maximize race window
    let config = UniConfig {
        auto_flush_interval: Some(std::time::Duration::from_secs(2)),
        auto_flush_threshold: 100,
        ..Default::default()
    };

    let db = Uni::open(path).config(config).build().await?;

    db.schema()
        .label("Message")
        .property("content", DataType::String)
        .done()
        .label("Session")
        .property("name", DataType::String)
        .done()
        .label("Participant")
        .property("name", DataType::String)
        .done()
        .edge_type("SENT_BY", &["Message"], &["Participant"])
        .done()
        .edge_type("IN_SESSION", &["Message"], &["Session"])
        .done()
        .apply()
        .await?;

    let session = db.session();

    // Create target nodes
    let tx = session.tx().await?;
    tx.execute("CREATE (:Session {name: 'test-session'})")
        .await?;
    tx.execute("CREATE (:Participant {name: 'alice'})").await?;
    tx.commit().await?;

    // Insert 300 messages, each with 2 edges — triggers flush + compaction cycles
    for i in 0..NUM_INSERTS {
        let tx = session.tx().await?;
        tx.execute_with(
            "CREATE (m:Message {content: $c}) \
             WITH m MATCH (p:Participant {name: 'alice'}) CREATE (m)-[:SENT_BY]->(p) \
             WITH m MATCH (s:Session {name: 'test-session'}) CREATE (m)-[:IN_SESSION]->(s)",
        )
        .param("c", format!("Message number {i}"))
        .run()
        .await?;
        tx.commit().await?;

        if i % 50 == 0 {
            eprintln!("insert {i}/{NUM_INSERTS}");
        }
    }

    // Verify data integrity
    let result = session
        .query("MATCH (m:Message) RETURN count(m) AS cnt")
        .await?;
    let count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(count, NUM_INSERTS as i64, "Expected {NUM_INSERTS} messages");

    let result = session
        .query("MATCH ()-[r:SENT_BY]->() RETURN count(r) AS cnt")
        .await?;
    let sent_count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(
        sent_count, NUM_INSERTS as i64,
        "Expected {NUM_INSERTS} SENT_BY edges"
    );

    let result = session
        .query("MATCH ()-[r:IN_SESSION]->() RETURN count(r) AS cnt")
        .await?;
    let session_count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(
        session_count, NUM_INSERTS as i64,
        "Expected {NUM_INSERTS} IN_SESSION edges"
    );

    eprintln!("PASS: {NUM_INSERTS} messages + {sent_count} SENT_BY + {session_count} IN_SESSION");

    Ok(())
}
