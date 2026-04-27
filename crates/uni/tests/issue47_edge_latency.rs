// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Diagnostic test for issue #47: create_edge latency grows linearly.
// Measures node creation vs edge creation independently.

use anyhow::Result;
use std::time::Instant;
use uni_db::{DataType, Uni};

#[tokio::test]
#[ignore]
async fn issue47_edge_latency_growth() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();

    let db = Uni::open(path).build().await?;

    db.schema()
        .label("Message")
        .property("content", DataType::String)
        .done()
        .label("Participant")
        .property("name", DataType::String)
        .done()
        .label("Session")
        .property("name", DataType::String)
        .done()
        .edge_type("SENT_BY", &["Message"], &["Participant"])
        .done()
        .edge_type("IN_SESSION", &["Message"], &["Session"])
        .done()
        .edge_type("NEXT", &["Message"], &["Message"])
        .done()
        .apply()
        .await?;

    let session = db.session();

    // Create target nodes
    let tx = session.tx().await?;
    tx.execute("CREATE (:Participant {name: 'alice'})").await?;
    tx.execute("CREATE (:Session {name: 'sess-1'})").await?;
    tx.commit().await?;

    eprintln!("\n=== Issue #47: Edge latency growth diagnostic ===\n");
    eprintln!(
        "{:>6} {:>10} {:>10} {:>10} {:>10}",
        "msg#", "node_ms", "sent_ms", "sess_ms", "next_ms"
    );
    eprintln!("{}", "-".repeat(52));

    let mut prev_msg_vid: Option<i64> = None;

    for i in 0..300 {
        // 1. Create node
        let node_start = Instant::now();
        let tx = session.tx().await?;
        tx.execute_with("CREATE (m:Message {content: $c}) RETURN m._vid AS vid")
            .param("c", format!("Message {i}"))
            .run()
            .await?;
        tx.commit().await?;
        let node_ms = node_start.elapsed().as_millis();

        // Get the VID of the message we just created (query L0)
        let result = session
            .query_with("MATCH (m:Message {content: $c}) RETURN m._vid AS vid")
            .param("c", format!("Message {i}"))
            .fetch_all()
            .await?;
        let msg_vid: i64 = result.rows()[0].get("vid")?;

        // 2. Create SENT_BY edge
        let sent_start = Instant::now();
        let tx = session.tx().await?;
        tx.execute_with(
            "MATCH (m:Message) WHERE m._vid = $mv \
             MATCH (p:Participant {name: 'alice'}) \
             CREATE (m)-[:SENT_BY]->(p)",
        )
        .param("mv", msg_vid)
        .run()
        .await?;
        tx.commit().await?;
        let sent_ms = sent_start.elapsed().as_millis();

        // 3. Create IN_SESSION edge
        let sess_start = Instant::now();
        let tx = session.tx().await?;
        tx.execute_with(
            "MATCH (m:Message) WHERE m._vid = $mv \
             MATCH (s:Session {name: 'sess-1'}) \
             CREATE (m)-[:IN_SESSION]->(s)",
        )
        .param("mv", msg_vid)
        .run()
        .await?;
        tx.commit().await?;
        let sess_ms = sess_start.elapsed().as_millis();

        // 4. Create NEXT edge (to previous message)
        let mut next_ms = 0u128;
        if let Some(prev_vid) = prev_msg_vid {
            let next_start = Instant::now();
            let tx = session.tx().await?;
            tx.execute_with(
                "MATCH (prev:Message) WHERE prev._vid = $pv \
                 MATCH (cur:Message) WHERE cur._vid = $cv \
                 CREATE (prev)-[:NEXT]->(cur)",
            )
            .param("pv", prev_vid)
            .param("cv", msg_vid)
            .run()
            .await?;
            tx.commit().await?;
            next_ms = next_start.elapsed().as_millis();
        }
        prev_msg_vid = Some(msg_vid);

        if i % 20 == 0 || i == 299 {
            eprintln!(
                "{:>6} {:>10} {:>10} {:>10} {:>10}",
                i, node_ms, sent_ms, sess_ms, next_ms
            );
        }
    }

    // Summary buckets
    eprintln!("\n=== Done ===");

    Ok(())
}
