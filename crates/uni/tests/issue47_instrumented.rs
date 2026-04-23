// SPDX-License-Identifier: Apache-2.0
// Instrumented version of issue #47 repro — times each internal step
// over 500 nodes + 1000 edges.
//
// Run with:
//   cargo nextest run -p uni-db --test issue47_instrumented --run-ignored all --no-capture

use std::time::Instant;
use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

const NUM_NODES: usize = 500;

#[tokio::test]
#[ignore]
async fn instrumented_node_edge_ingestion() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_str().unwrap();

    let db = Uni::open(path).build().await.unwrap();

    db.schema()
        .label("Msg")
        .property("mid", DataType::String)
        .index("mid", IndexType::Scalar(ScalarType::Hash))
        .done()
        .label("User")
        .property("uid", DataType::String)
        .done()
        .label("Session")
        .property("sid", DataType::String)
        .done()
        .edge_type("SENT_BY", &["Msg"], &["User"])
        .done()
        .edge_type("IN_SESSION", &["Msg"], &["Session"])
        .done()
        .apply()
        .await
        .unwrap();

    let session = db.session();

    // Create target nodes
    let tx = session.tx().await.unwrap();
    tx.execute("CREATE (:User {uid: 'alice'})").await.unwrap();
    tx.execute("CREATE (:Session {sid: 'sess-1'})")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let user_vid: i64 = session
        .query("MATCH (u:User {uid: 'alice'}) RETURN id(u) AS v")
        .await
        .unwrap()
        .rows()[0]
        .get("v")
        .unwrap();

    let sess_vid: i64 = session
        .query("MATCH (s:Session {sid: 'sess-1'}) RETURN id(s) AS v")
        .await
        .unwrap()
        .rows()[0]
        .get("v")
        .unwrap();

    // Accumulate per-step timings
    struct StepTiming {
        create_node_ms: u128,
        commit_node_ms: u128,
        edge1_exec_ms: u128,
        edge1_commit_ms: u128,
        edge2_exec_ms: u128,
        edge2_commit_ms: u128,
        total_ms: u128,
    }

    let mut timings: Vec<StepTiming> = Vec::with_capacity(NUM_NODES);
    let total_start = Instant::now();

    for i in 0..NUM_NODES {
        let iter_start = Instant::now();

        // Step 1: Create node
        let t = Instant::now();
        let tx = session.tx().await.unwrap();
        let result = tx
            .query_with("CREATE (m:Msg {mid: $mid}) RETURN id(m) AS v")
            .param("mid", Value::String(format!("m-{i:04}")))
            .fetch_all()
            .await
            .unwrap();
        let msg_vid: i64 = result.rows()[0].get("v").unwrap();
        let create_node_ms = t.elapsed().as_millis();

        // Step 2: Commit node
        let t = Instant::now();
        tx.commit().await.unwrap();
        let commit_node_ms = t.elapsed().as_millis();

        // Step 3: Create SENT_BY edge (execute)
        let t = Instant::now();
        let tx = session.tx().await.unwrap();
        tx.query_with(
            "MATCH (m), (u) WHERE id(m) = $mid AND id(u) = $uid \
             CREATE (m)-[:SENT_BY]->(u)",
        )
        .param("mid", Value::Int(msg_vid))
        .param("uid", Value::Int(user_vid))
        .fetch_all()
        .await
        .unwrap();
        let edge1_exec_ms = t.elapsed().as_millis();

        // Step 4: Commit SENT_BY edge
        let t = Instant::now();
        tx.commit().await.unwrap();
        let edge1_commit_ms = t.elapsed().as_millis();

        // Step 5: Create IN_SESSION edge (execute)
        let t = Instant::now();
        let tx = session.tx().await.unwrap();
        tx.query_with(
            "MATCH (m), (s) WHERE id(m) = $mid AND id(s) = $sid \
             CREATE (m)-[:IN_SESSION]->(s)",
        )
        .param("mid", Value::Int(msg_vid))
        .param("sid", Value::Int(sess_vid))
        .fetch_all()
        .await
        .unwrap();
        let edge2_exec_ms = t.elapsed().as_millis();

        // Step 6: Commit IN_SESSION edge
        let t = Instant::now();
        tx.commit().await.unwrap();
        let edge2_commit_ms = t.elapsed().as_millis();

        let total_ms = iter_start.elapsed().as_millis();

        timings.push(StepTiming {
            create_node_ms,
            commit_node_ms,
            edge1_exec_ms,
            edge1_commit_ms,
            edge2_exec_ms,
            edge2_commit_ms,
            total_ms,
        });

        if i % 50 == 0 || i == NUM_NODES - 1 {
            eprintln!(
                "i={:>4} | node={:>4}ms commit={:>4}ms | e1={:>4}ms c1={:>4}ms | e2={:>4}ms c2={:>4}ms | total={:>4}ms",
                i, create_node_ms, commit_node_ms,
                edge1_exec_ms, edge1_commit_ms,
                edge2_exec_ms, edge2_commit_ms,
                total_ms
            );
        }
    }

    let total_secs = total_start.elapsed().as_secs_f64();

    // Bucket analysis (50-item buckets)
    eprintln!("\n--- Bucket averages (50-item) ---");
    eprintln!(
        "{:>10} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "range", "node", "c_node", "edge1", "c_edge1", "edge2", "c_edge2", "total"
    );
    for chunk_start in (0..NUM_NODES).step_by(50) {
        let chunk_end = (chunk_start + 50).min(NUM_NODES);
        let skip = if chunk_start == 0 { 1 } else { 0 }; // skip cold start
        let slice = &timings[chunk_start + skip..chunk_end];
        if slice.is_empty() {
            continue;
        }
        let n = slice.len() as f64;
        let avg = |f: fn(&StepTiming) -> u128| -> f64 {
            slice.iter().map(f).sum::<u128>() as f64 / n
        };
        eprintln!(
            "{:>4}-{:>4} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8.1}",
            chunk_start,
            chunk_end - 1,
            avg(|t| t.create_node_ms),
            avg(|t| t.commit_node_ms),
            avg(|t| t.edge1_exec_ms),
            avg(|t| t.edge1_commit_ms),
            avg(|t| t.edge2_exec_ms),
            avg(|t| t.edge2_commit_ms),
            avg(|t| t.total_ms),
        );
    }

    // Overall stats
    let first_50 = &timings[1..50];
    let last_50 = &timings[NUM_NODES - 50..];
    let avg_total = |s: &[StepTiming]| -> f64 {
        s.iter().map(|t| t.total_ms).sum::<u128>() as f64 / s.len() as f64
    };
    let ratio = avg_total(last_50) / avg_total(first_50).max(1.0);

    eprintln!("\n--- Summary ---");
    eprintln!("Total: {total_secs:.1}s for {NUM_NODES} nodes + {} edges", NUM_NODES * 2);
    eprintln!("First 50 avg total: {:.1}ms", avg_total(first_50));
    eprintln!("Last 50 avg total:  {:.1}ms", avg_total(last_50));
    eprintln!("Growth ratio: {ratio:.1}x");
}
