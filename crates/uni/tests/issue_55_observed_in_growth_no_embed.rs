//! Bisect probe for issue #55 PR #4 (Bucket J).
//!
//! Identical workload shape to `issue_55_observed_in_growth.rs` (same
//! schema, same per-turn write counts, same MATCH+CREATE pattern, same
//! batch sizes) BUT with auto-embed disabled. If the linear write-path
//! growth persists here, auto-embed is innocent; the cost lives in the
//! storage layer's MATCH+CREATE-edge path. If growth disappears, the
//! pipeline that fires under auto-embed is the cause.
//!
//! Run:
//!   cargo nextest run -p uni-db --test issue_55_observed_in_growth_no_embed \
//!       --release --no-capture

use std::collections::HashMap;
use std::time::Instant;

use uni_db::{DataType, Uni, Value};

const TURNS: usize = 400;
const OBS_PER_TURN: usize = 3;

const MESSAGE_TEXT: &str =
    "The quick brown fox jumps over the lazy dog at the local park yesterday afternoon.";

async fn setup_db() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Message")
        .property("message_id", DataType::String)
        .property("content", DataType::String)
        // No embedding property, no vector index.
        .done()
        .label("Observation")
        .property("observation_id", DataType::String)
        .property("content", DataType::String)
        // No embedding property, no vector index.
        .done()
        .edge_type("OBSERVED_IN", &["Observation"], &["Message"])
        .done()
        .apply()
        .await
        .unwrap();
    db
}

async fn create_message(db: &Uni, turn: usize) -> i64 {
    let session = db.session();
    let tx = session.tx().await.unwrap();
    let result = tx
        .query_with("CREATE (n:Message {message_id: $id, content: $c}) RETURN id(n) AS vid")
        .param("id", Value::String(format!("msg-{turn:04}")))
        .param("c", Value::String(format!("{MESSAGE_TEXT} (turn {turn})")))
        .fetch_all()
        .await
        .unwrap();
    tx.commit().await.unwrap();
    result.rows().first().unwrap().get::<i64>("vid").unwrap()
}

async fn batch_create_observations(db: &Uni, turn: usize, n: usize) -> Vec<i64> {
    let session = db.session();
    let tx = session.tx().await.unwrap();
    let items: Vec<Value> = (0..n)
        .map(|i| {
            let mut m = HashMap::new();
            m.insert(
                "observation_id".to_string(),
                Value::String(format!("obs-{turn:04}-{i}")),
            );
            m.insert(
                "content".to_string(),
                Value::String(format!("Observation {i} from turn {turn}: {MESSAGE_TEXT}")),
            );
            Value::Map(m)
        })
        .collect();

    let result = tx
        .query_with(
            "UNWIND $items AS item \
             CREATE (n:Observation {observation_id: item.observation_id, content: item.content}) \
             RETURN id(n) AS vid",
        )
        .param("items", Value::List(items))
        .fetch_all()
        .await
        .unwrap();
    tx.commit().await.unwrap();
    result
        .rows()
        .iter()
        .map(|r| r.get::<i64>("vid").unwrap())
        .collect()
}

async fn batch_create_observed_in(db: &Uni, obs_ids: &[i64], msg_id: i64) -> u128 {
    let session = db.session();
    let tx = session.tx().await.unwrap();

    let edges: Vec<Value> = obs_ids
        .iter()
        .map(|&oid| {
            let mut m = HashMap::new();
            m.insert("src".to_string(), Value::Int(oid));
            m.insert("dst".to_string(), Value::Int(msg_id));
            Value::Map(m)
        })
        .collect();

    let cypher = "UNWIND $edges AS e \
                  MATCH (a:Observation) WHERE id(a) = e.src \
                  MATCH (b:Message) WHERE id(b) = e.dst \
                  CREATE (a)-[r:OBSERVED_IN]->(b)";

    let t0 = Instant::now();
    tx.execute_with(cypher)
        .param("edges", Value::List(edges))
        .run()
        .await
        .unwrap();
    let query_us = t0.elapsed().as_micros();

    tx.commit().await.unwrap();
    query_us
}

#[tokio::test]
async fn probe_observed_in_growth_no_embed() {
    let db = setup_db().await;
    let mut samples: Vec<u128> = Vec::with_capacity(TURNS);

    for turn in 0..TURNS {
        let msg_id = create_message(&db, turn).await;
        let obs_ids = batch_create_observations(&db, turn, OBS_PER_TURN).await;
        let q_us = batch_create_observed_in(&db, &obs_ids, msg_id).await;
        samples.push(q_us);

        if turn % 50 == 0 || turn == TURNS - 1 {
            let window_start = samples.len().saturating_sub(20);
            let window: f64 = samples[window_start..]
                .iter()
                .map(|v| *v as f64)
                .sum::<f64>()
                / (samples.len() - window_start) as f64
                / 1000.0;
            eprintln!(
                "turn {turn:>4}: latest_query_ms={:.1}  window_mean(last20)_ms={:.1}",
                samples.last().unwrap() / 1000,
                window
            );
        }
    }

    let first_50_mean: f64 = samples[..50].iter().map(|v| *v as f64).sum::<f64>() / 50.0 / 1000.0;
    let last_50_mean: f64 = samples[samples.len() - 50..]
        .iter()
        .map(|v| *v as f64)
        .sum::<f64>()
        / 50.0
        / 1000.0;
    let growth = last_50_mean / first_50_mean;

    eprintln!("\n--- Summary (no-auto-embed bisect probe) ---");
    eprintln!("turns: {TURNS}, observations per turn: {OBS_PER_TURN}");
    eprintln!("first 50 calls mean query time: {first_50_mean:.1}ms");
    eprintln!("last  50 calls mean query time: {last_50_mean:.1}ms");
    eprintln!("growth: {growth:.1}x");

    db.shutdown().await.unwrap();
}
