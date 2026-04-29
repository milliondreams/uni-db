//! Probe for issue #55 root cause beneath the storage layer.
//!
//! After PR #1 (commit 0aabd2b4) added segment short-circuits to
//! `AdjacencyManager::get_neighbors`, the unit-tested storage hot path
//! is verified O(out-degree). But the customer-facing Cypher latency
//! still steps from ~1.7 ms to ~5 ms after the first L0 → L1 flush.
//!
//! That ~3 ms premium isn't in the storage call (already proved).
//! This probe compares THREE query shapes per round to narrow it down:
//!
//!   A. point lookup, no traversal:  `MATCH (a) WHERE id(a)=$nid RETURN id(a)`
//!   B. count-only traversal:        `MATCH (a)-[r:LINK]->() ... RETURN count(r)`
//!   C. full traversal w/ binding:   `MATCH (a)-[r:LINK]->(b) ... RETURN id(r)`
//!
//! If A also slows post-flush → cost is in per-query setup
//!   (QueryContext, plan cache, snapshot capture).
//! If A is flat but B+C slow → cost is in traversal / get_neighbors path.
//! If A+B flat but C slows → cost is in destination binding (visibility on b).

use std::time::Instant;

use uni_db::{Uni, Value};

const PARTICIPANT_EDGES: usize = 20;
const FILLER_PER_ROUND: usize = 200;
const ROUNDS: usize = 15;
const SAMPLES_PER_ROUND: usize = 5;

async fn setup_db() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Participant")
        .property("name", uni_db::DataType::String)
        .done()
        .label("Session")
        .property("session_id", uni_db::DataType::String)
        .done()
        .label("Message")
        .property("content", uni_db::DataType::String)
        .done()
        .edge_type("LINK", &["Participant"], &["Session"])
        .done()
        .edge_type("IN_SESSION", &["Message"], &["Session"])
        .done()
        .apply()
        .await
        .unwrap();
    db
}

async fn create_node(db: &Uni, label: &str, props: &[(&str, Value)]) -> i64 {
    let session = db.session();
    let tx = session.tx().await.unwrap();
    let prop_str: Vec<String> = props
        .iter()
        .enumerate()
        .map(|(i, (k, _))| format!("{k}: $p{i}"))
        .collect();
    let cypher = format!(
        "CREATE (n:{label} {{{}}}) RETURN id(n) AS vid",
        prop_str.join(", ")
    );
    let mut qb = tx.query_with(&cypher);
    for (i, (_, v)) in props.iter().enumerate() {
        qb = qb.param(&format!("p{i}"), v.clone());
    }
    let result = qb.fetch_all().await.unwrap();
    tx.commit().await.unwrap();
    result.rows().first().unwrap().get::<i64>("vid").unwrap()
}

async fn create_edge(db: &Uni, edge_type: &str, from: i64, to: i64) -> i64 {
    let session = db.session();
    let tx = session.tx().await.unwrap();
    let cypher = format!(
        "MATCH (a), (b) WHERE id(a) = $src AND id(b) = $dst \
         CREATE (a)-[r:{edge_type}]->(b) RETURN id(r) AS eid"
    );
    let result = tx
        .query_with(&cypher)
        .param("src", from)
        .param("dst", to)
        .fetch_all()
        .await
        .unwrap();
    tx.commit().await.unwrap();
    result.rows().first().unwrap().get::<i64>("eid").unwrap()
}

async fn measure_query(db: &Uni, query: &str, nid: i64, samples: usize) -> f64 {
    let session = db.session();
    let mut times = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        let _ = session
            .query_with(query)
            .param("nid", nid)
            .fetch_all()
            .await
            .unwrap();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[0]
}

#[tokio::test]
async fn probe_get_edges_layer_attribution() {
    let db = setup_db().await;

    let participant_id = create_node(
        &db,
        "Participant",
        &[("name", Value::String("alice".into()))],
    )
    .await;

    let mut session_ids = Vec::with_capacity(PARTICIPANT_EDGES);
    for i in 0..PARTICIPANT_EDGES {
        let sid = create_node(
            &db,
            "Session",
            &[("session_id", Value::String(format!("s-{i:03}")))],
        )
        .await;
        create_edge(&db, "LINK", participant_id, sid).await;
        session_ids.push(sid);
    }

    let q_a = "MATCH (a) WHERE id(a) = $nid RETURN id(a) AS eid";
    let q_b = "MATCH (a)-[r:LINK]->() WHERE id(a) = $nid RETURN count(r) AS eid";
    let q_c = "MATCH (a)-[r:LINK]->(b) WHERE id(a) = $nid RETURN id(r) AS eid";

    eprintln!(
        "{:>22} | {:>10} | {:>10} | {:>10}",
        "stage", "A no-trav", "B count", "C full"
    );
    eprintln!("{}", "-".repeat(64));

    let report = |stage: &str, a: f64, b: f64, c: f64| {
        eprintln!("{stage:>22} | {a:>10.2} | {b:>10.2} | {c:>10.2}");
    };

    let a = measure_query(&db, q_a, participant_id, SAMPLES_PER_ROUND).await;
    let b = measure_query(&db, q_b, participant_id, SAMPLES_PER_ROUND).await;
    let c = measure_query(&db, q_c, participant_id, SAMPLES_PER_ROUND).await;
    report("baseline (~21 nodes)", a, b, c);

    let mut total_filler = 0usize;
    for round in 0..ROUNDS {
        for j in 0..FILLER_PER_ROUND {
            let msg_id = create_node(
                &db,
                "Message",
                &[(
                    "content",
                    Value::String(format!("filler r{round}-{j}")),
                )],
            )
            .await;
            let target_session = session_ids[j % session_ids.len()];
            create_edge(&db, "IN_SESSION", msg_id, target_session).await;
        }
        total_filler += FILLER_PER_ROUND;

        let a = measure_query(&db, q_a, participant_id, SAMPLES_PER_ROUND).await;
        let b = measure_query(&db, q_b, participant_id, SAMPLES_PER_ROUND).await;
        let c = measure_query(&db, q_c, participant_id, SAMPLES_PER_ROUND).await;
        let stage = format!("round {round} (+{total_filler})");
        report(&stage, a, b, c);
    }

    db.shutdown().await.unwrap();
}
