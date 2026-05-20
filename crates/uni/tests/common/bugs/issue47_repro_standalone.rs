// SPDX-License-Identifier: Apache-2.0
// Standalone repro from issue #47 comment — no uniko dependencies.

use std::time::Instant;
use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

#[tokio::test]
#[ignore]
async fn create_edge_latency_grows() {
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
        .edge_type("SENT_BY", &["Msg"], &["User"])
        .done()
        .apply()
        .await
        .unwrap();

    let session = db.session();
    let tx = session.tx().await.unwrap();
    tx.execute("CREATE (:User {uid: 'u1'})").await.unwrap();
    tx.commit().await.unwrap();

    let user_nid: i64 = session
        .query("MATCH (u:User {uid: 'u1'}) RETURN id(u) AS nid")
        .await
        .unwrap()
        .rows()[0]
        .get("nid")
        .unwrap();

    let mut edge_times = Vec::new();
    for i in 0..300 {
        let tx = session.tx().await.unwrap();
        let result = tx
            .query_with("CREATE (m:Msg {mid: $mid}) RETURN id(m) AS nid")
            .param("mid", Value::String(format!("m-{i:04}")))
            .fetch_all()
            .await
            .unwrap();
        let msg_nid: i64 = result.rows()[0].get("nid").unwrap();
        tx.commit().await.unwrap();

        let t = Instant::now();
        let tx = session.tx().await.unwrap();
        tx.query_with(
            "MATCH (m), (u) WHERE id(m) = $mid AND id(u) = $uid \
             CREATE (m)-[:SENT_BY]->(u)",
        )
        .param("mid", Value::Int(msg_nid))
        .param("uid", Value::Int(user_nid))
        .fetch_all()
        .await
        .unwrap();
        tx.commit().await.unwrap();
        edge_times.push(t.elapsed().as_millis());
    }

    let avg = |s: &[u128]| s.iter().sum::<u128>() as f64 / s.len() as f64;
    let first = avg(&edge_times[..20]);
    let last = avg(&edge_times[280..]);
    let ratio = last / first.max(1.0);

    eprintln!("\nFirst 20 avg: {first:.1}ms");
    eprintln!("Last 20 avg:  {last:.1}ms");
    eprintln!("Ratio: {ratio:.1}x\n");
    for i in (0..300).step_by(20) {
        eprintln!("  edge {i:>4}: {}ms", edge_times[i]);
    }

    assert!(ratio < 5.0, "create_edge grew {ratio:.1}x");
}
