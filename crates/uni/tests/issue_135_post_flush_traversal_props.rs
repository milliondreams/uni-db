//! Issue #135: relationship-traversal reads return NULL properties after the
//! L0->Lance auto-flush. A node scan over the same flushed data is unaffected.
//!
//! This is NOT the "data race under CPU oversubscription" the issue report
//! hypothesizes: it is a deterministic post-flush read defect, reproduced here
//! with an explicit synchronous `db.flush()` and zero concurrency.

use uni_db::{DataType, Uni};

const CHILDREN: &[(i64, i64, f64)] = &[(1, 50, 30.0), (2, 50, 31.0), (3, 49, 29.4), (4, 51, 30.6)];

async fn build() -> anyhow::Result<Uni> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("MCTSNode")
        .property("node_id", DataType::Int64)
        .property("visits", DataType::Int64)
        .property("value_sum", DataType::Float64)
        .property("terminal", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:MCTSNode {node_id:0, visits:200, value_sum:0.0, terminal:0})")
        .await?;
    for (nid, v, w) in CHILDREN {
        tx.execute(&format!(
            "MATCH (p:MCTSNode {{node_id:0}}) \
             CREATE (p)-[:PARENT]->(:MCTSNode {{node_id:{nid}, visits:{v}, value_sum:{w}, terminal:0}})"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(db)
}

const REL: &str = "MATCH (p:MCTSNode {node_id:0})-[:PARENT]->(c:MCTSNode) \
                   RETURN c.node_id AS id, c.visits AS v, c.value_sum AS w";
const SCAN: &str = "MATCH (n:MCTSNode) WHERE n.node_id IN [1,2,3,4] \
                    RETURN n.node_id AS id, n.visits AS v, n.value_sum AS w";

async fn rel_ids(db: &Uni, q: &str) -> anyhow::Result<Vec<Option<i64>>> {
    let result = db.session().query_with(q).fetch_all().await?;
    Ok(result
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id").ok())
        .collect())
}

#[tokio::test(flavor = "multi_thread")]
async fn issue_135_traversal_props_survive_flush() -> anyhow::Result<()> {
    let db = build().await?;

    // Pre-flush: rows live in the L0 buffer, served by Phase 1.
    let pre = rel_ids(&db, REL).await?;
    eprintln!("PRE-FLUSH  REL ids = {pre:?}");
    assert_eq!(pre.len(), 4, "pre-flush: 4 children expected");
    assert!(pre.iter().all(|x| x.is_some()), "pre-flush props must be non-null");

    // Force the L0 -> Lance transition (what the 5s auto-flush timer does).
    db.flush().await?;

    let scan = rel_ids(&db, SCAN).await?;
    eprintln!("POST-FLUSH SCAN ids = {scan:?}");
    let rel = rel_ids(&db, REL).await?;
    eprintln!("POST-FLUSH REL  ids = {rel:?}");

    assert_eq!(scan.len(), 4);
    assert!(scan.iter().all(|x| x.is_some()), "node scan must read flushed props");

    assert_eq!(rel.len(), 4, "post-flush: 4 children still expanded");
    assert!(
        rel.iter().all(|x| x.is_some()),
        "BUG #135: traversal props NULL after flush: {rel:?}"
    );

    // The whole-node (`RETURN c`) / `_all_props` path was equally affected.
    let whole = db
        .session()
        .query_with(
            "MATCH (p:MCTSNode {node_id:0})-[:PARENT]->(c:MCTSNode) \
             RETURN c.node_id AS id, c.visits AS v, c.value_sum AS w",
        )
        .fetch_all()
        .await?;
    let visits: Vec<Option<i64>> = whole.rows().iter().map(|r| r.get::<i64>("v").ok()).collect();
    assert!(
        visits.iter().all(|x| x.is_some()),
        "BUG #135: multi-property traversal NULL after flush: {visits:?}"
    );
    Ok(())
}
