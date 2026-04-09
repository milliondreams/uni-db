// Regression test for GitHub issue #27:
// Graph algorithms must see L0 (unflushed) data via AlgoContext.

use uni_db::{DataType, Uni};

#[tokio::test]
async fn pagerank_sees_l0_data_without_flush() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Node")
        .property("name", DataType::String)
        .done()
        .edge_type("LINKS", &["Node"], &["Node"])
        .done()
        .apply()
        .await?;

    let session = db.session();

    // Create nodes and edges via transaction.
    let tx = session.tx().await?;
    tx.execute("CREATE (:Node {name: 'A'})").await?;
    tx.execute("CREATE (:Node {name: 'B'})").await?;
    tx.execute("CREATE (:Node {name: 'C'})").await?;
    tx.execute("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (b)-[:LINKS]->(a)")
        .await?;
    tx.execute("MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) CREATE (c)-[:LINKS]->(a)")
        .await?;
    tx.commit().await?;

    // DO NOT flush — data is in L0 only.

    // Verify session query sees the nodes (merges L0 correctly).
    let res = session
        .query("MATCH (n:Node) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(
        res.rows()[0].get::<i64>("cnt")?,
        3,
        "Session query should see 3 nodes in L0"
    );

    // Run PageRank — should see the same 3 nodes in L0.
    let pr = session
        .query(
            "CALL uni.algo.pageRank(['Node'], ['LINKS'], 0.85, 20, 0.000001) \
             YIELD nodeId, score RETURN count(*) AS cnt",
        )
        .await?;

    let pr_count = pr.rows()[0].get::<i64>("cnt")?;
    assert!(
        pr_count >= 3,
        "PageRank should see L0 nodes, but got {pr_count} rows"
    );

    Ok(())
}

#[tokio::test]
async fn wcc_sees_l0_data_without_flush() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Node")
        .property("name", DataType::String)
        .done()
        .edge_type("LINKS", &["Node"], &["Node"])
        .done()
        .apply()
        .await?;

    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:Node {name: 'A'})").await?;
    tx.execute("CREATE (:Node {name: 'B'})").await?;
    tx.execute("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:LINKS]->(b)")
        .await?;
    tx.commit().await?;

    // No flush — L0 only.

    let cc = session
        .query(
            "CALL uni.algo.wcc(['Node'], ['LINKS']) \
             YIELD nodeId, componentId RETURN count(*) AS cnt",
        )
        .await?;

    let cc_count = cc.rows()[0].get::<i64>("cnt")?;
    assert!(
        cc_count >= 2,
        "wcc should see L0 nodes, but got {cc_count} rows"
    );

    Ok(())
}
