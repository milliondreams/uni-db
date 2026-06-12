// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_path_variable() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Int64)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:Person {name: 'Alice'})").await?;
    tx.execute("CREATE (b:Person {name: 'Bob'})").await?;
    tx.execute("CREATE (c:Person {name: 'Charlie'})").await?;
    tx.execute("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2020}]->(b)").await?;
    tx.execute("MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS {since: 2021}]->(c)").await?;
    tx.commit().await?;

    // 1. Path variable in Variable Length Traversal
    // MATCH p = (a)-[:KNOWS*1..2]->(b) WHERE a.name = 'Alice' RETURN p
    let result = db.session().query("MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b) RETURN p, length(p) AS len ORDER BY len").await?;

    // Should have 2 paths: Alice->Bob (len 1), Alice->Bob->Charlie (len 2)
    assert_eq!(result.len(), 2);

    // Path 1 (len 1)
    let row1 = &result.rows()[0];
    let len1: i64 = row1.get("len")?;
    assert_eq!(len1, 1);

    // Path 2 (len 2)
    let row2 = &result.rows()[1];
    let len2: i64 = row2.get("len")?;
    assert_eq!(len2, 2);

    // Verify Path object structure (via JSON/Value inspection if possible, or specialized getters)
    // Currently public API Row::get returns FromValue types.
    // types::Path is public.
    // Let's see if we can get it as Path
    // uni_db::Path is re-exported from uni-query::types::Path

    let p1: uni_db::Path = row1.get("p")?;
    assert_eq!(p1.nodes.len(), 2); // Alice, Bob
    assert_eq!(p1.edges.len(), 1); // KNOWS

    let p2: uni_db::Path = row2.get("p")?;
    assert_eq!(p2.nodes.len(), 3); // Alice, Bob, Charlie
    assert_eq!(p2.edges.len(), 2); // KNOWS, KNOWS

    // 2. NODES() and RELATIONSHIPS() functions
    let result = db.session().query("MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN nodes(p) AS ns, relationships(p) AS rels").await?;
    assert_eq!(result.len(), 1);
    let row = &result.rows()[0];

    // nodes(p) returns List<Node>
    // but Row::get returns FromValue.
    // Vec<Node> implements FromValue via Vec<T>.
    let ns: Vec<uni_db::Node> = row.get("ns")?;
    assert_eq!(ns.len(), 2);
    // Note: Node objects reconstructed from Path currently have empty labels/properties in executor
    // because fetch logic is not implemented inside build_traverse_match for efficiency/complexity reasons yet.
    // They contain VIDs.
    // We can verify IDs match.

    // relationships(p) returns List<Edge>
    let rels: Vec<uni_db::Edge> = row.get("rels")?;
    assert_eq!(rels.len(), 1);

    Ok(())
}

/// Regression: a path relationship must report its STORED (start -> end)
/// direction even when the path traverses it backward.
///
/// Given a stored edge `(a)-[:T]->(b)`, an undirected `(b)-[:T]-(a)`, an
/// incoming `(b)<-[:T]-(a)`, and an undirected fixed variable-length match must
/// all report the relationship with `src = a` and `dst = b` — the storage order,
/// not the traversal order. Guards the path-Value construction fix independently
/// of the openCypher TCK (see uni-query df_graph path builders).
#[tokio::test]
async fn test_path_relationship_reports_stored_direction() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("N")
        .property("name", DataType::String)
        .edge_type("T", &["N"], &["N"])
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:N {name: 'a'})").await?;
    tx.execute("CREATE (b:N {name: 'b'})").await?;
    tx.execute("MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) CREATE (a)-[:T]->(b)")
        .await?;
    tx.commit().await?;

    // Resolve the stored VIDs of a and b for direction-sensitive assertions.
    let ids = db
        .session()
        .query("MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) RETURN id(a) AS a_id, id(b) AS b_id")
        .await?;
    let a_vid: i64 = ids.rows()[0].get("a_id")?;
    let b_vid: i64 = ids.rows()[0].get("b_id")?;
    assert_ne!(a_vid, b_vid);

    // Each query traverses the edge backward relative to its storage direction.
    // The returned relationship must still point a -> b (stored order).
    let cases = [
        // Undirected, fixed single hop.
        "MATCH p = (b:N {name: 'b'})-[:T]-(a:N {name: 'a'}) RETURN p",
        // Incoming, fixed single hop.
        "MATCH p = (b:N {name: 'b'})<-[:T]-(a:N {name: 'a'}) RETURN p",
        // Undirected, fixed variable-length pattern (*1..1).
        "MATCH p = (b:N {name: 'b'})-[:T*1..1]-(a:N {name: 'a'}) RETURN p",
    ];

    for query in cases {
        let result = db.session().query(query).await?;
        assert_eq!(result.len(), 1, "query produced no path: {query}");
        let path: uni_db::Path = result.rows()[0].get("p")?;
        assert_eq!(path.edges.len(), 1, "expected one relationship: {query}");
        let edge = &path.edges[0];
        assert_eq!(
            edge.src.as_u64() as i64,
            a_vid,
            "relationship src must be stored start (a): {query}"
        );
        assert_eq!(
            edge.dst.as_u64() as i64,
            b_vid,
            "relationship dst must be stored end (b): {query}"
        );
    }

    // Sanity: matching in the stored (forward) direction is unchanged.
    let forward = db
        .session()
        .query("MATCH p = (a:N {name: 'a'})-[:T]->(b:N {name: 'b'}) RETURN p")
        .await?;
    let fpath: uni_db::Path = forward.rows()[0].get("p")?;
    assert_eq!(fpath.edges[0].src.as_u64() as i64, a_vid);
    assert_eq!(fpath.edges[0].dst.as_u64() as i64, b_vid);

    Ok(())
}

/// Regression: a path relationship must report its STORED direction even after
/// the edge has been flushed out of the L0 buffers into durable L1 storage.
///
/// Identical to [`test_path_relationship_reports_stored_direction`], but inserts
/// a `db.flush().await?` after commit so the edge is no longer resident in any
/// L0 buffer. This exercises the L1-resident path-build code, which the
/// openCypher TCK cannot reach (its data stays in L0).
#[tokio::test]
async fn test_path_relationship_reports_stored_direction_after_flush() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("N")
        .property("name", DataType::String)
        .edge_type("T", &["N"], &["N"])
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:N {name: 'a'})").await?;
    tx.execute("CREATE (b:N {name: 'b'})").await?;
    tx.execute("MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) CREATE (a)-[:T]->(b)")
        .await?;
    tx.commit().await?;

    // Force the edge out of L0 into durable L1 storage. After this the L0
    // visibility chain no longer holds the edge's stored endpoints.
    db.flush().await?;

    // Resolve the stored VIDs of a and b for direction-sensitive assertions.
    let ids = db
        .session()
        .query("MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) RETURN id(a) AS a_id, id(b) AS b_id")
        .await?;
    let a_vid: i64 = ids.rows()[0].get("a_id")?;
    let b_vid: i64 = ids.rows()[0].get("b_id")?;
    assert_ne!(a_vid, b_vid);

    // Each query traverses the (now flushed) edge backward relative to its
    // storage direction. The returned relationship must still point a -> b.
    let cases = [
        // Undirected, fixed single hop.
        "MATCH p = (b:N {name: 'b'})-[:T]-(a:N {name: 'a'}) RETURN p",
        // Incoming, fixed single hop.
        "MATCH p = (b:N {name: 'b'})<-[:T]-(a:N {name: 'a'}) RETURN p",
        // Undirected, fixed variable-length pattern (*1..1).
        "MATCH p = (b:N {name: 'b'})-[:T*1..1]-(a:N {name: 'a'}) RETURN p",
    ];

    for query in cases {
        let result = db.session().query(query).await?;
        assert_eq!(result.len(), 1, "query produced no path: {query}");
        let path: uni_db::Path = result.rows()[0].get("p")?;
        assert_eq!(path.edges.len(), 1, "expected one relationship: {query}");
        let edge = &path.edges[0];
        assert_eq!(
            edge.src.as_u64() as i64,
            a_vid,
            "relationship src must be stored start (a) after flush: {query}"
        );
        assert_eq!(
            edge.dst.as_u64() as i64,
            b_vid,
            "relationship dst must be stored end (b) after flush: {query}"
        );
    }

    // shortestPath() builds its own path struct (shortest_path.rs). Traversing
    // the flushed edge backward must still report stored a -> b.
    let sp = db
        .session()
        .query(
            "MATCH (b:N {name: 'b'}), (a:N {name: 'a'}) \
             MATCH p = shortestPath((b)-[:T*1..2]-(a)) RETURN p",
        )
        .await?;
    assert_eq!(sp.len(), 1, "shortestPath produced no path after flush");
    let sp_path: uni_db::Path = sp.rows()[0].get("p")?;
    assert_eq!(
        sp_path.edges.len(),
        1,
        "shortestPath: expected one relationship"
    );
    assert_eq!(
        sp_path.edges[0].src.as_u64() as i64,
        a_vid,
        "shortestPath relationship src must be stored start (a) after flush"
    );
    assert_eq!(
        sp_path.edges[0].dst.as_u64() as i64,
        b_vid,
        "shortestPath relationship dst must be stored end (b) after flush"
    );

    // Pattern comprehension builds its own path struct (pattern_comprehension.rs).
    // The collected relationship must report stored a -> b for the backward hop.
    let pc = db
        .session()
        .query(
            "MATCH (b:N {name: 'b'}) \
             RETURN [p = (b)-[:T]-(:N) | relationships(p)[0]] AS rels",
        )
        .await?;
    assert_eq!(
        pc.len(),
        1,
        "pattern comprehension produced no row after flush"
    );
    let pc_rels: Vec<uni_db::Edge> = pc.rows()[0].get("rels")?;
    assert_eq!(
        pc_rels.len(),
        1,
        "pattern comprehension: expected one relationship"
    );
    assert_eq!(
        pc_rels[0].src.as_u64() as i64,
        a_vid,
        "pattern-comprehension relationship src must be stored start (a) after flush"
    );
    assert_eq!(
        pc_rels[0].dst.as_u64() as i64,
        b_vid,
        "pattern-comprehension relationship dst must be stored end (b) after flush"
    );

    Ok(())
}

/// Test path variable with chained multi-hop patterns (not variable-length)
/// This tests: p = (a)-[r1]->(b)-[r2]->(c)
#[tokio::test]
async fn test_multihop_chained_path_variable() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:Person {name: 'Alice'})").await?;
    tx.execute("CREATE (b:Person {name: 'Bob'})").await?;
    tx.execute("CREATE (c:Person {name: 'Charlie'})").await?;
    tx.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .await?;
    tx.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)",
    )
    .await?;
    tx.commit().await?;

    // Test chained multi-hop pattern with path variable
    // This pattern was previously blocked with "Named path variables not yet supported for multi-hop patterns"
    let result = db.session().query(
        "MATCH p = (a:Person {name: 'Alice'})-[r1:KNOWS]->(b:Person)-[r2:KNOWS]->(c:Person) RETURN p, a.name AS a_name, c.name AS c_name"
    ).await?;

    assert_eq!(result.len(), 1, "Should return 1 path: Alice->Bob->Charlie");

    let row = &result.rows()[0];

    // Verify node names
    let a_name: String = row.get("a_name")?;
    let c_name: String = row.get("c_name")?;
    assert_eq!(a_name, "Alice");
    assert_eq!(c_name, "Charlie");

    // Verify path structure
    let path: uni_db::Path = row.get("p")?;
    assert_eq!(
        path.nodes.len(),
        3,
        "Path should have 3 nodes: Alice, Bob, Charlie"
    );
    assert_eq!(path.edges.len(), 2, "Path should have 2 edges: r1, r2");

    Ok(())
}
