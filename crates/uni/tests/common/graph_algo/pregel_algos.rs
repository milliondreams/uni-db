// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end tests for the first-party Pregel programs (`uni.algo.pagerank`,
//! `uni.algo.sssp`), authored on top of the public `GraphView`.
//!
//! Exercises the vertex-centric executor through real `CALL` queries — the same
//! provider-dispatch + `HostQuery` path the reachability provider uses.

use uni_db::{DataType, Uni};

/// Build a directed graph and return `(vid_a, vid_b, vid_c, vid_d)`.
///
/// Edges: `A→B`, `A→C`, `B→C`, `C→B`, `D→C`. Node `C` has the most inbound
/// links, so PageRank must rank it highest.
async fn build_graph(db: &Uni) -> anyhow::Result<[i64; 4]> {
    db.schema()
        .label("Page")
        .property("name", DataType::String)
        .done()
        .edge_type("LINK", &["Page"], &["Page"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    for name in ["A", "B", "C", "D"] {
        tx.execute(&format!("CREATE (:Page {{name: '{name}'}})"))
            .await?;
    }
    for (from, to) in [("A", "B"), ("A", "C"), ("B", "C"), ("C", "B"), ("D", "C")] {
        tx.execute(&format!(
            "MATCH (a:Page {{name: '{from}'}}), (b:Page {{name: '{to}'}}) CREATE (a)-[:LINK]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;

    let mut vids = [0_i64; 4];
    for (i, name) in ["A", "B", "C", "D"].iter().enumerate() {
        let res = session
            .query(&format!(
                "MATCH (p:Page {{name: '{name}'}}) RETURN id(p) AS vid"
            ))
            .await?;
        vids[i] = res.rows()[0].get::<i64>("vid")?;
    }
    Ok(vids)
}

#[tokio::test]
async fn pagerank_ranks_most_linked_highest() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let [_a, _b, vid_c, _d] = build_graph(&db).await?;

    let res = db
        .session()
        .query(
            "CALL uni.algo.pagerank({nodeLabels: ['Page'], edgeTypes: ['LINK']}) \
             YIELD nodeId, rank RETURN nodeId, rank",
        )
        .await?;
    let rows = res.rows();
    assert_eq!(rows.len(), 4, "one rank row per page");

    // C has the most inbound links (from A, B, D) → highest rank.
    let top = rows
        .iter()
        .max_by(|x, y| {
            x.get::<f64>("rank")
                .unwrap_or(0.0)
                .partial_cmp(&y.get::<f64>("rank").unwrap_or(0.0))
                .unwrap()
        })
        .unwrap();
    assert_eq!(
        top.get::<i64>("nodeId")?,
        vid_c,
        "C (most inbound) must rank first: {rows:?}"
    );

    // Ranks are a probability-like distribution; all positive and finite.
    for r in rows {
        let rank = r.get::<f64>("rank")?;
        assert!(
            rank > 0.0 && rank.is_finite(),
            "rank must be positive: {rank}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn sssp_computes_hop_distances_from_source() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let [vid_a, vid_b, vid_c, vid_d] = build_graph(&db).await?;

    let query = format!(
        "CALL uni.algo.sssp({vid_a}, {{nodeLabels: ['Page'], edgeTypes: ['LINK']}}) \
         YIELD nodeId, distance RETURN nodeId, distance"
    );
    let res = db.session().query(&query).await?;

    let dist = |vid: i64| -> f64 {
        res.rows()
            .iter()
            .find(|r| r.get::<i64>("nodeId").ok() == Some(vid))
            .and_then(|r| r.get::<f64>("distance").ok())
            .unwrap_or(f64::NAN)
    };

    // A→B and A→C are one hop; B and C reachable at distance 1. D is not
    // reachable from A → +inf.
    assert_eq!(dist(vid_a), 0.0, "source at distance 0");
    assert_eq!(dist(vid_b), 1.0, "A→B one hop");
    assert_eq!(dist(vid_c), 1.0, "A→C one hop");
    assert!(dist(vid_d).is_infinite(), "D unreachable from A");
    Ok(())
}

#[tokio::test]
async fn expand_respects_depth_direction_and_min_level() -> anyhow::Result<()> {
    // Edges: A→B, A→C, B→C, C→B, D→C.
    let db = Uni::in_memory().build().await?;
    let [vid_a, _b, _c, vid_d] = build_graph(&db).await?;
    let session = db.session();

    // Outbound, maxLevel 1: reach A@0, B@1, C@1 (D is not reachable outbound).
    let res = session
        .query(&format!(
            "CALL uni.path.expand({vid_a}, \
             {{edgeTypes: ['LINK'], direction: 'out', maxLevel: 1}}) \
             YIELD nodeId, level RETURN nodeId, level"
        ))
        .await?;
    assert_eq!(
        res.rows().len(),
        3,
        "A reaches {{A@0, B@1, C@1}} within depth 1"
    );
    assert!(
        res.rows()
            .iter()
            .all(|r| r.get::<i64>("level").unwrap() <= 1),
        "no row exceeds maxLevel 1"
    );
    assert!(
        !res.rows()
            .iter()
            .any(|r| r.get::<i64>("nodeId").unwrap() == vid_d),
        "D is unreachable outbound from A"
    );

    // minLevel 1 excludes the source (level 0).
    let res_min = session
        .query(&format!(
            "CALL uni.path.expand({vid_a}, \
             {{edgeTypes: ['LINK'], direction: 'out', minLevel: 1, maxLevel: 1}}) \
             YIELD nodeId, level RETURN nodeId, level"
        ))
        .await?;
    assert_eq!(res_min.rows().len(), 2, "minLevel 1 drops the source A@0");
    assert!(
        res_min
            .rows()
            .iter()
            .all(|r| r.get::<i64>("level").unwrap() == 1),
        "only level-1 rows remain"
    );

    // Both directions, depth 2: reaches D via C's inbound edge (D→C).
    let res_both = session
        .query(&format!(
            "CALL uni.path.expand({vid_a}, \
             {{edgeTypes: ['LINK'], direction: 'both', maxLevel: 2}}) \
             YIELD nodeId, level RETURN nodeId, level"
        ))
        .await?;
    assert!(
        res_both
            .rows()
            .iter()
            .any(|r| r.get::<i64>("nodeId").unwrap() == vid_d),
        "both-direction expansion reaches D through C's inbound edge"
    );
    Ok(())
}
