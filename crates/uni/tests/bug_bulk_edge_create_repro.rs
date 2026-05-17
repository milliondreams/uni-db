// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Reproducers for two suspected bugs surfaced while ingesting a Hetionet
// subgraph through the Python binding (see prepare_adverse_drug_reaction_
// notebook_data.py workarounds). Each test is intentionally scoped to the
// smallest input that reproduces the symptom.
//
//   Bug A — single-tx MATCH+CREATE silently drops edges past some ceiling.
//           The tx commits cleanly; no error is raised; only the row count
//           on the final query reveals the loss.
//
//   Bug B — running multiple sequential edge-creating transactions causes
//           edges written by EARLIER txs to disappear after LATER txs
//           commit. Each tx in isolation reports its expected write count;
//           only the cross-tx aggregate query shows the corruption.
//
// Both tests are deliberately NOT `#[ignore]`d: they finish in a few
// seconds and are the kind of regression we want gating CI.

use anyhow::Result;
use uni_db::{DataType, Uni};

async fn setup_bipartite_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Src")
        .property("sid", DataType::Int64)
        .done()
        .label("Dst")
        .property("did", DataType::Int64)
        .done()
        .edge_type("REL", &["Src"], &["Dst"])
        .apply()
        .await?;
    Ok(db)
}

/// Seed `n_src` Src nodes and `n_dst` Dst nodes in a single tx.
async fn seed_nodes(db: &Uni, n_src: i64, n_dst: i64) -> Result<()> {
    let tx = db.session().tx().await?;
    for i in 0..n_src {
        tx.execute(&format!("CREATE (:Src {{sid: {i}}})")).await?;
    }
    for j in 0..n_dst {
        tx.execute(&format!("CREATE (:Dst {{did: {j}}})")).await?;
    }
    tx.commit().await?;
    Ok(())
}

// ── Bug A: single-tx MATCH+CREATE silent drop ────────────────────────

/// Sanity check at small scale — establishes the baseline that
/// MATCH+CREATE works correctly when the expanded edge set is small.
#[tokio::test]
async fn bug_a_match_create_small_scale_baseline() -> Result<()> {
    let db = setup_bipartite_db().await?;
    seed_nodes(&db, 10, 10).await?; // 100 edges expected from cartesian

    let tx = db.session().tx().await?;
    tx.execute("MATCH (s:Src), (d:Dst) CREATE (s)-[:REL]->(d)")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH ()-[r:REL]->() RETURN count(r) AS cnt")
        .await?;
    let cnt = result.rows()[0].get::<i64>("cnt")?;
    assert_eq!(cnt, 100, "baseline cartesian-CREATE lost edges: got {cnt}");
    Ok(())
}

/// Hypothesised silent-drop point. 100 × 60 = 6000 expected edges,
/// well above the ~2000 ceiling we suspect.
#[tokio::test]
async fn bug_a_match_create_above_suspected_ceiling() -> Result<()> {
    let db = setup_bipartite_db().await?;
    seed_nodes(&db, 100, 60).await?; // 6000 edges expected

    let tx = db.session().tx().await?;
    // Read row count via the in-tx query both BEFORE commit (to isolate
    // whether the planner already drops) and after commit (to isolate
    // whether the writer drops on flush).
    tx.execute("MATCH (s:Src), (d:Dst) CREATE (s)-[:REL]->(d)")
        .await?;
    let in_tx = tx
        .query("MATCH ()-[r:REL]->() RETURN count(r) AS cnt")
        .await?;
    let in_tx_cnt = in_tx.rows()[0].get::<i64>("cnt")?;
    tx.commit().await?;

    let after = db
        .session()
        .query("MATCH ()-[r:REL]->() RETURN count(r) AS cnt")
        .await?;
    let after_cnt = after.rows()[0].get::<i64>("cnt")?;

    assert_eq!(
        in_tx_cnt, 6000,
        "PLANNER-LEVEL DROP: in-tx read saw {in_tx_cnt} edges, expected 6000"
    );
    assert_eq!(
        after_cnt, 6000,
        "WRITER-LEVEL DROP: post-commit read saw {after_cnt} edges, expected 6000 \
         (in-tx had {in_tx_cnt})"
    );
    Ok(())
}

/// Binary-search-style ladder to localise the ceiling.
/// Runs a sequence of independent dbs, each at a different expansion size,
/// reporting the first size at which loss occurs. Always passes — its job
/// is to print the diagnostic, not gate CI. The above test is the gate.
#[tokio::test]
async fn bug_a_locate_ceiling_diagnostic() -> Result<()> {
    let targets: [(i64, i64); 7] = [
        (20, 20),   // 400
        (30, 30),   // 900
        (40, 40),   // 1600
        (45, 45),   // 2025
        (50, 50),   // 2500
        (60, 60),   // 3600
        (80, 80),   // 6400
    ];
    let mut report = String::from("\nbug_a ceiling diagnostic\n");
    for (ns, nd) in targets {
        let db = setup_bipartite_db().await?;
        seed_nodes(&db, ns, nd).await?;
        let expected = ns * nd;

        let tx = db.session().tx().await?;
        tx.execute("MATCH (s:Src), (d:Dst) CREATE (s)-[:REL]->(d)")
            .await?;
        tx.commit().await?;

        let after = db
            .session()
            .query("MATCH ()-[r:REL]->() RETURN count(r) AS cnt")
            .await?;
        let got = after.rows()[0].get::<i64>("cnt")?;
        let marker = if got == expected { "ok" } else { "LOSS" };
        report.push_str(&format!(
            "  {ns:>3} x {nd:>3} = {expected:>5} expected, {got:>5} actual  [{marker}]\n"
        ));
    }
    eprintln!("{report}");
    Ok(())
}

// ── Bug B: multi-tx edge corruption ──────────────────────────────────

/// Three sequential edge-creating txs, each writing a disjoint slice of
/// the bipartite product. After each commit we read the cumulative edge
/// count. Bug expectation: a later tx's commit reduces what an earlier
/// tx wrote, so totals stop growing monotonically.
#[tokio::test]
async fn bug_b_sequential_edge_txs_preserve_each_other() -> Result<()> {
    let db = setup_bipartite_db().await?;
    seed_nodes(&db, 60, 60).await?; // 3600 cartesian capacity

    // Tx 1: edges where (sid + did) % 3 == 0  -> ~1200 edges
    // Tx 2: edges where (sid + did) % 3 == 1  -> ~1200 edges
    // Tx 3: edges where (sid + did) % 3 == 2  -> ~1200 edges
    let predicates = [
        "(s.sid + d.did) % 3 = 0",
        "(s.sid + d.did) % 3 = 1",
        "(s.sid + d.did) % 3 = 2",
    ];

    let mut running_total: i64 = 0;
    for (idx, pred) in predicates.iter().enumerate() {
        let tx = db.session().tx().await?;
        tx.execute(&format!(
            "MATCH (s:Src), (d:Dst) WHERE {pred} CREATE (s)-[:REL]->(d)"
        ))
        .await?;
        tx.commit().await?;

        let after = db
            .session()
            .query("MATCH ()-[r:REL]->() RETURN count(r) AS cnt")
            .await?;
        let cnt = after.rows()[0].get::<i64>("cnt")?;
        assert!(
            cnt > running_total,
            "tx{} reduced total edge count: was {running_total}, now {cnt}",
            idx + 1
        );
        running_total = cnt;
    }

    // Final total must equal the full cartesian 3600.
    assert_eq!(
        running_total, 3600,
        "multi-tx edge accumulation lost edges: final={running_total}, expected 3600"
    );
    Ok(())
}

/// Same as above but with many small txs (10 slices of ~360 edges).
/// More txs = more opportunities for a snapshot/visibility bug to bite.
#[tokio::test]
async fn bug_b_many_small_edge_txs_preserve_each_other() -> Result<()> {
    let db = setup_bipartite_db().await?;
    seed_nodes(&db, 60, 60).await?;

    let n_slices: i64 = 10;
    let mut running_total: i64 = 0;
    for slice in 0..n_slices {
        let tx = db.session().tx().await?;
        tx.execute(&format!(
            "MATCH (s:Src), (d:Dst) WHERE (s.sid + d.did) % {n_slices} = {slice} \
             CREATE (s)-[:REL]->(d)"
        ))
        .await?;
        tx.commit().await?;

        let after = db
            .session()
            .query("MATCH ()-[r:REL]->() RETURN count(r) AS cnt")
            .await?;
        let cnt = after.rows()[0].get::<i64>("cnt")?;
        assert!(
            cnt > running_total,
            "tx for slice {slice} reduced edge count: was {running_total}, now {cnt}"
        );
        running_total = cnt;
    }

    assert_eq!(
        running_total, 3600,
        "10-tx edge accumulation lost edges: final={running_total}, expected 3600"
    );
    Ok(())
}
