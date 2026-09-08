// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #223 (end-to-end half) — the query read path does not scale its
//! reported scan count with edge count.
//!
//! #218 was a loop issuing one storage scan per edge. The batched and per-item
//! forms **return identical answers**, so no correctness test distinguishes
//! them; the guard has to be a count. #223 is what made counting possible.
//!
//! # What this test does and does not cover
//!
//! Measured, not assumed: the query below is served by the adjacency path, and
//! a probe confirmed it never reaches `MainEdgeDataset::execute_query` — before
//! **or** after compaction. So this test does **not** guard the main-edge
//! plumbing, and reverting that plumbing does not make it fail. The proof that
//! main-edge scans are counted, and that the counter tells a per-edge loop from
//! a batched read (60 scans against 1), lives in
//! `uni-store`'s `bugs::issue_223_main_edge_scan_shape` instead.
//!
//! What it does guard is the shape of the whole read path as a customer
//! exercises it: an 8x increase in edges must not multiply the reported scan
//! count. A future per-edge scan loop anywhere in a counted part of that path
//! fails this. Its limit is that path's counter coverage — work in tiers that
//! do not yet carry counters stays invisible here, which is why the paired
//! `> 0` denominator assertion is not optional.

use uni_db::{Uni, Value};

/// Edge counts for the two measured points. The ratio is what the scaling
/// assertion is about; the absolute sizes only need to be large enough that a
/// per-edge loop would be unmistakable.
const SMALL: i64 = 40;
const LARGE: i64 = 320;

/// Build a hub with `n` outgoing edges carrying a schemaless property, flush so
/// the read touches persisted storage rather than L0, and return the db plus the
/// hub's vid.
async fn fixture(n: i64) -> (Uni, i64) {
    let db = Uni::temporary().build().await.unwrap();

    // `LINK` is declared with no properties, so `weight` is a schemaless
    // property. Which storage tier ultimately serves it is not what this test
    // asserts — see the module docs.
    db.schema()
        .label("Hub")
        .done()
        .label("Leaf")
        .done()
        .edge_type("LINK", &["Hub"], &["Leaf"])
        .done()
        .apply()
        .await
        .unwrap();

    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.query("CREATE (:Hub {name: 'hub'})").await.unwrap();
    tx.query_with(
        "MATCH (h:Hub) \
         UNWIND range(0, $n - 1) AS i \
         CREATE (h)-[:LINK {weight: i}]->(:Leaf {idx: i})",
    )
    .param("n", Value::Int(n))
    .fetch_all()
    .await
    .unwrap();
    tx.commit().await.unwrap();
    // Without the flush the edges stay in L0 and the query touches no Lance scan
    // at all, taking the denominator assertion below to zero.
    db.flush().await.unwrap();
    // Compact as well, so the measurement is taken in the post-compaction state
    // #218 was reported against rather than only against a freshly flushed one.
    // (Compaction does not move this query onto the main-edge path -- it stays on
    // the adjacency path either way; see the module docs.)
    db.compaction().compact("LINK").await.unwrap();

    let hub = db
        .session()
        .query("MATCH (h:Hub) RETURN id(h) AS vid")
        .await
        .unwrap()
        .rows()[0]
        .get::<i64>("vid")
        .unwrap();

    (db, hub)
}

/// Read every edge's schemaless property off the hub, returning
/// `(rows, scans_reported)`.
async fn measure(db: &Uni, hub: i64) -> (usize, u64) {
    let r = db
        .session()
        .query_with(
            "MATCH (h:Hub)-[r:LINK]->(l:Leaf) WHERE id(h) = $hub \
             RETURN r.weight AS w",
        )
        .param("hub", Value::Int(hub))
        .fetch_all()
        .await
        .unwrap();
    let m = r.metrics().clone();
    (r.rows().len(), m.scans_reported)
}

/// The denominator: the query reports at least one scan.
///
/// Without this, the scaling assertion below compares zero against zero and
/// holds for any shape at all — including the per-edge loop it exists to
/// reject. It is the same denominator role `scans_reported` plays throughout.
#[tokio::test]
async fn edge_property_query_reports_at_least_one_scan() {
    let (db, hub) = fixture(SMALL).await;
    let (rows, scans) = measure(&db, hub).await;

    assert_eq!(rows, SMALL as usize, "fixture did not produce the edges");
    assert!(
        scans > 0,
        "a query reading {SMALL} schemaless edge properties reported zero Lance \
         scans, so the scaling assertion in this module has no denominator and \
         would hold for any shape"
    );
}

/// The shape guard: 8x the edges must not cost 8x the scans.
///
/// The bound is deliberately loose. The claim under test is *sub-linear*, not a
/// specific constant -- a batched read may take one pass or several depending on
/// chunking and on `prefers_full_scan`, and pinning the exact number would make
/// this fail on unrelated strategy changes. A per-edge loop puts the ratio at
/// ~8x; anything at or under 2x cannot be one.
#[tokio::test]
async fn edge_property_reads_do_not_scan_per_edge() {
    let (db_s, hub_s) = fixture(SMALL).await;
    let (rows_s, scans_s) = measure(&db_s, hub_s).await;

    let (db_l, hub_l) = fixture(LARGE).await;
    let (rows_l, scans_l) = measure(&db_l, hub_l).await;

    eprintln!(
        "issue #223 scan shape: {rows_s} edges -> {scans_s} scans, {rows_l} edges -> {scans_l} scans"
    );

    assert_eq!(rows_s, SMALL as usize);
    assert_eq!(rows_l, LARGE as usize);
    assert!(
        scans_s > 0 && scans_l > 0,
        "no scans reported at either size, so this measured nothing \
         (small={scans_s}, large={scans_l})"
    );

    let edge_ratio = LARGE as f64 / SMALL as f64;
    let scan_ratio = scans_l as f64 / scans_s as f64;
    assert!(
        scan_ratio <= 2.0,
        "scan count tracks edge count: {SMALL} edges took {scans_s} scans and \
         {LARGE} edges took {scans_l} ({scan_ratio:.2}x for {edge_ratio:.0}x the \
         edges). That is the per-edge-loop shape #218 removed."
    );
}
