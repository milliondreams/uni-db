// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #269 — a `BulkWriter`-inserted vertex is absent from `VidLabelsIndex`
//! on the inserting handle, so a batched property read fans out across every
//! declared label.
//!
//! `PropertyManager::get_batch_vertex_props` resolves the labels to scan by
//! looking each vid up in the index, and the first miss falls back to scanning
//! every declared label (#264). The index is populated at startup from the main
//! vertex table and thereafter only by the flush path — and `uni-bulk` writes
//! straight to Lance, registering nothing.
//!
//! # Why this needed its own fix rather than #264's
//!
//! #264 proposes consulting L0 for the labels the index is missing, on the
//! grounds that L0 "knows the labels of exactly the vertices the index is
//! missing, since it is what wrote them". That holds for #264's own trigger — a
//! written-but-unflushed vertex — and fails here: a bulk row was **never in
//! L0**, so L0 cannot answer for it. Had #264 been closed that way alone, this
//! path would have kept fanning out with nothing red.
//!
//! Both are fixed now, by the two independent routes that were actually needed:
//! `BulkWriter` registers what it writes, and the resolver falls back to L0.
//!
//! Three triggers for the same fan-out, which is worth keeping straight:
//!
//! | | trigger |
//! |---|---|
//! | #222 (edges) | L0 is **cold** |
//! | #264 (vertices) | L0 is **hot** — written, not yet flushed |
//! | #269 (here) | the row was **never in L0** |
//!
//! # Why these call the `PropertyManager` rather than issuing a query
//!
//! Two earlier probes measured this through Cypher and both reported *no
//! difference* — and both were wrong, because neither shape reaches the
//! resolving call:
//!
//! * `MATCH (n) WHERE id(n) = $v RETURN n.tag` — 1 scan on every arm.
//! * `MATCH p = (s:Src)-[:R]->(t) WHERE id(t) = $v RETURN p` — 12 on every arm.
//!
//! What exposed them was running #264's *known* trigger as a control in the same
//! probe: it did not fan out either, which cannot be true, so the probe was
//! blind. Without that control either run would have read as a clean
//! falsification of a defect that is real.
//!
//! So the reads below go direct, which is also the evidence level #264's own
//! tests use (`uni-store/tests/common/bugs/repro_vertex_props_unresolved_vid_fan_out.rs`).
//!
//! # Why the assertions are contrasts, not thresholds
//!
//! The answer is identical either way — the vertex is served correctly however
//! many label datasets were scanned — so no correctness test can tell the two
//! shapes apart and the guard has to be a count. A bare `scans == LABELS` would
//! also hold if the counter broke and every arm went to zero, so each test is
//! read against the resolved arm as its own denominator.

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use uni_common::core::id::Vid;
use uni_db::{DataType, Uni, Value};
use uni_store::QueryCounters;
use uni_store::runtime::PropertyManager;
use uni_store::runtime::context::QueryContext;

/// Declared labels. The fan-out costs one scan per declared label, so this is
/// the multiplier a miss produces.
const LABELS: usize = 8;

struct Fixture {
    /// Held, not leaked: `std::mem::forget(TempDir)` leaves a directory on disk
    /// every run, which is a leak miri has already flagged once in this tree.
    _dir: tempfile::TempDir,
    db: Uni,
    /// A flushed vertex, so the index knows it.
    resolved: i64,
    /// Inserted through `BulkWriter` on this same handle.
    bulk: i64,
    /// Written through L0 and deliberately not flushed — #264's trigger.
    unflushed: i64,
}

async fn vid_of(db: &Uni, tag: &str) -> Result<i64> {
    let r = db
        .session()
        .query_with("MATCH (n:L0) WHERE n.tag = $t RETURN id(n) AS v")
        .param("t", Value::String(tag.to_string()))
        .fetch_all()
        .await?;
    match r.rows().first().map(|row| row.values()[0].clone()) {
        Some(Value::Int(v)) => Ok(v),
        other => anyhow::bail!("no vid for tag {tag}: {other:?}"),
    }
}

async fn fixture() -> Result<Fixture> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    let mut sb = db.schema();
    for i in 0..LABELS {
        sb = sb
            .label(&format!("L{i}"))
            .property("tag", DataType::String)
            .done();
    }
    sb.apply().await?;

    // One vertex per label, flushed, so the index is populated.
    let session = db.session();
    let tx = session.tx().await?;
    for i in 0..LABELS {
        tx.execute(&format!("CREATE (:L{i} {{tag: 'plain{i}'}})"))
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    let resolved = vid_of(&db, "plain0").await?;

    // A bulk-inserted vertex on the same handle.
    let tx = session.tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let mut p = HashMap::new();
    p.insert("tag".to_string(), Value::String("bulk".to_string()));
    bulk.insert_vertices("L0", vec![p]).await?;
    bulk.commit().await?;
    tx.commit().await?;
    let bulk_vid = vid_of(&db, "bulk").await?;

    // And #264's trigger, for contrast.
    let tx = session.tx().await?;
    tx.execute("CREATE (:L0 {tag: 'unflushed'})").await?;
    tx.commit().await?;
    let unflushed = vid_of(&db, "unflushed").await?;

    Ok(Fixture {
        _dir: dir,
        db,
        resolved,
        bulk: bulk_vid,
        unflushed,
    })
}

/// Read `tag` for one vid through the batched path, returning
/// `(rows_resolved, scans_reported)`.
async fn measure(db: &Uni, vid: i64) -> Result<(usize, u64)> {
    let pm = PropertyManager::new(db.storage(), db.schema_manager(), 0);
    let counters = Arc::new(QueryCounters::new());
    let l0 = db
        .writer()
        .expect("fixture database is writable")
        .l0_manager
        .get_current();
    let mut ctx = QueryContext::new(l0);
    ctx.counters = Some(counters.clone());
    let got = pm
        .get_batch_vertex_props(&[Vid::from(vid as u64)], &["tag"], Some(&ctx))
        .await?;
    Ok((got.len(), counters.scans_reported()))
}

/// The denominator: a vid the index knows costs one scan, not `LABELS`.
///
/// Every assertion below is read against this. Without it, "the bulk vid costs
/// 1 scan" would also hold if the counter had stopped observing this path.
#[tokio::test]
async fn a_resolved_vid_scans_only_its_own_label() -> Result<()> {
    let f = fixture().await?;
    let (rows, scans) = measure(&f.db, f.resolved).await?;
    assert_eq!(rows, 1, "the resolved vertex must be found");
    assert_eq!(
        scans, 1,
        "a vid in the index must cost one scan across {LABELS} declared labels; \
         got {scans}. If this is 0 the counter is not observing the path and \
         the other tests here mean nothing."
    );
    Ok(())
}

/// The defect: a bulk-inserted vid must cost the same as a resolved one.
#[tokio::test]
async fn a_bulk_inserted_vid_does_not_fan_out_across_every_label() -> Result<()> {
    let f = fixture().await?;
    let (_, resolved_scans) = measure(&f.db, f.resolved).await?;
    let (rows, bulk_scans) = measure(&f.db, f.bulk).await?;

    assert_eq!(rows, 1, "the bulk vertex must be found");
    assert_eq!(
        bulk_scans, resolved_scans,
        "a bulk-inserted vid cost {bulk_scans} scans against {resolved_scans} \
         for a resolved one, across {LABELS} declared labels. `BulkWriter` \
         writes straight to Lance and used to register nothing in \
         `VidLabelsIndex`, so the resolution loop missed and fell back to every \
         declared label."
    );
    Ok(())
}

/// A reopen was the workaround, and must stay correct.
///
/// The index is rebuilt at startup from the main vertex table, which is where
/// the bulk rows already were — so this arm passed before the fix too. It is
/// here to catch a fix that registers labels at commit but breaks the rebuild.
#[tokio::test]
async fn the_bulk_vid_is_resolved_after_a_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = Uni::open(&path).build().await?;
        db.schema()
            .label("L0")
            .property("tag", DataType::String)
            .done()
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        let mut bulk = tx.bulk_writer().build()?;
        let mut p = HashMap::new();
        p.insert("tag".to_string(), Value::String("bulk".to_string()));
        bulk.insert_vertices("L0", vec![p]).await?;
        bulk.commit().await?;
        tx.commit().await?;
        db.shutdown().await?;
    }

    let db = Uni::open(&path).build().await?;
    let vid = vid_of(&db, "bulk").await?;
    let (rows, scans) = measure(&db, vid).await?;
    assert_eq!(rows, 1, "the bulk vertex must survive the reopen");
    assert_eq!(
        scans, 1,
        "after a reopen the index is rebuilt from the main vertex table, so the \
         bulk vid must resolve; got {scans} scans"
    );
    Ok(())
}

/// #264's trigger is now fixed too, and this pins the pair.
///
/// It was written asserting that an unflushed vid still fanned out, with a note
/// to update rather than delete it if #264 was ever fixed. It was, in the same
/// change: `get_batch_vertex_props` now consults L0 for the labels the index is
/// missing. Both triggers therefore resolve, by different routes — the bulk vid
/// because `BulkWriter` registers it, the unflushed one because L0 wrote it —
/// and the two routes are independent, so this arm going red while the bulk arm
/// stays green would mean the L0 consultation regressed and nothing else.
#[tokio::test]
async fn an_unflushed_vid_also_resolves_now_that_l0_is_consulted() -> Result<()> {
    let f = fixture().await?;
    let (_, resolved_scans) = measure(&f.db, f.resolved).await?;
    let (rows, unflushed_scans) = measure(&f.db, f.unflushed).await?;

    assert_eq!(rows, 1, "the unflushed vertex is served correctly from L0");
    assert_eq!(
        unflushed_scans, resolved_scans,
        "#264's trigger must cost what a resolved vid costs ({resolved_scans} \
         scans); got {unflushed_scans} across {LABELS} declared labels. Equal to \
         {LABELS} means the L0 consultation stopped firing."
    );
    Ok(())
}
