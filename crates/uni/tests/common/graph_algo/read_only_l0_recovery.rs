// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A read-only open must not discard the WAL-replayed L0 (Phase 2, bug A).
//!
//! Opening a database replays the WAL suffix above the snapshot manifest's
//! high-water mark into the writer's `L0Manager`. The WAL is appended at *commit*
//! time, so that suffix is exactly the committed-but-not-yet-flushed set. A
//! read-only open then dropped the writer — and with it the only handle to that
//! recovered L0 — so `Executor::get_context()` returned `None` and every read on
//! the session saw flushed L1 only.
//!
//! The effect was a silent rollback of committed transactions, and it was not
//! GraphCompute-specific: a plain `MATCH` missed the rows too. Measured on
//! `a7c665045`, a partially-flushed database returned **2 of 4** committed nodes
//! with no error. The GraphCompute case is kept because it is the path that
//! surfaced this and it exercises the whole chain down to `ProjectionBuilder`.
//!
//! Needs a real on-disk database: an in-memory one has no WAL to replay.

use uni_db::{DataType, Uni, UniConfig};

use crate::ssi_support::reopen::DiskHarness;

/// Auto-flush disabled: the 5 s default races `drop(db)` (shutdown only *sends*
/// the signal without awaiting the flush task), so leaving it on makes the
/// "closed without flushing" state nondeterministic. This is the same idiom the
/// dense-resilience recovery tests use.
fn no_autoflush() -> UniConfig {
    UniConfig {
        auto_flush_interval: None,
        ..Default::default()
    }
}

/// Flushes two `:Node` rows to L1, then commits two more plus all four
/// `:LINKS` edges and drops the handle **without flushing**.
///
/// The first flush is required, not incidental: a database with WAL segments and
/// no snapshot manifest refuses to reopen at all ("cannot safely determine
/// version counter"). It also produces the more realistic — and sharper — shape:
/// a partially-flushed database, where losing L0 yields *stale* data rather than
/// none. Reading 2 nodes instead of 4 is exactly the plausible-but-wrong answer
/// this class of bug produces.
async fn seed_then_close_uncleanly(h: &DiskHarness) -> anyhow::Result<()> {
    let db = h.open_with(no_autoflush()).await?;
    db.schema()
        .label("Node")
        .property("name", DataType::String)
        .done()
        .edge_type("LINKS", &["Node"], &["Node"])
        .done()
        .apply()
        .await?;
    let session = db.session();

    // Durable half.
    let tx = session.tx().await?;
    for name in ["A", "B"] {
        tx.execute(&format!("CREATE (:Node {{name: '{name}'}})"))
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    // WAL-only half: committed, never flushed.
    let tx = session.tx().await?;
    for name in ["C", "D"] {
        tx.execute(&format!("CREATE (:Node {{name: '{name}'}})"))
            .await?;
    }
    for (a, b) in [("A", "B"), ("B", "C"), ("C", "A"), ("A", "D")] {
        tx.execute(&format!(
            "MATCH (a:Node {{name: '{a}'}}), (b:Node {{name: '{b}'}}) CREATE (a)-[:LINKS]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;
    // Deliberately NO db.flush(): this commit lives only in the WAL.
    drop(db);
    Ok(())
}

async fn open_read_only(h: &DiskHarness) -> anyhow::Result<Uni> {
    Ok(Uni::open(h.uri())
        .config(no_autoflush())
        .read_only()
        .build()
        .await?)
}

/// The general regression: an ordinary `MATCH` must see the recovered rows.
#[tokio::test]
async fn read_only_reopen_sees_wal_replayed_rows() -> anyhow::Result<()> {
    let h = DiskHarness::new()?;
    seed_then_close_uncleanly(&h).await?;

    let db = open_read_only(&h).await?;
    let rows = db
        .session()
        .query("MATCH (n:Node) RETURN count(n) AS c")
        .await?;
    let count: i64 = rows.rows()[0].get("c")?;
    assert_eq!(
        count, 4,
        "a read-only open replays the WAL and must expose the recovered rows; \
         hiding them silently rolls back a committed transaction (a count of 2 \
         means only the flushed half was visible)"
    );
    db.shutdown().await?;
    Ok(())
}

/// The same visibility, all the way through a GraphCompute projection.
#[tokio::test]
async fn read_only_reopen_graph_compute_projects_replayed_rows() -> anyhow::Result<()> {
    let h = DiskHarness::new()?;
    seed_then_close_uncleanly(&h).await?;

    let db = open_read_only(&h).await?;
    let session = db.session();
    let vid: i64 = session
        .query("MATCH (a:Node {name: 'A'}) RETURN id(a) AS vid")
        .await?
        .rows()[0]
        .get("vid")?;

    let res = session
        .query(&format!(
            "CALL uni.algo.gcpagerank({vid}, 0.85, \
             {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}}) \
             YIELD nodeId, score RETURN nodeId, score"
        ))
        .await?;
    assert_eq!(
        res.rows().len(),
        4,
        "the projection must see the WAL-recovered rows, not an empty graph"
    );
    for row in res.rows() {
        let s: f64 = row.get("score")?;
        assert!(s.is_finite(), "score must be finite, got {s}");
    }
    db.shutdown().await?;
    Ok(())
}

/// Handing the session an `L0Manager` must not re-enable writing.
///
/// Writes are gated on `UniInner.writer`, which stays `None`; this pins that the
/// fix widened read visibility only.
#[tokio::test]
async fn read_only_reopen_still_rejects_writes() -> anyhow::Result<()> {
    let h = DiskHarness::new()?;
    seed_then_close_uncleanly(&h).await?;

    let db = open_read_only(&h).await?;
    assert!(
        db.session().tx().await.is_err(),
        "a read-only database must still refuse to open a write transaction"
    );
    db.shutdown().await?;
    Ok(())
}
