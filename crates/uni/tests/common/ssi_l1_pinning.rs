// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Component C2 — transaction-level L1 (Lance) pinning.
//!
//! A read-write transaction pins the L0 tier via its `SnapshotView` (C1),
//! and since C2 also pins the L1 tier: its scans route through a
//! `StorageManager` clone whose reads filter to
//! `_version <= started_at_version`. An L0→L1 flush completing
//! mid-transaction therefore cannot leak post-snapshot rows into the
//! transaction's view (previously: "flush-boundary read skew").
//!
//! Scope notes:
//! - L1 vertex rows are single-versioned (`MergeInsert` replaces matched
//!   rows in place), so the pin guarantees post-snapshot INSERTS stay
//!   invisible. A row whose only pre-transaction state lives in L1 and that
//!   is updated-and-flushed mid-transaction is *excluded* from the pinned
//!   view rather than shown at its old value — the same boundary the
//!   time-travel feature has. Rows resident in the pinned L0 generations
//!   are immune (the L0 layer shadows L1).
//! - The EDGE tier is not version-pinned: traversals share the live
//!   `AdjacencyManager` (its commit-replay overlay is the only source of
//!   unflushed edges), so post-snapshot edges remain visible to traversals
//!   exactly as before C2. Edge reads are in the OCC read-set, so a
//!   conflicting read-modify-write still aborts at commit.

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig};

async fn items_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .property("value", DataType::Int)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:Item {name: 'a', value: 1})").await?;
    tx.commit().await?;
    // Push the seed row to L1 so the transaction under test reads it from
    // the Lance tier, not from a pinned L0 generation.
    db.flush().await?;
    Ok(db)
}

/// THE C2 repro: rows inserted and flushed to L1 mid-transaction must not
/// appear in the transaction's scans. Pre-fix, the L1 scan saw the latest
/// Lance data and the new row leaked in.
#[tokio::test]
async fn mid_tx_flush_does_not_leak_new_rows() -> Result<()> {
    let db = items_db().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let before = tx.query("MATCH (i:Item) RETURN count(*) AS n").await?;
    assert_eq!(before.rows()[0].get::<i64>("n")?, 1);

    // A concurrent commit inserts a new row, and a flush pushes it to L1
    // while the transaction is still open.
    {
        let s2 = db.session();
        let tx2 = s2.tx().await?;
        tx2.execute("CREATE (:Item {name: 'b', value: 2})").await?;
        tx2.commit().await?;
    }
    db.flush().await?;

    // The open transaction must still see exactly its snapshot.
    let after = tx.query("MATCH (i:Item) RETURN count(*) AS n").await?;
    assert_eq!(
        after.rows()[0].get::<i64>("n")?,
        1,
        "post-snapshot row flushed to L1 leaked into an open transaction"
    );
    // Property reads agree with the scan tier.
    let val = tx
        .query("MATCH (i:Item {name: 'a'}) RETURN i.value AS v")
        .await?;
    assert_eq!(val.rows()[0].get::<i64>("v")?, 1);

    tx.commit().await?;
    // After commit, a fresh read sees both rows.
    let fresh = s.query("MATCH (i:Item) RETURN count(*) AS n").await?;
    assert_eq!(fresh.rows()[0].get::<i64>("n")?, 2);
    Ok(())
}

/// Same scenario through `tx.locy()` — Locy clause bodies read through the
/// pinned snapshot too (C1+C2 threaded into the Locy executor).
#[tokio::test]
async fn mid_tx_flush_does_not_leak_into_locy() -> Result<()> {
    let db = items_db().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let before = tx
        .locy("CREATE RULE items AS MATCH (i:Item) YIELD KEY i")
        .await?;
    assert_eq!(before.derived.get("items").map(Vec::len), Some(1));

    {
        let s2 = db.session();
        let tx2 = s2.tx().await?;
        tx2.execute("CREATE (:Item {name: 'b', value: 2})").await?;
        tx2.commit().await?;
    }
    db.flush().await?;

    let after = tx
        .locy("CREATE RULE items AS MATCH (i:Item) YIELD KEY i")
        .await?;
    assert_eq!(
        after.derived.get("items").map(Vec::len),
        Some(1),
        "post-snapshot L1 row leaked into a Locy evaluation inside the tx"
    );
    tx.rollback();
    Ok(())
}

/// `FOR UPDATE` on a fresh transaction re-pins to the latest committed
/// state — the rebuilt L1 pin must see rows committed+flushed before the
/// lock acquisition.
#[tokio::test]
async fn for_update_repin_sees_latest_l1() -> Result<()> {
    let db = items_db().await?;

    // Begin the tx FIRST, while only the seed row exists.
    let s = db.session();
    let tx = s.tx().await?;

    // A second row commits and reaches L1 after tx begin.
    {
        let s2 = db.session();
        let tx2 = s2.tx().await?;
        tx2.execute("CREATE (:Item {name: 'b', value: 2})").await?;
        tx2.commit().await?;
    }
    db.flush().await?;

    // The fresh tx's first read uses FOR UPDATE → snapshot re-pins to NOW,
    // so the locked read must see the post-begin row.
    let r = tx
        .query("MATCH (i:Item {name: 'b'}) FOR UPDATE RETURN i.value AS v")
        .await?;
    assert_eq!(
        r.rows().len(),
        1,
        "FOR UPDATE re-pin must refresh the L1 pin to the new baseline"
    );
    tx.rollback();
    Ok(())
}

/// With SSI disabled nothing is pinned: an open transaction reads live data
/// (the documented 1.x legacy behavior).
#[tokio::test]
async fn ssi_disabled_keeps_live_reads() -> Result<()> {
    let config = UniConfig {
        ssi_enabled: false,
        ..Default::default()
    };
    let db = Uni::in_memory().config(config).build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx0 = s.tx().await?;
    tx0.execute("CREATE (:Item {name: 'a'})").await?;
    tx0.commit().await?;
    db.flush().await?;

    let tx = s.tx().await?;
    {
        let s2 = db.session();
        let tx2 = s2.tx().await?;
        tx2.execute("CREATE (:Item {name: 'b'})").await?;
        tx2.commit().await?;
    }
    db.flush().await?;

    let n = tx.query("MATCH (i:Item) RETURN count(*) AS n").await?;
    assert_eq!(
        n.rows()[0].get::<i64>("n")?,
        2,
        "ssi_enabled=false must keep live (unpinned) reads"
    );
    tx.rollback();
    Ok(())
}
