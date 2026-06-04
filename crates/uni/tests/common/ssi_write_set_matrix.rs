// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The write-set derivation matrix: every mutation kind enters the write-set and
//! conflicts at commit.
//!
//! Mirror of the read-path matrix for the write side. For each mutation operator
//! we assert: two transactions begun from the same snapshot that both write item
//! X must not both commit — the second aborts on a write-write (or read-write)
//! conflict. This proves `WriteSet::from_l0` captures the item for every mutation
//! shape (SET / REMOVE / DELETE / DETACH DELETE / label change / edge write /
//! MERGE), so none can slip a lost update past OCC.

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

use crate::ssi_support::reopen::DiskHarness;
use crate::ssi_support::schedule::{
    assert_committed, assert_conflict, assert_serialization_conflict,
};

/// `T(id, val)` + edge `R:T->T`, seeded T{x,y}=0 and `(x)-[:R {w: 0}]->(y)`.
async fn ws_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("T")
        .property("id", DataType::String)
        .property("val", DataType::Int)
        .done()
        .edge_type("R", &["T"], &["T"])
        .property("w", DataType::Int)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (x:T {id: 'x', val: 0})-[:R {w: 0}]->(y:T {id: 'y', val: 0})")
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// Two transactions from the same snapshot run `op_a` and `op_b` (both touching
/// the same item). The first commits; the second must abort.
async fn assert_mutations_conflict(op_a: &str, op_b: &str) -> Result<()> {
    let db = ws_db().await?;
    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    let tb = sb.tx().await?;

    ta.execute(op_a).await?;
    tb.execute(op_b).await?;

    assert_committed(ta.commit().await);
    assert_serialization_conflict(tb.commit().await);
    Ok(())
}

// ── Property mutations ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_vs_set() -> Result<()> {
    assert_mutations_conflict(
        "MATCH (n:T {id: 'x'}) SET n.val = 1",
        "MATCH (n:T {id: 'x'}) SET n.val = 2",
    )
    .await
}

/// G7 (FIXED): a label-only mutation enters the write-set and conflicts with a
/// concurrent write to the same vertex. `SET n:Label` now routes to the
/// transaction's `tx_l0` via `L0Buffer::set_vertex_labels`, flagging the vid in
/// `vertex_label_overwrites` so `WriteSet::from_l0` sees the write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_vs_label_add() -> Result<()> {
    assert_mutations_conflict(
        "MATCH (n:T {id: 'x'}) SET n.val = 1",
        "MATCH (n:T {id: 'x'}) SET n:Tagged",
    )
    .await
}

/// G7 (FIXED) companion for `REMOVE n:Label` (same path — see `set_vs_label_add`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_vs_label_remove() -> Result<()> {
    let db = ws_db().await?;
    {
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute("MATCH (n:T {id: 'x'}) SET n:Tagged").await?;
        tx.commit().await?;
    }
    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    let tb = sb.tx().await?;
    ta.execute("MATCH (n:T {id: 'x'}) SET n.val = 1").await?;
    tb.execute("MATCH (n:T {id: 'x'}) REMOVE n:Tagged").await?;
    assert_committed(ta.commit().await);
    assert_serialization_conflict(tb.commit().await);
    Ok(())
}

// ── Deletions ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_vs_detach_delete() -> Result<()> {
    assert_mutations_conflict(
        "MATCH (n:T {id: 'x'}) SET n.val = 1",
        "MATCH (n:T {id: 'x'}) DETACH DELETE n",
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_vs_delete() -> Result<()> {
    assert_mutations_conflict(
        "MATCH (n:T {id: 'x'}) DETACH DELETE n",
        "MATCH (n:T {id: 'x'}) DETACH DELETE n",
    )
    .await
}

// ── Edge mutations ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn edge_set_vs_edge_set() -> Result<()> {
    assert_mutations_conflict(
        "MATCH (:T {id: 'x'})-[r:R]->(:T {id: 'y'}) SET r.w = 1",
        "MATCH (:T {id: 'x'})-[r:R]->(:T {id: 'y'}) SET r.w = 2",
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn edge_set_vs_edge_delete() -> Result<()> {
    assert_mutations_conflict(
        "MATCH (:T {id: 'x'})-[r:R]->(:T {id: 'y'}) SET r.w = 1",
        "MATCH (:T {id: 'x'})-[r:R]->(:T {id: 'y'}) DELETE r",
    )
    .await
}

// ── MERGE serialization (unique key) ─────────────────────────────────────────

/// Two transactions MERGE the same unique key from the same snapshot: exactly
/// one creates the row, the other aborts on the serializable-MERGE constraint
/// check (it then observes the existing row on retry).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_same_unique_key_conflicts() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("U")
        .property("code", DataType::String)
        .done()
        .apply()
        .await?;
    {
        let tx = db.session().tx().await?;
        tx.execute("CREATE CONSTRAINT u_code ON (u:U) ASSERT u.code IS UNIQUE")
            .await?;
        tx.commit().await?;
    }

    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    let tb = sb.tx().await?;
    ta.execute("MERGE (u:U {code: 'k'})").await?;
    tb.execute("MERGE (u:U {code: 'k'})").await?;

    assert_committed(ta.commit().await);
    // tb created a second 'k' under its snapshot; the commit-time unique-key
    // check rejects it (ConstraintConflict — retriable).
    assert_conflict(tb.commit().await);

    let r = db
        .session()
        .query("MATCH (u:U) RETURN count(u) AS c")
        .await?;
    assert_eq!(r.rows()[0].value("c"), Some(&uni_db::Value::Int(1)));
    Ok(())
}

// ── G7 label-mutation persistence + regression guards ────────────────────────

/// `count(*)` of `T` carrying `label`.
async fn count_with_label(db: &Uni, label: &str) -> Result<i64> {
    let r = db
        .session()
        .query(&format!("MATCH (n:{label}) RETURN count(n) AS c"))
        .await?;
    match r.rows()[0].value("c") {
        Some(Value::Int(n)) => Ok(*n),
        other => panic!("expected Int, got {other:?}"),
    }
}

/// A committed `SET n:Label` actually persists: the node becomes matchable by the
/// new label (it must route through the tx and survive merge, not vanish).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_add_persists_after_commit() -> Result<()> {
    let db = ws_db().await?;
    {
        let tx = db.session().tx().await?;
        tx.execute("MATCH (n:T {id: 'x'}) SET n:Tagged").await?;
        tx.commit().await?;
    }
    assert_eq!(count_with_label(&db, "Tagged").await?, 1, "label add lost");
    // The original label is retained (replace used the full resolved set).
    assert_eq!(
        count_with_label(&db, "T").await?,
        2,
        "original label dropped"
    );
    Ok(())
}

/// A committed `REMOVE n:Label` persists: the node is no longer matchable by the
/// removed label, but keeps its other labels.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_remove_persists_after_commit() -> Result<()> {
    let db = ws_db().await?;
    {
        let tx = db.session().tx().await?;
        tx.execute("MATCH (n:T {id: 'x'}) SET n:Tagged").await?;
        tx.commit().await?;
    }
    {
        let tx = db.session().tx().await?;
        tx.execute("MATCH (n:T {id: 'x'}) REMOVE n:Tagged").await?;
        tx.commit().await?;
    }
    assert_eq!(
        count_with_label(&db, "Tagged").await?,
        0,
        "label not removed"
    );
    assert_eq!(
        count_with_label(&db, "T").await?,
        2,
        "removal wiped other labels"
    );
    Ok(())
}

/// THE key regression guard: a property-only `SET` must NOT touch labels. The
/// merge "replace" path is gated on `vertex_label_overwrites`, which a property
/// write never sets — so x keeps its `T` label.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn property_set_does_not_wipe_labels() -> Result<()> {
    let db = ws_db().await?;
    {
        let tx = db.session().tx().await?;
        tx.execute("MATCH (n:T {id: 'x'}) SET n.val = 42").await?;
        tx.commit().await?;
    }
    // Both x and y still carry T (a property write must not drop labels).
    assert_eq!(
        count_with_label(&db, "T").await?,
        2,
        "property SET wiped labels"
    );
    Ok(())
}

/// G7 durability: a committed label change survives close + reopen (it is now
/// written to the WAL as `Mutation::SetVertexLabels` and replayed on recovery).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_change_survives_reopen() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        db.schema()
            .label("T")
            .property("id", DataType::String)
            .property("val", DataType::Int)
            .done()
            .apply()
            .await?;
        {
            let tx = db.session().tx().await?;
            tx.execute("CREATE (:T {id: 'x', val: 0})").await?;
            tx.commit().await?;
        }
        {
            let tx = db.session().tx().await?;
            tx.execute("MATCH (n:T {id: 'x'}) SET n:Tagged").await?;
            tx.commit().await?;
        }
        db.flush().await?;
    }
    let db = h.open().await?;
    assert_eq!(
        count_with_label(&db, "Tagged").await?,
        1,
        "label change did not survive reopen (not WAL-durable)"
    );
    Ok(())
}
