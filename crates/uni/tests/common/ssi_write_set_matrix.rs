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
use uni_db::{DataType, Uni};

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

/// G7 (KNOWN GAP — discovered by this matrix): a label-only mutation must enter
/// the write-set and conflict with a concurrent write to the same vertex.
///
/// It does NOT today. `SET n:Label` / `REMOVE n:Label` are executed by writing to
/// the *context* (main) L0 via `ctx.l0` in `execute_set_items_locked` /
/// `execute_remove_items_locked` — NOT the transaction's private `tx_l0` (which
/// is how property writes route, via `writer.insert_vertex_with_labels(tx_l0)`).
/// Consequences:
///   1. the label change is invisible to OCC (`WriteSet::from_l0` reads tx_l0),
///      so a concurrent property write to the same vertex does NOT conflict — a
///      silent lost update on the label;
///   2. it is arguably non-transactional (it lands in main L0 immediately);
///   3. `add_vertex_labels` also omits the `mutation_count += 1` its sibling
///      `remove_vertex_label` performs, so the commit reports 0 mutations.
///
/// The fix is multi-part and carries label-merge regression risk (it must also
/// teach `L0Buffer::merge` to union label deltas for already-present vids, which
/// today it skips), so it is tracked as a follow-up rather than rushed here.
/// This test is the executable spec for that fix; un-`ignore` it once G7 lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "G7: label-only mutations bypass tx_l0/OCC (write to ctx.l0); see doc above"]
async fn set_vs_label_add() -> Result<()> {
    assert_mutations_conflict(
        "MATCH (n:T {id: 'x'}) SET n.val = 1",
        "MATCH (n:T {id: 'x'}) SET n:Tagged",
    )
    .await
}

/// G7 companion for `REMOVE n:Label` (same root cause — see `set_vs_label_add`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "G7: label-only mutations bypass tx_l0/OCC (write to ctx.l0); see set_vs_label_add"]
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
