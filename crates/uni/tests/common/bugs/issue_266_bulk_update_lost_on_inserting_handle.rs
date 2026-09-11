// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #266 — a `SET` against a `BulkWriter`-inserted vertex is silently
//! discarded on the handle that performed the insert.
//!
//! The transaction commits successfully, nothing is raised, and the value is
//! unchanged — through Cypher and through Locy, and it stays wrong across a
//! reopen. Dropping the handle and reopening *before* the update makes the
//! identical `SET` work.
//!
//! # It is not a lost write; the `MATCH` matched nothing
//!
//! The obvious theory is MVCC: bulk rows land at some version, the `SET` writes
//! a lower one, and dedup keeps the bulk row. That theory is wrong, and it
//! matters because it points at `merge_lance_and_l0`, which is correct — it
//! concatenates L0 after Lance and breaks version ties in L0's favour.
//!
//! What actually happens is an off-by-one on the *read* side:
//!
//! | step | value |
//! |---|---|
//! | fresh database, no manifest | `L0Buffer::current_version = 0` |
//! | `BulkWriter` writes rows straight to Lance | `_version = 1`, hard-coded |
//! | the bulk commit advances the counter | it does not — still `0` |
//! | `session.tx()` pins L1 reads (SSI is on by default) | `_version <= 0` |
//!
//! `1 <= 0` is false, so the label scan inside the transaction returns **zero
//! rows**. `MATCH … SET` updates nothing and commits cleanly. The value was
//! never written, so no version was ever assigned to it.
//!
//! Every symptom follows from that one number:
//!
//! * **A reopen fixes it** — the manifest's high-water mark is `0`, and reopen
//!   starts at `hwm + 1`, so the pin becomes `_version <= 1` and the rows are
//!   visible.
//! * **A fresh `session()` does not** — the counter lives on the `Writer`, not
//!   the session.
//! * **`CREATE`d vertices are fine** — they live in L0, which the transaction
//!   pins but does not version-filter, and they also push the counter past `1`,
//!   which is why a `CREATE` before the `SET` masks the bug entirely.
//! * **`flush()` does not help** — it publishes `version_high_water_mark =
//!   current_version`, which is still `0`.
//! * **Row count, index presence and statement form are irrelevant** — the
//!   predicate is applied at the scan, below all of them.
//!
//! # A second defect with no test behind it
//!
//! Bulk rows were stamped with a literal `1` rather than a version drawn from
//! the live counter, so loading into a database whose `current_version` had
//! already passed `1` wrote rows that read as *older* than rows already stored.
//! Drawing the version from the counter fixes that too.
//!
//! It is **not** pinned here, and the reason is worth recording: bulk inserts
//! allocate fresh vids, so a bulk row never contends with an existing row for
//! the same `_vid`, and MVCC dedup is per-vid. `a_bulk_load_into_a_written_
//! database_is_visible` below was written for this and passes **with and
//! without** the fix — it is a control that the non-empty-database path works,
//! not a witness for the stamping. Constructing a real witness needs a vid
//! collision, which this API does not offer.

// Rust guideline compliant

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

const ROWS: usize = 50;

/// A database with `ROWS` `Entity` vertices inserted through `BulkWriter`,
/// every one of them `blocked = false`.
async fn bulk_loaded(dir: &std::path::Path) -> Result<Uni> {
    let db = Uni::open(dir.to_str().unwrap()).build().await?;
    db.schema()
        .label("Entity")
        .property("uid", DataType::String)
        .property("blocked", DataType::Bool)
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let rows: Vec<HashMap<String, Value>> = (0..ROWS)
        .map(|i| {
            let mut p = HashMap::new();
            p.insert("uid".to_string(), Value::String(format!("e{i}")));
            p.insert("blocked".to_string(), Value::Bool(false));
            p
        })
        .collect();
    bulk.insert_vertices("Entity", rows).await?;
    bulk.commit().await?;
    tx.commit().await?;
    Ok(db)
}

/// `SET e.blocked = true` on `e0`, then read it back outside a transaction.
async fn set_and_read(db: &Uni) -> Result<Value> {
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (e:Entity {uid: 'e0'}) SET e.blocked = true")
        .await?;
    tx.commit().await?;

    let r = session
        .query("MATCH (e:Entity) WHERE e.uid = 'e0' RETURN e.blocked AS b")
        .await?;
    Ok(r.rows()
        .first()
        .map(|row| row.values()[0].clone())
        .unwrap_or(Value::Null))
}

/// The defect: the update must land on the handle that did the bulk insert.
#[tokio::test]
async fn a_bulk_inserted_vertex_can_be_updated_on_the_inserting_handle() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = bulk_loaded(dir.path()).await?;

    let got = set_and_read(&db).await?;
    assert_eq!(
        got,
        Value::Bool(true),
        "the SET committed without error and the value is still {got:?}. The \
         transaction pins L1 reads to `_version <= started_at_version`, so if \
         the bulk rows carry a version the inserting handle's counter never \
         reached, the MATCH sees nothing and the write is silently dropped."
    );
    Ok(())
}

/// The control that makes the assertion above mean something.
///
/// If the same `SET` failed after a reopen too, the fixture would simply be
/// wrong — the vertex missing, the predicate not matching — and the test above
/// would be reporting on that rather than on the defect. A reopen re-reads the
/// high-water mark from the manifest, which is what made this work before.
#[tokio::test]
async fn the_same_update_works_after_a_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    {
        let db = bulk_loaded(dir.path()).await?;
        db.shutdown().await?;
    }
    let reopened = Uni::open(dir.path().to_str().unwrap()).build().await?;

    assert_eq!(
        set_and_read(&reopened).await?,
        Value::Bool(true),
        "control: the update must work after a reopen, or the fixture is broken \
         rather than the inserting handle"
    );
    Ok(())
}

/// A `CREATE`d vertex updates on the same handle — the contrast that localises
/// the defect to rows that came from `BulkWriter`.
#[tokio::test]
async fn an_ordinary_created_vertex_updates_on_the_same_handle() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Entity")
        .property("uid", DataType::String)
        .property("blocked", DataType::Bool)
        .done()
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Entity {uid: 'e0', blocked: false})")
        .await?;
    tx.commit().await?;

    assert_eq!(
        set_and_read(&db).await?,
        Value::Bool(true),
        "contrast: an ordinary CREATE goes through L0, which a transaction pins \
         but does not version-filter, so this arm was never affected"
    );
    Ok(())
}

/// A control: bulk-loading into an already-written database works, and the row
/// is updatable on the same handle.
///
/// This exercises the fix with the counter starting well above zero, where the
/// reserved version and the transaction pin are both non-trivial — the reported
/// reproduction only ever starts from an empty database.
///
/// **Measured non-discriminating for the version stamping**: it passes with and
/// without the fix, because bulk inserts allocate fresh vids and so never
/// contend with an existing row for the same `_vid`. Read it as a guard on the
/// non-empty path, not as evidence about `_version`.
#[tokio::test]
async fn a_bulk_load_into_a_written_database_is_visible() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Entity")
        .property("uid", DataType::String)
        .property("blocked", DataType::Bool)
        .done()
        .apply()
        .await?;

    // Push `current_version` well past 1 before the bulk load.
    let session = db.session();
    for i in 0..5 {
        let tx = session.tx().await?;
        tx.execute(&format!(
            "CREATE (:Entity {{uid: 'pre{i}', blocked: false}})"
        ))
        .await?;
        tx.commit().await?;
    }

    let tx = session.tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let mut p = HashMap::new();
    p.insert("uid".to_string(), Value::String("bulk0".to_string()));
    p.insert("blocked".to_string(), Value::Bool(false));
    bulk.insert_vertices("Entity", vec![p]).await?;
    bulk.commit().await?;
    tx.commit().await?;

    let r = session
        .query("MATCH (e:Entity) WHERE e.uid = 'bulk0' RETURN e.uid AS u")
        .await?;
    assert_eq!(
        r.rows().len(),
        1,
        "a vertex bulk-loaded into an already-written database must be \
         readable; a row stamped with a fixed version below the live counter \
         reads as superseded"
    );

    let tx = session.tx().await?;
    tx.execute("MATCH (e:Entity {uid: 'bulk0'}) SET e.blocked = true")
        .await?;
    tx.commit().await?;
    let r = session
        .query("MATCH (e:Entity) WHERE e.uid = 'bulk0' RETURN e.blocked AS b")
        .await?;
    assert_eq!(
        r.rows().first().map(|row| row.values()[0].clone()),
        Some(Value::Bool(true)),
        "and it must be updatable on the same handle"
    );
    Ok(())
}
