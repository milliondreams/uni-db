// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! #249: declaring a new property on a label that already has flushed data
//! leaves the Lance dataset unchanged, so the next write to that label fails.
//!
//! ```text
//! Internal error: Write to 'vertices_A' (Append) failed: ...
//! ```
//!
//! `.apply()` records the property in uni's schema and never reaches storage;
//! `get_arrow_schema` then emits a Field for every declared property, so the
//! flush batch carries a column the existing dataset has never had.
//!
//! **Fixed.** `ensure_table_accepts` (`uni-store/src/storage/schema_evolution.rs`)
//! widens the stored schema before the write, via Lance `add_columns` with
//! `NewColumnTransform::AllNulls` — metadata-only, no data rewrite.
//!
//! These tests previously pinned the *broken* behaviour (each asserted the
//! failure it measured) so that a fix would turn them red. It did; they now
//! assert the fixed behaviour instead. Each checks a **value**, not just that
//! the call returned `Ok`: an all-nulls widening makes every one of these
//! operations succeed while returning NULL, so an `is_ok()`-only assertion
//! would pass without proving the data survived.

// Rust guideline compliant

use std::collections::HashMap;

use tempfile::TempDir;
use uni_db::{DataType, Result, Uni, Value};

fn rows(n: i64) -> Vec<HashMap<String, Value>> {
    (0..n)
        .map(|i| {
            let mut m = HashMap::new();
            m.insert("name".to_string(), Value::String(format!("name-{i}")));
            m.insert("num".to_string(), Value::Int(i));
            m
        })
        .collect()
}

/// Step 1 of the issue's repro: declare `A`, bulk-load rows, flush.
async fn seeded(dir: &TempDir) -> Result<Uni> {
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("A")
        .property("name", DataType::String)
        .property("num", DataType::Int)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.bulk_insert_vertices_labeled(&["A"], rows(500)).await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// (a) `.apply()` of the late property, then a CREATE that uses it.
#[tokio::test]
async fn issue_249_create_after_late_property() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir).await?;

    // (a) does the schema change itself succeed?
    let applied = db
        .schema()
        .label("A")
        .property_nullable("added_later", DataType::String)
        .done()
        .apply()
        .await;
    eprintln!("ISSUE249 apply() -> {applied:?}");
    assert!(applied.is_ok(), "apply() of the late property");
    let flushed = db.flush().await;
    eprintln!("ISSUE249 flush-after-apply -> {flushed:?}");

    let s = db.session();
    let tx = s.tx().await?;
    let created = tx
        .execute("CREATE (:A {name: 'late', num: 99999, added_later: 'x'})")
        .await;
    eprintln!("ISSUE249 CREATE -> {created:?}");
    let committed = if created.is_ok() {
        let c = tx.commit().await.map(|_| ());
        eprintln!("ISSUE249 commit -> {c:?}");
        c
    } else {
        {
            tx.rollback();
            Ok(())
        }
    };
    let post_flush = db.flush().await;
    eprintln!("ISSUE249 flush-after-create -> {post_flush:?}");

    // (e) do reads on the label still work?
    let read = s.query("MATCH (n:A) RETURN count(n) AS c").await;
    eprintln!(
        "ISSUE249 read -> {:?}",
        read.as_ref().map(|r| r.rows().len())
    );
    if let Ok(r) = &read {
        eprintln!(
            "ISSUE249 read count = {:?}",
            r.rows().first().and_then(|x| x.get::<i64>("c").ok())
        );
    }
    let read_new = s
        .query("MATCH (n:A) RETURN n.added_later AS v LIMIT 3")
        .await;
    eprintln!("ISSUE249 read-new-prop ok? {}", read_new.is_ok());
    if let Err(e) = &read_new {
        eprintln!("ISSUE249 read-new-prop err: {e}");
    }

    created.expect("CREATE after a late property");
    committed.expect("commit after a late property");
    post_flush.expect("flush after a late property -- the #249 wedge");
    read_new.expect("projecting the new property must not fail on a type mismatch");
    let read = read.expect("count query on the widened label");
    assert_eq!(
        read.rows().first().and_then(|x| x.get::<i64>("c").ok()),
        Some(501),
        "500 seeded rows plus the one created after the property was declared"
    );

    // The written value must survive. `add_columns(AllNulls)` makes every call
    // above succeed while leaving the column NULL, so this is the assertion
    // that distinguishes a real fix from a silent one.
    let v = s
        .query("MATCH (n:A) WHERE n.num = 99999 RETURN n.added_later AS v")
        .await
        .expect("project the late property");
    assert_eq!(
        v.rows()
            .first()
            .and_then(|r| r.get::<String>("v").ok())
            .as_deref(),
        Some("x"),
        "the late property must round-trip through the widened column"
    );
    // A row written before the property existed reads NULL, not an error.
    let old = s
        .query("MATCH (n:A) WHERE n.num = 0 RETURN n.added_later AS v")
        .await
        .expect("project the late property on a pre-existing row");
    assert!(
        old.rows()
            .first()
            .map(|r| r.get::<String>("v").is_err())
            .unwrap_or(false),
        "a row predating the property must read NULL"
    );

    db.shutdown().await?;
    Ok(())
}

/// The bulk-API arm of the same repro.
#[tokio::test]
async fn issue_249_bulk_insert_after_late_property() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir).await?;
    db.schema()
        .label("A")
        .property_nullable("added_later", DataType::String)
        .done()
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    let mut m = HashMap::new();
    m.insert("name".to_string(), Value::String("late".into()));
    m.insert("num".to_string(), Value::Int(99999));
    m.insert("added_later".to_string(), Value::String("x".into()));
    let inserted = tx.bulk_insert_vertices_labeled(&["A"], vec![m]).await;
    eprintln!("ISSUE249 bulk insert -> {inserted:?}");
    let committed = if inserted.is_ok() {
        tx.commit().await.map(|_| ())
    } else {
        {
            tx.rollback();
            Ok(())
        }
    };
    eprintln!("ISSUE249 bulk commit -> {committed:?}");
    let post_flush = db.flush().await;
    eprintln!("ISSUE249 bulk flush -> {post_flush:?}");
    inserted.expect("bulk insert after a late property");
    committed.expect("bulk commit after a late property");
    post_flush.expect("bulk flush after a late property -- the #249 wedge");
    let v = s
        .query("MATCH (n:A) WHERE n.num = 99999 RETURN n.added_later AS v")
        .await
        .expect("project the late property after a bulk insert");
    assert_eq!(
        v.rows()
            .first()
            .and_then(|r| r.get::<String>("v").ok())
            .as_deref(),
        Some("x"),
        "bulk arm: the late property must round-trip"
    );
    db.shutdown().await?;
    Ok(())
}

/// (b) does the wedge survive a close and reopen?
#[tokio::test]
async fn issue_249_survives_reopen() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir).await?;
    db.schema()
        .label("A")
        .property_nullable("added_later", DataType::String)
        .done()
        .apply()
        .await?;
    db.shutdown().await?;

    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    let s = db.session();
    let tx = s.tx().await?;
    let created = tx
        .execute("CREATE (:A {name: 'late', num: 1, added_later: 'x'})")
        .await;
    eprintln!("ISSUE249 reopen CREATE -> {created:?}");
    let committed = if created.is_ok() {
        tx.commit().await.map(|_| ())
    } else {
        {
            tx.rollback();
            Ok(())
        }
    };
    eprintln!("ISSUE249 reopen commit -> {committed:?}");
    let post_flush = db.flush().await;
    eprintln!("ISSUE249 reopen flush -> {post_flush:?}");
    let read = s.query("MATCH (n:A) RETURN count(n) AS c").await;
    eprintln!("ISSUE249 reopen read ok? {}", read.is_ok());

    created.expect("CREATE after reopen");
    committed.expect("commit after reopen");
    post_flush.expect("flush after reopen -- the wedge used to survive restart");
    read.expect("read after reopen");
    // The widening is a Lance manifest change, so it must still be in effect
    // after a close/reopen cycle -- the original bug's defining symptom was
    // that the wedge survived exactly this.
    // Match on `name`, not `num`: the seed writes num = 0..499, so `num = 1`
    // also matches a pre-existing row whose `added_later` is legitimately NULL.
    let v = s
        .query("MATCH (n:A) WHERE n.name = 'late' RETURN n.added_later AS v")
        .await
        .expect("project the late property after reopen");
    assert_eq!(
        v.rows()
            .first()
            .and_then(|r| r.get::<String>("v").ok())
            .as_deref(),
        Some("x"),
        "reopen arm: the late property must round-trip"
    );
    db.shutdown().await?;
    Ok(())
}

/// (c) a SET that does not touch the new property, and
/// (d) a SET that does — plus a CREATE that omits the new property entirely.
#[tokio::test]
async fn issue_249_partial_writes() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir).await?;
    db.schema()
        .label("A")
        .property_nullable("added_later", DataType::String)
        .done()
        .apply()
        .await?;
    let s = db.session();

    // (c) SET on an old column only.
    let tx = s.tx().await?;
    let r = tx
        .execute("MATCH (n:A) WHERE n.num = 0 SET n.name = 'renamed'")
        .await;
    eprintln!("ISSUE249 SET-old -> {r:?}");
    let c = if r.is_ok() {
        tx.commit().await.map(|_| ())
    } else {
        {
            tx.rollback();
            Ok(())
        }
    };
    eprintln!("ISSUE249 SET-old commit -> {c:?}");
    let f = db.flush().await;
    eprintln!("ISSUE249 SET-old flush -> {f:?}");
    eprintln!(
        "ISSUE249 SUMMARY set_old_ok = {}",
        r.is_ok() && c.is_ok() && f.is_ok()
    );

    // (d) SET on the new column.
    let tx = s.tx().await?;
    let r2 = tx
        .execute("MATCH (n:A) WHERE n.num = 1 SET n.added_later = 'y'")
        .await;
    eprintln!("ISSUE249 SET-new -> {r2:?}");
    let c2 = if r2.is_ok() {
        tx.commit().await.map(|_| ())
    } else {
        {
            tx.rollback();
            Ok(())
        }
    };
    eprintln!("ISSUE249 SET-new commit -> {c2:?}");
    let f2 = db.flush().await;
    eprintln!("ISSUE249 SET-new flush -> {f2:?}");
    eprintln!(
        "ISSUE249 SUMMARY set_new_ok = {}",
        r2.is_ok() && c2.is_ok() && f2.is_ok()
    );

    // CREATE that omits the new property entirely.
    let tx = s.tx().await?;
    let r3 = tx.execute("CREATE (:A {name: 'plain', num: 4242})").await;
    eprintln!("ISSUE249 CREATE-without-new-prop -> {r3:?}");
    let c3 = if r3.is_ok() {
        tx.commit().await.map(|_| ())
    } else {
        {
            tx.rollback();
            Ok(())
        }
    };
    let f3 = db.flush().await;
    eprintln!("ISSUE249 CREATE-without commit -> {c3:?} flush -> {f3:?}");
    eprintln!(
        "ISSUE249 SUMMARY create_without_new_prop_ok = {}",
        r3.is_ok() && c3.is_ok() && f3.is_ok()
    );

    // (e) reads.
    let read = s.query("MATCH (n:A) RETURN count(n) AS c").await;
    eprintln!("ISSUE249 SUMMARY read_ok = {}", read.is_ok());
    if let Ok(r) = &read {
        eprintln!(
            "ISSUE249 read count = {:?}",
            r.rows().first().and_then(|x| x.get::<i64>("c").ok())
        );
    } else if let Err(e) = &read {
        eprintln!("ISSUE249 read err: {e}");
    }

    // (c) a SET touching only pre-existing columns. L0 re-materialises the
    //     whole declared property set for the row, so this batch carries the
    //     new column too and used to be rejected at flush.
    r.expect("(c) SET on an old column");
    c.expect("(c) commit");
    f.expect("(c) flush -- this is where the wedge used to bite");
    // (d) a SET touching the new column. This one goes through the
    //     `merge_insert` partial-batch path, which is a *different* write
    //     helper from (c)'s append -- both need the reconcile.
    r2.expect("(d) SET on the new column");
    c2.expect("(d) commit");
    f2.expect("(d) flush");
    // A CREATE that never mentions the new property.
    r3.expect("CREATE omitting the new property");
    c3.expect("CREATE-omitting commit");
    f3.expect("CREATE-omitting flush");
    read.expect("(e) reads on the label");

    // Values, not just success. (d) wrote through the partial path; if the
    // reconcile widened the table but the write silently dropped the column,
    // every call above would still be `Ok`.
    let v = s
        .query("MATCH (n:A) WHERE n.num = 1 RETURN n.added_later AS v")
        .await
        .expect("project the new property after a partial SET");
    assert_eq!(
        v.rows()
            .first()
            .and_then(|r| r.get::<String>("v").ok())
            .as_deref(),
        Some("y"),
        "(d) the SET on the new column must be readable"
    );
    // (c)'s row was renamed and must not have lost its old columns.
    let n = s
        .query("MATCH (n:A) WHERE n.num = 0 RETURN n.name AS name")
        .await
        .expect("project an old property after the widening");
    assert_eq!(
        n.rows()
            .first()
            .and_then(|r| r.get::<String>("name").ok())
            .as_deref(),
        Some("renamed"),
        "(c) widening must not disturb pre-existing columns"
    );

    db.shutdown().await?;
    Ok(())
}

/// Promoting a *schemaless* property to a declared one must not lose its
/// values.
///
/// An undeclared property lives in the `overflow_json` blob. The projected
/// read path prefers a typed column when one exists and falls back to the blob
/// only when it does not — so materialising an all-NULL typed column would make
/// `RETURN n.tag` answer NULL while `RETURN properties(n)`, which coalesces
/// through the blob, still answered `"gold"`. Two read paths, two answers, no
/// error. Measured before the backfill existed; both are asserted here.
#[tokio::test]
async fn issue_249_promoting_a_schemaless_property_keeps_its_values() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("P")
        .property("name", DataType::String)
        .done()
        .apply()
        .await?;

    // `tag` is NOT declared, so it lands in overflow_json.
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:P {name: 'a', tag: 'gold'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let before = s
        .query("MATCH (n:P) WHERE n.name = 'a' RETURN n.tag AS v")
        .await?;
    assert_eq!(
        before
            .rows()
            .first()
            .and_then(|r| r.get::<String>("v").ok())
            .as_deref(),
        Some("gold"),
        "precondition: the schemaless value is readable before declaring it"
    );

    db.schema()
        .label("P")
        .property_nullable("tag", DataType::String)
        .done()
        .apply()
        .await?;

    // Projected read: the typed column must now hold the promoted value.
    let after = s
        .query("MATCH (n:P) WHERE n.name = 'a' RETURN n.tag AS v")
        .await?;
    assert_eq!(
        after
            .rows()
            .first()
            .and_then(|r| r.get::<String>("v").ok())
            .as_deref(),
        Some("gold"),
        "declaring a property must not blank the values it already had"
    );

    // …and the map-valued path must agree with it.
    let props = s
        .query("MATCH (n:P) WHERE n.name = 'a' RETURN properties(n) AS p")
        .await?;
    let rendered = format!("{:?}", props.rows().first());
    assert!(
        rendered.contains("gold"),
        "properties(n) disagrees with the projected read: {rendered}"
    );

    db.shutdown().await?;
    Ok(())
}

/// After promotion, setting the property to NULL must make it stay NULL.
///
/// The backfill strips the promoted key from `overflow_json`. Without that,
/// the blob keeps a shadow copy, and `build_all_props_column_for_schema_scan`
/// — which falls back to the blob whenever the typed value is NULL — hands the
/// deleted value straight back.
#[tokio::test]
async fn issue_249_setting_a_promoted_property_to_null_does_not_resurrect_it() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("P")
        .property("name", DataType::String)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:P {name: 'a', tag: 'gold'})").await?;
    tx.commit().await?;
    db.flush().await?;

    db.schema()
        .label("P")
        .property_nullable("tag", DataType::String)
        .done()
        .apply()
        .await?;

    let tx = s.tx().await?;
    tx.execute("MATCH (n:P) WHERE n.name = 'a' SET n.tag = NULL")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let after = s
        .query("MATCH (n:P) WHERE n.name = 'a' RETURN n.tag AS v")
        .await?;
    assert!(
        after
            .rows()
            .first()
            .map(|r| r.get::<String>("v").is_err())
            .unwrap_or(true),
        "projected read resurrected the cleared value"
    );

    let props = s
        .query("MATCH (n:P) WHERE n.name = 'a' RETURN properties(n) AS p")
        .await?;
    let rendered = format!("{:?}", props.rows().first());
    assert!(
        !rendered.contains("gold"),
        "properties(n) resurrected the cleared value from the stale blob: {rendered}"
    );

    db.shutdown().await?;
    Ok(())
}

/// The mirror of #249: **dropping** a `NOT NULL` property wedges the table too.
///
/// Lance tolerates a column missing from an appended batch only when the
/// *stored* field is nullable (`allow_missing_if_nullable && expected.nullable`),
/// and `property()` defaults to `NOT NULL` — so the original `create_table`
/// wrote a non-nullable field. Dropping it from the catalog makes every
/// subsequent flush fail with `missing=[doomed], unexpected=[]`.
///
/// The write-path reconcile handles this by relaxing the stored column to
/// nullable, which is metadata-only just as the add is.
#[tokio::test]
async fn issue_249_dropping_a_not_null_property_does_not_wedge() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("D")
        .property("name", DataType::String)
        .property("doomed", DataType::String)
        .done()
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:D {name: 'a', doomed: 'x'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = s.tx().await?;
    tx.execute("ALTER LABEL D DROP PROPERTY doomed").await?;
    tx.commit().await?;

    // A write after the drop builds a batch that no longer carries `doomed`.
    let tx = s.tx().await?;
    tx.execute("CREATE (:D {name: 'b'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let rows = s.query("MATCH (n:D) RETURN count(n) AS c").await?;
    assert_eq!(
        rows.rows().first().and_then(|r| r.get::<i64>("c").ok()),
        Some(2),
        "both rows must be present after dropping a NOT NULL property"
    );

    db.shutdown().await?;
    Ok(())
}

/// Declaring a `NOT NULL` property on a populated label.
///
/// `property()` defaults to `nullable: false`, and it is the most common
/// builder call — rejecting it would make #249's own reproduction fail. The
/// pre-existing rows have no value for the new column and cannot satisfy
/// `NOT NULL`, so the declaration is recorded as nullable instead, keeping the
/// catalog honest about what the data can support. (`NOT NULL` is enforced
/// forward, at write time, either way.)
#[tokio::test]
async fn issue_249_not_null_property_on_a_populated_label_is_recorded_nullable() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir).await?;

    // The default, NOT NULL builder call — must be accepted.
    db.schema()
        .label("A")
        .property("added_not_null", DataType::String)
        .done()
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:A {name: 'z', num: 424242, added_not_null: 'v'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let v = s
        .query("MATCH (n:A) WHERE n.num = 424242 RETURN n.added_not_null AS v")
        .await?;
    assert_eq!(
        v.rows()
            .first()
            .and_then(|r| r.get::<String>("v").ok())
            .as_deref(),
        Some("v")
    );

    // A pre-existing row simply reads NULL rather than erroring.
    let old = s
        .query("MATCH (n:A) WHERE n.num = 0 RETURN n.added_not_null AS v")
        .await?;
    assert!(
        old.rows()
            .first()
            .map(|r| r.get::<String>("v").is_err())
            .unwrap_or(false),
        "a row predating the property must read NULL"
    );

    db.shutdown().await?;
    Ok(())
}
