// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression tests for `SchemaBuilder::apply()` idempotency
//! (issue rustic-ai/uni-db#63).
//!
//! `apply()` is the documented "register-this-schema-on-every-KB-open"
//! pattern. Re-applying the same schema must NOT bloat `schema.indexes`.
//! Pre-fix, three layers conspired to grow the vector super-linearly per
//! apply (≈doubling), which inflated production `catalog/schema.json`
//! files to tens of thousands of duplicate entries and made KB-open
//! take minutes (the synchronous Lance rebuild loop iterated through
//! every duplicate).
//!
//! Coverage:
//! - `add_index_appends_duplicate_on_repeated_apply` — minimal repro.
//! - `repeated_apply_grows_indexes_linearly` — 10 applies with a wall-time
//!   bound (the issue's repro took 15.7 s for the same case).
//! - `duplicates_persist_across_reopen_on_disk` — bloat survives reopens
//!   pre-fix; post-fix the disk catalog stays clean.
//! - `load_dedups_legacy_bloated_catalog` — self-heal pass for catalogs
//!   that were bloated before the fix landed.

use std::time::{Duration, Instant};

use uni_db::{DataType, IndexType, ScalarType, Uni};

async fn apply_canonical_schema(db: &Uni) {
    db.schema()
        .label("Foo")
        .property("name", DataType::String)
        .index("name", IndexType::Scalar(ScalarType::Hash))
        .done()
        .apply()
        .await
        .unwrap();
}

#[tokio::test]
async fn add_index_appends_duplicate_on_repeated_apply() {
    let db = Uni::in_memory().build().await.unwrap();

    apply_canonical_schema(&db).await;
    assert_eq!(db.schema_manager().schema().indexes.len(), 1);

    apply_canonical_schema(&db).await;
    assert_eq!(
        db.schema_manager().schema().indexes.len(),
        1,
        "second apply must be idempotent — pre-fix len was 2"
    );

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn repeated_apply_grows_indexes_linearly() {
    let db = Uni::in_memory().build().await.unwrap();

    let started = Instant::now();
    for _ in 0..10 {
        apply_canonical_schema(&db).await;
    }
    let elapsed = started.elapsed();

    assert_eq!(
        db.schema_manager().schema().indexes.len(),
        1,
        "10 applies must collapse to 1 entry — pre-fix len was 2046"
    );
    // Pre-fix took 15.7 s for the same workload (Lance rebuild walks the
    // bloated indexes list once per apply). Post-fix should finish in
    // well under a second on any reasonable machine.
    assert!(
        elapsed < Duration::from_secs(2),
        "10 applies should finish in < 2 s, took {elapsed:?}"
    );

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicates_persist_across_reopen_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();

    {
        let db = Uni::open(&path).build().await.unwrap();
        apply_canonical_schema(&db).await;
        assert_eq!(db.schema_manager().schema().indexes.len(), 1);
        db.shutdown().await.unwrap();
    }

    {
        let db = Uni::open(&path).build().await.unwrap();
        // Schema reloaded from disk:
        assert_eq!(
            db.schema_manager().schema().indexes.len(),
            1,
            "reopened schema must still have 1 index"
        );
        apply_canonical_schema(&db).await;
        assert_eq!(
            db.schema_manager().schema().indexes.len(),
            1,
            "re-apply after reopen must stay idempotent"
        );
        db.shutdown().await.unwrap();
    }
}

/// Self-heal: a catalog that was bloated by the pre-fix `add_index`
/// (potentially tens of thousands of duplicate entries with the same name)
/// should be silently collapsed to one entry per name on next open.
#[tokio::test]
async fn load_dedups_legacy_bloated_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();

    // First, produce a real catalog with one index by going through the
    // normal apply() path. We then hand-rewrite the on-disk schema.json to
    // simulate the pre-fix bloat (50 duplicates of the single entry).
    {
        let db = Uni::open(&path).build().await.unwrap();
        apply_canonical_schema(&db).await;
        assert_eq!(db.schema_manager().schema().indexes.len(), 1);
        db.shutdown().await.unwrap();
    }

    let schema_path = dir.path().join("catalog").join("schema.json");
    let raw = std::fs::read_to_string(&schema_path).expect("schema.json must exist");
    let mut json: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let single = json["indexes"][0].clone();
    let dup_count = 50;
    let bloated: Vec<serde_json::Value> = std::iter::repeat_n(single, dup_count).collect();
    json["indexes"] = serde_json::Value::Array(bloated);
    std::fs::write(&schema_path, serde_json::to_string_pretty(&json).unwrap()).unwrap();

    {
        let db = Uni::open(&path).build().await.unwrap();
        let len = db.schema_manager().schema().indexes.len();
        assert_eq!(
            len, 1,
            "load() must dedup legacy bloated catalog from {dup_count} entries down to 1"
        );
        db.shutdown().await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// Issue #286 — re-applying an unchanged schema once the entity holds rows.
// ---------------------------------------------------------------------------

/// The suite above re-applies schemas but never writes a row, which is why
/// #286 shipped: the guard it trips only runs when the entity is populated.
///
/// `SchemaBuilder::property` defaults to NOT NULL, and a NOT NULL property
/// declared on a populated entity is deliberately recorded as nullable, since
/// existing rows have no value for it. That adjustment used to run on every
/// declaration, including for a property already in the catalog — so a label
/// declared while empty recorded NOT NULL, and the next identical `apply()`
/// rewrote the declaration to nullable and collided with the recorded value.
/// A store could not be reopened by an app that re-registers its schema, which
/// is the documented pattern.
///
/// The empty-label control is what isolates the row count as the trigger
/// rather than re-declaration itself.
async fn declare_person(db: &Uni) -> Result<(), uni_db::UniError> {
    db.schema()
        .label("Person")
        .property("name", DataType::String) // NOT NULL by default
        .done()
        .apply()
        .await
        .map(|_| ())
}

#[tokio::test]
async fn reapplying_an_unchanged_schema_after_writing_rows_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = dir.path().join("store");

    {
        let db = Uni::open(store.to_string_lossy()).build().await.unwrap();
        declare_person(&db).await.expect("first apply");
        let tx = db.session().tx().await.unwrap();
        tx.execute("CREATE (p:Person {name: 'ada'})").await.unwrap();
        tx.commit().await.unwrap();
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(store.to_string_lossy()).build().await.unwrap();
    declare_person(&db)
        .await
        .expect("re-applying an unchanged schema to a populated label (issue #286)");

    assert!(
        !db.schema_manager().schema().properties["Person"]["name"].nullable,
        "the recorded declaration must survive re-registration unchanged"
    );
    db.shutdown().await.unwrap();
}

/// Control for the above: identical flow, no rows. Passed pre-fix, so it is
/// what distinguishes "populated" from "re-declaration" as the trigger.
#[tokio::test]
async fn reapplying_an_unchanged_schema_on_an_empty_label_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = dir.path().join("store");

    {
        let db = Uni::open(store.to_string_lossy()).build().await.unwrap();
        declare_person(&db).await.expect("first apply");
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(store.to_string_lossy()).build().await.unwrap();
    declare_person(&db)
        .await
        .expect("re-apply on an empty label");
    db.shutdown().await.unwrap();
}

/// Edge-type properties go through the same declaration path — the code keys
/// on a single `label_or_type` — so they need the same guarantee.
#[tokio::test]
async fn reapplying_an_unchanged_edge_type_after_writing_rows_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = dir.path().join("store");

    async fn declare(db: &Uni) -> Result<(), uni_db::UniError> {
        db.schema()
            .label("Person")
            .property("name", DataType::String)
            .done()
            .edge_type("KNOWS", &["Person"], &["Person"])
            .property("since", DataType::String) // NOT NULL by default
            .done()
            .apply()
            .await
            .map(|_| ())
    }

    {
        let db = Uni::open(store.to_string_lossy()).build().await.unwrap();
        declare(&db).await.expect("first apply");
        let tx = db.session().tx().await.unwrap();
        tx.execute(
            "CREATE (a:Person {name: 'ada'}), (b:Person {name: 'bob'}), \
             (a)-[:KNOWS {since: '2020'}]->(b)",
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(store.to_string_lossy()).build().await.unwrap();
    declare(&db)
        .await
        .expect("re-applying an unchanged edge-type property to a populated edge type");
    db.shutdown().await.unwrap();
}

/// The adjustment itself must survive: a genuinely NEW NOT NULL property on a
/// populated label is still recorded as nullable, and re-applying that same
/// schema is still a no-op rather than a conflict against the value it just
/// recorded. This is the case that worked before the fix and must keep working
/// — narrowing the guard to new properties could easily have broken it.
#[tokio::test]
async fn a_new_not_null_property_on_a_populated_label_is_nullable_and_reappliable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = dir.path().join("store");

    async fn declare_two(db: &Uni) -> Result<(), uni_db::UniError> {
        db.schema()
            .label("Person")
            .property("name", DataType::String)
            .property("email", DataType::String) // added later, NOT NULL by default
            .done()
            .apply()
            .await
            .map(|_| ())
    }

    {
        let db = Uni::open(store.to_string_lossy()).build().await.unwrap();
        declare_person(&db).await.expect("first apply");
        let tx = db.session().tx().await.unwrap();
        tx.execute("CREATE (p:Person {name: 'ada'})").await.unwrap();
        tx.commit().await.unwrap();
        // The flush is load-bearing, not incidental. The guard consults
        // `materialized_row_count`, which counts L1 rows only — a committed but
        // unflushed row leaves the label looking empty, and the property is
        // then recorded NOT NULL. So whether this adjustment fires depends on
        // flush timing, which is worth knowing but is not what this test pins.
        db.flush().await.unwrap();
        // `email` is new, and the label already has a materialized row with no
        // value for it.
        declare_two(&db)
            .await
            .expect("add a property to a populated label");
        assert!(
            db.schema_manager().schema().properties["Person"]["email"].nullable,
            "a NOT NULL property added to a populated label is recorded nullable"
        );
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(store.to_string_lossy()).build().await.unwrap();
    declare_two(&db)
        .await
        .expect("re-declaring NOT NULL over the nullable value this path recorded");
    assert!(
        db.schema_manager().schema().properties["Person"]["email"].nullable,
        "re-registration must not flip the recorded value"
    );
    db.shutdown().await.unwrap();
}
