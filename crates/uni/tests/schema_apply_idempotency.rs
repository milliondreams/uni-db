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
