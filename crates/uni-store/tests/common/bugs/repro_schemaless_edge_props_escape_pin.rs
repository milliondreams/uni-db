// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for `storage/main_edge.rs:296` (Tier 1.1).
//!
//! `MainEdgeDataset::find_props_by_eid` took only `(backend, eid)`. Its filter
//! was `_eid = n` alone, and the winner was simply the highest `_version` row —
//! with no snapshot bound anywhere.
//!
//! Every other tier of the same read *is* bounded: `PropertyManager` conjoins
//! `apply_version_filter` onto the delta scan, and `overlay_l0_edge_batch`
//! skips entries whose `entry_version > hwm`. Only this L1 main-table fallback
//! read at HEAD. So a snapshot-pinned reader saw its own snapshot in L0 and
//! delta, and post-snapshot state in L1 — a snapshot-isolation violation.
//!
//! Scope, established after the fix landed: the bound only takes effect when the
//! calling `PropertyManager` was constructed over pinned storage, which today
//! means `UniInner::at_snapshot`'s time-travel view. A read-write transaction
//! routes its *scans* through `pinned_at_version` but deliberately keeps the
//! live `PropertyManager`, so property point-reads honour read-your-writes and
//! cross-transaction property skew is caught by OCC at commit instead. The
//! tests below exercise the dataset directly, which is the layer the bound
//! lives at.
//!
//! The vertex side has had the bound since it was written
//! (`MainVertexDataset::find_props_by_vid` takes `version: Option<u64>`), and
//! `repro_17_overlay_tombstone_ungated.rs` is the vertex-side analogue of the
//! delete half below.
//!
//! Two things make this wider than it looks:
//!
//! * **It is not compaction-only.** Schemaless and overflow edge properties
//!   live *only* in `props_json` and never in delta columns
//!   (`property_manager.rs`), so the fallback fires on every such read.
//! * **It is bidirectional.** A post-pin *delete* writes a tombstone that wins
//!   the version race and yields `None`, so an edge that existed at the
//!   snapshot vanishes — not merely a stale-value read.

#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::Value;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::SchemaManager;
use uni_store::storage::main_edge::MainEdgeDataset;
use uni_store::storage::manager::StorageManager;

/// Versions below and above the pin.
const AT_SNAPSHOT: u64 = 5;
const PIN: u64 = 100;
const AFTER_SNAPSHOT: u64 = 101;

async fn storage_with_edge_rows(
    dir: &std::path::Path,
    rows: &[(Eid, Vid, Vid, Properties, bool, u64)],
) -> Arc<StorageManager> {
    let path = dir.to_str().unwrap().to_string();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json"))
            .await
            .unwrap(),
    );
    schema_manager.save().await.unwrap();

    let storage = Arc::new(
        StorageManager::new(&path, schema_manager.clone())
            .await
            .unwrap(),
    );

    // Write straight into the main `edges` table so `_version` is exact. The
    // properties are schemaless, so they live only in `props_json` — which is
    // precisely the column `find_props_by_eid` exists to read.
    let edges: Vec<_> = rows
        .iter()
        .map(|(eid, src, dst, props, deleted, version)| {
            (
                *eid,
                *src,
                *dst,
                "LINKS".to_string(),
                props.clone(),
                *deleted,
                *version,
            )
        })
        .collect();
    let batch = MainEdgeDataset::build_record_batch(&edges, None, None).unwrap();
    MainEdgeDataset::write_batch(storage.backend(), batch)
        .await
        .unwrap();

    storage
}

type Properties = HashMap<String, Value>;

fn props(v: i64) -> Properties {
    let mut p = HashMap::new();
    p.insert("v".to_string(), Value::Int(v));
    p
}

/// A property update committed *after* the snapshot must not be visible to a
/// read pinned at that snapshot.
#[tokio::test]
async fn pinned_read_ignores_a_post_pin_schemaless_edge_update() {
    let dir = tempdir().unwrap();
    let eid = Eid::new(10);
    let (src, dst) = (Vid::new(1), Vid::new(2));

    let storage = storage_with_edge_rows(
        dir.path(),
        &[
            (eid, src, dst, props(1), false, AT_SNAPSHOT),
            (eid, src, dst, props(2), false, AFTER_SNAPSHOT),
        ],
    )
    .await;

    let at_pin = MainEdgeDataset::find_props_by_eid(storage.backend(), eid, Some(PIN))
        .await
        .unwrap()
        .expect("the edge existed at the snapshot");

    // Asserting the VALUE, not merely that a row came back: stripping the bound
    // would also return `Some`, just the wrong `Some`.
    assert_eq!(
        at_pin.get("v"),
        Some(&Value::Int(1)),
        "a pinned read must see the value as of its snapshot; got {at_pin:?}"
    );

    // Control: unpinned, the later write is exactly what should be seen. This
    // is what proves the bound narrowed visibility rather than blinding the
    // reader outright.
    let at_head = MainEdgeDataset::find_props_by_eid(storage.backend(), eid, None)
        .await
        .unwrap()
        .expect("the edge is live at HEAD");
    assert_eq!(
        at_head.get("v"),
        Some(&Value::Int(2)),
        "an unpinned read must still see the latest value; got {at_head:?}"
    );
}

/// The other direction, which has no vertex-side twin: a delete committed after
/// the snapshot must not make the edge disappear from the pinned read.
#[tokio::test]
async fn pinned_read_survives_a_post_pin_edge_delete() {
    let dir = tempdir().unwrap();
    let eid = Eid::new(11);
    let (src, dst) = (Vid::new(1), Vid::new(2));

    let storage = storage_with_edge_rows(
        dir.path(),
        &[
            (eid, src, dst, props(1), false, AT_SNAPSHOT),
            // The tombstone wins the version race and yields `None`.
            (eid, src, dst, HashMap::new(), true, AFTER_SNAPSHOT),
        ],
    )
    .await;

    let at_pin = MainEdgeDataset::find_props_by_eid(storage.backend(), eid, Some(PIN))
        .await
        .unwrap()
        .expect("a post-pin delete must not erase the edge from the pinned snapshot");
    assert_eq!(
        at_pin.get("v"),
        Some(&Value::Int(1)),
        "the pinned read must still see the pre-delete properties; got {at_pin:?}"
    );

    // Control: at HEAD the edge really is deleted.
    let at_head = MainEdgeDataset::find_props_by_eid(storage.backend(), eid, None)
        .await
        .unwrap();
    assert!(
        at_head.is_none(),
        "the tombstone must still win for an unpinned read; got {at_head:?}"
    );
}

/// Inverse guard: the version bound is a conjunct, not a replacement for the
/// tombstone-winner rule (issue #53 / review C2).
///
/// With both the live row and its tombstone at or below the pin, the tombstone
/// must still win. A fix that filtered `_deleted = false` instead of adding the
/// version conjunct would pass the two tests above and fail this one.
#[tokio::test]
async fn version_bound_does_not_resurrect_a_tombstoned_edge() {
    let dir = tempdir().unwrap();
    let eid = Eid::new(12);
    let (src, dst) = (Vid::new(1), Vid::new(2));

    let storage = storage_with_edge_rows(
        dir.path(),
        &[
            (eid, src, dst, props(1), false, AT_SNAPSHOT),
            (eid, src, dst, HashMap::new(), true, AT_SNAPSHOT + 1),
        ],
    )
    .await;

    let at_pin = MainEdgeDataset::find_props_by_eid(storage.backend(), eid, Some(PIN))
        .await
        .unwrap();
    assert!(
        at_pin.is_none(),
        "a tombstone below the pin must still win; got {at_pin:?}"
    );
}
