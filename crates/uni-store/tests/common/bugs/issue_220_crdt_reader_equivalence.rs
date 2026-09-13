// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #220, site 4 — do the singular and batched property readers agree on a
//! CRDT property?
//!
//! #220 lists `get_batch_vertex_props_for_label` as the batched primitive for
//! the CRDT pre-merge loop in `Writer::insert_vertices_batch`, which reads
//! exclusively CRDT-typed keys one vertex at a time. That listing is a claim
//! that the two readers are interchangeable, and it had not been tested.
//!
//! They take different paths. The singular `get_vertex_prop_with_ctx` branches
//! on CRDT-ness and runs `accumulate_crdt_from_l0` then `finalize_crdt_lookup`,
//! which **merges** the L0 value into the storage value. The batched form
//! folds L0 over storage with `entry(k).or_insert(v)` — last writer wins per
//! key — and then runs `normalize_crdt_properties`, which repairs JSON shape
//! and merges nothing.
//!
//! For a `GCounter` the difference is visible and directional: merging two
//! replicas keeps both actors' counts, while letting L0 win drops the actor
//! that only exists in storage. A pre-merge that reads the wrong one writes a
//! counter that has gone *backwards* — the lost update this codebase already
//! has history with.
//!
//! This test exists to establish which reader the write path must use, before
//! any swap. It is written to be able to fail.

// Rust guideline compliant
#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::TempDir;
use uni_common::Value;
use uni_common::core::schema::{CrdtType, DataType, SchemaManager};
use uni_crdt::{Crdt, GCounter};
use uni_store::runtime::PropertyManager;
use uni_store::runtime::context::QueryContext;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

/// A `GCounter` value carrying one count per named actor.
fn gcounter(counts: &[(&str, u64)]) -> Value {
    let mut gc = GCounter::new();
    for (actor, count) in counts {
        gc.increment(actor, *count);
    }
    serde_json::to_value(Crdt::GCounter(gc))
        .expect("serialising a GCounter must succeed")
        .into()
}

/// Total across all actors, so the two readers can be compared by value rather
/// than by JSON spelling.
fn total(v: &Value) -> u64 {
    let json: serde_json::Value = v.clone().into();
    let Ok(Crdt::GCounter(gc)) = serde_json::from_value::<Crdt>(json.clone()) else {
        panic!("not a GCounter: {json}");
    };
    gc.value()
}

/// On the state the ordinary write path produces, the two readers agree.
///
/// This one is NOT by itself evidence that the readers are interchangeable, and
/// it is kept because saying so is the useful part. Measured: after a flushed
/// `actor1=10` and an unflushed `actor2=20`, L0 holds **both** actors —
/// `insert_vertex_with_labels` merges the incoming CRDT against storage at
/// write time. So both readers answer 30 from the overlay alone and never
/// consult storage for it. A reader that ignored storage entirely would also
/// pass here.
///
/// The discriminating case is its twin below, which builds the overlay by hand.
#[tokio::test]
async fn the_two_readers_agree_on_the_state_the_write_path_produces() -> Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let sm = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json")).await?,
    );
    sm.add_label("Counter")?;
    sm.add_property("Counter", "count", DataType::Crdt(CrdtType::GCounter), true)?;
    sm.save().await?;

    let storage = Arc::new(StorageManager::new(path, sm.clone()).await?);
    let writer = Writer::new(storage.clone(), sm.clone(), 1).await?;
    // Cache capacity 0: the singular reader consults an LRU before storage, and
    // a warm entry would make this measure the cache rather than the readers.
    let pm = PropertyManager::new(storage.clone(), sm.clone(), 0);

    let vid = writer.next_vid().await?;

    // actor1 = 10, flushed — visible only through storage.
    writer
        .insert_vertex_with_labels(
            vid,
            HashMap::from([("count".to_string(), gcounter(&[("actor1", 10)]))]),
            &["Counter".to_string()],
            None,
        )
        .await?;
    writer.flush_to_l1(None).await?;

    // actor2 = 20, left in L0 — the half that only the overlay knows.
    writer
        .insert_vertex_with_labels(
            vid,
            HashMap::from([("count".to_string(), gcounter(&[("actor2", 20)]))]),
            &["Counter".to_string()],
            None,
        )
        .await?;

    let l0 = writer.l0_manager.get_current();
    let ctx = QueryContext::new(l0);

    let singular = pm
        .get_vertex_prop_with_ctx(vid, "count", Some(&ctx))
        .await?;
    let batched = pm
        .get_batch_vertex_props_for_label(&[vid], "Counter", Some(&ctx))
        .await?
        .get(&vid)
        .and_then(|props| props.get("count").cloned())
        .expect("the batched reader returned no value for a live vertex");

    // The premise: both halves are genuinely split, so a reader that sees only
    // one of them is distinguishable from one that sees both.
    assert_eq!(
        total(&singular),
        30,
        "the singular reader lost a half: expected actor1=10 + actor2=20, got {singular:?}"
    );

    assert_eq!(
        total(&batched),
        total(&singular),
        "the batched reader disagrees with the singular one on a CRDT split \
         across L0 and storage: batched={} singular={}. The CRDT pre-merge in \
         `insert_vertices_batch` reads this property to merge against, so the \
         reader that drops a half writes a counter that has gone backwards \
         (#220 site 4).",
        total(&batched),
        total(&singular)
    );
    Ok(())
}

/// With an overlay holding a CRDT that does *not* already subsume storage, do
/// the readers still agree?
///
/// This is the case the write-path test above cannot reach, because that path
/// merges before it ever lands in L0. It is built by hand: storage holds
/// `actor1=10`, and a fresh overlay buffer holds `actor2=20` alone. A reader
/// that merges the two answers 30; one that lets the overlay win answers 20.
///
/// The assertion is deliberately an equality between the two readers rather
/// than against a literal, because the question #220 site 4 poses is
/// interchangeability, not which number is right in the abstract.
///
/// Answered: they are not. The generic batched reader answers 20 where the
/// singular answers 30, which is why `get_batch_vertex_crdt_props` exists and
/// is what site 4 uses. Both facts are asserted below.
#[tokio::test]
async fn the_two_readers_agree_on_an_overlay_that_does_not_subsume_storage() -> Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let sm = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json")).await?,
    );
    sm.add_label("Counter")?;
    sm.add_property("Counter", "count", DataType::Crdt(CrdtType::GCounter), true)?;
    sm.save().await?;

    let storage = Arc::new(StorageManager::new(path, sm.clone()).await?);
    let writer = Writer::new(storage.clone(), sm.clone(), 1).await?;
    let pm = PropertyManager::new(storage.clone(), sm.clone(), 0);

    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(
            vid,
            HashMap::from([("count".to_string(), gcounter(&[("actor1", 10)]))]),
            &["Counter".to_string()],
            None,
        )
        .await?;
    writer.flush_to_l1(None).await?;

    // A partial overlay, built directly so no write-time merge folds storage in.
    let overlay = Arc::new(parking_lot::RwLock::new(
        uni_store::runtime::l0::L0Buffer::new(0, None),
    ));
    overlay.write().insert_vertex_with_labels(
        vid,
        HashMap::from([("count".to_string(), gcounter(&[("actor2", 20)]))]),
        &["Counter".to_string()],
    );
    let ctx = QueryContext::new(overlay);

    let singular = pm
        .get_vertex_prop_with_ctx(vid, "count", Some(&ctx))
        .await?;
    let batched = pm
        .get_batch_vertex_crdt_props(&[vid], "Counter", &["count".to_string()], Some(&ctx))
        .await?
        .get(&vid)
        .and_then(|props| props.get("count").cloned())
        .expect("the batched CRDT reader returned no value for a live vertex");

    // The generic batched reader is what #220 proposed, and it is why
    // `get_batch_vertex_crdt_props` exists: pinned here so the divergence
    // cannot quietly close and leave the extra method looking unmotivated.
    let generic = pm
        .get_batch_vertex_props_for_label(&[vid], "Counter", Some(&ctx))
        .await?
        .get(&vid)
        .and_then(|props| props.get("count").cloned())
        .expect("the generic batched reader returned no value for a live vertex");
    assert_eq!(
        total(&generic),
        20,
        "the generic batched reader no longer drops storage's replica \
         (got {}). If that is now intentional, #220 site 4 can use it directly \
         and `get_batch_vertex_crdt_props` should go.",
        total(&generic)
    );

    // The premise: the overlay really is partial. If this reads 30 the fixture
    // has folded storage in somewhere and the comparison means nothing again.
    let raw_overlay = total(
        &ctx.l0
            .read()
            .vertex_properties
            .get(&vid)
            .and_then(|p| p.get("count").cloned())
            .expect("overlay lost its own write"),
    );
    assert_eq!(
        raw_overlay, 20,
        "the overlay was supposed to hold actor2 alone but totals {raw_overlay};          the fixture is no longer discriminating"
    );

    assert_eq!(
        total(&batched),
        total(&singular),
        "the readers disagree on a partial overlay: batched={} singular={}. \
         `insert_vertices_batch`'s CRDT pre-merge reads this value to merge \
         against, so the reader that drops storage writes a counter that has \
         gone backwards (#220 site 4).",
        total(&batched),
        total(&singular)
    );
    Ok(())
}

/// The batch insert's CRDT pre-merge keeps storage's replica, and probes for it
/// once for the batch rather than once per vertex.
///
/// Both halves matter and neither implies the other. The scan count alone would
/// pass for a pre-merge that read nothing at all; the value alone would pass for
/// the per-vertex loop this replaces.
///
/// # The fixture has to inject a partial overlay, and an earlier one did not
///
/// Seeding storage and flushing is not enough: a flush empties L0, so both
/// readers then see only storage and agree. Written that way this test passed
/// with the defective reader wired in. The divergence needs an overlay holding
/// a CRDT that does not subsume storage at the moment the pre-merge runs, so
/// one is written straight into the writer's live L0, bypassing the write-time
/// merge that would otherwise fold storage in.
///
/// # And the assertion has to read L0, not the vertex
///
/// Reading the property back through the singular reader masks the defect: it
/// merges L0 with storage on the way out and restores the replica the pre-merge
/// dropped. What is under test is the value the pre-merge *wrote*, so the
/// assertion reads it out of L0 directly.
#[tokio::test]
async fn a_batch_insert_premerges_crdts_without_losing_storage() -> Result<()> {
    use object_store::ObjectStore;
    use uni_common::config::UniConfig;
    use uni_store::backend::lance::LanceDbBackend;

    use super::fault_backend::FaultBackend;

    const N: usize = 6;

    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();
    let lance = LanceDbBackend::connect(&uri, None).await?;
    let fault = Arc::new(FaultBackend::new(Arc::new(lance)));
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let sm = Arc::new(
        SchemaManager::load_from_store(store.clone(), &ObjectStorePath::from("schema.json"))
            .await?,
    );
    sm.add_label("Counter")?;
    sm.add_property("Counter", "count", DataType::Crdt(CrdtType::GCounter), true)?;
    sm.save().await?;
    let storage = Arc::new(
        StorageManager::new_with_backend(
            &uri,
            store,
            fault.clone(),
            sm.clone(),
            UniConfig::default(),
        )
        .await?,
    );
    let writer = Writer::new(storage.clone(), sm.clone(), 1).await?;
    // Seed 2N vertices with actor1 and flush, so the pre-merge has a storage
    // replica to lose.
    let mut vids = Vec::new();
    for _ in 0..N * 2 {
        let vid = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(
                vid,
                HashMap::from([("count".to_string(), gcounter(&[("actor1", 10)]))]),
                &["Counter".to_string()],
                None,
            )
            .await?;
        vids.push(vid);
    }
    writer.flush_to_l1(None).await?;

    // A partial overlay for every seeded vid: actor2 alone, written raw so no
    // write-time merge folds storage's actor1 into it.
    {
        let l0 = writer.l0_manager.get_current();
        let mut guard = l0.write();
        for vid in &vids {
            guard.insert_vertex_with_labels(
                *vid,
                HashMap::from([("count".to_string(), gcounter(&[("actor2", 20)]))]),
                &["Counter".to_string()],
            );
        }
    }

    let batch_of = |n: usize| -> Vec<uni_common::Properties> {
        (0..n)
            .map(|_| HashMap::from([("count".to_string(), gcounter(&[("actor3", 30)]))]))
            .collect()
    };

    fault.reset_scans();
    writer
        .insert_vertices_batch(
            vids[..N].to_vec(),
            batch_of(N),
            vec!["Counter".to_string()],
            None,
        )
        .await?;
    let small_scans = fault.scans();

    fault.reset_scans();
    writer
        .insert_vertices_batch(
            vids[N..].to_vec(),
            batch_of(N),
            vec!["Counter".to_string()],
            None,
        )
        .await?;
    let large_scans = fault.scans();

    // Correctness: actor1=10 from storage, actor2=20 from the overlay, and
    // actor3=30 from the batch. A pre-merge that read an overlay-wins value
    // never sees actor1 and writes 50.
    let l0 = writer.l0_manager.get_current();
    for vid in &vids {
        let written = l0
            .read()
            .vertex_properties
            .get(vid)
            .and_then(|p| p.get("count").cloned())
            .expect("the batch insert wrote no value for this vertex");
        assert_eq!(
            total(&written),
            60,
            "vertex {vid:?} had {} written by the pre-merge, expected 60 \
             (actor1=10 storage + actor2=20 overlay + actor3=30 batch). 50 means \
             the pre-merge read a value with storage's replica dropped, so the \
             counter has gone backwards",
            total(&written)
        );
    }

    assert!(
        small_scans > 0,
        "the batch issued zero storage scans, so the decorator is not observing \
         the pre-merge and the comparison below cannot mean anything"
    );
    assert_eq!(
        small_scans, large_scans,
        "two batches of {N} issued {small_scans} and {large_scans} storage \
         scans. Both are the same size, so they must match; they are compared \
         rather than asserted against a literal because the seed flush leaves a \
         fixed number of unrelated reads in the count"
    );
    Ok(())
}
