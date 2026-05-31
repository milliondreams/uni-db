// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase A acceptance tests for optimistic write-set conflict detection.
//!
//! These exercise the lost-update fix directly at the Writer commit boundary
//! (`commit_transaction_l0`): two transactions that begin with the same read
//! sequence and write the same vertex must not both commit. See
//! `docs/proposals/serializable_snapshot_isolation.md` (Component C4, Request 1).

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::TempDir;
use uni_common::UniError;
use uni_common::core::schema::{
    Constraint, ConstraintTarget, ConstraintType, CrdtType, DataType, SchemaManager,
};
use uni_store::runtime::QueryContext;
use uni_store::runtime::l0_visibility::lookup_vertex_prop;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

async fn make_writer() -> Result<(Arc<Writer>, TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Counter")?;
    schema_manager.save().await?;
    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Arc::new(Writer::new(storage, schema_manager, 1).await?);
    Ok((writer, dir))
}

fn counter_props(n: i64) -> HashMap<String, uni_common::Value> {
    let mut props = HashMap::new();
    props.insert("n".to_string(), uni_common::Value::Int(n));
    props
}

const LABEL: &str = "Counter";

/// Two concurrent read-modify-writes of the same vertex: the second committer
/// must abort with a serialization conflict (the lost update is prevented).
#[tokio::test]
async fn concurrent_writes_to_same_vertex_conflict() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    // Both transactions begin before either commits → same read sequence.
    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, counter_props(1), &[LABEL.to_string()], Some(&tx_a))
        .await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(1), &[LABEL.to_string()], Some(&tx_b))
        .await?;

    // First commit wins.
    writer.commit_transaction_l0(tx_a).await?;

    // Second commit aborts — its read sequence predates A's committed write.
    let err = writer.commit_transaction_l0(tx_b).await.unwrap_err();
    match err.downcast::<UniError>() {
        Ok(UniError::SerializationConflict { .. }) => Ok(()),
        Ok(other) => panic!("expected SerializationConflict, got {other:?}"),
        Err(other) => panic!("expected typed UniError, got {other:?}"),
    }
}

/// Disjoint write-sets do not conflict — both transactions commit.
#[tokio::test]
async fn concurrent_writes_to_disjoint_vertices_both_commit() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid_a = writer.next_vid().await?;
    let vid_b = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid_a, counter_props(0), &[LABEL.to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(vid_b, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid_a, counter_props(1), &[LABEL.to_string()], Some(&tx_a))
        .await?;
    writer
        .insert_vertex_with_labels(vid_b, counter_props(1), &[LABEL.to_string()], Some(&tx_b))
        .await?;

    writer.commit_transaction_l0(tx_a).await?;
    writer.commit_transaction_l0(tx_b).await?;
    Ok(())
}

/// A transaction that begins after a conflicting commit (newer read sequence)
/// does not falsely conflict with it.
#[tokio::test]
async fn transaction_begun_after_commit_does_not_conflict() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    let tx_a = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, counter_props(1), &[LABEL.to_string()], Some(&tx_a))
        .await?;
    writer.commit_transaction_l0(tx_a).await?;

    // Begins now, observing A's commit → higher read sequence → no conflict.
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, counter_props(2), &[LABEL.to_string()], Some(&tx_b))
        .await?;
    writer.commit_transaction_l0(tx_b).await?;
    Ok(())
}

async fn make_writer_unique() -> Result<(Arc<Writer>, TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("E")?;
    schema_manager.add_constraint(Constraint {
        name: "E_eid_unique".to_string(),
        constraint_type: ConstraintType::Unique {
            properties: vec!["eid".to_string()],
        },
        target: ConstraintTarget::Label("E".to_string()),
        enabled: true,
    })?;
    schema_manager.save().await?;
    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Arc::new(Writer::new(storage, schema_manager, 1).await?);
    Ok((writer, dir))
}

fn eid_props(value: &str) -> HashMap<String, uni_common::Value> {
    let mut props = HashMap::new();
    props.insert(
        "eid".to_string(),
        uni_common::Value::String(value.to_string()),
    );
    props
}

/// Serializable MERGE: two transactions concurrently create distinct vertices
/// carrying the same unique key. Their write-sets are disjoint (different vids),
/// so write-set OCC does not catch it — the commit-time unique-key check must.
#[tokio::test]
async fn concurrent_unique_key_inserts_conflict() -> Result<()> {
    let (writer, _dir) = make_writer_unique().await?;
    let vid_a = writer.next_vid().await?;
    let vid_b = writer.next_vid().await?;

    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid_a, eid_props("shared"), &["E".to_string()], Some(&tx_a))
        .await?;
    writer
        .insert_vertex_with_labels(vid_b, eid_props("shared"), &["E".to_string()], Some(&tx_b))
        .await?;

    writer.commit_transaction_l0(tx_a).await?;

    let err = writer.commit_transaction_l0(tx_b).await.unwrap_err();
    match err.downcast::<UniError>() {
        Ok(UniError::ConstraintConflict { .. }) => Ok(()),
        Ok(other) => panic!("expected ConstraintConflict, got {other:?}"),
        Err(other) => panic!("expected typed UniError, got {other:?}"),
    }
}

/// SSI read-write antidependency: a transaction that read a vertex which a
/// concurrent transaction then wrote must abort, even though their write-sets
/// are disjoint (write-set OCC alone would miss it).
#[tokio::test]
async fn read_write_antidependency_aborts() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let x = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(x, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    // tx_a begins and reads X, recording it in tx_a's read-set.
    let tx_a = writer.create_transaction_l0();
    {
        let ctx = QueryContext::new_with_tx(writer.l0_manager.get_current(), Some(tx_a.clone()));
        let _ = lookup_vertex_prop(x, "n", Some(&ctx));
    }

    // tx_b writes X and commits.
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(x, counter_props(1), &[LABEL.to_string()], Some(&tx_b))
        .await?;
    writer.commit_transaction_l0(tx_b).await?;

    // tx_a writes an unrelated vertex Y, then commits — must abort because it
    // read X, which tx_b wrote after tx_a began.
    let y = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(y, counter_props(9), &[LABEL.to_string()], Some(&tx_a))
        .await?;
    let err = writer.commit_transaction_l0(tx_a).await.unwrap_err();
    match err.downcast::<UniError>() {
        Ok(UniError::SerializationConflict { .. }) => Ok(()),
        Ok(other) => panic!("expected SerializationConflict, got {other:?}"),
        Err(other) => panic!("expected typed UniError, got {other:?}"),
    }
}

/// Control: without the read, the same disjoint-write interleaving commits fine.
#[tokio::test]
async fn disjoint_writes_without_read_do_not_conflict() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let x = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(x, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    let tx_a = writer.create_transaction_l0(); // does NOT read X
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(x, counter_props(1), &[LABEL.to_string()], Some(&tx_b))
        .await?;
    writer.commit_transaction_l0(tx_b).await?;

    let y = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(y, counter_props(9), &[LABEL.to_string()], Some(&tx_a))
        .await?;
    writer.commit_transaction_l0(tx_a).await?;
    Ok(())
}

// ── CRDT carve-out (FIX A): concurrent commutative writes must merge, not abort ──

/// A property map with a single GCounter under `counter`, written with no
/// labels so the OCC CRDT carve-out applies.
fn gcounter_props(actor: &str, n: u64) -> HashMap<String, uni_common::Value> {
    let mut gc = uni_crdt::GCounter::new();
    gc.increment(actor, n);
    let v: uni_common::Value = serde_json::to_value(uni_crdt::Crdt::GCounter(gc))
        .unwrap()
        .into();
    HashMap::from([("counter".to_string(), v)])
}

/// Reads back the committed `counter` GCounter total for `vid` from the main L0.
fn gcounter_total(writer: &Writer, vid: uni_common::core::id::Vid) -> u64 {
    let ctx = QueryContext::new(writer.l0_manager.get_current());
    let v = lookup_vertex_prop(vid, "counter", Some(&ctx)).expect("counter present");
    let json: serde_json::Value = v.into();
    match serde_json::from_value::<uni_crdt::Crdt>(json).unwrap() {
        uni_crdt::Crdt::GCounter(gc) => gc.value(),
        other => panic!("expected GCounter, got {other:?}"),
    }
}

/// Two concurrent CRDT-counter increments to the same vertex must BOTH commit
/// and merge — the conflict that would defeat CRDT semantics is carved out.
#[tokio::test]
async fn concurrent_crdt_increments_merge_instead_of_conflicting() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    // Seed an empty counter (no labels → carve-out eligible).
    writer
        .insert_vertex_with_labels(vid, gcounter_props("seed", 0), &[], None)
        .await?;

    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, gcounter_props("a", 5), &[], Some(&tx_a))
        .await?;
    writer
        .insert_vertex_with_labels(vid, gcounter_props("b", 7), &[], Some(&tx_b))
        .await?;

    // Neither aborts; the second merges into the first's committed state.
    writer.commit_transaction_l0(tx_a).await?;
    writer.commit_transaction_l0(tx_b).await?;

    assert_eq!(gcounter_total(&writer, vid), 12, "5 + 7 should merge to 12");
    Ok(())
}

/// R1 (documented limitation): a CRDT-only writer and a concurrent LWW writer to
/// the same property both commit — the item-level carve-out cannot make them
/// conflict. No new hazard: `merge_crdt_properties` already overwrites a CRDT
/// value with a non-CRDT one. The final value is commit-order dependent.
#[tokio::test]
async fn crdt_only_vs_lww_same_prop_both_commit() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, gcounter_props("seed", 0), &[], None)
        .await?;

    let tx_a = writer.create_transaction_l0(); // CRDT increment (carved out)
    let tx_b = writer.create_transaction_l0(); // plain overwrite of `counter`
    writer
        .insert_vertex_with_labels(vid, gcounter_props("a", 5), &[], Some(&tx_a))
        .await?;
    let lww = HashMap::from([("counter".to_string(), uni_common::Value::Int(99))]);
    writer
        .insert_vertex_with_labels(vid, lww, &[], Some(&tx_b))
        .await?;

    // Both commit (no conflict); this documents the accepted R1 semantics.
    writer.commit_transaction_l0(tx_a).await?;
    writer.commit_transaction_l0(tx_b).await?;
    Ok(())
}

/// A write mixing a CRDT property with a non-CRDT property stays conflictable —
/// the LWW part can still be lost, so the carve-out must not apply.
#[tokio::test]
async fn mixed_crdt_and_lww_write_still_conflicts() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, gcounter_props("seed", 0), &[], None)
        .await?;

    let mut mixed_a = gcounter_props("a", 5);
    mixed_a.insert("n".to_string(), uni_common::Value::Int(1));
    let mut mixed_b = gcounter_props("b", 7);
    mixed_b.insert("n".to_string(), uni_common::Value::Int(2));

    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, mixed_a, &[], Some(&tx_a))
        .await?;
    writer
        .insert_vertex_with_labels(vid, mixed_b, &[], Some(&tx_b))
        .await?;

    writer.commit_transaction_l0(tx_a).await?;
    let err = writer.commit_transaction_l0(tx_b).await.unwrap_err();
    match err.downcast::<UniError>() {
        Ok(UniError::SerializationConflict { .. }) => Ok(()),
        Ok(other) => panic!("expected SerializationConflict, got {other:?}"),
        Err(other) => panic!("expected typed UniError, got {other:?}"),
    }
}

/// A delete and a concurrent update of the same vertex conflict (the tombstone
/// is in the write-set; deletion is not commutative with an update).
#[tokio::test]
async fn delete_vs_update_same_vertex_conflicts() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    writer
        .delete_vertex(vid, Some(vec![LABEL.to_string()]), Some(&tx_a))
        .await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(1), &[LABEL.to_string()], Some(&tx_b))
        .await?;

    writer.commit_transaction_l0(tx_a).await?;
    let err = writer.commit_transaction_l0(tx_b).await.unwrap_err();
    match err.downcast::<UniError>() {
        Ok(UniError::SerializationConflict { .. }) => Ok(()),
        Ok(other) => panic!("expected SerializationConflict, got {other:?}"),
        Err(other) => panic!("expected typed UniError, got {other:?}"),
    }
}

/// An aborted commit leaves no trace: after the loser aborts, the committed
/// value is the winner's, and the loser's write is fully discarded.
#[tokio::test]
async fn aborted_commit_leaves_no_trace() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, counter_props(11), &[LABEL.to_string()], Some(&tx_a))
        .await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(22), &[LABEL.to_string()], Some(&tx_b))
        .await?;

    writer.commit_transaction_l0(tx_a).await?;
    let _ = writer.commit_transaction_l0(tx_b).await.unwrap_err();

    // The committed value is A's (11); B's 22 left no trace.
    let ctx = QueryContext::new(writer.l0_manager.get_current());
    assert_eq!(
        lookup_vertex_prop(vid, "n", Some(&ctx)),
        Some(uni_common::Value::Int(11)),
    );
    Ok(())
}

/// An aborted commit leaves no *durable* trace either: after the loser aborts,
/// a flush to L1 persists only the winner's value (validation runs before the
/// WAL/flush commit point, so the loser never reaches durable storage).
#[tokio::test]
async fn aborted_commit_leaves_no_trace_after_flush() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, counter_props(11), &[LABEL.to_string()], Some(&tx_a))
        .await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(22), &[LABEL.to_string()], Some(&tx_b))
        .await?;

    writer.commit_transaction_l0(tx_a).await?;
    let _ = writer.commit_transaction_l0(tx_b).await.unwrap_err();

    // Flushing must persist only A's committed write; B's aborted 22 is gone.
    writer.flush_to_l1(None).await?;
    let pm = writer
        .property_manager
        .as_ref()
        .expect("writer has a property manager");
    let n = pm.get_vertex_prop(vid, "n").await?;
    assert_eq!(
        n,
        uni_common::Value::Int(11),
        "only the winner's value is durable after flush",
    );
    Ok(())
}

// ── CRDT carve-out soundness (FIX A): variant mismatch must abort, not lose ──

/// A property map with a single GSet under `counter`, no labels (carve-out
/// eligible) — a *different* CRDT variant than [`gcounter_props`].
fn gset_props(item: &str) -> HashMap<String, uni_common::Value> {
    let mut gs = uni_crdt::GSet::new();
    gs.add(item.to_string());
    let v: uni_common::Value = serde_json::to_value(uni_crdt::Crdt::GSet(gs))
        .unwrap()
        .into();
    HashMap::from([("counter".to_string(), v)])
}

/// Builds a writer whose `Counter` label declares `counter` as a GCounter CRDT,
/// so write-time variant enforcement (`prepare_vertex_upsert`) applies.
async fn make_writer_crdt() -> Result<(Arc<Writer>, TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label(LABEL)?;
    schema_manager.add_property(LABEL, "counter", DataType::Crdt(CrdtType::GCounter), true)?;
    schema_manager.save().await?;
    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Arc::new(Writer::new(storage, schema_manager, 1).await?);
    Ok((writer, dir))
}

/// FIX A (commit-time, layer 2): two concurrent carved-out CRDT writes of
/// *different* variants to the same property. Without the fix, the second
/// committer's GSet would silently overwrite the first's committed GCounter (a
/// lost update the carve-out hid). The commit-time soundness check aborts it.
#[tokio::test]
async fn concurrent_crdt_variant_mismatch_aborts() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;

    // No labels → carve-out eligible → bypasses write-time enforcement, so this
    // exercises the commit-time main-L0 check specifically.
    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, gcounter_props("a", 5), &[], Some(&tx_a))
        .await?;
    writer
        .insert_vertex_with_labels(vid, gset_props("x"), &[], Some(&tx_b))
        .await?;

    // tx_a commits a GCounter into main L0.
    writer.commit_transaction_l0(tx_a).await?;
    // tx_b's GSet would overwrite that GCounter at merge → must abort.
    let err = writer.commit_transaction_l0(tx_b).await.unwrap_err();
    match err.downcast::<UniError>() {
        Ok(UniError::SerializationConflict { .. }) => Ok(()),
        Ok(other) => panic!("expected SerializationConflict, got {other:?}"),
        Err(other) => panic!("expected typed UniError, got {other:?}"),
    }
}

/// FIX A (write-time, layer 1): writing a CRDT property with the wrong declared
/// variant (a GSet where the schema declares a GCounter) is rejected at the
/// source, before commit — keeping concurrent CRDT writes commutative.
#[tokio::test]
async fn write_time_rejects_wrong_crdt_variant() -> Result<()> {
    let (writer, _dir) = make_writer_crdt().await?;
    let vid = writer.next_vid().await?;
    // Labelled write → the schema resolves → variant enforcement fires.
    let err = writer
        .insert_vertex_with_labels(vid, gset_props("x"), &[LABEL.to_string()], None)
        .await
        .unwrap_err();
    match err.downcast::<UniError>() {
        Ok(UniError::Constraint { .. }) => Ok(()),
        Ok(other) => panic!("expected Constraint, got {other:?}"),
        Err(other) => panic!("expected typed UniError, got {other:?}"),
    }
}

/// Regression guard: a CRDT written in the *string* form (the Cypher
/// `'{"t":"gc",...}'` representation) is accepted — write-time enforcement only
/// polices the parsed `Map` form (the carve-out's domain), never the string form
/// (which is not carved out and stays conflictable).
#[tokio::test]
async fn write_time_accepts_string_form_crdt() -> Result<()> {
    let (writer, _dir) = make_writer_crdt().await?;
    let vid = writer.next_vid().await?;
    let string_form = HashMap::from([(
        "counter".to_string(),
        uni_common::Value::String(r#"{"t":"gc","d":{"counts":{"a":5}}}"#.to_string()),
    )]);
    writer
        .insert_vertex_with_labels(vid, string_form, &[LABEL.to_string()], None)
        .await?;
    Ok(())
}

/// Control: the correct declared variant is accepted at write time.
#[tokio::test]
async fn write_time_accepts_declared_crdt_variant() -> Result<()> {
    let (writer, _dir) = make_writer_crdt().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, gcounter_props("a", 5), &[LABEL.to_string()], None)
        .await?;
    Ok(())
}

/// R1 outcome pinned: a concurrent non-CRDT (LWW) write committing *after* the
/// CRDT writer wins last-writer-wins — the final value is the scalar and the
/// merged CRDT state is discarded (with a logged warning). Documents that the
/// item-level carve-out cannot make CRDT-vs-LWW conflict (the accepted R1).
#[tokio::test]
async fn r1_crdt_overwritten_by_lww_pins_value() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, gcounter_props("seed", 0), &[], None)
        .await?;

    let tx_a = writer.create_transaction_l0(); // CRDT increment (carved out)
    let tx_b = writer.create_transaction_l0(); // LWW scalar overwrite
    writer
        .insert_vertex_with_labels(vid, gcounter_props("a", 5), &[], Some(&tx_a))
        .await?;
    let lww = HashMap::from([("counter".to_string(), uni_common::Value::Int(99))]);
    writer
        .insert_vertex_with_labels(vid, lww, &[], Some(&tx_b))
        .await?;

    writer.commit_transaction_l0(tx_a).await?;
    writer.commit_transaction_l0(tx_b).await?; // R1: overwrites, no abort

    let ctx = QueryContext::new(writer.l0_manager.get_current());
    assert_eq!(
        lookup_vertex_prop(vid, "counter", Some(&ctx)),
        Some(uni_common::Value::Int(99)),
        "last-writer-wins: the scalar overwrites the CRDT",
    );
    Ok(())
}

// ── C1 snapshot freeze (self-pin fix): freeze fires iff the gen is pinned ────────
//
// A freeze installs a NEW current buffer (clone-on-freeze), so the generation's
// `Arc` pointer changes; an in-place merge keeps the same buffer. These pin the
// freeze decision so the self-pin defect (a tx's own pin freezing its own commit)
// cannot regress.

/// No snapshot pinned ⇒ the commit merges in place; the generation is unchanged.
#[tokio::test]
async fn commit_without_pin_does_not_freeze() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    let before = Arc::as_ptr(&writer.l0_manager.get_current());
    let tx = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, counter_props(1), &[LABEL.to_string()], Some(&tx))
        .await?;
    writer.commit_transaction_l0(tx).await?;
    let after = Arc::as_ptr(&writer.l0_manager.get_current());

    assert_eq!(before, after, "no pin ⇒ no freeze (in-place merge)");
    Ok(())
}

/// A commit while another snapshot pins the generation freezes it aside (new
/// buffer), and the held snapshot still reads the pre-commit value.
#[tokio::test]
async fn commit_with_held_pin_freezes_and_isolates() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    // A concurrent reader holds a snapshot of the current generation.
    let snap = writer.l0_manager.pin_snapshot();
    let before = Arc::as_ptr(&writer.l0_manager.get_current());

    let tx = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, counter_props(1), &[LABEL.to_string()], Some(&tx))
        .await?;
    writer.commit_transaction_l0(tx).await?;
    let after = Arc::as_ptr(&writer.l0_manager.get_current());

    assert_ne!(before, after, "held pin ⇒ freeze (new generation)");
    // The held snapshot is isolated: it still reads the pre-commit value.
    let ctx = QueryContext::new(snap.main.clone());
    assert_eq!(
        lookup_vertex_prop(vid, "n", Some(&ctx)),
        Some(uni_common::Value::Int(0)),
    );
    Ok(())
}

/// Mirrors the self-pin fix: releasing the pin BEFORE the commit avoids the freeze.
#[tokio::test]
async fn commit_after_releasing_pin_does_not_freeze() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    let snap = writer.l0_manager.pin_snapshot();
    assert!(writer.l0_manager.is_current_pinned());
    drop(snap); // release the pin before committing — what the fix does in commit()
    assert!(!writer.l0_manager.is_current_pinned());

    let before = Arc::as_ptr(&writer.l0_manager.get_current());
    let tx = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, counter_props(1), &[LABEL.to_string()], Some(&tx))
        .await?;
    writer.commit_transaction_l0(tx).await?;
    let after = Arc::as_ptr(&writer.l0_manager.get_current());

    assert_eq!(before, after, "pin released before commit ⇒ no freeze");
    Ok(())
}
