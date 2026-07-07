// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Regression test for crates/uni-bulk/src/flush_intent.rs `recover_interrupted_bulk_load`
// (finding [2] / D9, High) — now FIXED.
//
// Previously, the `Active` branch rolled every touched table back but a rollback
// failure only did `failures += 1` + a warn!; the error was swallowed and
// `failures` never gated control flow. Execution then unconditionally reached
// `clear(&store).await?`, which DELETED the marker `catalog/bulk_flush_intent.json`.
// Once gone, the next reopen's read() returned None and no further reconciliation
// ever ran — a table that failed to roll back was permanently abandoned in its
// divergent state.
//
// The fix threads the `Active` rollback `failures` count out of the match and,
// when non-zero, returns an error WITHOUT clearing the marker — mirroring the
// `Committed` branch's fail-before-clear contract. So the marker survives for a
// later reopen to retry.
//
// This drives the REAL public `recover_interrupted_bulk_load` against a real
// StorageManager. The rollback is made to fail (no mock) by pointing the intent
// at tables that do not exist, so `backend.rollback_table` errors — exactly a
// transient object-store failure's control-flow effect.

use anyhow::Result;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path as ObjectStorePath};
use uni_db::Uni;

fn intent_path() -> ObjectStorePath {
    ObjectStorePath::from("catalog/bulk_flush_intent.json")
}

// Regression for FIXED finding uni-bulk[2] / D9: the `Active` recovery branch now
// RETAINS the intent marker and surfaces an error when a table rollback fails, so
// the divergent tables are reconciled on a later reopen instead of being abandoned.
#[tokio::test]
async fn active_recovery_retains_marker_when_rollback_fails() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Uni::open(temp_dir.path().to_str().unwrap()).build().await?;
    let storage = db.storage();
    let store = storage.store();

    // Premise: rolling back these tables genuinely FAILS (they were never
    // created), so the Active branch will take its swallowed-error path.
    let rb = storage
        .backend()
        .rollback_table("vertices_ghost_label", 5)
        .await;
    assert!(
        rb.is_err(),
        "premise failed: rollback_table on a non-existent table should error, got {rb:?}"
    );

    // Persist an Active bulk-flush intent naming two tables to roll back:
    //  - vertices_ghost_label -> pre-version 3
    //  - vertices_main        -> pre-version 5
    // Both rollbacks will fail (tables absent), mirroring a crash where the
    // per-label table rolled back but the main table's rollback timed out.
    let intent_json = br#"{"phase":"Active","tables":{"vertices_ghost_label":3,"vertices_main":5},"snapshot_id":null}"#;
    store
        .put(&intent_path(), PutPayload::from(intent_json.to_vec()))
        .await?;

    // Marker is present before recovery.
    assert!(
        store.get(&intent_path()).await.is_ok(),
        "setup failed: intent marker should exist before recovery"
    );

    // Run recovery. Both table rollbacks fail, so recovery must surface an error
    // rather than silently reporting success.
    let rec = uni_bulk::recover_interrupted_bulk_load(&storage).await;
    assert!(
        rec.is_err(),
        "recovery should propagate the rollback failure so the marker is kept for retry, got: {rec:?}"
    );

    // The marker is RETAINED because reconciliation did not succeed — so a later
    // reopen's read() still returns it and the divergent tables get another
    // reconciliation attempt instead of being abandoned.
    assert!(
        store.get(&intent_path()).await.is_ok(),
        "expected the intent marker to be RETAINED after a failed rollback (for retry); it was deleted"
    );
    Ok(())
}
