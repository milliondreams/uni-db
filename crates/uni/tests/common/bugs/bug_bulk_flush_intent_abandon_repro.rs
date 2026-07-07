// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-bulk/src/flush_intent.rs:184 (finding [2], High).
//
// In `recover_interrupted_bulk_load`, the `Active` branch rolls every touched
// table back. A rollback failure only does `failures += 1` + a warn!; the
// error is swallowed and `failures` never gates control flow. Execution then
// unconditionally reaches `clear(&store).await?` (line 184), which DELETES the
// marker `catalog/bulk_flush_intent.json`. Once gone, the next reopen's read()
// returns None and no further reconciliation ever runs — so a table that
// failed to roll back is permanently abandoned in its divergent state.
//
// Contrast: the `Committed` branch propagates `set_latest_snapshot` failure via
// `?` BEFORE clear(), preserving the marker for retry — the correct pattern
// that was not applied to `Active`.
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

// Pins OPEN finding uni-bulk[2] (flush_intent.rs:184): the `Active` recovery
// branch deletes the intent marker even when a table rollback failed, permanently
// abandoning the divergent tables. Tracked in docs/correctness-deferred.md as D9.
// When fixed, remove `#[ignore]` and flip the assertions to require the marker be
// RETAINED (and recovery to surface the failure).
#[tokio::test]
#[ignore = "pins OPEN finding uni-bulk[2] (flush_intent.rs:184); tracked as D9 in docs/correctness-deferred.md"]
async fn active_recovery_deletes_marker_even_when_rollback_fails() -> Result<()> {
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

    // Run recovery. Despite BOTH table rollbacks failing, it returns Ok(()).
    let rec = uni_bulk::recover_interrupted_bulk_load(&storage).await;
    // BUG: recovery swallows the rollback failures and reports success.
    // (repro for flush_intent.rs:184)
    assert!(
        rec.is_ok(),
        "recovery unexpectedly propagated the failure (bug may be fixed): {rec:?}"
    );

    // The marker is now GONE even though reconciliation did not succeed — so on
    // the next reopen read() returns None and the divergent tables are never
    // reconciled again: permanent abandonment.
    let after = store.get(&intent_path()).await;
    // BUG: expected the marker RETAINED for retry (as the Committed branch does
    // on failure); observed it deleted.
    assert!(
        after.is_err(),
        "repro for flush_intent.rs:184 — expected marker to be DELETED despite \
         failed rollback (proving abandonment); it still exists: {after:?}"
    );
    let msg = format!("{:?}", after.err().unwrap()).to_lowercase();
    assert!(
        msg.contains("not found") || msg.contains("notfound"),
        "marker deletion should manifest as NotFound, got: {msg}"
    );
    Ok(())
}
