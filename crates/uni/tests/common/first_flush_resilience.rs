// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #275 — a crash before a brand-new store's first L0→L1 flush.
//!
//! The snapshot manifest is written only by a flush. On a fresh store the first
//! flush is whichever comes first: `auto_flush_interval` (default 5s),
//! `auto_flush_threshold`, or an explicit `flush()`/`shutdown()`. A crash inside
//! that window leaves WAL segments and no manifest.
//!
//! `Uni::build` used to refuse that combination outright, because it is also the
//! shape of issue #72 — a store that *had* flushed and then lost its manifests,
//! where restarting the version counter at 0 would collide with the versions
//! already baked into L1 and corrupt data. The guard could not tell the two
//! apart: it asked "manifests?" and "WAL?" and never "is there any L1 data?".
//!
//! On a store that never flushed there is no L1 data, so version 0 collides with
//! nothing and the WAL is the whole truth. These tests pin both halves — the
//! fresh-store crash must recover, and #72's lost-manifest case must still fail
//! loudly — so a future relaxation of one cannot quietly undo the other.
//!
//! Uses the child-process abort harness (`crash_harness`) rather than a panic +
//! drop: `Drop for Uni` runs a full `flush_to_l1`, which would write the very
//! manifest whose absence is the bug.

// Rust guideline compliant

// Same gate as `crash_harness`, which this depends on. The abort itself needs
// no fail point, but the harness is `failpoints`-gated, and matching it keeps
// this file in the lane that already runs `test(/resilience|.../)` rather than
// inventing a second child-spawn path.
#![cfg(all(unix, feature = "failpoints"))]

use std::path::Path;

use uni_db::{DataType, Uni};

use crate::crash_harness;

const ENTRY: &str = "first_flush_resilience::first_flush_abort_child";

/// Writes one committed row into a brand-new store, then aborts.
///
/// `scenario` selects whether a flush runs first:
/// * `no_flush` — abort with the commit acknowledged and no manifest on disk.
/// * `after_flush` — the control; one `flush()` publishes a manifest, and
///   everything else about the run is identical.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "child-process entry point; driven by run_child"]
async fn first_flush_abort_child() {
    let Some((scenario, path)) = crash_harness::child_env() else {
        return;
    };

    let db = Uni::open(path.to_string_lossy())
        .build()
        .await
        .expect("open store");
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .apply()
        .await
        .expect("register schema");

    let tx = db.session().tx().await.expect("begin tx");
    tx.execute("CREATE (p:Person {name: 'ada'})")
        .await
        .expect("create");
    tx.commit().await.expect("commit");

    match scenario.as_str() {
        "no_flush" => {}
        "after_flush" => db.flush().await.expect("flush"),
        other => crash_harness::unknown_scenario(ENTRY, other),
    }

    // No destructor, no `Drop for Uni`, no shutdown hook. What survives is
    // exactly what was fsynced.
    std::process::abort();
}

/// Asserts the store reopens and still holds the committed row.
async fn assert_row_survived(store: &Path) {
    let db = Uni::open(store.to_string_lossy())
        .build()
        .await
        .expect("reopen a store whose only defect is that it never flushed");

    let rows = db
        .session()
        .query("MATCH (p:Person) RETURN p.name AS name")
        .await
        .expect("query");
    let name: String = rows
        .rows()
        .first()
        .expect("one row")
        .get("name")
        .expect("name");
    assert_eq!(name, "ada", "committed row did not survive the crash");
}

/// Issue #275: the failing case. A crash before the first flush must not make
/// committed writes permanently unreachable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_before_first_flush_still_opens() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = dir.path().join("store");

    crash_harness::run_child_async(ENTRY, "no_flush", &store).await;

    // The WAL is on disk and complete; nothing else is. Asserting this here
    // means a later change that starts flushing eagerly turns this test into a
    // loud failure rather than a silent pass against a different scenario.
    let wal = store.join("wal");
    assert!(
        wal.is_dir()
            && wal
                .read_dir()
                .expect("read wal dir")
                .next()
                .is_some_and(|e| e.is_ok()),
        "no WAL segments were written — the child did not commit, so this test \
         would pass without exercising the bug"
    );

    assert_row_survived(&store).await;
}

/// Control for the above: same crash, same writes, one flush first. Byte-identical
/// otherwise. If this ever fails, the bug is in crash recovery generally and not
/// in the first-flush window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_a_flush_recovers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = dir.path().join("store");

    crash_harness::run_child_async(ENTRY, "after_flush", &store).await;
    assert_row_survived(&store).await;
}
