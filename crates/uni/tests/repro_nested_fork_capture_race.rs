// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Deterministic regression for crates/uni/src/api/fork.rs (finding uni[6], D2).
//!
//! A NESTED fork used to read its parent fork branch's LIVE Lance tip in
//! `build_datasets_for_fork` — AFTER `flush_and_capture_fork_point` released the
//! `flush_lock`. A concurrent commit+flush on the parent fork landing between
//! fork-point capture and the branch cut advanced the parent branch tip, and the
//! child branched off that post-fork-point version — a snapshot-isolation
//! violation. The fix captures the parent-branch tip under `flush_lock` (in
//! `ForkPoint::parent_branch_versions`) and the nested arm branches at that
//! captured version, mirroring the primary-parent arm's M1 fix.
//!
//! This test drives the race deterministically via the `nested_fork_before_branch`
//! failpoint (active only under `--features failpoints`): it pauses the nested
//! fork AFTER capture but BEFORE the branch cut, advances the parent branch on
//! another task, then resumes and asserts the child did not observe the
//! post-capture write. Lives in its own test binary so the process-global
//! failpoint registry is isolated from other fork-creating tests.
//!
//! Run: `cargo nextest run -p uni-db --features failpoints -E 'test(nested_fork)'`.

#![cfg(feature = "failpoints")]

use std::sync::Arc;
use std::time::Duration;

use uni_db::{DataType, Uni};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_fork_branches_at_captured_parent_tip() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("k", DataType::Int64)
        .apply()
        .await?;

    // Primary row k=0.
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {k: 0})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Fork `a`, add k=1, flush → `a`'s branch tip covers k=0,k=1.
    let a = Arc::new(primary.fork("a").await?);
    let tx = a.tx().await?;
    tx.execute("CREATE (:Person {k: 1})").await?;
    tx.commit().await?;
    a.flush().await?;

    // Arm the nested-fork branch-point seam to PAUSE: the nested-fork task will
    // capture `a`'s branch tip under `flush_lock` (k<=1), then pause at the seam
    // BEFORE cutting the child branch.
    fail::cfg("nested_fork_before_branch", "pause").unwrap();

    // Create nested fork `b` on another task; it pauses at the seam.
    let a_fork = Arc::clone(&a);
    let forker = tokio::spawn(async move { a_fork.fork("b").await });

    // Let the forker pass capture (fast, under `flush_lock`) and reach the pause.
    // Because the seam is strictly AFTER `flush_and_capture_fork_point`, once the
    // task is paused, the fork point (k<=1) is already captured — so the k=2
    // write below cannot land in the capture.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Advance `a`'s branch tip PAST the fork point: commit+flush k=2.
    let tx = a.tx().await?;
    tx.execute("CREATE (:Person {k: 2})").await?;
    tx.commit().await?;
    a.flush().await?;

    // Release the seam; the nested fork now cuts its branch.
    fail::cfg("nested_fork_before_branch", "off").unwrap();
    let b = forker.await.expect("forker task panicked")?;
    fail::remove("nested_fork_before_branch");

    // The child is snapshot-isolated at the fork point (k<=1): with the fix it
    // branches at the CAPTURED tip (pre-k=2). The old live-read code would branch
    // at the concurrently-advanced tip and leak k=2.
    let rows = b
        .query("MATCH (p:Person) RETURN max(p.k) AS mk")
        .await?;
    let child_max: i64 = rows.rows()[0].get("mk").unwrap_or(-1);
    assert_eq!(
        child_max, 1,
        "nested child must branch at the captured fork-point tip (k<=1), not the \
         concurrently-advanced live tip; saw k={child_max}"
    );

    db.shutdown().await?;
    Ok(())
}
