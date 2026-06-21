// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Concurrency: `fork(name).build()` racing `drop_fork(name)` must never hand
//! back a session over force-deleted Lance branches (production-readiness review
//! H2/M9).
//!
//! `build()` holds the registry's per-name lock for its whole open-or-create
//! flow; `drop_fork` must hold the same lock across tombstone → delete_branch →
//! finish_drop, so the two are mutually exclusive per name. Without it, a
//! concurrent open observes `Active`, registers a holder, and is mid-construction
//! when the drop force-deletes the branches → the returned session reads errors
//! or zero rows.

// Rust guideline compliant

use std::sync::Arc;

use anyhow::Result;
use uni_db::Uni;

/// Open vs drop, hammered. Whenever the opener gets a `Session` back, it MUST
/// read the seeded row — never an error and never 0 (which would mean it opened
/// over a half-deleted branch).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_open_never_races_drop_into_use_after_delete() -> Result<()> {
    let db = Arc::new(Uni::in_memory().build().await?);
    {
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute("CREATE (:Seed {k: 1})").await?;
        tx.commit().await?;
    }

    for _ in 0..200 {
        // Ensure the fork exists with the inherited seed at the start of the round.
        {
            let s = db.session();
            let _f = s.fork("x").await?;
        }

        let d_open = db.clone();
        let opener = tokio::spawn(async move {
            let s = d_open.session();
            match s.fork("x").await {
                // We got a live session: it must see exactly the seed row.
                Ok(fork) => match fork.query("MATCH (n:Seed) RETURN count(n) AS c").await {
                    Ok(rows) => Ok(rows.rows().first().and_then(|r| r.get::<i64>("c").ok())),
                    Err(e) => Err(format!("query on opened fork failed: {e}")),
                },
                // Drop won the race first: a clean lifecycle error is fine.
                Err(_) => Ok(None),
            }
        });
        let d_drop = db.clone();
        let dropper = tokio::spawn(async move {
            // May succeed, or report not-found/in-use; all acceptable.
            let _ = d_drop.drop_fork("x").await;
        });

        let (opened, _) = tokio::join!(opener, dropper);
        match opened.expect("opener task panicked") {
            Ok(Some(c)) => assert_eq!(
                c, 1,
                "an opened fork session must read the seed, not a deleted branch (review H2)"
            ),
            Ok(None) => {} // opener lost the race or query returned no rows handle — acceptable
            Err(e) => panic!("opened a fork session but it was unusable: {e} (review H2)"),
        }

        // Make sure the fork is gone before the next round.
        let _ = db.drop_fork("x").await;
    }

    Ok(())
}
