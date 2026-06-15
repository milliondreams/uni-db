// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! RC2 (uniko `UNI_DB_WORKAROUNDS.md`): MERGE insert-phantoms + the atomic-SET
//! wishlist. The downstream catalog bundles these as one "open" item; verifying
//! against uni HEAD splits them, and they now have different dispositions.
//!
//! **(1) MERGE insert-phantom — FIXED.** Concurrent `MERGE (e:E {code:'x'})` of
//! the same key with **no** declared UNIQUE constraint used to each see an empty
//! match and all insert → duplicate nodes (SSI's `OccReadSet` tracks item-level
//! reads only). uni-db now keeps a per-transaction implicit MERGE-key guard
//! (`L0Buffer::merge_guard_index`): a MERGE that *creates* a node registers its
//! `(label, key-props)`, and `commit_transaction_l0` re-probes it against the
//! committed overlay (reusing the UNIQUE-constraint machinery) — so a concurrent
//! MERGE of the same key aborts retriably (`ConstraintConflict`) instead of
//! duplicating. A plain `CREATE` registers no key and is unaffected. Two guards
//! below pin this: losers abort (no retry) with no duplicate, and converge to one
//! node under retry. This is what lets uniko drop its `StripedLocks` RMW layer for
//! `transact_with_retry`.
//!
//! **(2) Atomic SET without a client retry loop — WISHLIST, not a bug.** SSI
//! already prevents lost updates *with* retry (`ssi_occ_e2e.rs::atomic_increment_*`
//! converge to N via `execute_with_retry`). "Server-side atomic `SET n = n + 1`
//! that needs no client retry loop" is a convenience feature (a CAS / commutative
//! counter), not a correctness gap — the engine is already *sound* without it.
//! The guard below pins that soundness and documents the wishlist.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration bug_rc2_merge_phantom_and_atomic_set

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Barrier;
use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

/// A `:E {code}` schema with a Hash index on the merge key but, deliberately,
/// **no** UNIQUE constraint (mirrors how uniko declares merge keys).
async fn e_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("code", DataType::String)
        .index("code", IndexType::Scalar(ScalarType::Hash))
        .done()
        .apply()
        .await?;
    Ok(db)
}

/// Counts `:E` nodes.
async fn count_e(db: &Uni) -> Result<i64> {
    let r = db
        .session()
        .query("MATCH (e:E) RETURN count(e) AS c")
        .await?;
    Ok(r.rows()[0].get::<i64>("c")?)
}

/// FIXED: without a UNIQUE constraint, concurrent `MERGE` of one key no longer
/// duplicates — the implicit MERGE-key guard makes losers abort retriably, so at
/// most one node is created. (Mirrors `ssi_occ_e2e.rs::concurrent_merge_same_key_yields_one_node`
/// but with **no** declared constraint — that's the point.)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_merge_without_constraint_no_duplicate() -> Result<()> {
    const WRITERS: usize = 16;
    for round in 0..3 {
        let db = Arc::new(e_db().await?);

        // Align all writers at the empty-match snapshot, then release them into
        // the MERGE together to maximise the phantom window.
        let gate = Arc::new(Barrier::new(WRITERS));
        let mut handles = Vec::new();
        for _ in 0..WRITERS {
            let db = db.clone();
            let gate = gate.clone();
            handles.push(tokio::spawn(async move {
                let session = db.session();
                let tx = session.tx().await?;
                gate.wait().await;
                tx.execute("MERGE (e:E {code: 'shared'})").await?;
                tx.commit().await.map(|_| ())
            }));
        }
        // Losers abort retriably (ConstraintConflict) — that's the guard working;
        // collect rather than `?` so an abort doesn't fail the test.
        let mut committed = 0;
        for h in handles {
            if h.await.expect("task panicked").is_ok() {
                committed += 1;
            }
        }
        assert!(
            committed >= 1,
            "round {round}: at least one MERGE must commit"
        );

        let count = count_e(&db).await?;
        assert_eq!(
            count, 1,
            "round {round}: concurrent MERGE of one key must yield exactly one node \
             (no insert-phantom), got {count}"
        );
    }
    Ok(())
}

/// FIXED: wrapped in the standard retry helper, concurrent `MERGE` of one key
/// converges to exactly one node with **all** writers succeeding (losers retry on
/// `ConstraintConflict` and observe the committed row). This is the pattern that
/// replaces uniko's `StripedLocks`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_merge_without_constraint_converges_with_retry() -> Result<()> {
    const WRITERS: usize = 16;
    let db = Arc::new(e_db().await?);

    let gate = Arc::new(Barrier::new(WRITERS));
    let mut handles = Vec::new();
    for _ in 0..WRITERS {
        let db = db.clone();
        let gate = gate.clone();
        handles.push(tokio::spawn(async move {
            let session = db.session();
            gate.wait().await;
            session
                .execute_with_retry("MERGE (e:E {code: 'shared'})")
                .await
                .map(|_| ())
        }));
    }
    for h in handles {
        // Every writer succeeds: the loser retries and matches the existing node.
        h.await.expect("task panicked")?;
    }

    assert_eq!(
        count_e(&db).await?,
        1,
        "retried concurrent MERGE must converge to exactly one node"
    );
    Ok(())
}

/// WISHLIST (not a bug): there is no server-side atomic `SET n = n + 1`, so N
/// aligned concurrent increments **without** a client retry loop do not all apply.
/// What the engine *does* guarantee — and what this guards — is soundness: every
/// increment that commits is reflected, with no lost update among committed
/// transactions (the rest abort retriably, they are not silently dropped).
///
/// The supported way to reach `n == N` is a retry loop — see
/// `ssi_occ_e2e.rs::atomic_increment_two_writers_converges` /
/// `atomic_increment_many_writers_converges`. A server-side atomic SET / CAS that
/// removes the need for the client retry loop is a documented wishlist.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn atomic_set_without_retry_is_sound_but_needs_retry_to_reach_n() -> Result<()> {
    const WRITERS: i64 = 8;
    let db = Arc::new({
        let db = Uni::in_memory().build().await?;
        db.schema()
            .label("Counter")
            .property("id", DataType::String)
            .property("n", DataType::Int)
            .done()
            .apply()
            .await?;
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Counter {id: 'x', n: 0})").await?;
        tx.commit().await?;
        db
    });

    let gate = Arc::new(Barrier::new(WRITERS as usize));
    let mut handles = Vec::new();
    for _ in 0..WRITERS {
        let db = db.clone();
        let gate = gate.clone();
        handles.push(tokio::spawn(async move {
            let session = db.session();
            let tx = session.tx().await?;
            gate.wait().await;
            // Plain write — no retry loop on purpose (the wishlist scenario).
            tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
                .await?;
            tx.commit().await.map(|_| ())
        }));
    }
    let mut committed = 0i64;
    for h in handles {
        if h.await.expect("task panicked").is_ok() {
            committed += 1;
        }
    }

    let r = db
        .session()
        .query("MATCH (c:Counter {id: 'x'}) RETURN c.n AS n")
        .await?;
    let n = r.rows()[0].get::<i64>("n")?;

    // Soundness: the counter equals the number of committed increments (no lost
    // update among committed txns), and at least one made progress. Reaching N
    // requires a retry loop (the wishlist), so we do NOT assert n == WRITERS here.
    assert!(committed >= 1, "at least one increment must commit");
    assert_eq!(
        n, committed,
        "counter ({n}) must equal committed increments ({committed}) — no lost \
         update among committed transactions"
    );
    Ok(())
}
