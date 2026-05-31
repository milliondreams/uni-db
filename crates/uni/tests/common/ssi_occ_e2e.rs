// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end SSI/OCC tests driven through the public API with real concurrency.
//!
//! These spawn genuinely concurrent transactions (`tokio::spawn`) that contend
//! on `flush_lock`, exercising the actual race window — unlike the deterministic
//! writer-level simulations in `uni-store`. They cover the two wishlist
//! acceptance repros (atomic increment, serializable MERGE), the bounded-retry
//! helper, and read-write antidependency detection across the scan and traversal
//! read paths. See `docs/proposals/serializable_snapshot_isolation.md` (§10).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use uni_db::{DataType, RetryOptions, Uni, UniError, Value};

/// A `Counter` schema seeded with `{id: 'x', n: 0}`.
async fn counter_db() -> Result<Uni> {
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
    Ok(db)
}

/// Reads back the committed `n` for the `x` counter.
async fn counter_value(db: &Uni) -> Result<i64> {
    let r = db
        .session()
        .query("MATCH (c:Counter {id: 'x'}) RETURN c.n AS n")
        .await?;
    match r.rows()[0].value("n") {
        Some(Value::Int(n)) => Ok(*n),
        other => panic!("expected Int, got {other:?}"),
    }
}

// ── §10 acceptance repro 1: atomic increment ────────────────────────────────

/// Two concurrent read-modify-write increments, each retried on conflict, must
/// not lose an update: the final value is exactly 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_increment_two_writers_converges() -> Result<()> {
    let db = Arc::new(counter_db().await?);
    let mut handles = Vec::new();
    for _ in 0..2 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            db.session()
                .execute_with_retry("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
                .await
        }));
    }
    for h in handles {
        h.await.expect("task panicked")?;
    }
    assert_eq!(counter_value(&db).await?, 2, "no lost update");
    Ok(())
}

/// Stress: 16 concurrent retried increments converge to 16. Looped to shake out
/// nondeterministic interleavings of the real `flush_lock` race.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn atomic_increment_many_writers_converges() -> Result<()> {
    const WRITERS: i64 = 16;
    for _round in 0..3 {
        let db = Arc::new(counter_db().await?);
        let mut handles = Vec::new();
        for _ in 0..WRITERS {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                db.session()
                    .transact_with_retry(
                        // Headroom for 16-way contention; jittered backoff converges.
                        RetryOptions {
                            max_attempts: 64,
                            ..Default::default()
                        },
                        |tx| {
                            Box::pin(async move {
                                tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
                                    .await?;
                                Ok(())
                            })
                        },
                    )
                    .await
            }));
        }
        for h in handles {
            h.await.expect("task panicked")?;
        }
        assert_eq!(
            counter_value(&db).await?,
            WRITERS,
            "all increments converge"
        );
    }
    Ok(())
}

// ── §10 acceptance repro 2: serializable MERGE ──────────────────────────────

/// 16 concurrent `MERGE` of the same unique key yield exactly one node: losers
/// abort on the unique-key check, retry, and observe the existing row.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_merge_same_key_yields_one_node() -> Result<()> {
    let db = Arc::new({
        let db = Uni::in_memory().build().await?;
        db.schema()
            .label("E")
            .property("code", DataType::String)
            .done()
            .apply()
            .await?;
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE CONSTRAINT e_code ON (e:E) ASSERT e.code IS UNIQUE")
            .await?;
        tx.commit().await?;
        db
    });

    let mut handles = Vec::new();
    for _ in 0..16 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            let session = db.session();
            let tx = session.tx().await?;
            tx.execute("MERGE (e:E {code: 'shared'})").await?;
            tx.commit().await.map(|_| ())
        }));
    }
    // Losers surface a conflict / duplicate-key error — that is the unique-key
    // serialization at work. The guarantee under test is the final state: at
    // least one writer wins and exactly one node exists.
    let mut wins = 0;
    for h in handles {
        if h.await.expect("task panicked").is_ok() {
            wins += 1;
        }
    }
    assert!(wins >= 1, "at least one MERGE must commit");

    let r = db
        .session()
        .query("MATCH (e:E) RETURN count(e) AS c")
        .await?;
    match r.rows()[0].value("c") {
        Some(Value::Int(1)) => Ok(()),
        other => panic!("expected exactly one node, got count {other:?}"),
    }
}

// ── Happy paths ─────────────────────────────────────────────────────────────

/// Concurrent writers to disjoint vertices both commit (no false abort).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disjoint_writers_both_commit() -> Result<()> {
    let db = Arc::new(counter_db().await?);
    {
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Counter {id: 'y', n: 0})").await?;
        tx.commit().await?;
    }

    let run = |db: Arc<Uni>, id: &'static str| {
        tokio::spawn(async move {
            let session = db.session();
            let tx = session.tx().await?;
            tx.execute(&format!("MATCH (c:Counter {{id: '{id}'}}) SET c.n = 5"))
                .await?;
            tx.commit().await.map(|_| ())
        })
    };
    let h1 = run(db.clone(), "x");
    let h2 = run(db.clone(), "y");
    h1.await.expect("task panicked")?;
    h2.await.expect("task panicked")?;
    Ok(())
}

/// Read-only queries never abort or block under a stream of concurrent writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_queries_never_abort_under_writes() -> Result<()> {
    let db = Arc::new(counter_db().await?);
    let writer = {
        let db = db.clone();
        tokio::spawn(async move {
            for _ in 0..20 {
                db.session()
                    .execute_with_retry("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
                    .await
                    .expect("write should succeed");
            }
        })
    };
    let reader = {
        let db = db.clone();
        tokio::spawn(async move {
            for _ in 0..60 {
                db.session()
                    .query("MATCH (c:Counter {id: 'x'}) RETURN c.n")
                    .await
                    .expect("read-only query must never abort");
            }
        })
    };
    writer.await.expect("writer panicked");
    reader.await.expect("reader panicked");
    Ok(())
}

// ── Failure paths / antidependencies ────────────────────────────────────────

/// Read-write antidependency through a keyed label scan aborts: a transaction
/// that read `x` via a scan, then writes after a concurrent committer modifies
/// `x`, must abort. The read-set is captured AFTER the residual filter (by
/// `ReadSetRecordingExec`), so only the matched row `x` enters the read-set —
/// not the whole label (see `scan_read_disjoint_key_no_false_abort`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_read_antidependency_aborts() -> Result<()> {
    let db = counter_db().await?;
    let s_a = db.session();
    let tx_a = s_a.tx().await?;
    // tx_a reads x via a label scan — recorded in the read-set post-filter.
    tx_a.query("MATCH (c:Counter {id: 'x'}) RETURN c.n").await?;

    // A concurrent transaction writes x and commits.
    {
        let s_b = db.session();
        let tx_b = s_b.tx().await?;
        tx_b.execute("MATCH (c:Counter {id: 'x'}) SET c.n = 100")
            .await?;
        tx_b.commit().await?;
    }

    // tx_a writes an unrelated vertex and commits — must abort, because x (which
    // tx_a read) was concurrently written: a read-write antidependency.
    tx_a.execute("CREATE (:Counter {id: 'y', n: 0})").await?;
    match tx_a.commit().await {
        Err(UniError::SerializationConflict { .. }) => Ok(()),
        other => panic!("expected SerializationConflict, got {other:?}"),
    }
}

/// Post-filter read-set precision: a transaction that read `x` via a keyed scan
/// does NOT abort when a *disjoint* vertex `y` is concurrently written. If the
/// scan over-recorded the whole label (pre-filter), this would falsely conflict
/// — this pins that `ReadSetRecordingExec` records only the matched row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_read_disjoint_key_no_false_abort() -> Result<()> {
    let db = counter_db().await?;
    {
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Counter {id: 'y', n: 0})").await?;
        tx.commit().await?;
    }

    let s_a = db.session();
    let tx_a = s_a.tx().await?;
    // tx_a reads only x (the residual filter narrows the Counter scan to x).
    tx_a.query("MATCH (c:Counter {id: 'x'}) RETURN c.n").await?;

    // A concurrent transaction writes the disjoint vertex y and commits.
    {
        let s_b = db.session();
        let tx_b = s_b.tx().await?;
        tx_b.execute("MATCH (c:Counter {id: 'y'}) SET c.n = 100")
            .await?;
        tx_b.commit().await?;
    }

    // tx_a writes an unrelated vertex and commits — must SUCCEED: x was not
    // written, and y was never in tx_a's read-set.
    tx_a.execute("CREATE (:Counter {id: 'z', n: 0})").await?;
    tx_a.commit().await?;
    Ok(())
}

/// A transaction that *traversed* to a neighbour vertex aborts when that
/// neighbour is concurrently written — exercising the traversal read-set hook.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traversal_antidependency_aborts() -> Result<()> {
    let db = {
        let db = Uni::in_memory().build().await?;
        db.schema()
            .label("N")
            .property("id", DataType::String)
            .property("v", DataType::Int)
            .done()
            .edge_type("R", &["N"], &["N"])
            .done()
            .apply()
            .await?;
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (a:N {id: 'a', v: 0})-[:R]->(b:N {id: 'b', v: 0})")
            .await?;
        tx.commit().await?;
        db
    };

    let s_a = db.session();
    let tx_a = s_a.tx().await?;
    // Traverse a -> b. `RETURN a.id` does not hydrate b, so b enters the
    // read-set only via the traversal hook (`record_neighbor_reads`).
    tx_a.query("MATCH (a:N {id: 'a'})-[r:R]->(nbr) RETURN a.id")
        .await?;

    // Concurrently modify the neighbour b and commit.
    {
        let s_b = db.session();
        let tx_b = s_b.tx().await?;
        tx_b.execute("MATCH (b:N {id: 'b'}) SET b.v = 1").await?;
        tx_b.commit().await?;
    }

    tx_a.execute("CREATE (:N {id: 'c', v: 0})").await?;
    match tx_a.commit().await {
        Err(UniError::SerializationConflict { .. }) => Ok(()),
        other => panic!("expected SerializationConflict, got {other:?}"),
    }
}

/// A transaction that read an *edge* aborts when that edge is concurrently
/// modified — exercising edge read-set capture.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edge_read_antidependency_aborts() -> Result<()> {
    let db = {
        let db = Uni::in_memory().build().await?;
        db.schema()
            .label("N")
            .property("id", DataType::String)
            .property("v", DataType::Int)
            .done()
            .edge_type("R", &["N"], &["N"])
            .property("w", DataType::Int)
            .done()
            .apply()
            .await?;
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (a:N {id: 'a', v: 0})-[:R {w: 0}]->(b:N {id: 'b', v: 0})")
            .await?;
        tx.commit().await?;
        db
    };

    let s_a = db.session();
    let tx_a = s_a.tx().await?;
    // Read the edge — records its id in tx_a's read-set.
    tx_a.query("MATCH (a:N {id: 'a'})-[r:R]->(b) RETURN r.w")
        .await?;

    // Concurrently modify that edge and commit.
    {
        let s_b = db.session();
        let tx_b = s_b.tx().await?;
        tx_b.execute("MATCH (a:N {id: 'a'})-[r:R]->(b) SET r.w = 1")
            .await?;
        tx_b.commit().await?;
    }

    tx_a.execute("CREATE (:N {id: 'c', v: 0})").await?;
    match tx_a.commit().await {
        Err(UniError::SerializationConflict { .. }) => Ok(()),
        other => panic!("expected SerializationConflict, got {other:?}"),
    }
}

/// A *schemaless* traversal aborts on a concurrent neighbour write. An undeclared
/// edge type routes the traversal through `TraverseMainByType` →
/// `build_edge_adjacency_map`, which scans the whole edge type — exercising the
/// schemaless read-set hook (`record_edge_adjacency`), distinct from the
/// schema-typed `record_neighbor_reads` path covered above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_traversal_antidependency_aborts() -> Result<()> {
    let db = {
        let db = Uni::in_memory().build().await?;
        // Declare the vertex label but NOT edge type `R`: an unknown edge type
        // routes the traversal through the schemaless main path.
        db.schema()
            .label("N")
            .property("id", DataType::String)
            .property("v", DataType::Int)
            .done()
            .apply()
            .await?;
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (a:N {id: 'a', v: 0})-[:R]->(b:N {id: 'b', v: 0})")
            .await?;
        tx.commit().await?;
        db
    };

    let s_a = db.session();
    let tx_a = s_a.tx().await?;
    // Schemaless traversal a -> b; b enters the read-set via the schemaless
    // adjacency hook. `RETURN a.id` does not hydrate b.
    tx_a.query("MATCH (a:N {id: 'a'})-[:R]->(nbr) RETURN a.id")
        .await?;

    // Concurrently modify the neighbour b and commit.
    {
        let s_b = db.session();
        let tx_b = s_b.tx().await?;
        tx_b.execute("MATCH (b:N {id: 'b'}) SET b.v = 1").await?;
        tx_b.commit().await?;
    }

    tx_a.execute("CREATE (:N {id: 'c', v: 0})").await?;
    match tx_a.commit().await {
        Err(UniError::SerializationConflict { .. }) => Ok(()),
        other => panic!("expected SerializationConflict, got {other:?}"),
    }
}

/// A read-only transaction is never a serialization pivot: a concurrent writer to
/// a row the reader observed still commits, and the read-only transaction itself
/// commits cleanly afterwards (empty write-set → snapshot isolation, §7).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_transaction_is_not_a_pivot() -> Result<()> {
    let db = counter_db().await?;

    let s_r = db.session();
    let tx_r = s_r.tx().await?;
    // Read x inside a transaction (records a read-set), but never write.
    tx_r.query("MATCH (c:Counter {id: 'x'}) RETURN c.n").await?;

    // A writer modifies the same row x and commits — must succeed; a concurrent
    // reader is never a write-conflict pivot.
    {
        let s_w = db.session();
        let tx_w = s_w.tx().await?;
        tx_w.execute("MATCH (c:Counter {id: 'x'}) SET c.n = 5").await?;
        tx_w.commit().await?;
    }

    // The read-only transaction commits cleanly even though x changed under it.
    tx_r.commit().await?;
    assert_eq!(counter_value(&db).await?, 5);
    Ok(())
}

/// VidLookupJoin → HashJoin fallback regression: under an active SSI read-set, a
/// cross-MATCH equi-join (`id(b) = a.linked_vid`, the `VidLookupJoinExec` path)
/// must fall back to a HashJoin so the probed target's read is recorded. A
/// concurrent write to that target then aborts the reader. If the fast path were
/// taken, it would probe `b` by vid and bypass `ReadSetRecordingExec`, so `b`
/// would never enter the read-set and tx_a would NOT abort.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vid_lookup_join_records_reads_via_fallback() -> Result<()> {
    let db = {
        let db = Uni::in_memory().build().await?;
        db.schema()
            .label("Source")
            .property("name", DataType::String)
            .property_nullable("linked_vid", DataType::Int64)
            .property_nullable("score", DataType::Float64)
            .done()
            .label("Target")
            .property("name", DataType::String)
            .done()
            .apply()
            .await?;
        db
    };

    // Create target `b`, then a source linked to it by vid.
    let b_vid: i64 = {
        let s = db.session();
        let tx = s.tx().await?;
        let r = tx
            .query("CREATE (n:Target {name: 'b'}) RETURN id(n) AS vid")
            .await?;
        let vid = match r.rows()[0].value("vid") {
            Some(Value::Int(v)) => *v,
            other => panic!("expected Int vid, got {other:?}"),
        };
        tx.commit().await?;
        vid
    };
    {
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute(&format!(
            "CREATE (:Source {{name: 'a', linked_vid: {b_vid}, score: 0.9}})"
        ))
        .await?;
        tx.commit().await?;
    }

    // RW tx_a reads `b` through the VidLookupJoin-eligible cross-MATCH.
    let s_a = db.session();
    let tx_a = s_a.tx().await?;
    tx_a.query(
        "MATCH (a:Source) WHERE a.score > 0.5 \
         MATCH (b:Target) WHERE id(b) = a.linked_vid \
         RETURN b.name",
    )
    .await?;

    // Concurrently modify the probed target `b` and commit.
    {
        let s_b = db.session();
        let tx_b = s_b.tx().await?;
        tx_b.execute("MATCH (b:Target {name: 'b'}) SET b.name = 'changed'")
            .await?;
        tx_b.commit().await?;
    }

    // tx_a writes a disjoint vertex and commits — must abort, because it read `b`
    // (via the join) which was concurrently written.
    tx_a.execute("CREATE (:Target {name: 'c'})").await?;
    match tx_a.commit().await {
        Err(UniError::SerializationConflict { .. }) => Ok(()),
        other => panic!("expected SerializationConflict, got {other:?}"),
    }
}

// ── Retry-helper semantics ──────────────────────────────────────────────────

/// When conflicts persist past `max_attempts`, the underlying conflict error is
/// returned (not swallowed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_exhaustion_returns_conflict() -> Result<()> {
    let db = Arc::new(counter_db().await?);
    let interferer = db.clone();
    // max_attempts = 1: no retry. The closure deterministically forces a conflict
    // by committing a concurrent write to the same row after this tx began.
    let res: uni_db::Result<()> = db
        .session()
        .transact_with_retry(
            RetryOptions {
                max_attempts: 1,
                ..Default::default()
            },
            move |tx| {
                let interferer = interferer.clone();
                Box::pin(async move {
                    tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
                        .await?;
                    interferer
                        .session()
                        .execute_with_retry("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
                        .await?;
                    Ok(())
                })
            },
        )
        .await;
    match res {
        Err(UniError::SerializationConflict { .. }) => Ok(()),
        other => panic!("expected SerializationConflict, got {other:?}"),
    }
}

/// A non-retriable error (a parse failure) surfaces immediately, without retry.
#[tokio::test]
async fn non_retriable_error_surfaces_without_retry() -> Result<()> {
    let db = counter_db().await?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let res: uni_db::Result<()> = db
        .session()
        .transact_with_retry(RetryOptions::default(), move |tx| {
            let counter = counter.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                // Malformed Cypher → deterministic parse error.
                tx.execute("MATCH (n RETURN n").await?;
                Ok(())
            })
        })
        .await;
    assert!(res.is_err(), "bad query must fail");
    assert!(
        !matches!(res, Err(UniError::SerializationConflict { .. })),
        "a parse error is not a conflict"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "non-retriable error must not be retried"
    );
    Ok(())
}
