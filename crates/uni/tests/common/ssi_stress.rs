// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Stress / endurance: scale, hot-key contention, mixed OLAP+OLTP, and soak.
//!
//! Where the invariant-oracle tests (`ssi_invariants`) randomize *shape*, these
//! push *scale and duration*: many writers on hot keys, long analytical readers
//! racing high-frequency writers, and a sustained mixed workload that would
//! surface leaks (FOR UPDATE lock map, frozen generations) or livelock. The
//! heavy / long ones are `#[ignore]`d and run on demand:
//!
//! ```bash
//! cargo nextest run -p uni-db --features ssi --run-ignored all ssi_stress
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use uni_db::{DataType, RetryOptions, Uni, Value};

use crate::ssi_support::oracle;

async fn counter_db(keys: &[&str]) -> Result<Arc<Uni>> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Counter")
        .property("id", DataType::String)
        .property("n", DataType::Int)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    for k in keys {
        tx.execute(&format!("CREATE (:Counter {{id: '{k}', n: 0}})"))
            .await?;
    }
    tx.commit().await?;
    Ok(Arc::new(db))
}

async fn counter_of(db: &Uni, key: &str) -> Result<i64> {
    let r = db
        .session()
        .query(&format!("MATCH (c:Counter {{id: '{key}'}}) RETURN c.n AS n"))
        .await?;
    match r.rows()[0].value("n") {
        Some(Value::Int(n)) => Ok(*n),
        other => panic!("expected Int, got {other:?}"),
    }
}

// ── Hot-key contention strategies ────────────────────────────────────────────

/// 16 retried writers on ONE key converge to exactly 16 — the OCC+retry path
/// holds under sustained worst-case contention with no livelock.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn hot_key_ssi_retry_converges() -> Result<()> {
    const WRITERS: i64 = 16;
    let db = counter_db(&["x"]).await?;
    let mut handles = Vec::new();
    for _ in 0..WRITERS {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            db.session()
                .transact_with_retry(
                    RetryOptions {
                        max_attempts: 200,
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
    oracle::assert_counter(counter_of(&db, "x").await?, WRITERS);
    Ok(())
}

/// FOR UPDATE serializes hot-key writers pessimistically AND reads-latest under
/// the lock — so a `FOR UPDATE` read-modify-write converges with NO retry.
///
/// Semantics (Component C5): FOR UPDATE is an exact-key mutex; acquiring it on a
/// fresh transaction also re-pins the snapshot to lock-acquisition time, so each
/// serialized writer reads the latest committed value and its commit does not
/// conflict. This test pins the retry-free guarantee: 16 writers, no retry, exact
/// convergence. (Mixed read-then-FOR-UPDATE still needs retry — see
/// `ssi_for_update::read_before_for_update_keeps_begin_snapshot`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn hot_key_for_update_no_retry_converges() -> Result<()> {
    const WRITERS: i64 = 16;
    let db = counter_db(&["x"]).await?;
    let mut handles = Vec::new();
    for _ in 0..WRITERS {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            // No retry: any conflict surfaces as an Err and fails the test.
            let s = db.session();
            let tx = s.tx().await?;
            tx.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
                .await?;
            tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
                .await?;
            tx.commit().await.map(|_| ())
        }));
    }
    for h in handles {
        h.await.expect("task panicked")?;
    }
    oracle::assert_counter(counter_of(&db, "x").await?, WRITERS);
    Ok(())
}

// ── Mixed OLAP + OLTP ────────────────────────────────────────────────────────

/// Long-running analytical readers race high-frequency writers: the readers see
/// stable snapshots and never abort or block the writers, the writers converge,
/// and nothing deadlocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn mixed_olap_oltp_no_deadlock() -> Result<()> {
    const KEYS: &[&str] = &["a", "b", "c", "d"];
    const WRITES_PER_KEY: i64 = 10;
    let db = counter_db(KEYS).await?;
    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));

    // Hard cap: the whole workload must finish well under this bound. If it does
    // not, the test FAILS with a clear message instead of hanging — a deadlock or
    // livelock surfaces as a red test, never an 8-hour wedge.
    let workload = async {
        // Analytical readers: repeated aggregate scans (read-only → snapshot).
        let mut readers = Vec::new();
        for _ in 0..2 {
            let db = db.clone();
            let stop = stop.clone();
            let reads = reads.clone();
            readers.push(tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    db.session()
                        .query("MATCH (c:Counter) RETURN sum(c.n) AS total")
                        .await
                        .expect("analytical read must never abort");
                    reads.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        // Writers: each key incremented WRITES_PER_KEY times. A generous retry
        // budget ensures they converge under contention rather than giving up.
        let mut writers = Vec::new();
        for &k in KEYS {
            for _ in 0..WRITES_PER_KEY {
                let db = db.clone();
                writers.push(tokio::spawn(async move {
                    db.session()
                        .transact_with_retry(
                            RetryOptions {
                                max_attempts: 200,
                                ..Default::default()
                            },
                            move |tx| {
                                Box::pin(async move {
                                    tx.execute(&format!(
                                        "MATCH (c:Counter {{id: '{k}'}}) SET c.n = c.n + 1"
                                    ))
                                    .await?;
                                    Ok(())
                                })
                            },
                        )
                        .await
                        .expect("writer should converge");
                }));
            }
        }
        for w in writers {
            w.await.expect("writer panicked");
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.await.expect("reader panicked");
        }
    };

    tokio::time::timeout(Duration::from_secs(25), workload)
        .await
        .expect("mixed OLAP+OLTP workload did not finish in 25s — possible deadlock/livelock");

    // Writers converged...
    for &k in KEYS {
        oracle::assert_counter(counter_of(&db, k).await?, WRITES_PER_KEY);
    }
    // ...and the readers actually ran concurrently.
    assert!(reads.load(Ordering::Relaxed) > 0, "no analytical reads completed");
    Ok(())
}

// ── Scale sweep ──────────────────────────────────────────────────────────────

/// Throughput-shaped correctness: a sweep of writer counts on a small key space,
/// each converging exactly. Catches livelock / starvation as concurrency grows.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn writer_scaling_converges() -> Result<()> {
    for &writers in &[4i64, 12, 24] {
        let db = counter_db(&["x", "y"]).await?;
        let mut handles = Vec::new();
        for w in 0..writers {
            let db = db.clone();
            let key = if w % 2 == 0 { "x" } else { "y" };
            handles.push(tokio::spawn(async move {
                db.session()
                    .transact_with_retry(
                        RetryOptions {
                            max_attempts: 200,
                            ..Default::default()
                        },
                        move |tx| {
                            Box::pin(async move {
                                tx.execute(&format!(
                                    "MATCH (c:Counter {{id: '{key}'}}) SET c.n = c.n + 1"
                                ))
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
        let total = counter_of(&db, "x").await? + counter_of(&db, "y").await?;
        oracle::assert_counter(total, writers);
    }
    Ok(())
}

// ── Soak / endurance (heavy) ─────────────────────────────────────────────────

/// Sustained mixed workload over many rounds. Asserts final correctness AND that
/// the FOR UPDATE lock map returns to empty (no leak across thousands of
/// transactions — the G5 regression sentinel at scale).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "soak: runs a sustained workload over many rounds"]
async fn soak_mixed_workload() -> Result<()> {
    const ROUNDS: i64 = 200;
    const WRITERS: i64 = 8;
    let db = counter_db(&["x"]).await?;
    let writer = db.writer().expect("in-memory db has a writer");

    let mut expected = 0i64;
    for _ in 0..ROUNDS {
        let mut handles = Vec::new();
        for w in 0..WRITERS {
            let db = db.clone();
            // Half the writers use FOR UPDATE, half use OCC+retry — exercise both
            // mitigations interleaved.
            let use_lock = w % 2 == 0;
            handles.push(tokio::spawn(async move {
                let opts = RetryOptions {
                    max_attempts: 200,
                    ..Default::default()
                };
                if use_lock {
                    // FOR UPDATE under retry (it is a mutex, not read-latest).
                    db.session()
                        .transact_with_retry(opts, |tx| {
                            Box::pin(async move {
                                tx.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
                                    .await?;
                                tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
                                    .await?;
                                Ok(())
                            })
                        })
                        .await
                } else {
                    db.session()
                        .transact_with_retry(opts, |tx| {
                            Box::pin(async move {
                                tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
                                    .await?;
                                Ok(())
                            })
                        })
                        .await
                }
            }));
        }
        for h in handles {
            h.await.expect("task panicked")?;
        }
        expected += WRITERS;
    }

    oracle::assert_counter(counter_of(&db, "x").await?, expected);
    // No lock-map leak after thousands of FOR UPDATE acquisitions.
    assert_eq!(
        writer.for_update_lock_count(),
        0,
        "FOR UPDATE lock map leaked across the soak"
    );
    Ok(())
}

/// A brief settle to let any background flush tasks finish before the harness
/// tears down — keeps the soak from racing teardown when run standalone.
#[allow(dead_code)]
async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}
