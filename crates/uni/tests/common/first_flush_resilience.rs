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

// ---------------------------------------------------------------------------
// Issue #281 — the same crash window, with edges.
// ---------------------------------------------------------------------------

const EDGE_ENTRY: &str = "first_flush_resilience::first_flush_edge_abort_child";

/// Commits vertices and edges into a brand-new store, then aborts.
///
/// `scenario` selects the shape:
/// * `with_edges` — three vertices and two edges in one transaction.
/// * `vertices_only` — the control: five vertices across two transactions and
///   no edges at all, so a recovery that is broken generally fails here too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "child-process entry point; driven by run_child"]
async fn first_flush_edge_abort_child() {
    let Some((scenario, path)) = crash_harness::child_env() else {
        return;
    };

    let db = Uni::open(path.to_string_lossy())
        .build()
        .await
        .expect("open store");
    db.schema()
        .label("Entity")
        .property("name", DataType::String)
        .done()
        .edge_type("OWNS", &["Entity"], &["Entity"])
        .property("pct", DataType::Float)
        .done()
        .apply()
        .await
        .expect("register schema");

    let session = db.session();
    match scenario.as_str() {
        "with_edges" => {
            let tx = session.tx().await.expect("begin tx");
            for name in ["N6", "N8", "N1"] {
                tx.execute_with("CREATE (:Entity {name: $n})")
                    .param("n", name)
                    .run()
                    .await
                    .expect("create vertex");
            }
            for (owner, asset, pct) in [("N6", "N8", 11.64_f64), ("N8", "N1", 59.24)] {
                tx.execute_with(
                    "MATCH (o:Entity {name: $o}), (a:Entity {name: $a}) \
                     CREATE (o)-[:OWNS {pct: $p}]->(a)",
                )
                .param("o", owner)
                .param("a", asset)
                .param("p", pct)
                .run()
                .await
                .expect("create edge");
            }
            tx.commit().await.expect("commit");
        }
        "flush_then_more_edges" => {
            let tx = session.tx().await.expect("begin tx");
            for name in ["N6", "N8", "N1"] {
                tx.execute_with("CREATE (:Entity {name: $n})")
                    .param("n", name)
                    .run()
                    .await
                    .expect("create vertex");
            }
            for (owner, asset, pct) in [("N6", "N8", 11.64_f64), ("N8", "N1", 59.24)] {
                tx.execute_with(
                    "MATCH (o:Entity {name: $o}), (a:Entity {name: $a}) \
                     CREATE (o)-[:OWNS {pct: $p}]->(a)",
                )
                .param("o", owner)
                .param("a", asset)
                .param("p", pct)
                .run()
                .await
                .expect("create edge");
            }
            tx.commit().await.expect("commit");

            // These edges reach L1, so on reopen they arrive via the Main CSR.
            db.flush().await.expect("flush");

            // These do not: they live only in the WAL and arrive via replay.
            let tx = session.tx().await.expect("begin tx");
            tx.execute_with("CREATE (:Entity {name: $n})")
                .param("n", "N9")
                .run()
                .await
                .expect("create vertex");
            for (owner, asset, pct) in [("N1", "N9", 1.5_f64), ("N9", "N6", 2.5)] {
                tx.execute_with(
                    "MATCH (o:Entity {name: $o}), (a:Entity {name: $a}) \
                     CREATE (o)-[:OWNS {pct: $p}]->(a)",
                )
                .param("o", owner)
                .param("a", asset)
                .param("p", pct)
                .run()
                .await
                .expect("create edge");
            }
            tx.commit().await.expect("commit");
        }
        "vertices_only" => {
            for batch in [["N6", "N8", "N1"].as_slice(), ["X1", "X2"].as_slice()] {
                let tx = session.tx().await.expect("begin tx");
                for name in batch {
                    tx.execute_with("CREATE (:Entity {name: $n})")
                        .param("n", *name)
                        .run()
                        .await
                        .expect("create vertex");
                }
                tx.commit().await.expect("commit");
            }
        }
        other => crash_harness::unknown_scenario(EDGE_ENTRY, other),
    }

    std::process::abort();
}

/// Counts committed vertices and `OWNS` edges after reopening `store`.
async fn survey_after_recovery(store: &Path) -> (i64, i64) {
    let db = Uni::open(store.to_string_lossy())
        .build()
        .await
        .expect("reopen a store whose only defect is that it never flushed");
    let session = db.session();

    let vertices = session
        .query("MATCH (e:Entity) RETURN count(*) AS n")
        .await
        .expect("count vertices")
        .rows()[0]
        .get::<i64>("n")
        .expect("n");
    let edges = session
        .query("MATCH (:Entity)-[:OWNS]->(:Entity) RETURN count(*) AS n")
        .await
        .expect("count edges")
        .rows()[0]
        .get::<i64>("n")
        .expect("n");

    (vertices, edges)
}

/// Issue #281: a crash before the first flush recovered every committed vertex
/// and none of the committed edges, with no error.
///
/// Issue #275 made this store reopen at all; the edges were still missing,
/// because WAL replay restored them into the L0 buffer without mirroring them
/// into the adjacency overlay that the traversal read path actually reads. The
/// vertex assertion is the control — it passed on the buggy build, so it is
/// what distinguishes "edges are lost" from "nothing recovered".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_before_first_flush_keeps_edges() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = dir.path().join("store");

    crash_harness::run_child_async(EDGE_ENTRY, "with_edges", &store).await;

    let (vertices, edges) = survey_after_recovery(&store).await;
    assert_eq!(
        vertices, 3,
        "control: no committed vertices came back either, so this run says \
         nothing about the edge path"
    );
    assert_eq!(
        edges, 2,
        "recovery returned all {vertices} committed vertices but {edges} of 2 \
         committed edges (issue #281)"
    );
}

/// Control for the above: identical crash, no edges anywhere. If this fails,
/// the defect is in first-flush recovery generally and not in the edge path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_before_first_flush_vertices_only_control() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = dir.path().join("store");

    crash_harness::run_child_async(EDGE_ENTRY, "vertices_only", &store).await;

    let (vertices, edges) = survey_after_recovery(&store).await;
    assert_eq!(vertices, 5, "vertices-only recovery is incomplete");
    assert_eq!(edges, 0, "no edges were ever written");
}

// ---------------------------------------------------------------------------
// Timestamps across recovery.
// ---------------------------------------------------------------------------

/// Reads a `Temporal` value's nanos, or `None` if the column came back NULL.
fn nanos(v: Option<&uni_common::Value>) -> Option<i64> {
    use uni_common::Value;
    use uni_common::value::TemporalValue;
    match v? {
        Value::Temporal(TemporalValue::DateTime {
            nanos_since_epoch, ..
        })
        | Value::Temporal(TemporalValue::LocalDateTime {
            nanos_since_epoch, ..
        }) => Some(*nanos_since_epoch),
        _ => None,
    }
}

fn wall_nanos() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos() as i64
}

/// `created_at` / `updated_at` are stamped by the live write path and are
/// user-visible through the `created_at(n)` Cypher function. Nothing else in a
/// WAL record can reconstruct them, so before they were carried in the record
/// a recovered row came back NULL and the next flush made that permanent.
///
/// The bound that makes this test mean something is the upper one: recovery
/// runs strictly after the child process has exited, so a timestamp invented at
/// recovery time — rather than restored from the WAL — lands above
/// `child_exited` and fails. Asserting merely "not null" would pass against a
/// `now()` fabricated during replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_before_first_flush_keeps_timestamps() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = dir.path().join("store");

    let before_child = wall_nanos();
    crash_harness::run_child_async(EDGE_ENTRY, "with_edges", &store).await;
    let child_exited = wall_nanos();

    let db = Uni::open(store.to_string_lossy())
        .build()
        .await
        .expect("reopen");
    let session = db.session();

    let vertex = session
        .query("MATCH (e:Entity {name: 'N6'}) RETURN created_at(e) AS c, updated_at(e) AS u")
        .await
        .expect("query vertex timestamps");
    let edge = session
        .query(
            "MATCH (:Entity)-[r:OWNS]->(:Entity) \
             RETURN created_at(r) AS c, updated_at(r) AS u LIMIT 1",
        )
        .await
        .expect("query edge timestamps");

    for (what, rows) in [("vertex", &vertex), ("edge", &edge)] {
        let row = rows.rows().first().unwrap_or_else(|| {
            panic!(
                "control: no {what} row came back at all, so this run says nothing about timestamps"
            )
        });
        for field in ["c", "u"] {
            let ts = nanos(row.value(field)).unwrap_or_else(|| {
                panic!(
                    "{what} {field} came back NULL after recovery — the WAL record did not carry it"
                )
            });
            assert!(
                ts >= before_child && ts <= child_exited,
                "{what} {field} = {ts} is outside the child's lifetime \
                 [{before_child}, {child_exited}] — it was invented at recovery \
                 time rather than restored from the WAL"
            );
        }
    }
}

/// Recovery must MERGE the two sides, not double-count them.
///
/// `mirror_edges_into_adjacency` pushes every edge in the recovered L0 buffer
/// into the adjacency overlay. The Main CSR is built separately, from L1. If
/// the replayed WAL range were to include edges that had already been flushed,
/// those edges would land in both structures and a traversal could return them
/// twice — trading a silent undercount for a silent overcount, which is the
/// obvious way to "fix" issues #281/#282 and make things worse.
///
/// This crashes with two edges in L1 and two more only in the WAL, so recovery
/// has to reconcile both. Exact counts, and a de-duplicated pair list, are the
/// assertions: 4 means merged, 6 would mean the flushed pair came back twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_merges_flushed_and_replayed_edges_without_duplicating() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = dir.path().join("store");

    crash_harness::run_child_async(EDGE_ENTRY, "flush_then_more_edges", &store).await;

    let db = Uni::open(store.to_string_lossy())
        .build()
        .await
        .expect("reopen");
    let session = db.session();

    let (vertices, edges) = {
        let v = session
            .query("MATCH (e:Entity) RETURN count(*) AS n")
            .await
            .expect("count vertices")
            .rows()[0]
            .get::<i64>("n")
            .expect("n");
        let e = session
            .query("MATCH (:Entity)-[:OWNS]->(:Entity) RETURN count(*) AS n")
            .await
            .expect("count edges")
            .rows()[0]
            .get::<i64>("n")
            .expect("n");
        (v, e)
    };

    assert_eq!(
        vertices, 4,
        "expected 3 flushed vertices plus 1 from the WAL"
    );
    assert_eq!(
        edges, 4,
        "expected 2 flushed edges plus 2 from the WAL; 6 would mean the flushed          pair was mirrored into the overlay on top of the Main CSR and counted twice"
    );

    let rows = session
        .query("MATCH (a:Entity)-[:OWNS]->(b:Entity) RETURN a.name AS a, b.name AS b")
        .await
        .expect("list edges");
    let mut pairs: Vec<(String, String)> = rows
        .rows()
        .iter()
        .map(|r| {
            (
                r.get::<String>("a").expect("a"),
                r.get::<String>("b").expect("b"),
            )
        })
        .collect();
    pairs.sort();
    let mut deduped = pairs.clone();
    deduped.dedup();
    assert_eq!(
        pairs, deduped,
        "a committed edge came back more than once: {pairs:?}"
    );
    assert_eq!(
        pairs,
        vec![
            ("N1".to_string(), "N9".to_string()),
            ("N6".to_string(), "N8".to_string()),
            ("N8".to_string(), "N1".to_string()),
            ("N9".to_string(), "N6".to_string()),
        ]
    );
}
