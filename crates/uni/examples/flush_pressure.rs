// Measures aggregate cost of mid-commit flushes (the path async-flush
// proposes to parallelize).
//
// Runs the same workload (N sessions × M tx-per-session × K vertices-per-tx)
// under two configurations:
//
//   AUTO_FLUSH ON: default auto_flush_threshold = 10_000 mutations. With
//   enough total mutations, several flushes trigger during commits and
//   serialize through flush_lock.
//
//   AUTO_FLUSH OFF: auto_flush_threshold = usize::MAX so check_flush never
//   triggers flush_to_l1. A single final explicit db.flush() drains
//   everything at the end of the run.
//
// The wall-time delta between OFF and ON, at high concurrency, is the
// aggregate cost of mid-commit flushes that async-flush would amortize.
//
// Run with: cargo run --release --example flush_pressure

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimalloc::MiMalloc;
use uni_common::config::UniConfig;
use uni_db::{Uni, Value};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const SESSIONS: usize = 24;
const TXS_PER_SESSION: usize = 200;
const VERTICES_PER_TX: usize = 25;
const REPS: usize = 3;
// Per round: 24 × 200 × 25 = 120,000 mutations spread across 4,800 commits.
// Pattern: many tiny txs at high concurrency — closest to uniko-style
// ingestion (lots of small per-message commits). Each tx is 25 mutations,
// so check_flush fires every 400 commits (10_000 / 25).
// Default auto_flush_threshold = 10,000 → ~12 flushes expected.

async fn build_db_with_threshold(threshold: usize) -> anyhow::Result<Arc<Uni>> {
    build_db_with_settings(threshold, false).await
}

async fn build_db_with_settings(threshold: usize, async_flush: bool) -> anyhow::Result<Arc<Uni>> {
    let config = UniConfig {
        auto_flush_threshold: threshold,
        auto_flush_interval: None, // disable time-based; isolate count-based
        async_flush_enabled: async_flush,
        ..Default::default()
    };
    let db = Arc::new(Uni::in_memory().config(config).build().await?);
    let s = db.session();
    let t = s.tx().await?;
    t.execute("CREATE LABEL Person (idx INT, sess INT, txn INT)")
        .await?;
    t.commit().await?;
    Ok(db)
}

async fn build_db(auto_flush_on: bool) -> anyhow::Result<Arc<Uni>> {
    if auto_flush_on {
        build_db_with_threshold(10_000).await
    } else {
        build_db_with_threshold(usize::MAX).await
    }
}

async fn run_one_task(db: Arc<Uni>, task_id: usize) -> anyhow::Result<(Duration, Duration)> {
    let session = db.session();
    let mut total_exec = Duration::ZERO;
    let mut total_commit = Duration::ZERO;
    for txn in 0..TXS_PER_SESSION {
        let tx = session.tx().await?;
        let mut props: Vec<HashMap<String, Value>> = Vec::with_capacity(VERTICES_PER_TX);
        for i in 0..VERTICES_PER_TX {
            props.push(HashMap::from([
                ("idx".to_string(), Value::Int(i as i64)),
                ("sess".to_string(), Value::Int(task_id as i64)),
                ("txn".to_string(), Value::Int(txn as i64)),
            ]));
        }
        let exec_start = Instant::now();
        tx.bulk_insert_vertices("Person", props).await?;
        total_exec += exec_start.elapsed();
        let commit_start = Instant::now();
        tx.commit().await?;
        total_commit += commit_start.elapsed();
    }
    Ok((total_exec, total_commit))
}

async fn one_rep_with_threshold(
    threshold: usize,
) -> anyhow::Result<(Duration, Vec<Duration>, Vec<Duration>)> {
    let db = build_db_with_threshold(threshold).await?;
    one_rep_inner(db, threshold == usize::MAX).await
}

async fn one_rep(auto_flush_on: bool) -> anyhow::Result<(Duration, Vec<Duration>, Vec<Duration>)> {
    let db = build_db(auto_flush_on).await?;
    one_rep_inner(db, !auto_flush_on).await
}

async fn one_rep_inner(
    db: Arc<Uni>,
    drain_at_end: bool,
) -> anyhow::Result<(Duration, Vec<Duration>, Vec<Duration>)> {
    let wall_start = Instant::now();
    let mut handles = Vec::with_capacity(SESSIONS);
    for s in 0..SESSIONS {
        let db = db.clone();
        handles.push(tokio::spawn(async move { run_one_task(db, s).await }));
    }
    let mut execs = Vec::with_capacity(SESSIONS);
    let mut commits = Vec::with_capacity(SESSIONS);
    for h in handles {
        let (e, c) = h.await??;
        execs.push(e);
        commits.push(c);
    }
    // Drain with one explicit flush at the end (for the OFF case).
    if drain_at_end {
        db.flush().await?;
    }
    let wall = wall_start.elapsed();
    Ok((wall, execs, commits))
}

fn mean(ds: &[Duration]) -> Duration {
    let nanos: u128 = ds.iter().map(|d| d.as_nanos()).sum();
    Duration::from_nanos((nanos / ds.len() as u128) as u64)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!(
        "Flush-pressure measurement\n  {} sessions × {} tx × {} vertices = {} total mutations/rep × {} reps",
        SESSIONS,
        TXS_PER_SESSION,
        VERTICES_PER_TX,
        SESSIONS * TXS_PER_SESSION * VERTICES_PER_TX,
        REPS
    );
    println!(
        "  Default auto_flush_threshold = 10_000 → expect ~{} flushes per rep with AUTO_FLUSH ON",
        (SESSIONS * TXS_PER_SESSION * VERTICES_PER_TX) / 10_000
    );
    println!();
    println!(
        " {:>16}  {:>12}  {:>14}  {:>14}",
        "config", "wall", "exec_mean", "commit_mean"
    );
    println!(
        " {:>16}  {:>12}  {:>14}  {:>14}",
        "----------------", "------------", "--------------", "--------------"
    );

    let mut on_walls = Vec::new();
    let mut on_execs = Vec::new();
    let mut on_commits = Vec::new();
    for _ in 0..REPS {
        let (w, e, c) = one_rep(true).await?;
        on_walls.push(w);
        on_execs.extend(e);
        on_commits.extend(c);
    }
    println!(
        " {:>16}  {:>12?}  {:>14?}  {:>14?}",
        "AUTO_FLUSH ON",
        mean(&on_walls),
        mean(&on_execs),
        mean(&on_commits)
    );

    let mut off_walls = Vec::new();
    let mut off_execs = Vec::new();
    let mut off_commits = Vec::new();
    for _ in 0..REPS {
        let (w, e, c) = one_rep(false).await?;
        off_walls.push(w);
        off_execs.extend(e);
        off_commits.extend(c);
    }
    println!(
        " {:>16}  {:>12?}  {:>14?}  {:>14?}",
        "AUTO_FLUSH OFF",
        mean(&off_walls),
        mean(&off_execs),
        mean(&off_commits)
    );

    let on_w = mean(&on_walls).as_secs_f64();
    let off_w = mean(&off_walls).as_secs_f64();
    let delta = on_w - off_w;
    let ratio = if off_w > 0.0 { on_w / off_w } else { 0.0 };
    println!();
    println!("Wall-time gap (ON - OFF) = {:.3} s = {:.1}x", delta, ratio);
    println!();

    // Sweep auto_flush_threshold to see the cost curve. Smaller threshold =
    // more frequent flushes; larger = bigger per-flush work. If async-flush
    // would help, we'd see wall-time hump at some "many medium flushes"
    // regime where serialization dominates.
    println!("Threshold sweep (same workload, varying auto_flush_threshold):");
    println!(
        " {:>12}  {:>8}  {:>12}  {:>14}",
        "threshold", "flushes", "wall", "ns/mutation"
    );
    println!(
        " {:>12}  {:>8}  {:>12}  {:>14}",
        "------------", "--------", "------------", "--------------"
    );
    let total_muts = SESSIONS * TXS_PER_SESSION * VERTICES_PER_TX;
    for &thr in &[
        1_000usize,
        2_500,
        5_000,
        10_000,
        25_000,
        50_000,
        100_000,
        usize::MAX,
    ] {
        let mut walls = Vec::new();
        for _ in 0..REPS {
            let (w, _, _) = one_rep_with_threshold(thr).await?;
            walls.push(w);
        }
        let wall = mean(&walls);
        let expected_flushes = if thr == usize::MAX {
            1 // just the explicit end-of-rep drain
        } else {
            total_muts.div_ceil(thr)
        };
        let ns_per_mut = wall.as_nanos() / (total_muts as u128);
        let thr_display = if thr == usize::MAX {
            "MAX".to_string()
        } else {
            thr.to_string()
        };
        println!(
            " {:>12}  {:>8}  {:>12?}  {:>11} ns",
            thr_display, expected_flushes, wall, ns_per_mut
        );
    }

    // The headline comparison: sync vs async flush at varying thresholds.
    println!("\n--- ASYNC vs SYNC flush comparison (sess=24, varying threshold) ---");
    println!(
        " {:>10}  {:>14}  {:>14}  {:>10}",
        "threshold", "sync wall", "async wall", "speedup"
    );
    println!(
        " {:>10}  {:>14}  {:>14}  {:>10}",
        "----------", "--------------", "--------------", "----------"
    );
    for &thr in &[2_500usize, 5_000, 10_000, 25_000] {
        let mut sync_walls = Vec::new();
        let mut async_walls = Vec::new();
        for _ in 0..REPS {
            // SYNC
            let db = build_db_with_settings(thr, false).await?;
            let (w, _, _) = one_rep_inner(db, true).await?;
            sync_walls.push(w);
            // ASYNC — drain at end so we measure all the work, not just
            // what happens before the spawned tasks complete.
            let db = build_db_with_settings(thr, true).await?;
            let (w, _, _) = one_rep_inner(db, true).await?;
            async_walls.push(w);
        }
        let s = mean(&sync_walls);
        let a = mean(&async_walls);
        let speedup = s.as_secs_f64() / a.as_secs_f64();
        println!(" {:>10}  {:>14?}  {:>14?}  {:>9.2}x", thr, s, a, speedup);
    }

    Ok(())
}
