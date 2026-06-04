// End-to-end edge-commit contention microbench.
//
// Spawns N tokio tasks. Each task:
//   1. Pre-creates 2 vertices (source, dest)
//   2. Inside a tx, calls bulk_insert_edges with EDGES_PER_TX edges
//   3. Commits the tx
//
// The bulk_insert_edges path goes through writer.insert_edge() which writes
// only to tx_l0 (skipping adjacency). Adjacency dual-write happens at commit
// time inside flush_lock. So this measures the COMMIT-TIME adjacency cost,
// which is what uniko actually experiences.
//
// Two patterns:
//   HOT: every task uses the SAME edge type
//   COLD: each task uses a DISTINCT edge type
//
// If commit-time adjacency replay is shard-contended, HOT will be slower
// than COLD at high N. If commit-time is dominated by flush_lock serialization
// (which is edge-type-agnostic), HOT and COLD will look the same.
//
// Run with: cargo run --release --example edge_commit_contention

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimalloc::MiMalloc;
use uni_db::{Uni, Value};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const EDGES_PER_TX: usize = 200;
const REPS: usize = 5;
const SESSIONS_SWEEP: &[usize] = &[1, 4, 12, 24];
const EDGE_COUNT_SWEEP: &[usize] = &[1, 10, 100, 1000];

#[derive(Clone, Copy)]
enum Pattern {
    Hot,  // edge type "EDGE_1" for every task
    Cold, // task i uses "EDGE_i"
}

async fn setup_db(pattern: Pattern, n_sessions: usize) -> anyhow::Result<Arc<Uni>> {
    let db = Arc::new(Uni::in_memory().build().await?);
    // Pre-create vertex label + edge type(s) via DDL so bulk APIs can resolve them.
    let s = db.session();
    let t = s.tx().await?;
    t.execute("CREATE LABEL Node (idx INT)").await?;
    match pattern {
        Pattern::Hot => {
            t.execute("CREATE EDGE TYPE EDGE_1 FROM Node TO Node")
                .await?;
        }
        Pattern::Cold => {
            for i in 0..n_sessions {
                let stmt = format!("CREATE EDGE TYPE EDGE_{} FROM Node TO Node", i);
                t.execute(&stmt).await?;
            }
        }
    }
    t.commit().await?;
    Ok(db)
}

async fn run_one_task(
    db: Arc<Uni>,
    task_id: usize,
    pattern: Pattern,
    edges_per_tx: usize,
) -> anyhow::Result<(Duration, Duration)> {
    let edge_type = match pattern {
        Pattern::Hot => "EDGE_1".to_string(),
        Pattern::Cold => format!("EDGE_{}", task_id),
    };

    // Pre-create source + dest vertices for this task (outside the timed window).
    let s = db.session();
    let setup_tx = s.tx().await?;
    let props: Vec<HashMap<String, Value>> = vec![
        HashMap::from([("idx".to_string(), Value::Int(task_id as i64))]),
        HashMap::from([("idx".to_string(), Value::Int((task_id + 10_000) as i64))]),
    ];
    let vids = setup_tx.bulk_insert_vertices("Node", props).await?;
    setup_tx.commit().await?;
    let src = vids[0];
    let dst = vids[1];

    // Build the edge list.
    let mut edges = Vec::with_capacity(edges_per_tx);
    for _ in 0..edges_per_tx {
        edges.push((src, dst, HashMap::new()));
    }

    // Timed: tx open → bulk_insert_edges → commit.
    let tx = s.tx().await?;
    let exec_start = Instant::now();
    tx.bulk_insert_edges(&edge_type, edges).await?;
    let exec = exec_start.elapsed();
    let commit_start = Instant::now();
    tx.commit().await?;
    let commit = commit_start.elapsed();

    Ok((exec, commit))
}

async fn one_rep(
    n_sessions: usize,
    pattern: Pattern,
    edges_per_tx: usize,
) -> anyhow::Result<(Duration, Vec<Duration>, Vec<Duration>)> {
    let db = setup_db(pattern, n_sessions).await?;
    let wall_start = Instant::now();
    let mut handles = Vec::with_capacity(n_sessions);
    for s in 0..n_sessions {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            run_one_task(db, s, pattern, edges_per_tx).await
        }));
    }
    let mut execs = Vec::with_capacity(n_sessions);
    let mut commits = Vec::with_capacity(n_sessions);
    for h in handles {
        let (e, c) = h.await??;
        execs.push(e);
        commits.push(c);
    }
    Ok((wall_start.elapsed(), execs, commits))
}

fn mean(ds: &[Duration]) -> Duration {
    let nanos: u128 = ds.iter().map(|d| d.as_nanos()).sum();
    Duration::from_nanos((nanos / ds.len() as u128) as u64)
}

async fn run_pattern(label: &str, pattern: Pattern) -> anyhow::Result<()> {
    println!("\n--- {} ---", label);
    println!(
        " {:>4}  {:>12}  {:>14}  {:>14}",
        "N", "wall", "exec_mean", "commit_mean"
    );
    println!(
        " {:>4}  {:>12}  {:>14}  {:>14}",
        "-", "------------", "--------------", "--------------"
    );
    for &n in SESSIONS_SWEEP {
        let mut walls = Vec::new();
        let mut all_execs = Vec::new();
        let mut all_commits = Vec::new();
        for _ in 0..REPS {
            let (wall, execs, commits) = one_rep(n, pattern, EDGES_PER_TX).await?;
            walls.push(wall);
            all_execs.extend(execs);
            all_commits.extend(commits);
        }
        println!(
            " {:>4}  {:>12?}  {:>14?}  {:>14?}",
            n,
            mean(&walls),
            mean(&all_execs),
            mean(&all_commits),
        );
    }
    Ok(())
}

async fn run_edges_sweep() -> anyhow::Result<()> {
    println!("\n--- edges/tx sweep (Pattern::Cold, N=24) — isolates commit-time scaling ---");
    println!("If commit time scales linearly with edges/tx → WAL + adjacency dominate.");
    println!("If commit time is roughly flat → lock acquisition / fixed costs dominate.");
    println!(
        " {:>10}  {:>12}  {:>14}  {:>14}  {:>16}",
        "edges/tx", "wall", "exec_mean", "commit_mean", "commit_per_edge"
    );
    println!(
        " {:>10}  {:>12}  {:>14}  {:>14}  {:>16}",
        "----------", "------------", "--------------", "--------------", "----------------"
    );
    for &edges_per_tx in EDGE_COUNT_SWEEP {
        let mut walls = Vec::new();
        let mut all_execs = Vec::new();
        let mut all_commits = Vec::new();
        for _ in 0..REPS {
            let (wall, execs, commits) = one_rep(24, Pattern::Cold, edges_per_tx).await?;
            walls.push(wall);
            all_execs.extend(execs);
            all_commits.extend(commits);
        }
        let cmean = mean(&all_commits);
        let per_edge_ns = if edges_per_tx > 0 {
            cmean.as_nanos() / edges_per_tx as u128
        } else {
            0
        };
        println!(
            " {:>10}  {:>12?}  {:>14?}  {:>14?}  {:>13} ns",
            edges_per_tx,
            mean(&walls),
            mean(&all_execs),
            cmean,
            per_edge_ns,
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!(
        "Edge-commit contention microbench — {} edges/tx, {} reps",
        EDGES_PER_TX, REPS
    );
    println!("Path: bulk_insert_edges → commit. Adjacency replay happens at commit.");
    println!("HOT = all tasks use same edge type. COLD = distinct edge types.");
    println!("Gap reveals commit-time adjacency-shard contention.");

    run_pattern("HOT (all tasks → EDGE_1)", Pattern::Hot).await?;
    run_pattern("COLD (each task → EDGE_i)", Pattern::Cold).await?;
    run_edges_sweep().await?;
    Ok(())
}
