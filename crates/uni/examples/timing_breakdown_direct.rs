// Variant of timing_breakdown that bypasses Cypher parse/plan by using
// the direct mutation API (Transaction::bulk_insert_vertices).
//
// If this version scales linearly while timing_breakdown does not, the
// contention is in parse/plan/executor-setup. If both are equally bad,
// the contention is in the Writer / storage / schema path that both share.
//
// Run with: cargo run --release --example timing_breakdown_direct

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uni_db::{Uni, Value};

const VERTICES_PER_TASK: usize = 100;
const REPS: usize = 5;
const SESSIONS_SWEEP: &[usize] = &[1, 4, 12, 24];

async fn run_one_task(db: Arc<Uni>, task_id: usize) -> anyhow::Result<(Duration, Duration)> {
    let session = db.session();
    let tx = session.tx().await?;

    let exec_start = Instant::now();
    let mut rows = Vec::with_capacity(VERTICES_PER_TASK);
    for i in 0..VERTICES_PER_TASK {
        let mut props: HashMap<String, Value> = HashMap::new();
        props.insert("idx".to_string(), Value::Int(i as i64));
        props.insert("sess".to_string(), Value::Int(task_id as i64));
        rows.push(props);
    }
    tx.bulk_insert_vertices("Person", rows).await?;
    let execute = exec_start.elapsed();

    let commit_start = Instant::now();
    tx.commit().await?;
    let commit = commit_start.elapsed();

    Ok((execute, commit))
}

async fn one_rep(n_sessions: usize) -> anyhow::Result<(Duration, Vec<Duration>, Vec<Duration>)> {
    let db = Arc::new(Uni::in_memory().build().await?);
    // Pre-create the Person label via DDL so the direct API can resolve it.
    {
        let s = db.session();
        let t = s.tx().await?;
        t.execute("CREATE LABEL Person (idx INT, sess INT)").await?;
        t.commit().await?;
    }
    let wall_start = Instant::now();
    let mut handles = Vec::with_capacity(n_sessions);
    for s in 0..n_sessions {
        let db = db.clone();
        handles.push(tokio::spawn(async move { run_one_task(db, s).await }));
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
    if ds.is_empty() {
        return Duration::ZERO;
    }
    let nanos: u128 = ds.iter().map(|d| d.as_nanos()).sum();
    Duration::from_nanos((nanos / ds.len() as u128) as u64)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!(
        "Timing breakdown (DIRECT API) — bulk_insert_vertices, {} per task, {} reps",
        VERTICES_PER_TASK, REPS
    );
    println!(
        "\n {:>4}  {:>10}  {:>14}  {:>14}",
        "N", "wall", "exec_mean", "commit_mean"
    );
    println!(
        " {:>4}  {:>10}  {:>14}  {:>14}",
        "-", "----------", "--------------", "--------------"
    );

    for &n in SESSIONS_SWEEP {
        let mut walls = Vec::new();
        let mut all_execs = Vec::new();
        let mut all_commits = Vec::new();
        for _ in 0..REPS {
            let (wall, execs, commits) = one_rep(n).await?;
            walls.push(wall);
            all_execs.extend(execs);
            all_commits.extend(commits);
        }
        println!(
            " {:>4}  {:>10?}  {:>14?}  {:>14?}",
            n,
            mean(&walls),
            mean(&all_execs),
            mean(&all_commits),
        );
    }

    Ok(())
}
