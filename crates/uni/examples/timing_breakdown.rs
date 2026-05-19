// Timing breakdown for the concurrent_mutations workload.
//
// The Criterion benchmark reports wall time; this example splits each task's
// wall time into the `execute` phase (100 CREATE statements) versus the
// `commit` phase (1 commit) and aggregates across sessions, so we can see
// where the remaining serialization at high concurrency actually goes.
//
// Run with: cargo run --release --example timing_breakdown

use std::sync::Arc;
use std::time::{Duration, Instant};

use mimalloc::MiMalloc;
use uni_db::Uni;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const STATEMENTS_PER_TASK: usize = 100;
const REPS: usize = 5;
const SESSIONS_SWEEP: &[usize] = &[1, 4, 12, 24];

#[derive(Default, Clone, Copy)]
struct TaskTiming {
    execute: Duration,
    commit: Duration,
    /// Wall-time of the whole task. Populated for inspection / future
    /// aggregation; currently unused at print time.
    #[allow(dead_code)]
    total: Duration,
}

async fn run_one_task(
    db: Arc<Uni>,
    task_id: usize,
    per_stmt: bool,
) -> anyhow::Result<(TaskTiming, Vec<Duration>)> {
    let task_start = Instant::now();
    let session = db.session();
    let tx = session.tx().await?;

    let mut stmt_times = Vec::with_capacity(if per_stmt { STATEMENTS_PER_TASK } else { 0 });
    let exec_start = Instant::now();
    for i in 0..STATEMENTS_PER_TASK {
        let s = Instant::now();
        tx.execute(&format!("CREATE (n:Person {{idx: {i}, sess: {task_id}}})"))
            .await?;
        if per_stmt {
            stmt_times.push(s.elapsed());
        }
    }
    let execute = exec_start.elapsed();

    let commit_start = Instant::now();
    tx.commit().await?;
    let commit = commit_start.elapsed();

    Ok((
        TaskTiming {
            execute,
            commit,
            total: task_start.elapsed(),
        },
        stmt_times,
    ))
}

async fn one_rep(
    n_sessions: usize,
    per_stmt: bool,
) -> anyhow::Result<(Duration, Vec<TaskTiming>, Vec<Duration>)> {
    let db = Arc::new(Uni::in_memory().build().await?);
    let wall_start = Instant::now();
    let mut handles = Vec::with_capacity(n_sessions);
    for s in 0..n_sessions {
        let db = db.clone();
        handles.push(tokio::spawn(
            async move { run_one_task(db, s, per_stmt).await },
        ));
    }
    let mut per_task = Vec::with_capacity(n_sessions);
    let mut all_stmts = Vec::new();
    for h in handles {
        let (t, mut s) = h.await??;
        per_task.push(t);
        all_stmts.append(&mut s);
    }
    Ok((wall_start.elapsed(), per_task, all_stmts))
}

fn mean(ds: &[Duration]) -> Duration {
    if ds.is_empty() {
        return Duration::ZERO;
    }
    let nanos: u128 = ds.iter().map(|d| d.as_nanos()).sum();
    Duration::from_nanos((nanos / ds.len() as u128) as u64)
}

fn max(ds: &[Duration]) -> Duration {
    ds.iter().copied().max().unwrap_or(Duration::ZERO)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Timing breakdown — concurrent_mutations workload");
    println!(
        "Per task: {} CREATE statements + 1 commit. {} reps per N.",
        STATEMENTS_PER_TASK, REPS
    );
    println!(
        "\n {:>4}  {:>10}  {:>14}  {:>14}  {:>14}  {:>14}",
        "N", "wall", "exec_mean", "exec_max", "commit_mean", "commit_max"
    );
    println!(
        " {:>4}  {:>10}  {:>14}  {:>14}  {:>14}  {:>14}",
        "-", "----------", "--------------", "--------------", "--------------", "--------------"
    );

    let mut per_stmt_by_n: Vec<(usize, Vec<Duration>)> = Vec::new();
    for &n in SESSIONS_SWEEP {
        let mut walls = Vec::new();
        let mut all_execs = Vec::new();
        let mut all_commits = Vec::new();
        let mut all_stmts = Vec::new();

        for _ in 0..REPS {
            let (wall, tasks, stmts) = one_rep(n, true).await?;
            walls.push(wall);
            for t in tasks {
                all_execs.push(t.execute);
                all_commits.push(t.commit);
            }
            all_stmts.extend(stmts);
        }

        println!(
            " {:>4}  {:>10?}  {:>14?}  {:>14?}  {:>14?}  {:>14?}",
            n,
            mean(&walls),
            mean(&all_execs),
            max(&all_execs),
            mean(&all_commits),
            max(&all_commits),
        );
        per_stmt_by_n.push((n, all_stmts));
    }

    fn pct(v: &mut [Duration], q: f64) -> Duration {
        v.sort();
        let i = ((v.len() as f64) * q) as usize;
        v[i.min(v.len() - 1)]
    }
    println!("\nPer-statement distribution (across all tasks * reps):");
    println!(
        " {:>4}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
        "N", "min", "p50", "p90", "p99", "max"
    );
    for (n, mut stmts) in per_stmt_by_n {
        let min = stmts.iter().min().copied().unwrap_or_default();
        let max = stmts.iter().max().copied().unwrap_or_default();
        let p50 = pct(&mut stmts, 0.5);
        let p90 = pct(&mut stmts, 0.9);
        let p99 = pct(&mut stmts, 0.99);
        println!(
            " {:>4}  {:>10?}  {:>10?}  {:>10?}  {:>10?}  {:>10?}",
            n, min, p50, p90, p99, max,
        );
    }

    println!("\nReading guide:");
    println!("  exec_mean   = avg time one task spent in the 100 tx.execute() calls");
    println!("  commit_mean = avg time one task spent in tx.commit()");
    println!("  exec_max / commit_max = slowest task in the rep (captures tail)");
    println!("  At sess=N: if commit_mean grows linearly with N, the bottleneck is");
    println!("  flush_lock serializing commits. If exec_mean grows, something serializes");
    println!("  the per-statement path.");

    Ok(())
}
