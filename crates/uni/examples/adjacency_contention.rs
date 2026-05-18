// Isolated microbenchmark for AdjacencyManager::insert_edge contention.
//
// Bypasses the entire Cypher / Writer / commit pipeline. Constructs an
// AdjacencyManager directly and hammers `insert_edge` from N tokio tasks.
// Two scenarios:
//
//   HOT-SHARD: all tasks write the SAME (edge_type, direction) — the
//   pattern uniko sees with MENTIONS / HAS_CHUNK edges. If the
//   DashMap<(edge_type, dir), HashMap<...>> shard lock is the bottleneck,
//   this should NOT scale.
//
//   COLD-SHARD: each task writes a DISTINCT edge_type. DashMap hash should
//   spread the load across shards. This should scale linearly with N.
//
// The gap between HOT and COLD wall-times at high N is the contention
// caused by the nested-HashMap-under-DashMap-shard-lock pattern.
//
// Run with: cargo run --release --example adjacency_contention

use std::sync::Arc;
use std::time::{Duration, Instant};

use mimalloc::MiMalloc;
use uni_common::core::id::{Eid, Vid};
use uni_store::storage::adjacency_manager::AdjacencyManager;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const EDGES_PER_TASK: usize = 10_000;
const REPS: usize = 5;
const SESSIONS_SWEEP: &[usize] = &[1, 4, 12, 24];

#[derive(Clone, Copy)]
enum Pattern {
    HotShard,  // every task uses edge_type 1
    ColdShard, // task i uses edge_type i+1
}

async fn run_one_task(
    am: Arc<AdjacencyManager>,
    task_id: usize,
    pattern: Pattern,
) -> Duration {
    let edge_type = match pattern {
        Pattern::HotShard => 1u32,
        Pattern::ColdShard => (task_id as u32) + 1,
    };
    // Each task writes to a disjoint src-vid range to remove false sharing
    // on the inner Vec<(dst, eid, version)> — that's a separate concern from
    // the shard-lock hypothesis we're testing.
    let src_base = (task_id as u64) * (EDGES_PER_TASK as u64) * 2;
    let dst_base = src_base + 1_000_000;

    let start = Instant::now();
    for i in 0..EDGES_PER_TASK {
        let src = Vid::new(src_base + i as u64);
        let dst = Vid::new(dst_base + i as u64);
        let eid = Eid::new(src_base + i as u64);
        am.insert_edge(src, dst, eid, edge_type, 0);
    }
    start.elapsed()
}

async fn one_rep(n_sessions: usize, pattern: Pattern) -> (Duration, Vec<Duration>) {
    // Fresh AdjacencyManager per rep so we don't accumulate state.
    let am = Arc::new(AdjacencyManager::new(usize::MAX));
    let wall_start = Instant::now();
    let mut handles = Vec::with_capacity(n_sessions);
    for s in 0..n_sessions {
        let am = am.clone();
        handles.push(tokio::spawn(async move {
            run_one_task(am, s, pattern).await
        }));
    }
    let mut per_task = Vec::with_capacity(n_sessions);
    for h in handles {
        per_task.push(h.await.unwrap());
    }
    (wall_start.elapsed(), per_task)
}

fn mean(ds: &[Duration]) -> Duration {
    let nanos: u128 = ds.iter().map(|d| d.as_nanos()).sum();
    Duration::from_nanos((nanos / ds.len() as u128) as u64)
}

fn max(ds: &[Duration]) -> Duration {
    ds.iter().copied().max().unwrap_or_default()
}

async fn run_pattern(label: &str, pattern: Pattern) {
    println!("\n--- {} ---", label);
    println!(
        " {:>4}  {:>12}  {:>14}  {:>14}  {:>14}",
        "N", "wall", "per_task_mean", "per_task_max", "edges/sec"
    );
    println!(
        " {:>4}  {:>12}  {:>14}  {:>14}  {:>14}",
        "-", "------------", "--------------", "--------------", "--------------"
    );
    for &n in SESSIONS_SWEEP {
        let mut walls = Vec::new();
        let mut per_tasks = Vec::new();
        for _ in 0..REPS {
            let (wall, tasks) = one_rep(n, pattern).await;
            walls.push(wall);
            per_tasks.extend(tasks);
        }
        let wall_mean = mean(&walls);
        let total_edges = n * EDGES_PER_TASK;
        let edges_per_sec = (total_edges as f64) / wall_mean.as_secs_f64();
        println!(
            " {:>4}  {:>12?}  {:>14?}  {:>14?}  {:>11.0}/s",
            n,
            wall_mean,
            mean(&per_tasks),
            max(&per_tasks),
            edges_per_sec,
        );
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!(
        "AdjacencyManager::insert_edge microbenchmark — {} edges/task, {} reps",
        EDGES_PER_TASK, REPS
    );
    println!("Tests whether DashMap<(edge_type, dir), HashMap<...>> shard lock");
    println!("is the bottleneck for concurrent edge writes.");
    println!();
    println!("Expected if shard-lock IS the bottleneck:");
    println!("  HOT-SHARD: wall time grows roughly linearly with N (no parallelism)");
    println!("  COLD-SHARD: wall time roughly constant with N (full parallelism)");

    run_pattern("HOT-SHARD (all tasks → edge_type 1)", Pattern::HotShard).await;
    run_pattern("COLD-SHARD (each task → distinct edge_type)", Pattern::ColdShard).await;

    Ok(())
}
