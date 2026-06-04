// Quick single-shot async vs sync at one threshold, with timeouts so we
// catch pathological behavior.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimalloc::MiMalloc;
use uni_common::config::UniConfig;
use uni_db::{Uni, Value};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const SESSIONS: usize = 8;
const TXS_PER_SESSION: usize = 50;
const VERTICES_PER_TX: usize = 50;
const THRESHOLD: usize = 5000;

async fn build_db(async_flush: bool) -> anyhow::Result<Arc<Uni>> {
    let config = UniConfig {
        auto_flush_threshold: THRESHOLD,
        auto_flush_interval: None,
        async_flush_enabled: async_flush,
        ..Default::default()
    };
    let db = Arc::new(Uni::in_memory().config(config).build().await?);
    let s = db.session();
    let t = s.tx().await?;
    t.execute("CREATE LABEL Person (idx INT, sess INT)").await?;
    t.commit().await?;
    Ok(db)
}

async fn run_one_task(db: Arc<Uni>, task_id: usize) -> anyhow::Result<(Duration, Duration)> {
    let session = db.session();
    let mut total_exec = Duration::ZERO;
    let mut total_commit = Duration::ZERO;
    for _txn in 0..TXS_PER_SESSION {
        let tx = session.tx().await?;
        let props: Vec<HashMap<String, Value>> = (0..VERTICES_PER_TX)
            .map(|i| {
                HashMap::from([
                    ("idx".to_string(), Value::Int(i as i64)),
                    ("sess".to_string(), Value::Int(task_id as i64)),
                ])
            })
            .collect();
        let e = Instant::now();
        tx.bulk_insert_vertices("Person", props).await?;
        total_exec += e.elapsed();
        let c = Instant::now();
        tx.commit().await?;
        total_commit += c.elapsed();
    }
    Ok((total_exec, total_commit))
}

async fn one_rep(async_flush: bool) -> anyhow::Result<(Duration, Duration, Duration)> {
    let db = build_db(async_flush).await?;
    let wall = Instant::now();
    let mut handles = vec![];
    for s in 0..SESSIONS {
        let db = db.clone();
        handles.push(tokio::spawn(async move { run_one_task(db, s).await }));
    }
    let mut total_exec = Duration::ZERO;
    let mut total_commit = Duration::ZERO;
    for h in handles {
        let (e, c) = h.await??;
        total_exec += e;
        total_commit += c;
    }
    // Always drain at end so both variants have apples-to-apples wall.
    db.flush().await?;
    let w = wall.elapsed();
    Ok((
        w,
        total_exec / SESSIONS as u32,
        total_commit / SESSIONS as u32,
    ))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!(
        "Quick test: {} sessions × {} tx × {} vertices, threshold={}",
        SESSIONS, TXS_PER_SESSION, VERTICES_PER_TX, THRESHOLD
    );
    println!(
        "  Total mutations per rep: {} (~{} flushes expected)",
        SESSIONS * TXS_PER_SESSION * VERTICES_PER_TX,
        SESSIONS * TXS_PER_SESSION * VERTICES_PER_TX / THRESHOLD
    );

    for &(label, async_flush) in &[("SYNC ", false), ("ASYNC", true)] {
        println!("\n[{}]", label);
        for rep in 0..3 {
            let res = tokio::time::timeout(Duration::from_secs(60), one_rep(async_flush)).await;
            match res {
                Ok(Ok((w, e, c))) => {
                    println!(
                        "  rep {}: wall={:?}  exec_avg={:?}  commit_avg={:?}",
                        rep, w, e, c
                    );
                }
                Ok(Err(e)) => println!("  rep {}: ERROR: {}", rep, e),
                Err(_) => {
                    println!(
                        "  rep {}: TIMEOUT after 60s — async-flush MVP is pathological",
                        rep
                    );
                    break;
                }
            }
        }
    }
    Ok(())
}
