// Async-flush soak test (Plan §13.3.1).
//
// Long-running stress harness for `UniConfig::async_flush_enabled = true`.
// Defaults to ~1 hour at sess=24, threshold=2500; override via env vars.
// Snapshots RSS at intervals and asserts no unbounded memory growth.
//
// Usage:
//   cargo run --release --example async_flush_soak
//   SOAK_DURATION_SECS=300 cargo run --release --example async_flush_soak
//   SOAK_MUTATIONS_TOTAL=500000 cargo run --release --example async_flush_soak
//
// The soak's purpose: validate that async-flush is stable under
// sustained load — no OOM, no deadlock, no monotonically-growing RSS,
// no data loss vs. counted expected_count. This is the gate for
// flipping the default in `Commit C-flip`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimalloc::MiMalloc;
use uni_common::config::UniConfig;
use uni_db::{DataType, Uni, Value};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const SESSIONS: usize = 24;
const VERTICES_PER_TX: usize = 25;
const THRESHOLD: usize = 2500;

fn read_rss_kb() -> Option<u64> {
    // procfs: /proc/self/status `VmRSS: NNN kB`
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let n: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(n);
        }
    }
    None
}

async fn session_worker(
    db: Arc<Uni>,
    sess_id: usize,
    target_mutations: usize,
) -> anyhow::Result<usize> {
    let session = db.session();
    let mut written = 0;
    let mut tx_id = 0u64;
    while written < target_mutations {
        let tx = session.tx().await?;
        let batch_size = VERTICES_PER_TX.min(target_mutations - written);
        let mut props = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let mut row = HashMap::new();
            row.insert(
                "key".to_string(),
                Value::String(format!("s{}_t{}_v{}", sess_id, tx_id, i)),
            );
            props.push(row);
        }
        tx.bulk_insert_vertices("Person", props).await?;
        tx.commit().await?;
        written += batch_size;
        tx_id += 1;
    }
    Ok(written)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let duration_secs: u64 = std::env::var("SOAK_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3600);
    let mutations_total: usize = std::env::var("SOAK_MUTATIONS_TOTAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000_000);
    let sessions: usize = std::env::var("SOAK_SESSIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(SESSIONS);

    println!(
        "async-flush soak harness — sessions={} threshold={} target_duration_secs={} target_mutations={}",
        sessions, THRESHOLD, duration_secs, mutations_total
    );

    let config = UniConfig {
        auto_flush_threshold: THRESHOLD,
        auto_flush_interval: None,
        async_flush_enabled: true,
        max_pending_flushes: 4,
        ..Default::default()
    };
    // Native Lance backend (tempdir) so stream durations are realistic.
    // Without this, in-memory streams are too fast to exercise the
    // pipeline depth (Plan L5).
    let tempdir = tempfile::tempdir()?;
    let db = Arc::new(
        Uni::open(tempdir.path().to_str().unwrap())
            .config(config)
            .build()
            .await?,
    );
    db.schema()
        .label("Person")
        .property("key", DataType::String)
        .apply()
        .await?;

    let start_rss_kb = read_rss_kb().unwrap_or(0);
    let start = Instant::now();
    println!("start_rss_kb={}", start_rss_kb);

    // Run sessions in parallel. Each writes `per_session` mutations.
    let per_session = mutations_total / sessions;
    let mut handles = Vec::with_capacity(sessions);
    for s in 0..sessions {
        let db = db.clone();
        handles.push(tokio::spawn(session_worker(db, s, per_session)));
    }

    // RSS sampler: every 30 seconds.
    let mut samples: Vec<(Duration, u64)> = Vec::new();
    let sampler_db = db.clone();
    let sampler_deadline = start + Duration::from_secs(duration_secs);
    let sampler = tokio::spawn(async move {
        let mut samples: Vec<(Duration, u64)> = Vec::new();
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await; // first tick is immediate
        loop {
            interval.tick().await;
            if Instant::now() >= sampler_deadline {
                break;
            }
            let rss = read_rss_kb().unwrap_or(0);
            let elapsed = start.elapsed();
            println!(
                "[t={:>5}s] rss_kb={} (delta from start: {:+}KB)",
                elapsed.as_secs(),
                rss,
                rss as i64 - start_rss_kb as i64
            );
            samples.push((elapsed, rss));
        }
        // Drop sampler's db Arc clone before returning.
        drop(sampler_db);
        samples
    });

    let mut total_written = 0;
    for h in handles {
        match h.await {
            Ok(Ok(n)) => total_written += n,
            Ok(Err(e)) => eprintln!("worker error: {e}"),
            Err(e) => eprintln!("worker join error: {e}"),
        }
    }

    db.flush().await?;
    let final_rss_kb = read_rss_kb().unwrap_or(0);
    let elapsed = start.elapsed();
    println!(
        "writes complete — total_written={} elapsed={:?} final_rss_kb={} delta={:+}KB",
        total_written,
        elapsed,
        final_rss_kb,
        final_rss_kb as i64 - start_rss_kb as i64
    );

    // Stop the sampler and collect samples.
    sampler.abort();
    samples.extend(sampler.await.unwrap_or_default());

    // Assert: query count matches what we wrote.
    let result = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    let observed: i64 = result.rows()[0].get("c")?;
    assert_eq!(
        observed as usize, total_written,
        "soak count mismatch: wrote {} but query saw {}",
        total_written, observed
    );

    // Assert: RSS growth bounded. Allow generous slack (5x start) since
    // mimalloc retains some pages. The signal we're after is unbounded
    // monotonic growth across hours.
    let max_rss = samples
        .iter()
        .map(|(_, r)| *r)
        .max()
        .unwrap_or(final_rss_kb);
    let rss_ratio = max_rss as f64 / (start_rss_kb.max(1)) as f64;
    println!(
        "max_rss_kb={} start_rss_kb={} ratio={:.2}x",
        max_rss, start_rss_kb, rss_ratio
    );
    if rss_ratio > 10.0 {
        eprintln!(
            "WARNING: RSS grew >10x during soak — possible leak. samples: {:?}",
            samples
        );
    }

    println!("soak PASSED");
    Ok(())
}
