//! Ad-hoc query probe against a persisted LDBC graph. Reads one Cypher query per
//! line from stdin so query shapes can be bisected without a rebuild.
//! Kept as a general instrument: it takes arbitrary Cypher on stdin against a
//! persisted LDBC graph, so a query shape can be bisected without a rebuild.
//! (The one-off probes for defects now covered by tests were deleted; see
//! `docs/testing/single-shape-coverage-2026-08-27.md` for the discipline.)
use std::io::BufRead;
use uni_db::Uni;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let secs: u64 = std::env::var("PROBE_TIMEOUT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    // `PROBE_BATCH_SIZE` holds the edge set fixed while varying the morsel
    // size: a per-batch cost scales with it, a per-row cost does not.
    let batch_size: usize = std::env::var("PROBE_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024);
    eprintln!("[probe] batch_size={batch_size} query_timeout={secs}s");
    // `PROBE_MAX_MB` tightens the query memory pool, so a plan that cannot
    // spill can be told apart from one that can (#213).
    let max_mb: usize = std::env::var("PROBE_MAX_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut config = uni_db::UniConfig {
        query_timeout: std::time::Duration::from_secs(secs),
        batch_size,
        ..Default::default()
    };
    if max_mb > 0 {
        config.max_query_memory = max_mb * 1024 * 1024;
        eprintln!("[probe] max_query_memory={max_mb} MB");
    }
    let db = Uni::open_existing(std::env::var("LDBC_DB")?)
        .config(config)
        .build()
        .await?;
    for line in std::io::stdin().lock().lines() {
        let q = line?;
        let q = q.trim();
        if q.is_empty() || q.starts_with('#') {
            continue;
        }
        // Elapsed is printed per query: a bisection ladder without per-step
        // timing cannot attribute a cost to a step, only observe that it ran.
        if let Some(inner) = q.strip_prefix("PROFILE ") {
            let t = std::time::Instant::now();
            match db.session().query_with(inner).profile().await {
                Ok((r, p)) => {
                    let ms = t.elapsed().as_secs_f64() * 1000.0;
                    println!(
                        "PROFILE rows={} wall={ms:.1}ms total={}ms peak={}MB\n  {inner}",
                        r.rows().len(),
                        p.total_time_ms,
                        p.peak_memory_bytes / 1_048_576
                    );
                    let mut sorted = p.runtime_stats.clone();
                    sorted.sort_by(|a, b| b.time_ms.partial_cmp(&a.time_ms).unwrap());
                    let sum: f64 = p.runtime_stats.iter().map(|o| o.time_ms).sum();
                    println!("  accounted={sum:.1}ms of wall={ms:.1}ms");
                    for o in sorted.iter().take(12) {
                        println!(
                            "    {:<34} {:>9.1}ms rows={:<10} mem={}MB",
                            o.operator,
                            o.time_ms,
                            o.actual_rows,
                            o.memory_bytes / 1_048_576
                        );
                    }
                }
                Err(e) => println!("PROFILE ERROR {e}\n  {inner}"),
            }
            continue;
        }
        let t = std::time::Instant::now();
        let outcome = db.session().query(q).await;
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        match outcome {
            Ok(r) => {
                // `scans_reported` counts storage round trips, which is what
                // separates "this plan is chunked" from "this plan is one
                // scan" -- a timing alone cannot tell those apart.
                let m = r.metrics();
                println!(
                    "rows={:<8} ms={ms:>10.1} scans={:<6} idx={:<5} cmp={:<12} first={:?}\n  {q}",
                    r.rows().len(),
                    m.scans_reported,
                    m.index_scans,
                    m.index_comparisons,
                    r.rows().first().map(|x| x.values().to_vec())
                )
            }
            Err(e) => println!("ERROR ms={ms:>10.1} {e}\n  {q}"),
        }
    }
    Ok(())
}
