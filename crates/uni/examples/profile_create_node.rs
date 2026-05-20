// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Rust guideline compliant
//
// Granular per-stage profile of single-row Cypher CREATE node insert.
//
// Mirrors the shape of `timing_breakdown.rs`: one transaction, N
// `tx.execute("CREATE (n:Person ...)")` calls, then a single commit. Per-stage
// timings are accumulated via `uni_store::profile` and dumped at the end.
//
// Run with: cargo run --release --example profile_create_node

use std::sync::Arc;
use std::time::Instant;

use mimalloc::MiMalloc;
use uni_db::Uni;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const WARMUP: usize = 50;
const MEASURED: usize = 1_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Arc::new(Uni::in_memory().build().await?);
    let session = db.session();

    // Warmup: prime caches, JIT-ish effects in DataFusion, allocator, etc.
    {
        let tx = session.tx().await?;
        for i in 0..WARMUP {
            tx.execute(&format!("CREATE (n:Person {{idx: {i}}})"))
                .await?;
        }
        tx.commit().await?;
    }

    // Reset profile registry so warmup doesn't pollute numbers.
    uni_store::profile::reset();

    // Measurement: one transaction, MEASURED execute() calls, one commit.
    let tx = session.tx().await?;
    let mut per_stmt = Vec::with_capacity(MEASURED);
    let exec_total_start = Instant::now();
    for i in 0..MEASURED {
        let s = Instant::now();
        tx.execute(&format!("CREATE (n:Person {{idx: {i}}})"))
            .await?;
        per_stmt.push(s.elapsed());
    }
    let exec_total = exec_total_start.elapsed();

    let commit_start = Instant::now();
    tx.commit().await?;
    let commit_total = commit_start.elapsed();

    // Statistics on per-statement wall time.
    per_stmt.sort();
    let p50 = per_stmt[per_stmt.len() / 2];
    let p90 = per_stmt[(per_stmt.len() * 90) / 100];
    let p99 = per_stmt[(per_stmt.len() * 99) / 100];
    let mean_ns: u128 =
        per_stmt.iter().map(|d| d.as_nanos()).sum::<u128>() / per_stmt.len() as u128;
    let total_us = exec_total.as_micros();
    let amortized_commit_ns = commit_total.as_nanos() / per_stmt.len() as u128;

    println!("Single-row CREATE profile (InMemory backend)");
    println!(
        "  warmup           = {} statements (then 1 commit, profile reset)",
        WARMUP
    );
    println!("  measured         = {} statements + 1 commit", MEASURED);
    println!(
        "\n  exec total       = {:>8} us  ({} statements)",
        total_us, MEASURED
    );
    println!("  exec per-stmt    : mean={mean_ns:>6} ns  p50={p50:?}  p90={p90:?}  p99={p99:?}");
    println!(
        "  commit total     = {:>8} us  (amortized {} ns/stmt)",
        commit_total.as_micros(),
        amortized_commit_ns
    );
    println!("\nPer-stage breakdown (divisor = {} statements):", MEASURED);
    println!("{}", uni_store::profile::dump(MEASURED as u64));

    Ok(())
}
