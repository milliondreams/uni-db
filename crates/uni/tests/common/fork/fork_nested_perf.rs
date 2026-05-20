// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 3 perf-sanity — depth-5 read latency.
//!
//! Per spec exit criteria for Phase 3: a query on a 5-deep fork chain
//! should land within `5.0 × depth_1_baseline + 5ms`. The `+5ms` floor
//! tolerates noise when the baseline is in the microsecond range; the
//! `5.0 ×` factor matches the spec.
//!
//! Marked `#[ignore]` because perf assertions are CI-flaky. Opt in
//! locally with:
//!
//! ```sh
//! cargo nextest run -p uni-db --test fork_nested_perf \
//!     --run-ignored ignored-only
//! ```

// Rust guideline compliant

use anyhow::Result;
use std::time::{Duration, Instant};
use uni_common::core::schema::DataType;
use uni_db::Uni;

const ITERATIONS: usize = 10;
const QUERY: &str = "MATCH (p:Person) RETURN count(p)";
const TOLERANCE_FACTOR: f64 = 5.0;
const TOLERANCE_FLOOR: Duration = Duration::from_millis(5);

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

#[tokio::test]
#[ignore = "perf-sensitive; opt in with --run-ignored ignored-only"]
async fn depth_5_read_latency_within_5x_baseline() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Seed primary with enough rows that the query does real work.
    let primary = db.session();
    {
        let tx = primary.tx().await?;
        for i in 0..100 {
            tx.execute(&format!("CREATE (:Person {{name: 'P-{i}'}})"))
                .await?;
        }
        tx.commit().await?;
    }
    db.flush().await?;

    // Build a 5-deep chain. Each level writes one row so its branch
    // tip is non-trivial, forcing Lance to actually chain through
    // that level's commits.
    let a = primary.fork("a").await?;
    write_one(&a, "A-1").await?;

    let b = a.fork("b").await?;
    write_one(&b, "B-1").await?;

    let c = b.fork("c").await?;
    write_one(&c, "C-1").await?;

    let d = c.fork("d").await?;
    write_one(&d, "D-1").await?;

    let e = d.fork("e").await?;
    write_one(&e, "E-1").await?;

    // Warm caches on primary before measuring.
    for _ in 0..3 {
        primary.query(QUERY).await?;
    }

    // Baseline: depth 1 (primary directly).
    let mut primary_samples = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let t = Instant::now();
        primary.query(QUERY).await?;
        primary_samples.push(t.elapsed());
    }
    let baseline = median(primary_samples);

    // Warm caches on leaf before measuring.
    for _ in 0..3 {
        e.query(QUERY).await?;
    }

    // Depth-5 measurement.
    let mut leaf_samples = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let t = Instant::now();
        e.query(QUERY).await?;
        leaf_samples.push(t.elapsed());
    }
    let depth5 = median(leaf_samples);

    let budget = baseline.mul_f64(TOLERANCE_FACTOR) + TOLERANCE_FLOOR;
    eprintln!(
        "baseline (depth 1) median = {:?}; depth-5 median = {:?}; budget = {:?}",
        baseline, depth5, budget
    );
    assert!(
        depth5 <= budget,
        "depth-5 leaf query exceeded budget: {depth5:?} > {budget:?} \
         (baseline {baseline:?}, factor {TOLERANCE_FACTOR}, floor {TOLERANCE_FLOOR:?})"
    );

    db.shutdown().await?;
    Ok(())
}

async fn write_one(session: &uni_db::Session, name: &str) -> Result<()> {
    let tx = session.tx().await?;
    tx.execute(&format!("CREATE (:Person {{name: '{name}'}})"))
        .await?;
    tx.commit().await?;
    Ok(())
}
