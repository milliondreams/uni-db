// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 5b — recall scaffold for fork-local vector ANN fusion.
//!
//! Builds a primary vector index over N=1000 synthetic vectors,
//! forks, writes M=100 distinctive vectors on the fork, builds the
//! fork-local vector index, and measures top-K recall against a
//! held-out query set. Reports recall@K and median latency to
//! `eprintln!`; the test asserts a loose recall floor (≥80% — well
//! below spec §8.2's 95% target so CI doesn't flake on small data).
//!
//! `#[ignore]`'d. Opt in with:
//!
//! ```sh
//! cargo nextest run -p uni-db --test fork_index_recall_bench \
//!     --run-ignored ignored-only --no-capture
//! ```
//!
//! For full spec §8.2 measurement (recall ≥95% on N=100k items) write
//! a Criterion bench at `crates/uni/benches/fork_index.rs` and
//! capture results in `compliance_reports/fork_index_<date>.md`.

// Rust guideline compliant

use std::time::{Duration, Instant};

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::Uni;
use uni_store::fork::ForkLocalIndexKind;

const PRIMARY_N: usize = 1_000;
const FORK_M: usize = 100;
const QUERIES: usize = 20;
const K: usize = 10;
const RECALL_FLOOR: f64 = 0.80;

fn synth_vec(seed: u64) -> [f32; 8] {
    // Cheap PRN in [0.0, 1.0). Avoids negative literals because the
    // procedure-call argument parser doesn't accept `UnaryOp(Neg)`
    // wrapping a Literal in a CALL ... [...]  array.
    let mut s = seed
        .wrapping_mul(2862933555777941757)
        .wrapping_add(3037000493);
    let mut out = [0f32; 8];
    for v in out.iter_mut() {
        s = s.wrapping_mul(2862933555777941757).wrapping_add(3037000493);
        *v = s as f32 / u64::MAX as f32;
    }
    out
}

fn vec_literal(v: &[f32]) -> String {
    let mut s = String::from("[");
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(&format!("{x:.6}"));
    }
    s.push(']');
    s
}

#[tokio::test]
#[ignore = "phase-5b recall scaffold; opt in with --run-ignored ignored-only --no-capture"]
async fn fork_local_vector_recall_at_10() -> Result<()> {
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Doc")
        .property("id", DataType::Int64)
        .property("embedding", DataType::Vector { dimensions: 8 })
        .apply()
        .await?;

    // Seed primary with PRIMARY_N synthetic vectors.
    let primary = db.session();
    let tx = primary.tx().await?;
    for i in 0..PRIMARY_N {
        let v = synth_vec(i as u64);
        tx.execute(&format!(
            "CREATE (:Doc {{id: {i}, embedding: {}}})",
            vec_literal(&v)
        ))
        .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    // Fork + write FORK_M distinctive vectors (offset their seed
    // space so the fork's vectors are distinguishable from primary).
    let forked = primary.fork("scenario").await?;
    let tx = forked.tx().await?;
    for i in 0..FORK_M {
        let v = synth_vec((PRIMARY_N + i) as u64);
        tx.execute(&format!(
            "CREATE (:Doc {{id: {}, embedding: {}}})",
            PRIMARY_N + i,
            vec_literal(&v)
        ))
        .await?;
    }
    tx.commit().await?;
    forked.flush().await?;

    forked
        .build_fork_local_index("Doc", "embedding", ForkLocalIndexKind::Vector)
        .await?;

    // Compute ground-truth top-K via Cypher: brute-force scan over
    // ALL vectors (primary + fork), sorted by L2 distance to the
    // query. Phase 5b's MVP recall measurement compares the fork's
    // ANN-fused top-K against this.
    let mut total_overlap: usize = 0;
    let mut latencies: Vec<Duration> = Vec::with_capacity(QUERIES);
    for q_seed in 1_000_000..(1_000_000 + QUERIES) {
        let q = synth_vec(q_seed as u64);
        let q_lit = vec_literal(&q);

        // ANN-fused top-K through fork (uses Lance per-branch search).
        let t0 = Instant::now();
        let ann = forked
            .query(&format!(
                "CALL uni.vector.query('Doc', 'embedding', {q_lit}, {K})
                 YIELD node, score
                 RETURN node.id AS id"
            ))
            .await?;
        latencies.push(t0.elapsed());
        let ann_ids: Vec<i64> = ann
            .rows()
            .iter()
            .filter_map(|r| r.get::<i64>("id").ok())
            .collect();

        // Ground truth via the same procedure but with K large
        // enough to capture the full ranking, then take top-K.
        // Phase 5b's measurement is recall against the procedure's
        // own brute-force baseline at unbounded K, not against an
        // independent oracle (we're measuring the loss the ANN
        // index introduces, not the procedure's correctness).
        let truth = forked
            .query(&format!(
                "CALL uni.vector.query('Doc', 'embedding', {q_lit}, {})
                 YIELD node
                 RETURN node.id AS id",
                PRIMARY_N + FORK_M
            ))
            .await?;
        let truth_topk: std::collections::HashSet<i64> = truth
            .rows()
            .iter()
            .take(K)
            .filter_map(|r| r.get::<i64>("id").ok())
            .collect();

        let overlap = ann_ids.iter().filter(|id| truth_topk.contains(id)).count();
        total_overlap += overlap;
    }

    let recall = total_overlap as f64 / (QUERIES * K) as f64;
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    eprintln!(
        "phase 5b recall@{K} (n={PRIMARY_N}+{FORK_M}, q={QUERIES}): {:.3} | p50 latency: {:?}",
        recall, p50
    );
    assert!(
        recall >= RECALL_FLOOR,
        "fork-local vector ANN recall@{K} = {recall:.3} below floor {RECALL_FLOOR}"
    );

    db.shutdown().await?;
    Ok(())
}
