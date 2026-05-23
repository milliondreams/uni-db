// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Stress / regression test for batched `UNWIND ... MATCH WHERE id(n)=u.nid SET ...`.
//
// Run with:
//   cargo nextest run -p uni-db --test perf update_microbench_stress \
//     --run-ignored all --no-capture --cargo-profile release
//
// Reports wall + exec_time across batch sizes 1/3/10/100/1000 plus a
// `.profile()` per-operator breakdown at batch=3 and batch=1000.
//
// Regression baseline (release build, post-Phase-A + post-systemic-pushdown):
//   - batch=1000 wall: < 100 ms (was ~450 ms pre-fix)
//   - GraphScanExec.actual_rows at batch=1000: <= batch size (was 4000)
//   - MutationSetExec.time_ms / 1000 rows: ~1.3 ms/row
//
// If any of those numbers regress dramatically, the Phase A prefetch or
// the IN-list pushdown pass has likely been broken.

use std::collections::HashMap;
use std::sync::Once;
use std::time::Instant;

use uni_db::common::TemporalValue;
use uni_db::{Uni, Value};

static TRACING_INIT: Once = Once::new();

fn init_tracing() {
    TRACING_INIT.call_once(|| {
        use tracing_subscriber::EnvFilter;
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("warn"));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_thread_ids(false)
            .with_writer(std::io::stderr)
            .try_init();
    });
}

fn now_value() -> Value {
    Value::Temporal(TemporalValue::DateTime {
        nanos_since_epoch: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        offset_seconds: 0,
        timezone_name: None,
    })
}

const UPDATE_CYPHER: &str = "\
    UNWIND $updates AS u \
    MATCH (n:Entity) WHERE id(n) = u.nid \
    SET n.frequency = u.new_frequency, \
        n.last_seen = $now, \
        n.confidence = u.new_confidence";

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn update_microbench_stress() -> anyhow::Result<()> {
    init_tracing();

    let tmp = tempfile::tempdir()?;
    let db = Uni::open(tmp.path().to_string_lossy().to_string())
        .build()
        .await?;
    let session = db.session();

    // ── Schema
    {
        let tx = session.tx().await?;
        tx.execute(
            "CREATE LABEL Entity (\
               entity_id STRING NOT NULL, \
               name STRING NOT NULL, \
               frequency INT, \
               last_seen DATETIME, \
               confidence FLOAT)",
        )
        .await?;
        // Production-like schema noise: 25 sibling labels populated with
        // some data, so the planner has to deal with a non-trivial catalog.
        for i in 0..25 {
            tx.execute(&format!("CREATE LABEL Sibling{i} (x INT)"))
                .await?;
        }
        tx.commit().await?;
        let tx = session.tx().await?;
        for i in 0..25 {
            let label = format!("Sibling{i}");
            let mut rows = Vec::with_capacity(100);
            for j in 0..100 {
                let mut h = HashMap::new();
                h.insert("x".into(), Value::Int(j));
                rows.push(h);
            }
            tx.bulk_insert_vertices(&label, rows).await?;
        }
        tx.commit().await?;
    }

    // ── Insert 4000 entities (this is fast; not what we're measuring)
    const N: usize = 4000;
    let all_vids: Vec<i64> = {
        let tx = session.tx().await?;
        let mut rows: Vec<HashMap<String, Value>> = Vec::with_capacity(N);
        for i in 0..N {
            let mut h = HashMap::new();
            h.insert("entity_id".into(), Value::String(format!("e:{i}")));
            h.insert("name".into(), Value::String(format!("entity_{i}")));
            h.insert("frequency".into(), Value::Int(1));
            h.insert("last_seen".into(), now_value());
            h.insert("confidence".into(), Value::Float(0.5));
            rows.push(h);
        }
        let vids = tx.bulk_insert_vertices("Entity", rows).await?;
        tx.commit().await?;
        vids.iter().map(|v| v.as_u64() as i64).collect()
    };
    eprintln!("Setup: {N} Entity nodes inserted.\n");

    // ── Measure UPDATE wall + exec_time across batch sizes
    eprintln!("## Wall + exec_time vs batch size (median of 5 iters)");
    eprintln!(
        "{:>6} {:>10} {:>10} {:>10}",
        "batch", "wall_ms", "exec_ms", "ms/row"
    );
    for &batch in &[1usize, 3, 10, 100, 1000] {
        let updates: Vec<Value> = all_vids[..batch]
            .iter()
            .enumerate()
            .map(|(i, &vid)| {
                let mut m = HashMap::new();
                m.insert("nid".into(), Value::Int(vid));
                m.insert("new_frequency".into(), Value::Int((i as i64) + 2));
                m.insert("new_confidence".into(), Value::Float(0.7));
                Value::Map(m)
            })
            .collect();

        let mut walls = Vec::new();
        let mut execs = Vec::new();
        for _ in 0..5 {
            let tx = session.tx().await?;
            let t = Instant::now();
            let result = tx
                .execute_with(UPDATE_CYPHER)
                .param("updates", Value::List(updates.clone()))
                .param("now", now_value())
                .run()
                .await?;
            walls.push(t.elapsed().as_secs_f64() * 1000.0);
            execs.push(result.metrics().exec_time.as_secs_f64() * 1000.0);
            tx.commit().await?;
        }
        let med = |mut v: Vec<f64>| {
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            v[v.len() / 2]
        };
        let w = med(walls);
        let e = med(execs);
        eprintln!(
            "{:>6} {:>10.2} {:>10.2} {:>10.3}",
            batch,
            w,
            e,
            w / batch as f64
        );
    }

    // ── Per-operator breakdown via .profile()
    eprintln!("\n## .profile() per-op breakdown");
    for &batch in &[3usize, 1000] {
        let updates: Vec<Value> = all_vids[..batch]
            .iter()
            .enumerate()
            .map(|(i, &vid)| {
                let mut m = HashMap::new();
                m.insert("nid".into(), Value::Int(vid));
                m.insert("new_frequency".into(), Value::Int((i as i64) + 2));
                m.insert("new_confidence".into(), Value::Float(0.7));
                Value::Map(m)
            })
            .collect();
        let tx = session.tx().await?;
        let (_res, profile) = tx
            .execute_with(UPDATE_CYPHER)
            .param("updates", Value::List(updates))
            .param("now", now_value())
            .profile()
            .await?;
        eprintln!(
            "--- batch={batch} profile total={} ms peak_mem={} B ---",
            profile.total_time_ms, profile.peak_memory_bytes
        );
        let mut accounted = 0.0_f64;
        for (i, op) in profile.runtime_stats.iter().enumerate() {
            accounted += op.time_ms;
            eprintln!(
                "  [{i}] {:<24} rows={:>6}  time={:>9.3} ms",
                op.operator, op.actual_rows, op.time_ms
            );
        }
        let total = profile.total_time_ms as f64;
        eprintln!(
            "  → accounted op time = {:.2} ms of {:.0} ms profile total ({:.1}% unaccounted)",
            accounted,
            total,
            100.0 * (1.0 - accounted / total)
        );
        tx.commit().await?;
    }

    Ok(())
}
