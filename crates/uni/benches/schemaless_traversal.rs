// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Schemaless (undeclared edge type) traversal cost.
//!
//! A traversal over an edge type that is not in the declared schema runs
//! through `GraphTraverseMainExec`, which today materializes EVERY edge of the
//! type (with full property maps) regardless of how many source vertices feed
//! the operator. This bench contrasts a 1-source traversal with an all-sources
//! traversal over the same 100k-edge table: with source-VID pushdown the
//! 1-source arm should become orders of magnitude cheaper, while the
//! all-sources arm must not regress.
//!
//! ```bash
//! cargo bench --bench schemaless_traversal
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use uni_db::{DataType, Uni, Value};

const NODES: usize = 10_000;
const EDGES: usize = 100_000;
const CHUNK: usize = 10_000;

/// Build an edge-spec chunk: src = i % NODES, dst = (i * 7 + 1) % NODES.
fn edge_chunk(from: usize, to: usize) -> Value {
    Value::List(
        (from..to)
            .map(|i| {
                let mut m = HashMap::new();
                m.insert("src".to_string(), Value::Int((i % NODES) as i64));
                m.insert("dst".to_string(), Value::Int(((i * 7 + 1) % NODES) as i64));
                Value::Map(m)
            })
            .collect(),
    )
}

/// Seed NODES declared `:N` vertices and EDGES undeclared `:R` edges, flushed.
async fn seed() -> Arc<Uni> {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("N")
        .property("id", DataType::Int)
        .done()
        .apply()
        .await
        .unwrap();

    let s = db.session();
    for start in (0..NODES).step_by(CHUNK) {
        let rows = Value::List(
            (start..(start + CHUNK).min(NODES))
                .map(|i| {
                    let mut m = HashMap::new();
                    m.insert("id".to_string(), Value::Int(i as i64));
                    Value::Map(m)
                })
                .collect(),
        );
        let tx = s.tx().await.unwrap();
        tx.execute_with("UNWIND $rows AS r CREATE (:N {id: r.id})")
            .param("rows", rows)
            .run()
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    for start in (0..EDGES).step_by(CHUNK) {
        let tx = s.tx().await.unwrap();
        tx.execute_with(
            "UNWIND $edges AS e \
             MATCH (a:N {id: e.src}), (b:N {id: e.dst}) \
             CREATE (a)-[:R]->(b)",
        )
        .param("edges", edge_chunk(start, (start + CHUNK).min(EDGES)))
        .run()
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }

    db.flush().await.unwrap();

    // Best-effort plan probe: the 1-source query must route through the
    // schemaless main-traverse operator for this bench to measure the right
    // path. Printed, not asserted, so plan-shape drift doesn't break CI.
    if let Ok(res) = s
        .query("EXPLAIN MATCH (a:N {id: 0})-[:R]->(b) RETURN count(b) AS c")
        .await
    {
        let plan = format!("{:?}", res.rows());
        if !plan.contains("TraverseMain") {
            eprintln!("WARNING: 1-source plan does not contain TraverseMain:\n{plan}");
        }
    }

    Arc::new(db)
}

fn bench_schemaless_traversal(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = rt.block_on(seed());
    let mut group = c.benchmark_group("schemaless_traversal");
    group.sample_size(10);

    group.bench_function("one_source_100k_edges", |b| {
        b.iter(|| {
            rt.block_on(async {
                let s = db.session();
                let res = s
                    .query("MATCH (a:N {id: 0})-[:R]->(b) RETURN count(b) AS c")
                    .await
                    .unwrap();
                assert_eq!(res.rows().len(), 1);
            })
        })
    });

    group.bench_function("all_sources_100k_edges", |b| {
        b.iter(|| {
            rt.block_on(async {
                let s = db.session();
                let res = s
                    .query("MATCH (a:N)-[:R]->(b) RETURN count(b) AS c")
                    .await
                    .unwrap();
                assert_eq!(res.rows().len(), 1);
            })
        })
    });

    group.finish();
}

criterion_group!(benches, bench_schemaless_traversal);
criterion_main!(benches);
