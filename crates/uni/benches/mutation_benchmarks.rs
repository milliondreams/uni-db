// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Mutation benchmarks for the DataFusion engine.
//!
//! ```bash
//! cargo bench --bench mutation_benchmarks
//! ```

use std::collections::HashMap;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

/// Create an in-memory Uni instance.
async fn make_db() -> Uni {
    Uni::in_memory().build().await.unwrap()
}

/// Seed `count` schemaless nodes with an `idx` property.
async fn seed_nodes(db: &Uni, count: usize) {
    let s = db.session();
    let tx = s.tx().await.unwrap();
    for i in 0..count {
        tx.execute(&format!("CREATE (n:BenchNode {{idx: {i}}})"))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
}

fn bench_create_100_nodes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_create_100_nodes");
    group.sample_size(10);

    group.bench_function("df_path", |b| {
        b.iter_batched(
            || rt.block_on(make_db()),
            |db| {
                rt.block_on(async {
                    let s = db.session();
                    let tx = s.tx().await.unwrap();
                    for i in 0..100 {
                        tx.execute(&format!("CREATE (n:BenchNode {{idx: {i}}})"))
                            .await
                            .unwrap();
                    }
                    tx.commit().await.unwrap();
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_set_100_properties(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_set_100_props");
    group.sample_size(10);

    group.bench_function("df_path", |b| {
        b.iter_batched(
            || {
                let db = rt.block_on(make_db());
                rt.block_on(seed_nodes(&db, 100));
                db
            },
            |db| {
                rt.block_on(async {
                    let s = db.session();
                    let tx = s.tx().await.unwrap();
                    tx.execute("MATCH (n:BenchNode) SET n.updated = true")
                        .await
                        .unwrap();
                    tx.commit().await.unwrap();
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_delete_100_nodes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_delete_100_nodes");
    group.sample_size(10);

    group.bench_function("df_path", |b| {
        b.iter_batched(
            || {
                let db = rt.block_on(make_db());
                rt.block_on(seed_nodes(&db, 100));
                db
            },
            |db| {
                rt.block_on(async {
                    let s = db.session();
                    let tx = s.tx().await.unwrap();
                    tx.execute("MATCH (n:BenchNode) DETACH DELETE n")
                        .await
                        .unwrap();
                    tx.commit().await.unwrap();
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_create_then_match(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_create_then_match");
    group.sample_size(10);

    group.bench_function("df_path", |b| {
        b.iter_batched(
            || rt.block_on(make_db()),
            |db| {
                rt.block_on(async {
                    let s = db.session();
                    let tx = s.tx().await.unwrap();
                    for i in 0..50 {
                        tx.execute(&format!("CREATE (n:BenchNode {{idx: {i}}})"))
                            .await
                            .unwrap();
                    }
                    tx.commit().await.unwrap();
                    let result = s
                        .query("MATCH (n:BenchNode) RETURN count(n) AS cnt")
                        .await
                        .unwrap();
                    assert_eq!(result.rows().len(), 1);
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_merge_50_nodes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_merge_50_nodes");
    group.sample_size(10);

    group.bench_function("df_path", |b| {
        b.iter_batched(
            || rt.block_on(make_db()),
            |db| {
                rt.block_on(async {
                    let s = db.session();
                    let tx = s.tx().await.unwrap();
                    // First pass: all creates
                    for i in 0..50 {
                        tx.execute(&format!("MERGE (n:BenchNode {{idx: {i}}})"))
                            .await
                            .unwrap();
                    }
                    // Second pass: all matches
                    for i in 0..50 {
                        tx.execute(&format!("MERGE (n:BenchNode {{idx: {i}}})"))
                            .await
                            .unwrap();
                    }
                    tx.commit().await.unwrap();
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

/// Build the `$rows` param for the ext_id ingest bench.
fn extid_rows(n: usize) -> Value {
    Value::List(
        (0..n)
            .map(|i| {
                let mut m = HashMap::new();
                m.insert("eid".to_string(), Value::String(format!("ext{i}")));
                Value::Map(m)
            })
            .collect(),
    )
}

/// Ingest of vertices carrying `ext_id` (globally-unique check per insert).
///
/// The per-insert uniqueness check scans every L0 vertex-property map, so a
/// single transaction inserting N rows is O(N^2) today. The size sweep makes
/// the quadratic curve visible (4x rows should be ~16x time if quadratic).
fn bench_extid_ingest(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_extid_ingest");
    group.sample_size(10);

    for &n in &[1_000usize, 4_000] {
        group.bench_with_input(BenchmarkId::new("unwind_create", n), &n, |b, &n| {
            b.iter_batched(
                || (rt.block_on(make_db()), extid_rows(n)),
                |(db, rows)| {
                    rt.block_on(async {
                        let s = db.session();
                        let tx = s.tx().await.unwrap();
                        tx.execute_with("UNWIND $rows AS r CREATE (:ExtNode {ext_id: r.eid})")
                            .param("rows", rows)
                            .run()
                            .await
                            .unwrap();
                        tx.commit().await.unwrap();
                    })
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

/// Build the `$batch` param for the batched-MERGE bench: ids `k{from}..k{to}`.
fn merge_batch(from: usize, to: usize) -> Value {
    Value::List(
        (from..to)
            .map(|i| {
                let mut m = HashMap::new();
                m.insert("id".to_string(), Value::String(format!("k{i}")));
                Value::Map(m)
            })
            .collect(),
    )
}

/// Batched `UNWIND $batch MERGE` against a flushed (persisted) vertex table.
///
/// Seeds 1000 keyed nodes and flushes them to Lance so the MERGE lookup must
/// consult the persisted tier; the batch is a 50/50 mix of existing and new
/// keys. Today each row issues an independent Lance scan; a batched lookup
/// should collapse this to one scan per statement. The transaction is rolled
/// back each iteration so every sample sees identical state.
fn bench_unwind_merge_batched(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_unwind_merge_batched");
    group.sample_size(10);

    let db = rt.block_on(async {
        let db = make_db().await;
        db.schema()
            .label("MergeNode")
            .property("entity_id", DataType::String)
            .index("entity_id", IndexType::Scalar(ScalarType::BTree))
            .done()
            .apply()
            .await
            .unwrap();
        let s = db.session();
        let tx = s.tx().await.unwrap();
        tx.execute_with("UNWIND $rows AS r CREATE (:MergeNode {entity_id: r.id})")
            .param("rows", merge_batch(0, 1_000))
            .run()
            .await
            .unwrap();
        tx.commit().await.unwrap();
        db.flush().await.unwrap();
        db
    });

    group.bench_function("merge_1000_rows_50pct_hit", |b| {
        b.iter_batched(
            || merge_batch(500, 1_500),
            |batch| {
                rt.block_on(async {
                    let s = db.session();
                    let tx = s.tx().await.unwrap();
                    tx.execute_with(
                        "UNWIND $batch AS e MERGE (n:MergeNode {entity_id: e.id}) \
                         ON CREATE SET n.fresh = true ON MATCH SET n.seen = true",
                    )
                    .param("batch", batch)
                    .run()
                    .await
                    .unwrap();
                    tx.rollback();
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

/// `{src, dst, w}` rows linking `k{i}` → `k{i+1}` for the edge-MERGE bench.
fn edge_batch(n: usize) -> Value {
    Value::List(
        (0..n)
            .map(|i| {
                let mut m = HashMap::new();
                m.insert("src".to_string(), Value::String(format!("k{i}")));
                m.insert("dst".to_string(), Value::String(format!("k{}", i + 1)));
                m.insert("w".to_string(), Value::Int(1));
                Value::Map(m)
            })
            .collect(),
    )
}

/// Batched `UNWIND … MATCH … MERGE (a)-[r]->(b)` — the GENERAL (non-fastpath)
/// MERGE path: a relationship pattern never qualifies for
/// `merge_single_node_fastpath`, so every row plans and runs a full
/// `execute_merge_match`, and ON CREATE/ON MATCH SET property reads execute
/// with an empty prefetch. Canonical edge-ingest shape.
///
/// Seeds 1001 keyed nodes and 500 `REL` edges, flushed to Lance; the batch is
/// a 50/50 mix of existing and new edges. The transaction is rolled back each
/// iteration so every sample sees identical state.
fn bench_unwind_merge_edge_general(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_unwind_merge_edge_general");
    group.sample_size(10);

    let db = rt.block_on(async {
        let db = make_db().await;
        db.schema()
            .label("MergeNode")
            .property("entity_id", DataType::String)
            .index("entity_id", IndexType::Scalar(ScalarType::BTree))
            .done()
            .edge_type("REL", &["MergeNode"], &["MergeNode"])
            .property_nullable("weight", DataType::Int64)
            .done()
            .apply()
            .await
            .unwrap();
        let s = db.session();
        let tx = s.tx().await.unwrap();
        tx.execute_with("UNWIND $rows AS r CREATE (:MergeNode {entity_id: r.id})")
            .param("rows", merge_batch(0, 1_001))
            .run()
            .await
            .unwrap();
        tx.execute_with(
            "UNWIND $rows AS e \
             MATCH (a:MergeNode {entity_id: e.src}), (b:MergeNode {entity_id: e.dst}) \
             CREATE (a)-[:REL {weight: 1}]->(b)",
        )
        .param("rows", edge_batch(500))
        .run()
        .await
        .unwrap();
        tx.commit().await.unwrap();
        db.flush().await.unwrap();
        db
    });

    // No-SET variant — isolates the per-row `execute_merge_match`
    // plan/execute cost from the ON CREATE/ON MATCH SET property reads.
    group.bench_function("merge_edge_1000_rows_no_set", |b| {
        b.iter_batched(
            || edge_batch(1_000),
            |batch| {
                rt.block_on(async {
                    let s = db.session();
                    let tx = s.tx().await.unwrap();
                    tx.execute_with(
                        "UNWIND $batch AS e \
                         MATCH (a:MergeNode {entity_id: e.src}), (b:MergeNode {entity_id: e.dst}) \
                         MERGE (a)-[r:REL]->(b)",
                    )
                    .param("batch", batch)
                    .run()
                    .await
                    .unwrap();
                    tx.rollback();
                })
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("merge_edge_1000_rows_50pct_hit", |b| {
        b.iter_batched(
            || edge_batch(1_000),
            |batch| {
                rt.block_on(async {
                    let s = db.session();
                    let tx = s.tx().await.unwrap();
                    tx.execute_with(
                        "UNWIND $batch AS e \
                         MATCH (a:MergeNode {entity_id: e.src}), (b:MergeNode {entity_id: e.dst}) \
                         MERGE (a)-[r:REL]->(b) \
                         ON CREATE SET r.weight = e.w \
                         ON MATCH SET r.weight = r.weight + e.w",
                    )
                    .param("batch", batch)
                    .run()
                    .await
                    .unwrap();
                    tx.rollback();
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_create_100_nodes,
    bench_set_100_properties,
    bench_delete_100_nodes,
    bench_create_then_match,
    bench_merge_50_nodes,
    bench_extid_ingest,
    bench_unwind_merge_batched,
    bench_unwind_merge_edge_general,
);
criterion_main!(benches);
