// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::builder::{
    FixedSizeListBuilder, Float32Builder, GenericListBuilder, LargeBinaryBuilder, ListBuilder,
    StringBuilder, TimestampNanosecondBuilder, UInt64Builder,
};
use arrow_array::{BooleanArray, RecordBatch, StringArray, UInt64Array};
use criterion::{Criterion, criterion_group, criterion_main};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::runtime::Runtime;
use uni_common::graph::simple_graph::SimpleGraph;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::runtime::Direction as SimpleDirection;
use uni_db::storage::manager::StorageManager;

fn bench_vector_search(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let storage_str = path.join("storage").to_str().unwrap().to_string();

    // Setup Data (Once)
    rt.block_on(async {
        let schema_manager = SchemaManager::load(&path.join("schema.json"))
            .await
            .unwrap();
        schema_manager.add_label("Item").unwrap();
        schema_manager
            .add_property(
                "Item",
                "embedding",
                DataType::Vector { dimensions: 128 },
                false,
            )
            .unwrap();
        schema_manager.save().await.unwrap();

        let schema_manager = Arc::new(schema_manager);
        let storage = StorageManager::new(&storage_str, schema_manager.clone())
            .await
            .unwrap();
        let ds = storage.vertex_dataset("Item").unwrap();
        let schema = ds.get_arrow_schema(&schema_manager.schema()).unwrap();

        // Write 1000 items with 128-dim vectors
        let num_items = 1000;
        let dim = 128;

        let vids: Vec<u64> = (0..num_items).map(|i| Vid::new(i).as_u64()).collect();

        let mut vec_builder = FixedSizeListBuilder::new(Float32Builder::new(), dim);
        for _ in 0..num_items {
            for j in 0..dim {
                vec_builder.values().append_value(j as f32);
            }
            vec_builder.append(true);
        }

        // Build nullable metadata columns
        let n = num_items as usize;
        let ext_id_array = StringArray::from(vec![None::<&str>; n]); // ext_id (nullable)

        let mut labels_builder: GenericListBuilder<i32, StringBuilder> =
            ListBuilder::new(StringBuilder::new());
        for _ in 0..n {
            labels_builder.values().append_value("Item");
            labels_builder.append(true);
        }
        let labels_array = labels_builder.finish();

        let mut created_builder = TimestampNanosecondBuilder::new().with_timezone("UTC");
        for _ in 0..n {
            created_builder.append_null();
        }
        let created_array = created_builder.finish();

        let mut updated_builder = TimestampNanosecondBuilder::new().with_timezone("UTC");
        for _ in 0..n {
            updated_builder.append_null();
        }
        let updated_array = updated_builder.finish();

        let mut overflow_builder = LargeBinaryBuilder::new();
        for _ in 0..n {
            overflow_builder.append_null();
        }
        let overflow_array = overflow_builder.finish();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vids)), // _vid
                Arc::new(arrow_array::FixedSizeBinaryArray::new(
                    32,
                    vec![0u8; 32 * n].into(),
                    None,
                )), // _uid
                Arc::new(BooleanArray::from(vec![false; n])), // _deleted
                Arc::new(UInt64Array::from(vec![1; n])), // _version
                Arc::new(ext_id_array),            // ext_id
                Arc::new(labels_array),            // _labels
                Arc::new(created_array),           // _created_at
                Arc::new(updated_array),           // _updated_at
                Arc::new(vec_builder.finish()),    // embedding
                Arc::new(overflow_array),          // overflow_json
            ],
        )
        .unwrap();

        let backend = storage.backend();
        ds.write_batch(backend, batch, &schema_manager.schema())
            .await
            .unwrap();
    });

    let schema_manager = rt
        .block_on(SchemaManager::load(&path.join("schema.json")))
        .unwrap();
    let storage = rt
        .block_on(StorageManager::new(&storage_str, Arc::new(schema_manager)))
        .unwrap();

    let query = vec![0.0f32; 128];

    c.bench_function("vector_search_1k_128d", |b| {
        b.iter(|| {
            rt.block_on(async {
                storage
                    .vector_search("Item", "embedding", &query, 10, None, None)
                    .await
                    .unwrap();
            })
        })
    });
}

fn bench_graph_traversal(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let storage_str = path.join("storage").to_str().unwrap().to_string();

    rt.block_on(async {
        let schema_manager = SchemaManager::load(&path.join("schema.json"))
            .await
            .unwrap();
        let _l_node = schema_manager.add_label("Node").unwrap();
        let _t_link = schema_manager
            .add_edge_type("LINK", vec!["Node".into()], vec!["Node".into()])
            .unwrap();
        schema_manager.save().await.unwrap();

        let schema_manager = Arc::new(schema_manager);
        let storage = StorageManager::new(&storage_str, schema_manager.clone())
            .await
            .unwrap();

        // Chain: 0 -> 1 -> 2 ... -> 100
        let num_nodes = 100;
        let adj_ds = storage.adjacency_dataset("LINK", "Node", "fwd").unwrap();

        let mut src_vids = Vec::new();
        let mut n_builder = ListBuilder::new(UInt64Builder::new());
        let mut e_builder = ListBuilder::new(UInt64Builder::new());

        for i in 0..num_nodes {
            src_vids.push(Vid::new(i as u64).as_u64());
            // Link to i+1
            if i < num_nodes - 1 {
                n_builder
                    .values()
                    .append_value(Vid::new(i as u64 + 1).as_u64());
                n_builder.append(true);
                e_builder.values().append_value(Eid::new(i as u64).as_u64());
                e_builder.append(true);
            } else {
                n_builder.append(true);
                e_builder.append(true);
            }
        }

        let batch = RecordBatch::try_new(
            adj_ds.get_arrow_schema(),
            vec![
                Arc::new(UInt64Array::from(src_vids)),
                Arc::new(n_builder.finish()),
                Arc::new(e_builder.finish()),
            ],
        )
        .unwrap();
        let backend = storage.backend();
        adj_ds.write_chunk(backend, batch).await.unwrap();
    });

    let schema_manager = rt
        .block_on(SchemaManager::load(&path.join("schema.json")))
        .unwrap();
    let storage = rt
        .block_on(StorageManager::new(&storage_str, Arc::new(schema_manager)))
        .unwrap();
    let start_vid = Vid::new(0); // Node 0

    c.bench_function("traversal_1hop_100_chain", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Traverse 1 hop from 0
                storage
                    .load_subgraph(&[start_vid], &[1], 1, SimpleDirection::Outgoing, None)
                    .await
                    .unwrap();
            })
        })
    });
}

// Benchmark SimpleGraph construction
fn bench_graph_construction(c: &mut Criterion) {
    let num_vertices = 10000;
    let num_edges = 50000;

    // Pre-generate data
    let vids: Vec<Vid> = (0..num_vertices).map(Vid::new).collect();
    let edges: Vec<(usize, usize, Eid)> = (0..num_edges)
        .map(|i| {
            let src = i % num_vertices as usize;
            let dst = (i * 7 + 13) % num_vertices as usize;
            (src, dst, Eid::new(i as u64))
        })
        .collect();

    c.bench_function("simple_graph_construct_10k_50k", |b| {
        b.iter(|| {
            let mut g = SimpleGraph::new();
            for &vid in &vids {
                g.add_vertex(vid);
            }
            for &(src, dst, eid) in &edges {
                g.add_edge(vids[src], vids[dst], eid, 1);
            }
            g
        })
    });

    // Optimized: pre-allocate + unchecked edge insertion
    c.bench_function("simple_graph_construct_optimized_10k_50k", |b| {
        b.iter(|| {
            let mut g = SimpleGraph::with_capacity(num_vertices.try_into().unwrap(), num_edges);
            for &vid in &vids {
                g.add_vertex(vid);
            }
            for &(src, dst, eid) in &edges {
                g.add_edge_unchecked(vids[src], vids[dst], eid, 1);
            }
            g
        })
    });
}

fn bench_neighbor_iteration(c: &mut Criterion) {
    let num_vertices = 1000;
    let edges_per_vertex = 50;

    // Build SimpleGraph
    let mut sg = SimpleGraph::new();
    let vids: Vec<Vid> = (0..num_vertices).map(Vid::new).collect();
    for &vid in &vids {
        sg.add_vertex(vid);
    }
    for (i, &vid) in vids.iter().enumerate() {
        for j in 0..edges_per_vertex {
            let dst_idx = (i + j + 1) % num_vertices as usize;
            sg.add_edge(
                vid,
                vids[dst_idx],
                Eid::new((i * edges_per_vertex + j) as u64),
                1,
            );
        }
    }

    let test_vid = vids[500];

    c.bench_function("simple_graph_neighbors_iterate_1k_50deg", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for edge in sg.neighbors(test_vid, SimpleDirection::Outgoing) {
                sum += edge.dst_vid.as_u64();
            }
            sum
        })
    });

    // Also test just getting the slice/iterator (access pattern)
    c.bench_function("simple_graph_neighbors_access_1k_50deg", |b| {
        b.iter(|| sg.neighbors(test_vid, SimpleDirection::Outgoing).len())
    });
}

criterion_group!(
    benches,
    bench_vector_search,
    bench_graph_traversal,
    bench_graph_construction,
    bench_neighbor_iteration
);
criterion_main!(benches);
