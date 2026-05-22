// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Standalone probe: prove that Lance 3.0.1's `MergeInsertBuilder` can
//! update a subset of columns on rows joined by `_vid` WITHOUT
//! disturbing an HnswSq vector index on the untouched `embedding`
//! column.
//!
//! This is the blocking question for the "true partial L0 writes"
//! workstream documented in
//! `/home/rohit/.claude/plans/plan-and-implement-a-valiant-flame.md`
//! (Round 3 deferral). If MergeInsert preserves untouched columns and
//! the HnswSq index continues returning correct nearest-neighbor
//! results after `optimize_indices(OptimizeOptions::append())`, we can
//! wire it into `execute_set_items_locked` and skip the
//! read-all-then-write-all cycle on UPDATE paths.
//!
//! Outcomes the probe verifies:
//!   1. `frequency` / `confidence` (the SET targets) reflect the merge.
//!   2. `embedding` is byte-equal to the pre-merge value for every row
//!      (no NULL fill, no zero fill).
//!   3. After `optimize_indices(append())`, a KNN query returns the
//!      same row that the pre-merge query did (index integrity).
//!   4. `MergeStats::num_updated_rows` matches the source batch size.

// Rust guideline compliant

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, UInt64Type};
use arrow_array::{
    FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch, RecordBatchIterator,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched};
use lance::index::vector::VectorIndexParams;
use lance_index::DatasetIndexExt;
use lance_index::optimize::OptimizeOptions;
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_linalg::distance::MetricType;
use tempfile::TempDir;

const DIM: usize = 64;
const N_ROWS: usize = 256;

fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("_vid", DataType::UInt64, false),
        Field::new("frequency", DataType::Int64, true),
        Field::new("confidence", DataType::Float64, true),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            true,
        ),
    ]))
}

fn merge_source_schema() -> Arc<ArrowSchema> {
    // SET clause touches frequency + confidence only; embedding is absent.
    Arc::new(ArrowSchema::new(vec![
        Field::new("_vid", DataType::UInt64, false),
        Field::new("frequency", DataType::Int64, true),
        Field::new("confidence", DataType::Float64, true),
    ]))
}

/// Deterministic embedding values: rowi[j] = (i * 1.0 + j * 0.001).
/// Each row's embedding is distinct, so KNN with row k's embedding as
/// the query has a unique nearest neighbor (row k itself).
fn deterministic_floats() -> Vec<f32> {
    let mut out = Vec::with_capacity(N_ROWS * DIM);
    for i in 0..N_ROWS {
        for j in 0..DIM {
            out.push(i as f32 + j as f32 * 0.001);
        }
    }
    out
}

fn make_seed_batch() -> (RecordBatch, Vec<f32>) {
    let all_floats = deterministic_floats();
    let vids: Vec<u64> = (0..N_ROWS as u64).collect();
    let freqs: Vec<i64> = (0..N_ROWS as i64).map(|i| i * 10).collect();
    let confs: Vec<f64> = (0..N_ROWS).map(|i| i as f64 * 0.01).collect();

    let values = Arc::new(Float32Array::from(all_floats.clone()));
    let field = Arc::new(Field::new("item", DataType::Float32, true));
    let embedding = FixedSizeListArray::try_new(field, DIM as i32, values, None).unwrap();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(UInt64Array::from(vids)),
            Arc::new(Int64Array::from(freqs)),
            Arc::new(Float64Array::from(confs)),
            Arc::new(embedding),
        ],
    )
    .unwrap();
    (batch, all_floats)
}

fn make_merge_source(updated_count: usize) -> RecordBatch {
    let vids: Vec<u64> = (0..updated_count as u64).collect();
    let freqs: Vec<i64> = (0..updated_count as i64).map(|i| -1 - i).collect();
    let confs: Vec<f64> = (0..updated_count).map(|i| 0.5 + i as f64 * 0.001).collect();
    RecordBatch::try_new(
        merge_source_schema(),
        vec![
            Arc::new(UInt64Array::from(vids)),
            Arc::new(Int64Array::from(freqs)),
            Arc::new(Float64Array::from(confs)),
        ],
    )
    .unwrap()
}

/// E1 — Probe Lance MergeInsert with partial-column source against an
/// HnswSq vector index. See module docs.
#[tokio::test]
async fn lance_merge_insert_partial_columns_preserves_hnsw_sq() {
    let dir = TempDir::new().unwrap();
    let uri = format!("{}/ds.lance", dir.path().display());

    // 1. Seed dataset.
    let (seed_batch, original_floats) = make_seed_batch();
    let reader =
        RecordBatchIterator::new(std::iter::once(Ok(seed_batch.clone())), seed_batch.schema());
    let mut dataset = Dataset::write(reader, &uri, None).await.unwrap();

    // 2. Build HnswSq vector index on `embedding`.
    let ivf = IvfBuildParams::new(2);
    let hnsw = HnswBuildParams::default();
    let sq = SQBuildParams::default();
    let params = VectorIndexParams::with_ivf_hnsw_sq_params(MetricType::L2, ivf, hnsw, sq);
    dataset
        .create_index(
            &["embedding"],
            lance_index::IndexType::Vector,
            None,
            &params,
            true,
        )
        .await
        .unwrap();

    // 3. Baseline KNN: querying with row 7's embedding must return row 7.
    let q: Vec<f32> = original_floats[7 * DIM..8 * DIM].to_vec();
    let knn_pre = run_knn(&dataset, &q).await;
    assert_eq!(
        knn_pre, 7,
        "pre-merge KNN must return seed row 7; got {knn_pre}"
    );

    // 4. MergeInsert with partial-column source (NO `embedding` column).
    let merge_count = 50;
    let source = make_merge_source(merge_count);
    let source_schema = source.schema();
    let source_iter = RecordBatchIterator::new(std::iter::once(Ok(source.clone())), source_schema);
    let merge_job =
        MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["_vid".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap();
    let (updated_dataset, merge_stats) = merge_job.execute_reader(source_iter).await.unwrap();
    assert_eq!(
        merge_stats.num_updated_rows, merge_count as u64,
        "MergeStats.num_updated_rows mismatch"
    );
    assert_eq!(
        merge_stats.num_inserted_rows, 0,
        "no inserts expected with WhenNotMatched::DoNothing"
    );

    // 5. Row-by-row verification: embedding preserved, frequency/confidence updated.
    let scanner = updated_dataset.scan().try_into_stream().await.unwrap();
    let batches: Vec<RecordBatch> = scanner.try_collect().await.unwrap();
    let mut rows: std::collections::HashMap<u64, (i64, f64, Vec<f32>)> =
        std::collections::HashMap::new();
    for batch in batches {
        let vids = batch
            .column_by_name("_vid")
            .unwrap()
            .as_primitive::<UInt64Type>();
        let freqs = batch
            .column_by_name("frequency")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>();
        let confs = batch
            .column_by_name("confidence")
            .unwrap()
            .as_primitive::<arrow_array::types::Float64Type>();
        let embs = batch
            .column_by_name("embedding")
            .unwrap()
            .as_fixed_size_list();
        for i in 0..batch.num_rows() {
            let vid = vids.value(i);
            let freq = freqs.value(i);
            let conf = confs.value(i);
            let emb_vec: Vec<f32> = embs
                .value(i)
                .as_primitive::<Float32Type>()
                .values()
                .to_vec();
            rows.insert(vid, (freq, conf, emb_vec));
        }
    }

    for vid in 0..N_ROWS as u64 {
        let (freq, conf, emb) = rows
            .get(&vid)
            .unwrap_or_else(|| panic!("row {vid} missing"));
        let expected_emb = &original_floats[vid as usize * DIM..(vid as usize + 1) * DIM];
        assert_eq!(
            emb.as_slice(),
            expected_emb,
            "row {vid}: embedding was disturbed by partial MergeInsert"
        );
        if (vid as usize) < merge_count {
            assert_eq!(*freq, -1 - vid as i64, "row {vid}: frequency not updated");
            let want_conf = 0.5 + vid as f64 * 0.001;
            assert!(
                (conf - want_conf).abs() < 1e-9,
                "row {vid}: confidence not updated (got {conf}, want {want_conf})"
            );
        } else {
            assert_eq!(*freq, vid as i64 * 10, "row {vid}: untouched freq mutated");
        }
    }

    // 6. Bring HnswSq up-to-date with new fragments and re-query.
    let mut updated_for_optimize = (*updated_dataset).clone();
    updated_for_optimize
        .optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();
    let dataset = Dataset::open(&uri).await.unwrap();
    let knn_post = run_knn(&dataset, &q).await;
    assert_eq!(
        knn_post, 7,
        "post-merge HnswSq query returned {knn_post}, expected 7"
    );
}

async fn run_knn(dataset: &Dataset, query: &[f32]) -> u64 {
    let mut scanner = dataset.scan();
    scanner
        .nearest("embedding", &Float32Array::from(query.to_vec()), 1)
        .unwrap()
        .project(&["_vid"])
        .unwrap();
    let batch = scanner.try_into_batch().await.unwrap();
    let vids = batch
        .column_by_name("_vid")
        .unwrap()
        .as_primitive::<UInt64Type>();
    vids.value(0)
}
