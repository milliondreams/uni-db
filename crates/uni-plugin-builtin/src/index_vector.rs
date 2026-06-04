//! Built-in vector-index `IndexKindProvider` — reference exact-KNN
//! implementation.
//!
//! M5b reference: a working in-memory exact-K-Nearest-Neighbor index
//! over Float32 vectors. Useful for tests, prototyping, and as the
//! authoring template user plugins follow when shipping richer index
//! kinds (HNSW, IVF, learned indexes, …).
//!
//! Linear scan over stored vectors; cost is `O(rows × k)` per probe.
//! For production-scale vector workloads, the upcoming Lance-backed
//! vector index (the proposal's M5a/M5b cutover) is the home; this
//! crate keeps the exact-scan path as the conformance-suite fixture.
//!
//! Storage layout:
//! - `vids`: `Vec<i64>` — one per stored row, lookup keys for results.
//! - `vectors`: `Vec<Vec<f32>>` — same row count; each entry is the
//!   stored vector.
//! - `dim`: vector dimension (read from the first input row's vector
//!   length).
//!
//! Source-batch schema (consumed by `build`):
//! - Column 0: `vid` (Int64).
//! - Column 1: `vector` (`FixedSizeList<Float32>`).
//!
//! Query-batch schema (consumed by `probe`):
//! - Column 0: `vector` (`FixedSizeList<Float32>`, length = `dim`).
//!
//! Probe-result schema:
//! - `vid` (Int64).
//! - `distance` (Float32, L2 squared — caller takes sqrt if needed).

use std::sync::Arc;

use arrow_array::{Array, FixedSizeListArray, Float32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use uni_plugin::traits::index::{IndexBuild, IndexHandle, IndexKind, IndexKindProvider};
use uni_plugin::{FnError, PluginError, PluginRegistrar};

/// Register the built-in vector-index provider.
///
/// # Errors
///
/// Returns [`PluginError`] on duplicate kind registration.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.index_kind(IndexKind::new("vector"), Arc::new(VectorIndexProvider))?;
    Ok(())
}

/// Provider for the built-in exact-KNN vector index.
///
/// User plugins can override the `vector` kind by registering after
/// this — the framework's last-write-wins semantics apply per the
/// proposal §3.4.
#[derive(Debug)]
pub struct VectorIndexProvider;

impl IndexKindProvider for VectorIndexProvider {
    fn kind(&self) -> IndexKind {
        IndexKind::new("vector")
    }

    fn build(&self, source: &RecordBatch, _options: &str) -> Result<Box<dyn IndexBuild>, FnError> {
        // Schema validation: column 0 must be Int64 (vid), column 1 must
        // be FixedSizeList<Float32> (vector). The caller provides this
        // shape; we don't try to be too lenient.
        if source.num_columns() < 2 {
            return Err(FnError::new(
                0xA00,
                "vector index: build batch needs (vid: Int64, vector: FixedSizeList<Float32>)",
            ));
        }
        let vids = source
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| FnError::new(0xA01, "vector index: column 0 must be Int64"))?;
        let vector_col = source.column(1);
        let fsl = vector_col
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| {
                FnError::new(
                    0xA02,
                    "vector index: column 1 must be FixedSizeList<Float32>",
                )
            })?;
        let dim = fsl.value_length() as usize;
        if !matches!(fsl.value_type(), DataType::Float32) {
            return Err(FnError::new(
                0xA03,
                "vector index: FixedSizeList element type must be Float32",
            ));
        }

        let mut build = MemoryVectorIndexBuild {
            dim,
            vids: Vec::with_capacity(source.num_rows()),
            vectors: Vec::with_capacity(source.num_rows()),
        };
        for row in 0..source.num_rows() {
            if vids.is_null(row) || vector_col.is_null(row) {
                continue;
            }
            let inner = fsl.value(row);
            let arr = inner
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    FnError::new(0xA04, "vector index: inner array must be Float32Array")
                })?;
            let vec: Vec<f32> = (0..arr.len()).map(|i| arr.value(i)).collect();
            build.vids.push(vids.value(row));
            build.vectors.push(vec);
        }
        Ok(Box::new(build))
    }

    fn open(&self, persisted: &[u8]) -> Result<Box<dyn IndexHandle>, FnError> {
        let (dim, vids, vectors): (usize, Vec<i64>, Vec<Vec<f32>>) =
            serde_json::from_slice(persisted)
                .map_err(|e| FnError::new(0xA10, format!("vector index: open deserialize: {e}")))?;
        Ok(Box::new(MemoryVectorIndex { dim, vids, vectors }))
    }
}

/// In-flight build accumulator.
#[derive(Debug)]
pub struct MemoryVectorIndexBuild {
    dim: usize,
    vids: Vec<i64>,
    vectors: Vec<Vec<f32>>,
}

impl IndexBuild for MemoryVectorIndexBuild {
    fn finalize(self: Box<Self>) -> Result<Box<dyn IndexHandle>, FnError> {
        let Self { dim, vids, vectors } = *self;
        Ok(Box::new(MemoryVectorIndex { dim, vids, vectors }))
    }
}

/// Queryable in-memory exact-KNN vector index.
#[derive(Debug)]
pub struct MemoryVectorIndex {
    dim: usize,
    vids: Vec<i64>,
    vectors: Vec<Vec<f32>>,
}

impl IndexHandle for MemoryVectorIndex {
    fn probe(&self, query: &RecordBatch, k: usize) -> Result<RecordBatch, FnError> {
        if query.num_columns() < 1 {
            return Err(FnError::new(
                0xA20,
                "vector index: probe batch needs column 0 = FixedSizeList<Float32>",
            ));
        }
        let q_col = query.column(0);
        let fsl = q_col
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| {
                FnError::new(0xA21, "vector index: probe column 0 must be FixedSizeList")
            })?;
        if fsl.value_length() as usize != self.dim {
            return Err(FnError::new(
                0xA22,
                format!(
                    "vector index: probe vector dim {} != index dim {}",
                    fsl.value_length(),
                    self.dim
                ),
            ));
        }

        // For each query row, compute L2² to every stored vector and
        // return the top-k by ascending distance.
        let mut out_vids: Vec<i64> = Vec::new();
        let mut out_dists: Vec<f32> = Vec::new();
        for row in 0..query.num_rows() {
            let inner = fsl.value(row);
            let qa = inner
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| FnError::new(0xA23, "vector index: inner must be Float32"))?;
            let q: Vec<f32> = (0..qa.len()).map(|i| qa.value(i)).collect();

            let mut scored: Vec<(f32, i64)> = self
                .vectors
                .iter()
                .zip(self.vids.iter())
                .map(|(v, id)| (l2_squared(&q, v), *id))
                .collect();
            // Partial sort by ascending distance.
            scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            for (d, id) in scored.into_iter().take(k) {
                out_dists.push(d);
                out_vids.push(id);
            }
        }

        let schema = self.schema();
        let vids_arr: Arc<dyn Array> = Arc::new(Int64Array::from(out_vids));
        let dists_arr: Arc<dyn Array> = Arc::new(Float32Array::from(out_dists));
        RecordBatch::try_new(Arc::clone(&schema), vec![vids_arr, dists_arr])
            .map_err(|e| FnError::new(0xA24, format!("vector index: build result: {e}")))
    }

    fn persist(&self) -> Result<Vec<u8>, FnError> {
        serde_json::to_vec(&(self.dim, &self.vids, &self.vectors))
            .map_err(|e| FnError::new(0xA30, format!("vector index: persist: {e}")))
    }

    fn schema(&self) -> SchemaRef {
        static SCHEMA: std::sync::OnceLock<SchemaRef> = std::sync::OnceLock::new();
        Arc::clone(SCHEMA.get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("vid", DataType::Int64, false),
                Field::new("distance", DataType::Float32, false),
            ]))
        }))
    }
}

fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_build_batch(dim: usize, rows: &[(i64, Vec<f32>)]) -> RecordBatch {
        use arrow_array::builder::FixedSizeListBuilder;
        let schema = Arc::new(Schema::new(vec![
            Field::new("vid", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dim as i32,
                ),
                false,
            ),
        ]));
        let vids: Vec<i64> = rows.iter().map(|(v, _)| *v).collect();
        let mut fsb =
            FixedSizeListBuilder::new(arrow_array::builder::Float32Builder::new(), dim as i32);
        for (_, v) in rows {
            assert_eq!(v.len(), dim, "test fixture vector dim mismatch");
            for x in v {
                fsb.values().append_value(*x);
            }
            fsb.append(true);
        }
        let vec_arr = fsb.finish();
        let vids_arr: Arc<dyn Array> = Arc::new(Int64Array::from(vids));
        let vec_arr: Arc<dyn Array> = Arc::new(vec_arr);
        RecordBatch::try_new(schema, vec![vids_arr, vec_arr]).unwrap()
    }

    fn make_query_batch(dim: usize, queries: &[Vec<f32>]) -> RecordBatch {
        use arrow_array::builder::FixedSizeListBuilder;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            false,
        )]));
        let mut fsb =
            FixedSizeListBuilder::new(arrow_array::builder::Float32Builder::new(), dim as i32);
        for v in queries {
            for x in v {
                fsb.values().append_value(*x);
            }
            fsb.append(true);
        }
        let fsl = fsb.finish();
        let arr: Arc<dyn Array> = Arc::new(fsl);
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    #[test]
    fn build_then_probe_returns_nearest_first() {
        let dim = 2;
        let source = make_build_batch(
            dim,
            &[
                (10, vec![0.0, 0.0]),
                (20, vec![1.0, 0.0]),
                (30, vec![0.0, 1.0]),
                (40, vec![10.0, 10.0]),
            ],
        );
        let provider = VectorIndexProvider;
        let build = provider.build(&source, "").unwrap();
        let handle = build.finalize().unwrap();

        let query = make_query_batch(dim, &[vec![0.1, 0.1]]);
        let result = handle.probe(&query, 2).unwrap();
        assert_eq!(result.num_rows(), 2);
        let vids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // (0,0) is nearest to (0.1,0.1); both (1,0) and (0,1) tie for 2nd.
        assert_eq!(vids.value(0), 10);
        assert!(matches!(vids.value(1), 20 | 30));
    }

    #[test]
    fn probe_rejects_mismatched_dim() {
        let dim = 2;
        let source = make_build_batch(dim, &[(1, vec![0.0, 0.0])]);
        let handle = VectorIndexProvider
            .build(&source, "")
            .unwrap()
            .finalize()
            .unwrap();
        let bad_query = make_query_batch(3, &[vec![0.0, 0.0, 0.0]]);
        let err = handle.probe(&bad_query, 1).unwrap_err();
        assert_eq!(err.code, 0xA22);
    }

    #[test]
    fn persist_round_trip() {
        let dim = 2;
        let source = make_build_batch(dim, &[(7, vec![0.5, 0.5]), (8, vec![1.0, 1.0])]);
        let handle = VectorIndexProvider
            .build(&source, "")
            .unwrap()
            .finalize()
            .unwrap();
        let bytes = handle.persist().unwrap();
        let restored = VectorIndexProvider.open(&bytes).unwrap();
        let query = make_query_batch(dim, &[vec![0.4, 0.5]]);
        let r = restored.probe(&query, 1).unwrap();
        let v = r.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(v.value(0), 7);
    }

    #[test]
    fn probe_handles_multi_row_query_batch() {
        let dim = 2;
        let source = make_build_batch(dim, &[(1, vec![0.0, 0.0]), (2, vec![5.0, 5.0])]);
        let handle = VectorIndexProvider
            .build(&source, "")
            .unwrap()
            .finalize()
            .unwrap();
        let query = make_query_batch(dim, &[vec![0.1, 0.1], vec![5.1, 5.1]]);
        let r = handle.probe(&query, 1).unwrap();
        // 2 queries × k=1 each → 2 result rows.
        assert_eq!(r.num_rows(), 2);
        let vids = r.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(vids.value(0), 1);
        assert_eq!(vids.value(1), 2);
    }

    #[test]
    fn provider_kind_is_vector() {
        assert_eq!(VectorIndexProvider.kind(), IndexKind::new("vector"));
    }
}
