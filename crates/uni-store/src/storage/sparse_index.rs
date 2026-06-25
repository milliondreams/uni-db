// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Scored sparse-vector (SPLADE / learned-sparse) inverted index.
//!
//! Forked from [`super::inverted_index`], but with three differences that make
//! it a *scored* index rather than a set-membership one:
//!
//! 1. Postings are `term_id (u32) -> [(vid, weight)]` instead of
//!    `term (String) -> [vid]`. Scoring is a dot product over shared terms.
//! 2. The on-disk schema carries per-term `weights` and a `max_impact` upper
//!    bound (the prerequisite for P2 block-max pruning).
//! 3. [`SparseVectorIndex::query_topk`] returns scored, ranked results via a
//!    dot-product accumulator + bounded min-heap, not an unordered VID set.
//!
//! Weights are stored losslessly as `f32` in this milestone; 8-bit quantization
//! (config `quantize`, paired with P2 block-max pruning) is a later milestone.
//! MVCC / tombstone correctness is applied by the *query orchestration* layer
//! (uni-query), exactly as the dense `vector_search` path does — this module is
//! the storage kernel.

use crate::storage::vertex::VertexDataset;
use anyhow::{Result, anyhow};
use arrow_array::types::{Float32Type, UInt64Type};
use arrow_array::{
    Array, Float32Array, ListArray, RecordBatch, RecordBatchIterator, StructArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::Dataset;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, info, instrument};
use uni_common::core::id::Vid;
use uni_common::core::schema::SparseVectorIndexConfig;

/// Default memory limit for postings accumulation (256 MB), matching the
/// set-membership inverted index.
const DEFAULT_MAX_POSTINGS_MEMORY: usize = 256 * 1024 * 1024;

/// One term's postings: parallel vid + weight vectors.
type Postings = HashMap<u32, Vec<(u64, f32)>>;

/// Estimate memory usage of a postings map (term key + parallel vid/weight pairs).
fn estimated_postings_memory(postings: &Postings) -> usize {
    postings
        .values()
        .map(|v| std::mem::size_of::<u32>() + std::mem::size_of::<Vec<(u64, f32)>>() + v.len() * 12)
        .sum()
}

/// Merge multiple postings segments into one (concatenates per-term lists).
fn merge_postings_segments(segments: Vec<Postings>) -> Postings {
    let mut merged: Postings = HashMap::new();
    for segment in segments {
        for (term, entries) in segment {
            merged.entry(term).or_default().extend(entries);
        }
    }
    merged
}

/// Read a sparse vector `(indices, values)` from row `row` of a `Struct
/// { indices: List<UInt32>, values: List<Float32> }` column. Returns `None` if
/// the struct row is null (deleted / absent).
fn read_sparse_row(struct_arr: &StructArray, row: usize) -> Option<(Vec<u32>, Vec<f32>)> {
    if struct_arr.is_null(row) {
        return None;
    }
    let indices_list = struct_arr
        .column_by_name("indices")?
        .as_any()
        .downcast_ref::<ListArray>()?;
    let values_list = struct_arr
        .column_by_name("values")?
        .as_any()
        .downcast_ref::<ListArray>()?;
    let idx_vals = indices_list.value(row);
    let idx_arr = idx_vals.as_any().downcast_ref::<UInt32Array>()?;
    let w_vals = values_list.value(row);
    let w_arr = w_vals.as_any().downcast_ref::<Float32Array>()?;
    let indices = (0..idx_arr.len()).map(|i| idx_arr.value(i)).collect();
    let values = (0..w_arr.len()).map(|i| w_arr.value(i)).collect();
    Some((indices, values))
}

/// Scored sparse-vector inverted index over a `DataType::SparseVector` column.
pub struct SparseVectorIndex {
    dataset: Option<Dataset>,
    base_uri: String,
    label: String,
    property: String,
    #[allow(dead_code)]
    config: SparseVectorIndexConfig,
}

impl std::fmt::Debug for SparseVectorIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparseVectorIndex")
            .field("base_uri", &self.base_uri)
            .field("label", &self.label)
            .field("property", &self.property)
            .field("initialized", &self.dataset.is_some())
            .finish_non_exhaustive()
    }
}

impl SparseVectorIndex {
    /// The on-disk dataset path for this index's postings.
    fn postings_path(base_uri: &str, label: &str, property: &str) -> String {
        format!("{base_uri}/indexes/{label}/{property}_sparse")
    }

    /// Open or initialize a sparse index at `base_uri` for the given config.
    pub async fn new(base_uri: &str, config: SparseVectorIndexConfig) -> Result<Self> {
        let path = Self::postings_path(base_uri, &config.label, &config.property);
        let dataset = (Dataset::open(&path).await).ok();
        Ok(Self {
            dataset,
            base_uri: base_uri.to_string(),
            label: config.label.clone(),
            property: config.property.clone(),
            config,
        })
    }

    /// Accumulate one record batch's sparse rows into `postings`. Returns the
    /// count of documents (rows carrying a non-null sparse value) processed.
    /// Deleted rows store a null struct (see `build_sparse_vector_column`), so
    /// `read_sparse_row` skips them and they never enter the postings.
    fn accumulate_batch(&self, batch: &RecordBatch, postings: &mut Postings) -> Result<usize> {
        let vid_col = batch
            .column_by_name("_vid")
            .ok_or_else(|| anyhow!("Missing _vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("Invalid _vid type"))?;
        let term_col = batch
            .column_by_name(&self.property)
            .ok_or_else(|| anyhow!("Missing property {}", self.property))?;
        let struct_arr = term_col
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                anyhow!(
                    "Property {} must be a sparse-vector struct, got {:?}",
                    self.property,
                    term_col.data_type()
                )
            })?;
        let mut count = 0;
        for i in 0..batch.num_rows() {
            let vid = vid_col.value(i);
            let Some((indices, values)) = read_sparse_row(struct_arr, i) else {
                continue;
            };
            for (term, weight) in indices.into_iter().zip(values) {
                postings.entry(term).or_default().push((vid, weight));
            }
            count += 1;
        }
        Ok(count)
    }

    /// Merge accumulated postings (+ any spilled segments) and write to disk.
    async fn finish_build(
        &mut self,
        postings: Postings,
        mut temp_segments: Vec<Postings>,
    ) -> Result<()> {
        if temp_segments.is_empty() {
            self.write_postings(postings).await
        } else {
            temp_segments.push(postings);
            info!(
                segments = temp_segments.len(),
                "Merging sparse postings segments"
            );
            let merged = merge_postings_segments(temp_segments);
            self.write_postings(merged).await
        }
    }

    /// Rebuild the index from already-scanned record batches (the storage
    /// backend's view of the flushed vertex table). This is the canonical
    /// backfill path: the LanceDB-managed table is opened by the backend, not
    /// by a raw `lance::Dataset::open` (whose physical path differs). Uses
    /// segmented accumulation to stay within the memory limit.
    pub async fn build_from_batches(
        &mut self,
        batches: &[RecordBatch],
        progress: impl Fn(usize),
    ) -> Result<()> {
        let mut postings: Postings = HashMap::new();
        let mut temp_segments: Vec<Postings> = Vec::new();
        let mut count = 0;
        for batch in batches {
            count += self.accumulate_batch(batch, &mut postings)?;
            progress(count);
            if estimated_postings_memory(&postings) > DEFAULT_MAX_POSTINGS_MEMORY {
                temp_segments.push(std::mem::take(&mut postings));
            }
        }
        self.finish_build(postings, temp_segments).await
    }

    /// Rebuild the index by scanning `vertex_dataset` directly (raw Lance). Used
    /// where a `VertexDataset` is available (e.g. fork branch reads); the
    /// primary backfill path is [`Self::build_from_batches`] via the backend.
    pub async fn build_from_dataset(
        &mut self,
        vertex_dataset: &VertexDataset,
        progress: impl Fn(usize),
    ) -> Result<()> {
        let mut postings: Postings = HashMap::new();
        let mut temp_segments: Vec<Postings> = Vec::new();
        let mut count = 0;

        debug!(property = %self.property, "Building sparse index from dataset");
        if let Ok(ds) = vertex_dataset.open().await {
            let scanner = ds.scan();
            let mut stream = scanner.try_into_stream().await?;
            while let Some(batch) = stream.try_next().await? {
                count += self.accumulate_batch(&batch, &mut postings)?;
                progress(count);
                if estimated_postings_memory(&postings) > DEFAULT_MAX_POSTINGS_MEMORY {
                    temp_segments.push(std::mem::take(&mut postings));
                }
            }
        } else {
            debug!("Vertex dataset not found, creating empty sparse index");
        }
        self.finish_build(postings, temp_segments).await
    }

    /// Overwrite the on-disk postings with the provided map.
    ///
    /// Schema: `(term_id: UInt32, vids: List<UInt64>, weights: List<Float32>,
    /// max_impact: Float32)`. `max_impact` is the per-term maximum weight — the
    /// upper bound P2 block-max pruning consumes.
    async fn write_postings(&mut self, postings: Postings) -> Result<()> {
        let mut term_ids = Vec::with_capacity(postings.len());
        let mut vid_lists: Vec<Option<Vec<Option<u64>>>> = Vec::with_capacity(postings.len());
        let mut weight_lists: Vec<Option<Vec<Option<f32>>>> = Vec::with_capacity(postings.len());
        let mut max_impacts = Vec::with_capacity(postings.len());

        for (term, entries) in postings {
            // True maximum weight (the P2 block-max upper bound). Start at
            // NEG_INFINITY so an all-negative-weight term records its real max
            // rather than a spurious 0.0; empty terms (unreachable here) fall
            // back to 0.0.
            let mut max_impact = f32::NEG_INFINITY;
            let mut vids = Vec::with_capacity(entries.len());
            let mut weights = Vec::with_capacity(entries.len());
            for (vid, weight) in entries {
                vids.push(Some(vid));
                weights.push(Some(weight));
                if weight > max_impact {
                    max_impact = weight;
                }
            }
            if !max_impact.is_finite() {
                max_impact = 0.0;
            }
            term_ids.push(term);
            vid_lists.push(Some(vids));
            weight_lists.push(Some(weights));
            max_impacts.push(max_impact);
        }

        let term_array = UInt32Array::from(term_ids);
        let vid_list_array = ListArray::from_iter_primitive::<UInt64Type, _, _>(vid_lists);
        let weight_list_array = ListArray::from_iter_primitive::<Float32Type, _, _>(weight_lists);
        let max_impact_array = Float32Array::from(max_impacts);

        let batch = arrow_array::RecordBatch::try_from_iter(vec![
            ("term_id", Arc::new(term_array) as Arc<dyn Array>),
            ("vids", Arc::new(vid_list_array) as Arc<dyn Array>),
            ("weights", Arc::new(weight_list_array) as Arc<dyn Array>),
            ("max_impact", Arc::new(max_impact_array) as Arc<dyn Array>),
        ])?;

        let path = Self::postings_path(&self.base_uri, &self.label, &self.property);
        let write_params = lance::dataset::WriteParams {
            mode: lance::dataset::WriteMode::Overwrite,
            ..Default::default()
        };
        let iterator = RecordBatchIterator::new(vec![Ok(batch)], Self::postings_schema());
        let ds = Dataset::write(iterator, &path, Some(write_params)).await?;
        self.dataset = Some(ds);
        Ok(())
    }

    /// Arrow schema of the postings dataset.
    fn postings_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("term_id", DataType::UInt32, false),
            Field::new(
                "vids",
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
                false,
            ),
            Field::new(
                "weights",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                false,
            ),
            Field::new("max_impact", DataType::Float32, false),
        ]))
    }

    /// Score the corpus against `query` (`[(term_id, weight)]`) by dot product
    /// and return the top `k` `(Vid, score)` pairs, highest score first.
    ///
    /// P1 brute-force document-at-a-time: filter postings to the query terms,
    /// accumulate per-vid dot products, then drain a bounded min-heap. No
    /// MVCC/tombstone filtering happens here — the orchestration layer applies
    /// it, mirroring the dense `vector_search` path.
    pub async fn query_topk(&self, query: &[(u32, f32)], k: usize) -> Result<Vec<(Vid, f32)>> {
        let Some(ds) = &self.dataset else {
            debug!("Sparse index not initialized, returning empty result");
            return Ok(Vec::new());
        };
        if query.is_empty() || k == 0 {
            return Ok(Vec::new());
        }

        let query_weights: HashMap<u32, f32> = query.iter().copied().collect();
        let term_filter = query_weights
            .keys()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let filter = format!("term_id IN ({term_filter})");

        let mut scanner = ds.scan();
        scanner.filter(&filter)?;
        let mut stream = scanner.try_into_stream().await?;

        let mut scores: HashMap<u64, f32> = HashMap::new();
        while let Some(batch) = stream.try_next().await? {
            let term_col = batch
                .column_by_name("term_id")
                .ok_or_else(|| anyhow!("Missing term_id column"))?
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| anyhow!("Invalid term_id column"))?;
            let vids_col = batch
                .column_by_name("vids")
                .ok_or_else(|| anyhow!("Missing vids column"))?
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid vids column"))?;
            let weights_col = batch
                .column_by_name("weights")
                .ok_or_else(|| anyhow!("Missing weights column"))?
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid weights column"))?;

            for i in 0..batch.num_rows() {
                let term = term_col.value(i);
                let Some(&qw) = query_weights.get(&term) else {
                    continue;
                };
                if vids_col.is_null(i) || weights_col.is_null(i) {
                    continue;
                }
                let vids_arr = vids_col.value(i);
                let vids = vids_arr
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("Invalid inner vids type"))?;
                let weights_arr = weights_col.value(i);
                let weights = weights_arr
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| anyhow!("Invalid inner weights type"))?;

                for j in 0..vids.len() {
                    if vids.is_null(j) || weights.is_null(j) {
                        continue;
                    }
                    *scores.entry(vids.value(j)).or_insert(0.0) += qw * weights.value(j);
                }
            }
        }

        Ok(Self::top_k_from_scores(scores, k))
    }

    /// Drain a score map into the top-`k` `(Vid, score)` pairs (descending),
    /// using a bounded min-heap so memory stays O(k).
    fn top_k_from_scores(scores: HashMap<u64, f32>, k: usize) -> Vec<(Vid, f32)> {
        // Min-heap keyed on score (via OrderedF32 + Reverse) capped at k.
        let mut heap: BinaryHeap<Reverse<HeapEntry>> = BinaryHeap::with_capacity(k + 1);
        for (vid, score) in scores {
            heap.push(Reverse(HeapEntry { score, vid }));
            if heap.len() > k {
                heap.pop();
            }
        }
        let mut out: Vec<(Vid, f32)> = heap
            .into_iter()
            .map(|Reverse(e)| (Vid::from(e.vid), e.score))
            .collect();
        out.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.as_u64().cmp(&b.0.as_u64()))
        });
        out
    }

    /// Load all postings from disk into a memory map. Empty if no dataset yet.
    #[instrument(skip(self), level = "debug")]
    async fn load_postings(&self) -> Result<Postings> {
        let Some(ds) = &self.dataset else {
            return Ok(HashMap::new());
        };
        let mut postings: Postings = HashMap::new();
        let scanner = ds.scan();
        let mut stream = scanner.try_into_stream().await?;
        while let Some(batch) = stream.try_next().await? {
            let term_col = batch
                .column_by_name("term_id")
                .ok_or_else(|| anyhow!("Missing term_id column"))?
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| anyhow!("Invalid term_id column"))?;
            let vids_col = batch
                .column_by_name("vids")
                .ok_or_else(|| anyhow!("Missing vids column"))?
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid vids column"))?;
            let weights_col = batch
                .column_by_name("weights")
                .ok_or_else(|| anyhow!("Missing weights column"))?
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid weights column"))?;

            for i in 0..batch.num_rows() {
                if vids_col.is_null(i) || weights_col.is_null(i) {
                    continue;
                }
                let term = term_col.value(i);
                let vids_arr = vids_col.value(i);
                let vids = vids_arr
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("Invalid inner vids type"))?;
                let weights_arr = weights_col.value(i);
                let weights = weights_arr
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| anyhow!("Invalid inner weights type"))?;
                let entry = postings.entry(term).or_default();
                for j in 0..vids.len() {
                    if !vids.is_null(j) && !weights.is_null(j) {
                        entry.push((vids.value(j), weights.value(j)));
                    }
                }
            }
        }
        Ok(postings)
    }

    /// Apply incremental updates: drop removed VIDs from every posting, then add
    /// the new vertices' `(term, weight)` pairs, and rewrite. Mirrors the
    /// set-membership inverted index's load-modify-write semantics.
    #[instrument(skip(self, added, removed), level = "info", fields(
        label = %self.label,
        property = %self.property,
        added_count = added.len(),
        removed_count = removed.len()
    ))]
    pub async fn apply_incremental_updates(
        &mut self,
        added: &HashMap<Vid, Vec<(u32, f32)>>,
        removed: &HashSet<Vid>,
    ) -> Result<()> {
        let mut postings = self.load_postings().await?;

        if !removed.is_empty() {
            let removed_u64: HashSet<u64> = removed.iter().map(|v| v.as_u64()).collect();
            for entries in postings.values_mut() {
                entries.retain(|(vid, _)| !removed_u64.contains(vid));
            }
            postings.retain(|_, entries| !entries.is_empty());
        }

        for (vid, terms) in added {
            let vid_u64 = vid.as_u64();
            for &(term, weight) in terms {
                postings.entry(term).or_default().push((vid_u64, weight));
            }
        }

        self.write_postings(postings).await?;
        Ok(())
    }

    /// Returns true if the index dataset exists.
    pub fn is_initialized(&self) -> bool {
        self.dataset.is_some()
    }

    /// Returns the property name this index is built on.
    pub fn property(&self) -> &str {
        &self.property
    }
}

/// Heap entry ordered by score (NaN treated as smallest), tie-broken by vid.
struct HeapEntry {
    score: f32,
    vid: u64,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.vid.cmp(&other.vid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_postings_segments_overlapping() {
        let seg1: Postings = [(1u32, vec![(10u64, 1.0f32)]), (2, vec![(11, 2.0)])]
            .into_iter()
            .collect();
        let seg2: Postings = [(1u32, vec![(12u64, 3.0f32)]), (3, vec![(13, 4.0)])]
            .into_iter()
            .collect();
        let merged = merge_postings_segments(vec![seg1, seg2]);
        assert_eq!(merged.get(&1).unwrap().len(), 2);
        assert_eq!(merged.get(&2).unwrap(), &vec![(11, 2.0)]);
        assert_eq!(merged.get(&3).unwrap(), &vec![(13, 4.0)]);
    }

    #[test]
    fn test_top_k_from_scores_orders_desc_and_caps() {
        let scores: HashMap<u64, f32> = [(1u64, 0.5f32), (2, 3.0), (3, 1.0), (4, 2.0)]
            .into_iter()
            .collect();
        let top = SparseVectorIndex::top_k_from_scores(scores, 2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0.as_u64(), 2);
        assert_eq!(top[0].1, 3.0);
        assert_eq!(top[1].0.as_u64(), 4);
        assert_eq!(top[1].1, 2.0);
    }

    #[test]
    fn test_top_k_tie_break_by_vid() {
        let scores: HashMap<u64, f32> = [(7u64, 1.0f32), (3, 1.0)].into_iter().collect();
        let top = SparseVectorIndex::top_k_from_scores(scores, 2);
        // Equal scores → lower vid first (deterministic).
        assert_eq!(top[0].0.as_u64(), 3);
        assert_eq!(top[1].0.as_u64(), 7);
    }

    #[test]
    fn test_top_k_empty() {
        assert!(SparseVectorIndex::top_k_from_scores(HashMap::new(), 5).is_empty());
    }
}
