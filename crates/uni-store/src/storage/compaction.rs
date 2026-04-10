// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::storage::delta::{ENTRY_SIZE_ESTIMATE, L1Entry, Op};
use crate::storage::manager::StorageManager;
use anyhow::{Result, anyhow};
use arrow_array::Array;
use arrow_array::builder::{ArrayBuilder, ListBuilder, UInt64Builder};
use arrow_array::{ListArray, RecordBatch, UInt64Array};
use metrics;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{error, info, instrument};
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::DataType;
use uni_common::{Properties, Value};
use uni_crdt::Crdt;

pub struct Compactor {
    storage: Arc<StorageManager>,
}

impl Compactor {
    pub fn new(storage: Arc<StorageManager>) -> Self {
        Self { storage }
    }

    #[instrument(skip(self), level = "info")]
    pub async fn compact_all(&self) -> Result<Vec<CompactionInfo>> {
        let start = std::time::Instant::now();
        let schema = self.storage.schema_manager().schema();
        let mut compaction_results = Vec::new();

        // Compact Vertices
        for label in schema.labels.keys() {
            info!("Compacting vertices for label {}", label);
            if let Err(e) = self.compact_vertices(label).await {
                error!("Failed to compact vertices for {}: {}", label, e);
            }
        }

        // Compact Edges
        for (edge_type, meta) in &schema.edge_types {
            // Outgoing: src_labels
            for label in &meta.src_labels {
                info!("Compacting adjacency {} -> {} (fwd)", label, edge_type);
                match self.compact_adjacency(edge_type, label, "fwd").await {
                    Ok(info) => compaction_results.push(info),
                    Err(e) => {
                        error!(
                            "Failed to compact adjacency {} -> {}: {}",
                            label, edge_type, e
                        );
                    }
                }
            }

            // Incoming: dst_labels
            for label in &meta.dst_labels {
                info!("Compacting adjacency {} <- {} (bwd)", label, edge_type);
                match self.compact_adjacency(edge_type, label, "bwd").await {
                    Ok(info) => compaction_results.push(info),
                    Err(e) => {
                        error!(
                            "Failed to compact adjacency {} <- {}: {}",
                            label, edge_type, e
                        );
                    }
                }
            }
        }

        metrics::counter!("uni_compaction_runs_total").increment(1);
        metrics::histogram!("uni_compaction_duration_seconds")
            .record(start.elapsed().as_secs_f64());

        Ok(compaction_results)
    }

    #[instrument(skip(self), fields(rows_processed, duration_ms), level = "info")]
    pub async fn compact_vertices(&self, label: &str) -> Result<()> {
        let start = std::time::Instant::now();
        let schema_manager = self.storage.schema_manager();
        let schema = schema_manager.schema();

        let label_props = schema
            .properties
            .get(label)
            .ok_or_else(|| anyhow!("Label not found"))?;

        // Identify CRDT properties
        let crdt_props: HashSet<String> = label_props
            .iter()
            .filter(|(_, meta)| matches!(meta.r#type, DataType::Crdt(_)))
            .map(|(name, _)| name.clone())
            .collect();

        let dataset = self.storage.vertex_dataset(label)?;
        let backend = self.storage.backend();
        let table_name = dataset.table_name();

        // Check if table exists
        if !backend.table_exists(&table_name).await.unwrap_or(false) {
            info!("No vertex data to compact for label '{}'", label);
            return Ok(());
        }

        // In-memory compaction for now (MVP).
        // For large datasets, this needs to be streaming/chunked with external sort.
        // Current approach: Read ALL, merge in map, write NEW.
        // TODO(perf): This accumulates ALL vertices in memory, causing OOM for large
        // labels (millions of vertices). Refactor to use streaming merge-sort with
        // constant memory usage (e.g., external sort or Lance fragment-by-fragment merge).

        let row_count = backend.count_rows(&table_name, None).await?;
        crate::storage::delta::check_oom_guard(
            row_count,
            self.storage.config.max_compaction_rows,
            label,
            "vertices",
        )?;

        info!(
            label = %label,
            row_count,
            estimated_bytes = row_count * 200,
            "Starting vertex compaction"
        );

        use crate::backend::types::ScanRequest;
        let batches: Vec<RecordBatch> = backend.scan(ScanRequest::all(&table_name)).await?;

        // Vid -> (Properties, Deleted)
        let mut vertex_state: HashMap<Vid, (Properties, bool)> = HashMap::new();
        let mut vertex_versions: HashMap<Vid, u64> = HashMap::new();
        let mut vertex_labels: HashMap<Vid, Vec<String>> = HashMap::new();

        let mut rows_processed = 0;

        for batch in batches {
            rows_processed += batch.num_rows();
            let vid_col = batch
                .column_by_name("_vid")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let ver_col = batch
                .column_by_name("_version")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let del_col = batch
                .column_by_name("_deleted")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .unwrap();

            // Read _labels column (List<Utf8>) if present
            let labels_col = batch
                .column_by_name("_labels")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::ListArray>());

            for i in 0..batch.num_rows() {
                let vid = Vid::from(vid_col.value(i));
                let version = ver_col.value(i);
                let deleted = del_col.value(i);

                // Extract labels from the _labels column (keep latest version's labels)
                if let Some(list_arr) = labels_col
                    && version >= *vertex_versions.entry(vid).or_insert(0)
                {
                    let labels = crate::storage::arrow_convert::labels_from_list_array(list_arr, i);
                    if !labels.is_empty() {
                        vertex_labels.insert(vid, labels);
                    }
                }

                let current_entry = vertex_state
                    .entry(vid)
                    .or_insert((Properties::new(), false));
                let current_version = vertex_versions.entry(vid).or_insert(0);

                // If this row is newer than what we've seen (or same), we apply logic.
                // Wait, if we process unordered, we need to be careful.
                // For CRDTs, we MERGE regardless of version (commutative).
                // For LWW, we take MAX version.

                // If it's a deletion, and it's newer, it wins.
                if deleted {
                    if version >= *current_version {
                        current_entry.1 = true;
                        current_entry.0.clear(); // Clear properties on delete
                        *current_version = version;
                    }
                    continue;
                }

                // It's an update/insert
                // Extract props and track NULLs (property removals)
                let mut row_props = Properties::new();
                let mut null_props = Vec::new(); // Track explicitly NULL properties
                for (name, meta) in label_props {
                    if let Some(col) = batch.column_by_name(name) {
                        if col.is_null(i) {
                            // Property was explicitly removed (set to NULL)
                            null_props.push(name.clone());
                        } else {
                            let val = crate::storage::value_codec::decode_column_value(
                                col.as_ref(),
                                &meta.r#type,
                                i,
                                crate::storage::value_codec::CrdtDecodeMode::Strict,
                            )?;
                            row_props.insert(name.clone(), val);
                        }
                    }
                }

                Self::merge_row_into_state(
                    row_props,
                    null_props,
                    version,
                    current_entry,
                    current_version,
                    &crdt_props,
                )?;
            }
        }

        // Convert state to RecordBatch and write OVERWRITE
        let mut valid_vertices = Vec::new();
        let mut valid_versions = Vec::new();
        let mut valid_deleted = Vec::new(); // Should be all false if we filter out tombstones?
        // Or we keep tombstones if they are recent?
        // Compaction usually removes tombstones.

        for (vid, (props, deleted)) in vertex_state {
            if !deleted {
                let labels = vertex_labels.remove(&vid).unwrap_or_default();
                valid_vertices.push((vid, labels, props));
                valid_versions.push(vertex_versions[&vid]);
                valid_deleted.push(false);
            }
        }

        if !valid_vertices.is_empty() {
            let batch = dataset.build_record_batch(
                &valid_vertices,
                &valid_deleted,
                &valid_versions,
                &schema,
            )?;
            dataset
                .replace(self.storage.backend(), batch, &schema)
                .await?;
        }

        let duration = start.elapsed();
        let rows_reclaimed = rows_processed as u64 - valid_vertices.len() as u64;
        metrics::counter!("uni_compaction_rows_reclaimed_total", "type" => "vertex")
            .increment(rows_reclaimed);

        tracing::Span::current().record("rows_processed", rows_processed);
        tracing::Span::current().record("duration_ms", duration.as_millis());
        info!(
            rows = rows_processed,
            duration_ms = duration.as_millis(),
            "Vertex compaction completed"
        );

        metrics::histogram!("uni_compaction_duration_seconds", "type" => "vertex")
            .record(duration.as_secs_f64());

        Ok(())
    }

    fn merge_crdt_values(a: &Value, b: &Value) -> Result<Value> {
        if a.is_null() {
            return Ok(b.clone());
        }
        if b.is_null() {
            return Ok(a.clone());
        }
        let mut crdt_a: Crdt = serde_json::from_value(a.clone().into())?;
        let crdt_b: Crdt = serde_json::from_value(b.clone().into())?;
        crdt_a
            .try_merge(&crdt_b)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(Value::from(serde_json::to_value(crdt_a)?))
    }

    /// Merge row properties into state based on version comparison.
    fn merge_row_into_state(
        row_props: Properties,
        null_props: Vec<String>,
        version: u64,
        current_entry: &mut (Properties, bool),
        current_version: &mut u64,
        crdt_props: &HashSet<String>,
    ) -> Result<()> {
        if version > *current_version {
            // New version wins for LWW, merge for CRDTs
            *current_version = version;
            current_entry.1 = false;

            for (k, v) in row_props {
                if crdt_props.contains(&k) {
                    let existing = current_entry.0.entry(k.clone()).or_insert(Value::Null);
                    *existing = Self::merge_crdt_values(existing, &v)?;
                } else {
                    current_entry.0.insert(k, v);
                }
            }

            // Remove properties explicitly set to NULL in the newer version
            for null_prop in &null_props {
                if !crdt_props.contains(null_prop) {
                    current_entry.0.remove(null_prop);
                }
            }
        } else if version == *current_version {
            // Same version: merge all
            current_entry.1 = false;
            for (k, v) in row_props {
                if crdt_props.contains(&k) {
                    let existing = current_entry.0.entry(k.clone()).or_insert(Value::Null);
                    *existing = Self::merge_crdt_values(existing, &v)?;
                } else {
                    current_entry.0.insert(k, v);
                }
            }
        } else {
            // Older version: only merge CRDTs
            if !current_entry.1 {
                for (k, v) in row_props {
                    if crdt_props.contains(&k) {
                        let existing = current_entry.0.entry(k.clone()).or_insert(Value::Null);
                        *existing = Self::merge_crdt_values(existing, &v)?;
                    }
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self), fields(delta_count, duration_ms), level = "info")]
    pub async fn compact_adjacency(
        &self,
        edge_type: &str,
        label: &str,
        direction: &str,
    ) -> Result<CompactionInfo> {
        let start = std::time::Instant::now();
        let schema = self.storage.schema_manager().schema();

        // 1. Load all L1 Deltas sorted by key
        let delta_ds = self.storage.delta_dataset(edge_type, direction)?;
        let deltas = delta_ds
            .scan_all_backend(self.storage.backend(), &schema)
            .await?;

        let delta_count = deltas.len();
        tracing::Span::current().record("delta_count", delta_count);

        if deltas.is_empty() {
            // Nothing to compact, return info anyway
            return Ok(CompactionInfo {
                edge_type: edge_type.to_string(),
                direction: direction.to_string(),
            });
        }

        // Group deltas by src_vid (if fwd) or dst_vid (if bwd)
        // We'll use a HashMap for now since we loaded all into memory.
        // Value is list of ops for that vertex.
        let mut delta_map: HashMap<Vid, Vec<L1Entry>> = HashMap::new();
        for entry in &deltas {
            let key = if direction == "fwd" {
                entry.src_vid
            } else {
                entry.dst_vid
            };
            delta_map.entry(key).or_default().push(entry.clone());
        }

        // Sort each VID's ops by version to ensure correct ordering
        // This guarantees Delete(v=2) beats Insert(v=1) regardless of scan order
        for ops in delta_map.values_mut() {
            ops.sort_by_key(|e| e.version);
        }

        // 2. Open L2 Adjacency stream
        let adj_ds = self
            .storage
            .adjacency_dataset(edge_type, label, direction)?;

        // We need to write a NEW version.
        // Strategy:
        // - Read L2 batch by batch.
        // - For each row (vertex), check if we have deltas.
        // - Apply deltas.
        // - Write to new batch.
        // - Track which vertices from deltas we've processed.
        // - After L2 stream ends, process remaining "new" vertices from deltas.

        // Output Builders
        let mut src_vid_builder = UInt64Builder::new();
        let mut neighbors_builder = ListBuilder::new(UInt64Builder::new());
        let mut edge_ids_builder = ListBuilder::new(UInt64Builder::new());

        let mut processed_vids = HashSet::new();

        // Try to read from backend (canonical storage)
        let backend = self.storage.backend();
        let adj_table_name = adj_ds.table_name();
        if backend.table_exists(&adj_table_name).await.unwrap_or(false) {
            let adj_row_count = backend.count_rows(&adj_table_name, None).await?;
            crate::storage::delta::check_oom_guard(
                adj_row_count,
                self.storage.config.max_compaction_rows,
                &format!("{}_{}", edge_type, label),
                direction,
            )?;

            info!(
                edge_type = %edge_type,
                label = %label,
                direction = %direction,
                adj_row_count,
                delta_count,
                estimated_bytes = adj_row_count * 100 + delta_count * ENTRY_SIZE_ESTIMATE,
                "Starting adjacency compaction"
            );

            use crate::backend::types::ScanRequest;
            let batches: Vec<RecordBatch> = backend.scan(ScanRequest::all(&adj_table_name)).await?;

            for batch in batches {
                let src_col = batch
                    .column_by_name("src_vid")
                    .ok_or(anyhow!("Missing src_vid"))?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or(anyhow!("Invalid src_vid"))?;
                let neighbors_col = batch
                    .column_by_name("neighbors")
                    .ok_or(anyhow!("Missing neighbors"))?
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or(anyhow!("Invalid neighbors"))?;
                let edge_ids_col = batch
                    .column_by_name("edge_ids")
                    .ok_or(anyhow!("Missing edge_ids"))?
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or(anyhow!("Invalid edge_ids"))?;

                for i in 0..batch.num_rows() {
                    let vid = Vid::from(src_col.value(i));
                    processed_vids.insert(vid);

                    // Reconstruct current adjacency list
                    let n_list = neighbors_col.value(i);
                    let n_array = n_list.as_any().downcast_ref::<UInt64Array>().unwrap();
                    let e_list = edge_ids_col.value(i);
                    let e_array = e_list.as_any().downcast_ref::<UInt64Array>().unwrap();

                    let mut current_edges: HashMap<Eid, Vid> = HashMap::new();
                    for j in 0..n_array.len() {
                        current_edges
                            .insert(Eid::from(e_array.value(j)), Vid::from(n_array.value(j)));
                    }

                    if let Some(ops) = delta_map.get(&vid) {
                        apply_deltas_to_edges(&mut current_edges, ops, direction);
                    }

                    append_edges_to_builders(
                        vid,
                        &current_edges,
                        &mut src_vid_builder,
                        &mut neighbors_builder,
                        &mut edge_ids_builder,
                    );
                }
            }
        }

        // Process new vertices (in deltas but not in L2)
        for (vid, ops) in delta_map {
            if processed_vids.contains(&vid) {
                continue;
            }

            let mut current_edges: HashMap<Eid, Vid> = HashMap::new();
            apply_deltas_to_edges(&mut current_edges, &ops, direction);

            append_edges_to_builders(
                vid,
                &current_edges,
                &mut src_vid_builder,
                &mut neighbors_builder,
                &mut edge_ids_builder,
            );
        }

        // Final Flush
        if src_vid_builder.len() > 0 {
            let src_arr = Arc::new(src_vid_builder.finish());
            let neighbors_arr = Arc::new(neighbors_builder.finish());
            let edge_ids_arr = Arc::new(edge_ids_builder.finish());

            let schema = adj_ds.get_arrow_schema();
            let batch = RecordBatch::try_new(schema, vec![src_arr, neighbors_arr, edge_ids_arr])?;

            // Replace the table with compacted data
            adj_ds.replace(self.storage.backend(), batch).await?;
        }

        // CRITICAL: Clear Delta L1 after compaction
        // Topology ops from Delta L1 are now incorporated into L2 adjacency.
        // Edge properties survive in main_edges (dual-written during flush).
        // Clearing Delta L1 prevents stale topology data from being read.
        if !deltas.is_empty() {
            info!(
                "Clearing Delta L1 for edge_type={} direction={} after compaction (incorporated {} ops)",
                edge_type,
                direction,
                deltas.len()
            );

            // Invariant: Every EID in Delta L1 must have a corresponding entry in
            // main_edges, because Writer::flush_to_l1 performs a dual-write.
            // Tests that create delta entries directly (for schema/overflow testing)
            // must not call compact_adjacency without also populating main_edges.
            #[cfg(debug_assertions)]
            {
                use crate::storage::main_edge::MainEdgeDataset;

                let delta_eids: std::collections::HashSet<Eid> =
                    deltas.iter().map(|e| e.eid).collect();

                for eid in delta_eids {
                    let main_edge_exists =
                        MainEdgeDataset::exists_by_eid(self.storage.backend(), eid)
                            .await
                            .unwrap_or(false);

                    debug_assert!(
                        main_edge_exists,
                        "EID {} from Delta L1 not found in main_edges after compaction. \
                        This indicates edge properties were not dual-written during flush.",
                        eid.as_u64()
                    );
                }
            }

            // Clear the Delta L1 table by replacing with empty batch
            let delta_ds = self.storage.delta_dataset(edge_type, direction)?;
            let delta_schema = delta_ds.get_arrow_schema(&schema)?;
            let empty_batch = RecordBatch::new_empty(delta_schema);
            delta_ds
                .replace(self.storage.backend(), empty_batch)
                .await?;
        }

        let duration = start.elapsed();
        tracing::Span::current().record("duration_ms", duration.as_millis());
        info!(
            delta_count,
            duration_ms = duration.as_millis(),
            "Adjacency compaction completed"
        );

        metrics::histogram!("uni_compaction_duration_seconds", "type" => "adjacency")
            .record(duration.as_secs_f64());

        Ok(CompactionInfo {
            edge_type: edge_type.to_string(),
            direction: direction.to_string(),
        })
    }
}

/// Apply delta operations to an edge map, returning the resolved neighbor for the direction.
fn apply_deltas_to_edges(current_edges: &mut HashMap<Eid, Vid>, ops: &[L1Entry], direction: &str) {
    for op in ops {
        match op.op {
            Op::Insert => {
                let neighbor = if direction == "fwd" {
                    op.dst_vid
                } else {
                    op.src_vid
                };
                current_edges.insert(op.eid, neighbor);
            }
            Op::Delete => {
                current_edges.remove(&op.eid);
            }
        }
    }
}

/// Write sorted edges from a HashMap into adjacency list builders.
fn append_edges_to_builders(
    vid: Vid,
    current_edges: &HashMap<Eid, Vid>,
    src_vid_builder: &mut UInt64Builder,
    neighbors_builder: &mut ListBuilder<UInt64Builder>,
    edge_ids_builder: &mut ListBuilder<UInt64Builder>,
) {
    if current_edges.is_empty() {
        return;
    }
    src_vid_builder.append_value(vid.as_u64());

    let mut sorted_eids: Vec<_> = current_edges.keys().cloned().collect();
    sorted_eids.sort();

    for eid in sorted_eids {
        let neighbor = current_edges[&eid];
        neighbors_builder.values().append_value(neighbor.as_u64());
        edge_ids_builder.values().append_value(eid.as_u64());
    }
    neighbors_builder.append(true);
    edge_ids_builder.append(true);
}

/// Information returned by adjacency compaction about what was compacted.
/// Used to coordinate in-memory CSR re-warm after storage compaction.
#[derive(Debug, Clone)]
pub struct CompactionInfo {
    pub edge_type: String,
    pub direction: String,
}
