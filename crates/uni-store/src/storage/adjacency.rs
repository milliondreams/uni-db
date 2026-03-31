// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::backend::StorageBackend;
use crate::backend::table_names;
use crate::backend::types::{ScanRequest, WriteMode};
use anyhow::{Result, anyhow};
use arrow_array::{ListArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
#[cfg(feature = "lance-backend")]
use futures::TryStreamExt;
#[cfg(feature = "lance-backend")]
use lance::dataset::Dataset;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::core::id::{Eid, Vid};

/// Type alias for adjacency list data (neighbors, edge_ids).
type AdjacencyLists = (Vec<Vid>, Vec<Eid>);

/// Type alias for grouped adjacency data by source vertex.
type GroupedAdjacencyLists = HashMap<Vid, (Vec<Vid>, Vec<Eid>)>;

/// Downcast the neighbors and edge_ids list columns from a RecordBatch.
fn downcast_adjacency_lists(batch: &RecordBatch) -> Result<(&ListArray, &ListArray)> {
    let neighbors_list = batch
        .column_by_name("neighbors")
        .ok_or(anyhow!("Missing neighbors"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or(anyhow!("Invalid neighbors type"))?;

    let edge_ids_list = batch
        .column_by_name("edge_ids")
        .ok_or(anyhow!("Missing edge_ids"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or(anyhow!("Invalid edge_ids type"))?;

    Ok((neighbors_list, edge_ids_list))
}

/// Extract (neighbors, edge_ids) from a single row of the adjacency list columns.
fn extract_row_adjacency(
    neighbors_list: &ListArray,
    edge_ids_list: &ListArray,
    row_idx: usize,
) -> Result<(Vec<Vid>, Vec<Eid>)> {
    let neighbors_array = neighbors_list.value(row_idx);
    let neighbors_uint64 = neighbors_array
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or(anyhow!("Invalid neighbors inner type"))?;

    let edge_ids_array = edge_ids_list.value(row_idx);
    let edge_ids_uint64 = edge_ids_array
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or(anyhow!("Invalid edge_ids inner type"))?;

    let neighbors = (0..neighbors_uint64.len())
        .map(|i| Vid::from(neighbors_uint64.value(i)))
        .collect();
    let eids = (0..edge_ids_uint64.len())
        .map(|i| Eid::from(edge_ids_uint64.value(i)))
        .collect();

    Ok((neighbors, eids))
}

/// Extract adjacency data (neighbors, edge IDs) from a single row of a RecordBatch.
///
/// Returns `None` if the batch is empty or columns are missing.
fn extract_adjacency_from_batch(batch: &RecordBatch) -> Result<Option<AdjacencyLists>> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }

    let (neighbors_list, edge_ids_list) = downcast_adjacency_lists(batch)?;

    let mut all_neighbors = Vec::new();
    let mut all_eids = Vec::new();

    for row_idx in 0..batch.num_rows() {
        let (neighbors, eids) = extract_row_adjacency(neighbors_list, edge_ids_list, row_idx)?;
        all_neighbors.extend(neighbors);
        all_eids.extend(eids);
    }

    Ok(Some((all_neighbors, all_eids)))
}

/// Extract adjacency data from a batch, grouped by src_vid.
///
/// Returns a HashMap mapping each src_vid to its (neighbors, edge_ids).
fn extract_adjacency_from_batch_grouped(batch: &RecordBatch) -> Result<GroupedAdjacencyLists> {
    if batch.num_rows() == 0 {
        return Ok(HashMap::new());
    }

    let src_vid_col = batch
        .column_by_name("src_vid")
        .ok_or(anyhow!("Missing src_vid"))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or(anyhow!("Invalid src_vid type"))?;

    let (neighbors_list, edge_ids_list) = downcast_adjacency_lists(batch)?;

    let mut result: HashMap<Vid, (Vec<Vid>, Vec<Eid>)> = HashMap::new();

    for row_idx in 0..batch.num_rows() {
        let src_vid = Vid::from(src_vid_col.value(row_idx));
        let (neighbors, eids) = extract_row_adjacency(neighbors_list, edge_ids_list, row_idx)?;
        result.insert(src_vid, (neighbors, eids));
    }

    Ok(result)
}

pub struct AdjacencyDataset {
    #[cfg_attr(not(feature = "lance-backend"), allow(dead_code))]
    uri: String,
    edge_type: String,
    direction: String,
}

impl AdjacencyDataset {
    pub fn new(base_uri: &str, edge_type: &str, label: &str, direction: &str) -> Self {
        let uri = format!(
            "{}/adjacency/{}_{}_{}",
            base_uri, direction, edge_type, label
        );
        Self {
            uri,
            edge_type: edge_type.to_string(),
            direction: direction.to_string(),
        }
    }

    #[cfg(feature = "lance-backend")]
    pub async fn open(&self) -> Result<Arc<Dataset>> {
        self.open_at(None).await
    }

    #[cfg(feature = "lance-backend")]
    pub async fn open_at(&self, version: Option<u64>) -> Result<Arc<Dataset>> {
        let mut ds = Dataset::open(&self.uri).await?;
        if let Some(v) = version {
            ds = ds.checkout_version(v).await?;
        }
        Ok(Arc::new(ds))
    }

    pub fn get_arrow_schema(&self) -> Arc<ArrowSchema> {
        let fields = vec![
            Field::new("src_vid", ArrowDataType::UInt64, false),
            // neighbors: list<uint64>
            Field::new(
                "neighbors",
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::UInt64, true))),
                false,
            ),
            // edge_ids: list<uint64>
            Field::new(
                "edge_ids",
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::UInt64, true))),
                false,
            ),
        ];

        Arc::new(ArrowSchema::new(fields))
    }

    #[cfg(feature = "lance-backend")]
    pub async fn read_adjacency(&self, vid: Vid) -> Result<Option<(Vec<Vid>, Vec<Eid>)>> {
        self.read_adjacency_at(vid, None).await
    }

    #[cfg(feature = "lance-backend")]
    pub async fn read_adjacency_at(
        &self,
        vid: Vid,
        version: Option<u64>,
    ) -> Result<Option<(Vec<Vid>, Vec<Eid>)>> {
        let ds = match self.open_at(version).await {
            Ok(ds) => ds,
            Err(_) => return Ok(None),
        };

        let mut stream = ds
            .scan()
            .filter(&format!("src_vid = {}", vid.as_u64()))?
            .try_into_stream()
            .await?;

        if let Some(batch) = stream.try_next().await? {
            return extract_adjacency_from_batch(&batch);
        }

        Ok(None)
    }

    // ========================================================================
    // Backend-agnostic Methods
    // ========================================================================

    /// Read adjacency data for a vertex from the storage backend.
    ///
    /// Returns `None` if the table doesn't exist or no data for the vertex.
    pub async fn read_adjacency_backend(
        &self,
        backend: &dyn StorageBackend,
        vid: Vid,
    ) -> Result<Option<(Vec<Vid>, Vec<Eid>)>> {
        let table_name = table_names::adjacency_table_name(&self.edge_type, &self.direction);

        if !backend.table_exists(&table_name).await? {
            return Ok(None);
        }

        let filter = format!("src_vid = {}", vid.as_u64());
        let batches = backend
            .scan(ScanRequest::all(&table_name).with_filter(filter))
            .await?;

        for batch in batches {
            if let Some(result) = extract_adjacency_from_batch(&batch)? {
                return Ok(Some(result));
            }
        }

        Ok(None)
    }

    /// Read adjacency data for multiple vertices in a single batch query.
    ///
    /// Returns a HashMap mapping each vid to its (neighbors, edge_ids).
    /// VIDs with no adjacency data will not be in the map.
    pub async fn read_adjacency_backend_batch(
        &self,
        backend: &dyn StorageBackend,
        vids: &[Vid],
    ) -> Result<HashMap<Vid, (Vec<Vid>, Vec<Eid>)>> {
        if vids.is_empty() {
            return Ok(HashMap::new());
        }

        let table_name = table_names::adjacency_table_name(&self.edge_type, &self.direction);

        if !backend.table_exists(&table_name).await? {
            return Ok(HashMap::new());
        }

        // Build IN filter for batch query
        let vid_list = vids
            .iter()
            .map(|v| v.as_u64().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let filter = format!("src_vid IN ({})", vid_list);
        let batches = backend
            .scan(ScanRequest::all(&table_name).with_filter(filter))
            .await?;

        let mut result = HashMap::new();
        for batch in batches {
            let batch_result = extract_adjacency_from_batch_grouped(&batch)?;
            result.extend(batch_result);
        }

        Ok(result)
    }

    /// Open or create an adjacency table via the storage backend.
    pub async fn open_or_create(&self, backend: &dyn StorageBackend) -> Result<()> {
        let table_name = table_names::adjacency_table_name(&self.edge_type, &self.direction);
        let arrow_schema = self.get_arrow_schema();
        backend
            .open_or_create_table(&table_name, arrow_schema)
            .await
    }

    /// Write a chunk to an adjacency table.
    ///
    /// Creates the table if it doesn't exist, otherwise appends to it.
    pub async fn write_chunk(
        &self,
        backend: &dyn StorageBackend,
        batch: RecordBatch,
    ) -> Result<()> {
        let table_name = table_names::adjacency_table_name(&self.edge_type, &self.direction);
        if backend.table_exists(&table_name).await? {
            backend
                .write(&table_name, vec![batch], WriteMode::Append)
                .await
        } else {
            backend.create_table(&table_name, vec![batch]).await
        }
    }

    /// Get the table name for this adjacency dataset.
    pub fn table_name(&self) -> String {
        table_names::adjacency_table_name(&self.edge_type, &self.direction)
    }

    /// Replace an adjacency table's contents atomically.
    ///
    /// Used by compaction to rewrite the table with merged data.
    pub async fn replace(&self, backend: &dyn StorageBackend, batch: RecordBatch) -> Result<()> {
        let table_name = self.table_name();
        let arrow_schema = self.get_arrow_schema();
        backend
            .replace_table_atomic(&table_name, vec![batch], arrow_schema)
            .await
    }
}
