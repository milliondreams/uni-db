// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Main vertex table for unified vertex storage.
//!
//! This module implements the main `vertices` table as described in STORAGE_DESIGN.md.
//! The main table contains all vertices in the graph with:
//! - `_vid`: Internal vertex ID (primary key)
//! - `_uid`: Content-addressed unique ID (SHA3-256 hash)
//! - `ext_id`: Optional external/user-provided ID (globally unique)
//! - `labels`: List of label names (OpenCypher multi-label)
//! - `props_json`: All properties as JSONB blob
//! - `_deleted`: Soft-delete flag
//! - `_version`: MVCC version
//! - `_created_at`: Creation timestamp
//! - `_updated_at`: Update timestamp

use crate::backend::StorageBackend;
use crate::backend::table_names;
use crate::backend::types::{ScalarIndexType, ScanRequest, WriteMode};
use crate::storage::arrow_convert::build_timestamp_column_from_vid_map;
use anyhow::{Result, anyhow};
use arrow_array::builder::{
    FixedSizeBinaryBuilder, LargeBinaryBuilder, ListBuilder, StringBuilder,
};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use sha3::{Digest, Sha3_256};
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::Properties;
use uni_common::core::id::{UniId, Vid};

/// Main vertex dataset for the unified `vertices` table.
///
/// This table contains all vertices regardless of label, providing:
/// - Fast ID-based lookups without knowing the label
/// - Global ext_id uniqueness enforcement
/// - Multi-label storage with labels as a list column
#[derive(Debug)]
pub struct MainVertexDataset {
    _base_uri: String,
}

impl MainVertexDataset {
    /// Create a new MainVertexDataset.
    pub fn new(base_uri: &str) -> Self {
        Self {
            _base_uri: base_uri.to_string(),
        }
    }

    /// Get the Arrow schema for the main vertices table.
    pub fn get_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("_uid", DataType::FixedSizeBinary(32), true),
            Field::new("ext_id", DataType::Utf8, true),
            Field::new(
                "labels",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new("props_json", DataType::LargeBinary, true),
            Field::new("_deleted", DataType::Boolean, false),
            Field::new("_version", DataType::UInt64, false),
            Field::new(
                "_created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "_updated_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ]))
    }

    /// Get the table name for the main vertices table.
    pub fn table_name() -> &'static str {
        table_names::main_vertex_table_name()
    }

    /// Compute the UniId (content-addressed hash) for a vertex.
    fn compute_vertex_uid(labels: &[String], ext_id: Option<&str>, props: &Properties) -> UniId {
        let mut hasher = Sha3_256::new();

        // Hash labels (sorted for consistency)
        let mut sorted_labels = labels.to_vec();
        sorted_labels.sort();
        for label in &sorted_labels {
            hasher.update(label.as_bytes());
            hasher.update(b"\0");
        }

        // Hash ext_id if present
        if let Some(ext_id) = ext_id {
            hasher.update(b"ext_id:");
            hasher.update(ext_id.as_bytes());
            hasher.update(b"\0");
        }

        // Hash properties (sorted by key for deterministic hashing)
        let mut sorted_keys: Vec<_> = props.keys().collect();
        sorted_keys.sort();
        for key in sorted_keys {
            if key == "ext_id" {
                continue; // Already handled above
            }
            if let Some(val) = props.get(key) {
                hasher.update(key.as_bytes());
                hasher.update(b":");
                hasher.update(val.to_string().as_bytes());
                hasher.update(b"\0");
            }
        }

        let result = hasher.finalize();
        UniId::from_bytes(result.into())
    }

    /// Build a record batch for the main vertices table.
    ///
    /// # Arguments
    /// * `vertices` - List of (vid, labels, properties, deleted, version) tuples
    /// * `created_at` - Optional map of Vid -> nanoseconds since epoch
    /// * `updated_at` - Optional map of Vid -> nanoseconds since epoch
    pub fn build_record_batch(
        vertices: &[(Vid, Vec<String>, Properties, bool, u64)],
        created_at: Option<&HashMap<Vid, i64>>,
        updated_at: Option<&HashMap<Vid, i64>>,
    ) -> Result<RecordBatch> {
        let arrow_schema = Self::get_arrow_schema();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

        // _vid column
        let vids: Vec<u64> = vertices.iter().map(|(v, _, _, _, _)| v.as_u64()).collect();
        columns.push(Arc::new(UInt64Array::from(vids)));

        // _uid column
        let mut uid_builder = FixedSizeBinaryBuilder::new(32);
        for (_, labels, props, _, _) in vertices.iter() {
            let ext_id = props.get("ext_id").and_then(|v| v.as_str());
            let uid = Self::compute_vertex_uid(labels, ext_id, props);
            uid_builder.append_value(uid.as_bytes())?;
        }
        columns.push(Arc::new(uid_builder.finish()));

        // ext_id column
        let mut ext_id_builder = StringBuilder::new();
        for (_, _, props, _, _) in vertices.iter() {
            if let Some(ext_id_val) = props.get("ext_id").and_then(|v| v.as_str()) {
                ext_id_builder.append_value(ext_id_val);
            } else {
                ext_id_builder.append_null();
            }
        }
        columns.push(Arc::new(ext_id_builder.finish()));

        // labels column (List<String>)
        let mut labels_builder = ListBuilder::new(StringBuilder::new());
        for (_, labels, _, _, _) in vertices.iter() {
            let values_builder = labels_builder.values();
            for label in labels {
                values_builder.append_value(label);
            }
            labels_builder.append(true);
        }
        columns.push(Arc::new(labels_builder.finish()));

        // props_json column (JSONB binary encoding)
        let mut props_json_builder = LargeBinaryBuilder::new();
        for (_, _, props, _, _) in vertices.iter() {
            let jsonb_bytes = {
                let json_val = serde_json::to_value(props).unwrap_or(serde_json::json!({}));
                let uni_val: uni_common::Value = json_val.into();
                uni_common::cypher_value_codec::encode(&uni_val)
            };
            props_json_builder.append_value(&jsonb_bytes);
        }
        columns.push(Arc::new(props_json_builder.finish()));

        // _deleted column
        let deleted: Vec<bool> = vertices.iter().map(|(_, _, _, d, _)| *d).collect();
        columns.push(Arc::new(BooleanArray::from(deleted)));

        // _version column
        let versions: Vec<u64> = vertices.iter().map(|(_, _, _, _, v)| *v).collect();
        columns.push(Arc::new(UInt64Array::from(versions)));

        // _created_at and _updated_at columns using shared builder
        let vids = vertices.iter().map(|(v, _, _, _, _)| *v);
        columns.push(build_timestamp_column_from_vid_map(
            vids.clone(),
            created_at,
        ));
        columns.push(build_timestamp_column_from_vid_map(vids, updated_at));

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    /// Write a batch to the main vertices table.
    ///
    /// Creates the table if it doesn't exist, otherwise appends to it.
    pub async fn write_batch(backend: &dyn StorageBackend, batch: RecordBatch) -> Result<()> {
        let table_name = table_names::main_vertex_table_name();

        if backend.table_exists(table_name).await? {
            backend
                .write(table_name, vec![batch], WriteMode::Append)
                .await
        } else {
            backend.create_table(table_name, vec![batch]).await
        }
    }

    /// Ensure default indexes exist on the main vertices table.
    pub async fn ensure_default_indexes(backend: &dyn StorageBackend) -> Result<()> {
        let table_name = table_names::main_vertex_table_name();

        // BTree indexes for primary key and lookup columns
        let _ = backend
            .create_scalar_index(table_name, "_vid", ScalarIndexType::BTree)
            .await;
        let _ = backend
            .create_scalar_index(table_name, "ext_id", ScalarIndexType::BTree)
            .await;
        let _ = backend
            .create_scalar_index(table_name, "_uid", ScalarIndexType::BTree)
            .await;

        // LabelList index for array_contains() queries on labels
        let _ = backend
            .create_scalar_index(table_name, "labels", ScalarIndexType::LabelList)
            .await;

        Ok(())
    }

    /// Query the main vertices table for a vertex by ext_id.
    ///
    /// Returns the Vid if found, None otherwise.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    ///   Pass `None` for writer uniqueness checks (global visibility).
    ///   Pass `Some(hwm)` for query-time snapshot isolation.
    pub async fn find_by_ext_id(
        backend: &dyn StorageBackend,
        ext_id: &str,
        version: Option<u64>,
    ) -> Result<Option<Vid>> {
        let table_name = table_names::main_vertex_table_name();

        if !backend.table_exists(table_name).await? {
            return Ok(None);
        }

        let mut filter = format!(
            "ext_id = '{}' AND _deleted = false",
            ext_id.replace('\'', "''")
        );
        if let Some(hwm) = version {
            filter.push_str(&format!(" AND _version <= {}", hwm));
        }

        let results = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec!["_vid".to_string()]),
            )
            .await?;

        for batch in results {
            if batch.num_rows() > 0
                && let Some(vid_col) = batch.column_by_name("_vid")
                && let Some(vid_arr) = vid_col.as_any().downcast_ref::<UInt64Array>()
            {
                return Ok(Some(Vid::from(vid_arr.value(0))));
            }
        }

        Ok(None)
    }

    /// Check if an ext_id already exists in the main vertices table.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    pub async fn ext_id_exists(
        backend: &dyn StorageBackend,
        ext_id: &str,
        version: Option<u64>,
    ) -> Result<bool> {
        Ok(Self::find_by_ext_id(backend, ext_id, version)
            .await?
            .is_some())
    }

    /// Find labels for a vertex by VID in the main vertices table.
    ///
    /// Returns the list of labels if found, None otherwise.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    pub async fn find_labels_by_vid(
        backend: &dyn StorageBackend,
        vid: Vid,
        version: Option<u64>,
    ) -> Result<Option<Vec<String>>> {
        let table_name = table_names::main_vertex_table_name();

        if !backend.table_exists(table_name).await? {
            return Ok(None);
        }

        let mut filter = format!("_vid = {} AND _deleted = false", vid.as_u64());
        if let Some(hwm) = version {
            filter.push_str(&format!(" AND _version <= {}", hwm));
        }

        let results = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec!["labels".to_string()]),
            )
            .await?;

        for batch in results {
            if batch.num_rows() > 0
                && let Some(labels_col) = batch.column_by_name("labels")
                && let Some(list_arr) = labels_col.as_any().downcast_ref::<arrow_array::ListArray>()
            {
                // Labels is a List<Utf8> column
                let values = list_arr.value(0);
                if let Some(str_arr) = values.as_any().downcast_ref::<arrow_array::StringArray>() {
                    let labels: Vec<String> = (0..str_arr.len())
                        .filter_map(|i| {
                            if str_arr.is_null(i) {
                                None
                            } else {
                                Some(str_arr.value(i).to_string())
                            }
                        })
                        .collect();
                    return Ok(Some(labels));
                }
            }
        }

        Ok(None)
    }

    /// Find all non-deleted VIDs in the main vertices table.
    ///
    /// Returns all VIDs where `_deleted = false`.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    ///
    /// # Errors
    ///
    /// Returns an error if the table query fails.
    pub async fn find_all_vids(
        backend: &dyn StorageBackend,
        version: Option<u64>,
    ) -> Result<Vec<Vid>> {
        let table_name = table_names::main_vertex_table_name();

        if !backend.table_exists(table_name).await? {
            return Ok(Vec::new());
        }

        let mut filter = "_deleted = false".to_string();
        if let Some(hwm) = version {
            filter.push_str(&format!(" AND _version <= {}", hwm));
        }

        let results = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec!["_vid".to_string()]),
            )
            .await?;

        let mut vids = Vec::new();
        for batch in results {
            if let Some(vid_col) = batch.column_by_name("_vid")
                && let Some(vid_arr) = vid_col.as_any().downcast_ref::<UInt64Array>()
            {
                for i in 0..vid_arr.len() {
                    if !vid_arr.is_null(i) {
                        vids.push(Vid::new(vid_arr.value(i)));
                    }
                }
            }
        }

        Ok(vids)
    }

    /// Find VIDs by label name in the main vertices table.
    ///
    /// Searches for vertices where the labels array contains the given label
    /// and `_deleted = false`.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    ///
    /// # Errors
    ///
    /// Returns an error if the table query fails.
    pub async fn find_vids_by_label_name(
        backend: &dyn StorageBackend,
        label: &str,
        version: Option<u64>,
    ) -> Result<Vec<Vid>> {
        let table_name = table_names::main_vertex_table_name();

        if !backend.table_exists(table_name).await? {
            return Ok(Vec::new());
        }

        // Use SQL array_contains to filter by label
        let mut filter = format!("_deleted = false AND array_contains(labels, '{}')", label);
        if let Some(hwm) = version {
            filter.push_str(&format!(" AND _version <= {}", hwm));
        }

        let results = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec!["_vid".to_string()]),
            )
            .await?;

        let mut vids = Vec::new();
        for batch in results {
            if let Some(vid_col) = batch.column_by_name("_vid")
                && let Some(vid_arr) = vid_col.as_any().downcast_ref::<UInt64Array>()
            {
                for i in 0..vid_arr.len() {
                    if !vid_arr.is_null(i) {
                        vids.push(Vid::new(vid_arr.value(i)));
                    }
                }
            }
        }

        Ok(vids)
    }

    /// Find VIDs by multiple label names (intersection semantics).
    ///
    /// Returns vertices that have ALL the specified labels.
    /// Uses `array_contains(labels, 'A') AND array_contains(labels, 'B')` filtering.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    pub async fn find_vids_by_labels(
        backend: &dyn StorageBackend,
        labels: &[&str],
        version: Option<u64>,
    ) -> Result<Vec<Vid>> {
        let table_name = table_names::main_vertex_table_name();

        if labels.is_empty() || !backend.table_exists(table_name).await? {
            return Ok(Vec::new());
        }

        // Build AND conditions for each label
        let label_conditions: Vec<String> = labels
            .iter()
            .map(|label| {
                let escaped = label.replace('\'', "''");
                format!("array_contains(labels, '{}')", escaped)
            })
            .collect();

        let mut filter = format!("_deleted = false AND {}", label_conditions.join(" AND "));
        if let Some(hwm) = version {
            filter.push_str(&format!(" AND _version <= {}", hwm));
        }

        let results = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec!["_vid".to_string()]),
            )
            .await?;

        let mut vids = Vec::new();
        for batch in results {
            if let Some(vid_col) = batch.column_by_name("_vid")
                && let Some(vid_arr) = vid_col.as_any().downcast_ref::<UInt64Array>()
            {
                for i in 0..vid_arr.len() {
                    if !vid_arr.is_null(i) {
                        vids.push(Vid::new(vid_arr.value(i)));
                    }
                }
            }
        }

        Ok(vids)
    }

    /// Batch-fetch properties for multiple VIDs from the main vertices table.
    ///
    /// Returns a HashMap mapping VIDs to their parsed properties.
    /// Non-deleted vertices are returned with properties from props_json.
    /// This is used for schemaless vertex scans via DataFusion.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    ///
    /// # Errors
    ///
    /// Returns an error if the table query fails or JSON parsing fails.
    pub async fn find_batch_props_by_vids(
        backend: &dyn StorageBackend,
        vids: &[Vid],
        version: Option<u64>,
    ) -> Result<HashMap<Vid, Properties>> {
        let table_name = table_names::main_vertex_table_name();

        if vids.is_empty() || !backend.table_exists(table_name).await? {
            return Ok(HashMap::new());
        }

        // Build IN clause for VIDs
        let vid_list: Vec<String> = vids.iter().map(|v| v.as_u64().to_string()).collect();
        let mut filter = format!("_vid IN ({}) AND _deleted = false", vid_list.join(", "));
        if let Some(hwm) = version {
            filter.push_str(&format!(" AND _version <= {}", hwm));
        }

        let results = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec!["_vid".to_string(), "props_json".to_string()]),
            )
            .await?;

        let mut props_map = HashMap::new();

        for batch in results {
            let vid_col = batch.column_by_name("_vid");
            let props_col = batch.column_by_name("props_json");

            if let (Some(vid_arr), Some(props_arr)) = (
                vid_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
                props_col.and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>()),
            ) {
                for i in 0..batch.num_rows() {
                    if vid_arr.is_null(i) {
                        continue;
                    }
                    let vid = Vid::new(vid_arr.value(i));

                    let props: Properties = if props_arr.is_null(i) || props_arr.value(i).is_empty()
                    {
                        Properties::new()
                    } else {
                        let bytes = props_arr.value(i);
                        let uni_val = uni_common::cypher_value_codec::decode(bytes)
                            .map_err(|e| anyhow!("Failed to decode CypherValue: {}", e))?;
                        let json_val: serde_json::Value = uni_val.into();
                        serde_json::from_value(json_val)
                            .map_err(|e| anyhow!("Failed to parse props_json: {}", e))?
                    };

                    props_map.insert(vid, props);
                }
            }
        }

        Ok(props_map)
    }

    /// Find properties for a vertex by VID in the main vertices table.
    ///
    /// Returns the props_json parsed into a Properties HashMap if found.
    /// This is used as a fallback for unknown/schemaless labels.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    ///
    /// # Errors
    ///
    /// Returns an error if the table query fails or JSON parsing fails.
    pub async fn find_props_by_vid(
        backend: &dyn StorageBackend,
        vid: Vid,
        version: Option<u64>,
    ) -> Result<Option<Properties>> {
        let table_name = table_names::main_vertex_table_name();

        if !backend.table_exists(table_name).await? {
            return Ok(None);
        }

        let mut filter = format!("_vid = {} AND _deleted = false", vid.as_u64());
        if let Some(hwm) = version {
            filter.push_str(&format!(" AND _version <= {}", hwm));
        }

        let results = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec!["props_json".to_string(), "_version".to_string()]),
            )
            .await?;

        // Find the row with highest version (latest)
        let mut best_props: Option<Properties> = None;
        let mut best_version: u64 = 0;

        for batch in results {
            let props_col = batch.column_by_name("props_json");
            let version_col = batch.column_by_name("_version");

            if let (Some(props_arr), Some(ver_arr)) = (
                props_col.and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>()),
                version_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
            ) {
                for i in 0..batch.num_rows() {
                    let version = if ver_arr.is_null(i) {
                        0
                    } else {
                        ver_arr.value(i)
                    };

                    if version >= best_version {
                        best_version = version;
                        if props_arr.is_null(i) || props_arr.value(i).is_empty() {
                            best_props = Some(Properties::new());
                        } else {
                            let bytes = props_arr.value(i);
                            let uni_val = uni_common::cypher_value_codec::decode(bytes)
                                .map_err(|e| anyhow!("Failed to decode CypherValue: {}", e))?;
                            let json_val: serde_json::Value = uni_val.into();
                            let parsed: Properties = serde_json::from_value(json_val)
                                .map_err(|e| anyhow!("Failed to parse props_json: {}", e))?;
                            best_props = Some(parsed);
                        }
                    }
                }
            }
        }

        Ok(best_props)
    }

    /// Batch-fetch labels for multiple VIDs from the main vertices table.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    pub async fn find_batch_labels_by_vids(
        backend: &dyn StorageBackend,
        vids: &[Vid],
        version: Option<u64>,
    ) -> Result<HashMap<Vid, Vec<String>>> {
        let table_name = table_names::main_vertex_table_name();

        if vids.is_empty() || !backend.table_exists(table_name).await? {
            return Ok(HashMap::new());
        }

        // Build IN clause for VIDs
        let vid_list: Vec<String> = vids.iter().map(|v| v.as_u64().to_string()).collect();
        let mut filter = format!("_vid IN ({}) AND _deleted = false", vid_list.join(", "));
        if let Some(hwm) = version {
            filter.push_str(&format!(" AND _version <= {}", hwm));
        }

        let results = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec!["_vid".to_string(), "labels".to_string()]),
            )
            .await?;

        let mut label_map = HashMap::new();

        for batch in results {
            let vid_col = batch.column_by_name("_vid");
            let labels_col = batch.column_by_name("labels");

            if let (Some(vid_arr), Some(labels_arr)) = (
                vid_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
                labels_col.and_then(|c| c.as_any().downcast_ref::<arrow_array::ListArray>()),
            ) {
                for i in 0..batch.num_rows() {
                    if vid_arr.is_null(i) {
                        continue;
                    }
                    let vid = Vid::new(vid_arr.value(i));

                    let values = labels_arr.value(i);
                    if let Some(str_arr) =
                        values.as_any().downcast_ref::<arrow_array::StringArray>()
                    {
                        let labels: Vec<String> = (0..str_arr.len())
                            .filter_map(|j| {
                                if str_arr.is_null(j) {
                                    None
                                } else {
                                    Some(str_arr.value(j).to_string())
                                }
                            })
                            .collect();
                        label_map.insert(vid, labels);
                    }
                }
            }
        }

        Ok(label_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;

    #[test]
    fn test_main_vertex_schema() {
        let schema = MainVertexDataset::get_arrow_schema();
        assert_eq!(schema.fields().len(), 9);
        assert!(schema.field_with_name("_vid").is_ok());
        assert!(schema.field_with_name("_uid").is_ok());
        assert!(schema.field_with_name("ext_id").is_ok());
        assert!(schema.field_with_name("labels").is_ok());
        assert!(schema.field_with_name("props_json").is_ok());
        assert!(schema.field_with_name("_deleted").is_ok());
        assert!(schema.field_with_name("_version").is_ok());
        assert!(schema.field_with_name("_created_at").is_ok());
        assert!(schema.field_with_name("_updated_at").is_ok());
    }

    #[test]
    fn test_build_record_batch() {
        use uni_common::Value;
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("ext_id".to_string(), Value::String("user_001".to_string()));

        let vertices = vec![(Vid::new(1), vec!["Person".to_string()], props, false, 1u64)];

        let batch = MainVertexDataset::build_record_batch(&vertices, None, None).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 9);

        // Check ext_id was extracted
        let ext_id_col = batch.column_by_name("ext_id").unwrap();
        let ext_id_arr = ext_id_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(ext_id_arr.value(0), "user_001");
    }
}
