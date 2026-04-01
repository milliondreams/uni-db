// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::backend::StorageBackend;
use crate::backend::table_names;
use crate::backend::types::{ScalarIndexType, WriteMode};
use crate::storage::arrow_convert::build_timestamp_column_from_vid_map;
use crate::storage::property_builder::PropertyColumnBuilder;
use anyhow::{Result, anyhow};
use arrow_array::builder::{FixedSizeBinaryBuilder, ListBuilder, StringBuilder};
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{Field, Schema as ArrowSchema, TimeUnit};
#[cfg(feature = "lance-backend")]
use lance::dataset::Dataset;
use sha3::{Digest, Sha3_256};
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::Properties;
use uni_common::core::id::{UniId, Vid};
use uni_common::core::schema::Schema;

pub struct VertexDataset {
    #[cfg_attr(not(feature = "lance-backend"), allow(dead_code))]
    uri: String,
    label: String,
    _label_id: u16,
}

impl VertexDataset {
    pub fn new(base_uri: &str, label: &str, label_id: u16) -> Self {
        let uri = format!("{}/vertices_{}", base_uri, label);
        Self {
            uri,
            label: label.to_string(),
            _label_id: label_id,
        }
    }

    /// Compute UniId from vertex content.
    /// Canonical form: sorted JSON of (label, ext_id, properties)
    pub fn compute_vertex_uid(label: &str, ext_id: Option<&str>, properties: &Properties) -> UniId {
        let mut hasher = Sha3_256::new();

        // Include label
        hasher.update(label.as_bytes());
        hasher.update(b"\x00"); // separator

        // Include ext_id if present
        if let Some(eid) = ext_id {
            hasher.update(eid.as_bytes());
        }
        hasher.update(b"\x00");

        // Include sorted properties for determinism
        let mut sorted_props: Vec<_> = properties.iter().collect();
        sorted_props.sort_by_key(|(k, _)| *k);
        for (key, value) in sorted_props {
            hasher.update(key.as_bytes());
            hasher.update(b"=");
            hasher.update(value.to_string().as_bytes());
            hasher.update(b"\x00");
        }

        let hash: [u8; 32] = hasher.finalize().into();
        UniId::from_bytes(hash)
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

    #[cfg(feature = "lance-backend")]
    pub async fn open_raw(&self) -> Result<Dataset> {
        let ds = Dataset::open(&self.uri).await?;
        Ok(ds)
    }

    /// Build a record batch from vertices with optional timestamp metadata.
    ///
    /// If timestamps are not provided, they default to None (null).
    pub fn build_record_batch(
        &self,
        vertices: &[(Vid, Vec<String>, Properties)],
        deleted: &[bool],
        versions: &[u64],
        schema: &Schema,
    ) -> Result<RecordBatch> {
        self.build_record_batch_with_timestamps(vertices, deleted, versions, schema, None, None)
    }

    /// Build a record batch with explicit timestamp metadata.
    ///
    /// # Arguments
    /// * `vertices` - Vertex ID, labels, and properties triples
    /// * `deleted` - Deletion flags per vertex
    /// * `versions` - Version numbers per vertex
    /// * `schema` - Database schema
    /// * `created_at` - Optional map of Vid -> nanoseconds since epoch
    /// * `updated_at` - Optional map of Vid -> nanoseconds since epoch
    pub fn build_record_batch_with_timestamps(
        &self,
        vertices: &[(Vid, Vec<String>, Properties)],
        deleted: &[bool],
        versions: &[u64],
        schema: &Schema,
        created_at: Option<&HashMap<Vid, i64>>,
        updated_at: Option<&HashMap<Vid, i64>>,
    ) -> Result<RecordBatch> {
        let arrow_schema = self.get_arrow_schema(schema)?;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

        let vids: Vec<u64> = vertices.iter().map(|(v, _, _)| v.as_u64()).collect();
        columns.push(Arc::new(UInt64Array::from(vids)));

        let mut uid_builder = FixedSizeBinaryBuilder::new(32);
        for (_vid, _labels, props) in vertices.iter() {
            let ext_id = props.get("ext_id").and_then(|v| v.as_str());
            let uid = Self::compute_vertex_uid(&self.label, ext_id, props);
            uid_builder.append_value(uid.as_bytes())?;
        }
        columns.push(Arc::new(uid_builder.finish()));

        columns.push(Arc::new(BooleanArray::from(deleted.to_vec())));
        columns.push(Arc::new(UInt64Array::from(versions.to_vec())));

        // Build ext_id column (extracted from properties as dedicated column)
        let mut ext_id_builder = StringBuilder::new();
        for (_vid, _labels, props) in vertices.iter() {
            if let Some(ext_id_val) = props.get("ext_id").and_then(|v| v.as_str()) {
                ext_id_builder.append_value(ext_id_val);
            } else {
                ext_id_builder.append_null();
            }
        }
        columns.push(Arc::new(ext_id_builder.finish()));

        // Build _labels column (List<Utf8>)
        let mut labels_builder = ListBuilder::new(StringBuilder::new());
        for (_vid, labels, _props) in vertices.iter() {
            let values = labels_builder.values();
            for lbl in labels {
                values.append_value(lbl);
            }
            labels_builder.append(true);
        }
        columns.push(Arc::new(labels_builder.finish()));

        // Build _created_at and _updated_at columns using shared builder
        let vids = vertices.iter().map(|(v, _, _)| *v);
        columns.push(build_timestamp_column_from_vid_map(
            vids.clone(),
            created_at,
        ));
        columns.push(build_timestamp_column_from_vid_map(vids, updated_at));

        // Build property columns using shared builder
        let prop_columns = PropertyColumnBuilder::new(schema, &self.label, vertices.len())
            .with_deleted(deleted)
            .build(|i| &vertices[i].2)?;

        columns.extend(prop_columns);

        // Build overflow_json column for non-schema properties
        let overflow_column = self.build_overflow_json_column(vertices, schema)?;
        columns.push(overflow_column);

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    /// Build the overflow_json column containing properties not in schema.
    fn build_overflow_json_column(
        &self,
        vertices: &[(Vid, Vec<String>, Properties)],
        schema: &Schema,
    ) -> Result<ArrayRef> {
        crate::storage::property_builder::build_overflow_json_column(
            vertices.len(),
            &self.label,
            schema,
            |i| &vertices[i].2,
            &["ext_id"],
        )
    }

    pub fn get_arrow_schema(&self, schema: &Schema) -> Result<Arc<ArrowSchema>> {
        let mut fields = vec![
            Field::new("_vid", arrow_schema::DataType::UInt64, false),
            Field::new("_uid", arrow_schema::DataType::FixedSizeBinary(32), true),
            Field::new("_deleted", arrow_schema::DataType::Boolean, false),
            Field::new("_version", arrow_schema::DataType::UInt64, false),
            // New metadata columns per STORAGE_DESIGN.md
            Field::new("ext_id", arrow_schema::DataType::Utf8, true),
            Field::new(
                "_labels",
                arrow_schema::DataType::List(Arc::new(Field::new(
                    "item",
                    arrow_schema::DataType::Utf8,
                    true,
                ))),
                true,
            ),
            Field::new(
                "_created_at",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "_updated_at",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ];

        if let Some(label_props) = schema.properties.get(&self.label) {
            let mut sorted_props: Vec<_> = label_props.iter().collect();
            sorted_props.sort_by_key(|(name, _)| *name);

            for (name, meta) in sorted_props {
                fields.push(Field::new(name, meta.r#type.to_arrow(), meta.nullable));
            }
        }

        // Add overflow_json column for non-schema properties (JSONB binary format)
        fields.push(Field::new(
            "overflow_json",
            arrow_schema::DataType::LargeBinary,
            true,
        ));

        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    // ========================================================================
    // Backend-agnostic Methods
    // ========================================================================

    /// Open or create a vertex table via the storage backend.
    pub async fn open_or_create(
        &self,
        backend: &dyn StorageBackend,
        schema: &Schema,
    ) -> Result<()> {
        let table_name = table_names::vertex_table_name(&self.label);
        let arrow_schema = self.get_arrow_schema(schema)?;
        backend
            .open_or_create_table(&table_name, arrow_schema)
            .await
    }

    /// Write a batch to a vertex table.
    ///
    /// Creates the table if it doesn't exist, otherwise appends to it.
    pub async fn write_batch(
        &self,
        backend: &dyn StorageBackend,
        batch: RecordBatch,
        _schema: &Schema,
    ) -> Result<()> {
        let table_name = table_names::vertex_table_name(&self.label);
        if backend.table_exists(&table_name).await? {
            backend
                .write(&table_name, vec![batch], WriteMode::Append)
                .await
        } else {
            backend.create_table(&table_name, vec![batch]).await
        }
    }

    /// Ensure default scalar indexes exist on system columns (_vid, _uid, ext_id).
    pub async fn ensure_default_indexes(&self, backend: &dyn StorageBackend) -> Result<()> {
        let table_name = table_names::vertex_table_name(&self.label);
        let indices = backend.list_indexes(&table_name).await?;

        let has_index = |col: &str| {
            indices
                .iter()
                .any(|idx| idx.columns.contains(&col.to_string()))
        };

        for column in &["_vid", "_uid", "ext_id"] {
            if has_index(column) {
                continue;
            }
            log::info!("Creating {} BTree index for label '{}'", column, self.label);
            if let Err(e) = backend
                .create_scalar_index(&table_name, column, ScalarIndexType::BTree)
                .await
            {
                log::warn!(
                    "Failed to create {} index for '{}': {}",
                    column,
                    self.label,
                    e
                );
            }
        }

        Ok(())
    }

    /// Get the table name for this vertex dataset.
    pub fn table_name(&self) -> String {
        table_names::vertex_table_name(&self.label)
    }

    /// Replace a vertex table's contents atomically.
    ///
    /// Used by compaction to rewrite the table with merged data.
    pub async fn replace(
        &self,
        backend: &dyn StorageBackend,
        batch: RecordBatch,
        schema: &Schema,
    ) -> Result<()> {
        let table_name = self.table_name();
        let arrow_schema = self.get_arrow_schema(schema)?;
        backend
            .replace_table_atomic(&table_name, vec![batch], arrow_schema)
            .await
    }
}
