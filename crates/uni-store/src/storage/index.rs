// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use arrow_array::builder::{FixedSizeBinaryBuilder, StringBuilder};
use arrow_array::{Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance_index::DatasetIndexExt;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::core::id::{UniId, Vid};

/// Convert a UniId to hex string for filter pushdown.
fn uid_to_hex(uid: &UniId) -> String {
    uid.as_bytes()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

pub struct UidIndex {
    uri: String,
}

impl UidIndex {
    pub fn new(base_uri: &str, label: &str) -> Self {
        let uri = format!("{}/indexes/uni_id_to_vid/{}/index.lance", base_uri, label);
        Self { uri }
    }

    pub async fn open(&self) -> Result<Arc<Dataset>> {
        let ds = Dataset::open(&self.uri).await?;
        Ok(Arc::new(ds))
    }

    pub fn get_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("_uid", ArrowDataType::FixedSizeBinary(32), false),
            Field::new("_vid", ArrowDataType::UInt64, false),
            Field::new("_uid_hex", ArrowDataType::Utf8, false), // hex-encoded _uid for filtering
        ]))
    }

    pub async fn write_mapping(&self, mappings: &[(UniId, Vid)]) -> Result<()> {
        let schema = Self::get_arrow_schema();

        let mut uid_builder = FixedSizeBinaryBuilder::new(32);
        let mut vids = Vec::with_capacity(mappings.len());
        let mut uid_hex_builder = StringBuilder::new();

        for (uid, vid) in mappings {
            uid_builder.append_value(uid.as_bytes()).unwrap();
            vids.push(vid.as_u64());
            uid_hex_builder.append_value(uid_to_hex(uid));
        }

        let uid_array = uid_builder.finish();
        let vid_array = UInt64Array::from(vids);
        let uid_hex_array = uid_hex_builder.finish();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(uid_array),
                Arc::new(vid_array),
                Arc::new(uid_hex_array),
            ],
        )?;

        let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), schema);

        let params = WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        };

        Dataset::write(Box::new(reader), &self.uri, Some(params)).await?;
        self.ensure_uid_hex_index().await?;
        Ok(())
    }

    /// Create a BTree scalar index on _uid_hex for O(log N) lookups.
    /// Non-fatal: if index creation fails, filter pushdown still works without the index.
    pub async fn ensure_uid_hex_index(&self) -> Result<()> {
        let mut ds = match Dataset::open(&self.uri).await {
            Ok(ds) => ds,
            Err(_) => return Ok(()), // Index doesn't exist yet, skip
        };

        // Create BTree index on _uid_hex column for faster lookups
        ds.create_index(
            &["_uid_hex"],
            lance_index::IndexType::Scalar,
            Some("idx_uid_hex".to_string()),
            &lance_index::scalar::ScalarIndexParams::default(),
            true, // replace if exists
        )
        .await
        .ok(); // Non-fatal: filter pushdown works without index

        Ok(())
    }

    pub async fn get_vid(&self, uid: &UniId) -> Result<Option<Vid>> {
        let ds = match self.open().await {
            Ok(ds) => ds,
            Err(_) => return Ok(None),
        };

        // Use filter pushdown on _uid_hex for O(log N) or better lookup
        let hex = uid_to_hex(uid);
        let filter = format!("_uid_hex = '{}'", hex);

        let mut stream = ds
            .scan()
            .filter(&filter)?
            .project(&["_vid"])?
            .limit(Some(1), None)? // Only need first match
            .try_into_stream()
            .await?;

        if let Some(batch) = stream.try_next().await?
            && batch.num_rows() > 0
        {
            let vid_col = batch
                .column_by_name("_vid")
                .ok_or(anyhow!("Missing _vid column"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or(anyhow!("Invalid _vid column type"))?;

            return Ok(Some(Vid::from(vid_col.value(0))));
        }

        Ok(None)
    }

    pub async fn resolve_uids(&self, uids: &[UniId]) -> Result<HashMap<UniId, Vid>> {
        if uids.is_empty() {
            return Ok(HashMap::new());
        }

        let ds = match self.open().await {
            Ok(ds) => ds,
            Err(_) => return Ok(HashMap::new()),
        };

        // Batch scan using IN filter on _uid_hex
        let hex_values: Vec<String> = uids.iter().map(uid_to_hex).collect();
        let filter = format!(
            "_uid_hex IN ({})",
            hex_values
                .iter()
                .map(|h| format!("'{}'", h))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let mut stream = ds
            .scan()
            .filter(&filter)?
            .project(&["_uid_hex", "_vid"])?
            .try_into_stream()
            .await?;

        // Build reverse map: hex -> UniId for fast lookup
        let hex_to_uid: HashMap<String, UniId> =
            uids.iter().map(|uid| (uid_to_hex(uid), *uid)).collect();

        let mut result = HashMap::new();

        while let Some(batch) = stream.try_next().await? {
            let uid_hex_col = batch
                .column_by_name("_uid_hex")
                .ok_or(anyhow!("Missing _uid_hex column"))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(anyhow!("Invalid _uid_hex column type"))?;

            let vid_col = batch
                .column_by_name("_vid")
                .ok_or(anyhow!("Missing _vid column"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or(anyhow!("Invalid _vid column type"))?;

            for i in 0..batch.num_rows() {
                if !uid_hex_col.is_null(i) {
                    let hex = uid_hex_col.value(i);
                    if let Some(&uid) = hex_to_uid.get(hex) {
                        result.insert(uid, Vid::from(vid_col.value(i)));
                    }
                }
            }
        }

        Ok(result)
    }
}
