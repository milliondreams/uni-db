// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use arrow_schema::{Field, Schema as ArrowSchema};
use std::sync::Arc;
use uni_common::core::schema::Schema;

pub struct EdgeDataset {
    edge_type: String,
    /// Lance branch for branched reads. `None` = primary.
    branch: Option<String>,
}

impl EdgeDataset {
    /// `_base_uri` is ignored: physical path resolution belongs to the storage
    /// backend, and this type now holds only the logical identity of the table.
    pub fn new(_base_uri: &str, edge_type: &str, _src_label: &str, _dst_label: &str) -> Self {
        Self {
            edge_type: edge_type.to_string(),
            branch: None,
        }
    }

    /// Construct an edge dataset that reads from a Lance branch.
    pub fn new_branched(
        base_uri: &str,
        edge_type: &str,
        src_label: &str,
        dst_label: &str,
        branch: impl Into<String>,
    ) -> Self {
        let mut ds = Self::new(base_uri, edge_type, src_label, dst_label);
        ds.branch = Some(branch.into());
        ds
    }

    pub fn get_arrow_schema(&self, schema: &Schema) -> Result<Arc<ArrowSchema>> {
        let mut fields = vec![
            Field::new("eid", arrow_schema::DataType::UInt64, false),
            Field::new("src_vid", arrow_schema::DataType::UInt64, false),
            Field::new("dst_vid", arrow_schema::DataType::UInt64, false),
            Field::new("_deleted", arrow_schema::DataType::Boolean, false),
            Field::new("_version", arrow_schema::DataType::UInt64, false),
        ];

        if let Some(type_props) = schema.properties.get(&self.edge_type) {
            let mut sorted_props: Vec<_> = type_props.iter().collect();
            sorted_props.sort_by_key(|(name, _)| *name);

            for (name, meta) in sorted_props {
                fields.push(Field::new(name, meta.r#type.to_arrow(), meta.nullable));
            }
        }

        Ok(Arc::new(ArrowSchema::new(fields)))
    }
}
