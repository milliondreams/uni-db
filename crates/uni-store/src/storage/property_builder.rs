// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Helper for building Arrow property columns from row-based data.

use crate::storage::arrow_convert::PropertyExtractor;
use anyhow::{Result, anyhow};
use arrow_array::ArrayRef;
use arrow_array::builder::LargeBinaryBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::{Properties, Schema, Value};

/// Builds property columns for a specific label/edge_type using the Schema.
pub struct PropertyColumnBuilder<'a> {
    schema: &'a Schema,
    label: &'a str,
    len: usize,
    deleted: Option<&'a [bool]>,
}

impl<'a> PropertyColumnBuilder<'a> {
    pub fn new(schema: &'a Schema, label: &'a str, len: usize) -> Self {
        Self {
            schema,
            label,
            len,
            deleted: None,
        }
    }

    pub fn with_deleted(mut self, deleted: &'a [bool]) -> Self {
        self.deleted = Some(deleted);
        self
    }

    pub fn build<F>(self, get_row_props: F) -> Result<Vec<ArrayRef>>
    where
        F: Fn(usize) -> &'a Properties,
    {
        let mut columns = Vec::new();

        if let Some(props) = self.schema.properties.get(self.label) {
            let mut sorted_props: Vec<_> = props.iter().collect();
            sorted_props.sort_by_key(|(name, _)| *name);

            let default_deleted = vec![false; self.len];
            let deleted = self.deleted.unwrap_or(&default_deleted);

            for (name, meta) in sorted_props {
                let extractor = PropertyExtractor::new(name, &meta.r#type);
                let column =
                    extractor.build_column(self.len, deleted, |i| get_row_props(i).get(name))?;
                columns.push(column);
            }
        }

        Ok(columns)
    }
}

/// Builds an `overflow_json` column (LargeBinary) for properties not defined in the schema.
///
/// Properties present in the schema are stored as typed columns; remaining properties
/// are serialized into a JSONB binary blob per row. Rows with no overflow properties
/// produce a null entry.
///
/// # Arguments
/// * `len` - Number of rows
/// * `label_or_type` - Label (for vertices) or edge type name used to look up schema properties
/// * `schema` - The database schema
/// * `get_row_props` - Closure that returns the full property map for a given row index
/// * `skip_keys` - Additional property keys to exclude (e.g., `"ext_id"` for vertices)
pub fn build_overflow_json_column<'a, F>(
    len: usize,
    label_or_type: &str,
    schema: &Schema,
    get_row_props: F,
    skip_keys: &[&str],
) -> Result<ArrayRef>
where
    F: Fn(usize) -> &'a Properties,
{
    let schema_props = schema.properties.get(label_or_type);
    let mut builder = LargeBinaryBuilder::new();

    for i in 0..len {
        let props = get_row_props(i);
        let mut overflow_props = HashMap::new();

        for (key, value) in props.iter() {
            if skip_keys.contains(&key.as_str()) {
                continue;
            }
            if !schema_props.is_some_and(|sp| sp.contains_key(key)) {
                overflow_props.insert(key.clone(), value.clone());
            }
        }

        if overflow_props.is_empty() {
            builder.append_null();
        } else {
            let json_val = serde_json::to_value(&overflow_props)
                .map_err(|e| anyhow!("Failed to serialize overflow properties: {}", e))?;
            let uni_val: Value = json_val.into();
            let jsonb = uni_common::cypher_value_codec::encode(&uni_val);
            builder.append_value(&jsonb);
        }
    }

    Ok(Arc::new(builder.finish()))
}
