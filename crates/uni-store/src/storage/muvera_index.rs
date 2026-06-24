// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Shared helpers for MUVERA (Fixed-Dimensional-Encoding) multi-vector indexes.
//!
//! A MUVERA index (`VectorIndexType::Muvera`) encodes a source multi-vector column
//! into a single derived `Vector<fde_dim>` column (`__fde_<index_name>`), over which a
//! normal single-vector ANN index is built. This module centralises:
//! - the derived column naming convention,
//! - reconstruction of [`FdeParams`] from a persisted index config + the source dim,
//! - extraction of a multi-vector [`Value`] into `Vec<Vec<f32>>` for the encoder.
//!
//! It is reachable from both the flush path (uni-store) and the query layer (uni-query),
//! so doc-time and query-time encoders are built from the same parameters. See
//! [`uni_common::muvera`] for the encoder itself.

use std::sync::Arc;

use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, new_null_array};
use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema};
use uni_common::Value;
use uni_common::core::schema::{
    DataType, IndexDefinition, Schema, VectorIndexConfig, VectorIndexType,
};
use uni_common::muvera::{FdeEncoder, FdeParams};

use crate::storage::arrow_convert::arrow_to_value;

/// Name of the internal derived FDE column for a MUVERA index. Tied to the index name
/// (not the property) so two MUVERA indexes on the same property don't collide and
/// dropping the index can drop its column. The `__` prefix hides it from user output.
pub fn fde_derived_column(index_name: &str) -> String {
    format!("__fde_{index_name}")
}

/// The embedding dimension of a vector or multi-vector property type, recursing
/// `List(Vector{dim})` (ColBERT multi-vector) to the inner `Vector{dim}`.
pub fn resolve_vector_dim(t: &DataType) -> Option<usize> {
    match t {
        DataType::Vector { dimensions } => Some(*dimensions),
        DataType::List(inner) => resolve_vector_dim(inner),
        _ => None,
    }
}

/// Everything needed to materialise / query one MUVERA index's FDE column.
#[derive(Debug, Clone)]
pub struct FdeSpec {
    /// The MUVERA index name.
    pub index_name: String,
    /// Label the index is on.
    pub label: String,
    /// Source multi-vector property name.
    pub source_prop: String,
    /// Derived FDE column name (`__fde_<index_name>`).
    pub derived_col: String,
    /// FDE transform parameters (with `input_dim` resolved from the source column).
    pub params: FdeParams,
}

/// Build an [`FdeSpec`] from a vector-index config if it is a MUVERA index whose source
/// property resolves to a (multi-)vector dimension; otherwise `None`.
pub fn fde_spec_for_config(schema: &Schema, cfg: &VectorIndexConfig) -> Option<FdeSpec> {
    let VectorIndexType::Muvera {
        k_sim,
        reps,
        d_proj,
        seed,
        ..
    } = &cfg.index_type
    else {
        return None;
    };
    let input_dim = schema
        .properties
        .get(&cfg.label)
        .and_then(|p| p.get(&cfg.property))
        .and_then(|m| resolve_vector_dim(&m.r#type))?;
    let params = FdeParams {
        k_sim: *k_sim,
        reps: *reps,
        d_proj: *d_proj,
        input_dim: input_dim as u32,
        seed: *seed,
    };
    Some(FdeSpec {
        index_name: cfg.name.clone(),
        label: cfg.label.clone(),
        source_prop: cfg.property.clone(),
        derived_col: fde_derived_column(&cfg.name),
        params,
    })
}

/// All MUVERA index specs in the schema.
pub fn fde_specs(schema: &Schema) -> Vec<FdeSpec> {
    schema
        .indexes
        .iter()
        .filter_map(|idx| match idx {
            IndexDefinition::Vector(cfg) => fde_spec_for_config(schema, cfg),
            _ => None,
        })
        .collect()
}

/// MUVERA index specs for a single label.
pub fn fde_specs_for_label(schema: &Schema, label: &str) -> Vec<FdeSpec> {
    fde_specs(schema)
        .into_iter()
        .filter(|s| s.label == label)
        .collect()
}

/// Extract a stored multi-vector [`Value`] into `Vec<Vec<f32>>` (one inner vector per
/// token). Accepts `Value::List` of `Value::Vector` or `Value::List`-of-numbers tokens;
/// returns an empty vec for anything else (e.g. a missing/NULL property). Token-dimension
/// validation is the encoder's job (it errors on a mismatch).
pub fn value_to_multivec(v: &Value) -> Vec<Vec<f32>> {
    let Value::List(tokens) = v else {
        return Vec::new();
    };
    tokens.iter().filter_map(token_to_f32).collect()
}

fn token_to_f32(v: &Value) -> Option<Vec<f32>> {
    match v {
        Value::Vector(f) => Some(f.clone()),
        Value::List(nums) => {
            let out: Vec<f32> = nums
                .iter()
                .filter_map(|n| n.as_f64().map(|x| x as f32))
                .collect();
            (out.len() == nums.len()).then_some(out)
        }
        _ => None,
    }
}

/// Rebuild a scanned vertex `RecordBatch` in `target_schema` order with the derived FDE
/// column spliced in. Every original column is copied verbatim by name (preserving
/// `overflow_json` and all other data); the `__fde_*` column is computed per row from the
/// source multi-vector via `encoder`; any `target_schema` field absent from the source
/// batch (schema evolution) is null-filled. Used by `StorageManager::prepare_muvera_index`
/// to backfill already-flushed rows at CREATE INDEX time.
pub fn splice_fde_batch(
    batch: &RecordBatch,
    target_schema: &Arc<ArrowSchema>,
    spec: &FdeSpec,
    encoder: &FdeEncoder,
    source_dt: Option<&DataType>,
) -> anyhow::Result<RecordBatch> {
    let nrows = batch.num_rows();
    let fde_dim = spec.params.fde_dim();

    // Compute the flattened FDE values (nrows * fde_dim, row-major) from the source.
    let mut flat: Vec<f32> = Vec::with_capacity(nrows * fde_dim);
    if let Some(src) = batch.column_by_name(&spec.source_prop) {
        for row in 0..nrows {
            let val = arrow_to_value(src.as_ref(), row, source_dt);
            let tokens = value_to_multivec(&val);
            let fde = encoder.encode_doc(&tokens)?;
            debug_assert_eq!(fde.len(), fde_dim);
            flat.extend_from_slice(&fde);
        }
    } else {
        // Source column absent on these rows → all-zero FDEs (rank last; harmless).
        flat.resize(nrows * fde_dim, 0.0);
    }

    // Build the FDE FixedSizeList<Float32, fde_dim> array using the EXACT child field from
    // the target schema, so the column type matches what future flush appends produce.
    let fde_field = target_schema
        .field_with_name(&spec.derived_col)
        .map_err(|e| anyhow::anyhow!("MUVERA derived field '{}' missing: {e}", spec.derived_col))?;
    let child_field = match fde_field.data_type() {
        ArrowDataType::FixedSizeList(child, size) => {
            anyhow::ensure!(
                *size as usize == fde_dim,
                "MUVERA fde_dim mismatch: schema {} vs params {fde_dim}",
                size
            );
            child.clone()
        }
        other => anyhow::bail!("MUVERA derived field is not FixedSizeList: {other:?}"),
    };
    let values = Arc::new(Float32Array::from(flat));
    let fde_array: ArrayRef = Arc::new(FixedSizeListArray::new(
        child_field,
        fde_dim as i32,
        values,
        None,
    ));

    // Assemble columns in target-schema order.
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        if field.name() == &spec.derived_col {
            columns.push(fde_array.clone());
        } else if let Some(col) = batch.column_by_name(field.name()) {
            columns.push(col.clone());
        } else {
            columns.push(new_null_array(field.data_type(), nrows));
        }
    }
    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| anyhow::anyhow!(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_column_name() {
        assert_eq!(
            fde_derived_column("doc_tokens_muvera"),
            "__fde_doc_tokens_muvera"
        );
        assert!(fde_derived_column("x").starts_with("__"));
    }

    #[test]
    fn resolve_dim_recurses_list() {
        assert_eq!(
            resolve_vector_dim(&DataType::Vector { dimensions: 96 }),
            Some(96)
        );
        assert_eq!(
            resolve_vector_dim(&DataType::List(Box::new(DataType::Vector {
                dimensions: 8
            }))),
            Some(8)
        );
        assert_eq!(resolve_vector_dim(&DataType::Int), None);
    }

    #[test]
    fn multivec_extraction() {
        let v = Value::List(vec![
            Value::Vector(vec![1.0, 0.0]),
            Value::List(vec![Value::Float(0.0), Value::Float(1.0)]),
        ]);
        let tokens = value_to_multivec(&v);
        assert_eq!(tokens, vec![vec![1.0, 0.0], vec![0.0, 1.0]]);
        // Non-list → empty (missing/NULL property).
        assert!(value_to_multivec(&Value::Null).is_empty());
    }
}
