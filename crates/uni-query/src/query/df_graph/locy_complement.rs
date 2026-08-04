// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! IS-NOT (negation) semantics over Arrow batches.
//!
//! Two complementary strategies for a negated body condition, plus the
//! post-processing step that folds their output back into the rule's PROB
//! column:
//!
//! * [`apply_prob_complement_composite`] — probabilistic complement, used when
//!   the stratum carries probabilities.
//! * [`apply_anti_join_composite`] — boolean anti-join, used otherwise.
//! * [`multiply_prob_factors`] — multiplies the `__prob_complement_*` factors
//!   into the PROB column and strips the internal columns.
//!
//! [`super::locy_delta`] carries the row-based (`Vec<FactRow>`) counterparts
//! used by the SLG / EXPLAIN dispatch path.

use crate::query::df_graph::common::arrow_err;
use arrow_array::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Probabilistic complement for composite (multi-column) join keys.
///
/// Builds a composite key from all `join_cols` right-side columns in
/// `neg_facts`, maps each composite key to a probability via noisy-OR
/// combination, then adds a single `complement_col_name` column with
/// `1 - p` for matched keys and `1.0` for absent keys.
pub fn apply_prob_complement_composite(
    batches: Vec<RecordBatch>,
    neg_facts: &[RecordBatch],
    join_cols: &[(String, String)],
    prob_col: &str,
    complement_col_name: &str,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    use arrow_array::{Array as _, Float64Array, UInt64Array};

    // Build composite-key → probability lookup from negative facts.
    let mut prob_map: HashMap<Vec<u64>, f64> = HashMap::new();
    for batch in neg_facts {
        let right_indices: Vec<usize> = join_cols
            .iter()
            .filter_map(|(_, rc)| batch.schema().index_of(rc).ok())
            .collect();
        if right_indices.len() != join_cols.len() {
            continue;
        }
        let Ok(prob_idx) = batch.schema().index_of(prob_col) else {
            continue;
        };
        let prob_arr = batch.column(prob_idx);
        let probs = prob_arr.as_any().downcast_ref::<Float64Array>();
        for row in 0..batch.num_rows() {
            let mut key = Vec::with_capacity(right_indices.len());
            let mut valid = true;
            for &ci in &right_indices {
                let col = batch.column(ci);
                if let Some(vids) = col.as_any().downcast_ref::<UInt64Array>() {
                    if vids.is_null(row) {
                        valid = false;
                        break;
                    }
                    key.push(vids.value(row));
                } else {
                    valid = false;
                    break;
                }
            }
            if !valid {
                continue;
            }
            let p = probs
                .and_then(|arr| {
                    if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row))
                    }
                })
                .unwrap_or(0.0);
            // Noisy-OR combination for duplicate composite keys.
            prob_map
                .entry(key)
                .and_modify(|existing| {
                    *existing = 1.0 - (1.0 - *existing) * (1.0 - p);
                })
                .or_insert(p);
        }
    }

    // Add complement column to each batch.
    let mut result = Vec::new();
    for batch in batches {
        let left_indices: Vec<usize> = join_cols
            .iter()
            .filter_map(|(lc, _)| batch.schema().index_of(lc).ok())
            .collect();
        if left_indices.len() != join_cols.len() {
            result.push(batch);
            continue;
        }
        let all_u64 = left_indices.iter().all(|&ci| {
            batch
                .column(ci)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .is_some()
        });
        if !all_u64 {
            result.push(batch);
            continue;
        }

        let complements: Vec<f64> = (0..batch.num_rows())
            .map(|row| {
                let mut key = Vec::with_capacity(left_indices.len());
                for &ci in &left_indices {
                    let vids = batch
                        .column(ci)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    if vids.is_null(row) {
                        return 1.0;
                    }
                    key.push(vids.value(row));
                }
                let p = prob_map.get(&key).copied().unwrap_or(0.0);
                1.0 - p
            })
            .collect();

        let complement_arr = Float64Array::from(complements);
        let mut columns: Vec<arrow_array::ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(complement_arr));

        let mut fields: Vec<Arc<arrow_schema::Field>> =
            batch.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(arrow_schema::Field::new(
            complement_col_name,
            arrow_schema::DataType::Float64,
            true,
        )));

        let new_schema = Arc::new(arrow_schema::Schema::new(fields));
        let new_batch = RecordBatch::try_new(new_schema, columns).map_err(arrow_err)?;
        result.push(new_batch);
    }
    Ok(result)
}

/// Boolean anti-join for composite (multi-column) join keys.
///
/// Builds a `HashSet<Vec<u64>>` from `neg_facts` using all right-side
/// columns in `join_cols`, then filters `batches` to keep only rows
/// whose composite left-side key is NOT in the set.
pub fn apply_anti_join_composite(
    batches: Vec<RecordBatch>,
    neg_facts: &[RecordBatch],
    join_cols: &[(String, String)],
) -> datafusion::error::Result<Vec<RecordBatch>> {
    use arrow::compute::filter_record_batch;
    use arrow_array::{Array as _, BooleanArray, UInt64Array};

    // Collect composite keys from the negated rule's derived facts.
    let mut banned: HashSet<Vec<u64>> = HashSet::new();
    for batch in neg_facts {
        let right_indices: Vec<usize> = join_cols
            .iter()
            .filter_map(|(_, rc)| batch.schema().index_of(rc).ok())
            .collect();
        if right_indices.len() != join_cols.len() {
            continue;
        }
        for row in 0..batch.num_rows() {
            let mut key = Vec::with_capacity(right_indices.len());
            let mut valid = true;
            for &ci in &right_indices {
                let col = batch.column(ci);
                if let Some(vids) = col.as_any().downcast_ref::<UInt64Array>() {
                    if vids.is_null(row) {
                        valid = false;
                        break;
                    }
                    key.push(vids.value(row));
                } else {
                    valid = false;
                    break;
                }
            }
            if valid {
                banned.insert(key);
            }
        }
    }

    if banned.is_empty() {
        return Ok(batches);
    }

    // Filter body batches: keep rows where composite left key NOT IN banned.
    let mut result = Vec::new();
    for batch in batches {
        let left_indices: Vec<usize> = join_cols
            .iter()
            .filter_map(|(lc, _)| batch.schema().index_of(lc).ok())
            .collect();
        if left_indices.len() != join_cols.len() {
            result.push(batch);
            continue;
        }
        let all_u64 = left_indices.iter().all(|&ci| {
            batch
                .column(ci)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .is_some()
        });
        if !all_u64 {
            result.push(batch);
            continue;
        }

        let keep: Vec<bool> = (0..batch.num_rows())
            .map(|row| {
                let mut key = Vec::with_capacity(left_indices.len());
                for &ci in &left_indices {
                    let vids = batch
                        .column(ci)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    if vids.is_null(row) {
                        return true; // null keys are never banned
                    }
                    key.push(vids.value(row));
                }
                !banned.contains(&key)
            })
            .collect();
        let keep_arr = BooleanArray::from(keep);
        let filtered = filter_record_batch(&batch, &keep_arr).map_err(arrow_err)?;
        if filtered.num_rows() > 0 {
            result.push(filtered);
        }
    }
    Ok(result)
}

/// Multiply `__prob_complement_*` columns into the rule's PROB column and clean up.
///
/// After IS NOT probabilistic complement semantics have added `__prob_complement_*`
/// columns to clause results, this function:
/// 1. Computes the product of all complement factor columns
/// 2. Multiplies the product into the existing PROB column (if any)
/// 3. Removes the internal `__prob_complement_*` columns from the output
///
/// If the rule has no PROB column, complement columns are simply removed
/// (the complement information is discarded and IS NOT acts as a keep-all).
pub fn multiply_prob_factors(
    batches: Vec<RecordBatch>,
    prob_col: Option<&str>,
    complement_cols: &[String],
) -> datafusion::error::Result<Vec<RecordBatch>> {
    use arrow_array::{Array as _, Float64Array};

    let mut result = Vec::with_capacity(batches.len());

    for batch in batches {
        if batch.num_rows() == 0 {
            // Remove complement columns from empty batches
            let keep: Vec<usize> = batch
                .schema()
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| !complement_cols.contains(f.name()))
                .map(|(i, _)| i)
                .collect();
            let fields: Vec<_> = keep
                .iter()
                .map(|&i| batch.schema().field(i).clone())
                .collect();
            let cols: Vec<_> = keep.iter().map(|&i| batch.column(i).clone()).collect();
            let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
            result.push(
                RecordBatch::try_new(schema, cols).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?,
            );
            continue;
        }

        let num_rows = batch.num_rows();

        // 1. Compute product of all complement factors
        let mut combined = vec![1.0f64; num_rows];
        for col_name in complement_cols {
            if let Ok(idx) = batch.schema().index_of(col_name) {
                let arr = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(format!(
                            "Expected Float64 for complement column {col_name}"
                        ))
                    })?;
                for (i, val) in combined.iter_mut().enumerate().take(num_rows) {
                    if !arr.is_null(i) {
                        *val *= arr.value(i);
                    }
                }
            }
        }

        // 2. If there's a PROB column, multiply combined into it
        let final_prob: Vec<f64> = if let Some(prob_name) = prob_col {
            if let Ok(idx) = batch.schema().index_of(prob_name) {
                let arr = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(format!(
                            "Expected Float64 for PROB column {prob_name}"
                        ))
                    })?;
                (0..num_rows)
                    .map(|i| {
                        if arr.is_null(i) {
                            combined[i]
                        } else {
                            arr.value(i) * combined[i]
                        }
                    })
                    .collect()
            } else {
                combined
            }
        } else {
            combined
        };

        let new_prob_array: arrow_array::ArrayRef =
            std::sync::Arc::new(Float64Array::from(final_prob));

        // 3. Build output: replace PROB column, remove complement columns
        let mut fields = Vec::new();
        let mut columns = Vec::new();

        for (idx, field) in batch.schema().fields().iter().enumerate() {
            if complement_cols.contains(field.name()) {
                continue;
            }
            if prob_col.is_some_and(|p| field.name() == p) {
                fields.push(field.clone());
                columns.push(new_prob_array.clone());
            } else {
                fields.push(field.clone());
                columns.push(batch.column(idx).clone());
            }
        }

        let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
        result.push(RecordBatch::try_new(schema, columns).map_err(arrow_err)?);
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Float64Array;
    use arrow_schema::{DataType, Field, Schema};

    fn make_vid_prob_batch(vids: &[u64], probs: &[f64]) -> RecordBatch {
        use arrow_array::UInt64Array;
        let schema = Arc::new(Schema::new(vec![
            Field::new("vid", DataType::UInt64, true),
            Field::new("prob", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vids.to_vec())),
                Arc::new(Float64Array::from(probs.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_prob_complement_basic() {
        // neg has VID=1 with prob=0.7 → complement=0.3; VID=2 absent → complement=1.0
        let body = make_vid_prob_batch(&[1, 2], &[0.9, 0.8]);
        let neg = make_vid_prob_batch(&[1], &[0.7]);
        let join_cols = vec![("vid".to_string(), "vid".to_string())];
        let result = apply_prob_complement_composite(
            vec![body],
            &[neg],
            &join_cols,
            "prob",
            "__complement_0",
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        let complement = batch
            .column_by_name("__complement_0")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // VID=1: complement = 1 - 0.7 = 0.3
        assert!(
            (complement.value(0) - 0.3).abs() < 1e-10,
            "expected 0.3, got {}",
            complement.value(0)
        );
        // VID=2: absent from neg → complement = 1.0
        assert!(
            (complement.value(1) - 1.0).abs() < 1e-10,
            "expected 1.0, got {}",
            complement.value(1)
        );
    }

    #[test]
    fn test_prob_complement_noisy_or_duplicates() {
        // neg has VID=1 twice with prob=0.3 and prob=0.5
        // Combined via noisy-OR: 1-(1-0.3)(1-0.5) = 0.65
        // Complement = 1 - 0.65 = 0.35
        let body = make_vid_prob_batch(&[1], &[0.9]);
        let neg = make_vid_prob_batch(&[1, 1], &[0.3, 0.5]);
        let join_cols = vec![("vid".to_string(), "vid".to_string())];
        let result = apply_prob_complement_composite(
            vec![body],
            &[neg],
            &join_cols,
            "prob",
            "__complement_0",
        )
        .unwrap();
        let batch = &result[0];
        let complement = batch
            .column_by_name("__complement_0")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (complement.value(0) - 0.35).abs() < 1e-10,
            "expected 0.35, got {}",
            complement.value(0)
        );
    }

    #[test]
    fn test_prob_complement_empty_neg() {
        // Empty neg_facts → body passes through with complement=1.0
        let body = make_vid_prob_batch(&[1, 2], &[0.5, 0.6]);
        let join_cols = vec![("vid".to_string(), "vid".to_string())];
        let result =
            apply_prob_complement_composite(vec![body], &[], &join_cols, "prob", "__complement_0")
                .unwrap();
        let batch = &result[0];
        let complement = batch
            .column_by_name("__complement_0")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..2 {
            assert!(
                (complement.value(i) - 1.0).abs() < 1e-10,
                "row {}: expected 1.0, got {}",
                i,
                complement.value(i)
            );
        }
    }

    #[test]
    fn test_anti_join_basic() {
        // body [1,2,3], neg [2] → result [1,3]
        use arrow_array::UInt64Array;
        let body = make_vid_prob_batch(&[1, 2, 3], &[0.5, 0.6, 0.7]);
        let neg = make_vid_prob_batch(&[2], &[0.0]);
        let join_cols = vec![("vid".to_string(), "vid".to_string())];
        let result = apply_anti_join_composite(vec![body], &[neg], &join_cols).unwrap();
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        let vids = batch
            .column_by_name("vid")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(vids.value(0), 1);
        assert_eq!(vids.value(1), 3);
    }

    #[test]
    fn test_anti_join_empty_neg() {
        // Empty neg → all rows kept
        let body = make_vid_prob_batch(&[1, 2, 3], &[0.5, 0.6, 0.7]);
        let join_cols = vec![("vid".to_string(), "vid".to_string())];
        let result = apply_anti_join_composite(vec![body], &[], &join_cols).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);
    }

    #[test]
    fn test_anti_join_all_excluded() {
        // neg covers all body rows → empty result
        let body = make_vid_prob_batch(&[1, 2], &[0.5, 0.6]);
        let neg = make_vid_prob_batch(&[1, 2], &[0.0, 0.0]);
        let join_cols = vec![("vid".to_string(), "vid".to_string())];
        let result = apply_anti_join_composite(vec![body], &[neg], &join_cols).unwrap();
        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn test_multiply_prob_single_complement() {
        // prob=0.8, complement=0.5 → output prob=0.4; complement col removed
        let body = make_vid_prob_batch(&[1], &[0.8]);
        // Add a complement column
        let complement_arr = Float64Array::from(vec![0.5]);
        let mut cols: Vec<arrow_array::ArrayRef> = body.columns().to_vec();
        cols.push(Arc::new(complement_arr));
        let mut fields: Vec<Arc<Field>> = body.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(
            "__complement_0",
            DataType::Float64,
            true,
        )));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, cols).unwrap();

        let result =
            multiply_prob_factors(vec![batch], Some("prob"), &["__complement_0".to_string()])
                .unwrap();
        assert_eq!(result.len(), 1);
        let out = &result[0];
        // Complement column should be removed
        assert!(out.column_by_name("__complement_0").is_none());
        let prob = out
            .column_by_name("prob")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (prob.value(0) - 0.4).abs() < 1e-10,
            "expected 0.4, got {}",
            prob.value(0)
        );
    }

    #[test]
    fn test_multiply_prob_multiple_complements() {
        // prob=0.8, c1=0.5, c2=0.6 → 0.8×0.5×0.6=0.24
        let body = make_vid_prob_batch(&[1], &[0.8]);
        let c1 = Float64Array::from(vec![0.5]);
        let c2 = Float64Array::from(vec![0.6]);
        let mut cols: Vec<arrow_array::ArrayRef> = body.columns().to_vec();
        cols.push(Arc::new(c1));
        cols.push(Arc::new(c2));
        let mut fields: Vec<Arc<Field>> = body.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new("__c1", DataType::Float64, true)));
        fields.push(Arc::new(Field::new("__c2", DataType::Float64, true)));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, cols).unwrap();

        let result = multiply_prob_factors(
            vec![batch],
            Some("prob"),
            &["__c1".to_string(), "__c2".to_string()],
        )
        .unwrap();
        let out = &result[0];
        assert!(out.column_by_name("__c1").is_none());
        assert!(out.column_by_name("__c2").is_none());
        let prob = out
            .column_by_name("prob")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (prob.value(0) - 0.24).abs() < 1e-10,
            "expected 0.24, got {}",
            prob.value(0)
        );
    }

    #[test]
    fn test_multiply_prob_no_prob_column() {
        // No prob column → combined complements become the output
        use arrow_array::UInt64Array;
        let schema = Arc::new(Schema::new(vec![
            Field::new("vid", DataType::UInt64, true),
            Field::new("__c1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(Float64Array::from(vec![0.7])),
            ],
        )
        .unwrap();

        let result = multiply_prob_factors(vec![batch], None, &["__c1".to_string()]).unwrap();
        let out = &result[0];
        // __c1 should be removed since it's a complement column
        assert!(out.column_by_name("__c1").is_none());
        // Only vid column remains
        assert_eq!(out.num_columns(), 1);
    }
}
