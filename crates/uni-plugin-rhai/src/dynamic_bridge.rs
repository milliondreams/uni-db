//! Bridge between Rhai's `Dynamic` and Arrow / DataFusion scalar types.
//!
//! Row mode (v1) iterates Arrow arrays row by row, converts each cell to
//! a `rhai::Dynamic`, calls the Rhai function, and writes the returned
//! `Dynamic` back into an Arrow array builder. This module owns those
//! conversions for the 5 supported primitive types: `Boolean`, `Int64`,
//! `Float64`, `Utf8`, and `Null`.
//!
//! `i64` round-trips through `Dynamic::from::<i64>` explicitly to avoid
//! accidental float coercion via Rhai's number-tower; the rhai `INT`
//! alias is `i64` under the default build.

#![cfg(feature = "rhai-runtime")]

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
    builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
};
use arrow_schema::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use rhai::Dynamic;
use std::sync::Arc;

use crate::error::RhaiError;

/// Read one cell from a `ColumnarValue` argument at `row` and convert to
/// a `Dynamic` suitable for passing to a Rhai function call.
///
/// `Scalar` args return the underlying scalar value regardless of `row`.
/// `Array` args return the value at the given row, propagating `Null`s
/// as `Dynamic::UNIT` (Rhai's `()` — the null/unit value).
pub fn column_row_to_dynamic(
    arg: &ColumnarValue,
    row: usize,
    expected: &DataType,
) -> Result<Dynamic, RhaiError> {
    match arg {
        ColumnarValue::Scalar(s) => scalar_to_dynamic(s),
        ColumnarValue::Array(a) => array_row_to_dynamic(a, row, expected),
    }
}

/// Convert a single row of an Arrow array to a `Dynamic`.
pub fn array_row_to_dynamic(
    arr: &ArrayRef,
    row: usize,
    expected: &DataType,
) -> Result<Dynamic, RhaiError> {
    if arr.is_null(row) {
        return Ok(Dynamic::UNIT);
    }
    match expected {
        DataType::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                RhaiError::Conversion(format!("expected Float64 array, got {:?}", arr.data_type()))
            })?;
            Ok(Dynamic::from(a.value(row)))
        }
        DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                RhaiError::Conversion(format!("expected Int64 array, got {:?}", arr.data_type()))
            })?;
            // Explicit i64 path to avoid Rhai's number-tower float coercion.
            Ok(Dynamic::from(a.value(row)))
        }
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                RhaiError::Conversion(format!("expected Utf8 array, got {:?}", arr.data_type()))
            })?;
            Ok(Dynamic::from(a.value(row).to_owned()))
        }
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                RhaiError::Conversion(format!("expected Boolean array, got {:?}", arr.data_type()))
            })?;
            Ok(Dynamic::from(a.value(row)))
        }
        DataType::Null => Ok(Dynamic::UNIT),
        other => Err(RhaiError::Conversion(format!(
            "unsupported Arrow type for Rhai bridge: {other:?}"
        ))),
    }
}

/// Convert a DataFusion `ScalarValue` to a `Dynamic`.
pub fn scalar_to_dynamic(s: &ScalarValue) -> Result<Dynamic, RhaiError> {
    Ok(match s {
        ScalarValue::Null => Dynamic::UNIT,
        ScalarValue::Boolean(Some(b)) => Dynamic::from(*b),
        ScalarValue::Float64(Some(f)) => Dynamic::from(*f),
        ScalarValue::Float32(Some(f)) => Dynamic::from(*f as f64),
        ScalarValue::Int64(Some(i)) => Dynamic::from(*i),
        ScalarValue::Int32(Some(i)) => Dynamic::from(*i as i64),
        ScalarValue::Utf8(Some(s)) => Dynamic::from(s.clone()),
        ScalarValue::LargeUtf8(Some(s)) => Dynamic::from(s.clone()),
        // Any `None` scalar (null) maps to Rhai unit.
        s if s.is_null() => Dynamic::UNIT,
        other => {
            return Err(RhaiError::Conversion(format!(
                "unsupported ScalarValue for Rhai bridge: {other:?}"
            )));
        }
    })
}

/// An accumulating output builder selected by the expected output Arrow
/// type. Used by the scalar adapter to write per-row Rhai return values
/// into a column.
#[derive(Debug)]
pub enum OutBuilder {
    /// Bool output column.
    Bool(BooleanBuilder),
    /// Int64 output column.
    Int(Int64Builder),
    /// Float64 output column.
    Float(Float64Builder),
    /// Utf8 output column.
    Str(StringBuilder),
}

impl OutBuilder {
    /// Allocate a builder sized for `rows` rows matching the expected
    /// Arrow `DataType`.
    pub fn new(ty: &DataType, rows: usize) -> Result<Self, RhaiError> {
        Ok(match ty {
            DataType::Boolean => Self::Bool(BooleanBuilder::with_capacity(rows)),
            DataType::Int64 => Self::Int(Int64Builder::with_capacity(rows)),
            DataType::Float64 => Self::Float(Float64Builder::with_capacity(rows)),
            DataType::Utf8 => Self::Str(StringBuilder::with_capacity(rows, 16 * rows)),
            other => {
                return Err(RhaiError::Conversion(format!(
                    "unsupported output Arrow type: {other:?}"
                )));
            }
        })
    }

    /// Append a `Dynamic` (or null on `Dynamic::UNIT`) to the builder.
    pub fn push(&mut self, d: Dynamic) -> Result<(), RhaiError> {
        if d.is_unit() {
            match self {
                Self::Bool(b) => b.append_null(),
                Self::Int(b) => b.append_null(),
                Self::Float(b) => b.append_null(),
                Self::Str(b) => b.append_null(),
            }
            return Ok(());
        }
        match self {
            Self::Bool(b) => {
                let v = d
                    .as_bool()
                    .map_err(|t| RhaiError::Conversion(format!("expected bool, got {t}")))?;
                b.append_value(v);
            }
            Self::Int(b) => {
                let v = d
                    .as_int()
                    .map_err(|t| RhaiError::Conversion(format!("expected int, got {t}")))?;
                b.append_value(v);
            }
            Self::Float(b) => {
                let v = d
                    .as_float()
                    .map_err(|t| RhaiError::Conversion(format!("expected float, got {t}")))?;
                b.append_value(v);
            }
            Self::Str(b) => {
                let v = d
                    .into_string()
                    .map_err(|t| RhaiError::Conversion(format!("expected string, got {t}")))?;
                b.append_value(v);
            }
        }
        Ok(())
    }

    /// Finalise into an `ArrayRef`.
    pub fn finish(self) -> ArrayRef {
        match self {
            Self::Bool(mut b) => Arc::new(b.finish()) as ArrayRef,
            Self::Int(mut b) => Arc::new(b.finish()) as ArrayRef,
            Self::Float(mut b) => Arc::new(b.finish()) as ArrayRef,
            Self::Str(mut b) => Arc::new(b.finish()) as ArrayRef,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;

    #[test]
    fn int64_array_roundtrips_without_float_coercion() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX, 0, i64::MIN]));
        let d0 = array_row_to_dynamic(&arr, 0, &DataType::Int64).unwrap();
        assert_eq!(d0.as_int().unwrap(), i64::MAX);
        let d2 = array_row_to_dynamic(&arr, 2, &DataType::Int64).unwrap();
        assert_eq!(d2.as_int().unwrap(), i64::MIN);
    }

    #[test]
    fn null_row_yields_unit() {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), None, Some(3.0)]));
        let d = array_row_to_dynamic(&arr, 1, &DataType::Float64).unwrap();
        assert!(d.is_unit());
    }

    #[test]
    fn out_builder_handles_nulls() {
        let mut b = OutBuilder::new(&DataType::Float64, 3).unwrap();
        b.push(Dynamic::from(1.5)).unwrap();
        b.push(Dynamic::UNIT).unwrap();
        b.push(Dynamic::from(3.0)).unwrap();
        let arr = b.finish();
        let f = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(f.value(0), 1.5);
        assert!(f.is_null(1));
        assert_eq!(f.value(2), 3.0);
    }

    #[test]
    fn string_roundtrip() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec![Some("hi"), None, Some("there")]));
        let d0 = array_row_to_dynamic(&arr, 0, &DataType::Utf8).unwrap();
        assert_eq!(d0.into_string().unwrap(), "hi");
        let d1 = array_row_to_dynamic(&arr, 1, &DataType::Utf8).unwrap();
        assert!(d1.is_unit());
    }
}
