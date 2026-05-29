//! Shared helpers used by both `adapter_scalar` and `adapter_aggregate`
//! for marshaling per-row values between Arrow arrays and Python objects.

#![cfg(feature = "pyo3")]

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow_schema::DataType;
use datafusion::scalar::ScalarValue;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

use uni_plugin::errors::FnError;

/// Read scalar at `row` from `arr` and produce a `Bound<PyAny>`.
///
/// # Errors
///
/// Returns [`FnError`] when the Arrow array kind doesn't match `dt`
/// or `dt` is outside the v1 supported set (Float64/Int64/Utf8/Boolean).
pub fn scalar_to_py<'py>(
    py: Python<'py>,
    arr: &dyn Array,
    row: usize,
    dt: &DataType,
) -> Result<Bound<'py, PyAny>, FnError> {
    match dt {
        DataType::Float64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| FnError::new(0x83, "expected Float64Array"))?;
            a.value(row)
                .into_bound_py_any(py)
                .map_err(|e| FnError::new(0x83, format!("f64→py: {e}")))
        }
        DataType::Int64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| FnError::new(0x83, "expected Int64Array"))?;
            a.value(row)
                .into_bound_py_any(py)
                .map_err(|e| FnError::new(0x83, format!("i64→py: {e}")))
        }
        DataType::Utf8 => {
            let a = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| FnError::new(0x83, "expected StringArray"))?;
            a.value(row)
                .into_bound_py_any(py)
                .map_err(|e| FnError::new(0x83, format!("utf8→py: {e}")))
        }
        DataType::Boolean => {
            let a = arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| FnError::new(0x83, "expected BooleanArray"))?;
            a.value(row)
                .into_bound_py_any(py)
                .map_err(|e| FnError::new(0x83, format!("bool→py: {e}")))
        }
        other => Err(FnError::new(
            0x83,
            format!(
                "PyO3 row helper: input type `{other}` not yet supported \
             (v1 covers Float64/Int64/Utf8/Boolean)"
            ),
        )),
    }
}

/// Decode a Python value into a [`ScalarValue`] typed by `dt`.
///
/// `None` Python values become typed-null scalars
/// (`Float64(None)`, `Int64(None)`, etc.).
///
/// # Errors
///
/// Returns [`FnError`] when the Python value cannot be extracted as
/// the expected primitive type, or `dt` is outside the v1 supported
/// set.
pub fn py_to_scalar(obj: &Bound<'_, PyAny>, dt: &DataType) -> Result<ScalarValue, FnError> {
    if obj.is_none() {
        return Ok(match dt {
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::Utf8 => ScalarValue::Utf8(None),
            DataType::Boolean => ScalarValue::Boolean(None),
            other => {
                return Err(FnError::new(
                    0x83,
                    format!("PyO3 py_to_scalar: unsupported `None` for `{other}`"),
                ));
            }
        });
    }
    match dt {
        DataType::Float64 => {
            let v: f64 = obj
                .extract()
                .map_err(|e| FnError::new(0x83, format!("py→f64: {e}")))?;
            Ok(ScalarValue::Float64(Some(v)))
        }
        DataType::Int64 => {
            let v: i64 = obj
                .extract()
                .map_err(|e| FnError::new(0x83, format!("py→i64: {e}")))?;
            Ok(ScalarValue::Int64(Some(v)))
        }
        DataType::Utf8 => {
            let v: String = obj
                .extract()
                .map_err(|e| FnError::new(0x83, format!("py→utf8: {e}")))?;
            Ok(ScalarValue::Utf8(Some(v)))
        }
        DataType::Boolean => {
            let v: bool = obj
                .extract()
                .map_err(|e| FnError::new(0x83, format!("py→bool: {e}")))?;
            Ok(ScalarValue::Boolean(Some(v)))
        }
        other => Err(FnError::new(
            0x83,
            format!(
                "PyO3 py_to_scalar: return type `{other}` not yet supported \
             (v1 covers Float64/Int64/Utf8/Boolean)"
            ),
        )),
    }
}
