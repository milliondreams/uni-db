//! Shared helpers used by the scalar / aggregate / procedure adapters
//! for marshaling per-row values between Arrow arrays and Python objects.

#![cfg(feature = "pyo3")]

use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow_schema::DataType;
use datafusion::logical_expr::Volatility;
use datafusion::scalar::ScalarValue;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyTracebackMethods};

use uni_plugin::errors::FnError;

/// Map a manifest type-name spelling to the v1 primitive [`DataType`].
///
/// Recognizes the float/int/string/bool families (case-insensitive,
/// trimmed). Returns `None` for unknown names so each caller can wrap
/// the failure in its own error type with a tailored message.
#[must_use]
pub fn type_name_to_datatype(name: &str) -> Option<DataType> {
    match name.trim().to_ascii_lowercase().as_str() {
        "float" | "float64" | "double" => Some(DataType::Float64),
        "int" | "int64" | "long" => Some(DataType::Int64),
        "string" | "str" | "utf8" => Some(DataType::Utf8),
        "bool" | "boolean" => Some(DataType::Boolean),
        _ => None,
    }
}

/// Map a determinism spelling (`pure` / `session` / other) to the
/// DataFusion [`Volatility`] used for the registered function.
#[must_use]
pub fn determinism_to_volatility(determinism: &str) -> Volatility {
    match determinism.trim().to_ascii_lowercase().as_str() {
        "pure" => Volatility::Immutable,
        "session" | "session-scoped" | "sessionscoped" => Volatility::Stable,
        _ => Volatility::Volatile,
    }
}

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
    /// One match arm per primitive: downcast, read the row, convert to Python.
    macro_rules! arm {
        ($variant:ident, $arr_ty:ty, $label:literal) => {{
            let a = arr
                .as_any()
                .downcast_ref::<$arr_ty>()
                .ok_or_else(|| FnError::new(0x83, concat!("expected ", stringify!($arr_ty))))?;
            a.value(row)
                .into_bound_py_any(py)
                .map_err(|e| FnError::new(0x83, format!(concat!($label, "→py: {}"), e)))
        }};
    }
    match dt {
        DataType::Float64 => arm!(Float64, Float64Array, "f64"),
        DataType::Int64 => arm!(Int64, Int64Array, "i64"),
        DataType::Utf8 => arm!(Utf8, StringArray, "utf8"),
        DataType::Boolean => arm!(Boolean, BooleanArray, "bool"),
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
    /// One match arm per primitive: extract the Rust type, wrap in the scalar.
    macro_rules! arm {
        ($variant:ident, $rust_ty:ty, $label:literal) => {{
            let v: $rust_ty = obj
                .extract()
                .map_err(|e| FnError::new(0x83, format!(concat!("py→", $label, ": {}"), e)))?;
            Ok(ScalarValue::$variant(Some(v)))
        }};
    }
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
        DataType::Float64 => arm!(Float64, f64, "f64"),
        DataType::Int64 => arm!(Int64, i64, "i64"),
        DataType::Utf8 => arm!(Utf8, String, "utf8"),
        DataType::Boolean => arm!(Boolean, bool, "bool"),
        other => Err(FnError::new(
            0x83,
            format!(
                "PyO3 py_to_scalar: return type `{other}` not yet supported \
             (v1 covers Float64/Int64/Utf8/Boolean)"
            ),
        )),
    }
}

/// Map a `PyErr` into an `FnError` in the `0x82–0x8F` family.
///
/// `label` is an optional adapter-kind prefix (`""`, `"aggregate "`,
/// `"procedure "`) inserted before the qname so messages stay
/// distinguishable across the three adapters. Acquires the GIL to
/// capture the exception `repr` and traceback (best effort).
pub fn classify_pyerr(code: u32, label: &str, qname: &str, e: PyErr) -> FnError {
    Python::attach(|py| {
        let traceback = e
            .traceback(py)
            .and_then(|tb| tb.format().ok())
            .unwrap_or_default();
        let value = e.value(py);
        let msg = value
            .repr()
            .map(|r| r.to_string())
            .unwrap_or_else(|_| e.to_string());
        FnError::new(code, format!("PyO3 {label}`{qname}`: {msg}\n{traceback}"))
    })
}

/// Per-batch / per-column output builder over the v1 primitive set
/// (Float64/Int64/Utf8/Boolean).
///
/// Shared by the row-mode scalar adapter and the procedure adapter:
/// both build one Arrow array per output column from Python values,
/// extracting the same Rust primitives and treating Python `None` as a
/// typed null.
#[derive(Debug)]
pub enum PrimitiveColumnBuilder {
    /// `Float64` column builder.
    Float64(Float64Builder),
    /// `Int64` column builder.
    Int64(Int64Builder),
    /// `Utf8` column builder.
    Utf8(StringBuilder),
    /// `Boolean` column builder.
    Boolean(BooleanBuilder),
}

impl PrimitiveColumnBuilder {
    /// Construct a builder for `dt` with the given initial capacity.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] when `dt` is outside the v1 supported set.
    /// `error_code` and `context` tailor the message to the caller
    /// (scalar return type vs. procedure yield type).
    pub fn new(
        dt: &DataType,
        capacity: usize,
        error_code: u32,
        context: &str,
    ) -> Result<Self, FnError> {
        Ok(match dt {
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, 0)),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            other => {
                return Err(FnError::new(
                    error_code,
                    format!(
                        "{context} `{other}` not yet supported \
                         (v1 covers Float64/Int64/Utf8/Boolean)"
                    ),
                ));
            }
        })
    }

    /// Append a typed null.
    pub fn push_null(&mut self) {
        match self {
            Self::Float64(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::Utf8(b) => b.append_null(),
            Self::Boolean(b) => b.append_null(),
        }
    }

    /// Append a Python value, extracting the matching Rust primitive.
    ///
    /// A Python `None` appends a typed null. Extraction failures are
    /// mapped through [`classify_pyerr`] with `code`/`label`/`qname`.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] when the Python value cannot be extracted as
    /// the builder's primitive type.
    pub fn push_py(
        &mut self,
        value: &Bound<'_, PyAny>,
        code: u32,
        label: &str,
        qname: &str,
    ) -> Result<(), FnError> {
        if value.is_none() {
            self.push_null();
            return Ok(());
        }
        match self {
            Self::Float64(b) => {
                let v: f64 = value
                    .extract()
                    .map_err(|e| classify_pyerr(code, label, qname, e))?;
                b.append_value(v);
            }
            Self::Int64(b) => {
                let v: i64 = value
                    .extract()
                    .map_err(|e| classify_pyerr(code, label, qname, e))?;
                b.append_value(v);
            }
            Self::Utf8(b) => {
                let v: String = value
                    .extract()
                    .map_err(|e| classify_pyerr(code, label, qname, e))?;
                b.append_value(v);
            }
            Self::Boolean(b) => {
                let v: bool = value
                    .extract()
                    .map_err(|e| classify_pyerr(code, label, qname, e))?;
                b.append_value(v);
            }
        }
        Ok(())
    }

    /// Finish the builder, producing the column array.
    #[must_use]
    pub fn finish(self) -> ArrayRef {
        match self {
            Self::Float64(mut b) => Arc::new(b.finish()),
            Self::Int64(mut b) => Arc::new(b.finish()),
            Self::Utf8(mut b) => Arc::new(b.finish()),
            Self::Boolean(mut b) => Arc::new(b.finish()),
        }
    }
}
