// Rust guideline compliant
//! Shared decoding / conversion helpers for declared scalar functions
//! and aggregates.
//!
//! `uni.plugin.declareFunction` ([`crate::scalar`]),
//! `declareAggregate` ([`crate::aggregate`]), the registry-installation
//! paths in [`crate::lib`], and the expression interpreter
//! ([`crate::eval`]) all share the same Arrow-row decoder, value
//! stringifier, type-name parser, qname splitters, and error mappers.
//! They live here so there is a single source of truth.

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, LargeBinaryArray, StringArray,
};
use arrow_schema::DataType;
use uni_common::Value;
use uni_plugin::{FnError, PluginError};

use crate::CustomError;
use crate::eval::EvalError;

/// Parse a declared return-type string into an Arrow [`DataType`].
///
/// Accepts the canonical Cypher type names plus their common aliases.
/// Returns `None` for unrecognized names.
#[must_use]
pub(crate) fn type_str_to_arrow(s: &str) -> Option<DataType> {
    match s.to_ascii_lowercase().as_str() {
        "string" | "utf8" | "str" => Some(DataType::Utf8),
        "int" | "integer" | "int64" | "i64" => Some(DataType::Int64),
        "float" | "double" | "float64" | "f64" => Some(DataType::Float64),
        "bool" | "boolean" => Some(DataType::Boolean),
        _ => None,
    }
}

/// Derive the synthetic plugin id from a declared qname.
///
/// Uses the first dotted segment as the plugin id so the registrar's
/// `validate_qname` accepts the declared qname (e.g. `mycorp.fullName`
/// registers under plugin id `mycorp`). Qnames without a `.` fall back
/// to [`crate::CustomPlugin::ID`].
#[must_use]
pub(crate) fn declared_plugin_id(qname: &str) -> String {
    qname
        .split_once('.')
        .map(|(ns, _)| ns.to_owned())
        .unwrap_or_else(|| crate::CustomPlugin::ID.to_owned())
}

/// Return the local part of a declared qname (everything after the
/// first `.`), or the whole string if it has no `.`.
#[must_use]
pub(crate) fn local_part(qname: &str) -> &str {
    qname.split_once('.').map(|(_, l)| l).unwrap_or(qname)
}

/// Map a registrar [`PluginError`] into a [`CustomError`], folding a
/// duplicate registration into [`CustomError::NativeShadow`] for the
/// given qname.
#[must_use]
pub(crate) fn map_plugin_error(e: PluginError, qname: &str) -> CustomError {
    match e {
        PluginError::DuplicateRegistration(_) => CustomError::NativeShadow(qname.to_owned()),
        other => CustomError::Registration(other.to_string()),
    }
}

/// Map an [`EvalError`] from the expression interpreter into a
/// [`FnError`] with a stable code.
#[must_use]
pub(crate) fn eval_err_to_fn(e: EvalError) -> FnError {
    let code = match &e {
        EvalError::UnboundParameter(_) => 0xB10,
        EvalError::Unsupported(_) => 0xB11,
        EvalError::TypeMismatch { .. } => FnError::CODE_TYPE_COERCION,
        EvalError::Arithmetic(_) => 0xB12,
    };
    FnError::new(code, e.to_string())
}

/// Render a [`uni_common::Value`] as a display string.
///
/// Scalars use their natural textual form; richer variants fall back to
/// their `Debug` representation.
#[must_use]
pub(crate) fn stringify(v: &Value) -> String {
    match v {
        Value::Null => "null".to_owned(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

/// Decode a single Arrow row into a [`uni_common::Value`].
///
/// Out-of-range rows and null slots decode to [`Value::Null`]. The
/// `LargeBinary` arm decodes uni-db's CypherValue-encoded property
/// blobs into the rich `Value` model (harmless for the scalar path,
/// which never sees `LargeBinary` columns).
///
/// # Errors
///
/// Returns [`FnError`] on downcast failure, blob-decode failure, or an
/// unsupported input column type.
pub(crate) fn array_value_at(arr: &ArrayRef, row: usize) -> Result<Value, FnError> {
    if row >= arr.len() || arr.is_null(row) {
        return Ok(Value::Null);
    }
    match arr.data_type() {
        DataType::Utf8 => {
            let a = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| FnError::new(FnError::CODE_TYPE_COERCION, "Utf8 downcast"))?;
            Ok(Value::String(a.value(row).to_owned()))
        }
        DataType::Int64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| FnError::new(FnError::CODE_TYPE_COERCION, "Int64 downcast"))?;
            Ok(Value::Int(a.value(row)))
        }
        DataType::Int32 => {
            let a = arr
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .ok_or_else(|| FnError::new(FnError::CODE_TYPE_COERCION, "Int32 downcast"))?;
            Ok(Value::Int(i64::from(a.value(row))))
        }
        DataType::Float64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| FnError::new(FnError::CODE_TYPE_COERCION, "Float64 downcast"))?;
            Ok(Value::Float(a.value(row)))
        }
        DataType::Float32 => {
            let a = arr
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .ok_or_else(|| FnError::new(FnError::CODE_TYPE_COERCION, "Float32 downcast"))?;
            Ok(Value::Float(f64::from(a.value(row))))
        }
        DataType::Boolean => {
            let a = arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| FnError::new(FnError::CODE_TYPE_COERCION, "Bool downcast"))?;
            Ok(Value::Bool(a.value(row)))
        }
        DataType::LargeBinary => {
            // uni-db stores node/edge properties as CypherValue-encoded
            // LargeBinary blobs. Decode into the rich `Value` model so
            // the interpreter can operate on Int/Float/String/etc.
            let a = arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| FnError::new(FnError::CODE_TYPE_COERCION, "LargeBinary downcast"))?;
            let bytes = a.value(row);
            uni_common::cypher_value_codec::decode(bytes).map_err(|e| {
                FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!("LargeBinary decode: {e}"),
                )
            })
        }
        other => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("declared fn input type {other:?} not supported"),
        )),
    }
}
