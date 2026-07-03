//! Translate wire-level type strings into internal Arrow `DataType`s and
//! `uni_plugin` signature types.
//!
//! Rhai scripts spell out arg / return types as short strings (`"float"`,
//! `"int"`, `"string"`, `"bool"`) inside their `uni_manifest()` return
//! value. This module converts those strings into the typed
//! `arrow_schema::DataType` and `uni_plugin::ArgType` values the executor
//! expects.

use arrow_schema::DataType;
use datafusion::logical_expr::Volatility;
use uni_plugin::traits::scalar::{ArgType, FnSignature, NullHandling};

use crate::error::RhaiError;

/// Convert a wire-level type name to an Arrow `DataType`.
///
/// Accepted names (case-insensitive): `"float"`, `"float64"`, `"double"`,
/// `"f64"` → `Float64`; `"float32"`, `"f32"` → `Float32`; `"int"`,
/// `"int64"`, `"long"`, `"i64"` → `Int64`; `"int32"`, `"i32"` → `Int32`;
/// `"string"`, `"utf8"`, `"str"` → `Utf8`; `"bool"`, `"boolean"` →
/// `Boolean`; `"null"`, `"void"`, `"()"` → `Null`.
pub fn type_name_to_datatype(name: &str) -> Result<DataType, RhaiError> {
    let normalized = name.trim().to_ascii_lowercase();
    Ok(match normalized.as_str() {
        "float" | "float64" | "double" | "f64" => DataType::Float64,
        "float32" | "f32" => DataType::Float32,
        "int" | "int64" | "long" | "i64" => DataType::Int64,
        "int32" | "i32" => DataType::Int32,
        "string" | "utf8" | "str" => DataType::Utf8,
        "bool" | "boolean" => DataType::Boolean,
        "null" | "void" | "()" => DataType::Null,
        other => {
            return Err(RhaiError::ManifestInvalid(format!(
                "unknown type name `{other}`; supported: float/int/string/bool/null"
            )));
        }
    })
}

/// Convert a wire-level type name to a uni-plugin `ArgType::Primitive`.
pub fn type_name_to_argtype(name: &str) -> Result<ArgType, RhaiError> {
    type_name_to_datatype(name).map(ArgType::Primitive)
}

/// Build an `FnSignature` from wire-level `args` + `returns` type names
/// and a determinism string from the manifest.
pub fn build_fn_signature(
    args: &[String],
    returns: &str,
    determinism: &str,
) -> Result<FnSignature, RhaiError> {
    let arg_types: Vec<ArgType> = args
        .iter()
        .map(|s| type_name_to_argtype(s))
        .collect::<Result<_, _>>()?;
    let return_type = type_name_to_argtype(returns)?;
    let volatility = determinism_to_volatility(determinism);
    Ok(FnSignature {
        args: arg_types,
        returns: return_type,
        volatility,
        null_handling: NullHandling::PropagateNulls,
    })
}

/// Map a manifest determinism string to a DataFusion volatility.
pub fn determinism_to_volatility(determinism: &str) -> Volatility {
    match determinism.trim().to_ascii_lowercase().as_str() {
        "pure" | "immutable" => Volatility::Immutable,
        "session-scoped" | "session" | "stable" => Volatility::Stable,
        _ => Volatility::Volatile,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_type_names_map() {
        assert!(matches!(
            type_name_to_datatype("float").unwrap(),
            DataType::Float64
        ));
        assert!(matches!(
            type_name_to_datatype("int").unwrap(),
            DataType::Int64
        ));
        assert!(matches!(
            type_name_to_datatype("string").unwrap(),
            DataType::Utf8
        ));
        assert!(matches!(
            type_name_to_datatype("bool").unwrap(),
            DataType::Boolean
        ));
    }

    #[test]
    fn unknown_type_name_rejected() {
        assert!(type_name_to_datatype("uuid").is_err());
    }

    #[test]
    fn determinism_maps_to_volatility() {
        assert!(matches!(
            determinism_to_volatility("pure"),
            Volatility::Immutable
        ));
        assert!(matches!(
            determinism_to_volatility("session"),
            Volatility::Stable
        ));
        assert!(matches!(
            determinism_to_volatility("nondeterministic"),
            Volatility::Volatile
        ));
    }
}
