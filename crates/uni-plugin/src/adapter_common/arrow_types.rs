//! `ArgType` ⇄ Arrow `DataType` mapping shared by every loader adapter.
//!
//! Before consolidation, four loader adapter files (`uni-plugin-wasm` ×2,
//! `uni-plugin-extism` ×2) each carried a byte-for-byte identical
//! `argtype_arrow` helper, and the `wasm` / `extism` loaders each had
//! their own `arrow_name_to_dt` / `arrow_name_to_datatype` enumeration of
//! supported wire-Arrow-primitive names. The wasm variant supported
//! `int32/int64/float32/float64/boolean/utf8/binary/largebinary`; the
//! extism variant added `date64` and `timestamp_ms`. This module hosts
//! the union — every supported wire name maps to a `DataType`, and the
//! caller's loader-specific error type is constructed on the
//! mapping failure path.
//!
//! These helpers are pure Arrow utilities; they do **not** depend on
//! `wasm-plugins` or `extism-plugins` feature gates.

// Rust guideline compliant

use std::sync::Arc;

use arrow_schema::{DataType, Field};

use crate::traits::scalar::ArgType;

/// Map an [`ArgType`] to the Arrow [`DataType`] used in the on-wire
/// arg/state/yield schema.
///
/// `Primitive` keeps its declared `DataType`. `Vector` maps to a
/// `FixedSizeList<element, len>`, matching the DataFusion UDAF bridge
/// (`arg_type_to_arrow` in `df_udaf_plugin.rs`) so the on-wire arg /
/// return / yield schema agrees with the type the query engine expects;
/// collapsing to the bare element type would make a vector-returning
/// aggregate a guaranteed type mismatch. `CypherValue` and `Variadic`
/// collapse to `LargeBinary` since both surfaces transport opaque encoded
/// payloads.
///
/// # Examples
///
/// ```
/// use arrow_schema::DataType;
/// use uni_plugin::adapter_common::arrow_types::argtype_to_arrow;
/// use uni_plugin::traits::scalar::ArgType;
///
/// assert_eq!(argtype_to_arrow(&ArgType::Primitive(DataType::Int64)), DataType::Int64);
/// assert_eq!(argtype_to_arrow(&ArgType::CypherValue), DataType::LargeBinary);
/// ```
#[must_use]
pub fn argtype_to_arrow(t: &ArgType) -> DataType {
    match t {
        ArgType::Primitive(d) => d.clone(),
        ArgType::CypherValue | ArgType::Variadic(_) => DataType::LargeBinary,
        ArgType::Vector { len, element } => {
            // A vector row is a `FixedSizeList`; clamp an absurd `len` to the
            // Arrow-representable `i32` range rather than overflowing.
            let list_len = i32::try_from(*len).unwrap_or(i32::MAX);
            DataType::FixedSizeList(
                Arc::new(Field::new("item", element.clone(), true)),
                list_len,
            )
        }
    }
}

/// Map a wire-protocol Arrow primitive name (lowercase, as plugins write
/// it on the wire) to the corresponding Arrow [`DataType`].
///
/// Returns `None` for any name outside the supported set. Both the
/// `wasm` and `extism` loaders previously enumerated this set in a local
/// match expression; the extism variant included `date64` and
/// `timestamp_ms` which the wasm variant did not. The shared helper
/// accepts the **union** — adding two names to the wasm acceptance set
/// is a strict superset and does not change behavior for the names wasm
/// already supported.
///
/// Callers wrap a `None` return in their loader-specific error variant
/// (`WasmError::InvalidWasm` / `ExtismError::ManifestInvalid`).
///
/// # Examples
///
/// ```
/// use arrow_schema::DataType;
/// use uni_plugin::adapter_common::arrow_types::arrow_name_to_datatype;
///
/// assert_eq!(arrow_name_to_datatype("int64"), Some(DataType::Int64));
/// assert_eq!(arrow_name_to_datatype("unknown"), None);
/// ```
#[must_use]
pub fn arrow_name_to_datatype(name: &str) -> Option<DataType> {
    Some(match name {
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "float32" => DataType::Float32,
        "float64" => DataType::Float64,
        "boolean" => DataType::Boolean,
        "utf8" => DataType::Utf8,
        "binary" => DataType::Binary,
        "largebinary" => DataType::LargeBinary,
        "date64" => DataType::Date64,
        "timestamp_ms" => DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argtype_primitive_passthrough() {
        assert_eq!(
            argtype_to_arrow(&ArgType::Primitive(DataType::Float64)),
            DataType::Float64
        );
    }

    #[test]
    fn argtype_cypher_value_is_large_binary() {
        assert_eq!(
            argtype_to_arrow(&ArgType::CypherValue),
            DataType::LargeBinary
        );
    }

    #[test]
    fn argtype_variadic_is_large_binary() {
        let inner = Box::new(ArgType::Primitive(DataType::Int32));
        assert_eq!(
            argtype_to_arrow(&ArgType::Variadic(inner)),
            DataType::LargeBinary
        );
    }

    #[test]
    fn argtype_vector_maps_to_fixed_size_list() {
        let v = ArgType::Vector {
            len: 4,
            element: DataType::Float32,
        };
        assert_eq!(
            argtype_to_arrow(&v),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4)
        );
    }

    #[test]
    fn arrow_name_known_primitives() {
        assert_eq!(arrow_name_to_datatype("int32"), Some(DataType::Int32));
        assert_eq!(arrow_name_to_datatype("int64"), Some(DataType::Int64));
        assert_eq!(arrow_name_to_datatype("float32"), Some(DataType::Float32));
        assert_eq!(arrow_name_to_datatype("float64"), Some(DataType::Float64));
        assert_eq!(arrow_name_to_datatype("boolean"), Some(DataType::Boolean));
        assert_eq!(arrow_name_to_datatype("utf8"), Some(DataType::Utf8));
        assert_eq!(arrow_name_to_datatype("binary"), Some(DataType::Binary));
        assert_eq!(
            arrow_name_to_datatype("largebinary"),
            Some(DataType::LargeBinary)
        );
        assert_eq!(arrow_name_to_datatype("date64"), Some(DataType::Date64));
        assert_eq!(
            arrow_name_to_datatype("timestamp_ms"),
            Some(DataType::Timestamp(
                arrow_schema::TimeUnit::Millisecond,
                None
            ))
        );
    }

    #[test]
    fn arrow_name_unknown_returns_none() {
        assert_eq!(arrow_name_to_datatype("super_int"), None);
        assert_eq!(arrow_name_to_datatype(""), None);
    }
}
