// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Adapter bridging legacy `CustomScalarFn` closures to `uni-plugin`'s
//! `ScalarPluginFn` trait.
//!
//! M2's facade keeps the public `CustomFunctionRegistry::register` API
//! intact while routing registrations into a shadow `PluginRegistry`. This
//! adapter is the bridge: it wraps a `Fn(&[Value]) -> Result<Value>`
//! closure into a type implementing `ScalarPluginFn` so it can live in the
//! plugin registry.
//!
//! As subsequent M2 commits migrate built-ins to native Arrow signatures
//! (`ArgType::Primitive`), this row-per-call adapter remains as the slow
//! path for legacy registrations declaring `ArgType::CypherValue`.

use std::sync::{Arc, OnceLock};

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, LargeBinaryArray, StringArray};
use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use uni_common::Value;
use uni_plugin::FnError;
use uni_plugin::traits::scalar::{ArgType, FnSignature, NullHandling, ScalarPluginFn};

use uni_query_functions::custom_functions::CustomScalarFn;

/// `ScalarPluginFn` impl that wraps a legacy `Fn(&[Value]) -> Result<Value>`
/// closure.
///
/// Used by `CustomFunctionRegistry` to populate its shadow `PluginRegistry`.
/// Iterates rows, converts each row's columns to `Value`s, invokes the
/// closure, and collects results into a `LargeBinary` column (the legacy
/// CypherValue transport).
///
/// This is the *slow path* — primitive-typed UDFs will go through a
/// `NativeArrowUdf` (M2 follow-up) that skips the per-row `Value`
/// round-trip entirely.
pub struct ValueRowFn {
    name: String,
    signature: OnceLock<FnSignature>,
    inner: CustomScalarFn,
}

impl ValueRowFn {
    /// Wrap a legacy closure into a plugin-compatible scalar function.
    #[must_use]
    pub fn new(name: impl Into<String>, inner: CustomScalarFn) -> Self {
        Self {
            name: name.into(),
            signature: OnceLock::new(),
            inner,
        }
    }
}

impl std::fmt::Debug for ValueRowFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueRowFn")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl ScalarPluginFn for ValueRowFn {
    fn signature(&self) -> &FnSignature {
        // The legacy registry has no signature metadata; we synthesize a
        // generic `CypherValue → CypherValue` signature that goes through
        // the LargeBinary transport on the DataFusion side.
        self.signature.get_or_init(|| FnSignature {
            // Variadic CypherValue input (the legacy closure shape never
            // declared arities).
            args: vec![ArgType::Variadic(Box::new(ArgType::CypherValue))],
            returns: ArgType::CypherValue,
            volatility: Volatility::Volatile,
            null_handling: NullHandling::UserHandled,
        })
    }

    fn invoke(&self, args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue, FnError> {
        // Materialize each ColumnarValue into a row-major Vec<Vec<Value>>.
        let materialized: Vec<Vec<Value>> = args
            .iter()
            .map(|c| columnar_to_values(c, rows))
            .collect::<Result<Vec<_>, _>>()?;

        let mut out_values: Vec<Value> = Vec::with_capacity(rows);
        for row in 0..rows {
            let mut row_args: Vec<Value> = Vec::with_capacity(materialized.len());
            for col in &materialized {
                row_args.push(col[row].clone());
            }
            let v = (self.inner)(&row_args).map_err(|e| {
                FnError::new(
                    0x1000,
                    format!("legacy scalar fn `{}` failed: {e}", self.name),
                )
            })?;
            out_values.push(v);
        }

        // Serialize as LargeBinary (the legacy CypherValue transport).
        values_to_large_binary(&out_values)
    }
}

/// Convert a [`ColumnarValue`] to a `Vec<Value>` of length `rows`.
fn columnar_to_values(c: &ColumnarValue, rows: usize) -> Result<Vec<Value>, FnError> {
    match c {
        ColumnarValue::Scalar(s) => {
            let v = scalar_to_value(s);
            Ok(vec![v; rows])
        }
        ColumnarValue::Array(arr) => array_to_values(arr.as_ref()),
    }
}

fn scalar_to_value(s: &datafusion::scalar::ScalarValue) -> Value {
    use datafusion::scalar::ScalarValue;
    match s {
        ScalarValue::Null => Value::Null,
        ScalarValue::Boolean(Some(b)) => Value::Bool(*b),
        ScalarValue::Boolean(None) => Value::Null,
        ScalarValue::Int64(Some(i)) => Value::Int(*i),
        ScalarValue::Int64(None) => Value::Null,
        ScalarValue::Float64(Some(f)) => Value::Float(*f),
        ScalarValue::Float64(None) => Value::Null,
        ScalarValue::Utf8(Some(s)) => Value::String(s.clone()),
        ScalarValue::Utf8(None) => Value::Null,
        ScalarValue::LargeBinary(Some(bytes)) => decode_cypher_value(bytes).unwrap_or(Value::Null),
        ScalarValue::LargeBinary(None) => Value::Null,
        // Other types: fall back to displaying as a String so the closure
        // sees something coherent. A future commit narrows this once the
        // legacy adapter is purely a transitional code path.
        _ => Value::String(s.to_string()),
    }
}

fn array_to_values(arr: &dyn Array) -> Result<Vec<Value>, FnError> {
    let n = arr.len();
    let mut out = Vec::with_capacity(n);

    match arr.data_type() {
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                FnError::new(FnError::CODE_TYPE_COERCION, "expected BooleanArray")
            })?;
            for i in 0..n {
                out.push(if a.is_null(i) {
                    Value::Null
                } else {
                    Value::Bool(a.value(i))
                });
            }
        }
        DataType::Int64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| FnError::new(FnError::CODE_TYPE_COERCION, "expected Int64Array"))?;
            for i in 0..n {
                out.push(if a.is_null(i) {
                    Value::Null
                } else {
                    Value::Int(a.value(i))
                });
            }
        }
        DataType::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                FnError::new(FnError::CODE_TYPE_COERCION, "expected Float64Array")
            })?;
            for i in 0..n {
                out.push(if a.is_null(i) {
                    Value::Null
                } else {
                    Value::Float(a.value(i))
                });
            }
        }
        DataType::Utf8 => {
            let a = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| FnError::new(FnError::CODE_TYPE_COERCION, "expected StringArray"))?;
            for i in 0..n {
                out.push(if a.is_null(i) {
                    Value::Null
                } else {
                    Value::String(a.value(i).to_owned())
                });
            }
        }
        DataType::LargeBinary => {
            let a = arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    FnError::new(FnError::CODE_TYPE_COERCION, "expected LargeBinaryArray")
                })?;
            for i in 0..n {
                out.push(if a.is_null(i) {
                    Value::Null
                } else {
                    decode_cypher_value(a.value(i)).unwrap_or(Value::Null)
                });
            }
        }
        other => {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("unsupported arrow type in legacy adapter: {other:?}"),
            ));
        }
    }

    Ok(out)
}

fn values_to_large_binary(values: &[Value]) -> Result<ColumnarValue, FnError> {
    let mut builder = arrow_array::builder::LargeBinaryBuilder::with_capacity(values.len(), 0);
    for v in values {
        match v {
            Value::Null => builder.append_null(),
            _ => {
                let bytes = encode_cypher_value(v)?;
                builder.append_value(&bytes);
            }
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn encode_cypher_value(v: &Value) -> Result<Vec<u8>, FnError> {
    // Use the canonical tagged codec — the same encoding every other
    // consumer in `uni-query` reads via `cypher_value_codec::decode`
    // (`scan.rs`, `apply.rs`, `df_expr.rs`, `similar_to_expr.rs`).
    // Previously this was `serde_json::to_vec(v)` which produced raw
    // textual bytes that downstream readers misinterpreted as tag
    // bytes (e.g. for `Value::Int(42)` the first byte was ASCII '4' =
    // 0x34 = 52, surfacing as "unknown CypherValue tag: 52").
    Ok(uni_common::cypher_value_codec::encode(v))
}

fn decode_cypher_value(bytes: &[u8]) -> Option<Value> {
    uni_common::cypher_value_codec::decode(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_common::Value;

    #[test]
    fn value_row_fn_invokes_closure_for_each_row() {
        let closure: CustomScalarFn = Arc::new(|args: &[Value]| {
            // double the first int
            match args.first() {
                Some(Value::Int(i)) => Ok(Value::Int(i * 2)),
                _ => Ok(Value::Null),
            }
        });
        let f = ValueRowFn::new("double", closure);
        let input =
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as Arc<dyn Array>);
        let out = f.invoke(&[input], 3).expect("invoke");
        // Output is LargeBinary; decode each value.
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array output"),
        };
        let lb = arr
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("LargeBinaryArray");
        let vs: Vec<Value> = (0..lb.len())
            .map(|i| decode_cypher_value(lb.value(i)).unwrap())
            .collect();
        assert_eq!(vs, vec![Value::Int(2), Value::Int(4), Value::Int(6)]);
    }

    #[test]
    fn value_row_fn_handles_nulls() {
        let closure: CustomScalarFn =
            Arc::new(|args: &[Value]| Ok(args.first().cloned().unwrap_or(Value::Null)));
        let f = ValueRowFn::new("identity", closure);
        let input = ColumnarValue::Array(
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as Arc<dyn Array>
        );
        let out = f.invoke(&[input], 3).expect("invoke");
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        let lb = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert!(!lb.is_null(0));
        assert!(lb.is_null(1));
        assert!(!lb.is_null(2));
    }
}
