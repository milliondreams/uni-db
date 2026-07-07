//! Aggregate function adapter — Rhai-side aggregate via four named fns.
//!
//! Rhai aggregates are declared in the manifest as a single `name`. The
//! script must export four functions following the naming convention:
//!
//! - `${name}_init()` — returns the initial state (typically a map).
//! - `${name}_accumulate(state, x)` — returns the updated state.
//! - `${name}_merge(state_a, state_b)` — returns the merged state.
//! - `${name}_finalize(state)` — returns the final aggregate value.
//!
//! This four-callable shape avoids the complexity of invoking Rhai
//! closures stored inside `const` maps; the trade-off is that authors
//! cannot inline-define an aggregate, but the wiring is straightforward
//! and survives the script's parse-time check.

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, LargeBinaryArray};
use arrow_schema::{DataType, Field};
use datafusion::scalar::ScalarValue;
use rhai::{Dynamic, Scope};
use smol_str::SmolStr;

use uni_plugin::errors::FnError;
use uni_plugin::traits::aggregate::{AggSignature, AggregatePluginFn, PluginAccumulator};
use uni_plugin::traits::scalar::ArgType;

use crate::dynamic_bridge::array_row_to_dynamic;
use crate::runtime::RhaiPluginRuntime;

/// Aggregate fn adapter — implements `AggregatePluginFn` by dispatching
/// to four Rhai callables.
#[derive(Debug)]
pub struct RhaiAggregateFn {
    runtime: Arc<RhaiPluginRuntime>,
    name: SmolStr,
    signature: AggSignature,
}

impl RhaiAggregateFn {
    /// Construct an aggregate adapter for `name`. The Rhai script must
    /// export `${name}_init`, `${name}_accumulate`, `${name}_merge`,
    /// `${name}_finalize`.
    #[must_use]
    pub fn new(
        runtime: Arc<RhaiPluginRuntime>,
        name: impl Into<SmolStr>,
        signature: AggSignature,
    ) -> Self {
        Self {
            runtime,
            name: name.into(),
            signature,
        }
    }
}

impl AggregatePluginFn for RhaiAggregateFn {
    fn signature(&self) -> &AggSignature {
        &self.signature
    }

    fn create_accumulator(&self) -> Box<dyn PluginAccumulator> {
        // Initialise state from `${name}_init()`. The previous form used
        // `.unwrap_or(Dynamic::UNIT)`, which silently substituted UNIT
        // for any init failure (missing function, panic, type error) and
        // then corrupted every downstream call. We now capture the init
        // error and surface it on the first call to any trait method.
        let mut scope = Scope::new();
        let init_fn = format!("{}_init", self.name);
        let (state, init_error) = match self.runtime.engine.call_fn::<Dynamic>(
            &mut scope,
            &self.runtime.ast,
            &init_fn,
            (),
        ) {
            Ok(s) => (s, None),
            Err(e) => (
                Dynamic::UNIT,
                Some(FnError::new(
                    0x723,
                    format!("Rhai aggregate `{}` init failed: {e}", self.name),
                )),
            ),
        };
        Box::new(RhaiAccumulator {
            runtime: Arc::clone(&self.runtime),
            name: self.name.clone(),
            state,
            input_types: self.signature.args.clone(),
            return_type: self.signature.returns.clone(),
            init_error,
        })
    }
}

/// Per-group accumulator backed by a `rhai::Dynamic` state value.
pub struct RhaiAccumulator {
    runtime: Arc<RhaiPluginRuntime>,
    name: SmolStr,
    state: Dynamic,
    input_types: Vec<ArgType>,
    /// Declared aggregate return type. `evaluate` coerces the finalize
    /// result to this type so the emitted `ScalarValue` matches the
    /// schema the UDAF adapter advertised via `return_type()`.
    return_type: ArgType,
    /// Set when `${name}_init` failed at construction. Every trait
    /// method short-circuits with this error so the accumulator can't
    /// silently produce garbage state.
    init_error: Option<FnError>,
}

impl std::fmt::Debug for RhaiAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RhaiAccumulator")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl RhaiAccumulator {
    /// Surface a cached init failure to any trait method. Cloning the
    /// `FnError` lets us keep the original so subsequent calls also
    /// fail (rather than succeeding on the second call once the error
    /// is taken).
    fn check_init(&self) -> Result<(), FnError> {
        match &self.init_error {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
}

impl PluginAccumulator for RhaiAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), FnError> {
        self.check_init()?;
        let accumulate_fn = format!("{}_accumulate", self.name);
        let n = values.first().map_or(0, |a| a.len());

        for row in 0..n {
            let mut dyn_args: Vec<Dynamic> = Vec::with_capacity(values.len() + 1);
            dyn_args.push(self.state.clone());
            for (i, arr) in values.iter().enumerate() {
                let dt = primitive_datatype(&self.input_types, i)?;
                let d = array_row_to_dynamic(arr, row, &dt)
                    .map_err(|e| FnError::new(0x12, e.to_string()))?;
                dyn_args.push(d);
            }
            let mut scope = Scope::new();
            let new_state = self
                .runtime
                .engine
                .call_fn::<Dynamic>(&mut scope, &self.runtime.ast, &accumulate_fn, dyn_args)
                .map_err(|e| FnError::new(0x720, format!("Rhai accumulate: {e}")))?;
            self.state = new_state;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), FnError> {
        self.check_init()?;
        let merge_fn = format!("{}_merge", self.name);
        let Some(state_arr) = states.first() else {
            return Ok(());
        };
        let n = state_arr.len();

        for row in 0..n {
            // Decode peer state bytes from a Binary/LargeBinary column.
            let bytes = peer_state_bytes(state_arr, row)?;
            let peer_state = decode_state(&bytes)?;
            let mut scope = Scope::new();
            let new_state = self
                .runtime
                .engine
                .call_fn::<Dynamic>(
                    &mut scope,
                    &self.runtime.ast,
                    &merge_fn,
                    (self.state.clone(), peer_state),
                )
                .map_err(|e| FnError::new(0x721, format!("Rhai merge: {e}")))?;
            self.state = new_state;
        }
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>, FnError> {
        self.check_init()?;
        let bytes = encode_state(&self.state)?;
        Ok(vec![ScalarValue::LargeBinary(Some(bytes))])
    }

    fn evaluate(&self) -> Result<ScalarValue, FnError> {
        self.check_init()?;
        let finalize_fn = format!("{}_finalize", self.name);
        let mut scope = Scope::new();
        let result = self
            .runtime
            .engine
            .call_fn::<Dynamic>(
                &mut scope,
                &self.runtime.ast,
                &finalize_fn,
                (self.state.clone(),),
            )
            .map_err(|e| FnError::new(0x722, format!("Rhai finalize: {e}")))?;
        dynamic_to_scalar_typed(result, &self.return_type)
    }

    fn size(&self) -> usize {
        // Conservative estimate. Dynamic doesn't expose memory_use().
        std::mem::size_of::<Self>() + 64
    }
}

fn primitive_datatype(args: &[ArgType], i: usize) -> Result<DataType, FnError> {
    match args.get(i) {
        Some(ArgType::Primitive(dt)) => Ok(dt.clone()),
        Some(other) => Err(FnError::new(
            0x10,
            format!("Rhai aggregate arg {i}: primitives only, got {other:?}"),
        )),
        None => Err(FnError::new(0x10, format!("missing arg type {i}"))),
    }
}

fn peer_state_bytes(arr: &ArrayRef, row: usize) -> Result<Vec<u8>, FnError> {
    if arr.is_null(row) {
        return Ok(Vec::new());
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(a.value(row).to_vec());
    }
    if let Some(a) = arr.as_any().downcast_ref::<BinaryArray>() {
        return Ok(a.value(row).to_vec());
    }
    Err(FnError::new(
        0x12,
        format!(
            "Rhai aggregate merge: expected Binary/LargeBinary state column, got {:?}",
            arr.data_type()
        ),
    ))
}

/// Serialize a Rhai aggregate's partial state for shipping to a peer.
///
/// Uses MessagePack rather than JSON: JSON has no token for non-finite
/// floats and silently rewrites `NaN`/`+/-Inf` to `null`, which would erase
/// a peer's partial state during a distributed merge. MessagePack encodes
/// every `f64` as its raw IEEE-754 bit pattern, so non-finite values
/// round-trip through [`decode_state`] intact.
///
/// # Errors
/// Returns [`FnError`] if the `Dynamic` state cannot be serialized.
fn encode_state(state: &Dynamic) -> Result<Vec<u8>, FnError> {
    rmp_serde::to_vec(state).map_err(|e| FnError::new(0x13, format!("Rhai state encode: {e}")))
}

/// Rehydrate a peer partial state produced by [`encode_state`].
///
/// # Errors
/// Returns [`FnError`] if the bytes are not a valid MessagePack `Dynamic`.
fn decode_state(bytes: &[u8]) -> Result<Dynamic, FnError> {
    if bytes.is_empty() {
        return Ok(Dynamic::UNIT);
    }
    rmp_serde::from_slice::<Dynamic>(bytes)
        .map_err(|e| FnError::new(0x13, format!("Rhai state decode: {e}")))
}

/// Convert a `serde_json::Value` into a `rhai::Dynamic`. Used for
/// rehydrating peer states during merge.
pub fn serde_json_to_dynamic(v: &serde_json::Value) -> Result<Dynamic, String> {
    use serde_json::Value as J;
    Ok(match v {
        J::Null => Dynamic::UNIT,
        J::Bool(b) => Dynamic::from(*b),
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                Dynamic::from(i)
            } else if let Some(f) = n.as_f64() {
                Dynamic::from(f)
            } else {
                return Err(format!("unrepresentable number: {n}"));
            }
        }
        J::String(s) => Dynamic::from(s.clone()),
        J::Array(arr) => {
            let mut out: rhai::Array = Vec::with_capacity(arr.len());
            for item in arr {
                out.push(serde_json_to_dynamic(item)?);
            }
            Dynamic::from(out)
        }
        J::Object(obj) => {
            let mut out: rhai::Map = rhai::Map::new();
            for (k, v) in obj {
                out.insert(k.as_str().into(), serde_json_to_dynamic(v)?);
            }
            Dynamic::from(out)
        }
    })
}

fn dynamic_to_scalar_loose(d: Dynamic) -> Result<ScalarValue, FnError> {
    if d.is_unit() {
        return Ok(ScalarValue::Null);
    }
    if let Ok(b) = d.as_bool() {
        return Ok(ScalarValue::Boolean(Some(b)));
    }
    if let Ok(i) = d.as_int() {
        return Ok(ScalarValue::Int64(Some(i)));
    }
    if let Ok(f) = d.as_float() {
        return Ok(ScalarValue::Float64(Some(f)));
    }
    if let Ok(s) = d.clone().into_string() {
        return Ok(ScalarValue::Utf8(Some(s)));
    }
    // Fallback: encode as JSON LargeUtf8 for unsupported composite types.
    let bytes = serde_json::to_string(&d).map_err(|e| FnError::new(0x13, e.to_string()))?;
    Ok(ScalarValue::LargeUtf8(Some(bytes)))
}

/// Coerce a finalize result to the aggregate's declared return type.
///
/// The declared return type — not the runtime value — drives the output so
/// the emitted `ScalarValue` matches the schema the UDAF adapter advertised
/// via `return_type()`. A unit result becomes a typed null of the declared
/// type (not an untyped `ScalarValue::Null`), and numeric results are
/// widened/narrowed to the target primitive (e.g. an `INT` finalize under a
/// `Float64` return becomes `Float64`). A non-primitive declared return
/// falls back to the value-directed [`dynamic_to_scalar_loose`].
///
/// # Errors
/// Returns [`FnError`] if the value cannot be coerced to the declared type.
fn dynamic_to_scalar_typed(d: Dynamic, ret: &ArgType) -> Result<ScalarValue, FnError> {
    let ArgType::Primitive(dt) = ret else {
        return dynamic_to_scalar_loose(d);
    };
    coerce_dynamic_to_datatype(d, dt)
}

/// Build a `ScalarValue` of Arrow type `dt` from a Rhai `Dynamic`.
///
/// # Errors
/// Returns [`FnError`] when a typed null cannot be constructed or the value
/// cannot be coerced into the requested primitive type.
fn coerce_dynamic_to_datatype(d: Dynamic, dt: &DataType) -> Result<ScalarValue, FnError> {
    // A unit finalize maps to a typed null of the declared type rather than
    // an untyped ScalarValue::Null (whose DataType is DataType::Null).
    if d.is_unit() {
        return ScalarValue::try_from(dt)
            .map_err(|e| FnError::new(0x13, format!("Rhai aggregate null of {dt:?}: {e}")));
    }
    // Capture the runtime type name up front so the error builder does not
    // borrow `d` (some match arms move it, e.g. `into_string`).
    let got = d.type_name();
    let type_err = move |want: &str| {
        FnError::new(
            0x13,
            format!("Rhai aggregate finalize: cannot coerce {got} to declared {want}"),
        )
    };
    match dt {
        DataType::Boolean => d.as_bool().map(|b| ScalarValue::Boolean(Some(b))).map_err(|_| type_err("bool")),
        DataType::Int64 => {
            if let Ok(i) = d.as_int() {
                Ok(ScalarValue::Int64(Some(i)))
            } else if let Ok(f) = d.as_float() {
                // Truncate a finite, in-range float; a non-finite or
                // out-of-range float becomes a typed null rather than a
                // silently-saturated `as i64` cast.
                if f.is_finite() && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    Ok(ScalarValue::Int64(Some(f as i64)))
                } else {
                    Ok(ScalarValue::Int64(None))
                }
            } else {
                Err(type_err("int"))
            }
        }
        DataType::Float64 => {
            if let Ok(f) = d.as_float() {
                Ok(ScalarValue::Float64(Some(f)))
            } else if let Ok(i) = d.as_int() {
                Ok(ScalarValue::Float64(Some(i as f64)))
            } else {
                Err(type_err("float"))
            }
        }
        DataType::Utf8 => d
            .into_string()
            .map(|s| ScalarValue::Utf8(Some(s)))
            .map_err(|_| type_err("string")),
        DataType::LargeUtf8 => match d.clone().into_string() {
            // A plain string returns verbatim; a composite value (map/array)
            // is JSON-encoded, matching the aggregate "map"/"object"/"any"
            // return convention in `build_agg_signature`.
            Ok(s) => Ok(ScalarValue::LargeUtf8(Some(s))),
            Err(_) => {
                let bytes = serde_json::to_string(&d).map_err(|e| FnError::new(0x13, e.to_string()))?;
                Ok(ScalarValue::LargeUtf8(Some(bytes)))
            }
        },
        DataType::Null => Ok(ScalarValue::Null),
        // Other declared types: best-effort value-directed conversion.
        _ => dynamic_to_scalar_loose(d),
    }
}

/// Build the standard state-field schema for a Rhai aggregate. v1
/// always serializes the Dynamic state as a single LargeBinary column.
#[must_use]
pub fn rhai_state_fields() -> Vec<Field> {
    vec![Field::new("rhai_state", DataType::LargeBinary, true)]
}

/// Helper to build an `AggSignature` for a Rhai aggregate from wire
/// strings.
pub fn build_agg_signature(
    args: &[String],
    returns: &str,
    determinism: &str,
) -> Result<AggSignature, crate::error::RhaiError> {
    use crate::wire_translate::{determinism_to_volatility, type_name_to_argtype};
    let arg_types: Vec<ArgType> = args
        .iter()
        .map(|s| type_name_to_argtype(s))
        .collect::<Result<_, _>>()?;
    // Return type for aggregates: aggregates often return a map; fall
    // back to LargeUtf8 when the wire-name maps to nothing we can encode
    // as a primitive (e.g. "map").
    let return_type = match returns.trim().to_ascii_lowercase().as_str() {
        "map" | "object" | "any" => ArgType::Primitive(DataType::LargeUtf8),
        _ => type_name_to_argtype(returns)?,
    };
    Ok(AggSignature {
        args: arg_types,
        returns: return_type,
        state_fields: rhai_state_fields(),
        volatility: determinism_to_volatility(determinism),
        supports_partial: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::build_engine;
    use crate::host_fns::RhaiHostFnRegistry;
    use crate::manifest::compile;
    use arrow_array::Float64Array;
    use datafusion::logical_expr::Volatility;
    use uni_plugin::{CapabilitySet, PluginId};

    fn build_runtime(script: &str) -> Arc<RhaiPluginRuntime> {
        let engine = build_engine(&CapabilitySet::new(), &RhaiHostFnRegistry::new());
        let ast = compile(&engine, script).unwrap();
        RhaiPluginRuntime::new(PluginId::new("test.agg"), engine, ast)
    }

    #[test]
    fn stats_aggregate_round_trips() {
        let script = r#"
            fn stats_init() {
                #{ n: 0, sum: 0.0, sum_sq: 0.0 }
            }
            fn stats_accumulate(state, x) {
                state.n += 1;
                state.sum += x;
                state.sum_sq += x * x;
                state
            }
            fn stats_merge(a, b) {
                #{ n: a.n + b.n, sum: a.sum + b.sum, sum_sq: a.sum_sq + b.sum_sq }
            }
            fn stats_finalize(s) {
                if s.n == 0 { return (); }
                s.sum / s.n
            }
        "#;
        let runtime = build_runtime(script);
        let sig = AggSignature {
            args: vec![ArgType::Primitive(DataType::Float64)],
            returns: ArgType::Primitive(DataType::Float64),
            state_fields: rhai_state_fields(),
            volatility: Volatility::Immutable,
            supports_partial: true,
        };
        let agg = RhaiAggregateFn::new(runtime, "stats", sig);
        let mut acc = agg.create_accumulator();
        let xs: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
        acc.update_batch(&[xs]).unwrap();
        let result = acc.evaluate().unwrap();
        match result {
            ScalarValue::Float64(Some(v)) => assert!((v - 2.5).abs() < 1e-9),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn state_serializes_and_merges() {
        let script = r#"
            fn sum_init() { 0.0 }
            fn sum_accumulate(state, x) { state + x }
            fn sum_merge(a, b) { a + b }
            fn sum_finalize(s) { s }
        "#;
        let runtime = build_runtime(script);
        let sig = AggSignature {
            args: vec![ArgType::Primitive(DataType::Float64)],
            returns: ArgType::Primitive(DataType::Float64),
            state_fields: rhai_state_fields(),
            volatility: Volatility::Immutable,
            supports_partial: true,
        };
        let agg = RhaiAggregateFn::new(runtime, "sum", sig);

        // First partition accumulates [1,2,3]; serializes its state.
        let mut a = agg.create_accumulator();
        let xs1: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        a.update_batch(&[xs1]).unwrap();
        let state_vec = a.state().unwrap();
        let state_bytes = match &state_vec[0] {
            ScalarValue::LargeBinary(Some(b)) => b.clone(),
            other => panic!("expected LargeBinary, got {other:?}"),
        };

        // Second partition accumulates [10,20]; merges first's state.
        let mut b = agg.create_accumulator();
        let xs2: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 20.0]));
        b.update_batch(&[xs2]).unwrap();
        let peer_arr: ArrayRef = Arc::new(LargeBinaryArray::from(vec![state_bytes.as_slice()]));
        b.merge_batch(&[peer_arr]).unwrap();
        let result = b.evaluate().unwrap();
        match result {
            ScalarValue::Float64(Some(v)) => assert!((v - 36.0).abs() < 1e-9),
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
