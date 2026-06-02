//! Scalar function adapter — turns a Rhai callable into an
//! [`uni_plugin::traits::scalar::ScalarPluginFn`].

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, StringArray};
use arrow_schema::DataType;
use datafusion::logical_expr::ColumnarValue;
use rhai::{Dynamic, Scope};
use smol_str::SmolStr;

use uni_plugin::errors::FnError;
use uni_plugin::traits::scalar::{ArgType, FnSignature, ScalarPluginFn};

use crate::columns::{Float64Column, Int64Column, MutableFloat64Column, Utf8Column};
use crate::dynamic_bridge::{OutBuilder, column_row_to_dynamic};
use crate::runtime::RhaiPluginRuntime;

/// Per-row Rhai scalar function adapter.
///
/// Holds a reference to the shared [`RhaiPluginRuntime`] (Engine + AST)
/// and the callable's local name. Each `invoke` walks the input batch
/// row-by-row, calls the Rhai fn via `Engine::call_fn`, and writes the
/// returned `Dynamic` into an output Arrow builder.
#[derive(Debug)]
pub struct RhaiScalarFn {
    runtime: Arc<RhaiPluginRuntime>,
    local_name: SmolStr,
    signature: FnSignature,
    vectorized: bool,
}

impl RhaiScalarFn {
    /// Construct a scalar adapter binding `local_name` against the
    /// shared runtime. Defaults to row mode.
    #[must_use]
    pub fn new(
        runtime: Arc<RhaiPluginRuntime>,
        local_name: impl Into<SmolStr>,
        signature: FnSignature,
    ) -> Self {
        Self {
            runtime,
            local_name: local_name.into(),
            signature,
            vectorized: false,
        }
    }

    /// Construct a vectorized scalar adapter. The script's function
    /// takes one column-userdata argument per declared input and
    /// returns a `MutableFloat64Column` (v1 supports `Float64` returns
    /// only for vectorized mode).
    #[must_use]
    pub fn new_vectorized(
        runtime: Arc<RhaiPluginRuntime>,
        local_name: impl Into<SmolStr>,
        signature: FnSignature,
    ) -> Self {
        Self {
            runtime,
            local_name: local_name.into(),
            signature,
            vectorized: true,
        }
    }

    fn return_datatype(&self) -> Result<&DataType, FnError> {
        match &self.signature.returns {
            ArgType::Primitive(dt) => Ok(dt),
            other => Err(FnError::new(
                0x10,
                format!("Rhai scalar adapter only supports primitive returns, got {other:?}"),
            )),
        }
    }

    fn arg_datatype(&self, i: usize) -> Result<&DataType, FnError> {
        match self.signature.args.get(i) {
            Some(ArgType::Primitive(dt)) => Ok(dt),
            Some(other) => Err(FnError::new(
                0x10,
                format!("Rhai scalar arg {i}: only primitives supported, got {other:?}"),
            )),
            None => Err(FnError::new(0x10, format!("missing arg type at index {i}"))),
        }
    }
}

impl ScalarPluginFn for RhaiScalarFn {
    fn signature(&self) -> &FnSignature {
        &self.signature
    }

    fn invoke(&self, args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue, FnError> {
        if self.vectorized {
            self.invoke_vectorized(args, rows)
        } else {
            self.invoke_row(args, rows)
        }
    }
}

impl RhaiScalarFn {
    fn invoke_row(&self, args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue, FnError> {
        let ret_ty = self.return_datatype()?.clone();
        let mut builder =
            OutBuilder::new(&ret_ty, rows).map_err(|e| FnError::new(0x11, e.to_string()))?;

        // Per-row dispatch. Engine + AST are Send+Sync via Rhai's `sync`
        // feature so multiple DataFusion partitions can share the Arc
        // without locking.
        for row in 0..rows {
            let mut dyn_args: Vec<Dynamic> = Vec::with_capacity(args.len());
            for (i, arg) in args.iter().enumerate() {
                let dt = self.arg_datatype(i)?;
                let d = column_row_to_dynamic(arg, row, dt)
                    .map_err(|e| FnError::new(0x12, e.to_string()))?;
                dyn_args.push(d);
            }

            // Call the Rhai function with a fresh per-call Scope.
            let mut scope = Scope::new();
            let result: Dynamic = self
                .runtime
                .engine
                .call_fn(
                    &mut scope,
                    &self.runtime.ast,
                    self.local_name.as_str(),
                    dyn_args,
                )
                .map_err(|e| classify_rhai_error(&self.local_name, &e))?;

            builder
                .push(result)
                .map_err(|e| FnError::new(0x14, e.to_string()))?;
        }

        Ok(ColumnarValue::Array(builder.finish()))
    }

    fn invoke_vectorized(
        &self,
        args: &[ColumnarValue],
        _rows: usize,
    ) -> Result<ColumnarValue, FnError> {
        // Materialize each ColumnarValue::Array as a column-userdata
        // wrapper matching its declared type.
        let mut dyn_args: Vec<Dynamic> = Vec::with_capacity(args.len());
        for (i, arg) in args.iter().enumerate() {
            let dt = self.arg_datatype(i)?;
            let arr = match arg {
                ColumnarValue::Array(a) => a.clone(),
                ColumnarValue::Scalar(_) => {
                    return Err(FnError::new(
                        0x10,
                        format!("Rhai vectorized: arg {i} must be an Array column, not Scalar"),
                    ));
                }
            };
            let d: Dynamic = match dt {
                DataType::Float64 => {
                    let a = arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                        FnError::new(0x12, format!("vectorized arg {i}: expected Float64Array"))
                    })?;
                    Dynamic::from(Float64Column::new(Arc::new(a.clone())))
                }
                DataType::Int64 => {
                    let a = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        FnError::new(0x12, format!("vectorized arg {i}: expected Int64Array"))
                    })?;
                    Dynamic::from(Int64Column::new(Arc::new(a.clone())))
                }
                DataType::Utf8 => {
                    let a = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                        FnError::new(0x12, format!("vectorized arg {i}: expected StringArray"))
                    })?;
                    Dynamic::from(Utf8Column::new(Arc::new(a.clone())))
                }
                other => {
                    return Err(FnError::new(
                        0x12,
                        format!("vectorized mode only supports Float64/Int64/Utf8, got {other:?}"),
                    ));
                }
            };
            dyn_args.push(d);
        }

        // Call the script's vectorized fn once.
        let mut scope = Scope::new();
        let result: Dynamic = self
            .runtime
            .engine
            .call_fn(
                &mut scope,
                &self.runtime.ast,
                self.local_name.as_str(),
                dyn_args,
            )
            .map_err(|e| classify_rhai_error(&self.local_name, &e))?;

        // v1 only supports MutableFloat64Column as the return shape.
        let out_col: MutableFloat64Column = result.try_cast().ok_or_else(|| {
            FnError::new(
                0x13,
                format!(
                    "vectorized `{}` must return a MutableFloat64Column (uni_float_column allocator)",
                    self.local_name
                ),
            )
        })?;
        let arr = out_col.freeze();
        Ok(ColumnarValue::Array(arr))
    }
}

/// Map a `rhai::EvalAltResult` into a `FnError` with a code in the
/// `0x71` family (used elsewhere in the framework for plugin-side
/// failures).
fn classify_rhai_error(local: &str, e: &rhai::EvalAltResult) -> FnError {
    use rhai::EvalAltResult as E;
    let code = match e {
        E::ErrorTooManyOperations(_) | E::ErrorTooManyModules(_) => 0x711,
        E::ErrorStackOverflow(_) => 0x712,
        E::ErrorDataTooLarge(..) => 0x713,
        E::ErrorTerminated(..) => 0x714,
        E::ErrorFunctionNotFound(..) => 0x715,
        _ => 0x710,
    };
    // All Rhai runtime failures are non-retryable (the default for `FnError`).
    FnError::new(code, format!("Rhai `{local}`: {e}"))
}
