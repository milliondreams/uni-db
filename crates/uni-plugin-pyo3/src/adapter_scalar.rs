//! Scalar function adapter — turns a Python callable into an
//! [`uni_plugin::traits::scalar::ScalarPluginFn`].
//!
//! Two modes (proposal §5.4):
//!
//! - **Vectorized** (`vectorized: true`): one [`Python::attach`] per
//!   batch; the user fn receives one PyArrow Array per input column
//!   and returns one PyArrow Array. Marshalling is zero-copy via the
//!   Arrow PyCapsule Interface ([`crate::arrow_bridge`]).
//!
//! - **Row-by-row** (`vectorized: false`): one [`Python::attach`] per
//!   batch (we hold the GIL across all rows in the batch — design
//!   decision in `plans/magical-rolling-pinwheel.md` §design #6);
//!   inside the closure the host iterates rows and calls the Python
//!   fn once per row with native Python scalar args.
//!
//! Both modes serialize on the global GIL — see proposal §5.4.1 for
//! the operational discussion.

#![cfg(feature = "pyo3")]

use std::sync::Arc;

use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use datafusion::logical_expr::ColumnarValue;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyTuple};
use smol_str::SmolStr;

use uni_plugin::errors::FnError;
use uni_plugin::traits::scalar::{ArgType, FnSignature, NullHandling, ScalarPluginFn};

use crate::adapter_scalar_helpers::{PrimitiveColumnBuilder, classify_pyerr, scalar_to_py};
use crate::arrow_bridge::{arrow_array_to_pyarrow, assert_array_datatype, pyarrow_to_arrow_array};
use crate::runtime::PyPluginRuntime;

/// Scalar plugin adapter dispatching to a Python callable held in
/// [`PyPluginRuntime`].
#[derive(Debug)]
pub struct PyScalarFn {
    runtime: Arc<PyPluginRuntime>,
    local_name: SmolStr,
    signature: FnSignature,
    vectorized: bool,
}

impl PyScalarFn {
    /// Construct a row-mode scalar adapter.
    #[must_use]
    pub fn new(
        runtime: Arc<PyPluginRuntime>,
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

    /// Construct a vectorized scalar adapter — Python sees PyArrow
    /// Arrays per column and returns a PyArrow Array.
    #[must_use]
    pub fn new_vectorized(
        runtime: Arc<PyPluginRuntime>,
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

    fn return_datatype(&self) -> Result<DataType, FnError> {
        match &self.signature.returns {
            ArgType::Primitive(dt) => Ok(dt.clone()),
            other => Err(FnError::new(
                0x80,
                format!("PyO3 scalar adapter only supports primitive returns, got {other:?}"),
            )),
        }
    }

    fn arg_datatype(&self, i: usize) -> Result<DataType, FnError> {
        match self.signature.args.get(i) {
            Some(ArgType::Primitive(dt)) => Ok(dt.clone()),
            Some(other) => Err(FnError::new(
                0x80,
                format!("PyO3 scalar arg {i}: only primitives supported, got {other:?}"),
            )),
            None => Err(FnError::new(0x80, format!("missing arg type at index {i}"))),
        }
    }

    fn lookup_callable(&self) -> Result<Py<PyAny>, FnError> {
        self.runtime.get(self.local_name.as_str()).ok_or_else(|| {
            FnError::new(
                0x82,
                format!(
                    "python callable `{}` not in runtime `{}`",
                    self.local_name,
                    self.runtime.plugin_id.as_str()
                ),
            )
        })
    }
}

impl ScalarPluginFn for PyScalarFn {
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

impl PyScalarFn {
    fn invoke_vectorized(
        &self,
        args: &[ColumnarValue],
        rows: usize,
    ) -> Result<ColumnarValue, FnError> {
        let callable = self.lookup_callable()?;
        let ret_ty = self.return_datatype()?;
        let arr_args = materialize_args(args, rows)?;

        let local_name = self.local_name.clone();
        let result_arr = Python::attach(|py| -> Result<ArrayRef, FnError> {
            let mut py_args: Vec<Bound<'_, PyAny>> = Vec::with_capacity(arr_args.len());
            for arr in &arr_args {
                let py_arr = arrow_array_to_pyarrow(py, arr.as_ref())
                    .map_err(|e| FnError::new(0x83, e.to_string()))?;
                py_args.push(py_arr);
            }
            let bound = callable.bind(py);
            let tuple = PyTuple::new(py, py_args)
                .map_err(|e| classify_pyerr(0x820, "", local_name.as_str(), e))?;
            let result = bound
                .call1(tuple)
                .map_err(|e| classify_pyerr(0x820, "", local_name.as_str(), e))?;
            let array = pyarrow_to_arrow_array(py, &result)
                .map_err(|e| FnError::new(0x84, e.to_string()))?;
            assert_array_datatype(array.as_ref(), &ret_ty)
                .map_err(|e| FnError::new(0x85, e.to_string()))?;
            if array.len() != rows {
                return Err(FnError::new(
                    0x86,
                    format!(
                        "PyO3 vectorized `{}` returned {} rows, expected {}",
                        local_name,
                        array.len(),
                        rows
                    ),
                ));
            }
            Ok(array)
        })?;
        Ok(ColumnarValue::Array(result_arr))
    }

    fn invoke_row(&self, args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue, FnError> {
        let callable = self.lookup_callable()?;
        let ret_ty = self.return_datatype()?;
        let arr_args = materialize_args(args, rows)?;
        let arg_dts: Vec<DataType> = (0..args.len())
            .map(|i| self.arg_datatype(i))
            .collect::<Result<_, FnError>>()?;
        let null_handling = self.signature.null_handling;
        let local_name = self.local_name.clone();

        Python::attach(|py| -> Result<ColumnarValue, FnError> {
            let bound = callable.bind(py);
            let mut out =
                PrimitiveColumnBuilder::new(&ret_ty, rows, 0x83, "PyO3 row-mode: return type")?;
            for row in 0..rows {
                // Build per-row Python args. Propagate NULLs by short-
                // circuiting to null output when any arg is null and
                // `PropagateNulls` is set.
                let mut py_args: Vec<Bound<'_, PyAny>> = Vec::with_capacity(args.len());
                let mut short_circuit = false;
                for (i, arr) in arr_args.iter().enumerate() {
                    if arr.is_null(row) {
                        match null_handling {
                            NullHandling::PropagateNulls => {
                                short_circuit = true;
                                break;
                            }
                            NullHandling::UserHandled => {
                                py_args.push(py.None().into_bound(py));
                            }
                        }
                    } else {
                        let v = scalar_to_py(py, arr.as_ref(), row, &arg_dts[i])?;
                        py_args.push(v);
                    }
                }
                if short_circuit {
                    out.push_null();
                    continue;
                }
                let tuple = PyTuple::new(py, py_args)
                    .map_err(|e| classify_pyerr(0x820, "", local_name.as_str(), e))?;
                let result = bound
                    .call1(tuple)
                    .map_err(|e| classify_pyerr(0x820, "", local_name.as_str(), e))?;
                out.push_py(&result, 0x820, "", &local_name)?;
            }
            Ok(ColumnarValue::Array(out.finish()))
        })
    }
}

/// Materialize each ColumnarValue as an `ArrayRef` of length `rows`.
fn materialize_args(args: &[ColumnarValue], rows: usize) -> Result<Vec<ArrayRef>, FnError> {
    args.iter()
        .map(|cv| match cv {
            ColumnarValue::Array(a) => Ok(a.clone()),
            ColumnarValue::Scalar(s) => s
                .to_array_of_size(rows)
                .map_err(|e| FnError::new(0x83, format!("scalar→array: {e}"))),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Float64Array;
    use datafusion::logical_expr::Volatility;
    use std::ffi::CString;

    use uni_plugin::PluginId;

    fn ensure_python() -> bool {
        Python::initialize();
        true
    }

    fn runtime_with_python_fn(name: &str, body: &str) -> Arc<PyPluginRuntime> {
        let rt = PyPluginRuntime::new(PluginId::new("ai.test.pyo3"));
        Python::attach(|py| {
            let code = CString::new(format!("def {name}{body}\n")).unwrap();
            let module = pyo3::types::PyModule::from_code(
                py,
                code.as_c_str(),
                std::ffi::CString::new("test_module.py").unwrap().as_c_str(),
                std::ffi::CString::new("test_module").unwrap().as_c_str(),
            )
            .expect("module compiles");
            let fn_obj = module.getattr(name).expect("function defined").unbind();
            rt.insert(name, fn_obj);
        });
        rt
    }

    fn float_sig(args: usize) -> FnSignature {
        FnSignature::new(
            (0..args)
                .map(|_| ArgType::Primitive(DataType::Float64))
                .collect(),
            ArgType::Primitive(DataType::Float64),
            Volatility::Immutable,
        )
    }

    #[test]
    fn scalar_vec_two_floats_add() {
        if !ensure_python() {
            return;
        }
        let rt = runtime_with_python_fn(
            "add",
            "(x, y):\n    import pyarrow.compute as pc\n    return pc.add(x, y)",
        );
        let adapter = PyScalarFn::new_vectorized(rt, "add", float_sig(2));
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![10.0_f64, 20.0, 30.0]));
        let out = adapter
            .invoke(&[ColumnarValue::Array(a), ColumnarValue::Array(b)], 3)
            .expect("invoke");
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let f = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((f.value(0) - 11.0).abs() < 1e-12);
        assert!((f.value(2) - 33.0).abs() < 1e-12);
    }

    #[test]
    fn scalar_row_two_floats_add() {
        if !ensure_python() {
            return;
        }
        let rt = runtime_with_python_fn("add_row", "(x, y):\n    return x + y");
        let adapter = PyScalarFn::new(rt, "add_row", float_sig(2));
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![0.5_f64, 0.25, 0.125]));
        let out = adapter
            .invoke(&[ColumnarValue::Array(a), ColumnarValue::Array(b)], 3)
            .expect("invoke");
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let f = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((f.value(0) - 1.5).abs() < 1e-12);
        assert!((f.value(1) - 2.25).abs() < 1e-12);
        assert!((f.value(2) - 3.125).abs() < 1e-12);
    }

    #[test]
    fn scalar_row_propagates_nulls() {
        if !ensure_python() {
            return;
        }
        let rt = runtime_with_python_fn("noop", "(x):\n    return x * 2.0");
        let adapter = PyScalarFn::new(
            rt,
            "noop",
            FnSignature::new(
                vec![ArgType::Primitive(DataType::Float64)],
                ArgType::Primitive(DataType::Float64),
                Volatility::Immutable,
            ),
        );
        let a: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)]));
        let out = adapter
            .invoke(&[ColumnarValue::Array(a)], 3)
            .expect("invoke");
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let f = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(!f.is_null(0));
        assert!(f.is_null(1));
        assert!(!f.is_null(2));
        assert!((f.value(2) - 6.0).abs() < 1e-12);
    }

    #[test]
    fn scalar_python_exception_maps_to_fnerror() {
        if !ensure_python() {
            return;
        }
        let rt = runtime_with_python_fn("boom", "(x):\n    raise ValueError('nope: ' + str(x))");
        let adapter = PyScalarFn::new(
            rt,
            "boom",
            FnSignature::new(
                vec![ArgType::Primitive(DataType::Float64)],
                ArgType::Primitive(DataType::Float64),
                Volatility::Immutable,
            ),
        );
        let a: ArrayRef = Arc::new(Float64Array::from(vec![42.0_f64]));
        let err = adapter.invoke(&[ColumnarValue::Array(a)], 1).unwrap_err();
        let msg = err.message;
        assert!(msg.contains("ValueError"), "unexpected msg: {msg}");
        assert!(msg.contains("nope"));
    }

    #[test]
    fn scalar_vec_returns_wrong_length_errors() {
        if !ensure_python() {
            return;
        }
        // Python returns a single-element array — adapter must surface as FnError.
        let rt = runtime_with_python_fn(
            "shrink",
            "(x):\n    import pyarrow as pa\n    return pa.array([99.0])",
        );
        let adapter = PyScalarFn::new_vectorized(rt, "shrink", float_sig(1));
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0]));
        let err = adapter.invoke(&[ColumnarValue::Array(a)], 3).unwrap_err();
        assert!(err.message.contains("returned 1 rows, expected 3"));
    }

    #[test]
    fn missing_callable_errors() {
        if !ensure_python() {
            return;
        }
        let rt = PyPluginRuntime::new(PluginId::new("ai.test.empty"));
        let adapter = PyScalarFn::new(rt, "nope", float_sig(1));
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1.0_f64]));
        let err = adapter.invoke(&[ColumnarValue::Array(a)], 1).unwrap_err();
        assert!(err.message.contains("`nope` not in runtime"));
    }
}
