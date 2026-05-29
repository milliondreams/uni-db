//! Aggregate function adapter — turns four Python callables
//! (`init` / `accumulate` / `merge` / `finalize`) into an
//! [`uni_plugin::traits::aggregate::AggregatePluginFn`] +
//! [`uni_plugin::traits::aggregate::PluginAccumulator`].
//!
//! Wire shape mirrors `RhaiAggregateFn` (proposal §15.4) — the
//! difference is the marshalling layer (PyArrow Array per column
//! instead of Rhai column userdata).
//!
//! # Cross-partition merge
//!
//! DataFusion partial aggregation calls `state()` on each partial
//! accumulator, ships the result across the partition boundary as
//! `ScalarValue`s, and replays via `merge_batch` on the final
//! accumulator. PyO3 plugins serialize their state as a **JSON
//! string** (a single-row `Utf8` `ScalarValue`) using Python's
//! standard `json` module — this carries Python primitives / dicts /
//! lists faithfully and avoids the unsafe-deserialize footprint of
//! `pickle`. The receiving accumulator's `merge_batch` calls
//! `json.loads` and then the user's `merge(a, b)` to fuse state.
//!
//! Design decision per `plans/magical-rolling-pinwheel.md` §design #4:
//! the user-supplied `merge` is the merge path — we don't try to
//! mechanically reconstruct state via pickle.

#![cfg(feature = "pyo3")]

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::{DataType, Field};
use datafusion::logical_expr::Volatility;
use datafusion::scalar::ScalarValue;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyTuple};
use smol_str::SmolStr;

use uni_plugin::errors::FnError;
use uni_plugin::traits::aggregate::{AggSignature, AggregatePluginFn, PluginAccumulator};
use uni_plugin::traits::scalar::ArgType;

use crate::runtime::PyPluginRuntime;

const STATE_FIELD_NAME: &str = "_py_state_json";

/// Aggregate plugin adapter dispatching to a Python aggregate spec
/// (four callables) held in [`PyPluginRuntime`].
#[derive(Debug)]
pub struct PyAggregateFn {
    runtime: Arc<PyPluginRuntime>,
    /// Local name of the aggregate (e.g., `"stats"`).
    local_name: SmolStr,
    signature: AggSignature,
}

impl PyAggregateFn {
    /// Construct an aggregate adapter.
    #[must_use]
    pub fn new(
        runtime: Arc<PyPluginRuntime>,
        local_name: impl Into<SmolStr>,
        signature: AggSignature,
    ) -> Self {
        Self {
            runtime,
            local_name: local_name.into(),
            signature,
        }
    }
}

impl AggregatePluginFn for PyAggregateFn {
    fn signature(&self) -> &AggSignature {
        &self.signature
    }

    fn create_accumulator(&self) -> Box<dyn PluginAccumulator> {
        Box::new(PyAccumulator::new(
            Arc::clone(&self.runtime),
            self.local_name.clone(),
            self.signature.clone(),
        ))
    }
}

/// Per-group accumulator backed by Python callables.
///
/// State is held as a `Py<PyAny>` resolved lazily via the user's
/// `init` callable on first use.
pub struct PyAccumulator {
    runtime: Arc<PyPluginRuntime>,
    local_name: SmolStr,
    signature: AggSignature,
    /// Lazily-initialized Python state object.
    state: Option<Py<PyAny>>,
}

impl std::fmt::Debug for PyAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyAccumulator")
            .field("plugin_id", &self.runtime.plugin_id.as_str())
            .field("local_name", &self.local_name)
            .field("state_initialised", &self.state.is_some())
            .finish()
    }
}

impl PyAccumulator {
    fn new(runtime: Arc<PyPluginRuntime>, local_name: SmolStr, signature: AggSignature) -> Self {
        Self {
            runtime,
            local_name,
            signature,
            state: None,
        }
    }

    fn callable(&self, method: &str) -> Result<Py<PyAny>, FnError> {
        let key = format!("{}::{method}", self.local_name);
        self.runtime.get(&key).ok_or_else(|| {
            FnError::new(
                0x820,
                format!(
                    "python aggregate callable `{key}` not in runtime `{}`",
                    self.runtime.plugin_id.as_str()
                ),
            )
        })
    }

    fn ensure_state(&mut self, py: Python<'_>) -> Result<(), FnError> {
        if self.state.is_some() {
            return Ok(());
        }
        let init = self.callable("init")?;
        let bound = init.bind(py);
        let state = bound
            .call0()
            .map_err(|e| classify_pyerr(self.local_name.as_str(), e))?
            .unbind();
        self.state = Some(state);
        Ok(())
    }

    fn input_dt(&self, i: usize) -> Result<DataType, FnError> {
        match self.signature.args.get(i) {
            Some(ArgType::Primitive(dt)) => Ok(dt.clone()),
            Some(other) => Err(FnError::new(
                0x80,
                format!("PyO3 aggregate arg {i}: only primitives supported, got {other:?}"),
            )),
            None => Err(FnError::new(0x80, format!("missing arg type at index {i}"))),
        }
    }
}

impl PluginAccumulator for PyAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), FnError> {
        if values.is_empty() {
            return Ok(());
        }
        let rows = values[0].len();
        if rows == 0 {
            return Ok(());
        }
        let arg_dts: Vec<DataType> = (0..values.len())
            .map(|i| self.input_dt(i))
            .collect::<Result<_, FnError>>()?;
        let accumulate = self.callable("accumulate")?;
        let local_name = self.local_name.clone();

        Python::attach(|py| -> Result<(), FnError> {
            self.ensure_state(py)?;
            let mut state = self
                .state
                .as_ref()
                .expect("ensure_state initialises")
                .clone_ref(py);
            let bound = accumulate.bind(py);
            for row in 0..rows {
                let mut args: Vec<Bound<'_, PyAny>> = Vec::with_capacity(values.len() + 1);
                args.push(state.bind(py).clone());
                for (i, arr) in values.iter().enumerate() {
                    if arr.is_null(row) {
                        // Aggregate sees `None` for nulls — user code
                        // decides how to handle.
                        args.push(py.None().into_bound(py));
                    } else {
                        args.push(crate::adapter_scalar_helpers::scalar_to_py(
                            py,
                            arr.as_ref(),
                            row,
                            &arg_dts[i],
                        )?);
                    }
                }
                let tuple =
                    PyTuple::new(py, args).map_err(|e| classify_pyerr(local_name.as_str(), e))?;
                let result = bound
                    .call1(tuple)
                    .map_err(|e| classify_pyerr(local_name.as_str(), e))?;
                state = result.unbind();
            }
            self.state = Some(state);
            Ok(())
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), FnError> {
        if states.is_empty() {
            return Ok(());
        }
        let state_arr = states[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                FnError::new(
                    0x822,
                    "PyO3 aggregate merge expects Utf8 state column".to_owned(),
                )
            })?;
        let merge = self.callable("merge")?;
        let local_name = self.local_name.clone();

        Python::attach(|py| -> Result<(), FnError> {
            self.ensure_state(py)?;
            let mut state = self
                .state
                .as_ref()
                .expect("ensure_state initialises")
                .clone_ref(py);
            let json_loads = py
                .import("json")
                .map_err(|e| classify_pyerr(local_name.as_str(), e))?
                .getattr("loads")
                .map_err(|e| classify_pyerr(local_name.as_str(), e))?;
            let merge_bound = merge.bind(py);
            for i in 0..state_arr.len() {
                if state_arr.is_null(i) {
                    continue;
                }
                let json_str = state_arr.value(i);
                let other = json_loads
                    .call1((json_str,))
                    .map_err(|e| classify_pyerr(local_name.as_str(), e))?;
                let tuple = PyTuple::new(py, [state.bind(py).clone(), other])
                    .map_err(|e| classify_pyerr(local_name.as_str(), e))?;
                let result = merge_bound
                    .call1(tuple)
                    .map_err(|e| classify_pyerr(local_name.as_str(), e))?;
                state = result.unbind();
            }
            self.state = Some(state);
            Ok(())
        })
    }

    fn state(&self) -> Result<Vec<ScalarValue>, FnError> {
        let local_name = self.local_name.clone();
        Python::attach(|py| -> Result<Vec<ScalarValue>, FnError> {
            let state_obj = match &self.state {
                Some(s) => s.clone_ref(py),
                None => {
                    // Empty accumulator — emit an empty state shape
                    // (`{}`) so the receiving merge_batch is a no-op.
                    return Ok(vec![ScalarValue::Utf8(Some("{}".into()))]);
                }
            };
            let json_dumps = py
                .import("json")
                .map_err(|e| classify_pyerr(local_name.as_str(), e))?
                .getattr("dumps")
                .map_err(|e| classify_pyerr(local_name.as_str(), e))?;
            let s: String = json_dumps
                .call1((state_obj,))
                .map_err(|e| classify_pyerr(local_name.as_str(), e))?
                .extract()
                .map_err(|e| classify_pyerr(local_name.as_str(), e))?;
            Ok(vec![ScalarValue::Utf8(Some(s))])
        })
    }

    fn evaluate(&self) -> Result<ScalarValue, FnError> {
        let finalize = self.callable("finalize")?;
        let local_name = self.local_name.clone();
        let return_dt = match &self.signature.returns {
            ArgType::Primitive(dt) => dt.clone(),
            other => {
                return Err(FnError::new(
                    0x80,
                    format!(
                        "PyO3 aggregate adapter only supports primitive returns, got {other:?}"
                    ),
                ));
            }
        };
        Python::attach(|py| -> Result<ScalarValue, FnError> {
            let state_obj = match &self.state {
                Some(s) => s.clone_ref(py),
                None => {
                    return Ok(default_scalar_for_type(&return_dt));
                }
            };
            let bound = finalize.bind(py);
            let result = bound
                .call1((state_obj,))
                .map_err(|e| classify_pyerr(local_name.as_str(), e))?;
            crate::adapter_scalar_helpers::py_to_scalar(&result, &return_dt)
        })
    }

    fn size(&self) -> usize {
        // We don't have direct access to Python object size without
        // GIL acquisition + sys.getsizeof; return a conservative
        // constant. DataFusion uses this for memory-pressure
        // accounting, where any conservative value is acceptable.
        128
    }
}

/// Build an `AggSignature` from declared arg type names + return type
/// name. The state schema is a single Utf8 column (`STATE_FIELD_NAME`)
/// carrying JSON-serialized state.
///
/// # Errors
///
/// Returns [`FnError`] when a type name is not recognised.
pub fn build_py_agg_signature(
    args: &[SmolStr],
    returns: &SmolStr,
    determinism: &str,
) -> Result<AggSignature, FnError> {
    let arg_types: Vec<ArgType> = args
        .iter()
        .map(|t| type_name_to_argtype(t.as_str()))
        .collect::<Result<_, FnError>>()?;
    let returns_type = type_name_to_argtype(returns.as_str())?;
    let state_fields = vec![Field::new(STATE_FIELD_NAME, DataType::Utf8, true)];
    let volatility = match determinism.trim().to_ascii_lowercase().as_str() {
        "pure" => Volatility::Immutable,
        "session" | "session-scoped" | "sessionscoped" => Volatility::Stable,
        _ => Volatility::Volatile,
    };
    Ok(AggSignature {
        args: arg_types,
        returns: returns_type,
        state_fields,
        volatility,
        supports_partial: true,
    })
}

fn type_name_to_argtype(name: &str) -> Result<ArgType, FnError> {
    let dt = match name.trim().to_ascii_lowercase().as_str() {
        "float" | "float64" | "double" => DataType::Float64,
        "int" | "int64" | "long" => DataType::Int64,
        "string" | "str" | "utf8" => DataType::Utf8,
        "bool" | "boolean" => DataType::Boolean,
        other => {
            return Err(FnError::new(
                0x80,
                format!("unknown aggregate type `{other}`"),
            ));
        }
    };
    Ok(ArgType::Primitive(dt))
}

fn default_scalar_for_type(dt: &DataType) -> ScalarValue {
    match dt {
        DataType::Float64 => ScalarValue::Float64(None),
        DataType::Int64 => ScalarValue::Int64(None),
        DataType::Utf8 => ScalarValue::Utf8(None),
        DataType::Boolean => ScalarValue::Boolean(None),
        _ => ScalarValue::Null,
    }
}

fn classify_pyerr(qname: &str, e: PyErr) -> FnError {
    use pyo3::types::PyTracebackMethods;
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
        FnError::new(
            0x820,
            format!("PyO3 aggregate `{qname}`: {msg}\n{traceback}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Float64Array;
    use std::ffi::CString;
    use uni_plugin::PluginId;

    fn ensure_python() -> bool {
        Python::initialize();
        true
    }

    /// Helper that loads a Python aggregate spec and inserts the four
    /// callables under `name::init` / `name::accumulate` / `name::merge`
    /// / `name::finalize` keys into a fresh runtime.
    fn runtime_with_agg(spec_src: &str) -> Arc<PyPluginRuntime> {
        let rt = PyPluginRuntime::new(PluginId::new("ai.test.agg"));
        Python::attach(|py| {
            let code = CString::new(spec_src).unwrap();
            let module = pyo3::types::PyModule::from_code(
                py,
                code.as_c_str(),
                CString::new("agg_module.py").unwrap().as_c_str(),
                CString::new("agg_module").unwrap().as_c_str(),
            )
            .expect("module compiles");
            for method in ["init", "accumulate", "merge", "finalize"] {
                let f = module.getattr(method).unwrap().unbind();
                rt.insert(format!("sum_floats::{method}"), f);
            }
        });
        rt
    }

    #[test]
    fn agg_sum_single_partition() {
        if !ensure_python() {
            return;
        }
        let rt = runtime_with_agg(
            r#"
def init():
    return {"sum": 0.0, "n": 0}

def accumulate(state, x):
    if x is None:
        return state
    state["sum"] += float(x)
    state["n"] += 1
    return state

def merge(a, b):
    return {"sum": a["sum"] + b["sum"], "n": a["n"] + b["n"]}

def finalize(state):
    return state["sum"]
"#,
        );
        let sig = build_py_agg_signature(&[SmolStr::new("float")], &SmolStr::new("float"), "pure")
            .expect("sig");
        let agg = PyAggregateFn::new(Arc::clone(&rt), "sum_floats", sig);
        let mut acc = agg.create_accumulator();
        let batch: ArrayRef = Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0, 4.5]));
        acc.update_batch(&[batch]).expect("update");
        let result = acc.evaluate().expect("evaluate");
        match result {
            ScalarValue::Float64(Some(v)) => assert!((v - 10.5).abs() < 1e-12),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn agg_sum_two_partitions_matches() {
        if !ensure_python() {
            return;
        }
        let rt = runtime_with_agg(
            r#"
def init():
    return {"sum": 0.0, "n": 0}

def accumulate(state, x):
    if x is None:
        return state
    state["sum"] += float(x)
    state["n"] += 1
    return state

def merge(a, b):
    return {"sum": a["sum"] + b["sum"], "n": a["n"] + b["n"]}

def finalize(state):
    return state["sum"]
"#,
        );
        let sig = build_py_agg_signature(&[SmolStr::new("float")], &SmolStr::new("float"), "pure")
            .expect("sig");
        let agg = PyAggregateFn::new(Arc::clone(&rt), "sum_floats", sig);

        // Partition A: [1, 2, 3]
        let mut acc_a = agg.create_accumulator();
        let batch_a: ArrayRef = Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0]));
        acc_a.update_batch(&[batch_a]).expect("update a");
        let state_a = acc_a.state().expect("state a");

        // Partition B: [10, 20]
        let mut acc_b = agg.create_accumulator();
        let batch_b: ArrayRef = Arc::new(Float64Array::from(vec![10.0_f64, 20.0]));
        acc_b.update_batch(&[batch_b]).expect("update b");
        let state_b = acc_b.state().expect("state b");

        // Final aggregator merges partial states.
        let mut acc_final = agg.create_accumulator();
        let state_strs: Vec<Option<String>> = vec![state_a, state_b]
            .into_iter()
            .map(|v| match &v[0] {
                ScalarValue::Utf8(s) => s.clone(),
                _ => panic!("expected Utf8 state"),
            })
            .collect();
        let merge_arr: ArrayRef = Arc::new(StringArray::from(state_strs));
        acc_final.merge_batch(&[merge_arr]).expect("merge");
        let result = acc_final.evaluate().expect("evaluate");
        match result {
            ScalarValue::Float64(Some(v)) => assert!((v - 36.0).abs() < 1e-12, "got {v}"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn agg_state_roundtrips_through_json() {
        if !ensure_python() {
            return;
        }
        let rt = runtime_with_agg(
            r#"
def init():
    return {"sum": 0.0, "n": 0, "items": []}

def accumulate(state, x):
    if x is None: return state
    state["sum"] += float(x); state["n"] += 1; state["items"].append(float(x))
    return state

def merge(a, b):
    return {"sum": a["sum"]+b["sum"], "n": a["n"]+b["n"],
            "items": a["items"]+b["items"]}

def finalize(state):
    return state["sum"]
"#,
        );
        let sig = build_py_agg_signature(&[SmolStr::new("float")], &SmolStr::new("float"), "pure")
            .expect("sig");
        let agg = PyAggregateFn::new(Arc::clone(&rt), "sum_floats", sig);
        let mut acc = agg.create_accumulator();
        let batch: ArrayRef = Arc::new(Float64Array::from(vec![1.0_f64, 2.0]));
        acc.update_batch(&[batch]).expect("update");
        let s = acc.state().expect("state");
        match &s[0] {
            ScalarValue::Utf8(Some(json)) => {
                assert!(json.contains("\"sum\":"));
                assert!(json.contains("3.0") || json.contains("3"));
                assert!(json.contains("\"items\":"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn agg_empty_evaluate_returns_null() {
        if !ensure_python() {
            return;
        }
        let rt = runtime_with_agg(
            r#"
def init():
    return 0.0
def accumulate(state, x):
    return state + (0.0 if x is None else float(x))
def merge(a, b):
    return a + b
def finalize(state):
    return state
"#,
        );
        let sig = build_py_agg_signature(&[SmolStr::new("float")], &SmolStr::new("float"), "pure")
            .expect("sig");
        let agg = PyAggregateFn::new(Arc::clone(&rt), "sum_floats", sig);
        let acc = agg.create_accumulator();
        // No update_batch — empty accumulator.
        let result = acc.evaluate().expect("evaluate");
        assert!(matches!(result, ScalarValue::Float64(None)));
    }
}
