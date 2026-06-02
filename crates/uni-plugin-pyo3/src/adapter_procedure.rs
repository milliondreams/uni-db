//! Procedure adapter — turns a Python callable into a
//! [`uni_plugin::traits::procedure::ProcedurePlugin`].
//!
//! # Return shape (v1)
//!
//! The Python procedure callable is invoked with scalar args (each
//! `ColumnarValue::Scalar` becomes a Python value via the standard
//! per-row helpers) and must return an **iterable of dicts** where
//! each dict maps the manifest's declared yield-column name to a
//! Python primitive. The adapter materializes the dicts into a
//! `RecordBatch` matching the declared yield schema, then wraps it in
//! a one-shot `SendableRecordBatchStream`.
//!
//! Users with pyarrow can pre-flatten via `.to_pylist()`:
//! ```python
//! @db.procedure("ranges", args=["int"], yields=["int","int"], mode="read")
//! def ranges(n):
//!     import pyarrow as pa
//!     batch = pa.RecordBatch.from_pylist([
//!         {"col0": i, "col1": i * 2} for i in range(n)
//!     ])
//!     return batch.to_pylist()
//! ```
//!
//! Native pyarrow.RecordBatch passthrough is M8-followup.

#![cfg(feature = "pyo3")]

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use futures::stream;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
use smol_str::SmolStr;

use uni_plugin::errors::FnError;
use uni_plugin::traits::procedure::{ProcedureContext, ProcedurePlugin, ProcedureSignature};

use crate::adapter_scalar_helpers::{PrimitiveColumnBuilder, classify_pyerr};
use crate::runtime::PyPluginRuntime;

/// Procedure adapter dispatching to a Python callable held in
/// [`PyPluginRuntime`].
#[derive(Debug)]
pub struct PyProcedure {
    runtime: Arc<PyPluginRuntime>,
    local_name: SmolStr,
    signature: ProcedureSignature,
}

impl PyProcedure {
    /// Construct a procedure adapter.
    #[must_use]
    pub fn new(
        runtime: Arc<PyPluginRuntime>,
        local_name: impl Into<SmolStr>,
        signature: ProcedureSignature,
    ) -> Self {
        Self {
            runtime,
            local_name: local_name.into(),
            signature,
        }
    }
}

impl ProcedurePlugin for PyProcedure {
    fn signature(&self) -> &ProcedureSignature {
        &self.signature
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let callable = self.runtime.get(self.local_name.as_str()).ok_or_else(|| {
            FnError::new(
                0x830,
                format!(
                    "python procedure callable `{}` not in runtime `{}`",
                    self.local_name,
                    self.runtime.plugin_id.as_str()
                ),
            )
        })?;
        let local_name = self.local_name.clone();

        // Materialize scalar args (procedures take scalars per the Rhai
        // procedure contract; vectorized columnar input is the
        // `batch_input` channel which v1 PyO3 doesn't yet support).
        let mut scalar_args: Vec<ScalarValue> = Vec::with_capacity(args.len());
        for (i, arg) in args.iter().enumerate() {
            match arg {
                ColumnarValue::Scalar(s) => scalar_args.push(s.clone()),
                ColumnarValue::Array(_) => {
                    return Err(FnError::new(
                        0x80,
                        format!("python procedure arg {i} must be a scalar (no array input)"),
                    ));
                }
            }
        }

        // Build the yield schema once.
        let schema: SchemaRef = Arc::new(Schema::new(self.signature.yields.clone()));

        let batch = Python::attach(|py| -> Result<RecordBatch, FnError> {
            let mut py_args: Vec<Bound<'_, PyAny>> = Vec::with_capacity(scalar_args.len());
            for s in &scalar_args {
                py_args.push(scalar_value_to_py(py, s)?);
            }
            let bound = callable.bind(py);
            let tuple = pyo3::types::PyTuple::new(py, py_args)
                .map_err(|e| classify_pyerr(0x830, "procedure ", local_name.as_str(), e))?;
            let result = bound
                .call1(tuple)
                .map_err(|e| classify_pyerr(0x830, "procedure ", local_name.as_str(), e))?;
            build_record_batch_from_dicts(&result, schema.clone(), local_name.as_str())
        })?;

        // Wrap as a single-element stream.
        let stream = stream::iter(std::iter::once(Ok(batch)));
        let adapter = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Box::pin(adapter))
    }
}

/// Convert a `ScalarValue` into a `Bound<PyAny>`.
fn scalar_value_to_py<'py>(py: Python<'py>, s: &ScalarValue) -> Result<Bound<'py, PyAny>, FnError> {
    use pyo3::IntoPyObjectExt;
    match s {
        ScalarValue::Float64(Some(v)) => v
            .into_bound_py_any(py)
            .map_err(|e| FnError::new(0x83, format!("f64→py: {e}"))),
        ScalarValue::Int64(Some(v)) => v
            .into_bound_py_any(py)
            .map_err(|e| FnError::new(0x83, format!("i64→py: {e}"))),
        ScalarValue::Utf8(Some(v)) => v
            .as_str()
            .into_bound_py_any(py)
            .map_err(|e| FnError::new(0x83, format!("utf8→py: {e}"))),
        ScalarValue::Boolean(Some(v)) => v
            .into_bound_py_any(py)
            .map_err(|e| FnError::new(0x83, format!("bool→py: {e}"))),
        // NULL scalars
        ScalarValue::Float64(None)
        | ScalarValue::Int64(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::Boolean(None)
        | ScalarValue::Null => Ok(py.None().into_bound(py)),
        other => Err(FnError::new(
            0x83,
            format!("PyO3 procedure: scalar arg type `{other}` not yet supported"),
        )),
    }
}

/// Iterate the Python value as a sequence of dicts and build a
/// `RecordBatch` matching `schema`.
///
/// Column names are taken from `schema.field(i).name()`. Each dict
/// row's missing key falls through to a typed null in the column.
fn build_record_batch_from_dicts(
    obj: &Bound<'_, PyAny>,
    schema: SchemaRef,
    qname: &str,
) -> Result<RecordBatch, FnError> {
    let mut builders: Vec<PrimitiveColumnBuilder> = schema
        .fields()
        .iter()
        .map(|f| PrimitiveColumnBuilder::new(f.data_type(), 0, 0x830, "PyO3 procedure: yield type"))
        .collect::<Result<_, FnError>>()?;

    let iter = obj
        .try_iter()
        .map_err(|e| classify_pyerr(0x830, "procedure ", qname, e))?;
    let mut row_count: usize = 0;
    for item in iter {
        let row = item.map_err(|e| classify_pyerr(0x830, "procedure ", qname, e))?;
        let dict = row.cast::<PyDict>().map_err(|_| {
            FnError::new(
                0x831,
                "python procedure: yielded row is not a dict".to_owned(),
            )
        })?;
        for (i, field) in schema.fields().iter().enumerate() {
            let name = field.name();
            let value = dict
                .get_item(name)
                .map_err(|e| classify_pyerr(0x830, "procedure ", qname, e))?;
            match value {
                Some(v) if !v.is_none() => {
                    builders[i].push_py(&v, 0x830, "procedure ", qname)?;
                }
                _ => builders[i].push_null(),
            }
        }
        row_count += 1;
    }

    let columns: Vec<ArrayRef> = builders
        .into_iter()
        .map(PrimitiveColumnBuilder::finish)
        .collect();
    RecordBatch::try_new(schema, columns).map_err(|e| {
        FnError::new(
            0x832,
            format!("python procedure `{qname}` build_record_batch: {e} (rows={row_count})"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use futures::StreamExt;
    use std::ffi::CString;
    use uni_plugin::PluginId;
    use uni_plugin::capability::SideEffects;
    use uni_plugin::traits::procedure::{NamedArgType, ProcedureMode};
    use uni_plugin::traits::scalar::ArgType;

    fn ensure_python() -> bool {
        Python::initialize();
        true
    }

    fn runtime_with_proc(name: &str, body: &str) -> Arc<PyPluginRuntime> {
        let rt = PyPluginRuntime::new(PluginId::new("ai.test.proc"));
        Python::attach(|py| {
            let code = CString::new(format!("def {name}{body}\n")).unwrap();
            let module = pyo3::types::PyModule::from_code(
                py,
                code.as_c_str(),
                CString::new("proc_module.py").unwrap().as_c_str(),
                CString::new("proc_module").unwrap().as_c_str(),
            )
            .expect("module compiles");
            let f = module.getattr(name).expect("fn defined").unbind();
            rt.insert(name, f);
        });
        rt
    }

    fn proc_sig(args: Vec<(&str, DataType)>, yields: Vec<(&str, DataType)>) -> ProcedureSignature {
        ProcedureSignature {
            args: args
                .into_iter()
                .map(|(name, dt)| NamedArgType {
                    name: SmolStr::new(name),
                    ty: ArgType::Primitive(dt),
                    default: None,
                    doc: String::new(),
                })
                .collect(),
            yields: yields
                .into_iter()
                .map(|(name, dt)| Field::new(name, dt, true))
                .collect(),
            mode: ProcedureMode::Read,
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: String::new(),
        }
    }

    #[tokio::test]
    async fn procedure_yields_recordbatch() {
        if !ensure_python() {
            return;
        }
        let rt = runtime_with_proc(
            "ranges",
            "(n):\n    return [{'idx': i, 'doubled': i * 2.0} for i in range(int(n))]",
        );
        let sig = proc_sig(
            vec![("n", DataType::Int64)],
            vec![("idx", DataType::Int64), ("doubled", DataType::Float64)],
        );
        let proc = PyProcedure::new(rt, "ranges", sig);
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(4)))];
        let mut stream = proc.invoke(ProcedureContext::new(), &args).expect("invoke");
        let batch = stream.next().await.expect("first batch").expect("ok");
        assert_eq!(batch.num_rows(), 4);
        let idx = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let dbl = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(idx.value(0), 0);
        assert_eq!(idx.value(3), 3);
        assert!((dbl.value(2) - 4.0).abs() < 1e-12);
    }

    #[tokio::test]
    async fn procedure_handles_missing_yield_keys_as_null() {
        if !ensure_python() {
            return;
        }
        let rt = runtime_with_proc(
            "partial",
            "(n):\n    return [{'x': 'hello'}, {'x': None}, {'x': 'world'}]",
        );
        let sig = proc_sig(
            vec![("n", DataType::Int64)],
            vec![("x", DataType::Utf8), ("y", DataType::Float64)],
        );
        let proc = PyProcedure::new(rt, "partial", sig);
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(3)))];
        let mut stream = proc.invoke(ProcedureContext::new(), &args).expect("invoke");
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        let x = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let y = batch.column(1);
        assert_eq!(x.value(0), "hello");
        assert!(x.is_null(1));
        assert_eq!(x.value(2), "world");
        assert_eq!(y.null_count(), 3);
    }

    #[tokio::test]
    async fn procedure_python_exception_surfaces() {
        if !ensure_python() {
            return;
        }
        let rt = runtime_with_proc("boom", "(n):\n    raise RuntimeError('procedure exploded')");
        let sig = proc_sig(vec![("n", DataType::Int64)], vec![("v", DataType::Float64)]);
        let proc = PyProcedure::new(rt, "boom", sig);
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(1)))];
        let err = match proc.invoke(ProcedureContext::new(), &args) {
            Err(e) => e,
            Ok(_) => panic!("expected procedure to fail"),
        };
        assert!(err.message.contains("RuntimeError"), "got: {}", err.message);
    }
}
