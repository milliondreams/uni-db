//! PyO3 live-callable plugin loader for uni-db.
//!
//! This crate bridges Python callables held in the host process to the
//! [`uni_plugin`] trait surfaces. Plugins are **session-scoped by
//! default** (`Scope::Session`) and run at host process privilege
//! (**no sandbox**). Capabilities are declared metadata and enforced
//! at the [`PluginRegistrar`](uni_plugin::PluginRegistrar) gate; there
//! is no structural sandbox layer for PyO3 because the callable is a
//! live Python object in the host process.
//!
//! # Crate layout
//!
//! - `error` — `PyPluginError`, the structured error type. `From<PyErr>`
//!   captures Python tracebacks under the GIL.
//! - `arrow_bridge` — Arrow ↔ PyArrow zero-copy via the
//!   [Arrow PyCapsule Interface]. No `pyo3-arrow` dependency.
//! - Manifest / loader / adapters land in later M8 sub-milestones.
//!
//! [Arrow PyCapsule Interface]: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
//!
//! # Loader execution model
//!
//! Two GIL strategies map onto the proposal's two modes:
//!
//! - **Vectorized** (`vectorized=True`): one `Python::with_gil` per
//!   RecordBatch. Each input column is marshaled to a pyarrow Array via
//!   the PyCapsule protocol (zero-copy); the user fn runs once per batch
//!   and returns a pyarrow Array; the result is marshaled back to Arrow.
//!   Recommended ceiling: ~5M+ rows/sec on trivial Float64 fns over
//!   8192-row batches.
//!
//! - **Row-by-row** (`vectorized=False`): one `Python::with_gil` *per
//!   batch* still (we hold the GIL across the rows in a batch — design
//!   decision #6 in `plans/magical-rolling-pinwheel.md`); inside the
//!   closure the host iterates rows and calls the Python fn once per
//!   row with native PyObject args. Approximate ceiling: ~100k
//!   rows/sec.
//!
//! Both modes serialize on the GIL, so a multi-partition DataFusion
//! scan with a PyO3 UDF collapses to single-core throughput. This is
//! the **dominant operational concern** with PyO3 UDFs and is
//! documented in the proposal at §5.4.1. Mitigations
//! (sub-interpreter parallelism, free-threading) are deferred to
//! follow-up milestones.

// Rust guideline compliant
#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

pub mod error;

#[cfg(feature = "pyo3")]
pub mod adapter_aggregate;
#[cfg(feature = "pyo3")]
pub mod adapter_procedure;
#[cfg(feature = "pyo3")]
pub mod adapter_scalar;
#[cfg(feature = "pyo3")]
pub(crate) mod adapter_scalar_helpers;
#[cfg(feature = "pyo3")]
pub mod arrow_bridge;
#[cfg(feature = "pyo3")]
pub mod loader;
#[cfg(feature = "pyo3")]
pub mod manifest;
#[cfg(feature = "pyo3")]
pub mod plugin_handle;
#[cfg(feature = "pyo3")]
pub mod runtime;

#[doc(inline)]
pub use crate::error::PyPluginError;

#[cfg(feature = "pyo3")]
#[doc(inline)]
pub use crate::adapter_aggregate::{PyAccumulator, PyAggregateFn, build_py_agg_signature};
#[cfg(feature = "pyo3")]
#[doc(inline)]
pub use crate::adapter_procedure::PyProcedure;
#[cfg(feature = "pyo3")]
#[doc(inline)]
pub use crate::adapter_scalar::PyScalarFn;
#[cfg(feature = "pyo3")]
#[doc(inline)]
pub use crate::loader::{
    LoadOutcome, PyDecoratorSink, PyDecoratorTrampoline, PyPluginLoader as PythonPluginLoader,
    make_aggregate_trampoline, make_procedure_trampoline, make_scalar_trampoline,
};
#[cfg(feature = "pyo3")]
#[doc(inline)]
pub use crate::manifest::{
    ManifestBuilder, PyAggregateEntry, PyManifest, PyProcedureEntry, PyScalarEntry,
};
#[cfg(feature = "pyo3")]
#[doc(inline)]
pub use crate::plugin_handle::PyPluginHandle;
#[cfg(feature = "pyo3")]
#[doc(inline)]
pub use crate::runtime::PyPluginRuntime;
