//! PyO3 plugin manifest — the decorator-accumulated record of which
//! Python callables a loaded plugin exposes.
//!
//! Unlike Rhai (which reads a `uni_manifest()` function from the
//! script) and Extism / Component-Model (which read a JSON `manifest`
//! export), PyO3 plugins declare entries via **decorator calls** at
//! module-execution time. The `_uni_decorator_sink` global the loader
//! installs in the module namespace before exec accumulates each
//! decorator invocation into a [`ManifestBuilder`]; on completion the
//! loader drains the builder into the host registry.
//!
//! Equivalent to proposal §5.4's `@db.scalar_fn(...)` decorator surface.

#![cfg(feature = "pyo3")]

use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::prelude::*;
use smol_str::SmolStr;

use crate::error::PyPluginError;

/// One scalar fn entry collected by the decorator sink.
#[derive(Debug)]
pub struct PyScalarEntry {
    /// User-facing name as declared in `@db.scalar_fn("name", ...)`.
    /// May be plugin-qualified (`"py.score"`) or local (`"haversine"`);
    /// the loader prefixes the plugin id when constructing the
    /// `QName`.
    pub name: SmolStr,
    /// Argument type names (`"float"`, `"int"`, `"string"`, `"bool"`).
    pub args: Vec<SmolStr>,
    /// Return type name in the same naming.
    pub returns: SmolStr,
    /// Vectorized mode flag (true = PyArrow Array per column, false = per row).
    pub vectorized: bool,
    /// Determinism declaration (`"pure"`, `"session"`, `"nondeterministic"`).
    pub determinism: SmolStr,
    /// The captured Python callable.
    pub callable: Py<PyAny>,
}

/// One aggregate fn entry.
#[derive(Debug)]
pub struct PyAggregateEntry {
    /// User-facing aggregate name.
    pub name: SmolStr,
    /// Argument types.
    pub args: Vec<SmolStr>,
    /// Final return type.
    pub returns: SmolStr,
    /// Determinism.
    pub determinism: SmolStr,
    /// `init` callable producing the per-group state.
    pub init: Py<PyAny>,
    /// `accumulate(state, *args) -> new_state` callable.
    pub accumulate: Py<PyAny>,
    /// `merge(state_a, state_b) -> state` callable for cross-partition merge.
    pub merge: Py<PyAny>,
    /// `finalize(state) -> result` callable.
    pub finalize: Py<PyAny>,
}

/// One procedure entry.
#[derive(Debug)]
pub struct PyProcedureEntry {
    /// User-facing procedure name.
    pub name: SmolStr,
    /// Argument types.
    pub args: Vec<SmolStr>,
    /// Yielded column type names.
    pub yields: Vec<SmolStr>,
    /// Procedure mode (`"read"`, `"write"`, `"schema"`, `"dbms"`).
    pub mode: SmolStr,
    /// The captured Python callable (returns PyArrow RecordBatch or
    /// iterable of dicts).
    pub callable: Py<PyAny>,
}

/// Top-level manifest accumulated by decorator calls.
#[derive(Debug, Default)]
pub struct PyManifest {
    /// Plugin id (defaults to `"py.live"` if the plugin did not call
    /// `db.set_plugin_id(...)` — the loader can override).
    pub id: SmolStr,
    /// Plugin version (defaults to `"0.0.0"`).
    pub version: SmolStr,
    /// Determinism (default `"nondeterministic"`).
    pub determinism: SmolStr,
    /// Scalar entries.
    pub scalar_fns: Vec<PyScalarEntry>,
    /// Aggregate entries.
    pub aggregate_fns: Vec<PyAggregateEntry>,
    /// Procedure entries.
    pub procedures: Vec<PyProcedureEntry>,
}

impl PyManifest {
    /// Construct an empty manifest with default id / version.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            id: SmolStr::new("py.live"),
            version: SmolStr::new("0.0.0"),
            determinism: SmolStr::new("nondeterministic"),
            scalar_fns: Vec::new(),
            aggregate_fns: Vec::new(),
            procedures: Vec::new(),
        }
    }

    /// Reject a manifest with zero declared entries — would register
    /// nothing.
    pub fn validate_non_empty(&self) -> Result<(), PyPluginError> {
        if self.scalar_fns.is_empty() && self.aggregate_fns.is_empty() && self.procedures.is_empty()
        {
            return Err(PyPluginError::ManifestInvalid(
                "no scalar / aggregate / procedure entries were declared by decorators".into(),
            ));
        }
        Ok(())
    }
}

/// Thread-safe builder consumed by the decorator sink.
///
/// Cloned `Arc<ManifestBuilder>` is installed as a Python attribute
/// (`_uni_decorator_sink._builder`) before the plugin module runs; each
/// decorator invocation locks the builder briefly to append. On
/// completion the loader calls `into_manifest()` to drain.
#[derive(Debug, Default)]
pub struct ManifestBuilder {
    inner: Mutex<PyManifest>,
}

impl ManifestBuilder {
    /// Construct an empty builder.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(PyManifest::empty()),
        })
    }

    /// Set the plugin id (called from `db.set_plugin_id(...)` or the
    /// loader if the user omitted it).
    pub fn set_id(&self, id: impl Into<SmolStr>) {
        self.inner.lock().id = id.into();
    }

    /// Set the plugin version.
    pub fn set_version(&self, version: impl Into<SmolStr>) {
        self.inner.lock().version = version.into();
    }

    /// Set the determinism declaration.
    pub fn set_determinism(&self, determinism: impl Into<SmolStr>) {
        self.inner.lock().determinism = determinism.into();
    }

    /// Append a scalar entry.
    pub fn push_scalar(&self, entry: PyScalarEntry) {
        self.inner.lock().scalar_fns.push(entry);
    }

    /// Append an aggregate entry.
    pub fn push_aggregate(&self, entry: PyAggregateEntry) {
        self.inner.lock().aggregate_fns.push(entry);
    }

    /// Append a procedure entry.
    pub fn push_procedure(&self, entry: PyProcedureEntry) {
        self.inner.lock().procedures.push(entry);
    }

    /// Drain the builder into the accumulated [`PyManifest`].
    #[must_use]
    pub fn into_manifest(&self) -> PyManifest {
        std::mem::replace(&mut *self.inner.lock(), PyManifest::empty())
    }

    /// Peek at the current count without draining.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        let m = self.inner.lock();
        m.scalar_fns.len() + m.aggregate_fns.len() + m.procedures.len()
    }
}
