//! Shared per-plugin runtime — the namespace of Python callables a
//! loaded plugin exposes.
//!
//! Each loaded Python plugin owns one [`PyPluginRuntime`]: a Send + Sync
//! handle wrapping a name→`Py<PyAny>` map of registered callables.
//! Adapters (`PyScalarFn`, `PyAggregateFn`, `PyProcedure`) hold
//! `Arc<PyPluginRuntime>` clones so the underlying state isn't
//! duplicated per registered function.

#![cfg(feature = "pyo3")]

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use pyo3::prelude::*;
use smol_str::SmolStr;

use uni_plugin::PluginId;

/// Per-plugin runtime state shared across every adapter the plugin
/// registers.
///
/// `Py<PyAny>` is `Send + Sync` (the GIL guards access). The internal
/// `RwLock` only guards the map mutation — once installed at load
/// time, callables are read-mostly.
#[derive(Debug)]
pub struct PyPluginRuntime {
    /// The plugin's id (for error reporting).
    pub plugin_id: PluginId,
    /// Name → Python callable map.
    callables: RwLock<HashMap<SmolStr, Py<PyAny>>>,
}

impl PyPluginRuntime {
    /// Construct a runtime handle.
    #[must_use]
    pub fn new(plugin_id: PluginId) -> Arc<Self> {
        Arc::new(Self {
            plugin_id,
            callables: RwLock::new(HashMap::new()),
        })
    }

    /// Insert a callable under a local name (e.g., `"haversine"`).
    pub fn insert(&self, name: impl Into<SmolStr>, callable: Py<PyAny>) {
        self.callables.write().insert(name.into(), callable);
    }

    /// Resolve a callable by local name. Returns a cheap GIL-free
    /// clone of the `Py<PyAny>` handle (a refcount bump).
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Py<PyAny>> {
        self.callables
            .read()
            .get(name)
            .map(|p| Python::attach(|py| p.clone_ref(py)))
    }

    /// Count of registered callables.
    #[must_use]
    pub fn len(&self) -> usize {
        self.callables.read().len()
    }

    /// Whether the runtime is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.callables.read().is_empty()
    }

    /// Iterate over registered callable names (snapshot).
    #[must_use]
    pub fn names(&self) -> Vec<SmolStr> {
        self.callables.read().keys().cloned().collect()
    }
}
