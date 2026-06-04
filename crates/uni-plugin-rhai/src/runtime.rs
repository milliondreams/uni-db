//! Shared per-plugin runtime — the `Engine` + `AST` adapters hold by
//! reference.
//!
//! Each loaded Rhai plugin owns one [`RhaiPluginRuntime`]: a Send + Sync
//! handle wrapping the configured `rhai::Engine` and the compiled `AST`.
//! Adapters (`RhaiScalarFn`, `RhaiAggregateFn`, `RhaiProcedure`) hold
//! `Arc<RhaiPluginRuntime>` clones so the underlying state isn't
//! duplicated per registered function.

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use rhai::{AST, Engine};

use uni_plugin::PluginId;

/// Per-plugin runtime state shared across every adapter the plugin
/// registers.
#[derive(Debug)]
pub struct RhaiPluginRuntime {
    /// The plugin's id (for error reporting).
    pub plugin_id: PluginId,
    /// The Rhai engine, configured for this plugin's effective
    /// capability set. `Send + Sync` via Rhai's `sync` feature.
    pub engine: Arc<Engine>,
    /// The compiled script AST. `Send + Sync` and cheaply `Clone`.
    pub ast: Arc<AST>,
}

impl RhaiPluginRuntime {
    /// Construct a runtime handle.
    #[must_use]
    pub fn new(plugin_id: PluginId, engine: Engine, ast: AST) -> Arc<Self> {
        Arc::new(Self {
            plugin_id,
            engine: Arc::new(engine),
            ast: Arc::new(ast),
        })
    }
}
