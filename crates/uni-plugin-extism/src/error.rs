//! Error types for the Extism loader.

use thiserror::Error;

/// Errors specific to the Extism loader.
///
/// Mirrors `uni_plugin_wasm::WasmError` in shape so the two loaders
/// surface comparable failure modes despite their different ABIs.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ExtismError {
    /// The supplied WASM bytes failed to parse or did not declare the
    /// expected Extism plugin shape (manifest export, function exports).
    #[error("extism plugin: invalid wasm or manifest: {0}")]
    InvalidPlugin(String),

    /// The plugin's declared manifest did not pass validation
    /// (unknown ABI, missing required fields, capability conflict).
    #[error("extism plugin manifest invalid: {0}")]
    ManifestInvalid(String),

    /// Extism plugin instantiation failed.
    #[error("extism instantiation failed: {0}")]
    Instantiate(String),

    /// A capability-gated host function was invoked without the matching
    /// grant. Unlike the Component Model path (where the import is
    /// absent at the linker level), Extism enforces capability checks at
    /// the host-fn body — this variant carries the call that was
    /// blocked.
    #[error("extism plugin called host fn `{host_fn}` without {capability:?} grant")]
    CapabilityDenied {
        /// Host function the plugin attempted to invoke.
        host_fn: String,
        /// The capability the plugin would need.
        capability: String,
    },

    /// The plugin's output failed to decode under the expected wire
    /// format (JSON or MessagePack for control surfaces).
    #[error("extism output decode error: {0}")]
    OutputDecode(String),

    /// Arrow IPC encode / decode failed across the wasm boundary.
    /// Distinct from [`Self::OutputDecode`] which is JSON / MessagePack
    /// for control surfaces.
    #[error("extism arrow IPC: {0}")]
    Ipc(#[from] uni_plugin_wasm_rt::IpcError),

    /// Wall-clock, fuel, or memory limit exceeded.
    #[error("extism plugin exceeded resource limit: {0}")]
    ResourceLimit(String),

    /// Internal / unexpected error.
    #[error("uni-plugin-extism internal error: {0}")]
    Internal(String),
}

impl uni_plugin_wasm_rt::pool::PoolResourceLimit for ExtismError {
    fn resource_limit(msg: String) -> Self {
        Self::ResourceLimit(msg)
    }
}
