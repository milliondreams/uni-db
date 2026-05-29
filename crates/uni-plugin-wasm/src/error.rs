//! Error types for the WASM loader.

use thiserror::Error;

/// Errors specific to the WASM loader.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WasmError {
    /// The supplied WASM bytes failed to parse or validate.
    #[error("wasm parse / validation failure: {0}")]
    InvalidWasm(String),

    /// The WASM module did not export a recognized plugin WIT world.
    #[error(
        "wasm module exports no recognized plugin world (expected one of: scalar, aggregate, procedure, locy-agg, hook)"
    )]
    NoRecognizedWorld,

    /// Component instantiation failed.
    #[error("wasm instantiation failed: {0}")]
    Instantiate(String),

    /// A host import the plugin requested is absent (capability ungranted).
    #[error("plugin imports `{import}` but capability is not granted")]
    MissingCapability {
        /// Name of the host import that was rejected.
        import: String,
    },

    /// Arrow IPC marshalling across the linear-memory boundary failed.
    #[error("arrow IPC at wasm boundary: {0}")]
    Ipc(#[from] uni_plugin_wasm_rt::IpcError),

    /// Wall-clock or fuel deadline exceeded.
    #[error("wasm plugin exceeded resource limit: {0}")]
    ResourceLimit(String),

    /// Loader scaffolding shipped without a complete cutover for this entry
    /// point. M6 cutover commits remove these.
    #[error("uni-plugin-wasm: {feature} not yet wired (M6 in progress)")]
    NotYetImplemented {
        /// The not-yet-wired feature.
        feature: String,
    },

    /// Internal / unexpected error.
    #[error("uni-plugin-wasm internal error: {0}")]
    Internal(String),

    /// The plugin's declared ABI range does not intersect any
    /// host-supported major (per the multi-version `Linker` cache in
    /// [`crate::multi_version`]).
    #[error("plugin abi {requested} unsupported; host majors: {supported:?}")]
    AbiUnsupported {
        /// The plugin's manifest `abi` range string.
        requested: String,
        /// Host-supported major versions.
        supported: Vec<u64>,
    },
}

impl WasmError {
    /// Construct a `NotYetImplemented` for the named feature.
    #[must_use]
    pub fn not_yet(feature: impl Into<String>) -> Self {
        Self::NotYetImplemented {
            feature: feature.into(),
        }
    }
}

impl uni_plugin_wasm_rt::pool::PoolResourceLimit for WasmError {
    fn resource_limit(msg: String) -> Self {
        Self::ResourceLimit(msg)
    }
}
