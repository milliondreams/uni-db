//! Extism loader for the uni-db plugin framework.
//!
//! `uni-plugin-extism` is the **user-facing** WASM loader. It sits
//! parallel to `uni-plugin-wasm` (the Component Model loader). It follows
//! the "Option C" hybrid pattern:
//!
//! - **Component Model** (`uni-plugin-wasm`) — typed WIT contracts,
//!   capability gating by linker absence, used for the Lua-host, trusted
//!   built-ins, and plugins where soundness is load-bearing.
//! - **Extism** (this crate) — bytes-in / bytes-out, runtime-checked
//!   capabilities, used for user-authored UDFs that benefit from
//!   Extism's mature 13-language host SDK story.
//!
//! Trust is a property of the granted [`CapabilitySet`], not of which
//! ABI a plugin happens to use. A user can author a "trusted" plugin in
//! Extism by granting it broad capabilities; a built-in can author
//! against Extism if the structural advantages of CM aren't load-bearing
//! for its use case. The framework intentionally decouples ABI choice
//! from trust tier.
//!
//! # Crate status
//!
//! Loader is wired through the `extism-sdk`. `ExtismLoader::load` runs a
//! two-pass instantiation (manifest probe, then plugin build) and
//! surfaces failures as [`ExtismError::Instantiate`]. The host-fn
//! registration surface ([`HostFnRegistry`]) and capability gating model
//! are in place; the `NotYetImplemented` placeholder has been retired.
//!
//! # Why two WASM loaders
//!
//! The proposal's loader matrix (§5.1) keeps the four authoring
//! categories (compile-time Rust, WASM, PyO3, Lua) but splits the WASM
//! row into two ABIs. Both ABIs converge on the same `PluginRegistrar`
//! — the executor cannot tell whether a registered `ScalarPluginFn` was
//! authored against WIT or Extism. The two loaders share the wasmtime
//! runtime (Extism is itself wasmtime-backed), so the runtime cost is
//! one wasmtime process, not two.
//!
//! [`CapabilitySet`]: uni_plugin::CapabilitySet

// Rust guideline compliant
#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

pub mod error;
pub mod host_fns;

/// Arrow IPC bridge — re-exported from `uni-plugin-wasm-rt`.
///
/// Lifted to the shared crate in M6.shared. The public API
/// (`encode_batch`, `decode_batch`, `encode_batches`, `decode_batches`)
/// stays at `uni_plugin_extism::ipc::*` for backwards-compatibility;
/// the implementation lives once, in `uni-plugin-wasm-rt`.
pub mod ipc {
    pub use uni_plugin_wasm_rt::ipc::{decode_batch, decode_batches, encode_batch, encode_batches};
}

#[cfg(feature = "extism-runtime")]
pub mod adapter;
#[cfg(feature = "extism-runtime")]
pub mod adapter_aggregate;
#[cfg(feature = "extism-runtime")]
pub mod adapter_common;
#[cfg(feature = "extism-runtime")]
pub mod adapter_procedure;

/// Instance pool — re-exported from `uni-plugin-wasm-rt`.
///
/// Type-aliased so `ExtismInstancePool<extism::Plugin>` and
/// `PooledInstance<extism::Plugin>` keep working unchanged downstream.
#[cfg(feature = "extism-runtime")]
pub mod pool {
    pub use uni_plugin_wasm_rt::pool::{PoolConfig, PoolMetrics};

    /// Type alias — generic `InstancePool` parameterized with
    /// [`crate::ExtismError`].
    pub type ExtismInstancePool<T> = uni_plugin_wasm_rt::pool::InstancePool<T, crate::ExtismError>;

    /// Type alias — generic `PooledInstance` parameterized with
    /// [`crate::ExtismError`].
    pub type PooledInstance<T> = uni_plugin_wasm_rt::pool::PooledInstance<T, crate::ExtismError>;
}
#[cfg(feature = "extism-runtime")]
pub mod exports;
#[cfg(feature = "extism-runtime")]
pub mod host_svc;
#[cfg(feature = "extism-runtime")]
pub mod loader;
#[cfg(feature = "extism-runtime")]
pub mod wire_translate;

#[doc(inline)]
pub use error::ExtismError;
#[doc(inline)]
pub use host_fns::HostFnRegistry;

#[cfg(feature = "extism-runtime")]
#[doc(inline)]
pub use adapter::ExtismScalarFn;
#[cfg(feature = "extism-runtime")]
#[doc(inline)]
pub use adapter_aggregate::ExtismAggregateFn;
#[cfg(feature = "extism-runtime")]
#[doc(inline)]
pub use adapter_procedure::ExtismProcedure;
#[cfg(feature = "extism-runtime")]
#[doc(inline)]
pub use exports::{
    RegistrationEntry, RegistrationManifest, WireArgType, WireFnSignature, parse_manifest_json,
    parse_registration_json, read_manifest_export, read_register_export,
};
#[cfg(feature = "extism-runtime")]
#[doc(inline)]
pub use host_svc::register_default_host_svc;
#[cfg(feature = "extism-runtime")]
#[doc(inline)]
pub use loader::ExtismLoader;
#[cfg(feature = "extism-runtime")]
#[doc(inline)]
pub use wire_translate::{
    arrow_name_to_datatype, wire_arg_to_internal, wire_fn_sig_to_internal,
    wire_null_handling_to_internal, wire_volatility_to_internal,
};
