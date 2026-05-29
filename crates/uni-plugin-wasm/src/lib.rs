//! WASM Component Model loader for the uni-db plugin framework.
//!
//! `uni-plugin-wasm` provides the host-side machinery to load WASM
//! plugins built against the `uni:plugin` WIT worlds. Per
//! `docs/proposals/plugin_framework.md` §6, the boundary uses:
//!
//! - **WIT-typed contracts** for each plugin kind (scalar, aggregate,
//!   procedure, locy-agg, hook, storage, …) — typed exports + capability-
//!   gated host imports.
//! - **Arrow IPC over linear memory** for `RecordBatch` exchange — host
//!   calls plugin's `alloc(len)`, copies IPC bytes in, calls the work
//!   export, reads IPC bytes back at the returned `(ptr, len)`.
//! - **Pre-warmed instance pools** to amortize the 10–100 ms wasmtime
//!   instantiation cost across hot-path UDF invocations.
//!
//! # Crate status
//!
//! M6 ships the **scaffolding**: the public `WasmLoader` type, the
//! [`WasmInstancePool`] skeleton, the `ipc` Arrow-IPC marshalling
//! helpers, and an error model. The actual wasmtime `Linker` wiring per
//! WIT world arrives in M6 cutover commits that bind a representative
//! WASM plugin end-to-end through the `scalar-plugin` world.
//!
//! Until the cutover, this crate compiles cleanly behind the
//! `wasmtime-runtime` feature (default-on) but the loader's `load()`
//! returns an `WasmError::NotYetImplemented` so callers exercise the
//! plumbing without depending on a working WASM artifact.

// Rust guideline compliant
#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

pub mod buffer;
pub mod error;

/// Arrow IPC bridge — re-exported from `uni-plugin-wasm-rt`.
///
/// Lifted to the shared crate in M6.shared. `WasmIpcBuffer` (the
/// wasmtime-specific linear-memory RAII helper) stays here in
/// [`crate::buffer`].
pub mod ipc {
    pub use uni_plugin_wasm_rt::ipc::{decode_batch, decode_batches, encode_batch, encode_batches};
}

/// Instance pool — re-exported from `uni-plugin-wasm-rt`.
///
/// Type-aliased so `WasmInstancePool<T>` keeps working unchanged
/// downstream.
pub mod pool {
    pub use uni_plugin_wasm_rt::pool::{PoolConfig, PoolMetrics};

    /// Type alias — generic `InstancePool` parameterized with
    /// [`crate::WasmError`].
    pub type WasmInstancePool<T> = uni_plugin_wasm_rt::pool::InstancePool<T, crate::WasmError>;

    /// Type alias — generic `PooledInstance` parameterized with
    /// [`crate::WasmError`].
    pub type PooledInstance<T> = uni_plugin_wasm_rt::pool::PooledInstance<T, crate::WasmError>;
}

#[cfg(feature = "wasmtime-runtime")]
pub mod adapter;
#[cfg(feature = "wasmtime-runtime")]
pub mod adapter_aggregate;
#[cfg(feature = "wasmtime-runtime")]
pub mod adapter_procedure;
#[cfg(feature = "wasmtime-runtime")]
pub mod bindings;
#[cfg(feature = "wasmtime-runtime")]
pub mod host_state;
#[cfg(feature = "wasmtime-runtime")]
pub mod linker;
#[cfg(feature = "wasmtime-runtime")]
pub mod loader;
#[cfg(feature = "wasmtime-runtime")]
pub mod multi_version;

#[doc(inline)]
pub use buffer::WasmIpcBuffer;
#[doc(inline)]
pub use error::WasmError;
#[doc(inline)]
pub use pool::WasmInstancePool;

#[cfg(feature = "wasmtime-runtime")]
#[doc(inline)]
pub use loader::WasmLoader;

#[cfg(feature = "wasmtime-runtime")]
#[doc(inline)]
pub use multi_version::{MultiVersionLinker, SUPPORTED_MAJORS};
