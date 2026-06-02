//! Rhai-script loader for the uni-db plugin framework.
//!
//! `uni-plugin-rhai` is the **host-embedded scripting loader**. It sits
//! parallel to `uni-plugin-extism` (Extism bytes-in/bytes-out WASM) and
//! `uni-plugin-wasm` (Component Model WIT) but takes a fundamentally
//! different shape: there is no WASM wrapper, no IPC, no instance pool —
//! the Rhai [`rhai::Engine`] is embedded directly in the host process.
//!
//! # Why Rhai
//!
//! Rhai fills the "dynamic AND sandboxed" loader quadrant:
//!
//! - **Pure Rust** — no C toolchain, no WASM wrapper, no separate runtime.
//!   Builds anywhere uni-db builds.
//! - **Sandboxed by language design** — Rhai has no built-in I/O. Every
//!   effectful operation comes from a host-registered function. Registering
//!   a function is opt-in; *absence* is the default and matches the
//!   framework's capability-gating contract (proposal §10.2).
//! - **Resource limits are first-class** on the `Engine`:
//!   `set_max_operations`, `set_max_call_levels`, `set_max_string_size`,
//!   `set_max_array_size`, `set_max_map_size`. No ad-hoc instruction-count
//!   shim.
//! - **Actively maintained** on crates.io 1.x.
//!
//! # Capability gating — Engine-import absence
//!
//! The capability-enforcement layer 2 for Rhai is *Engine-import absence*:
//! the loader registers a host function (`uni.fs.read`, `uni.http.get`,
//! `uni.query`, …) on the per-plugin Engine **only when** the corresponding
//! capability is in the effective grant set. A plugin without
//! `Capability::Filesystem` cannot call `uni.fs.read` — Rhai raises
//! `ErrorFunctionNotFound` at parse-resolution time. This is the in-host
//! analogue of Component Model's linker-absence guarantee.
//!
//! # Crate status
//!
//! Implemented in phases. Phase 1 (this) ships the crate scaffold;
//! phases 2+ wire the Engine factory, manifest parser, adapters, and
//! `Uni::load_rhai_plugin`.

// Rust guideline compliant
#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

pub mod error;
pub mod host_fns;
pub mod wire_translate;

#[cfg(feature = "rhai-runtime")]
pub mod host_fn_impls;

#[cfg(feature = "rhai-runtime")]
pub mod adapter;
#[cfg(feature = "rhai-runtime")]
pub mod adapter_aggregate;
#[cfg(feature = "rhai-runtime")]
pub mod adapter_procedure;
#[cfg(feature = "rhai-runtime")]
pub mod columns;
#[cfg(feature = "rhai-runtime")]
pub mod dynamic_bridge;
#[cfg(feature = "rhai-runtime")]
pub mod engine;
#[cfg(feature = "rhai-runtime")]
pub mod loader;
#[cfg(feature = "rhai-runtime")]
pub mod manifest;
#[cfg(feature = "rhai-runtime")]
pub mod runtime;

#[doc(inline)]
pub use error::RhaiError;
#[doc(inline)]
pub use host_fns::{RhaiHostFnRegistry, RhaiHostFnSpec};

#[cfg(feature = "rhai-runtime")]
#[doc(inline)]
pub use adapter::RhaiScalarFn;
#[cfg(feature = "rhai-runtime")]
#[doc(inline)]
pub use adapter_aggregate::{RhaiAccumulator, RhaiAggregateFn};
#[cfg(feature = "rhai-runtime")]
#[doc(inline)]
pub use adapter_procedure::RhaiProcedure;
#[cfg(feature = "rhai-runtime")]
#[doc(inline)]
pub use engine::{DEFAULT_MAX_CALL_LEVELS, build_engine};
#[cfg(feature = "rhai-runtime")]
#[doc(inline)]
pub use loader::{LoadOutcome, RhaiLoader};
#[cfg(feature = "rhai-runtime")]
#[doc(inline)]
pub use manifest::{AggregateEntry, ProcedureEntry, RhaiManifest, ScalarEntry};
#[cfg(feature = "rhai-runtime")]
#[doc(inline)]
pub use runtime::RhaiPluginRuntime;
