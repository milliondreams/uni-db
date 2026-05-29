//! Shared helpers reused by every loader adapter crate.
//!
//! Each of `uni-plugin-wasm`, `uni-plugin-extism`, `uni-plugin-pyo3`,
//! `uni-plugin-rhai`, and `uni-plugin-custom` previously reimplemented
//! Arrow type mapping, single-row `RecordBatch` construction, and a
//! manifest JSON round-trip on the `Loader::load` path. This module
//! hosts the consolidated versions.
//!
//! # Submodules
//!
//! - [`arrow_types`] — `ArgType` → `DataType` and wire-name → `DataType`
//!   mapping shared by every loader. Replaces four duplicated copies of
//!   `argtype_arrow` and two copies of `arrow_name_to_dt` /
//!   `arrow_name_to_datatype`.
//! - [`batch_builder`] — single-row `RecordBatch` + one-item
//!   `SendableRecordBatchStream` builders.
//!
//! Pool factory unification is **not** part of this module — the wasm
//! loader unifies its three internal factories via a crate-local
//! `WasmInstanceFactory` trait, and the extism loader keeps its single
//! factory. Pulling the factory shape into `uni-plugin` would require
//! the trait to be parameterized over wasmtime / extism types, which
//! would in turn force feature-gated re-exports here. Per §1.1's
//! "ship what unifies cleanly, leave the rest" guidance, generic pool
//! unification is deferred.
//!
//! The manifest JSON round-trip is removed by adding a `prepare_parsed`
//! entry point to each loader (`Loader::prepare_parsed(manifest, grants)`)
//! and routing `Loader::load` through it; the public
//! `Loader::prepare(json_bytes, grants)` is preserved for test and
//! external use.
//!
//! # Stability
//!
//! Helpers exported here are considered loader-internal — they live in
//! `uni-plugin` so the dependency graph stays acyclic, not because
//! out-of-tree code is expected to call them.

// Rust guideline compliant

pub mod arrow_types;
pub mod batch_builder;
