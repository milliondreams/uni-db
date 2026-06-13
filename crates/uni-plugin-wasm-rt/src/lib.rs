//! Shared runtime helpers for the uni-db WASM plugin loaders.
//!
//! `uni-plugin-wasm-rt` is the M6.shared lift — a crate that sits below
//! both `uni-plugin-extism` and `uni-plugin-wasm` in the dependency
//! graph and owns the two pieces of machinery they would otherwise
//! duplicate:
//!
//! - **Arrow IPC bridge** ([`ipc`]) — `RecordBatch` ↔ stream bytes,
//!   shared between Extism's bytes-in/bytes-out boundary and the
//!   Component Model's linear-memory boundary.
//! - **Per-plugin instance cache** ([`pool`]) — generic over the
//!   per-invoke instance type and the loader's error type. Both loaders
//!   alias this with their concrete `T` and error. It builds a fresh
//!   instance per acquire (so guest state can't leak across calls) and
//!   enforces a concurrency cap.
//!
//! Neither piece depends on extism or wasmtime; both depend only on
//! `arrow-ipc` and `parking_lot`. That keeps the
//! crate small and lets it stay below `uni-plugin` in the workspace
//! dep graph, so the trait-only embedder pays nothing for plumbing
//! they never invoke.

// Rust guideline compliant
#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

pub mod error;
pub mod ipc;
pub mod pool;

#[doc(inline)]
pub use error::IpcError;
#[doc(inline)]
pub use ipc::{decode_batch, decode_batches, encode_batch, encode_batches};
#[doc(inline)]
pub use pool::{InstancePool, PoolConfig, PoolMetrics, PooledInstance};
