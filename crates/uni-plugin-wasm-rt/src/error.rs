//! Shared error type for the wasm-rt IPC bridge.
//!
//! Each loader wraps [`IpcError`] in its own crate-level error enum
//! via `#[from]` so the trait-surface error variants stay
//! crate-specific (`ExtismError`, `WasmError`) while the IPC code lives
//! once. The pool is generic over its own error type — see
//! [`crate::pool::InstancePool`] — so it doesn't need a shared error.

use thiserror::Error;

/// Errors raised by the Arrow IPC bridge.
///
/// Both `uni-plugin-extism` and `uni-plugin-wasm` wrap this in their
/// crate-level error enums via `#[from]`.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum IpcError {
    /// Arrow IPC encode / decode failed at the wasm boundary.
    #[error("arrow IPC error: {0}")]
    Arrow(String),
    /// Called `encode_batches` with no input — there's no schema to
    /// write into the stream header.
    #[error("encode_batches: empty input — no schema to write")]
    EmptyBatchInput,
    /// FU-2: a plugin attempted to serialize a column tagged with the
    /// `uni-db.secret-handle` Arrow extension into its output batch.
    /// The host blocks the serialization so the opaque handle cannot
    /// leak across the wasm boundary as raw bytes.
    #[error(
        "secret leak attempt: column `{column}` is tagged with the `uni-db.secret-handle` Arrow extension and cannot cross the host↔plugin IPC boundary"
    )]
    SecretLeakAttempt {
        /// Name of the offending column.
        column: String,
    },
}
