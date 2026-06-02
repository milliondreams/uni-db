//! Error types for the Rhai loader.

use thiserror::Error;

/// Errors specific to the Rhai loader.
///
/// Mirrors `uni_plugin_extism::ExtismError` in shape so the three
/// loaders surface comparable failure modes.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RhaiError {
    /// The supplied Rhai source failed to compile, or did not declare a
    /// `uni_manifest()` function returning the expected shape.
    #[error("rhai plugin: invalid source or manifest: {0}")]
    InvalidPlugin(String),

    /// The plugin's declared manifest did not pass validation
    /// (unknown ABI, missing required fields, unknown type names).
    #[error("rhai plugin manifest invalid: {0}")]
    ManifestInvalid(String),

    /// Rhai source failed to parse — the engine could not produce an AST.
    /// The wrapped string includes Rhai's file:line:col context.
    #[error("rhai parse failed: {0}")]
    ParseFailed(String),

    /// Runtime error during script execution (type mismatch, arithmetic
    /// error, host-fn panic). Wraps Rhai's runtime error message.
    #[error("rhai runtime error: {0}")]
    RuntimeError(String),

    /// Conversion between Rhai `Dynamic` and Arrow `ScalarValue` /
    /// `ArrayRef` failed (unsupported type, precision loss, null handling).
    #[error("rhai <-> arrow conversion failure: {0}")]
    Conversion(String),

    /// Internal / unexpected error.
    #[error("uni-plugin-rhai internal error: {0}")]
    Internal(String),
}
