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

    /// A capability-gated host function was referenced by a script that
    /// did not receive the matching grant. Rhai raises
    /// `ErrorFunctionNotFound`; the loader normalises that into this
    /// variant when the function name maps to a known host fn in the
    /// registry.
    #[error("rhai plugin called host fn `{host_fn}` without {capability} grant")]
    CapabilityDenied {
        /// Host function the plugin attempted to invoke.
        host_fn: String,
        /// The capability the plugin would need.
        capability: String,
    },

    /// Runtime error during script execution (type mismatch, arithmetic
    /// error, host-fn panic). Wraps Rhai's runtime error message.
    #[error("rhai runtime error: {0}")]
    RuntimeError(String),

    /// Conversion between Rhai `Dynamic` and Arrow `ScalarValue` /
    /// `ArrayRef` failed (unsupported type, precision loss, null handling).
    #[error("rhai <-> arrow conversion failure: {0}")]
    Conversion(String),

    /// Resource limit exceeded: `set_max_operations`, wall-clock deadline,
    /// `set_max_call_levels`, or a memory cap.
    #[error("rhai plugin exceeded resource limit: {0}")]
    ResourceLimit(String),

    /// Loader scaffolding shipped without a complete cutover for this entry
    /// point. M7 phase commits remove these.
    #[error("uni-plugin-rhai: {feature} not yet wired (M7 in progress)")]
    NotYetImplemented {
        /// The not-yet-wired feature.
        feature: String,
    },

    /// Internal / unexpected error.
    #[error("uni-plugin-rhai internal error: {0}")]
    Internal(String),
}

impl RhaiError {
    /// Construct a `NotYetImplemented` for the named feature.
    #[must_use]
    pub fn not_yet(feature: impl Into<String>) -> Self {
        Self::NotYetImplemented {
            feature: feature.into(),
        }
    }
}
