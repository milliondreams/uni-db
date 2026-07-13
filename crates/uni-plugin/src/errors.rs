//! Error types for the plugin framework.
//!
//! Errors are split between [`PluginError`] (framework-level failures —
//! invalid manifest, capability denied, duplicate registration, ABI
//! mismatch) and [`FnError`] (per-invocation failures returned by a plugin's
//! work function and wrapped into a `UniError::Plugin` by the host adapter).

use std::fmt;

use thiserror::Error;

use crate::capability::Capability;
use crate::qname::QName;

/// Errors surfaced by the plugin framework itself.
///
/// `PluginError` covers framework operations: manifest parsing, capability
/// checks, registration validation, dependency resolution, WASM loading,
/// signing. Per-invocation errors from plugin code are represented by
/// [`FnError`] instead.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PluginError {
    /// The supplied manifest could not be parsed.
    #[error("plugin manifest parse failure: {0}")]
    ManifestParse(String),

    /// The manifest's `abi` range does not intersect any host-supported major.
    #[error(
        "plugin {plugin} requires uni-plugin ABI {required}; \
         host supports majors {supported:?}"
    )]
    AbiUnsupported {
        /// Plugin id reporting the mismatch.
        plugin: String,
        /// Required ABI range from the manifest.
        required: String,
        /// Host-supported major versions.
        supported: Vec<u64>,
    },

    /// A registration was attempted without the required capability.
    #[error("plugin attempted registration requiring capability {0:?}; not granted")]
    CapabilityRequired(Capability),

    /// A capability the plugin requested was denied by the host loader.
    #[error("plugin requested capability {0:?}; denied by host")]
    CapabilityDenied(Capability),

    /// An algorithm declared a capability slice/version the host lacks.
    ///
    /// Raised at load time when a provider's [`AlgorithmSignature::check_slices`]
    /// finds a requirement the host cannot satisfy, so a version mismatch fails
    /// registration with a clear message instead of trapping later on an unknown
    /// kernel op (proposal §4.3 / decision D6).
    ///
    /// [`AlgorithmSignature::check_slices`]: crate::traits::algorithm::AlgorithmSignature::check_slices
    #[error("plugin declared an unavailable capability slice: {0}")]
    SliceUnavailable(String),

    /// Two registrations attempted to claim the same qualified name.
    #[error("duplicate registration for qualified name {0}")]
    DuplicateRegistration(QName),

    /// A `depends_on` entry referenced a missing or version-incompatible plugin.
    #[error("plugin {dependent} depends on {dep_id} (req {req}); not satisfied")]
    DependencyMissing {
        /// Plugin id whose manifest declared the dependency.
        dependent: String,
        /// Missing dependency id.
        dep_id: String,
        /// Version requirement from the manifest.
        req: String,
    },

    /// A cycle was detected in the dependency graph.
    #[error("dependency cycle in plugin graph: {0:?}")]
    DependencyCycle(Vec<String>),

    /// The manifest's signature failed verification against the trust root.
    #[error("plugin manifest signature invalid: {0}")]
    SignatureInvalid(String),

    /// The plugin's hash did not match the pinned blake3 digest.
    #[error("plugin hash mismatch: expected {expected}, actual {actual}")]
    HashMismatch {
        /// Hash declared in the manifest.
        expected: String,
        /// Hash actually computed at load.
        actual: String,
    },

    /// WASM component instantiation failed (loader-side).
    #[error("WASM instantiate failure: {0}")]
    WasmInstantiate(String),

    /// Lua source parse / compile failed.
    #[error("Lua plugin parse failure: {0}")]
    LuaParse(String),

    /// Rhai source parse / compile failed.
    #[error("Rhai plugin parse failure: {0}")]
    RhaiParse(String),

    /// A qualified name failed to parse.
    #[error("invalid qualified name: `{0}`")]
    InvalidQName(String),

    /// A logical type registration conflicted with an existing extension type.
    #[error("logical-type conflict: extension name `{0}` already registered")]
    LogicalTypeConflict(String),

    /// Storage scheme already registered.
    #[error("storage scheme `{0}` already registered")]
    StorageSchemeConflict(String),

    /// Catch-all for genuinely internal errors that don't map to a variant above.
    #[error("internal plugin-framework error: {0}")]
    Internal(String),
}

impl PluginError {
    /// Construct an [`PluginError::Internal`] with a descriptive message.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

/// Per-invocation error returned by a plugin's work function.
///
/// `FnError` is what crosses the host↔plugin boundary on every call. The
/// host wraps it into the user-facing error chain. WASM plugins return this
/// shape over the WIT `fn-error` record.
#[derive(Clone, Debug)]
pub struct FnError {
    /// Plugin-defined error code. Reserved range `0..=0xFF` for framework
    /// errors; plugins use `0x100..=u32::MAX`.
    pub code: u32,
    /// Human-readable error message.
    pub message: String,
    /// Whether the caller should retry the operation (e.g., transient
    /// network failure).
    pub retryable: bool,
}

impl FnError {
    /// Build an `FnError` with the given code and message; not retryable.
    #[must_use]
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
        }
    }

    /// Build a retryable `FnError`.
    #[must_use]
    pub fn retryable(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: true,
        }
    }

    /// Framework-reserved code for "unknown function name at dispatch site".
    pub const CODE_UNKNOWN_FUNCTION: u32 = 0x01;
    /// Framework-reserved code for "type-coercion failure on input column".
    pub const CODE_TYPE_COERCION: u32 = 0x02;
    /// Framework-reserved code for "null encountered where forbidden".
    pub const CODE_UNEXPECTED_NULL: u32 = 0x03;
    /// Framework-reserved code for "resource limit exceeded".
    pub const CODE_RESOURCE_LIMIT: u32 = 0x04;
    /// Framework-reserved code for "plugin attempted forbidden side effect".
    pub const CODE_FORBIDDEN: u32 = 0x05;

    /// Convenience constructor for "unknown function" errors.
    #[must_use]
    pub fn unknown_function(name: impl AsRef<str>) -> Self {
        Self::new(
            Self::CODE_UNKNOWN_FUNCTION,
            format!("unknown function: {}", name.as_ref()),
        )
    }
}

impl fmt::Display for FnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "plugin fn error (code={}, retryable={}): {}",
            self.code, self.retryable, self.message
        )
    }
}

impl std::error::Error for FnError {}

/// Errors produced by the hot-reload pipeline.
///
/// Surfaced by [`crate::reload::ReloadDispatcher::dispatch`] and by
/// the host's `Uni::reload` / `Uni::remove_plugin` entry points. Each
/// variant maps to a distinct failure mode of the §11.2 epoch-fenced
/// cutover: a drain-state-machine failure, a per-kind schema-compat
/// rejection, a persistence/round-trip failure on a stateful surface,
/// or a generic plugin-framework error wrapped through.
///
/// Reload failures abort the cutover **before** the new plugin's
/// surfaces are committed to the registry, so the registry stays
/// consistent with the still-active old plugin.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ReloadError {
    /// The drain state machine rejected the request.
    #[error("drain failure during reload: {0}")]
    Drain(String),

    /// A per-kind schema-compat check rejected the new provider.
    ///
    /// Holds the kind name (e.g., `"crdt:lww-register"`) and a
    /// human-readable explanation of the incompatibility.
    #[error("schema-incompat for {kind}: {reason}")]
    SchemaIncompat {
        /// Per-kind discriminator with a `kind:value` prefix
        /// (`"crdt:lww-register"`, `"logical-type:geo.point"`).
        kind: String,
        /// Human-readable explanation of the incompatibility.
        reason: String,
    },

    /// A stateful surface failed to persist/round-trip during reload.
    #[error("persist/restore failure during reload: {0}")]
    Persist(FnError),

    /// The new plugin's `register()` (or other framework op) failed.
    #[error(transparent)]
    Plugin(#[from] PluginError),

    /// The host lookup for a plugin handle came up empty.
    #[error("plugin {0} not found in host registry")]
    PluginNotFound(String),
}

impl ReloadError {
    /// Convenience constructor for a schema-incompatibility rejection.
    #[must_use]
    pub fn schema_incompat(kind: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::SchemaIncompat {
            kind: kind.into(),
            reason: reason.into(),
        }
    }
}

/// Outcome of a host-side hook invocation.
///
/// Hooks may continue normally, request a rewrite of the operation, or
/// reject the operation outright with a reason.
#[derive(Debug)]
#[non_exhaustive]
pub enum HookOutcome {
    /// Continue normally.
    Continue,
    /// Reject the operation; surfaced as `UniError::HookRejected`.
    Reject {
        /// Human-readable rejection reason.
        reason: String,
    },
}

impl HookOutcome {
    /// Build a `Reject` outcome with the given reason.
    #[must_use]
    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject {
            reason: reason.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fn_error_constructors() {
        let e = FnError::new(0x100, "boom");
        assert_eq!(e.code, 0x100);
        assert!(!e.retryable);
        assert_eq!(e.message, "boom");

        let e = FnError::retryable(0x101, "transient");
        assert!(e.retryable);

        let e = FnError::unknown_function("nope");
        assert_eq!(e.code, FnError::CODE_UNKNOWN_FUNCTION);
        assert!(e.message.contains("nope"));
    }

    #[test]
    fn plugin_error_internal_constructor() {
        let e = PluginError::internal("oops");
        match e {
            PluginError::Internal(message) => assert_eq!(message, "oops"),
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    #[test]
    fn plugin_error_display_contains_context() {
        let e = PluginError::HashMismatch {
            expected: "abc".to_owned(),
            actual: "def".to_owned(),
        };
        let s = e.to_string();
        assert!(s.contains("abc"));
        assert!(s.contains("def"));
    }
}
