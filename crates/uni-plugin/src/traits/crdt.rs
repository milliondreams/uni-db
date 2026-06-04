//! CRDT kind plugins.

pub use datafusion::scalar::ScalarValue;
use smol_str::SmolStr;

use crate::errors::FnError;

/// Identifier for a CRDT kind (`"lww-register"`, `"or-set"`, …).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CrdtKind(pub SmolStr);

impl CrdtKind {
    /// Construct a `CrdtKind` from a string.
    #[must_use]
    pub fn new(s: impl Into<SmolStr>) -> Self {
        Self(s.into())
    }
}

/// Opaque CRDT operation payload — encoding is CRDT-kind-specific.
#[derive(Clone, Debug)]
pub struct CrdtOp {
    /// Raw operation bytes.
    pub bytes: Vec<u8>,
}

/// A CRDT-kind provider.
pub trait CrdtKindProvider: Send + Sync {
    /// The CRDT kind this provider implements.
    fn kind(&self) -> CrdtKind;

    /// Construct an empty state.
    fn empty(&self) -> Box<dyn CrdtState>;

    /// Restore state from persisted bytes.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the bytes cannot be deserialized.
    #[allow(
        clippy::wrong_self_convention,
        reason = "method belongs to the provider, not the persisted bytes"
    )]
    fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError>;

    /// Reject a reload that would tear in-flight CRDT merge state.
    ///
    /// Default implementation round-trips an empty `old` state through
    /// `self.from_persisted()`. Providers that store private schema
    /// metadata (version stamps, replica id widths) should override and
    /// emit a richer compat check.
    ///
    /// A CRDT hot-swap requires that bytes produced by the **old** provider's
    /// `persist()` are still readable by the **new** provider's
    /// `from_persisted()`. Failing this check is a hard reload error.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the new provider rejects the old
    /// provider's persisted shape.
    fn schema_compat_check(&self, old: &dyn CrdtKindProvider) -> Result<(), FnError> {
        let empty_old = old.empty();
        let bytes = empty_old.persist()?;
        self.from_persisted(&bytes).map(|_| ())
    }
}

/// Per-instance CRDT state.
///
/// `'static` is required so [`CrdtState::as_any`] can downcast safely in
/// `merge` implementations to access the concrete other-state.
pub trait CrdtState: Send + Sync + 'static {
    /// Return `&dyn Any` for safe downcasting in `merge` implementations.
    ///
    /// Implementations should expose `as_any` with the one-liner
    /// `fn as_any(&self) -> &dyn std::any::Any { self }`.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Apply an operation to this state.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] on application failure.
    fn apply(&mut self, op: &CrdtOp) -> Result<(), FnError>;

    /// Merge `other` into `self` (must be associative + commutative).
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] on merge failure.
    fn merge(&mut self, other: &dyn CrdtState) -> Result<(), FnError>;

    /// Query the current logical value.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the value cannot be computed.
    fn value(&self) -> Result<ScalarValue, FnError>;

    /// Serialize for persistence.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] on serialization failure.
    fn persist(&self) -> Result<Vec<u8>, FnError>;
}
