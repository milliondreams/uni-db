//! Logical type plugins — Arrow extension types as a plugin surface.
//!
//! Logical types are exposed via Arrow's extension-type mechanism: the
//! type identity travels in the Arrow `Field`'s `metadata` under the
//! standard `ARROW:extension:name` (e.g., `"geo.point"`) and
//! `ARROW:extension:metadata` keys.

use arrow_schema::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;

use crate::errors::FnError;

/// A logical type plugin (Arrow extension type).
pub trait LogicalTypeProvider: Send + Sync {
    /// Extension name stored in `ARROW:extension:name`.
    fn name(&self) -> &str;

    /// Physical Arrow storage type backing this logical type.
    fn arrow_type(&self) -> DataType;

    /// Parse a Cypher / Locy literal into the logical-typed scalar.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the literal is malformed for this type.
    #[allow(
        clippy::wrong_self_convention,
        reason = "method belongs to the provider, not the literal"
    )]
    fn from_literal(&self, s: &str) -> Result<ScalarValue, FnError>;

    /// Render a logical-typed value for display.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the value cannot be rendered.
    fn to_display(&self, v: &ScalarValue) -> Result<String, FnError>;

    /// Convert this logical-typed column to a different target type.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the cast is unsupported.
    fn cast_to(&self, v: &ColumnarValue, target: &DataType) -> Result<ColumnarValue, FnError>;

    /// Convert from a physical column to this logical type.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the source representation is incompatible.
    fn cast_from(&self, v: &ColumnarValue) -> Result<ColumnarValue, FnError>;

    /// Optional opaque version stamp for the on-disk encoding.
    ///
    /// Default is `"1"`. Override when bumping an encoding-breaking
    /// change so [`Self::compat_check`] can reject a reload that would
    /// silently mis-decode already-persisted data.
    fn encoding_version(&self) -> &str {
        "1"
    }

    /// Reject a reload that would change the Arrow extension contract.
    ///
    /// Default implementation enforces the §11.2.1 invariant: the new
    /// provider must keep the same extension `name()` *and* the same
    /// physical `arrow_type()` *and* the same [`Self::encoding_version`]
    /// as the old provider. Any mismatch is a hard reload error
    /// because previously-stored values would otherwise become
    /// unreadable.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] (code [`FnError::CODE_TYPE_COERCION`]) when
    /// the new provider's contract differs from the old.
    fn compat_check(&self, old: &dyn LogicalTypeProvider) -> Result<(), FnError> {
        if self.name() != old.name() {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "logical-type reload changed extension name: {} → {}",
                    old.name(),
                    self.name()
                ),
            ));
        }
        if self.arrow_type() != old.arrow_type() {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "logical-type {} reload changed arrow type: {:?} → {:?}",
                    self.name(),
                    old.arrow_type(),
                    self.arrow_type()
                ),
            ));
        }
        if self.encoding_version() != old.encoding_version() {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "logical-type {} reload changed encoding version: {} → {}",
                    self.name(),
                    old.encoding_version(),
                    self.encoding_version()
                ),
            ));
        }
        Ok(())
    }
}
