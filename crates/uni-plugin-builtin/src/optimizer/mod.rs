//! Built-in DataFusion optimizer rules (M5h).
//!
//! These rules are registered through the plugin registry and consulted
//! at `SessionContext` construction time in `uni-query`. The flagship
//! rule today is [`PushdownNegotiationRule`], which lets storage backends
//! / catalog tables that opt into the
//! [`uni_plugin::traits::pushdown`] marker traits negotiate filter
//! pushdown with the planner.
//
// Rust guideline compliant

use std::sync::Arc;

use uni_plugin::{PluginError, PluginRegistrar};

pub mod pushdown_negotiation;

pub use pushdown_negotiation::{
    PushdownAwareTable, PushdownMarkers, PushdownNegotiationProvider, PushdownNegotiationRule,
};

/// Register all built-in optimizer-rule providers into `r`.
///
/// # Errors
///
/// Returns [`PluginError`] only if the registrar surface itself rejects a
/// registration (the providers themselves are append-mode and cannot
/// conflict on qname).
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.optimizer_rule(Arc::new(PushdownNegotiationProvider))?;
    Ok(())
}
