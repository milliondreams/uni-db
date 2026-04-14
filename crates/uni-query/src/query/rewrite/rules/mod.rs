/// Built-in rewrite rules
pub mod btic;
pub mod temporal;

use crate::query::rewrite::registry::RewriteRegistry;
use std::sync::Arc;

/// Register all built-in rewrite rules
pub fn register_builtin_rules(registry: &mut RewriteRegistry) {
    // Register temporal rules
    registry.register(Arc::new(temporal::ValidAtRule));
    registry.register(Arc::new(temporal::OverlapsRule));
    registry.register(Arc::new(temporal::PrecedesRule));
    registry.register(Arc::new(temporal::SucceedsRule));
    registry.register(Arc::new(temporal::IsOngoingRule));
    registry.register(Arc::new(temporal::HasClosedRule));

    // BTIC rules: available but not registered by default.
    // btic_contains_point rewrite decomposes into btic_lo/btic_hi range
    // predicates, but btic_lo returns DateTime while the point argument is
    // typically Int (ms-since-epoch), causing a type mismatch.  Enable once
    // the pushdown layer can handle the decomposed form end-to-end.
    // registry.register(Arc::new(btic::BticContainsPointRule));

    tracing::debug!("Registered {} built-in rewrite rules", registry.len());
}
