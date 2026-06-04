/// Query rewriting framework
///
/// This module provides a general-purpose framework for transforming function calls
/// into equivalent predicate expressions at compile time. The framework enables:
///
/// - Full predicate pushdown to storage
/// - Index utilization
/// - Extensible plugin architecture for adding new rewrite rules
///
/// # Architecture
///
/// The framework consists of:
///
/// - **RewriteRule trait**: Interface for implementing rewrite transformations
/// - **RewriteRegistry**: Global registry of all rewrite rules
/// - **ExpressionWalker**: Traverses expression trees and applies rules
/// - **RewriteContext**: Contextual information during rewriting
///
/// # Example Usage
///
/// ```ignore
/// use uni_query::rewrite::rewrite_query;
///
/// // Rewrite a complete query
/// let rewritten = rewrite_query(query)?;
/// ```
///
/// # Adding New Rules
///
/// See `rules/README.md` for a guide on implementing custom rewrite rules.
pub mod context;
pub mod error;
pub mod function_rename;
pub mod registry;
pub mod rule;
pub mod rules;
pub mod walker;

use context::{RewriteContext, RewriteStats};
use error::RewriteError;
use registry::RewriteRegistry;
use walker::ExpressionWalker;

use std::sync::OnceLock;

/// Global registry of rewrite rules, initialized once on first use
static GLOBAL_REGISTRY: OnceLock<RewriteRegistry> = OnceLock::new();

/// Get the global rewrite registry, initializing it if needed
fn get_or_init_registry() -> &'static RewriteRegistry {
    GLOBAL_REGISTRY.get_or_init(|| {
        tracing::info!("Initializing query rewrite framework");
        RewriteRegistry::with_builtin_rules()
    })
}

/// Log rewrite statistics if any functions were visited
fn log_rewrite_stats(stats: &RewriteStats) {
    if stats.functions_visited > 0 {
        tracing::info!(
            "Rewrite pass complete: {} functions visited, {} rewritten, {} skipped",
            stats.functions_visited,
            stats.functions_rewritten,
            stats.functions_skipped
        );

        if !stats.errors.is_empty() {
            tracing::debug!("Rewrite errors: {:?}", stats.errors);
        }
    }
}

/// Rewrite a complete query
///
/// This is the main entry point for applying query rewrites. It walks the
/// entire query tree and applies registered rewrite rules to all function calls.
///
/// # Arguments
///
/// * `query` - The query to rewrite
///
/// # Returns
///
/// The rewritten query with function calls transformed into predicates.
///
/// # Example
///
/// ```ignore
/// let query = parse_cypher("MATCH (p)-[e:EMPLOYED_BY]->(c) WHERE uni.temporal.validAt(e, 'start', 'end', datetime('2021-06-15')) RETURN c")?;
/// let rewritten = rewrite_query(query)?;
/// // The validAt function will be transformed into: e.start <= ... AND (e.end IS NULL OR e.end >= ...)
/// ```
pub fn rewrite_query(
    query: uni_cypher::ast::Query,
) -> Result<uni_cypher::ast::Query, RewriteError> {
    let registry = get_or_init_registry();
    let context = RewriteContext::default();

    let mut walker = ExpressionWalker::new(registry, context);
    let rewritten_query = walker.rewrite_query(query);

    log_rewrite_stats(&walker.context().stats);

    Ok(rewritten_query)
}
