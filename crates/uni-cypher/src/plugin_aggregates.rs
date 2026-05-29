// Rust guideline compliant
//! Cross-crate registry of plugin-registered aggregate function names.
//!
//! The Cypher AST's [`crate::ast::Expr::is_aggregate`] uses a hardcoded
//! list of built-in aggregate names (`count`, `sum`, `min`, …) to
//! decide whether a `FunctionCall` routes through the planner's
//! aggregate translation. Plugin-registered aggregates (M9
//! `uni.plugin.declareAggregate` and any other
//! `uni_plugin::traits::aggregate::AggregatePluginFn` source) are
//! not in that list, so a Cypher query like `RETURN myAgg(n.value)`
//! would otherwise fall through to scalar UDF resolution and fail.
//!
//! Rather than thread a `PluginRegistry` reference through every AST
//! query, plugin registrars publish each aggregate's lowercased qname
//! into this process-wide set at registration time. The AST consults
//! it inside `is_aggregate`; the planner's own copy of the hardcoded
//! list (in `uni-query/src/query/planner.rs::is_aggregate_function_name`)
//! does the same.
//!
//! # Lifecycle
//!
//! Entries are added but never removed today (M9 declared aggregates
//! cannot be dropped while in-flight queries reference them). When
//! `dropDeclared` infrastructure matures past M11, a counterpart
//! `unregister_plugin_aggregate` will follow.

use std::collections::HashSet;
use std::sync::{OnceLock, RwLock};

fn names() -> &'static RwLock<HashSet<String>> {
    static SET: OnceLock<RwLock<HashSet<String>>> = OnceLock::new();
    SET.get_or_init(|| RwLock::new(HashSet::new()))
}

/// Register a fully-qualified aggregate name (`"namespace.local"`)
/// so the Cypher planner routes calls to it through the aggregate
/// translation path instead of scalar UDF resolution.
///
/// The name is stored lowercase. Calls are idempotent.
pub fn register_plugin_aggregate(qname: impl Into<String>) {
    let lc = qname.into().to_ascii_lowercase();
    if let Ok(mut set) = names().write() {
        set.insert(lc);
    }
}

/// Return `true` if `name` (case-insensitive) was previously registered
/// via [`register_plugin_aggregate`].
#[must_use]
pub fn is_known_plugin_aggregate(name: &str) -> bool {
    names()
        .read()
        .map(|set| set.contains(&name.to_ascii_lowercase()))
        .unwrap_or(false)
}
