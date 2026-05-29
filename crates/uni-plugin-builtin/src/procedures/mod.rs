//! Built-in procedure registrations.
//!
//! Per `docs/plans/plugin_framework_implementation.md` §4 M4, the 50+
//! hardcoded procedures in `crates/uni-query/src/query/df_graph/procedure_call.rs`
//! migrate into per-namespace submodules. APOC-equivalent namespaces
//! (`apoc.bitwise`, `apoc.text`, `apoc.coll`, …) live in
//! `uni-plugin-apoc-core` instead; this crate covers the closed-enum
//! retirement set only (`uni.admin.*`, `uni.schema.*`, `uni.vector.*`,
//! `uni.fts.*`, `uni.temporal.*`, `uni.algo.*` adapters).
//!
//! M4 scaffolding ships the module hierarchy plus one representative
//! procedure (`uni.system.echo`) that demonstrates the
//! [`uni_plugin::traits::procedure::ProcedurePlugin`] implementation
//! pattern. Subsequent commits port real built-ins one namespace at a
//! time, deleting the corresponding match arms in `procedure_call.rs`.

pub mod periodic;
pub mod system;

use uni_plugin::{PluginError, PluginRegistrar};

/// Register all built-in procedures into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a built-in qname is
/// already taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    system::register_into(r)?;
    // Subsequent commits add:
    // admin::register_into(r)?;
    // schema::register_into(r)?;
    // vector::register_into(r)?;
    // fts::register_into(r)?;
    // temporal::register_into(r)?;
    // algo::register_into(r)?;
    Ok(())
}
