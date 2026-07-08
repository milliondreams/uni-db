//! Built-in plugin registrations for uni-db.
//!
//! `uni-plugin-builtin` re-implements every built-in uni-db extension —
//! scalar functions, aggregates, procedures, algorithms, CRDTs, storage
//! backends, index kinds, hooks — as registrations against the
//! [`uni-plugin`](uni_plugin) framework. This is the **dogfooding crate**:
//! built-ins go through the same registration path as user plugins. If a
//! built-in cannot be expressed through the framework, the framework is
//! wrong and we fix the framework.
//!
//! The host (`uni-db::Uni::new`) constructs a single [`BuiltinPlugin`] and
//! registers it before any user plugin is allowed to load. Every extension
//! a user plugin can shadow has the same shape as its built-in counterpart.
//!
//! # Layout (built up across milestones)
//!
//! - M2 — [`scalar_fns`] migration (string, math, time, list, vector, …)
//! - M3 — `locy_aggregates` (`MIN` / `MAX` / `SUM` / `MNOR` / `MPROD` / …)
//! - M4 — `procedures/` (`uni.admin.*` / `uni.schema.*` / `uni.vector.*` / …)
//! - M5 — `storage_lance`, `index_vector`, `algorithms/*`, `crdts`, hooks
//!
//! Each milestone adds modules. The [`BuiltinPlugin::register`]
//! implementation grows accordingly; new built-ins are added by registering
//! them in a new module's `register_into(r)` function, not by adding
//! special-case code anywhere outside `uni-plugin-builtin`.

// Rust guideline compliant
#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

use std::sync::OnceLock;

use semver::Version;
use uni_plugin::{
    AbiRange, CapabilitySet, Determinism, Plugin, PluginError, PluginId, PluginManifest,
    PluginRegistrar, ProvidedSurfaces, Scope, SideEffects,
};

pub mod algorithms;
pub mod auth;
pub mod background_jobs;
pub mod collations;
pub mod crdts;
pub mod extras;
pub mod hooks;
pub mod index_vector;
pub mod locy_aggregates;
pub mod logical_types;
pub mod optimizer;
pub mod procedures;
pub mod scalar_fns;
pub mod storage;
pub mod storage_table_provider;
pub mod triggers;

/// The single built-in plugin bundled with uni-db.
///
/// Construct with [`BuiltinPlugin::new`] and add to a `Uni` instance via
/// `Uni::add_plugin(BuiltinPlugin::new())`. The plugin's manifest declares
/// every capability uni-db's built-ins use; the manifest's `id` is
/// [`BuiltinPlugin::ID`] and is reserved — no user plugin may claim it.
#[derive(Debug)]
pub struct BuiltinPlugin {
    manifest: OnceLock<PluginManifest>,
}

impl BuiltinPlugin {
    /// Reserved plugin id for the built-in plugin.
    pub const ID: &'static str = "builtin";

    /// Construct a fresh `BuiltinPlugin`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            manifest: OnceLock::new(),
        }
    }

    fn manifest_value() -> PluginManifest {
        PluginManifest {
            id: PluginId::new(Self::ID),
            version: env!("CARGO_PKG_VERSION")
                .parse::<Version>()
                .unwrap_or_else(|_| Version::new(0, 0, 0)),
            abi: AbiRange::parse("^1").expect("manifest ABI range is valid"),
            depends_on: vec![],
            capabilities: Self::declared_capabilities(),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "Built-in uni-db extensions registered through the plugin framework.".to_owned(),
            metadata: std::collections::BTreeMap::new(),
        }
    }

    fn declared_capabilities() -> CapabilitySet {
        use uni_plugin::Capability;
        CapabilitySet::from_iter_of([
            // Surface capabilities — every built-in registration kind.
            Capability::ScalarFn,
            Capability::AggregateFn,
            Capability::WindowFn,
            Capability::Procedure,
            Capability::ProcedureWrites,
            Capability::ProcedureSchema,
            Capability::ProcedureDbms,
            Capability::LocyAggregate,
            Capability::LocyPredicate,
            Capability::Operator,
            Capability::Index,
            Capability::Storage,
            Capability::Algorithm,
            Capability::Crdt,
            Capability::Hook,
            Capability::Trigger,
            Capability::Type,
            Capability::Auth,
            Capability::Authz,
            Capability::Collation,
            Capability::Catalog,
            Capability::Cdc,
        ])
    }
}

impl Default for BuiltinPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl Plugin for BuiltinPlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(Self::manifest_value)
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        // Each module registers its own built-ins into the registrar. As
        // milestones land, more modules are added here.
        scalar_fns::register_into(r)?;
        locy_aggregates::register_into(r)?;
        procedures::register_into(r)?;
        crdts::register_into(r)?;
        collations::register_into(r)?;
        hooks::register_into(r)?;
        logical_types::register_into(r)?;
        index_vector::register_into(r)?;
        triggers::register_into(r)?;
        auth::register_into(r)?;
        extras::register_into(r)?;
        optimizer::register_into(r)?;
        // NOTE: `algorithms::register_into(r)?;` is intentionally NOT
        // called here. Algorithm qnames live in the `uni` namespace
        // (e.g. `uni.algo.pageRank`) so they cannot be registered by
        // BuiltinPlugin (whose plugin id is `builtin`). The host's
        // `register_builtin_plugins` registers them under the `uni`
        // plugin id alongside the host-coupled procedures. See
        // `crates/uni/src/api/mod.rs:register_builtin_plugins`.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_plugin_constructs_with_valid_manifest() {
        let p = BuiltinPlugin::new();
        let m = p.manifest();
        assert_eq!(m.id.as_str(), BuiltinPlugin::ID);
        assert!(m.abi.matches(1));
        assert!(!m.capabilities.is_empty());
    }

    #[test]
    fn manifest_is_cached_across_calls() {
        let p = BuiltinPlugin::new();
        let a = p.manifest();
        let b = p.manifest();
        assert!(std::ptr::eq(a, b));
    }
}
