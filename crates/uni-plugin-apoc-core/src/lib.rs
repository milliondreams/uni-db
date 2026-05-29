//! APOC-equivalent built-ins implemented in Rust.
//!
//! `uni-plugin-apoc-core` ships the subset of APOC analogues that belong
//! in the host process rather than in user-loaded Lua/WASM plugins:
//!
//! - **Perf-critical scalar functions** invoked in inner loops (`apoc.text.*`,
//!   `apoc.coll.*`, `apoc.math.*`, `apoc.convert.*`) where a row-rate Lua
//!   bridge would be a correctness-equivalent but throughput-fatal choice.
//! - **Host-intimate procedures** that need access to internal mutation,
//!   schema, or catalog APIs not exposed across the capability membrane
//!   (`apoc.refactor.*`, `apoc.schema.*`, `apoc.atomic.*`).
//! - **Procedures with retry contracts or transaction-batching semantics**
//!   the host orchestrates (`apoc.atomic.*`, `apoc.periodic.iterate` body).
//!
//! Everything else — high-level orchestration, format/IO procedures,
//! external-system adapters, low-traffic utilities — lives in
//! [`uni-plugin-apoc-ext`](https://github.com/rustic-ai/uni-db) as Lua
//! source loaded at runtime through `uni-plugin-lua`.
//!
//! # Loader pattern
//!
//! ```ignore
//! use uni_plugin_apoc_core::ApocCorePlugin;
//! let db = uni::Uni::open("./data")?;
//! db.add_plugin(ApocCorePlugin::new())?;       // registers uni.apoc.*
//! ```
//!
//! In the top-level `uni` crate, the `apoc-core` cargo feature (default-on)
//! adds the dep and registers the plugin in `Uni::open`. Library embedders
//! who don't want APOC built-ins disable the feature.

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

pub mod procedures;

/// The APOC-core plugin: bundles every Rust-implemented APOC analogue.
///
/// Add via `Uni::add_plugin(ApocCorePlugin::new())`. The plugin reserves
/// the `apoc-core` plugin id; user plugins may not claim it.
#[derive(Debug)]
pub struct ApocCorePlugin {
    manifest: OnceLock<PluginManifest>,
}

impl ApocCorePlugin {
    /// Reserved plugin id.
    pub const ID: &'static str = "apoc-core";

    /// Construct a fresh `ApocCorePlugin`.
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
            docs: "APOC-equivalent built-ins implemented in Rust (perf-critical / host-intimate)."
                .to_owned(),
            metadata: std::collections::BTreeMap::new(),
        }
    }

    fn declared_capabilities() -> CapabilitySet {
        use uni_plugin::Capability;
        CapabilitySet::from_iter_of([
            Capability::ScalarFn,
            Capability::AggregateFn,
            Capability::Procedure,
            Capability::ProcedureWrites,
            Capability::ProcedureSchema,
        ])
    }
}

impl Default for ApocCorePlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl Plugin for ApocCorePlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(Self::manifest_value)
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        procedures::register_into(r)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apoc_core_plugin_constructs_with_valid_manifest() {
        let p = ApocCorePlugin::new();
        let m = p.manifest();
        assert_eq!(m.id.as_str(), ApocCorePlugin::ID);
        assert!(m.abi.matches(1));
        assert!(!m.capabilities.is_empty());
    }

    #[test]
    fn manifest_is_cached_across_calls() {
        let p = ApocCorePlugin::new();
        let a = p.manifest();
        let b = p.manifest();
        assert!(std::ptr::eq(a, b));
    }
}
