//! Host function registration surface for Rhai plugins.
//!
//! The Rhai equivalent of `uni_plugin_extism::HostFnRegistry`. The host
//! populates this once per uni-db instance with all gateable host fns
//! (`uni.fs.read`, `uni.http.get`, `uni.query`, `uni.kms.sign`,
//! `uni.secret.acquire`, …). At plugin load time,
//! [`crate::engine::build_engine`] filters the registry through the
//! plugin's granted capability set and registers only the matching fns
//! onto the per-plugin `rhai::Engine`.
//!
//! Per proposal §10.2, Rhai uses **Engine-import absence** as its
//! capability-enforcement layer 2 — ungranted host fns are simply not
//! registered, so the script fails at parse-resolution with Rhai's
//! `ErrorFunctionNotFound`. This is the in-host analogue of CM's
//! linker-absence guarantee.

use std::collections::BTreeMap;
use std::sync::Arc;

use uni_plugin::Capability;

/// Type-erased registrar closure invoked at engine-construction time.
///
/// Each closure receives a mutable `rhai::Engine` and the plugin's effective
/// [`uni_plugin::CapabilitySet`], then registers one or more host functions via
/// `Engine::register_fn`. The closure is only invoked when the host fn's
/// required capability is in the effective set — ungranted fns are never
/// registered (layer-2, Engine-import absence). The capability set is passed in
/// so a registered fn can additionally enforce **call-time attenuation**
/// (layer 3): e.g. matching a requested URL against a granted
/// `Network { allow }` allow-list, or a key id against `Kms { key_ids }`.
#[cfg(feature = "rhai-runtime")]
pub type RhaiHostFnRegister =
    Arc<dyn Fn(&mut rhai::Engine, &uni_plugin::CapabilitySet) + Send + Sync + 'static>;

#[cfg(not(feature = "rhai-runtime"))]
pub type RhaiHostFnRegister = Arc<dyn Fn() + Send + Sync + 'static>;

/// Metadata + registrar describing a single host function exposed to
/// Rhai plugins.
#[derive(Clone)]
pub struct RhaiHostFnSpec {
    /// Symbolic name surfaced to scripts (e.g., `"uni.fs.read"`). Used
    /// only for capability-gating decisions and for the `info` /
    /// `denied_capabilities` reporting in `LoadOutcome`. The actual Rhai
    /// function name registered on the Engine is determined by the
    /// `register` closure — typically by calling
    /// `engine.register_fn("uni_fs_read", ...)`.
    pub name: String,

    /// Capability required for this fn to be visible to a plugin. `None`
    /// means always-available.
    pub required_capability: Option<Capability>,

    /// Human-readable description; surfaced via `uni plugin info`.
    pub docs: String,

    /// Closure that registers the fn(s) on a freshly-built Rhai engine
    /// when the required capability is granted.
    pub register: RhaiHostFnRegister,
}

#[cfg(feature = "rhai-runtime")]
impl RhaiHostFnSpec {
    /// Build a spec for a capability-gated host fn.
    ///
    /// Convenience constructor for the common shape used by the built-in
    /// host-fn modules (`fs`, `net`, `kms`, `secret`): a symbolic `name`, a
    /// `required_capability` placeholder, human-readable `docs`, and a
    /// `register` closure. Folds the four-field struct literal into one call
    /// so a new field can't silently diverge across call sites.
    #[must_use]
    pub fn gated(
        name: impl Into<String>,
        required_capability: Capability,
        docs: impl Into<String>,
        register: impl Fn(&mut rhai::Engine, &uni_plugin::CapabilitySet) + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            required_capability: Some(required_capability),
            docs: docs.into(),
            register: Arc::new(register),
        }
    }
}

impl std::fmt::Debug for RhaiHostFnSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RhaiHostFnSpec")
            .field("name", &self.name)
            .field("required_capability", &self.required_capability)
            .field("docs", &self.docs)
            .finish_non_exhaustive()
    }
}

/// Registry of host functions available to Rhai plugins.
#[derive(Debug, Default, Clone)]
pub struct RhaiHostFnRegistry {
    specs: BTreeMap<String, RhaiHostFnSpec>,
}

impl RhaiHostFnRegistry {
    /// Construct a fresh, empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a host fn spec.
    pub fn register(&mut self, spec: RhaiHostFnSpec) {
        self.specs.insert(spec.name.clone(), spec);
    }

    /// Look up a registered spec by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&RhaiHostFnSpec> {
        self.specs.get(name)
    }

    /// Iterate all registered specs in insertion-name order.
    pub fn iter(&self) -> impl Iterator<Item = &RhaiHostFnSpec> {
        self.specs.values()
    }

    /// Number of registered host fns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.specs.len()
    }

    /// Returns true if no host fns are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_starts_empty() {
        let r = RhaiHostFnRegistry::new();
        assert!(r.is_empty());
        assert_eq!(r.len(), 0);
    }

    #[cfg(feature = "rhai-runtime")]
    #[test]
    fn register_and_lookup_round_trip() {
        let mut r = RhaiHostFnRegistry::new();
        r.register(RhaiHostFnSpec {
            name: "uni.fs.read".to_owned(),
            required_capability: Some(Capability::Filesystem {
                read: vec!["/data/**".into()],
                write: vec![],
            }),
            docs: "Read a file from the host filesystem.".to_owned(),
            register: Arc::new(|_engine, _caps| { /* stub */ }),
        });
        let spec = r.get("uni.fs.read").expect("registered");
        assert!(spec.required_capability.is_some());
        assert_eq!(r.len(), 1);
    }

    #[cfg(feature = "rhai-runtime")]
    #[test]
    fn always_available_fns_have_no_required_capability() {
        let mut r = RhaiHostFnRegistry::new();
        r.register(RhaiHostFnSpec {
            name: "uni.log".to_owned(),
            required_capability: None,
            docs: "Emit a tracing event.".to_owned(),
            register: Arc::new(|_engine, _caps| {}),
        });
        let spec = r.get("uni.log").expect("registered");
        assert!(spec.required_capability.is_none());
    }
}
