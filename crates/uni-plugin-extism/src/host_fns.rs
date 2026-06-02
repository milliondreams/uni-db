//! Host function registration surface for Extism plugins.
//!
//! Unlike the Component Model loader — where capability-gated imports
//! live in per-major `wasmtime::Linker`s and are added or omitted based
//! on the granted capability set — Extism plugins import host fns by
//! name and the host registers them imperatively at plugin construction
//! time. This module owns that registration surface.
//!
//! Per proposal §10.2, the Extism path collapses to one enforcement
//! layer: host-fn-body capability checks. The [`HostFnRegistry`] is
//! the bookkeeping that makes those checks consistent across host fns
//! (every gated fn checks its capability against the calling plugin's
//! grants via the same helper, not ad-hoc).

use std::collections::BTreeMap;

/// Registry of host functions available to Extism plugins.
///
/// The host populates this once per uni-db instance with all gateable
/// imports (`host_fs_read`, `host_net_http_get`, `host_query_run`,
/// `host_kms_sign`, `host_secrets_acquire`, etc). At plugin load time,
/// the loader filters the registry through the plugin's granted
/// capability set and registers only the matching host fns into the
/// Extism plugin's import table.
///
/// The capability check at call time is mechanical: each gated host fn
/// looks up its `HostFnSpec` in the registry and verifies the plugin's
/// grants. Plugins that bypass the capability check (because the host
/// author forgot) cannot exist — every host fn invocation routes
/// through the registry's check helper.
///
/// # Status
///
/// M6a scaffolding: the public API surface is in place; the actual
/// `Function` registration into Extism's runtime arrives in the cutover.
#[derive(Debug, Default)]
pub struct HostFnRegistry {
    specs: BTreeMap<String, HostFnSpec>,
}

/// Metadata describing a single host function exposed to Extism plugins.
#[derive(Debug, Clone)]
pub struct HostFnSpec {
    /// Name plugins use to import this fn (e.g., `"host_fs_read"`).
    pub name: String,
    /// Capability required for this fn to be visible to a plugin. `None`
    /// means always-available (e.g., `host_log`). Matched by *variant* against
    /// the plugin's effective set (attenuation patterns are enforced in the
    /// host-fn body, not at the visibility gate).
    pub required_capability: Option<uni_plugin::Capability>,
    /// Human-readable description; surfaced via `uni plugin info`.
    pub docs: String,
}

impl HostFnRegistry {
    /// Construct a fresh, empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a host fn spec.
    pub fn register(&mut self, spec: HostFnSpec) {
        self.specs.insert(spec.name.clone(), spec);
    }

    /// Look up a registered spec by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&HostFnSpec> {
        self.specs.get(name)
    }

    /// Iterate all registered specs.
    pub fn iter(&self) -> impl Iterator<Item = &HostFnSpec> {
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
        let r = HostFnRegistry::new();
        assert!(r.is_empty());
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn register_and_lookup_round_trip() {
        let mut r = HostFnRegistry::new();
        r.register(HostFnSpec {
            name: "host_fs_read".to_owned(),
            required_capability: Some(uni_plugin::Capability::Filesystem {
                read: vec![],
                write: vec![],
            }),
            docs: "Read a file from the host filesystem.".to_owned(),
        });
        let spec = r.get("host_fs_read").expect("registered");
        assert!(matches!(
            spec.required_capability,
            Some(uni_plugin::Capability::Filesystem { .. })
        ));
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn always_available_fns_have_no_required_capability() {
        let mut r = HostFnRegistry::new();
        r.register(HostFnSpec {
            name: "host_log".to_owned(),
            required_capability: None,
            docs: "Emit a tracing event.".to_owned(),
        });
        let spec = r.get("host_log").expect("registered");
        assert!(spec.required_capability.is_none());
    }

    #[test]
    fn iter_yields_registered_specs() {
        let mut r = HostFnRegistry::new();
        r.register(HostFnSpec {
            name: "a".to_owned(),
            required_capability: None,
            docs: String::new(),
        });
        r.register(HostFnSpec {
            name: "b".to_owned(),
            required_capability: Some(uni_plugin::Capability::Network { allow: vec![] }),
            docs: String::new(),
        });
        let names: Vec<&str> = r.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);
    }
}
