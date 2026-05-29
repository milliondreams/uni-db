//! Plugin capabilities — declared in manifest, granted at load time.
//!
//! A `Capability` is the unit of permission in the plugin framework. Every
//! extension surface (`Capability::ScalarFn`, `Capability::Storage`, …) is
//! gated by a capability; every host import that exposes powerful primitives
//! (network, filesystem, secrets, host-side query) is gated by an attenuated
//! capability (`Capability::Network { allow }`).
//!
//! Enforcement happens in three layers:
//!
//! 1. **Registrar gate** — `PluginRegistrar::scalar_fn` etc. check the
//!    effective capability set before accepting a registration.
//! 2. **WIT linker** — for WASM plugins, host imports for capability-gated
//!    functions are linked into the wasmtime `Linker` only when the
//!    corresponding capability is granted. Ungranted host functions are
//!    not present in the plugin's imports table.
//! 3. **Runtime pattern checks** — capability grants with patterns
//!    (`Filesystem { read: vec!["/data/**"] }`) validate the actual call
//!    arguments against the pattern before dispatching.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// A single permission grant.
///
/// `Capability` is the leaf node of the permission model. A
/// [`CapabilitySet`] is a collection of capabilities.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[non_exhaustive]
pub enum Capability {
    // ---- Host import surfaces (capability-gated host functions) ----
    /// HTTP / TCP egress; allow-list of URI patterns.
    Network {
        /// Glob patterns of permitted URIs (`https://api.example/**`).
        allow: Vec<SmolStr>,
    },
    /// Filesystem read / write access with per-direction path patterns.
    Filesystem {
        /// Glob patterns of readable paths.
        read: Vec<SmolStr>,
        /// Glob patterns of writable paths.
        write: Vec<SmolStr>,
    },
    /// Invoking Cypher / Locy queries back into the host session.
    HostQuery {
        /// If `true`, only read queries are permitted.
        read_only: bool,
        /// Optional scope-restriction (label / edge-type prefixes).
        scopes: Vec<SmolStr>,
    },
    /// KMS access for sign / verify operations.
    Kms {
        /// Permitted key identifiers.
        key_ids: Vec<SmolStr>,
    },
    /// Acquiring named secret handles (opaque to the plugin).
    Secret {
        /// Permitted secret identifiers.
        ids: Vec<SmolStr>,
    },
    /// Explicit lock primitives (`host.lock_nodes`, `host.lock_edges`).
    Lock {
        /// Granularity of locks permitted.
        granularity: LockGranularity,
    },
    /// Scoped configuration K/V access (`host.config_get`).
    Config {
        /// Patterns of permitted config keys.
        keys: Vec<SmolStr>,
    },
    /// Per-plugin K/V store (scoped namespace).
    PluginStorage,

    // ---- Extension surfaces (gate Registrar methods) ----
    /// Register Cypher scalar functions.
    ScalarFn,
    /// Register Cypher aggregate functions.
    AggregateFn,
    /// Register Cypher window functions.
    WindowFn,
    /// Register Cypher procedures (read-only mode).
    Procedure,
    /// Register procedures that may mutate the graph.
    ProcedureWrites,
    /// Register procedures that may issue DDL.
    ProcedureSchema,
    /// Register administrative procedures.
    ProcedureDbms,
    /// Register Locy aggregate functions.
    LocyAggregate,
    /// Register Locy predicates (including neural).
    LocyPredicate,
    /// Register physical operators / optimizer rules.
    Operator,
    /// Register index kinds.
    Index,
    /// Register storage backends by URI scheme.
    Storage,
    /// Register graph algorithms.
    Algorithm,
    /// Register CRDT kinds.
    Crdt,
    /// Register session / query lifecycle hooks.
    Hook,
    /// Register fine-grained mutation triggers.
    Trigger,
    /// Register background / scheduled jobs.
    BackgroundJob {
        /// Maximum concurrent invocations of this plugin's jobs.
        max_concurrent: u32,
    },
    /// Register logical (Arrow extension) types.
    Type,
    /// Register authentication providers.
    Auth,
    /// Register authorization policies.
    Authz,
    /// Register wire / connector protocols.
    Connector,
    /// Register collations (sort orders).
    Collation,
    /// Register CDC output sinks.
    Cdc,
    /// Register catalogs / virtual schemas.
    Catalog,
    /// Authority to call meta-procedures (`uni.plugin.declare*`).
    PluginDeclare,

    // ---- Resource quotas ----
    /// Maximum wasm linear memory per instance.
    MemoryBytes(u64),
    /// Maximum wasmtime fuel per call.
    FuelPerCall(u64),
    /// Maximum wall-clock milliseconds per call.
    WallClockMillisPerCall(u64),
    /// Maximum concurrent instances in the wasm pool.
    ConcurrentInstances(u32),
    /// Maximum total memory across all instances.
    TotalMemoryBytes(u64),
    /// Cap on rows yielded by a procedure.
    MaxResultRows(u64),
}

/// Granularity of lock-capability grants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum LockGranularity {
    /// Per-node locks only.
    Nodes,
    /// Per-edge locks only.
    Edges,
    /// Both nodes and edges.
    Both,
    /// Global (graph-wide) locks.
    Global,
}

/// A set of capabilities — declared by manifest, granted by loader.
///
/// The *effective* capability set is the intersection of declared and
/// granted. Registrations attempted without the corresponding capability in
/// the effective set fail with [`crate::PluginError::CapabilityRequired`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CapabilitySet {
    set: BTreeSet<Capability>,
}

impl CapabilitySet {
    /// Construct an empty capability set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a capability set from an iterable.
    #[must_use]
    pub fn from_iter_of(caps: impl IntoIterator<Item = Capability>) -> Self {
        Self {
            set: caps.into_iter().collect(),
        }
    }

    /// Insert a capability; returns `true` if the capability was not already present.
    pub fn insert(&mut self, cap: Capability) -> bool {
        self.set.insert(cap)
    }

    /// Check whether the set contains the given capability (exact equality).
    #[must_use]
    pub fn contains(&self, cap: &Capability) -> bool {
        self.set.contains(cap)
    }

    /// Check whether the set contains a registration-gating capability.
    ///
    /// Match is on the *variant* — `contains_variant(Capability::ScalarFn)`
    /// returns `true` regardless of any associated data on other variants.
    /// Useful for registrar gates like "any `BackgroundJob { max_concurrent }`
    /// is sufficient regardless of the cap."
    #[must_use]
    pub fn contains_variant(&self, target: &Capability) -> bool {
        self.set.iter().any(|c| variant_matches(c, target))
    }

    /// Intersect this set with another, returning a new set.
    ///
    /// The intersection is the effective capability set when manifest
    /// declarations are intersected with host grants. Caps that match by
    /// variant but differ in attenuation (e.g., two different `Network
    /// { allow }` patterns) are *both retained* — the runtime check enforces
    /// each individually.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        let mut out = Self::new();
        for c in &self.set {
            if other.contains_variant(c) {
                out.insert(c.clone());
            }
        }
        out
    }

    /// Returns an iterator over the contained capabilities.
    pub fn iter(&self) -> impl Iterator<Item = &Capability> {
        self.set.iter()
    }

    /// Returns the number of distinct capabilities in the set.
    #[must_use]
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Returns `true` if the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}

fn variant_matches(a: &Capability, b: &Capability) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

/// Determinism characterization — drives planner caching and hoisting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Determinism {
    /// Same inputs always produce identical output. Cacheable; hoistable
    /// from loops. Maps to DataFusion `Volatility::Immutable`.
    Pure,
    /// Stable within one session (e.g. `current_user()`). Maps to
    /// DataFusion `Volatility::Stable`.
    SessionScoped,
    /// Non-deterministic (`rand()`, `now()`). Maps to DataFusion
    /// `Volatility::Volatile`.
    #[default]
    Nondeterministic,
}

/// Declared side-effects of a plugin.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SideEffects {
    /// Reads only. Pure or session-scoped data access.
    #[default]
    ReadOnly,
    /// May write to the graph.
    Writes,
    /// May perform external I/O (network, filesystem).
    ExternalIo,
}

/// Lifetime scope of a plugin's registrations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Scope {
    /// Lives until `Uni::remove_plugin` or instance drop. Visible to every
    /// session. The default for compile-time and WASM plugins.
    #[default]
    Instance,
    /// Lives until the registering `Session` is dropped. Not visible to
    /// other sessions on the same instance. The default for PyO3 and Lua
    /// REPL-style plugins.
    Session,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_set_default_empty() {
        let s = CapabilitySet::new();
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn capability_set_insert_dedup() {
        let mut s = CapabilitySet::new();
        assert!(s.insert(Capability::ScalarFn));
        assert!(!s.insert(Capability::ScalarFn));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn intersect_keeps_matching_variants() {
        let a = CapabilitySet::from_iter_of([
            Capability::ScalarFn,
            Capability::Storage,
            Capability::Network {
                allow: vec![SmolStr::new("https://api.example/**")],
            },
        ]);
        let b = CapabilitySet::from_iter_of([
            Capability::ScalarFn,
            Capability::Network {
                allow: vec![SmolStr::new("https://api.example/**")],
            },
        ]);
        let inter = a.intersect(&b);
        assert!(inter.contains(&Capability::ScalarFn));
        assert!(!inter.contains_variant(&Capability::Storage));
        assert!(inter.contains_variant(&Capability::Network { allow: vec![] }));
    }

    #[test]
    fn contains_variant_ignores_attenuation() {
        let s = CapabilitySet::from_iter_of([Capability::Network {
            allow: vec![SmolStr::new("https://x.example/*")],
        }]);
        assert!(s.contains_variant(&Capability::Network { allow: vec![] }));
        // Exact equality requires identical attenuation.
        assert!(!s.contains(&Capability::Network { allow: vec![] }));
    }

    #[test]
    fn determinism_default_is_nondeterministic() {
        assert_eq!(Determinism::default(), Determinism::Nondeterministic);
    }
}
