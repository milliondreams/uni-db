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
        /// Glob patterns of permitted URIs (`https://api.example/**`). Defaults
        /// to empty (deny-all) so a bare `"network"` declaration grants no
        /// egress until patterns are specified.
        #[serde(default)]
        allow: Vec<SmolStr>,
    },
    /// Filesystem read / write access with per-direction path patterns.
    Filesystem {
        /// Glob patterns of readable paths (empty = deny-all).
        #[serde(default)]
        read: Vec<SmolStr>,
        /// Glob patterns of writable paths (empty = deny-all).
        #[serde(default)]
        write: Vec<SmolStr>,
    },
    /// Invoking Cypher / Locy queries back into the host session.
    HostQuery {
        /// If `true`, only read queries are permitted.
        #[serde(default)]
        read_only: bool,
        /// Optional scope-restriction (label / edge-type prefixes).
        #[serde(default)]
        scopes: Vec<SmolStr>,
    },
    /// KMS access for sign / verify operations.
    Kms {
        /// Permitted key identifiers (empty = deny-all).
        #[serde(default)]
        key_ids: Vec<SmolStr>,
    },
    /// Acquiring named secret handles (opaque to the plugin).
    Secret {
        /// Permitted secret identifiers (empty = deny-all).
        #[serde(default)]
        ids: Vec<SmolStr>,
    },
    /// Explicit lock primitives (`host.lock_nodes`, `host.lock_edges`).
    Lock {
        /// Granularity of locks permitted.
        granularity: LockGranularity,
    },
    /// Scoped configuration K/V access (`host.config_get`).
    Config {
        /// Patterns of permitted config keys (empty = deny-all).
        #[serde(default)]
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
    /// Register Locy generator predicates (table-valued, 1:N).
    LocyGenerator,
    /// Register physical operators / optimizer rules.
    Operator,
    /// Register index kinds.
    Index,
    /// Register storage backends by URI scheme.
    Storage,
    /// Register graph algorithms.
    Algorithm,
    /// Drive the GraphCompute coarse-kernel catalog from a guest algorithm.
    ///
    /// Gates the kernel surface (`graph-compute@1`). Orthogonal to
    /// [`Capability::HostQuery`], which additionally gates the data-read
    /// `project` kernel: a guest algorithm needs both to project a graph, but
    /// only `GraphCompute` to run kernels over an already-projected handle
    /// (GraphCompute proposal §4.6).
    GraphCompute,
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
    /// Cap on GraphCompute native-work units per invocation (proposal §12).
    GraphComputeWork(u64),
    /// Cap on GraphCompute handle-arena bytes per invocation (proposal §12).
    GraphComputeArenaBytes(u64),
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

    /// Construct a capability set from guest-manifest declarations, each of
    /// which may be a bare name or a structured [`ManifestCapability`].
    #[must_use]
    pub fn from_manifest(caps: impl IntoIterator<Item = ManifestCapability>) -> Self {
        Self::from_iter_of(caps.into_iter().map(|m| m.0))
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

    /// Intersect this (guest-declared) set with the host-granted `other`,
    /// returning the effective capability set.
    ///
    /// Loaders call `declared.intersect(grants)`, so `self` is the guest
    /// manifest and `other` is the host ceiling. A guest capability survives
    /// only if the host grants the same variant, and its **payload is attenuated
    /// against the host**: for the allow-list variants (`Network`,
    /// `Filesystem`, `Kms`, `Secret`, `Config`) and `HostQuery`, the effective
    /// grant permits a resource only if *both* the guest and the host permit it
    /// — the host is a true ceiling a guest cannot widen. Non-payload variants
    /// (registration gates, resource quotas) retain the guest value as before.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        let mut out = Self::new();
        for c in &self.set {
            if other.contains_variant(c) {
                out.insert(attenuate_to_host(c, other));
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

/// Attenuate a guest capability against the host grant (the ceiling).
///
/// For the allow-list payload variants and `HostQuery`, returns a capability
/// whose effective grant is the conjunction of guest and host; for every other
/// variant, returns the guest capability unchanged (registration gates and
/// quotas have no allow-list to narrow). See [`CapabilitySet::intersect`].
fn attenuate_to_host(guest: &Capability, host: &CapabilitySet) -> Capability {
    match guest {
        Capability::Network { allow } => Capability::Network {
            allow: intersect_globs(allow, &host_lists(host, network_allow)),
        },
        Capability::Filesystem { read, write } => Capability::Filesystem {
            read: intersect_globs(read, &host_lists(host, fs_read)),
            write: intersect_globs(write, &host_lists(host, fs_write)),
        },
        Capability::Kms { key_ids } => Capability::Kms {
            key_ids: intersect_globs(key_ids, &host_lists(host, kms_ids)),
        },
        Capability::Secret { ids } => Capability::Secret {
            ids: intersect_globs(ids, &host_lists(host, secret_ids)),
        },
        Capability::Config { keys } => Capability::Config {
            keys: intersect_globs(keys, &host_lists(host, config_keys)),
        },
        Capability::HostQuery { read_only, scopes } => {
            // `read_only` is restrictive-true: either side may force read-only.
            // `scopes` empty means "unrestricted", so an empty list on a side
            // imposes no narrowing (unlike the deny-on-empty allow-lists above).
            let host_read_only = host.set.iter().any(|c| {
                matches!(
                    c,
                    Capability::HostQuery {
                        read_only: true,
                        ..
                    }
                )
            });
            let host_scopes = host_lists(host, host_query_scopes);
            let scopes = if scopes.is_empty() {
                host_scopes
            } else if host_scopes.is_empty() {
                scopes.clone()
            } else {
                intersect_globs(scopes, &host_scopes)
            };
            Capability::HostQuery {
                read_only: *read_only || host_read_only,
                scopes,
            }
        }
        // Registration gates and resource quotas carry no allow-list to narrow.
        other => other.clone(),
    }
}

// Per-variant payload extractors used to gather the host ceiling. Each returns
// the allow-list for capabilities of its variant, `None` otherwise.
fn network_allow(c: &Capability) -> Option<&[SmolStr]> {
    match c {
        Capability::Network { allow } => Some(allow),
        _ => None,
    }
}
fn fs_read(c: &Capability) -> Option<&[SmolStr]> {
    match c {
        Capability::Filesystem { read, .. } => Some(read),
        _ => None,
    }
}
fn fs_write(c: &Capability) -> Option<&[SmolStr]> {
    match c {
        Capability::Filesystem { write, .. } => Some(write),
        _ => None,
    }
}
fn kms_ids(c: &Capability) -> Option<&[SmolStr]> {
    match c {
        Capability::Kms { key_ids } => Some(key_ids),
        _ => None,
    }
}
fn secret_ids(c: &Capability) -> Option<&[SmolStr]> {
    match c {
        Capability::Secret { ids } => Some(ids),
        _ => None,
    }
}
fn config_keys(c: &Capability) -> Option<&[SmolStr]> {
    match c {
        Capability::Config { keys } => Some(keys),
        _ => None,
    }
}
fn host_query_scopes(c: &Capability) -> Option<&[SmolStr]> {
    match c {
        Capability::HostQuery { scopes, .. } => Some(scopes),
        _ => None,
    }
}

/// Union the allow-lists of every host capability matching `extract`'s variant.
fn host_lists<'a>(
    host: &'a CapabilitySet,
    extract: impl Fn(&'a Capability) -> Option<&'a [SmolStr]>,
) -> Vec<SmolStr> {
    host.set
        .iter()
        .filter_map(extract)
        .flatten()
        .cloned()
        .collect()
}

/// Intersect two glob allow-lists with each side acting as a ceiling on the
/// other.
///
/// A pattern is kept only when some pattern in the opposite list *subsumes* it
/// (`wildcard_match(other_pattern, pattern)`), so the result permits a resource
/// only if both inputs would. Incomparable patterns are dropped (deny — the
/// safe direction). This is sound for the prefix-glob patterns capability
/// allow-lists use; it can under-grant only for exotic overlapping-but-
/// incomparable globs, never over-grant. An empty input yields an empty result
/// (deny-all), matching the allow-list "empty = deny" convention.
fn intersect_globs(a: &[SmolStr], b: &[SmolStr]) -> Vec<SmolStr> {
    let mut out: Vec<SmolStr> = Vec::new();
    let mut keep = |pat: &SmolStr, ceiling: &[SmolStr]| {
        if ceiling.iter().any(|q| wildcard_match(q, pat)) && !out.contains(pat) {
            out.push(pat.clone());
        }
    };
    for pat in a {
        keep(pat, b);
    }
    for pat in b {
        keep(pat, a);
    }
    out
}

impl Capability {
    /// True if this is a [`Capability::Network`] grant whose allow-list
    /// matches `url`.
    ///
    /// Used for layer-3 (call-time) attenuation of `uni.http.*` host fns: a
    /// granted `Network { allow }` only permits URLs matching one of its
    /// patterns. Non-`Network` capabilities never match.
    #[must_use]
    pub fn network_allows(&self, url: &str) -> bool {
        matches!(self, Capability::Network { allow } if allow.iter().any(|p| wildcard_match(p, url)))
    }

    /// True if this is a [`Capability::Kms`] grant permitting `key_id`.
    #[must_use]
    pub fn kms_allows(&self, key_id: &str) -> bool {
        matches!(self, Capability::Kms { key_ids } if key_ids.iter().any(|p| wildcard_match(p, key_id)))
    }

    /// True if this is a [`Capability::Secret`] grant permitting `id`.
    #[must_use]
    pub fn secret_allows(&self, id: &str) -> bool {
        matches!(self, Capability::Secret { ids } if ids.iter().any(|p| wildcard_match(p, id)))
    }

    /// True if this is a [`Capability::Filesystem`] grant whose `read`
    /// allow-list matches `path`.
    ///
    /// Patterns are matched with `wildcard_match` (path-opaque — `*` and `**`
    /// both span `/`), which suits the `/data/**`-style grants in use.
    #[must_use]
    pub fn filesystem_read_allows(&self, path: &str) -> bool {
        matches!(self, Capability::Filesystem { read, .. } if read.iter().any(|p| wildcard_match(p, path)))
    }

    /// True if this is a [`Capability::Filesystem`] grant whose `write`
    /// allow-list matches `path`.
    #[must_use]
    pub fn filesystem_write_allows(&self, path: &str) -> bool {
        matches!(self, Capability::Filesystem { write, .. } if write.iter().any(|p| wildcard_match(p, path)))
    }
}

/// A capability as it appears in a **guest plugin manifest** (WASM / Extism) —
/// either a bare capability name (`"network"`, `"scalar-fn"`) or a structured
/// object carrying attenuation patterns
/// (`{"kind":"network","allow":["https://api.example/**"]}`).
///
/// Bare names normalize to their **zero-attenuation** variant — e.g.
/// `"network"` → `Network { allow: [] }` (deny-all egress) — so a guest must
/// spell out patterns to gain real host-surface access. This lets guest
/// manifests opt into the same rich [`Capability`] model the in-process Rhai /
/// Rust paths use, while staying backward-compatible with manifests that listed
/// bare capability names.
#[derive(Clone, Debug)]
pub struct ManifestCapability(pub Capability);

impl<'de> Deserialize<'de> for ManifestCapability {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        /// String-or-object shim. A JSON string is a bare name; a map is the
        /// structured `Capability` form (internally tagged on `kind`).
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Bare(String),
            Full(Capability),
        }

        let cap = match Repr::deserialize(deserializer)? {
            Repr::Full(c) => c,
            Repr::Bare(name) => {
                // Reconstruct the internally-tagged object `{ "kind": <name> }`
                // so unit variants and (defaulted-field) structured variants
                // both round-trip through the canonical `Capability` serde.
                let tagged = serde_json::json!({ "kind": name });
                Capability::deserialize(tagged).map_err(serde::de::Error::custom)?
            }
        };
        Ok(ManifestCapability(cap))
    }
}

/// Anchored wildcard match where `*` (and `**`) match any run of characters.
///
/// Capability attenuation patterns (network URL allow-lists, KMS key ids,
/// secret ids) are globs over opaque strings, not paths, so `**` is treated
/// identically to `*` — both match any sequence including `/`. Uses the
/// standard greedy two-pointer algorithm with backtracking; matching is
/// anchored at both ends.
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let p = pattern.as_bytes();
    let t = text.as_bytes();
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star: Option<usize> = None;
    let mut mark = 0usize;
    while ti < t.len() {
        if pi < p.len() && p[pi] == b'*' {
            // Collapse consecutive `*` so `**` behaves like `*`.
            while pi < p.len() && p[pi] == b'*' {
                pi += 1;
            }
            if pi == p.len() {
                return true;
            }
            star = Some(pi);
            mark = ti;
        } else if pi < p.len() && p[pi] == t[ti] {
            pi += 1;
            ti += 1;
        } else if let Some(s) = star {
            pi = s;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
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

    /// Regression for the 2026-06-10 review #6: `intersect` must bound the
    /// guest's allow-list by the host grant (the host is the ceiling), not clone
    /// the guest's broader list. A guest that declares `**` must not reach hosts
    /// the grant excludes.
    #[test]
    fn intersect_attenuates_network_to_host_ceiling() {
        let guest = CapabilitySet::from_iter_of([Capability::Network {
            allow: vec![SmolStr::new("**")],
        }]);
        let host = CapabilitySet::from_iter_of([Capability::Network {
            allow: vec![SmolStr::new("https://api.example/**")],
        }]);

        // Loaders call declared.intersect(grants) — guest is `self`.
        let effective = guest.intersect(&host);

        assert!(
            effective
                .iter()
                .any(|c| c.network_allows("https://api.example/v1/x")),
            "host-permitted URL must remain allowed"
        );
        assert!(
            !effective
                .iter()
                .any(|c| c.network_allows("https://evil.example/x")),
            "guest's `**` must not survive the host ceiling — sandbox escape"
        );
    }

    /// A guest narrower than the host keeps its own (narrower) list.
    #[test]
    fn intersect_keeps_guest_when_narrower_than_host() {
        let guest = CapabilitySet::from_iter_of([Capability::Network {
            allow: vec![SmolStr::new("https://api.example/v1/**")],
        }]);
        let host = CapabilitySet::from_iter_of([Capability::Network {
            allow: vec![SmolStr::new("https://api.example/**")],
        }]);
        let effective = guest.intersect(&host);
        assert!(
            effective
                .iter()
                .any(|c| c.network_allows("https://api.example/v1/x"))
        );
        assert!(
            !effective
                .iter()
                .any(|c| c.network_allows("https://api.example/v2/x")),
            "guest's own restriction must still bind"
        );
    }

    /// KMS / Secret / Filesystem payloads attenuate the same way.
    #[test]
    fn intersect_attenuates_kms_secret_fs() {
        let guest = CapabilitySet::from_iter_of([
            Capability::Kms {
                key_ids: vec![SmolStr::new("**")],
            },
            Capability::Secret {
                ids: vec![SmolStr::new("**")],
            },
            Capability::Filesystem {
                read: vec![SmolStr::new("**")],
                write: vec![SmolStr::new("**")],
            },
        ]);
        let host = CapabilitySet::from_iter_of([
            Capability::Kms {
                key_ids: vec![SmolStr::new("prod/signing/**")],
            },
            Capability::Secret {
                ids: vec![SmolStr::new("db/**")],
            },
            Capability::Filesystem {
                read: vec![SmolStr::new("/data/**")],
                write: vec![], // host grants no write
            },
        ]);
        let effective = guest.intersect(&host);

        assert!(effective.iter().any(|c| c.kms_allows("prod/signing/key1")));
        assert!(!effective.iter().any(|c| c.kms_allows("dev/key")));
        assert!(effective.iter().any(|c| c.secret_allows("db/password")));
        assert!(!effective.iter().any(|c| c.secret_allows("kms/root")));
        // Host grants no write path → no writable path survives.
        assert!(
            !effective.iter().any(|c| matches!(
                c,
                Capability::Filesystem { write, .. } if !write.is_empty()
            )),
            "guest write `**` must not survive an empty host write grant"
        );
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

    #[test]
    fn wildcard_match_basics() {
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("**", "any/thing"));
        assert!(wildcard_match(
            "https://api.example/**",
            "https://api.example/v1/x"
        ));
        assert!(wildcard_match("exact", "exact"));
        assert!(!wildcard_match("exact", "other"));
        assert!(!wildcard_match(
            "https://api.example/**",
            "https://evil.example/x"
        ));
        assert!(wildcard_match("a*c", "abbbc"));
        assert!(!wildcard_match("a*c", "abbb"));
    }

    #[test]
    fn network_allows_matches_only_network_variant() {
        let net = Capability::Network {
            allow: vec![SmolStr::new("https://api.example/**")],
        };
        assert!(net.network_allows("https://api.example/v1/data"));
        assert!(!net.network_allows("https://evil.example/x"));
        // A non-network capability never grants network access.
        assert!(!Capability::ScalarFn.network_allows("https://api.example/x"));
    }

    #[test]
    fn kms_and_secret_allow_wildcard_and_exact() {
        let kms = Capability::Kms {
            key_ids: vec![SmolStr::new("*")],
        };
        assert!(kms.kms_allows("signing-key-1"));
        let secret = Capability::Secret {
            ids: vec![SmolStr::new("db-password")],
        };
        assert!(secret.secret_allows("db-password"));
        assert!(!secret.secret_allows("other"));
    }

    #[test]
    fn manifest_capability_parses_bare_and_structured() {
        // Bare name → zero-attenuation variant (deny-all egress).
        let bare: ManifestCapability = serde_json::from_str("\"network\"").unwrap();
        assert!(matches!(&bare.0, Capability::Network { allow } if allow.is_empty()));
        assert!(!bare.0.network_allows("https://api.example/x"));
        // Bare unit variant.
        let scalar: ManifestCapability = serde_json::from_str("\"scalar-fn\"").unwrap();
        assert_eq!(scalar.0, Capability::ScalarFn);
        // Structured object → carries the allow-list.
        let structured: ManifestCapability =
            serde_json::from_str(r#"{"kind":"network","allow":["https://api.example/**"]}"#)
                .unwrap();
        assert!(structured.0.network_allows("https://api.example/v1/x"));
        assert!(!structured.0.network_allows("https://evil.example/x"));
        // A whole manifest list folds into a CapabilitySet.
        let set = CapabilitySet::from_manifest([bare, scalar, structured]);
        assert!(set.contains_variant(&Capability::Network { allow: vec![] }));
        assert!(set.contains(&Capability::ScalarFn));
    }

    #[test]
    fn filesystem_allows_read_and_write_separately() {
        let fs = Capability::Filesystem {
            read: vec![SmolStr::new("/data/**")],
            write: vec![SmolStr::new("/tmp/out/**")],
        };
        assert!(fs.filesystem_read_allows("/data/x/y.txt"));
        assert!(!fs.filesystem_read_allows("/etc/passwd"));
        assert!(fs.filesystem_write_allows("/tmp/out/log"));
        // read grant does not imply write grant for the same path
        assert!(!fs.filesystem_write_allows("/data/x/y.txt"));
        // a non-filesystem capability never matches
        assert!(!Capability::ScalarFn.filesystem_read_allows("/data/x"));
    }
}
