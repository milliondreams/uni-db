//! `ExtismLoader` — top-level entry point for loading Extism plugins.
//!
//! **M6a partial:** manifest parsing, capability filtering, and real
//! `extism-sdk` instantiation (with cap-filtered host fns + resource
//! limits) ship here. The full end-to-end `load()` path — read manifest
//! export → re-instantiate with effective grants → read register export →
//! push adapters into `PluginRegistrar` — still returns
//! [`ExtismError::NotYetImplemented`] until the subsequent M6a commits.

// Rust guideline compliant

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::error::ExtismError;
use crate::host_fns::HostFnRegistry;

/// Plugin manifest in the Extism plugin's canonical JSON form.
///
/// Returned from the plugin's `manifest` export. Mirrors the shape of
/// the §14 manifest, but on the Extism wire.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtismPluginManifest {
    /// Reverse-DNS plugin id.
    pub id: String,
    /// Semver string.
    pub version: String,
    /// Extism ABI range the plugin was built against.
    #[serde(default, rename = "abi-extism")]
    pub abi_extism: Option<String>,
    /// Capabilities the plugin declares it needs — each a bare name
    /// (`"network"`) or a structured object with attenuation patterns
    /// (`{"kind":"network","allow":[...]}`); see [`uni_plugin::ManifestCapability`].
    #[serde(default)]
    pub capabilities: Vec<uni_plugin::ManifestCapability>,
    /// Determinism class (`"pure"`, `"session-scoped"`, `"nondeterministic"`).
    #[serde(default)]
    pub determinism: Option<String>,
    /// Free-form human description.
    #[serde(default)]
    pub description: Option<String>,

    // Resource limits. All optional — if absent, the host's defaults
    // apply. Plugin authors can request tighter limits than the host
    // default; the host's grant model decides whether to honor a looser
    // request (M6a leaves the negotiation to the caller of `build_plugin`).
    /// Per-call wasmtime fuel limit. Per proposal §10 / §5.5.4.
    #[serde(default)]
    pub fuel_per_call: Option<u64>,
    /// Maximum linear-memory pages (one page = 64 KiB).
    #[serde(default)]
    pub memory_max_pages: Option<u32>,
    /// Wall-clock per-call timeout in milliseconds.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

impl ExtismPluginManifest {
    /// The declared capabilities as a rich [`uni_plugin::CapabilitySet`].
    #[must_use]
    pub fn declared_capability_set(&self) -> uni_plugin::CapabilitySet {
        uni_plugin::CapabilitySet::from_manifest(self.capabilities.iter().cloned())
    }
}

/// Result of [`ExtismLoader::prepare`] — everything the host needs to
/// instantiate the plugin once the SDK integration is wired.
#[derive(Debug, Clone)]
pub struct PreparedExtismPlugin {
    /// Parsed manifest.
    pub manifest: ExtismPluginManifest,
    /// Capabilities granted to the plugin (rich, with attenuation patterns):
    /// intersection of declared (manifest) and granted (host).
    pub effective: uni_plugin::CapabilitySet,
    /// Host fns the plugin is allowed to import (post-capability filter).
    pub allowed_host_fns: Vec<String>,
    /// Capabilities the plugin requested but the host did not grant —
    /// the loader uses these for diagnostics and decides whether to
    /// reject the load or proceed with reduced functionality.
    pub denied_capabilities: Vec<String>,
}

/// Top-level Extism plugin loader.
///
/// Construct one per uni-db instance; the loader owns the
/// [`HostFnRegistry`] (capability metadata) and a parallel map of the
/// runtime-callable [`extism::Function`]s keyed by host-fn name. The
/// metadata map exists unconditionally so embedders without
/// `extism-runtime` can still introspect the host-fn surface; the
/// runtime functions only materialize when the SDK feature is on.
#[derive(Default)]
pub struct ExtismLoader {
    host_fns: HostFnRegistry,
    /// Concrete host-fn implementations. Inserts via
    /// [`Self::register_host_function`] keep this in lock-step with the
    /// [`HostFnSpec`] metadata; `build_plugin` filters this map by
    /// the plugin's effective capability set before handing functions to
    /// `extism::PluginBuilder`.
    // `extism::Function` doesn't implement Debug, so we hand-roll Debug
    // for the enclosing type below.
    runtime_fns: BTreeMap<String, extism::Function>,
    /// Optional KMS provider backing `uni_kms_*`. Absent → those fns error
    /// loudly at call time ("no KMS provider configured").
    kms: Option<std::sync::Arc<dyn uni_plugin::KmsProvider>>,
    /// Optional secret store backing `uni_secret_acquire`.
    secrets: Option<std::sync::Arc<uni_plugin::secrets::SecretStore>>,
    /// Optional HTTP egress backing `uni_http_*`.
    http: Option<std::sync::Arc<dyn uni_plugin::HttpEgress>>,
}

impl std::fmt::Debug for ExtismLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtismLoader")
            .field("host_fns", &self.host_fns)
            .field("runtime_fn_count", &self.runtime_fns.len())
            .finish()
    }
}

impl ExtismLoader {
    /// Construct a fresh loader with an empty host-fn registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mutable access to the host-fn registry (metadata).
    pub fn host_fns_mut(&mut self) -> &mut HostFnRegistry {
        &mut self.host_fns
    }

    /// Shared access to the host-fn registry (metadata).
    #[must_use]
    pub fn host_fns(&self) -> &HostFnRegistry {
        &self.host_fns
    }

    /// Register a host function with both its metadata and its concrete
    /// `extism::Function` implementation.
    ///
    /// The function is invocable from any plugin whose effective
    /// capability set contains `spec.required_capability` (or any plugin,
    /// if `required_capability` is `None`). The capability filter runs at
    /// [`Self::build_plugin`] time — plugins that don't pass the filter
    /// never see this function in their import table.
    pub fn register_host_function(
        &mut self,
        spec: crate::host_fns::HostFnSpec,
        function: extism::Function,
    ) {
        let name = spec.name.clone();
        self.host_fns.register(spec);
        self.runtime_fns.insert(name, function);
    }

    /// Number of registered runtime functions. Diagnostic / test helper.
    #[must_use]
    pub fn runtime_fn_count(&self) -> usize {
        self.runtime_fns.len()
    }

    /// Attach a KMS provider backing `uni_kms_*` (builder style).
    ///
    /// Pair with [`crate::host_svc::register_default_host_svc`] to register the
    /// metadata specs; the concrete functions are built per load with the
    /// effective grant set so call-time attenuation is enforced.
    #[must_use]
    pub fn with_kms(mut self, kms: std::sync::Arc<dyn uni_plugin::KmsProvider>) -> Self {
        self.kms = Some(kms);
        self
    }

    /// Attach a secret store backing `uni_secret_acquire` (builder style).
    #[must_use]
    pub fn with_secret_store(
        mut self,
        store: std::sync::Arc<uni_plugin::secrets::SecretStore>,
    ) -> Self {
        self.secrets = Some(store);
        self
    }

    /// Attach an HTTP egress backing `uni_http_*` (builder style).
    #[must_use]
    pub fn with_http(mut self, http: std::sync::Arc<dyn uni_plugin::HttpEgress>) -> Self {
        self.http = Some(http);
        self
    }

    /// The host-fn map for a single load: the static `runtime_fns` plus the
    /// per-load capability-gated service functions (`uni_kms_*`,
    /// `uni_secret_acquire`, `uni_http_*`).
    ///
    /// Each service function is built with `prepared.effective` and the loader's
    /// service handles baked into its [`extism::UserData`], so it enforces *this*
    /// load's attenuation patterns. Only the names this plugin is actually
    /// allowed (`prepared.allowed_host_fns`) are materialized, so a plugin
    /// without the matching capability variant never pays the build cost.
    fn runtime_fns_for_load(
        &self,
        prepared: &PreparedExtismPlugin,
    ) -> BTreeMap<String, extism::Function> {
        let mut fns = self.runtime_fns.clone();
        // Build the per-load context once; cloned (cheaply, Arc handles) into
        // each materialized service function.
        let ctx = crate::host_svc::HostSvcCtx {
            effective: prepared.effective.clone(),
            kms: self.kms.clone(),
            secrets: self.secrets.clone(),
            http: self.http.clone(),
        };
        for name in &prepared.allowed_host_fns {
            if fns.contains_key(name) {
                continue;
            }
            if let Some(function) = crate::host_svc::build_service_fn(name, &ctx) {
                fns.insert(name.clone(), function);
            }
        }
        fns
    }

    /// Parse a manifest JSON blob (as the plugin's `manifest` export
    /// returns) and filter the host-fn registry through the granted
    /// capability set.
    ///
    /// This is the **deterministic, sandbox-free** portion of the M6a
    /// loader path: it doesn't instantiate any wasm. The host can use
    /// the returned [`PreparedExtismPlugin`] to decide whether to
    /// proceed with full SDK instantiation, prompt the user for
    /// additional capability grants, or reject the load outright.
    ///
    /// # Errors
    ///
    /// - [`ExtismError::ManifestInvalid`] if the JSON doesn't parse or
    ///   doesn't match [`ExtismPluginManifest`].
    pub fn prepare(
        &self,
        manifest_json: &[u8],
        grants: &uni_plugin::CapabilitySet,
    ) -> Result<PreparedExtismPlugin, ExtismError> {
        let manifest: ExtismPluginManifest = serde_json::from_slice(manifest_json)
            .map_err(|e| ExtismError::ManifestInvalid(format!("json parse: {e}")))?;
        Ok(self.prepare_parsed(manifest, grants))
    }

    /// Intersect declared/granted capabilities for an already-parsed
    /// manifest, skipping the JSON round-trip.
    ///
    /// [`Self::load`] reads the manifest export off a bootstrap plugin
    /// (parsed `ExtismPluginManifest`), then needs the combined
    /// cap-intersection and host-fn-allow-list result. The previous
    /// implementation re-serialized the parsed struct to JSON and called
    /// [`Self::prepare`] which deserialized it straight back — a
    /// wasteful round-trip whose only purpose was reusing the
    /// cap-intersection loop. This entry point preserves the loop and
    /// skips the (de)serialization.
    #[must_use]
    pub fn prepare_parsed(
        &self,
        manifest: ExtismPluginManifest,
        grants: &uni_plugin::CapabilitySet,
    ) -> PreparedExtismPlugin {
        // Effective = declared ∩ granted (retains per-variant attenuation).
        let declared = manifest.declared_capability_set();
        let effective = declared.intersect(grants);
        let denied: Vec<String> = declared
            .iter()
            .filter(|c| !effective.contains_variant(c))
            .map(|c| format!("{c:?}"))
            .collect();

        // Host-fn filter: only fns whose required_capability *variant* is in
        // the effective set (or which have no required_capability — always
        // available). Pattern attenuation is enforced in the host-fn body.
        let allowed: Vec<String> = self
            .host_fns
            .iter()
            .filter(|spec| match &spec.required_capability {
                None => true,
                Some(req) => effective.contains_variant(req),
            })
            .map(|s| s.name.clone())
            .collect();

        PreparedExtismPlugin {
            manifest,
            effective,
            allowed_host_fns: allowed,
            denied_capabilities: denied,
        }
    }

    /// Build an `extism::Plugin` from raw wasm bytes against a prepared
    /// capability set.
    ///
    /// Capability-gated host functions are filtered through
    /// `prepared.allowed_host_fns` — fns whose `required_capability` is
    /// not in the plugin's effective set are *omitted from the plugin's
    /// import table*. This is the Extism analogue of Component Model's
    /// linker absence: the plugin literally cannot resolve an unauthorized
    /// host fn at link time. Per proposal §5.6.2 this is the structural
    /// half of capability enforcement; the runtime `checked_call` helper
    /// (M6a.1.4) is the defense-in-depth half.
    ///
    /// Resource limits declared in the parsed manifest are applied to
    /// the underlying wasmtime config: `memory_max_pages` (linear
    /// memory cap), `timeout_ms` (per-call wall-clock), `fuel_per_call`
    /// (instruction budget). If a field is `None`, the host's default
    /// (no cap) applies.
    ///
    /// # Errors
    ///
    /// - [`ExtismError::Instantiate`] if the wasm bytes fail to compile,
    ///   link, or instantiate (invalid wasm, missing required imports,
    ///   wasmtime errors).
    /// - [`ExtismError::Internal`] if a runtime function recorded in the
    ///   registry's allow-list is somehow absent from `runtime_fns`
    ///   (should be unreachable; indicates a registry-state bug).
    pub fn build_plugin(
        &self,
        bytes: &[u8],
        prepared: &PreparedExtismPlugin,
    ) -> Result<extism::Plugin, ExtismError> {
        build_plugin_from_parts(bytes, prepared, &self.runtime_fns_for_load(prepared))
    }

    /// Instantiate the plugin via the extism-sdk.
    ///
    /// This is the SDK-gated half of the M6a loader path. The caller
    /// supplies the prepared capability state (produced by
    /// [`Self::prepare`]); this method returns a live `extism::Plugin`
    /// ready for the manifest/register reader passes (M6a.1.2) and the
    /// scalar adapter (M6a.1.5).
    ///
    /// # Errors
    ///
    /// See [`Self::build_plugin`].
    pub fn instantiate(
        &self,
        bytes: &[u8],
        prepared: &PreparedExtismPlugin,
    ) -> Result<extism::Plugin, ExtismError> {
        self.build_plugin(bytes, prepared)
    }

    /// End-to-end load: read manifest, intersect with host grants,
    /// re-instantiate with effective caps, read register export, push
    /// adapters into the supplied [`uni_plugin::PluginRegistrar`].
    ///
    /// The two-pass dance is the proposal's §5.6 contract: the host
    /// cannot know what capabilities the plugin needs until it reads
    /// the `manifest` export, and reading that export requires a built
    /// plugin. The first pass uses an **empty grant set** — the
    /// `manifest` export must be implementable without any
    /// capability-gated host fn, which is trivially true (it just
    /// returns JSON). The second pass rebuilds with the intersected
    /// grants and the register export is read against that.
    ///
    /// The currently-supported registration kinds are
    /// [`crate::exports::RegistrationEntry::Scalar`]; aggregate and
    /// procedure adapters land in M6a.2. Entries of unsupported kinds
    /// cause [`ExtismError::OutputDecode`] — better to fail loudly than
    /// silently ignore part of a plugin's surface.
    ///
    /// # Errors
    ///
    /// - [`ExtismError::Instantiate`] for wasmtime / extism build
    ///   failures.
    /// - [`ExtismError::ManifestInvalid`] for malformed manifests or
    ///   unsupported argument types.
    /// - [`ExtismError::InvalidPlugin`] if required exports
    ///   (`manifest`, `register`) are missing.
    /// - [`ExtismError::OutputDecode`] for malformed register payloads
    ///   or unsupported entry kinds.
    /// - [`ExtismError::Internal`] for `PluginRegistrar` registration
    ///   failures (capability / qname conflicts).
    pub fn load(
        &self,
        bytes: &[u8],
        host_grants: &uni_plugin::CapabilitySet,
        registrar: &mut uni_plugin::PluginRegistrar<'_>,
    ) -> Result<LoadOutcome, ExtismError> {
        // Pass 1: read the manifest export. A wasm module resolves *all* of
        // its imports at instantiate time, so a guest that imports a host fn
        // (e.g. `uni_http_get`) cannot even be instantiated to read its
        // manifest unless that import is present in the linker. We don't yet
        // know the guest's declared caps, so bootstrap with the host's
        // *offered* grants: register the service fns whose capability variant
        // the host offers. This is safe because pass 1 invokes only the pure
        // `manifest` export — never a host-fn-calling `invoke` — and the live
        // execution pool below is rebuilt with the real `declared ∩ grants`
        // attenuation. A guest importing a host fn the host did *not* offer
        // fails to instantiate here, which is the intended link-time gate.
        let bootstrap_allowed: Vec<String> = self
            .host_fns
            .iter()
            .filter(|spec| match &spec.required_capability {
                None => true,
                Some(req) => host_grants.contains_variant(req),
            })
            .map(|s| s.name.clone())
            .collect();
        let bootstrap_prepared = PreparedExtismPlugin {
            manifest: ExtismPluginManifest {
                id: String::new(),
                version: String::new(),
                abi_extism: None,
                capabilities: Vec::new(),
                determinism: None,
                description: None,
                fuel_per_call: None,
                memory_max_pages: None,
                timeout_ms: None,
            },
            effective: host_grants.clone(),
            allowed_host_fns: bootstrap_allowed,
            denied_capabilities: Vec::new(),
        };
        let mut bootstrap_plugin = self.build_plugin(bytes, &bootstrap_prepared)?;
        let parsed_manifest = crate::exports::read_manifest_export(&mut bootstrap_plugin)?;
        drop(bootstrap_plugin);

        // Rewrite the registrar's plugin id to match the manifest. The
        // caller supplies a placeholder id (e.g., `"extism.loading"`)
        // because the canonical id is unknown until pass 1 reads the
        // manifest export. Setting it here lets `validate_qname`
        // accept entries in the plugin's declared namespace.
        registrar.set_plugin_id(uni_plugin::PluginId::new(parsed_manifest.id.clone()));

        // Pass 2: intersect declared/granted, re-build with full host
        // fn set, read register export. The parsed manifest from pass 1
        // is reused directly via `prepare_parsed`, avoiding a JSON
        // re-serialize / re-parse round-trip.
        let prepared = self.prepare_parsed(parsed_manifest, host_grants);

        // Build the instance pool: factory closes over owned bytes,
        // prepared (cap-filtered), and the per-load host-fn map (static
        // `runtime_fns` plus the capability-gated `uni_kms_*` / `uni_secret_*`
        // / `uni_http_*` service fns built with this load's effective grant
        // set). Pre-warm count is from `PoolConfig::default` (proposal §5.3.1 —
        // `min_warm = 1`); future commits surface this through the manifest.
        let pool = build_pool(bytes, &prepared, &self.runtime_fns_for_load(&prepared))?;

        // Lease one warm instance, read the register export once, and
        // drop the lease. A previous two-pass shape re-read the same
        // export from a fresh instance; both reads were pure JSON
        // parses of the same wasm export, so the second pass added no
        // signal.
        let mut leased = crate::pool::PooledInstance::acquire(std::sync::Arc::clone(&pool))?;
        let registration = crate::exports::read_register_export(leased.get_mut())?;
        drop(leased);

        let mut scalars_registered: Vec<String> = Vec::new();
        let mut aggregates_registered: Vec<String> = Vec::new();
        let mut procedures_registered: Vec<String> = Vec::new();

        for entry in registration.entries {
            match entry {
                crate::exports::RegistrationEntry::Scalar { qname, signature } => {
                    let parsed_qname = uni_plugin::QName::parse(&qname).map_err(|e| {
                        ExtismError::OutputDecode(format!("invalid qname `{qname}`: {e}"))
                    })?;
                    let sig = crate::wire_translate::wire_fn_sig_to_internal(&signature)?;
                    let adapter = std::sync::Arc::new(crate::adapter::ExtismScalarFn::new(
                        std::sync::Arc::clone(&pool),
                        parsed_qname.clone(),
                        sig.clone(),
                    ));
                    registrar
                        .scalar_fn(parsed_qname, sig, adapter)
                        .map_err(|e| {
                            ExtismError::Internal(format!("registrar.scalar_fn `{qname}`: {e}"))
                        })?;
                    scalars_registered.push(qname);
                }
                crate::exports::RegistrationEntry::Aggregate {
                    qname,
                    signature,
                    state,
                } => {
                    let parsed_qname = uni_plugin::QName::parse(&qname).map_err(|e| {
                        ExtismError::OutputDecode(format!("invalid qname `{qname}`: {e}"))
                    })?;
                    let sig = crate::wire_translate::wire_agg_sig_to_internal(&signature, &state)?;
                    let adapter =
                        std::sync::Arc::new(crate::adapter_aggregate::ExtismAggregateFn::new(
                            std::sync::Arc::clone(&pool),
                            parsed_qname.clone(),
                            sig.clone(),
                        ));
                    registrar
                        .aggregate_fn(parsed_qname, sig, adapter)
                        .map_err(|e| {
                            ExtismError::Internal(format!("registrar.aggregate_fn `{qname}`: {e}"))
                        })?;
                    aggregates_registered.push(qname);
                }
                crate::exports::RegistrationEntry::Procedure {
                    qname,
                    args,
                    yields,
                    mode,
                } => {
                    let parsed_qname = uni_plugin::QName::parse(&qname).map_err(|e| {
                        ExtismError::OutputDecode(format!("invalid qname `{qname}`: {e}"))
                    })?;
                    let sig =
                        crate::wire_translate::wire_proc_sig_to_internal(&args, &yields, &mode)?;
                    let adapter =
                        std::sync::Arc::new(crate::adapter_procedure::ExtismProcedure::new(
                            std::sync::Arc::clone(&pool),
                            parsed_qname.clone(),
                            sig.clone(),
                        ));
                    registrar
                        .procedure(parsed_qname, sig, adapter)
                        .map_err(|e| {
                            ExtismError::Internal(format!("registrar.procedure `{qname}`: {e}"))
                        })?;
                    procedures_registered.push(qname);
                }
            }
        }

        Ok(LoadOutcome {
            plugin_id: prepared.manifest.id.clone(),
            version: prepared.manifest.version.clone(),
            effective_capabilities: prepared
                .effective
                .iter()
                .map(|c| format!("{c:?}"))
                .collect(),
            denied_capabilities: prepared.denied_capabilities,
            scalars_registered,
            aggregates_registered,
            procedures_registered,
            pool,
        })
    }
}

/// Build an `extism::Plugin` from owned-data inputs.
///
/// Module-private free function so the pool factory closure can call
/// it without holding a reference to the loader. The closure captures
/// `Arc`-owned bytes / prepared / runtime_fns and re-invokes this each
/// time the pool needs to cold-construct a new instance.
fn build_plugin_from_parts(
    bytes: &[u8],
    prepared: &PreparedExtismPlugin,
    runtime_fns: &BTreeMap<String, extism::Function>,
) -> Result<extism::Plugin, ExtismError> {
    let manifest = build_extism_manifest(bytes, &prepared.manifest);
    let mut builder = extism::PluginBuilder::new(manifest).with_wasi(true);
    if let Some(fuel) = prepared.manifest.fuel_per_call {
        builder = builder.with_fuel_limit(fuel);
    }
    let mut selected: Vec<extism::Function> = Vec::with_capacity(prepared.allowed_host_fns.len());
    for fn_name in &prepared.allowed_host_fns {
        let function = runtime_fns.get(fn_name).ok_or_else(|| {
            ExtismError::Internal(format!(
                "allowed host fn `{fn_name}` missing from runtime_fns; \
                 registry-state bug — every spec.name should have a Function"
            ))
        })?;
        selected.push(function.clone());
    }
    builder = builder.with_functions(selected);
    builder
        .build()
        .map_err(|e| ExtismError::Instantiate(e.to_string()))
}

fn build_extism_manifest(bytes: &[u8], plugin_manifest: &ExtismPluginManifest) -> extism::Manifest {
    let mut m = extism::Manifest::new([extism::Wasm::data(bytes.to_vec())]);
    if let Some(pages) = plugin_manifest.memory_max_pages {
        m = m.with_memory_max(pages);
    }
    if let Some(ms) = plugin_manifest.timeout_ms {
        m = m.with_timeout(std::time::Duration::from_millis(ms));
    }
    m
}

fn build_pool(
    bytes: &[u8],
    prepared: &PreparedExtismPlugin,
    runtime_fns: &BTreeMap<String, extism::Function>,
) -> Result<std::sync::Arc<crate::pool::ExtismInstancePool<extism::Plugin>>, ExtismError> {
    let bytes_owned: std::sync::Arc<Vec<u8>> = std::sync::Arc::new(bytes.to_vec());
    let prepared_owned: std::sync::Arc<PreparedExtismPlugin> =
        std::sync::Arc::new(prepared.clone());
    let runtime_fns_owned: std::sync::Arc<BTreeMap<String, extism::Function>> =
        std::sync::Arc::new(runtime_fns.clone());

    let factory = {
        let bytes = std::sync::Arc::clone(&bytes_owned);
        let prepared = std::sync::Arc::clone(&prepared_owned);
        let runtime_fns = std::sync::Arc::clone(&runtime_fns_owned);
        move || build_plugin_from_parts(&bytes, &prepared, &runtime_fns)
    };

    let pool = crate::pool::ExtismInstancePool::new(crate::pool::PoolConfig::default(), factory)?;
    Ok(std::sync::Arc::new(pool))
}

/// Outcome of a successful [`ExtismLoader::load`].
///
/// Carries the diagnostic state the caller (typically `Uni::load_wasm_extism`)
/// needs to construct a `PluginHandle`, surface denied capabilities to the
/// user, and keep the live plugin alive for the duration of the
/// registration.
pub struct LoadOutcome {
    /// Reverse-DNS plugin id from the manifest.
    pub plugin_id: String,
    /// Plugin version from the manifest.
    pub version: String,
    /// Capabilities granted to the plugin (intersection of declared ∩ host).
    pub effective_capabilities: Vec<String>,
    /// Capabilities the plugin requested but the host did not grant.
    pub denied_capabilities: Vec<String>,
    /// Qnames registered as scalar fns.
    pub scalars_registered: Vec<String>,
    /// Qnames registered as aggregate fns.
    pub aggregates_registered: Vec<String>,
    /// Qnames registered as procedures.
    pub procedures_registered: Vec<String>,
    /// The instance pool, shared across every adapter bound to this
    /// plugin. Adapters hold an `Arc` clone; the pool is kept alive as
    /// long as any adapter remains in the registry.
    pub pool: std::sync::Arc<crate::pool::ExtismInstancePool<extism::Plugin>>,
}

impl std::fmt::Debug for LoadOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadOutcome")
            .field("plugin_id", &self.plugin_id)
            .field("version", &self.version)
            .field("effective_capabilities", &self.effective_capabilities)
            .field("denied_capabilities", &self.denied_capabilities)
            .field("scalars_registered", &self.scalars_registered)
            .field("aggregates_registered", &self.aggregates_registered)
            .field("procedures_registered", &self.procedures_registered)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_fns::HostFnSpec;
    use uni_plugin::{Capability, CapabilitySet};

    fn manifest_json(caps: &[&str]) -> String {
        let caps_json: Vec<String> = caps.iter().map(|c| format!("\"{c}\"")).collect();
        format!(
            r#"{{ "id": "ai.example.test", "version": "1.0.0", "capabilities": [{}] }}"#,
            caps_json.join(", ")
        )
    }

    #[test]
    fn loader_constructs_with_empty_host_fns() {
        let l = ExtismLoader::new();
        assert!(l.host_fns().is_empty());
    }

    // M6a.1.5: load() is now real. Smoke-test against garbage bytes —
    // pass-1 build_plugin fails with Instantiate. Full e2e against a
    // real plugin lives in tests/instantiate_with_minimal_wasm.rs and
    // (T#7) tests/example_extism_geo_e2e.rs.

    fn fs_cap() -> Capability {
        Capability::Filesystem {
            read: vec![],
            write: vec![],
        }
    }

    #[test]
    fn loader_accepts_host_fn_registrations() {
        let mut l = ExtismLoader::new();
        l.host_fns_mut().register(HostFnSpec {
            name: "host_fs_read".to_owned(),
            required_capability: Some(fs_cap()),
            docs: "Read file.".to_owned(),
        });
        assert_eq!(l.host_fns().len(), 1);
    }

    #[test]
    fn prepare_parses_minimal_manifest() {
        let l = ExtismLoader::new();
        let json = manifest_json(&[]);
        let prep = l.prepare(json.as_bytes(), &CapabilitySet::new()).unwrap();
        assert_eq!(prep.manifest.id, "ai.example.test");
        assert_eq!(prep.manifest.version, "1.0.0");
        assert!(prep.effective.is_empty());
        assert!(prep.denied_capabilities.is_empty());
        assert!(prep.allowed_host_fns.is_empty());
    }

    #[test]
    fn prepare_intersects_declared_and_granted_capabilities() {
        let l = ExtismLoader::new();
        // Declared (kebab bare names → zero-attenuation variants).
        let json = manifest_json(&["filesystem", "network", "kms"]);
        let grants = CapabilitySet::from_iter_of([fs_cap(), Capability::Network { allow: vec![] }]);
        let prep = l.prepare(json.as_bytes(), &grants).unwrap();
        // Granted: Filesystem + Network. Denied: Kms.
        assert_eq!(prep.effective.len(), 2);
        assert!(prep.effective.contains_variant(&fs_cap()));
        assert!(
            prep.effective
                .contains_variant(&Capability::Network { allow: vec![] })
        );
        assert!(
            !prep
                .effective
                .contains_variant(&Capability::Kms { key_ids: vec![] })
        );
    }

    #[test]
    fn prepare_filters_host_fns_through_effective_capabilities() {
        let mut l = ExtismLoader::new();
        l.host_fns_mut().register(HostFnSpec {
            name: "host_fs_read".to_owned(),
            required_capability: Some(fs_cap()),
            docs: "Read file.".to_owned(),
        });
        l.host_fns_mut().register(HostFnSpec {
            name: "host_net_http_get".to_owned(),
            required_capability: Some(Capability::Network { allow: vec![] }),
            docs: "HTTP GET.".to_owned(),
        });
        l.host_fns_mut().register(HostFnSpec {
            name: "host_log".to_owned(),
            required_capability: None, // always-available
            docs: "Log a message.".to_owned(),
        });

        // Plugin requests filesystem only; host grants filesystem only.
        let json = manifest_json(&["filesystem"]);
        let prep = l
            .prepare(json.as_bytes(), &CapabilitySet::from_iter_of([fs_cap()]))
            .unwrap();

        // host_log is always-available; host_fs_read enabled by grant;
        // host_net_http_get filtered out (Network not granted).
        assert_eq!(prep.allowed_host_fns.len(), 2);
        assert!(prep.allowed_host_fns.iter().any(|n| n == "host_log"));
        assert!(prep.allowed_host_fns.iter().any(|n| n == "host_fs_read"));
        assert!(
            !prep
                .allowed_host_fns
                .iter()
                .any(|n| n == "host_net_http_get")
        );
    }

    #[test]
    fn prepare_rejects_malformed_manifest() {
        let l = ExtismLoader::new();
        let err = l.prepare(b"not json", &CapabilitySet::new()).unwrap_err();
        assert!(matches!(err, ExtismError::ManifestInvalid(_)));
    }

    #[test]
    fn instantiate_rejects_garbage_bytes_as_instantiate_error() {
        // M6a.1.1: `instantiate` is real now. With garbage bytes,
        // wasmtime fails to compile/instantiate — surface as
        // `ExtismError::Instantiate`, not the old `NotYetImplemented`.
        let l = ExtismLoader::new();
        let prep = l
            .prepare(
                b"{\"id\":\"a.b\",\"version\":\"0.0.0\"}",
                &CapabilitySet::new(),
            )
            .unwrap();
        let err = l.instantiate(b"not real wasm", &prep).unwrap_err();
        assert!(
            matches!(err, ExtismError::Instantiate(_)),
            "expected Instantiate(_), got: {err:?}"
        );
    }
}
