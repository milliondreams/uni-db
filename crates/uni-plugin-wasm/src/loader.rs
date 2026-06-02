//! `WasmLoader` — top-level entry point for loading WASM Component
//! Model plugins.
//!
//! Two-pass dance per proposal §5.6:
//!
//! 1. Build engine with no caps; instantiate the component; call the
//!    `manifest` export to learn what caps the plugin needs.
//! 2. Intersect declared ∩ host grants; rebuild the engine with
//!    epoch-interruption + fuel metering enabled per the plugin
//!    manifest's resource limits; instantiate with the cap-gated
//!    Linker; call `register` to learn the qnames; for each entry
//!    construct an adapter (currently `ComponentScalarFn`; aggregate
//!    and procedure adapters land in M6b.2) and push it into the
//!    `PluginRegistrar`.
//!
//! The actual `wasmtime::Engine` + `Component` + `Linker<HostState>`
//! plumbing lives in this file; the linker construction details are
//! in [`crate::linker`].

// Rust guideline compliant

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store};

use crate::adapter::ComponentScalarFn;
use crate::adapter_aggregate::ComponentAggregateFn;
use crate::adapter_procedure::ComponentProcedure;
use crate::bindings::aggregate::AggregatePlugin;
use crate::bindings::procedure::ProcedurePlugin as ProcedurePluginBindings;
use crate::bindings::scalar::ScalarPlugin;
use crate::error::WasmError;
use crate::host_state::HostState;
use crate::pool::WasmInstancePool;

/// CM plugin manifest in canonical JSON form (the plugin's
/// `manifest` export's payload). Mirrors proposal §14 and the Extism
/// manifest shape — same fields, different ABI host.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentManifest {
    /// Reverse-DNS plugin id.
    pub id: String,
    /// Semver string.
    pub version: String,
    /// Component Model ABI range (e.g., `"^1.2"`).
    #[serde(default)]
    pub abi: Option<String>,
    /// Capabilities the plugin declares it needs.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Determinism class.
    #[serde(default)]
    pub determinism: Option<String>,
    /// Free-form human description.
    #[serde(default)]
    pub description: Option<String>,
    /// Per-call wasmtime fuel limit.
    #[serde(default)]
    pub fuel_per_call: Option<u64>,
    /// Maximum linear-memory pages (one page = 64 KiB).
    #[serde(default)]
    pub memory_max_pages: Option<u32>,
    /// Wall-clock per-call timeout in milliseconds.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// Wire-level scalar signature shipped by a plugin's `register` export.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireFnSignature {
    /// Argument types in `WireArgType` form.
    pub args: Vec<WireArgType>,
    /// Return type.
    pub returns: WireArgType,
    /// Volatility — `"immutable"`, `"stable"`, or `"volatile"`.
    #[serde(default = "default_volatility")]
    pub volatility: String,
    /// Null handling — `"propagate"` (default) or `"user_handled"`.
    #[serde(default = "default_null_handling")]
    pub null_handling: String,
}

fn default_volatility() -> String {
    "immutable".to_owned()
}
fn default_null_handling() -> String {
    "propagate".to_owned()
}
fn default_proc_mode() -> String {
    "read".to_owned()
}

/// Wire-level argument type shipped by a plugin.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WireArgType {
    /// Native Arrow primitive (`int64`, `float64`, `utf8`, …).
    Primitive {
        /// Arrow primitive name.
        arrow: String,
    },
    /// Opaque `CypherValue` transported as `LargeBinary`.
    CypherValue,
}

/// One registration entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RegistrationEntry {
    /// A Cypher scalar function.
    Scalar {
        /// Fully-qualified name.
        qname: String,
        /// Signature.
        signature: WireFnSignature,
    },
    /// A Cypher aggregate function.
    Aggregate {
        /// Fully-qualified name.
        qname: String,
        /// Per-row input + return types.
        signature: WireFnSignature,
        /// Opaque per-partition state type — typically
        /// `{"kind":"primitive","arrow":"binary"}`.
        state: WireArgType,
    },
    /// A Cypher procedure.
    Procedure {
        /// Fully-qualified name.
        qname: String,
        /// Argument types.
        args: Vec<WireArgType>,
        /// Yielded column types.
        yields: Vec<WireArgType>,
        /// Mode — `"read"`, `"write"`, `"schema"`, or `"dbms"`.
        #[serde(default = "default_proc_mode")]
        mode: String,
    },
}

/// Top-level `register` export payload.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrationManifest {
    /// Every qname provided by the plugin.
    pub entries: Vec<RegistrationEntry>,
}

/// Outcome of [`WasmLoader::prepare`] — everything the host needs to
/// decide whether to instantiate the component (and what to plumb in).
#[derive(Debug, Clone)]
pub struct PreparedComponent {
    /// Parsed manifest.
    pub manifest: ComponentManifest,
    /// Granted ∩ declared capabilities — what the plugin actually gets.
    pub effective_capabilities: Vec<String>,
    /// Declared-but-not-granted capabilities — used for diagnostics.
    pub denied_capabilities: Vec<String>,
}

/// Concrete instance type pooled by [`WasmInstancePool`].
///
/// Wraps a wasmtime `Store<HostState>` and the typed `ScalarPlugin`
/// binding. One `ScalarPluginInstance` per warm pool slot.
pub struct ScalarPluginInstance {
    store: Store<HostState>,
    bindings: ScalarPlugin,
}

impl std::fmt::Debug for ScalarPluginInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScalarPluginInstance")
            .finish_non_exhaustive()
    }
}

impl ScalarPluginInstance {
    /// Call the plugin's `invoke-scalar` export.
    ///
    /// # Errors
    ///
    /// - [`WasmError::Instantiate`] (re-purposed as the invoke-error
    ///   bucket) if the underlying wasmtime call traps.
    pub fn invoke_scalar(&mut self, qname: &str, ipc: &[u8]) -> Result<Vec<u8>, WasmError> {
        // Reset fuel for each call when the engine was built with
        // `consume_fuel = true`. The store carries the budget set by
        // the manifest's `fuel_per_call`; if absent, we leave the
        // store untouched.
        let result = self
            .bindings
            .call_invoke_scalar(&mut self.store, qname, ipc);
        match result {
            Ok(Ok(bytes)) => Ok(bytes),
            Ok(Err(fn_err)) => Err(WasmError::Instantiate(format!(
                "plugin returned fn-error code={} retryable={}: {}",
                fn_err.code, fn_err.retryable, fn_err.message
            ))),
            Err(e) => Err(WasmError::Instantiate(format!("invoke-scalar trap: {e}"))),
        }
    }

    /// Call the plugin's `manifest` export.
    fn read_manifest(&mut self) -> Result<ComponentManifest, WasmError> {
        let s = self
            .bindings
            .call_manifest(&mut self.store)
            .map_err(|e| WasmError::Instantiate(format!("call manifest: {e}")))?;
        serde_json::from_str(&s)
            .map_err(|e| WasmError::InvalidWasm(format!("manifest json parse: {e}")))
    }

    /// Call the plugin's `register` export.
    fn read_register(&mut self) -> Result<RegistrationManifest, WasmError> {
        let s = self
            .bindings
            .call_register(&mut self.store)
            .map_err(|e| WasmError::Instantiate(format!("call register: {e}")))?;
        serde_json::from_str(&s)
            .map_err(|e| WasmError::InvalidWasm(format!("register json parse: {e}")))
    }
}

/// Pooled instance for the `aggregate-plugin` world.
pub struct AggregatePluginInstance {
    store: Store<HostState>,
    bindings: AggregatePlugin,
}

impl std::fmt::Debug for AggregatePluginInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregatePluginInstance")
            .finish_non_exhaustive()
    }
}

impl AggregatePluginInstance {
    /// Call `agg-new`.
    pub fn agg_new(&mut self, qname: &str) -> Result<Vec<u8>, WasmError> {
        match self.bindings.call_agg_new(&mut self.store, qname) {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(WasmError::Instantiate(format!(
                "agg_new fn-error code={}: {}",
                e.code, e.message
            ))),
            Err(e) => Err(WasmError::Instantiate(format!("agg_new trap: {e}"))),
        }
    }

    /// Call `agg-update`.
    pub fn agg_update(
        &mut self,
        qname: &str,
        state: &[u8],
        values_ipc: &[u8],
    ) -> Result<Vec<u8>, WasmError> {
        match self
            .bindings
            .call_agg_update(&mut self.store, qname, state, values_ipc)
        {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(WasmError::Instantiate(format!(
                "agg_update fn-error code={}: {}",
                e.code, e.message
            ))),
            Err(e) => Err(WasmError::Instantiate(format!("agg_update trap: {e}"))),
        }
    }

    /// Call `agg-merge`.
    pub fn agg_merge(
        &mut self,
        qname: &str,
        state: &[u8],
        other_states_ipc: &[u8],
    ) -> Result<Vec<u8>, WasmError> {
        match self
            .bindings
            .call_agg_merge(&mut self.store, qname, state, other_states_ipc)
        {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(WasmError::Instantiate(format!(
                "agg_merge fn-error code={}: {}",
                e.code, e.message
            ))),
            Err(e) => Err(WasmError::Instantiate(format!("agg_merge trap: {e}"))),
        }
    }

    /// Call `agg-evaluate`.
    pub fn agg_evaluate(&mut self, qname: &str, state: &[u8]) -> Result<Vec<u8>, WasmError> {
        match self
            .bindings
            .call_agg_evaluate(&mut self.store, qname, state)
        {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(WasmError::Instantiate(format!(
                "agg_evaluate fn-error code={}: {}",
                e.code, e.message
            ))),
            Err(e) => Err(WasmError::Instantiate(format!("agg_evaluate trap: {e}"))),
        }
    }
}

/// Pooled instance for the `procedure-plugin` world.
pub struct ProcedurePluginInstance {
    store: Store<HostState>,
    bindings: ProcedurePluginBindings,
}

impl std::fmt::Debug for ProcedurePluginInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcedurePluginInstance")
            .finish_non_exhaustive()
    }
}

impl ProcedurePluginInstance {
    /// Call `invoke-procedure`.
    pub fn invoke_procedure(&mut self, qname: &str, args_ipc: &[u8]) -> Result<Vec<u8>, WasmError> {
        match self
            .bindings
            .call_invoke_procedure(&mut self.store, qname, args_ipc)
        {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(WasmError::Instantiate(format!(
                "invoke_procedure fn-error code={}: {}",
                e.code, e.message
            ))),
            Err(e) => Err(WasmError::Instantiate(format!(
                "invoke_procedure trap: {e}"
            ))),
        }
    }
}

/// Top-level WASM Component Model plugin loader.
#[derive(Debug, Default)]
pub struct WasmLoader {
    _marker: (),
}

impl WasmLoader {
    /// Construct a fresh loader.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse a CM-plugin manifest and intersect declared/granted
    /// capabilities. Deterministic — no wasmtime instantiation.
    ///
    /// # Errors
    ///
    /// - [`WasmError::InvalidWasm`] if the JSON doesn't parse.
    pub fn prepare(
        &self,
        manifest_json: &[u8],
        grants: &[String],
    ) -> Result<PreparedComponent, WasmError> {
        let manifest: ComponentManifest = serde_json::from_slice(manifest_json)
            .map_err(|e| WasmError::InvalidWasm(format!("manifest json parse: {e}")))?;
        Ok(self.prepare_parsed(manifest, grants))
    }

    /// Intersect declared/granted capabilities for an already-parsed
    /// manifest, skipping the JSON round-trip.
    ///
    /// [`Self::load`] reads the manifest export off a bootstrap instance
    /// (parsed `ComponentManifest`), then needs the cap-intersection
    /// result. The previous implementation re-serialized the parsed
    /// struct to JSON and called [`Self::prepare`] which deserialized it
    /// straight back — a wasteful round-trip whose only purpose was
    /// reusing the cap-intersection loop. This entry point preserves the
    /// loop and skips the (de)serialization.
    pub fn prepare_parsed(
        &self,
        manifest: ComponentManifest,
        grants: &[String],
    ) -> PreparedComponent {
        let granted_set: HashSet<&str> = grants.iter().map(String::as_str).collect();
        let mut effective: Vec<String> = Vec::new();
        let mut denied: Vec<String> = Vec::new();
        for cap in &manifest.capabilities {
            if granted_set.contains(cap.as_str()) {
                effective.push(cap.clone());
            } else {
                denied.push(cap.clone());
            }
        }
        PreparedComponent {
            manifest,
            effective_capabilities: effective,
            denied_capabilities: denied,
        }
    }

    /// Instantiate a CM plugin into a fresh `ScalarPluginInstance`.
    ///
    /// Used directly only by tests; production code goes through
    /// [`Self::load`] which two-passes the manifest negotiation.
    ///
    /// # Errors
    ///
    /// - [`WasmError::InvalidWasm`] on Component compilation failure.
    /// - [`WasmError::Instantiate`] on linker / instantiation failure.
    pub fn instantiate(
        &self,
        bytes: &[u8],
        prepared: &PreparedComponent,
    ) -> Result<ScalarPluginInstance, WasmError> {
        let engine = build_engine(&prepared.manifest)?;
        let component = Component::from_binary(&engine, bytes)
            .map_err(|e| WasmError::InvalidWasm(format!("component compile: {e}")))?;
        let linker: Linker<HostState> = select_linker_for_manifest(
            &engine,
            &prepared.manifest,
            &prepared.effective_capabilities,
        )?;
        let mut store = Store::new(
            &engine,
            HostState::new(prepared.effective_capabilities.clone()),
        );
        apply_resource_limits(&mut store, &prepared.manifest);
        let bindings = ScalarPlugin::instantiate(&mut store, &component, &linker)
            .map_err(|e| WasmError::Instantiate(format!("scalar-plugin instantiate: {e}")))?;
        Ok(ScalarPluginInstance { store, bindings })
    }

    /// End-to-end load: read manifest, intersect with host grants,
    /// rebuild with effective caps, read register export, register
    /// scalar adapters with the supplied registrar.
    ///
    /// # Errors
    ///
    /// See [`Self::instantiate`] + manifest / register parse failures.
    pub fn load(
        &self,
        bytes: &[u8],
        host_grants: &[String],
        registrar: &mut uni_plugin::PluginRegistrar<'_>,
    ) -> Result<LoadOutcome, WasmError> {
        // Pass 1 — minimal prepared state (no caps yet), instantiate
        // to read manifest.
        let bootstrap = PreparedComponent {
            manifest: ComponentManifest {
                id: String::new(),
                version: String::new(),
                abi: None,
                capabilities: Vec::new(),
                determinism: None,
                description: None,
                fuel_per_call: None,
                memory_max_pages: None,
                timeout_ms: None,
            },
            effective_capabilities: Vec::new(),
            denied_capabilities: Vec::new(),
        };
        let mut bootstrap_inst = self.instantiate(bytes, &bootstrap)?;
        let parsed_manifest = bootstrap_inst.read_manifest()?;
        drop(bootstrap_inst);

        // Rewrite the registrar's plugin id to match the manifest —
        // caller supplies a placeholder (e.g., "wasm.loading") because
        // the canonical id is unknown until pass 1.
        registrar.set_plugin_id(uni_plugin::PluginId::new(parsed_manifest.id.clone()));

        // Pass 2 — intersect caps, rebuild engine with limits. We
        // already have the parsed manifest from pass 1; route through
        // `prepare_parsed` to avoid a JSON re-serialize / re-parse
        // round-trip.
        let prepared = self.prepare_parsed(parsed_manifest, host_grants);

        // Build the pool. Factory captures owned bytes + prepared.
        let pool = build_scalar_pool(bytes, &prepared)?;

        // Use one warm instance to read the register export.
        let registration = {
            let mut leased = crate::pool::PooledInstance::acquire(Arc::clone(&pool))
                .map_err(|e| WasmError::Instantiate(format!("acquire warm instance: {e}")))?;
            let r = leased.get_mut().read_register()?;
            drop(leased);
            r
        };

        let names = apply_registration(bytes, &prepared, &pool, registration, registrar)?;

        Ok(LoadOutcome {
            plugin_id: prepared.manifest.id.clone(),
            version: prepared.manifest.version.clone(),
            effective_capabilities: prepared.effective_capabilities,
            denied_capabilities: prepared.denied_capabilities,
            scalars_registered: names.scalars,
            aggregates_registered: names.aggregates,
            procedures_registered: names.procedures,
            pool,
        })
    }

    /// Load a component and present it as a [`uni_plugin::Plugin`].
    ///
    /// Unlike [`Self::load`] — which registers adapters directly into a
    /// caller-supplied registrar — this returns a self-contained `Plugin`
    /// whose [`uni_plugin::Plugin::manifest`] is synthesized from the
    /// component's manifest and whose [`uni_plugin::Plugin::register`] replays
    /// the component's `register` entries. It is the bridge the conformance
    /// harness (`uni_plugin_conformance::WasmConformanceLoader`) needs to run
    /// the same probe suite against a real component as against a live-Rust
    /// plugin.
    ///
    /// The returned plugin owns the warm scalar pool plus the component bytes
    /// and negotiated capabilities, so `register` can rebuild
    /// aggregate/procedure pools and is safely re-runnable (the conformance
    /// idempotency probe registers twice).
    ///
    /// # Errors
    ///
    /// See [`Self::load`] — manifest / register parse + instantiation failures,
    /// plus [`WasmError::InvalidWasm`] if the manifest version is not semver.
    pub fn load_as_plugin(
        &self,
        bytes: &[u8],
        host_grants: &[String],
    ) -> Result<Box<dyn uni_plugin::Plugin + Send + Sync>, WasmError> {
        // Pass 1 — instantiate with no caps to read the manifest export.
        let bootstrap = PreparedComponent {
            manifest: ComponentManifest {
                id: String::new(),
                version: String::new(),
                abi: None,
                capabilities: Vec::new(),
                determinism: None,
                description: None,
                fuel_per_call: None,
                memory_max_pages: None,
                timeout_ms: None,
            },
            effective_capabilities: Vec::new(),
            denied_capabilities: Vec::new(),
        };
        let mut bootstrap_inst = self.instantiate(bytes, &bootstrap)?;
        let parsed_manifest = bootstrap_inst.read_manifest()?;
        drop(bootstrap_inst);

        // Pass 2 — intersect caps, build the scalar pool, read register.
        let prepared = self.prepare_parsed(parsed_manifest, host_grants);
        let scalar_pool = build_scalar_pool(bytes, &prepared)?;
        let registration = {
            let mut leased = crate::pool::PooledInstance::acquire(Arc::clone(&scalar_pool))
                .map_err(|e| WasmError::Instantiate(format!("acquire warm instance: {e}")))?;
            let r = leased.get_mut().read_register()?;
            drop(leased);
            r
        };
        let manifest = synthesize_plugin_manifest(&prepared.manifest, &registration)?;
        Ok(Box::new(ComponentPlugin {
            manifest,
            bytes: bytes.to_vec(),
            prepared,
            scalar_pool,
            registration,
        }))
    }
}

/// Outcome of a successful [`WasmLoader::load`].
pub struct LoadOutcome {
    /// Reverse-DNS plugin id from the manifest.
    pub plugin_id: String,
    /// Plugin version from the manifest.
    pub version: String,
    /// Capabilities granted (declared ∩ host).
    pub effective_capabilities: Vec<String>,
    /// Capabilities denied (declared but not granted).
    pub denied_capabilities: Vec<String>,
    /// Qnames registered as scalar fns.
    pub scalars_registered: Vec<String>,
    /// Qnames registered as aggregate fns.
    pub aggregates_registered: Vec<String>,
    /// Qnames registered as procedures.
    pub procedures_registered: Vec<String>,
    /// Scalar-plugin instance pool — `None` if no scalars registered.
    pub pool: Arc<WasmInstancePool<ScalarPluginInstance>>,
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

/// Pick the right per-major scalar linker for `manifest.abi`.
///
/// Bridges the loader to [`crate::multi_version::SUPPORTED_MAJORS`]. A
/// missing `abi` field defaults to v1 for backward compatibility with
/// the M6b cutover's single-major linker — early plugins predate the
/// multi-version surface.
fn select_linker_for_manifest(
    engine: &Engine,
    manifest: &ComponentManifest,
    effective_caps: &[String],
) -> Result<Linker<HostState>, WasmError> {
    use crate::linker::{build_scalar_linker_v1, build_scalar_linker_v2};
    use crate::multi_version::SUPPORTED_MAJORS;

    let Some(abi_str) = manifest.abi.as_deref() else {
        return build_scalar_linker_v1(engine, effective_caps);
    };
    let abi = uni_plugin::AbiRange::parse(abi_str)
        .map_err(|e| WasmError::InvalidWasm(format!("manifest abi parse: {e}")))?;
    let Some(major) = SUPPORTED_MAJORS.iter().copied().find(|m| abi.matches(*m)) else {
        return Err(WasmError::AbiUnsupported {
            requested: abi_str.to_owned(),
            supported: SUPPORTED_MAJORS.to_vec(),
        });
    };
    match major {
        1 => build_scalar_linker_v1(engine, effective_caps),
        2 => build_scalar_linker_v2(engine, effective_caps),
        _ => Err(WasmError::AbiUnsupported {
            requested: abi_str.to_owned(),
            supported: SUPPORTED_MAJORS.to_vec(),
        }),
    }
}

fn build_engine(manifest: &ComponentManifest) -> Result<Engine, WasmError> {
    let mut cfg = Config::new();
    cfg.wasm_component_model(true);
    if manifest.fuel_per_call.is_some() {
        cfg.consume_fuel(true);
    }
    if manifest.timeout_ms.is_some() {
        cfg.epoch_interruption(true);
    }
    Engine::new(&cfg).map_err(|e| WasmError::Instantiate(format!("engine config: {e}")))
}

fn apply_resource_limits(store: &mut Store<HostState>, manifest: &ComponentManifest) {
    if let Some(fuel) = manifest.fuel_per_call {
        // Best-effort fuel cap. Plugins consuming more than this trap
        // out of fuel; the host surfaces as `WasmError::ResourceLimit`.
        let _ = store.set_fuel(fuel);
    }
    if let Some(ms) = manifest.timeout_ms {
        // Set the store's epoch deadline; a per-engine timer ticks
        // the epoch and traps the plugin. Pure-compute plugins
        // without a timer config become no-op for this field.
        let _ = ms;
        store.set_epoch_deadline(1);
    }
}

/// Generic CM-plugin pool factory.
///
/// The per-surface (`scalar` / `aggregate` / `procedure`) builders only
/// differ in (a) which `wit-bindgen`-generated `instantiate` fn they
/// call (b) how they pack the resulting `Store` + bindings into the
/// surface-specific instance struct, and (c) the surface-name string
/// for error messages. The caller-supplied closure receives a freshly
/// built `Store` (already cap-limited) plus the linker and component
/// and returns the surface-specific instance. Engine config, component
/// compile, linker selection, and resource-limit application are
/// shared.
fn build_pool<I, F>(
    bytes: &[u8],
    prepared: &PreparedComponent,
    build_instance: F,
) -> Result<Arc<WasmInstancePool<I>>, WasmError>
where
    I: Send + 'static,
    F: Fn(Store<HostState>, &Component, &Linker<HostState>) -> Result<I, WasmError>
        + Send
        + Sync
        + 'static,
{
    let bytes_owned: Arc<Vec<u8>> = Arc::new(bytes.to_vec());
    let prepared_owned: Arc<PreparedComponent> = Arc::new(prepared.clone());
    let build_instance = Arc::new(build_instance);

    let factory = {
        let bytes = Arc::clone(&bytes_owned);
        let prepared = Arc::clone(&prepared_owned);
        let build_instance = Arc::clone(&build_instance);
        move || -> Result<I, WasmError> {
            let engine = build_engine(&prepared.manifest)?;
            let component = Component::from_binary(&engine, &bytes)
                .map_err(|e| WasmError::InvalidWasm(format!("component compile: {e}")))?;
            let linker: Linker<HostState> = select_linker_for_manifest(
                &engine,
                &prepared.manifest,
                &prepared.effective_capabilities,
            )?;
            let mut store = Store::new(
                &engine,
                HostState::new(prepared.effective_capabilities.clone()),
            );
            apply_resource_limits(&mut store, &prepared.manifest);
            build_instance(store, &component, &linker)
        }
    };

    let pool = WasmInstancePool::new(crate::pool::PoolConfig::default(), factory)?;
    Ok(Arc::new(pool))
}

/// Qnames registered by [`apply_registration`], grouped by surface.
struct RegisteredQNames {
    scalars: Vec<String>,
    aggregates: Vec<String>,
    procedures: Vec<String>,
}

/// Replay a parsed `register` manifest into `registrar`.
///
/// Constructs one adapter per entry from `scalar_pool` and from
/// aggregate/procedure pools built lazily from `bytes` + `prepared`. Shared by
/// [`WasmLoader::load`] and [`ComponentPlugin::register`] so both register
/// identically — no probe behaves differently against the two paths.
fn apply_registration(
    bytes: &[u8],
    prepared: &PreparedComponent,
    scalar_pool: &Arc<WasmInstancePool<ScalarPluginInstance>>,
    registration: RegistrationManifest,
    registrar: &mut uni_plugin::PluginRegistrar<'_>,
) -> Result<RegisteredQNames, WasmError> {
    let mut scalars = Vec::new();
    let mut aggregates = Vec::new();
    let mut procedures = Vec::new();
    let mut agg_pool: Option<Arc<WasmInstancePool<AggregatePluginInstance>>> = None;
    let mut proc_pool: Option<Arc<WasmInstancePool<ProcedurePluginInstance>>> = None;

    for entry in registration.entries {
        match entry {
            RegistrationEntry::Scalar { qname, signature } => {
                let parsed_qname = uni_plugin::QName::parse(&qname)
                    .map_err(|e| WasmError::InvalidWasm(format!("invalid qname `{qname}`: {e}")))?;
                let sig = wire_fn_sig_to_internal(&signature)?;
                let adapter = Arc::new(ComponentScalarFn::new(
                    Arc::clone(scalar_pool),
                    parsed_qname.clone(),
                    sig.clone(),
                ));
                registrar
                    .scalar_fn(parsed_qname, sig, adapter)
                    .map_err(|e| {
                        WasmError::Internal(format!("registrar.scalar_fn `{qname}`: {e}"))
                    })?;
                scalars.push(qname);
            }
            RegistrationEntry::Aggregate {
                qname,
                signature,
                state,
            } => {
                let parsed_qname = uni_plugin::QName::parse(&qname)
                    .map_err(|e| WasmError::InvalidWasm(format!("invalid qname `{qname}`: {e}")))?;
                let sig = wire_agg_sig_to_internal(&signature, &state)?;
                let pool_ref = match &agg_pool {
                    Some(p) => Arc::clone(p),
                    None => {
                        let p = build_aggregate_pool(bytes, prepared)?;
                        agg_pool = Some(Arc::clone(&p));
                        p
                    }
                };
                let adapter = Arc::new(ComponentAggregateFn::new(
                    pool_ref,
                    parsed_qname.clone(),
                    sig.clone(),
                ));
                registrar
                    .aggregate_fn(parsed_qname, sig, adapter)
                    .map_err(|e| {
                        WasmError::Internal(format!("registrar.aggregate_fn `{qname}`: {e}"))
                    })?;
                aggregates.push(qname);
            }
            RegistrationEntry::Procedure {
                qname,
                args,
                yields,
                mode,
            } => {
                let parsed_qname = uni_plugin::QName::parse(&qname)
                    .map_err(|e| WasmError::InvalidWasm(format!("invalid qname `{qname}`: {e}")))?;
                let sig = wire_proc_sig_to_internal(&args, &yields, &mode)?;
                let pool_ref = match &proc_pool {
                    Some(p) => Arc::clone(p),
                    None => {
                        let p = build_procedure_pool(bytes, prepared)?;
                        proc_pool = Some(Arc::clone(&p));
                        p
                    }
                };
                let adapter = Arc::new(ComponentProcedure::new(
                    pool_ref,
                    parsed_qname.clone(),
                    sig.clone(),
                ));
                registrar
                    .procedure(parsed_qname, sig, adapter)
                    .map_err(|e| {
                        WasmError::Internal(format!("registrar.procedure `{qname}`: {e}"))
                    })?;
                procedures.push(qname);
            }
        }
    }

    Ok(RegisteredQNames {
        scalars,
        aggregates,
        procedures,
    })
}

/// Synthesize a [`uni_plugin::PluginManifest`] from a component manifest.
///
/// The declared `CapabilitySet` includes the extension capabilities implied by
/// the `register` entries (`Capability::ScalarFn` for a scalar, etc.), so a
/// registrar built from this manifest permits exactly the registrations the
/// component will perform — which is what the conformance registration probes
/// rely on. ABI defaults to `^1` when the component omits it.
///
/// # Errors
///
/// Returns [`WasmError::InvalidWasm`] if the version is not valid semver or the
/// ABI range is malformed.
fn synthesize_plugin_manifest(
    component: &ComponentManifest,
    registration: &RegistrationManifest,
) -> Result<uni_plugin::PluginManifest, WasmError> {
    use uni_plugin::{
        AbiRange, Capability, CapabilitySet, Determinism, PluginId, ProvidedSurfaces, Scope,
        SideEffects,
    };

    let version = semver::Version::parse(&component.version).map_err(|e| {
        WasmError::InvalidWasm(format!("manifest version `{}`: {e}", component.version))
    })?;
    let abi = AbiRange::parse(component.abi.as_deref().unwrap_or("^1"))
        .map_err(|e| WasmError::InvalidWasm(format!("manifest abi: {e}")))?;

    let mut capabilities = CapabilitySet::new();
    let mut side_effects = SideEffects::ReadOnly;
    for entry in &registration.entries {
        match entry {
            RegistrationEntry::Scalar { .. } => {
                capabilities.insert(Capability::ScalarFn);
            }
            RegistrationEntry::Aggregate { .. } => {
                capabilities.insert(Capability::AggregateFn);
            }
            RegistrationEntry::Procedure { mode, .. } => {
                capabilities.insert(Capability::Procedure);
                match mode.as_str() {
                    "write" => {
                        capabilities.insert(Capability::ProcedureWrites);
                        side_effects = SideEffects::Writes;
                    }
                    "schema" => {
                        capabilities.insert(Capability::ProcedureSchema);
                        side_effects = SideEffects::Writes;
                    }
                    "dbms" => {
                        capabilities.insert(Capability::ProcedureDbms);
                    }
                    _ => {}
                }
            }
        }
    }

    let determinism = match component.determinism.as_deref() {
        Some("pure") => Determinism::Pure,
        Some("session-scoped" | "session_scoped") => Determinism::SessionScoped,
        _ => Determinism::Nondeterministic,
    };

    Ok(uni_plugin::PluginManifest {
        id: PluginId::new(component.id.clone()),
        version,
        abi,
        depends_on: Vec::new(),
        capabilities,
        determinism,
        side_effects,
        scope: Scope::Instance,
        hash: None,
        signature: None,
        provides: ProvidedSurfaces::default(),
        docs: component.description.clone().unwrap_or_default(),
        metadata: std::collections::BTreeMap::new(),
    })
}

/// A loaded WASM component presented as a [`uni_plugin::Plugin`].
///
/// Produced by [`WasmLoader::load_as_plugin`]. Holds the warm scalar pool plus
/// the component bytes and negotiated capabilities so its `register` impl can
/// rebuild aggregate/procedure pools and replay registration on each call.
pub struct ComponentPlugin {
    manifest: uni_plugin::PluginManifest,
    bytes: Vec<u8>,
    prepared: PreparedComponent,
    scalar_pool: Arc<WasmInstancePool<ScalarPluginInstance>>,
    registration: RegistrationManifest,
}

impl std::fmt::Debug for ComponentPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentPlugin")
            .field("id", &self.manifest.id.as_str())
            .field("scalars", &self.registration.entries.len())
            .finish()
    }
}

impl uni_plugin::Plugin for ComponentPlugin {
    fn manifest(&self) -> &uni_plugin::PluginManifest {
        &self.manifest
    }

    fn register(
        &self,
        r: &mut uni_plugin::PluginRegistrar<'_>,
    ) -> Result<(), uni_plugin::PluginError> {
        apply_registration(
            &self.bytes,
            &self.prepared,
            &self.scalar_pool,
            self.registration.clone(),
            r,
        )
        .map_err(|e| {
            uni_plugin::PluginError::WasmInstantiate(format!("component register: {e}"))
        })?;
        Ok(())
    }
}

fn build_scalar_pool(
    bytes: &[u8],
    prepared: &PreparedComponent,
) -> Result<Arc<WasmInstancePool<ScalarPluginInstance>>, WasmError> {
    build_pool(bytes, prepared, |mut store, component, linker| {
        let bindings = ScalarPlugin::instantiate(&mut store, component, linker)
            .map_err(|e| WasmError::Instantiate(format!("scalar-plugin instantiate: {e}")))?;
        Ok(ScalarPluginInstance { store, bindings })
    })
}

fn build_aggregate_pool(
    bytes: &[u8],
    prepared: &PreparedComponent,
) -> Result<Arc<WasmInstancePool<AggregatePluginInstance>>, WasmError> {
    build_pool(bytes, prepared, |mut store, component, linker| {
        let bindings = AggregatePlugin::instantiate(&mut store, component, linker)
            .map_err(|e| WasmError::Instantiate(format!("aggregate-plugin instantiate: {e}")))?;
        Ok(AggregatePluginInstance { store, bindings })
    })
}

fn build_procedure_pool(
    bytes: &[u8],
    prepared: &PreparedComponent,
) -> Result<Arc<WasmInstancePool<ProcedurePluginInstance>>, WasmError> {
    build_pool(bytes, prepared, |mut store, component, linker| {
        let bindings = ProcedurePluginBindings::instantiate(&mut store, component, linker)
            .map_err(|e| WasmError::Instantiate(format!("procedure-plugin instantiate: {e}")))?;
        Ok(ProcedurePluginInstance { store, bindings })
    })
}

fn wire_agg_sig_to_internal(
    wire_sig: &WireFnSignature,
    wire_state: &WireArgType,
) -> Result<uni_plugin::traits::aggregate::AggSignature, WasmError> {
    use arrow_schema::Field;
    use datafusion::logical_expr::Volatility;
    use uni_plugin::traits::aggregate::AggSignature;
    use uni_plugin::traits::scalar::ArgType;

    let internal = wire_fn_sig_to_internal(wire_sig)?;
    let state_field = match wire_state {
        WireArgType::Primitive { arrow } => {
            let dt = arrow_name_to_dt(arrow)?;
            Field::new("state", dt, true)
        }
        _ => {
            return Err(WasmError::InvalidWasm(
                "aggregate state must be a Primitive Arrow type".to_owned(),
            ));
        }
    };
    let volatility = match wire_sig.volatility.as_str() {
        "immutable" => Volatility::Immutable,
        "stable" => Volatility::Stable,
        "volatile" => Volatility::Volatile,
        other => {
            return Err(WasmError::InvalidWasm(format!(
                "unsupported volatility: `{other}`"
            )));
        }
    };
    let args: Vec<ArgType> = internal.args;
    let returns = internal.returns;
    Ok(AggSignature {
        args,
        returns,
        state_fields: vec![state_field],
        volatility,
        supports_partial: true,
    })
}

fn wire_proc_sig_to_internal(
    args: &[WireArgType],
    yields: &[WireArgType],
    mode: &str,
) -> Result<uni_plugin::traits::procedure::ProcedureSignature, WasmError> {
    use arrow_schema::Field;
    use uni_plugin::capability::SideEffects;
    use uni_plugin::traits::procedure::{NamedArgType, ProcedureMode, ProcedureSignature};
    use uni_plugin::traits::scalar::ArgType;

    fn wire_arg(w: &WireArgType) -> Result<ArgType, WasmError> {
        Ok(match w {
            WireArgType::Primitive { arrow } => ArgType::Primitive(arrow_name_to_dt(arrow)?),
            WireArgType::CypherValue => ArgType::CypherValue,
        })
    }
    let named_args: Vec<NamedArgType> = args
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let ty = wire_arg(w)?;
            Ok::<NamedArgType, WasmError>(NamedArgType {
                name: format!("arg{i}").into(),
                ty,
                default: None,
                doc: String::new(),
            })
        })
        .collect::<Result<_, _>>()?;
    let yield_fields: Vec<Field> = yields
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let ty = wire_arg(w)?;
            let dt = match ty {
                ArgType::Primitive(d) => d,
                ArgType::CypherValue | ArgType::Variadic(_) => arrow_schema::DataType::LargeBinary,
                ArgType::Vector { element, .. } => element,
            };
            Ok::<Field, WasmError>(Field::new(format!("yield{i}"), dt, true))
        })
        .collect::<Result<_, _>>()?;
    let proc_mode = match mode {
        "read" => ProcedureMode::Read,
        "write" => ProcedureMode::Write,
        "schema" => ProcedureMode::Schema,
        "dbms" => ProcedureMode::Dbms,
        other => {
            return Err(WasmError::InvalidWasm(format!(
                "unsupported procedure mode: `{other}`"
            )));
        }
    };
    Ok(ProcedureSignature {
        args: named_args,
        yields: yield_fields,
        mode: proc_mode,
        side_effects: SideEffects::default(),
        retry_contract: None,
        batch_input: None,
        docs: String::new(),
    })
}

fn arrow_name_to_dt(name: &str) -> Result<arrow_schema::DataType, WasmError> {
    uni_plugin::adapter_common::arrow_types::arrow_name_to_datatype(name)
        .ok_or_else(|| WasmError::InvalidWasm(format!("unsupported arrow primitive: `{name}`")))
}

fn wire_fn_sig_to_internal(
    wire: &WireFnSignature,
) -> Result<uni_plugin::traits::scalar::FnSignature, WasmError> {
    use arrow_schema::DataType;
    use datafusion::logical_expr::Volatility;
    use uni_plugin::traits::scalar::{ArgType, FnSignature, NullHandling};

    fn wire_arg(w: &WireArgType) -> Result<ArgType, WasmError> {
        Ok(match w {
            WireArgType::Primitive { arrow } => ArgType::Primitive(arrow_name(arrow)?),
            WireArgType::CypherValue => ArgType::CypherValue,
        })
    }
    fn arrow_name(name: &str) -> Result<DataType, WasmError> {
        arrow_name_to_dt(name)
    }
    let args: Vec<ArgType> = wire.args.iter().map(wire_arg).collect::<Result<_, _>>()?;
    let returns = wire_arg(&wire.returns)?;
    let volatility = match wire.volatility.as_str() {
        "immutable" => Volatility::Immutable,
        "stable" => Volatility::Stable,
        "volatile" => Volatility::Volatile,
        other => {
            return Err(WasmError::InvalidWasm(format!(
                "unsupported volatility: `{other}`"
            )));
        }
    };
    let null_handling = match wire.null_handling.as_str() {
        "propagate" => NullHandling::PropagateNulls,
        "user_handled" => NullHandling::UserHandled,
        other => {
            return Err(WasmError::InvalidWasm(format!(
                "unsupported null_handling: `{other}`"
            )));
        }
    };
    Ok(FnSignature {
        args,
        returns,
        volatility,
        null_handling,
    })
}

// Kept for future epoch-deadline timer use; currently no-op stub
// (timeout enforcement requires a per-engine timer task — Phase D
// expands this when the pool's prewarm path lands).
#[allow(dead_code)]
fn epoch_timeout_marker() -> Duration {
    Duration::from_millis(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_json(caps: &[&str]) -> String {
        let caps_json: Vec<String> = caps.iter().map(|c| format!("\"{c}\"")).collect();
        format!(
            r#"{{ "id": "ai.example.test", "version": "1.0.0", "capabilities": [{}] }}"#,
            caps_json.join(", ")
        )
    }

    #[test]
    fn loader_constructs() {
        let _ = WasmLoader::new();
    }

    #[test]
    fn prepare_parses_minimal_manifest() {
        let l = WasmLoader::new();
        let json = manifest_json(&[]);
        let prep = l.prepare(json.as_bytes(), &[]).unwrap();
        assert_eq!(prep.manifest.id, "ai.example.test");
        assert!(prep.effective_capabilities.is_empty());
    }

    #[test]
    fn prepare_intersects_capabilities() {
        let l = WasmLoader::new();
        let json = manifest_json(&["Filesystem", "Network", "Kms"]);
        let grants = vec!["Filesystem".to_owned(), "Network".to_owned()];
        let prep = l.prepare(json.as_bytes(), &grants).unwrap();
        assert_eq!(prep.effective_capabilities.len(), 2);
        assert_eq!(prep.denied_capabilities, vec!["Kms"]);
    }

    #[test]
    fn prepare_rejects_malformed_manifest() {
        let l = WasmLoader::new();
        let err = l.prepare(b"not json", &[]).unwrap_err();
        assert!(matches!(err, WasmError::InvalidWasm(_)));
    }

    #[test]
    fn instantiate_rejects_garbage_bytes() {
        let l = WasmLoader::new();
        let prep = l
            .prepare(b"{\"id\":\"a.b\",\"version\":\"0.0.0\"}", &[])
            .unwrap();
        let err = l.instantiate(b"not real wasm", &prep).unwrap_err();
        assert!(matches!(err, WasmError::InvalidWasm(_)));
    }
}
