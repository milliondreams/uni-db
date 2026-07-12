//! PyO3 loader — three-phase load mirroring `RhaiLoader::load` /
//! `ExtismLoader::load`.
//!
//! Phase 1: install a `_uni_decorator_sink` object into the module
//!           namespace whose `.scalar_fn(...)`, `.aggregate_fn(...)`,
//!           `.procedure(...)` methods return decorators that record
//!           the wrapped callable into a [`ManifestBuilder`].
//! Phase 2: execute the Python module source. Each decorator
//!           invocation appends to the builder; the user's callable
//!           is returned unwrapped so the module can still import /
//!           call its own functions.
//! Phase 3: drain the builder, intersect declared caps with host
//!           grants, and register each entry as an adapter on the
//!           supplied [`PluginRegistrar`].
//!
//! The caller commits the registrar to the registry on success.

#![cfg(feature = "pyo3")]

use std::ffi::CString;
use std::sync::Arc;

use arrow_schema::DataType;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
use smol_str::SmolStr;

use uni_plugin::traits::scalar::{ArgType, FnSignature, NullHandling};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, QName};

use crate::adapter_aggregate::{PyAggregateFn, build_py_agg_signature};
use crate::adapter_procedure::PyProcedure;
use crate::adapter_scalar::PyScalarFn;
use crate::adapter_scalar_helpers::{
    determinism_to_volatility, type_name_to_datatype as type_name_to_datatype_shared,
};
use crate::error::PyPluginError;
use crate::manifest::{
    ManifestBuilder, PyAggregateEntry, PyManifest, PyProcedureEntry, PyScalarEntry,
};
use crate::runtime::PyPluginRuntime;

/// Outcome of a successful Python plugin load.
#[derive(Debug)]
pub struct LoadOutcome {
    /// Plugin id (either declared via `db.set_plugin_id(...)` or
    /// defaulted from the loader's `default_plugin_id` argument).
    pub plugin_id: PluginId,
    /// Plugin version string (defaults to `"0.0.0"`).
    pub version: String,
    /// Capabilities both declared and granted (the intersection).
    pub effective_capabilities: CapabilitySet,
    /// Capabilities declared by entries but not granted by the host.
    pub denied_capabilities: Vec<Capability>,
    /// Scalar fn qnames registered.
    pub scalars_registered: Vec<String>,
    /// Aggregate qnames registered (M8.5).
    pub aggregates_registered: Vec<String>,
    /// Procedure qnames registered (M8.7).
    pub procedures_registered: Vec<String>,
    /// Strong reference to the per-plugin runtime. Adapters hold inner
    /// `Arc` clones; the host can drop this on unload to release the
    /// captured callables.
    pub runtime: Arc<PyPluginRuntime>,
}

/// PyO3 plugin loader.
///
/// Unlike the WASM loaders, the PyO3 loader doesn't manage a
/// host-function registry — Python plugins call host capabilities
/// directly via the `db` handle the bindings layer exposes. Capabilities
/// in M8 are *declared metadata* and gate registration at the
/// [`PluginRegistrar`] level; there is no structural sandbox layer.
#[derive(Default, Clone)]
pub struct PyPluginLoader {
    /// Default plugin id used when the loaded module does not call
    /// `db.set_plugin_id(...)`. Typically a session-scoped synthetic
    /// id like `"py.session.<id>"`.
    default_plugin_id: Option<SmolStr>,
}

impl std::fmt::Debug for PyPluginLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyPluginLoader")
            .field("default_plugin_id", &self.default_plugin_id)
            .finish()
    }
}

impl PyPluginLoader {
    /// Construct a loader with no default plugin id (modules MUST set
    /// one via the decorator sink or the load will fail with
    /// `ManifestInvalid`).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a loader with a default plugin id used when the
    /// module does not set one.
    #[must_use]
    pub fn with_default_plugin_id(id: impl Into<SmolStr>) -> Self {
        Self {
            default_plugin_id: Some(id.into()),
        }
    }

    /// Load a Python plugin from source.
    ///
    /// The caller passes `module_src` (Python source code) and
    /// `module_name` (the simulated `__name__` for the module — e.g.,
    /// `"ai.example.geo"`). The loader exec's the source against a
    /// fresh module namespace that includes a `_uni_decorator_sink`
    /// global; decorators on functions in the source append to the
    /// builder; on completion the loader drains the builder, builds a
    /// [`PyPluginRuntime`], and registers adapters on the registrar.
    ///
    /// # Errors
    ///
    /// - [`PyPluginError::ManifestInvalid`] if the module fails to
    ///   exec, declares no entries, or declares an entry with an
    ///   unknown type name.
    /// - [`PyPluginError::PythonException`] (via `From<PyErr>`) for
    ///   Python errors raised by the module body.
    /// - [`PyPluginError::RegistrarRejected`] if the registrar rejects
    ///   an adapter (missing capability, duplicate qname).
    pub fn load(
        &self,
        py: Python<'_>,
        module_src: &str,
        module_name: &str,
        registrar: &mut PluginRegistrar<'_>,
        registrar_caps: &CapabilitySet,
    ) -> Result<LoadOutcome, PyPluginError> {
        // Phase 1: install the decorator sink in fresh module globals.
        let builder = ManifestBuilder::new();
        let module = build_module_with_sink(py, module_name, &builder)?;

        // Phase 2: exec the source against the module's globals.
        let module_src_c = CString::new(module_src).map_err(|e| {
            PyPluginError::ManifestInvalid(format!("module source contains NUL: {e}"))
        })?;
        py.run(
            module_src_c.as_c_str(),
            Some(&module.dict()),
            Some(&module.dict()),
        )
        .map_err(|err| {
            // Tag the embedded "<unknown>" qname with the module name
            // so error messages tell the operator which module body
            // failed to import.
            PyPluginError::from(err).with_qname(format!("<module {module_name}>"))
        })?;

        // Phase 3: drain the builder and register adapters.
        let manifest = builder.into_manifest();
        self.finalize(&manifest, module_name, registrar, registrar_caps)
    }

    /// Drain a [`ManifestBuilder`] populated by the bindings-side
    /// incremental decorator pattern (`@db.scalar_fn` calls on a live
    /// `Database` handle).
    ///
    /// Used by `bindings/uni-db/src/plugin_pyo3.rs::finalize_plugin`
    /// to commit accumulated decorations.
    ///
    /// # Errors
    ///
    /// Same shape as [`Self::load`].
    pub fn load_from_builder(
        &self,
        builder: &ManifestBuilder,
        registrar: &mut PluginRegistrar<'_>,
        registrar_caps: &CapabilitySet,
    ) -> Result<LoadOutcome, PyPluginError> {
        let manifest = builder.into_manifest();
        self.finalize(&manifest, "py.live", registrar, registrar_caps)
    }

    /// Validate the drained manifest, resolve the plugin id, derive +
    /// intersect capabilities, and register each granted adapter family
    /// on `registrar`. Shared tail of [`Self::load`] and
    /// [`Self::load_from_builder`].
    ///
    /// `default_id` is the fallback module/scope name fed to
    /// [`Self::resolve_plugin_id`].
    ///
    /// # Errors
    ///
    /// See [`Self::load`].
    fn finalize(
        &self,
        manifest: &PyManifest,
        default_id: &str,
        registrar: &mut PluginRegistrar<'_>,
        registrar_caps: &CapabilitySet,
    ) -> Result<LoadOutcome, PyPluginError> {
        manifest.validate_non_empty()?;

        let resolved_id = self.resolve_plugin_id(manifest, default_id)?;
        let runtime = PyPluginRuntime::new(resolved_id.clone());

        let declared = derive_declared_capabilities(manifest);
        let (effective, denied) = intersect_caps(&declared, registrar_caps);

        registrar.set_plugin_id(resolved_id.clone());

        let scalars_registered = if effective.contains(&Capability::ScalarFn) {
            register_scalars(
                registrar,
                Arc::clone(&runtime),
                &resolved_id,
                &manifest.scalar_fns,
                &manifest.determinism,
            )?
        } else {
            Vec::new()
        };
        let aggregates_registered = if effective.contains(&Capability::AggregateFn) {
            register_aggregates(
                registrar,
                Arc::clone(&runtime),
                &resolved_id,
                &manifest.aggregate_fns,
                &manifest.determinism,
            )?
        } else {
            Vec::new()
        };
        let procedures_registered = if effective.contains(&Capability::Procedure) {
            register_procedures(
                registrar,
                Arc::clone(&runtime),
                &resolved_id,
                &manifest.procedures,
            )?
        } else {
            Vec::new()
        };
        if effective.contains(&Capability::Algorithm) {
            register_algorithms(
                registrar,
                Arc::clone(&runtime),
                &resolved_id,
                &manifest.algorithms,
            )?;
        }

        Ok(LoadOutcome {
            plugin_id: resolved_id,
            version: manifest.version.to_string(),
            effective_capabilities: effective,
            denied_capabilities: denied,
            scalars_registered,
            aggregates_registered,
            procedures_registered,
            runtime,
        })
    }

    fn resolve_plugin_id(
        &self,
        manifest: &PyManifest,
        module_name: &str,
    ) -> Result<PluginId, PyPluginError> {
        // Precedence: manifest > loader default > module name.
        let id_str: SmolStr = if manifest.id.as_str() != "py.live" {
            manifest.id.clone()
        } else if let Some(d) = &self.default_plugin_id {
            d.clone()
        } else if !module_name.is_empty() && module_name != "py.live" {
            SmolStr::new(module_name)
        } else {
            return Err(PyPluginError::ManifestInvalid(
                "plugin id was neither declared in the module nor supplied by the loader".into(),
            ));
        };
        Ok(PluginId::new(id_str))
    }
}

/// Resolve an entry's effective determinism: a per-entry declaration wins, and
/// the sentinel `"inherit"` (the decorator default) falls back to the
/// manifest-wide value set via `db.set_determinism(...)`. This is what makes
/// `set_determinism` observable — without it, entries always kept the decorator
/// default and the manifest-wide setting was dead.
fn effective_determinism<'a>(entry_determinism: &'a str, manifest_determinism: &'a str) -> &'a str {
    if entry_determinism == "inherit" {
        manifest_determinism
    } else {
        entry_determinism
    }
}

fn register_scalars(
    registrar: &mut PluginRegistrar<'_>,
    runtime: Arc<PyPluginRuntime>,
    plugin_id: &PluginId,
    entries: &[PyScalarEntry],
    manifest_determinism: &str,
) -> Result<Vec<String>, PyPluginError> {
    let mut registered = Vec::with_capacity(entries.len());
    for entry in entries {
        let args_types: Vec<ArgType> = entry
            .args
            .iter()
            .map(|t| type_name_to_argtype(t.as_str()))
            .collect::<Result<_, PyPluginError>>()?;
        let returns_type = type_name_to_argtype(entry.returns.as_str())?;
        let determinism = effective_determinism(entry.determinism.as_str(), manifest_determinism);
        let sig = FnSignature {
            args: args_types,
            returns: returns_type,
            volatility: determinism_to_volatility(determinism),
            null_handling: NullHandling::PropagateNulls,
        };

        let local_name = entry.name.clone();
        let qname = QName::new(plugin_id.as_str(), local_name.clone());

        // Install the captured callable into the runtime under the local name.
        let callable = Python::attach(|py| entry.callable.clone_ref(py));
        runtime.insert(local_name.clone(), callable);

        let adapter = if entry.vectorized {
            PyScalarFn::new_vectorized(Arc::clone(&runtime), local_name, sig.clone())
        } else {
            PyScalarFn::new(Arc::clone(&runtime), local_name, sig.clone())
        };

        registrar
            .scalar_fn(qname.clone(), sig, Arc::new(adapter))
            .map_err(PyPluginError::from)?;
        registered.push(qname.to_string());
    }
    Ok(registered)
}

fn register_aggregates(
    registrar: &mut PluginRegistrar<'_>,
    runtime: Arc<PyPluginRuntime>,
    plugin_id: &PluginId,
    entries: &[PyAggregateEntry],
    manifest_determinism: &str,
) -> Result<Vec<String>, PyPluginError> {
    let mut registered = Vec::with_capacity(entries.len());
    for entry in entries {
        let determinism = effective_determinism(entry.determinism.as_str(), manifest_determinism);
        let sig = build_py_agg_signature(&entry.args, &entry.returns, determinism)
            .map_err(|e| PyPluginError::ManifestInvalid(e.message))?;
        let local_name = entry.name.clone();
        let qname = QName::new(plugin_id.as_str(), local_name.clone());

        // Install the four callables under deterministic
        // `name::init|accumulate|merge|finalize` keys so the
        // accumulator can resolve them. We clone the Py refs under the
        // GIL.
        Python::attach(|py| {
            runtime.insert(format!("{local_name}::init"), entry.init.clone_ref(py));
            runtime.insert(
                format!("{local_name}::accumulate"),
                entry.accumulate.clone_ref(py),
            );
            runtime.insert(format!("{local_name}::merge"), entry.merge.clone_ref(py));
            runtime.insert(
                format!("{local_name}::finalize"),
                entry.finalize.clone_ref(py),
            );
        });

        let adapter = PyAggregateFn::new(Arc::clone(&runtime), local_name, sig.clone());
        registrar
            .aggregate_fn(qname.clone(), sig, Arc::new(adapter))
            .map_err(PyPluginError::from)?;
        registered.push(qname.to_string());
    }
    Ok(registered)
}

fn register_procedures(
    registrar: &mut PluginRegistrar<'_>,
    runtime: Arc<PyPluginRuntime>,
    plugin_id: &PluginId,
    entries: &[PyProcedureEntry],
) -> Result<Vec<String>, PyPluginError> {
    use arrow_schema::Field;
    use uni_plugin::capability::SideEffects;
    use uni_plugin::traits::procedure::{NamedArgType, ProcedureMode, ProcedureSignature};

    let mut registered = Vec::with_capacity(entries.len());
    for entry in entries {
        let args: Vec<NamedArgType> = entry
            .args
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let ty = type_name_to_argtype(t.as_str())?;
                Ok(NamedArgType {
                    name: SmolStr::from(format!("arg{i}")),
                    ty,
                    default: None,
                    doc: String::new(),
                })
            })
            .collect::<Result<_, PyPluginError>>()?;
        let yields: Vec<Field> = entry
            .yields
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let dt = type_name_to_datatype(t.as_str())?;
                Ok(Field::new(format!("col{i}"), dt, true))
            })
            .collect::<Result<_, PyPluginError>>()?;
        let mode = match entry.mode.trim().to_ascii_lowercase().as_str() {
            "write" => ProcedureMode::Write,
            "schema" => ProcedureMode::Schema,
            "dbms" => ProcedureMode::Dbms,
            _ => ProcedureMode::Read,
        };
        let side_effects = match mode {
            ProcedureMode::Read => SideEffects::ReadOnly,
            _ => SideEffects::Writes,
        };
        let sig = ProcedureSignature {
            args,
            yields,
            mode,
            side_effects,
            retry_contract: None,
            batch_input: None,
            docs: String::new(),
        };

        let local_name = entry.name.clone();
        let qname = QName::new(plugin_id.as_str(), local_name.clone());

        let callable = Python::attach(|py| entry.callable.clone_ref(py));
        runtime.insert(local_name.clone(), callable);
        let adapter = PyProcedure::new(Arc::clone(&runtime), local_name, sig.clone());
        registrar
            .procedure(qname.clone(), sig, Arc::new(adapter))
            .map_err(PyPluginError::from)?;
        registered.push(qname.to_string());
    }
    Ok(registered)
}

fn type_name_to_datatype(name: &str) -> Result<DataType, PyPluginError> {
    type_name_to_datatype_shared(name).ok_or_else(|| {
        let normalized = name.trim().to_ascii_lowercase();
        PyPluginError::ManifestInvalid(format!("unknown yield/arg type `{normalized}`"))
    })
}

fn type_name_to_argtype(name: &str) -> Result<ArgType, PyPluginError> {
    let dt = type_name_to_datatype_shared(name).ok_or_else(|| {
        let normalized = name.trim().to_ascii_lowercase();
        PyPluginError::ManifestInvalid(format!(
            "unknown argument/return type `{normalized}` — v1 covers float/int/string/bool"
        ))
    })?;
    Ok(ArgType::Primitive(dt))
}

fn derive_declared_capabilities(m: &PyManifest) -> CapabilitySet {
    let mut set = CapabilitySet::new();
    if !m.scalar_fns.is_empty() {
        set.insert(Capability::ScalarFn);
    }
    if !m.aggregate_fns.is_empty() {
        set.insert(Capability::AggregateFn);
    }
    if !m.procedures.is_empty() {
        set.insert(Capability::Procedure);
    }
    if !m.algorithms.is_empty() {
        // A GraphCompute algorithm needs three orthogonal grants (proposal
        // §4.6): `Algorithm` to register, `GraphCompute` for the kernels, and
        // `HostQuery` to project. Each is intersected with the host's grants.
        set.insert(Capability::Algorithm);
        set.insert(Capability::GraphCompute);
        set.insert(Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        });
    }
    set
}

fn register_algorithms(
    registrar: &mut PluginRegistrar<'_>,
    runtime: Arc<PyPluginRuntime>,
    plugin_id: &PluginId,
    entries: &[crate::manifest::PyAlgorithmEntry],
) -> Result<Vec<String>, PyPluginError> {
    use arrow_schema::Field;
    use uni_plugin::traits::algorithm::AlgorithmSignature;

    let mut registered = Vec::with_capacity(entries.len());
    for entry in entries {
        // Yields are declared `"name:type"` (e.g. `"score:float"`) so the
        // emitted column and the special `nodeId` column bind by name.
        let output_fields: Vec<Field> = entry
            .yields
            .iter()
            .enumerate()
            .map(|(i, spec)| {
                let (name, type_name) = match spec.split_once(':') {
                    Some((n, t)) => (n.trim().to_string(), t.trim()),
                    None => (format!("col{i}"), spec.as_str()),
                };
                let dt = type_name_to_datatype(type_name)?;
                Ok(Field::new(name, dt, false))
            })
            .collect::<Result<_, PyPluginError>>()?;
        let sig = AlgorithmSignature {
            output_fields,
            docs: String::new(),
            ..Default::default()
        };

        let local_name = entry.name.clone();
        let qname = QName::new(plugin_id.as_str(), local_name.clone());
        let callable = Python::attach(|py| entry.callable.clone_ref(py));
        runtime.insert(local_name.clone(), callable);
        let adapter =
            crate::adapter_algorithm::PyAlgorithm::new(Arc::clone(&runtime), local_name, sig);
        registrar
            .algorithm(qname.clone(), Arc::new(adapter))
            .map_err(PyPluginError::from)?;
        registered.push(qname.to_string());
    }
    Ok(registered)
}

fn intersect_caps(
    declared: &CapabilitySet,
    granted: &CapabilitySet,
) -> (CapabilitySet, Vec<Capability>) {
    let effective = declared.intersect(granted);
    let denied: Vec<Capability> = declared
        .iter()
        .filter(|c| !granted.contains(c))
        .cloned()
        .collect();
    (effective, denied)
}

/// Phase 1 helper: build a fresh Python module object, set its
/// `_uni_decorator_sink` global to a `DecoratorSink` instance, and
/// return the module so the loader can exec source against it.
fn build_module_with_sink<'py>(
    py: Python<'py>,
    module_name: &str,
    builder: &Arc<ManifestBuilder>,
) -> Result<Bound<'py, pyo3::types::PyModule>, PyPluginError> {
    // Build a minimal Python module-from-code with a stub body so we
    // can get a module object whose dict we control.
    let stub_src = CString::new("# uni-plugin-pyo3 host-injected module\n")
        .map_err(|e| PyPluginError::Internal(format!("CString stub: {e}")))?;
    let module_name_c = CString::new(module_name)
        .unwrap_or_else(|_| CString::new("uni_plugin_pyo3_module").expect("static"));
    let filename_c = CString::new(format!("{module_name}.py"))
        .unwrap_or_else(|_| CString::new("uni_plugin_pyo3_module.py").expect("static"));
    let module = pyo3::types::PyModule::from_code(
        py,
        stub_src.as_c_str(),
        filename_c.as_c_str(),
        module_name_c.as_c_str(),
    )
    .map_err(PyPluginError::from)?;

    // Install the sink. The sink is a Python class instance with
    // `.scalar_fn(...)`, `.aggregate_fn(...)`, `.procedure(...)`,
    // `.set_plugin_id(...)`, `.set_version(...)`, `.set_determinism(...)`
    // methods. Each decorator method returns a decorator that captures
    // the wrapped callable into our `ManifestBuilder` (passed through
    // as a `PyDecoratorSink` pyclass holding the `Arc<ManifestBuilder>`).
    let sink = Py::new(py, PyDecoratorSink::from_builder(Arc::clone(builder)))
        .map_err(PyPluginError::from)?;
    module
        .setattr("_uni_decorator_sink", sink.clone_ref(py))
        .map_err(PyPluginError::from)?;
    // Also alias as `db` for the proposal §5.4 surface — users write
    // `@db.scalar_fn(...)`. The session-scoped bindings layer in
    // `bindings/uni-db` provides the same name; the source-load form
    // reuses it for symmetry.
    module.setattr("db", sink).map_err(PyPluginError::from)?;
    Ok(module)
}

/// pyclass exposing `.scalar_fn(...)` / `.aggregate_fn(...)` /
/// `.procedure(...)` / setter methods to Python. Holds an
/// `Arc<ManifestBuilder>`. Each decorator method returns a Python
/// closure (a real Python `function` built via `exec` so user code
/// can use `inspect.signature` against the underlying callable) that
/// captures the wrapped fn into the builder and returns it unchanged.
/// Python-exposed decorator sink — the object aliased as `db` /
/// `_uni_decorator_sink` in the loaded module's globals, and the
/// pyclass that backs the host bindings' `@db.scalar_fn` /
/// `@db.aggregate_fn` / `@db.procedure` surfaces (M8 followup F2).
///
/// `#[doc(hidden)]` because the wire shape is an internal ABI between
/// `uni-plugin-pyo3` and `bindings/uni-db`; user-facing stability lives
/// at the Python-side decorator syntax, not at this Rust type.
#[derive(Debug)]
#[doc(hidden)]
#[pyclass]
pub struct PyDecoratorSink {
    pub(crate) builder: Arc<ManifestBuilder>,
}

impl PyDecoratorSink {
    /// Build a new sink backed by `builder`. Public so the bindings
    /// layer can construct one without going through the source-load
    /// path.
    #[doc(hidden)]
    #[must_use]
    pub fn from_builder(builder: Arc<ManifestBuilder>) -> Self {
        Self { builder }
    }
}

#[pymethods]
impl PyDecoratorSink {
    /// `@db.scalar_fn(name, args=[...], returns=..., vectorized=False, determinism='inherit')`
    ///
    /// The default `"inherit"` takes the manifest-wide determinism set via
    /// `db.set_determinism(...)` (itself defaulting to `"pure"`); pass an explicit
    /// spelling here to override it per entry.
    #[pyo3(signature = (name, args, returns, vectorized=false, determinism="inherit"))]
    fn scalar_fn(
        &self,
        py: Python<'_>,
        name: String,
        args: Bound<'_, PyAny>,
        returns: String,
        vectorized: bool,
        determinism: &str,
    ) -> PyResult<Py<PyAny>> {
        make_scalar_trampoline(
            py,
            Arc::clone(&self.builder),
            name,
            args,
            returns,
            vectorized,
            determinism,
        )
    }

    /// `@db.aggregate_fn(name, args=[...], returns=..., determinism='pure')`
    /// — the wrapped object MUST be a `dict` with `init`/`accumulate`/
    /// `merge`/`finalize` keys, or a class with those attributes. The
    /// trampoline validates on call.
    #[pyo3(signature = (name, args, returns, determinism="inherit"))]
    fn aggregate_fn(
        &self,
        py: Python<'_>,
        name: String,
        args: Bound<'_, PyAny>,
        returns: String,
        determinism: &str,
    ) -> PyResult<Py<PyAny>> {
        make_aggregate_trampoline(
            py,
            Arc::clone(&self.builder),
            name,
            args,
            returns,
            determinism,
        )
    }

    /// `@db.procedure(name, args=[...], yields=[...], mode='read')`
    #[pyo3(signature = (name, args, yields, mode="read"))]
    fn procedure(
        &self,
        py: Python<'_>,
        name: String,
        args: Bound<'_, PyAny>,
        yields: Bound<'_, PyAny>,
        mode: &str,
    ) -> PyResult<Py<PyAny>> {
        make_procedure_trampoline(py, Arc::clone(&self.builder), name, args, yields, mode)
    }

    /// `@db.algorithm(name, args=[...], yields=[...])` — a GraphCompute algorithm
    /// whose function receives an injected `GcSession` as its first argument.
    #[pyo3(signature = (name, args, yields))]
    fn algorithm(
        &self,
        py: Python<'_>,
        name: String,
        args: Bound<'_, PyAny>,
        yields: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        make_algorithm_trampoline(py, Arc::clone(&self.builder), name, args, yields)
    }

    /// `db.set_plugin_id("ai.example.geo")` — overrides the default
    /// id resolved by the loader.
    fn set_plugin_id(&self, id: String) {
        self.builder.set_id(id);
    }

    /// `db.set_version("0.1.0")`
    fn set_version(&self, version: String) {
        self.builder.set_version(version);
    }

    /// `db.set_determinism("pure")` — sets a manifest-wide default
    /// (per-entry decorators can still override).
    fn set_determinism(&self, determinism: String) {
        self.builder.set_determinism(determinism);
    }
}

/// Build a scalar-fn decorator trampoline that, when applied to a
/// user function (`@db.scalar_fn(...) def f(...): ...`), pushes a
/// `PyScalarEntry` into `builder` and returns the user function
/// unchanged.
///
/// Public so the `bindings/uni-db` `Database` / `Session` pyclasses
/// can expose the same `@db.scalar_fn` decorator surface without
/// going through the `_uni_decorator_sink` source-load path.
///
/// # Errors
///
/// Returns a `PyErr` if `args` cannot be coerced to a list of strings.
#[doc(hidden)]
pub fn make_scalar_trampoline(
    py: Python<'_>,
    builder: Arc<ManifestBuilder>,
    name: String,
    args: Bound<'_, PyAny>,
    returns: String,
    vectorized: bool,
    determinism: &str,
) -> PyResult<Py<PyAny>> {
    let args_vec = extract_args_list(&args)?;
    let trampoline = PyDecoratorTrampoline::new_scalar(
        builder,
        SmolStr::new(&name),
        args_vec,
        SmolStr::new(&returns),
        vectorized,
        SmolStr::new(determinism),
    );
    Ok(Py::new(py, trampoline)?.into_any())
}

/// Build an aggregate-fn decorator trampoline. See
/// [`make_scalar_trampoline`] for the contract.
///
/// # Errors
///
/// Returns a `PyErr` if `args` cannot be coerced to a list of strings.
#[doc(hidden)]
pub fn make_aggregate_trampoline(
    py: Python<'_>,
    builder: Arc<ManifestBuilder>,
    name: String,
    args: Bound<'_, PyAny>,
    returns: String,
    determinism: &str,
) -> PyResult<Py<PyAny>> {
    let args_vec = extract_args_list(&args)?;
    let trampoline = PyDecoratorTrampoline::new_aggregate(
        builder,
        SmolStr::new(&name),
        args_vec,
        SmolStr::new(&returns),
        SmolStr::new(determinism),
    );
    Ok(Py::new(py, trampoline)?.into_any())
}

/// Build a procedure decorator trampoline. See
/// [`make_scalar_trampoline`] for the contract.
///
/// # Errors
///
/// Returns a `PyErr` if `args` / `yields` cannot be coerced to lists.
#[doc(hidden)]
pub fn make_procedure_trampoline(
    py: Python<'_>,
    builder: Arc<ManifestBuilder>,
    name: String,
    args: Bound<'_, PyAny>,
    yields: Bound<'_, PyAny>,
    mode: &str,
) -> PyResult<Py<PyAny>> {
    let args_vec = extract_args_list(&args)?;
    let yields_vec = extract_args_list(&yields)?;
    let trampoline = PyDecoratorTrampoline::new_procedure(
        builder,
        SmolStr::new(&name),
        args_vec,
        yields_vec,
        SmolStr::new(mode),
    );
    Ok(Py::new(py, trampoline)?.into_any())
}

/// Build an algorithm-fn decorator trampoline (mirrors the procedure one).
///
/// # Errors
/// Returns a `PyErr` if `args`/`yields` cannot be coerced to lists of strings.
#[doc(hidden)]
pub fn make_algorithm_trampoline(
    py: Python<'_>,
    builder: Arc<ManifestBuilder>,
    name: String,
    args: Bound<'_, PyAny>,
    yields: Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let args_vec = extract_args_list(&args)?;
    let yields_vec = extract_args_list(&yields)?;
    let trampoline =
        PyDecoratorTrampoline::new_algorithm(builder, SmolStr::new(&name), args_vec, yields_vec);
    Ok(Py::new(py, trampoline)?.into_any())
}

fn extract_args_list(obj: &Bound<'_, PyAny>) -> PyResult<Vec<SmolStr>> {
    // Accept any iterable of strings (list / tuple / generator / ...).
    let mut out = Vec::new();
    for item in obj.try_iter()? {
        out.push(SmolStr::new(item?.extract::<String>()?));
    }
    Ok(out)
}

/// The decorator object returned by `@db.scalar_fn(...)`. Implements
/// `__call__` to capture the wrapped Python fn into the builder and
/// return it unchanged.
///
/// `#[doc(hidden)]` for the same reason as `PyDecoratorSink`.
#[derive(Debug)]
#[doc(hidden)]
#[pyclass]
pub struct PyDecoratorTrampoline {
    kind: TrampolineKind,
    builder: Arc<ManifestBuilder>,
    name: SmolStr,
    args: Vec<SmolStr>,
    returns: SmolStr,
    yields: Vec<SmolStr>,
    mode: SmolStr,
    vectorized: bool,
    determinism: SmolStr,
}

#[derive(Debug, Clone, Copy)]
enum TrampolineKind {
    Scalar,
    Aggregate,
    Procedure,
    Algorithm,
}

impl PyDecoratorTrampoline {
    fn new_scalar(
        builder: Arc<ManifestBuilder>,
        name: SmolStr,
        args: Vec<SmolStr>,
        returns: SmolStr,
        vectorized: bool,
        determinism: SmolStr,
    ) -> Self {
        Self {
            kind: TrampolineKind::Scalar,
            builder,
            name,
            args,
            returns,
            yields: Vec::new(),
            mode: SmolStr::default(),
            vectorized,
            determinism,
        }
    }

    fn new_aggregate(
        builder: Arc<ManifestBuilder>,
        name: SmolStr,
        args: Vec<SmolStr>,
        returns: SmolStr,
        determinism: SmolStr,
    ) -> Self {
        Self {
            kind: TrampolineKind::Aggregate,
            builder,
            name,
            args,
            returns,
            yields: Vec::new(),
            mode: SmolStr::default(),
            vectorized: false,
            determinism,
        }
    }

    fn new_procedure(
        builder: Arc<ManifestBuilder>,
        name: SmolStr,
        args: Vec<SmolStr>,
        yields: Vec<SmolStr>,
        mode: SmolStr,
    ) -> Self {
        Self {
            kind: TrampolineKind::Procedure,
            builder,
            name,
            args,
            yields,
            returns: SmolStr::default(),
            mode,
            vectorized: false,
            determinism: SmolStr::default(),
        }
    }

    fn new_algorithm(
        builder: Arc<ManifestBuilder>,
        name: SmolStr,
        args: Vec<SmolStr>,
        yields: Vec<SmolStr>,
    ) -> Self {
        Self {
            kind: TrampolineKind::Algorithm,
            builder,
            name,
            args,
            yields,
            returns: SmolStr::default(),
            mode: SmolStr::default(),
            vectorized: false,
            determinism: SmolStr::default(),
        }
    }
}

#[pymethods]
impl PyDecoratorTrampoline {
    fn __call__(&self, py: Python<'_>, target: Py<PyAny>) -> PyResult<Py<PyAny>> {
        match self.kind {
            TrampolineKind::Scalar => {
                let entry = PyScalarEntry {
                    name: self.name.clone(),
                    args: self.args.clone(),
                    returns: self.returns.clone(),
                    vectorized: self.vectorized,
                    determinism: self.determinism.clone(),
                    callable: target.clone_ref(py),
                };
                self.builder.push_scalar(entry);
            }
            TrampolineKind::Aggregate => {
                // The wrapped target is either a dict with init/accumulate/merge/finalize
                // keys OR a class exposing those as methods/attributes.
                let bound = target.bind(py);
                let (init, accumulate, merge, finalize) = extract_agg_methods(bound)?;
                let entry = crate::manifest::PyAggregateEntry {
                    name: self.name.clone(),
                    args: self.args.clone(),
                    returns: self.returns.clone(),
                    determinism: self.determinism.clone(),
                    init,
                    accumulate,
                    merge,
                    finalize,
                };
                self.builder.push_aggregate(entry);
            }
            TrampolineKind::Procedure => {
                let entry = crate::manifest::PyProcedureEntry {
                    name: self.name.clone(),
                    args: self.args.clone(),
                    yields: self.yields.clone(),
                    mode: self.mode.clone(),
                    callable: target.clone_ref(py),
                };
                self.builder.push_procedure(entry);
            }
            TrampolineKind::Algorithm => {
                let entry = crate::manifest::PyAlgorithmEntry {
                    name: self.name.clone(),
                    args: self.args.clone(),
                    yields: self.yields.clone(),
                    callable: target.clone_ref(py),
                };
                self.builder.push_algorithm(entry);
            }
        }
        // Decorators must return the wrapped target unchanged so user
        // code can continue to reference it under its original name.
        Ok(target)
    }
}

/// The four Python callables comprising an aggregate spec: `init`,
/// `accumulate`, `merge`, `finalize`.
type AggCallables = (Py<PyAny>, Py<PyAny>, Py<PyAny>, Py<PyAny>);

fn extract_agg_methods(obj: &Bound<'_, PyAny>) -> PyResult<AggCallables> {
    const KEYS: [&str; 4] = ["init", "accumulate", "merge", "finalize"];

    // If `obj` is a dict, read keys; else read attributes.
    let resolved: Vec<Py<PyAny>> = if let Ok(dict) = obj.cast::<PyDict>() {
        KEYS.iter()
            .map(|key| {
                dict.get_item(key)?
                    .ok_or_else(|| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "aggregate spec dict missing `{key}` key"
                        ))
                    })
                    .map(|v| v.unbind())
            })
            .collect::<PyResult<_>>()?
    } else {
        KEYS.iter()
            .map(|key| obj.getattr(*key).map(|v| v.unbind()))
            .collect::<PyResult<_>>()?
    };

    let [init, accumulate, merge, finalize] =
        resolved.try_into().expect("KEYS has exactly four entries");
    Ok((init, accumulate, merge, finalize))
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_plugin::PluginRegistry;

    fn loader_with_caps() -> (PyPluginLoader, CapabilitySet) {
        let loader = PyPluginLoader::with_default_plugin_id("ai.test.pyloader");
        let caps = CapabilitySet::from_iter_of([
            Capability::ScalarFn,
            Capability::AggregateFn,
            Capability::Procedure,
        ]);
        (loader, caps)
    }

    #[test]
    fn loader_registers_decorated_scalar() {
        Python::initialize();
        Python::attach(|py| {
            let (loader, caps) = loader_with_caps();
            let registry = PluginRegistry::new();
            let mut r =
                PluginRegistrar::new(PluginId::new("ai.test.placeholder"), &caps, &registry);
            let src = r#"
@db.scalar_fn("double", args=["float"], returns="float", determinism="pure")
def double(x):
    return x * 2.0
"#;
            let outcome = loader
                .load(py, src, "ai.test.pyloader", &mut r, &caps)
                .expect("load");
            assert_eq!(outcome.scalars_registered.len(), 1);
            assert!(outcome.denied_capabilities.is_empty());
            r.commit_to_registry().expect("commit");
            let q = QName::new("ai.test.pyloader", "double");
            assert!(registry.scalar_fn(&q).is_some());
        });
    }

    #[test]
    fn loader_denies_ungranted_caps() {
        Python::initialize();
        Python::attach(|py| {
            let loader = PyPluginLoader::with_default_plugin_id("ai.test.deny");
            // Grant only AggregateFn; module declares a scalar — should be denied.
            let caps = CapabilitySet::from_iter_of([Capability::AggregateFn]);
            let registry = PluginRegistry::new();
            let mut r =
                PluginRegistrar::new(PluginId::new("ai.test.placeholder"), &caps, &registry);
            let src = r#"
@db.scalar_fn("plus1", args=["float"], returns="float")
def plus1(x):
    return x + 1.0
"#;
            let outcome = loader
                .load(py, src, "ai.test.deny", &mut r, &caps)
                .expect("load");
            assert!(outcome.scalars_registered.is_empty());
            assert!(outcome.denied_capabilities.contains(&Capability::ScalarFn));
        });
    }

    #[test]
    fn loader_empty_module_errors() {
        Python::initialize();
        Python::attach(|py| {
            let (loader, caps) = loader_with_caps();
            let registry = PluginRegistry::new();
            let mut r =
                PluginRegistrar::new(PluginId::new("ai.test.placeholder"), &caps, &registry);
            let src = "x = 1 + 1\n";
            let err = loader
                .load(py, src, "ai.test.empty", &mut r, &caps)
                .unwrap_err();
            assert!(matches!(err, PyPluginError::ManifestInvalid(_)));
        });
    }

    #[test]
    fn loader_parse_error_surfaces() {
        Python::initialize();
        Python::attach(|py| {
            let (loader, caps) = loader_with_caps();
            let registry = PluginRegistry::new();
            let mut r =
                PluginRegistrar::new(PluginId::new("ai.test.placeholder"), &caps, &registry);
            let src = "this is @@@ not valid python\n";
            let err = loader
                .load(py, src, "ai.test.bad", &mut r, &caps)
                .unwrap_err();
            // Surfaces as a Python exception (SyntaxError).
            match err {
                PyPluginError::PythonException { message, .. } => {
                    assert!(message.contains("SyntaxError"), "got: {message}");
                }
                other => panic!("unexpected: {other:?}"),
            }
        });
    }

    #[test]
    fn loader_unknown_type_name_rejected() {
        Python::initialize();
        Python::attach(|py| {
            let (loader, caps) = loader_with_caps();
            let registry = PluginRegistry::new();
            let mut r =
                PluginRegistrar::new(PluginId::new("ai.test.placeholder"), &caps, &registry);
            let src = r#"
@db.scalar_fn("oops", args=["quaternion"], returns="float")
def oops(x):
    return x
"#;
            let err = loader
                .load(py, src, "ai.test.types", &mut r, &caps)
                .unwrap_err();
            assert!(matches!(err, PyPluginError::ManifestInvalid(_)));
        });
    }

    #[test]
    fn loader_set_plugin_id_overrides_default() {
        Python::initialize();
        Python::attach(|py| {
            let (loader, caps) = loader_with_caps();
            let registry = PluginRegistry::new();
            let mut r =
                PluginRegistrar::new(PluginId::new("ai.test.placeholder"), &caps, &registry);
            let src = r#"
db.set_plugin_id("ai.example.geo")
db.set_version("0.3.1")

@db.scalar_fn("haversine", args=["float","float","float","float"], returns="float", vectorized=True, determinism="pure")
def haversine(lat1, lon1, lat2, lon2):
    import pyarrow as pa
    return pa.array([0.0] * len(lat1))
"#;
            let outcome = loader
                .load(py, src, "ai.test.pyloader", &mut r, &caps)
                .expect("load");
            assert_eq!(outcome.plugin_id.as_str(), "ai.example.geo");
            assert_eq!(outcome.version, "0.3.1");
            r.commit_to_registry().expect("commit");
            let q = QName::new("ai.example.geo", "haversine");
            assert!(registry.scalar_fn(&q).is_some());
        });
    }
}
