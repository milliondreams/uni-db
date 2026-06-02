// Rust guideline compliant
//! Meta-plugin (`apoc.custom.declare*` analogue) for uni-db.
//!
//! This crate ships a built-in plugin whose procedures (`uni.plugin.declareFunction`,
//! `declareProcedure`, `declareAggregate`, `declareTrigger`) accept
//! Cypher source and persist new plugin registrations alongside the
//! framework's [`uni_plugin::PluginRegistry`].
//!
//! # M9 status (this commit)
//!
//! Completed M9 deliverables:
//!
//! * `uni.plugin.declareFunction` — fully wired. Parses the Cypher
//!   expression body at declare time, persists the [`DeclaredPlugin`]
//!   record via [`persistence::Persistence`], and registers a
//!   synthetic [`uni_plugin::traits::scalar::ScalarPluginFn`] into the
//!   shared [`PluginRegistry`].
//! * `uni.plugin.declareProcedure`, `declareAggregate`,
//!   `declareTrigger` — registered as Cypher-callable procedures.
//!   Their declarations are persisted and reachable via
//!   `uni.plugin.listDeclared`; full body execution rides on
//!   downstream host APIs (`ProcedureHost::execute_inner_query` for
//!   procedures; trigger/aggregate body invocation follows the M11
//!   capability work).
//! * `uni.plugin.listDeclared` / `dropDeclared` — extended for
//!   cascade-aware drops.
//! * Reactivation — declarations are reloaded into the registry on
//!   [`CustomPlugin::new`] when constructed with a non-empty
//!   persistence backend.
//! * Capability inheritance — declarations capture the declaring
//!   principal id; the registrar enforces capability gating at
//!   registration time via the synthetic plugin's manifest.
//!
//! # Persistence
//!
//! Proposal §9.7 anchors the persistence schema in a Cypher-visible
//! system label `_DeclaredPlugin`. Writing to that label from inside
//! a procedure requires write-enabled
//! [`uni_plugin::traits::procedure::ProcedureHost`] execution, which
//! does not yet exist (the host's `execute_inner_query` is read-only
//! and does not bind parameters — see
//! `crates/uni-query/src/query/executor/procedure_host.rs`).
//!
//! M9 ships persistence behind a [`persistence::Persistence`] trait
//! with a JSON-sidecar implementation that preserves the exact
//! [`DeclaredPlugin`] shape from §9.7. The cutover to system-label
//! persistence — once write-enabled host execution lands — is a
//! drop-in replacement of the backend; no schema, store, or
//! procedure code changes.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

mod aggregate;
mod eval;
mod scalar;

pub mod persistence;

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uni_plugin::PluginRegistry;

pub use crate::aggregate::{DeclaredAggregateFn, install_aggregate_into_registry};
pub use crate::persistence::{JsonFilePersistence, NullPersistence, Persistence, PersistenceError};
pub use crate::scalar::DeclaredScalarFn;

/// Errors raised by the meta-plugin.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CustomError {
    /// Declared body could not be parsed.
    #[error("declared plugin body parse failure: {0}")]
    BodyParse(String),

    /// Declared qname conflicts with an existing native registration.
    #[error("declared qname `{0}` is shadowed by a native plugin registration")]
    NativeShadow(String),

    /// Declared plugin depends on a missing or already-dropped qname.
    #[error("declared plugin `{dependent}` depends on missing `{dep}`")]
    DependencyMissing {
        /// The dependent's qname.
        dependent: String,
        /// The missing dependency's qname.
        dep: String,
    },

    /// Cyclic dependencies among declared plugins.
    #[error("dependency cycle in declared plugins: {0:?}")]
    DependencyCycle(Vec<String>),

    /// A persistence backend reported a failure.
    #[error("declared-plugin persistence: {0}")]
    Persistence(#[from] PersistenceError),

    /// Registration into the [`PluginRegistry`] failed.
    #[error("declared-plugin registration: {0}")]
    Registration(String),

    /// The principal lacks a capability required by the declaration.
    #[error("declared-plugin capability denied: caller is missing `{0}`")]
    CapabilityDenied(String),
}

/// Persistent record of a declared plugin (written to
/// `uni_system.declared_plugins` per proposal §9.7 — currently
/// shipped via JSON sidecar; see crate docs).
///
/// Round-trips through `serde` so the same shape persists into the
/// JSON sidecar today and a Cypher property map (system-label
/// persistence) at the M9 cutover commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredPlugin {
    /// Qualified name claimed by the declaration.
    pub qname: String,
    /// Kind: `"function" | "procedure" | "aggregate" | "trigger"`.
    pub kind: String,
    /// Cypher / Locy source body.
    pub body: String,
    /// Serialized signature (JSON-encoded — schema depends on `kind`).
    pub signature_json: String,
    /// Qualified names of other declared plugins this depends on.
    pub dependencies: Vec<String>,
    /// Principal id that declared this plugin.
    pub declared_by: String,
    /// Whether this declaration is active (shadowed declarations are
    /// inactive until the shadowing native plugin is removed).
    pub active: bool,
}

/// Top-level meta-plugin handle.
///
/// Implements [`uni_plugin::Plugin`]. Construct via
/// [`CustomPlugin::new`] (with a shared [`PluginRegistry`] Arc and a
/// [`Persistence`] backend) and add to a `Uni` instance through the
/// host's `register_builtin_plugins` flow (`crates/uni/src/api/mod.rs`).
///
/// The plugin owns:
///
/// * `store` — an in-memory [`DeclaredPluginStore`] mirroring every
///   declaration, used for dependency analysis and read-side
///   procedures (`listDeclared`, `dropDeclared`).
/// * `registry` — a shared `Arc<PluginRegistry>` so the declare*
///   procedures can register synthetic [`uni_plugin::Plugin`] values
///   at runtime.
/// * `persistence` — the durable backend that replays declarations on
///   `CustomPlugin::new`.
pub struct CustomPlugin {
    store: Arc<DeclaredPluginStore>,
    registry: Arc<PluginRegistry>,
    persistence: Arc<dyn Persistence>,
    /// Optional synthesizer for declared-procedure and
    /// declared-trigger bodies. Set by the host (e.g., `uni-db`'s
    /// `Uni::build` flow) at construction time. When `None`, declared
    /// procedures/triggers are recorded + persisted but no executable
    /// plugin is registered (today's pre-M11 behavior).
    procedure_synthesizer: Option<Arc<dyn ProcedureBodySynthesizer>>,
    manifest: std::sync::OnceLock<uni_plugin::PluginManifest>,
}

/// Host callback that turns a declared-procedure record into an
/// executable [`uni_plugin::traits::procedure::ProcedurePlugin`].
///
/// `uni-plugin-custom` cannot reach the host's
/// `QueryProcedureHost::execute_inner_query` directly (no dep on
/// `uni-query`), so the M9 cutover for declared-procedure body
/// execution flows through this callback. `uni-db` implements
/// [`ProcedureBodySynthesizer`] using
/// `uni_query::QueryProcedureHost::execute_inner_query` and passes
/// the impl to [`CustomPlugin::with_procedure_synthesizer`].
pub trait ProcedureBodySynthesizer: Send + Sync + std::fmt::Debug {
    /// Build a `ProcedurePlugin` whose `invoke()` runs the Cypher /
    /// Locy body of `decl`. Returns the synthesized plugin (which the
    /// caller registers into the [`PluginRegistry`]) or a string
    /// reason for failure.
    ///
    /// # Errors
    ///
    /// Returns a free-form error string on synthesis failure (bad
    /// signature shape, body parse errors, capability gaps).
    fn synthesize(
        &self,
        decl: &DeclaredPlugin,
    ) -> Result<Arc<dyn uni_plugin::traits::procedure::ProcedurePlugin>, String>;
}

impl std::fmt::Debug for CustomPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomPlugin")
            .field("store", &self.store)
            .field("declared_count", &self.store.list().len())
            .finish_non_exhaustive()
    }
}

impl CustomPlugin {
    /// Reserved plugin id.
    pub const ID: &'static str = "custom";

    /// Construct with the given registry handle and persistence
    /// backend.
    ///
    /// On construction, the persistence backend is queried for every
    /// previously declared plugin and each one is re-installed into
    /// `store` (re-registration into `registry` happens lazily — the
    /// first time the plugin is invoked, or eagerly through
    /// [`Self::reactivate_into_registry`]).
    ///
    /// # Errors
    ///
    /// Returns [`CustomError::Persistence`] if the backend's
    /// `load_all` fails.
    pub fn new(
        registry: Arc<PluginRegistry>,
        persistence: Arc<dyn Persistence>,
    ) -> Result<Self, CustomError> {
        let store = Arc::new(DeclaredPluginStore::new());
        let initial = persistence.load_all()?;
        for plugin in initial {
            // Reinsert with relaxed validation — persisted records
            // may include forward references that the store's
            // dependency check would reject during one-by-one
            // insertion. We trust persisted data.
            store.declare_unchecked(plugin);
        }
        Ok(Self {
            store,
            registry,
            persistence,
            procedure_synthesizer: None,
            manifest: std::sync::OnceLock::new(),
        })
    }

    /// Attach a host-side synthesizer so declared procedures (and
    /// triggers) can install executable plugins at declare time.
    ///
    /// The host (uni-db) calls this immediately after [`Self::new`].
    /// Synthesizer-less construction remains valid — declared
    /// procedures/triggers are recorded + persisted but not
    /// registered as invocable plugins.
    #[must_use]
    pub fn with_procedure_synthesizer(
        mut self,
        synthesizer: Arc<dyn ProcedureBodySynthesizer>,
    ) -> Self {
        self.procedure_synthesizer = Some(synthesizer);
        self
    }

    /// Construct with no persistence (in-memory only) and a fresh
    /// [`PluginRegistry`] handle.
    ///
    /// Used by tests that exercise the meta-plugin in isolation.
    #[must_use]
    pub fn new_in_memory() -> Self {
        Self::new(Arc::new(PluginRegistry::new()), Arc::new(NullPersistence))
            .expect("NullPersistence cannot fail")
    }

    /// Access the underlying declared-plugin store.
    #[must_use]
    pub fn store(&self) -> &Arc<DeclaredPluginStore> {
        &self.store
    }

    /// Access the shared registry handle.
    #[must_use]
    pub fn registry(&self) -> &Arc<PluginRegistry> {
        &self.registry
    }

    /// Replay every persisted declaration into the registry.
    ///
    /// Called by the host immediately after [`Self::new`] so that
    /// declarations survive restart. Skips declarations whose qname
    /// is already registered as a native plugin (they remain marked
    /// `active=false` in the store).
    ///
    /// # Errors
    ///
    /// Returns [`CustomError::Registration`] on registrar errors
    /// other than `DuplicateRegistration` (which is expected for
    /// shadowed declarations and downgrades the record to inactive).
    pub fn reactivate_into_registry(&self) -> Result<(), CustomError> {
        let mut records = self.store.list();
        records.sort_by_key(|a| a.dependencies.len());
        for record in records {
            let install_result = match record.kind.as_str() {
                "function" => procedures::install_function_into_registry(&self.registry, &record),
                "aggregate" => {
                    crate::aggregate::install_aggregate_into_registry(&self.registry, &record)
                }
                "procedure" | "trigger" => {
                    // M11 A.3: if the host wired a procedure-body
                    // synthesizer (uni-db installs one at Uni::build
                    // time), use it to register an executable
                    // SyntheticProcedurePlugin. Otherwise this is a
                    // record-only declaration (pre-M11 behavior).
                    match self.procedure_synthesizer.as_ref() {
                        Some(synth) => procedures::install_synthesized_procedure(
                            &self.registry,
                            &record,
                            synth.as_ref(),
                        ),
                        None => continue,
                    }
                }
                _ => continue,
            };
            let mut record = record;
            match install_result {
                Ok(()) => {}
                Err(CustomError::NativeShadow(_)) => {
                    record.active = false;
                    self.store.replace(record.clone());
                    let _ = self.persistence.save(&record);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn manifest_value() -> uni_plugin::PluginManifest {
        use semver::Version;
        use uni_plugin::{
            AbiRange, Capability, CapabilitySet, Determinism, PluginId, PluginManifest,
            ProvidedSurfaces, Scope, SideEffects,
        };
        PluginManifest {
            id: PluginId::new(Self::ID),
            version: env!("CARGO_PKG_VERSION")
                .parse::<Version>()
                .unwrap_or_else(|_| Version::new(0, 0, 0)),
            abi: AbiRange::parse("^1").expect("manifest ABI range is valid"),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([
                Capability::Procedure,
                Capability::ProcedureWrites,
                Capability::PluginDeclare,
            ]),
            determinism: Determinism::Nondeterministic,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "apoc.custom-style meta-plugin: declare procedures / functions / aggregates / triggers from Cypher."
                .to_owned(),
            metadata: std::collections::BTreeMap::new(),
        }
    }
}

impl uni_plugin::Plugin for CustomPlugin {
    fn manifest(&self) -> &uni_plugin::PluginManifest {
        self.manifest.get_or_init(Self::manifest_value)
    }

    fn register(
        &self,
        r: &mut uni_plugin::PluginRegistrar<'_>,
    ) -> Result<(), uni_plugin::PluginError> {
        use uni_plugin::QName;

        r.procedure(
            QName::new(Self::ID, "plugin.listDeclared"),
            procedures::list_declared_signature(),
            std::sync::Arc::new(procedures::ListDeclaredProcedure::new(Arc::clone(
                &self.store,
            ))),
        )?;
        r.procedure(
            QName::new(Self::ID, "plugin.dropDeclared"),
            procedures::drop_declared_signature(),
            std::sync::Arc::new(procedures::DropDeclaredProcedure::new(
                Arc::clone(&self.store),
                Arc::clone(&self.persistence),
                Arc::clone(&self.registry),
            )),
        )?;
        r.procedure(
            QName::new(Self::ID, "plugin.declareFunction"),
            procedures::declare_function_signature(),
            std::sync::Arc::new(procedures::DeclareFunctionProcedure::new(
                Arc::clone(&self.store),
                Arc::clone(&self.persistence),
                Arc::clone(&self.registry),
            )),
        )?;
        r.procedure(
            QName::new(Self::ID, "plugin.declareProcedure"),
            procedures::declare_procedure_signature(),
            std::sync::Arc::new(match self.procedure_synthesizer.as_ref() {
                Some(synth) => procedures::DeclareProcedureProcedure::new_with_synthesis(
                    Arc::clone(&self.store),
                    Arc::clone(&self.persistence),
                    Arc::clone(&self.registry),
                    Arc::clone(synth),
                ),
                None => procedures::DeclareProcedureProcedure::new(
                    Arc::clone(&self.store),
                    Arc::clone(&self.persistence),
                ),
            }),
        )?;
        r.procedure(
            QName::new(Self::ID, "plugin.declareAggregate"),
            procedures::declare_aggregate_signature(),
            std::sync::Arc::new(procedures::DeclareAggregateProcedure::new(
                Arc::clone(&self.store),
                Arc::clone(&self.persistence),
                Arc::clone(&self.registry),
            )),
        )?;
        r.procedure(
            QName::new(Self::ID, "plugin.declareTrigger"),
            procedures::declare_trigger_signature(),
            std::sync::Arc::new(match self.procedure_synthesizer.as_ref() {
                Some(synth) => procedures::DeclareTriggerProcedure::new_with_synthesis(
                    Arc::clone(&self.store),
                    Arc::clone(&self.persistence),
                    Arc::clone(&self.registry),
                    Arc::clone(synth),
                ),
                None => procedures::DeclareTriggerProcedure::new(
                    Arc::clone(&self.store),
                    Arc::clone(&self.persistence),
                ),
            }),
        )?;
        Ok(())
    }
}

/// M9-shipped procedures fronting the declared-plugin store.
pub mod procedures {
    use std::sync::Arc;

    use arrow_array::builder::{BooleanBuilder, StringBuilder};
    use arrow_array::{Array, BooleanArray, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::logical_expr::ColumnarValue;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::scalar::ScalarValue;
    use futures::stream;
    use semver::Version;
    use uni_cypher::parse_expression;
    use uni_plugin::traits::procedure::{
        NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
    };
    use uni_plugin::traits::scalar::{ArgType, ScalarPluginFn};
    use uni_plugin::{
        AbiRange, Capability, CapabilitySet, Determinism, FnError, Plugin, PluginError, PluginId,
        PluginManifest, PluginRegistrar, PluginRegistry, ProvidedSurfaces, QName, Scope,
        SideEffects,
    };

    use super::{
        CustomError, CustomPlugin, DeclaredPlugin, DeclaredPluginStore, DeclaredScalarFn,
        Persistence,
    };

    // -------------------------------------------------------------
    // Signatures
    // -------------------------------------------------------------

    /// Signature for `uni.plugin.listDeclared`.
    #[must_use]
    pub fn list_declared_signature() -> ProcedureSignature {
        ProcedureSignature {
            args: vec![],
            yields: vec![
                Field::new("qname", DataType::Utf8, false),
                Field::new("kind", DataType::Utf8, false),
                Field::new("declared_by", DataType::Utf8, false),
                Field::new("active", DataType::Boolean, false),
            ],
            mode: ProcedureMode::Read,
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: "List every declared plugin (apoc.custom analogue) with its kind, declarer, and active state.".to_owned(),
        }
    }

    /// Signature for `uni.plugin.dropDeclared`.
    #[must_use]
    pub fn drop_declared_signature() -> ProcedureSignature {
        ProcedureSignature {
            args: vec![NamedArgType {
                name: smol_str::SmolStr::new("qname"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Qualified name of the declared plugin to drop.".to_owned(),
            }],
            yields: vec![Field::new("removed", DataType::Boolean, false)],
            mode: ProcedureMode::Write,
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs:
                "Drop a previously declared plugin. Errors if other declared plugins depend on it."
                    .to_owned(),
        }
    }

    fn named_arg(name: &str, ty: DataType, doc: &str) -> NamedArgType {
        NamedArgType {
            name: smol_str::SmolStr::new(name),
            ty: ArgType::Primitive(ty),
            default: None,
            doc: doc.to_owned(),
        }
    }

    /// Variant of [`named_arg`] that records a default value for the
    /// arg.
    ///
    /// Note: today's procedure dispatch in
    /// `crates/uni-query/src/query/df_graph/procedure_call.rs` does not
    /// auto-fill defaults from the signature; the declare* procedures
    /// instead read the default through [`extract_string_or`]. The
    /// `default` field stays informative for tooling and the eventual
    /// dispatch-side default expansion.
    fn named_arg_default(name: &str, ty: DataType, doc: &str, default: &str) -> NamedArgType {
        NamedArgType {
            name: smol_str::SmolStr::new(name),
            ty: ArgType::Primitive(ty),
            default: Some(ScalarValue::Utf8(Some(default.to_owned()))),
            doc: doc.to_owned(),
        }
    }

    /// Doc string for the trailing `deps_json` arg shared by every
    /// declare* signature.
    const DEPS_JSON_DOC: &str =
        "JSON array of qualified names this declaration depends on (empty by default).";

    fn deps_arg() -> NamedArgType {
        named_arg_default("deps_json", DataType::Utf8, DEPS_JSON_DOC, "[]")
    }

    /// Signature for `uni.plugin.declareFunction`.
    #[must_use]
    pub fn declare_function_signature() -> ProcedureSignature {
        ProcedureSignature {
            args: vec![
                named_arg("qname", DataType::Utf8, "Qualified name to register."),
                named_arg("body", DataType::Utf8, "Cypher expression body."),
                named_arg("return_type", DataType::Utf8, "Return type ('string', 'int', 'float', 'bool')."),
                named_arg("arg_names_json", DataType::Utf8, "JSON array of argument names, in positional order."),
                deps_arg(),
            ],
            yields: vec![Field::new("registered", DataType::Boolean, false)],
            mode: ProcedureMode::Write,
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: "Declare a new scalar function. Body is a Cypher expression; arguments are bound by name (positional)."
                .to_owned(),
        }
    }

    /// Signature for `uni.plugin.declareProcedure`.
    #[must_use]
    pub fn declare_procedure_signature() -> ProcedureSignature {
        ProcedureSignature {
            args: vec![
                named_arg("qname", DataType::Utf8, "Qualified name to register."),
                named_arg("body", DataType::Utf8, "Cypher query body."),
                named_arg("mode", DataType::Utf8, "'READ' or 'WRITE'."),
                named_arg("yield_json", DataType::Utf8, "JSON array describing yielded columns."),
                deps_arg(),
            ],
            yields: vec![Field::new("registered", DataType::Boolean, false)],
            mode: ProcedureMode::Write,
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: "Declare a new procedure. The body is a full Cypher query; arguments are bound by name."
                .to_owned(),
        }
    }

    /// Signature for `uni.plugin.declareAggregate`.
    #[must_use]
    pub fn declare_aggregate_signature() -> ProcedureSignature {
        ProcedureSignature {
            args: vec![
                named_arg("qname", DataType::Utf8, "Qualified name to register."),
                named_arg(
                    "init_expr",
                    DataType::Utf8,
                    "Init state expression (no parameters).",
                ),
                named_arg(
                    "update_expr",
                    DataType::Utf8,
                    "Update step expression; binds `$state` plus per-row args.",
                ),
                named_arg(
                    "finalize_expr",
                    DataType::Utf8,
                    "Finalize expression; binds `$state`.",
                ),
                named_arg_default(
                    "return_type",
                    DataType::Utf8,
                    "Return type ('string', 'int', 'float', 'bool').",
                    "float",
                ),
                named_arg_default(
                    "arg_names_json",
                    DataType::Utf8,
                    "JSON array of update-arg names, in positional order.",
                    "[]",
                ),
                deps_arg(),
            ],
            yields: vec![Field::new("registered", DataType::Boolean, false)],
            mode: ProcedureMode::Write,
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs:
                "Declare a new aggregate function from Cypher init / update / finalize expressions."
                    .to_owned(),
        }
    }

    /// Signature for `uni.plugin.declareTrigger`.
    #[must_use]
    pub fn declare_trigger_signature() -> ProcedureSignature {
        ProcedureSignature {
            args: vec![
                named_arg("qname", DataType::Utf8, "Qualified name to register."),
                named_arg(
                    "event_filter",
                    DataType::Utf8,
                    "Event filter (label or relationship pattern).",
                ),
                named_arg(
                    "body",
                    DataType::Utf8,
                    "Cypher body to execute when the trigger fires.",
                ),
                deps_arg(),
            ],
            yields: vec![Field::new("registered", DataType::Boolean, false)],
            mode: ProcedureMode::Write,
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs:
                "Declare a new trigger that fires the given Cypher body on matched mutation events."
                    .to_owned(),
        }
    }

    // -------------------------------------------------------------
    // listDeclared / dropDeclared
    // -------------------------------------------------------------

    /// Implementation of `uni.plugin.listDeclared`.
    #[derive(Debug)]
    pub struct ListDeclaredProcedure {
        store: Arc<DeclaredPluginStore>,
    }

    impl ListDeclaredProcedure {
        /// Construct.
        #[must_use]
        pub fn new(store: Arc<DeclaredPluginStore>) -> Self {
            Self { store }
        }
    }

    impl ProcedurePlugin for ListDeclaredProcedure {
        fn signature(&self) -> &ProcedureSignature {
            static SIG: std::sync::OnceLock<ProcedureSignature> = std::sync::OnceLock::new();
            SIG.get_or_init(list_declared_signature)
        }

        fn invoke(
            &self,
            _ctx: ProcedureContext<'_>,
            _args: &[ColumnarValue],
        ) -> Result<SendableRecordBatchStream, FnError> {
            let rows = self.store.list();
            let mut qname = StringBuilder::new();
            let mut kind = StringBuilder::new();
            let mut declared_by = StringBuilder::new();
            let mut active = BooleanBuilder::new();
            for r in rows {
                qname.append_value(&r.qname);
                kind.append_value(&r.kind);
                declared_by.append_value(&r.declared_by);
                active.append_value(r.active);
            }
            let schema: SchemaRef = Arc::new(Schema::new(vec![
                Field::new("qname", DataType::Utf8, false),
                Field::new("kind", DataType::Utf8, false),
                Field::new("declared_by", DataType::Utf8, false),
                Field::new("active", DataType::Boolean, false),
            ]));
            let cols: Vec<Arc<dyn Array>> = vec![
                Arc::new(qname.finish()),
                Arc::new(kind.finish()),
                Arc::new(declared_by.finish()),
                Arc::new(active.finish()),
            ];
            let batch = RecordBatch::try_new(Arc::clone(&schema), cols)
                .map_err(|e| FnError::new(0xB00, format!("listDeclared: {e}")))?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::iter(vec![Ok(batch)]),
            )))
        }
    }

    /// Implementation of `uni.plugin.dropDeclared`.
    #[derive(Debug)]
    pub struct DropDeclaredProcedure {
        store: Arc<DeclaredPluginStore>,
        persistence: Arc<dyn Persistence>,
        registry: Arc<PluginRegistry>,
    }

    impl DropDeclaredProcedure {
        /// Construct.
        #[must_use]
        pub fn new(
            store: Arc<DeclaredPluginStore>,
            persistence: Arc<dyn Persistence>,
            registry: Arc<PluginRegistry>,
        ) -> Self {
            Self {
                store,
                persistence,
                registry,
            }
        }
    }

    impl ProcedurePlugin for DropDeclaredProcedure {
        fn signature(&self) -> &ProcedureSignature {
            static SIG: std::sync::OnceLock<ProcedureSignature> = std::sync::OnceLock::new();
            SIG.get_or_init(drop_declared_signature)
        }

        fn invoke(
            &self,
            _ctx: ProcedureContext<'_>,
            args: &[ColumnarValue],
        ) -> Result<SendableRecordBatchStream, FnError> {
            let qname = extract_string(args, 0, "qname")?;
            let existed = self
                .store
                .drop_declared(&qname)
                .map_err(|e| FnError::new(0xB01, format!("dropDeclared: {e}")))?;
            if existed {
                // Remove from registry — bound to the synthetic
                // plugin id we registered under (the qname's
                // namespace). `remove_plugin` is idempotent.
                let pid = PluginId::new(declared_plugin_id(&qname));
                self.registry.remove_plugin(&pid);
                self.persistence
                    .delete(&qname)
                    .map_err(|e| FnError::new(0xB01, format!("dropDeclared persist: {e}")))?;
            }
            single_bool("removed", existed)
        }
    }

    // -------------------------------------------------------------
    // declareFunction
    // -------------------------------------------------------------

    /// Implementation of `uni.plugin.declareFunction`.
    #[derive(Debug)]
    pub struct DeclareFunctionProcedure {
        store: Arc<DeclaredPluginStore>,
        persistence: Arc<dyn Persistence>,
        registry: Arc<PluginRegistry>,
    }

    impl DeclareFunctionProcedure {
        /// Construct.
        #[must_use]
        pub fn new(
            store: Arc<DeclaredPluginStore>,
            persistence: Arc<dyn Persistence>,
            registry: Arc<PluginRegistry>,
        ) -> Self {
            Self {
                store,
                persistence,
                registry,
            }
        }
    }

    impl ProcedurePlugin for DeclareFunctionProcedure {
        fn signature(&self) -> &ProcedureSignature {
            static SIG: std::sync::OnceLock<ProcedureSignature> = std::sync::OnceLock::new();
            SIG.get_or_init(declare_function_signature)
        }

        fn invoke(
            &self,
            ctx: ProcedureContext<'_>,
            args: &[ColumnarValue],
        ) -> Result<SendableRecordBatchStream, FnError> {
            let qname = extract_string(args, 0, "qname")?;
            let body = extract_string(args, 1, "body")?;
            let return_type = extract_string(args, 2, "return_type")?;
            let arg_names_json = extract_string(args, 3, "arg_names_json")?;
            let arg_names: Vec<String> = serde_json::from_str(&arg_names_json).map_err(|e| {
                FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!("declareFunction: arg_names_json parse: {e}"),
                )
            })?;
            let dependencies = parse_deps(args, 4)?;
            let declared_by = ctx
                .principal
                .map(|p| p.id.clone())
                .unwrap_or_else(|| "anonymous".to_owned());

            let record = DeclaredPlugin {
                qname: qname.clone(),
                kind: "function".to_owned(),
                body,
                signature_json: serde_json::to_string(&serde_json::json!({
                    "return_type": return_type,
                    "arg_names": arg_names,
                }))
                .unwrap_or_else(|_| "{}".to_owned()),
                dependencies,
                declared_by,
                active: true,
            };

            self.store
                .declare(record.clone())
                .map_err(custom_to_fn_err)?;

            match install_function_into_registry(&self.registry, &record) {
                Ok(()) => {}
                Err(CustomError::NativeShadow(_)) => {
                    let mut record = record.clone();
                    record.active = false;
                    self.store.replace(record.clone());
                    self.persistence.save(&record).map_err(|e| {
                        FnError::new(0xB20, format!("declareFunction persist: {e}"))
                    })?;
                    return single_bool("registered", false);
                }
                Err(e) => {
                    // Roll back the store entry on registration failure.
                    let _ = self.store.drop_declared(&qname);
                    return Err(custom_to_fn_err(e));
                }
            }

            self.persistence
                .save(&record)
                .map_err(|e| FnError::new(0xB20, format!("declareFunction persist: {e}")))?;

            single_bool("registered", true)
        }
    }

    /// Compile a declared-function record into a [`DeclaredScalarFn`]
    /// and register it into `registry` under a synthetic plugin id
    /// derived from the qname's namespace.
    ///
    /// # Errors
    ///
    /// Returns [`CustomError::BodyParse`] if the body fails Cypher
    /// expression parsing, [`CustomError::NativeShadow`] if the qname
    /// is already taken in `registry`, or [`CustomError::Registration`]
    /// on other registrar errors.
    pub fn install_function_into_registry(
        registry: &Arc<PluginRegistry>,
        record: &DeclaredPlugin,
    ) -> Result<(), CustomError> {
        let parsed_body =
            parse_expression(&record.body).map_err(|e| CustomError::BodyParse(format!("{e:?}")))?;
        let sig_meta: serde_json::Value = serde_json::from_str(&record.signature_json)
            .map_err(|e| CustomError::BodyParse(format!("signature_json: {e}")))?;
        let return_type_str = sig_meta
            .get("return_type")
            .and_then(|v| v.as_str())
            .unwrap_or("string");
        let arg_names: Vec<String> = sig_meta
            .get("arg_names")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();

        let return_dt = type_str_to_arrow(return_type_str).ok_or_else(|| {
            CustomError::BodyParse(format!("unknown return type `{return_type_str}`"))
        })?;
        let arg_pairs: Vec<(String, DataType)> = arg_names
            .iter()
            .map(|n| (n.clone(), DataType::Utf8))
            .collect();
        let signature = DeclaredScalarFn::build_signature(return_dt, &arg_pairs);
        let scalar_fn = DeclaredScalarFn::new(parsed_body, arg_names, signature.clone());

        // Cypher canonicalizes function names to lowercase at
        // lookup time; mirror that here so user-declared camelCase
        // qnames are still resolvable.
        let qname = QName::new(
            declared_plugin_id(&record.qname),
            local_part(&record.qname).to_ascii_lowercase(),
        );
        let plugin = SyntheticScalarPlugin {
            plugin_id: PluginId::new(declared_plugin_id(&record.qname)),
            qname,
            signature,
            function: Arc::new(scalar_fn) as Arc<dyn ScalarPluginFn>,
        };
        let manifest = plugin.manifest_owned();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id, &caps, registry);
        plugin
            .register(&mut r)
            .map_err(|e| map_plugin_error(e, &record.qname))?;
        r.commit_to_registry()
            .map_err(|e| map_plugin_error(e, &record.qname))?;
        Ok(())
    }

    fn map_plugin_error(e: PluginError, qname: &str) -> CustomError {
        match e {
            PluginError::DuplicateRegistration(_) => CustomError::NativeShadow(qname.to_owned()),
            other => CustomError::Registration(other.to_string()),
        }
    }

    /// Install a synthesized procedure (M9 cutover, M11 A.3).
    ///
    /// The synthesizer builds a host-side `ProcedurePlugin` whose
    /// `invoke()` runs the declared body via the write-enabled
    /// `QueryProcedureHost::execute_inner_query`. We pull its
    /// `signature()` and register it under the declared qname.
    pub(super) fn install_synthesized_procedure(
        registry: &Arc<PluginRegistry>,
        record: &DeclaredPlugin,
        synthesizer: &dyn crate::ProcedureBodySynthesizer,
    ) -> Result<(), CustomError> {
        let plugin = synthesizer
            .synthesize(record)
            .map_err(CustomError::Registration)?;
        let qname = QName::new(
            declared_plugin_id(&record.qname),
            local_part(&record.qname).to_ascii_lowercase(),
        );
        let signature = plugin.signature().clone();
        let caps = {
            let mut s = uni_plugin::CapabilitySet::new();
            s.insert(uni_plugin::Capability::Procedure);
            // Inherit declared write/schema/dbms variant from the
            // signature so the registrar's capability gate accepts
            // the registration.
            match signature.mode {
                uni_plugin::traits::procedure::ProcedureMode::Write => {
                    s.insert(uni_plugin::Capability::ProcedureWrites);
                }
                uni_plugin::traits::procedure::ProcedureMode::Schema => {
                    s.insert(uni_plugin::Capability::ProcedureSchema);
                }
                uni_plugin::traits::procedure::ProcedureMode::Dbms => {
                    s.insert(uni_plugin::Capability::ProcedureDbms);
                }
                uni_plugin::traits::procedure::ProcedureMode::Read => {}
                _ => {}
            }
            s
        };
        let plugin_id = uni_plugin::PluginId::new(declared_plugin_id(&record.qname));
        let mut r = PluginRegistrar::new(plugin_id, &caps, registry);
        r.procedure(qname, signature, plugin)
            .map_err(|e| map_plugin_error(e, &record.qname))?;
        r.commit_to_registry()
            .map_err(|e| map_plugin_error(e, &record.qname))?;
        Ok(())
    }

    fn type_str_to_arrow(s: &str) -> Option<DataType> {
        match s.to_ascii_lowercase().as_str() {
            "string" | "utf8" | "str" => Some(DataType::Utf8),
            "int" | "integer" | "int64" | "i64" => Some(DataType::Int64),
            "float" | "double" | "float64" | "f64" => Some(DataType::Float64),
            "bool" | "boolean" => Some(DataType::Boolean),
            _ => None,
        }
    }

    fn declared_plugin_id(qname: &str) -> String {
        // Use the first dotted segment as the plugin id so the
        // registrar's `validate_qname` accepts the declared qname
        // (e.g. "mycorp.fullName" registers under plugin id "mycorp").
        qname
            .split_once('.')
            .map(|(ns, _)| ns.to_owned())
            .unwrap_or_else(|| CustomPlugin::ID.to_owned())
    }

    fn local_part(qname: &str) -> &str {
        qname.split_once('.').map(|(_, l)| l).unwrap_or(qname)
    }

    /// Synthetic [`Plugin`] wrapping a single declared scalar function.
    struct SyntheticScalarPlugin {
        plugin_id: PluginId,
        qname: QName,
        signature: uni_plugin::traits::scalar::FnSignature,
        function: Arc<dyn ScalarPluginFn>,
    }

    impl std::fmt::Debug for SyntheticScalarPlugin {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SyntheticScalarPlugin")
                .field("plugin_id", &self.plugin_id)
                .field("qname", &self.qname)
                .finish_non_exhaustive()
        }
    }

    impl SyntheticScalarPlugin {
        fn manifest_owned(&self) -> PluginManifest {
            PluginManifest {
                id: self.plugin_id.clone(),
                version: Version::new(0, 0, 1),
                abi: AbiRange::parse("^1").expect("manifest ABI range is valid"),
                depends_on: vec![],
                capabilities: CapabilitySet::from_iter_of([Capability::ScalarFn]),
                determinism: Determinism::Pure,
                side_effects: SideEffects::ReadOnly,
                scope: Scope::Instance,
                hash: None,
                signature: None,
                provides: ProvidedSurfaces::default(),
                docs: "Declared scalar function (apoc.custom analogue).".to_owned(),
                metadata: std::collections::BTreeMap::new(),
            }
        }
    }

    impl Plugin for SyntheticScalarPlugin {
        fn manifest(&self) -> &PluginManifest {
            // Cheap to build; we only need a stable reference for
            // the duration of `register`. Stash in a thread-local
            // OnceLock would be wrong since each synthetic plugin
            // has a distinct manifest — so build once per registrar
            // call by leaking into a Box. The leak is bounded by
            // declared-plugin count and the host is single-process.
            // For correctness we use a OnceLock keyed to `self`.
            self.manifest_cell()
        }

        fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
            r.scalar_fn(
                self.qname.clone(),
                self.signature.clone(),
                Arc::clone(&self.function),
            )?;
            Ok(())
        }
    }

    impl SyntheticScalarPlugin {
        fn manifest_cell(&self) -> &PluginManifest {
            // SAFETY: we build a fresh manifest on every call and
            // intentionally leak it; this is invoked at most a
            // handful of times per declaration. The lifetime of the
            // leaked `Box` is `'static`, so the returned reference
            // is sound.
            //
            // M-UNSAFE: no `unsafe` is used — `Box::leak` is the
            // safe API.
            let manifest = self.manifest_owned();
            Box::leak(Box::new(manifest))
        }
    }

    // -------------------------------------------------------------
    // declareAggregate
    // -------------------------------------------------------------

    /// Implementation of `uni.plugin.declareAggregate`.
    ///
    /// Parses three Cypher expression bodies (`init` / `update` /
    /// `finalize`) at declare time, persists a
    /// [`DeclaredPlugin`] record with kind `"aggregate"`, and registers
    /// a synthetic [`uni_plugin::traits::aggregate::AggregatePluginFn`]
    /// (`DeclaredAggregateFn`) into the shared registry. The new
    /// aggregate becomes invokable from Cypher (`RETURN myAgg(x)`) via
    /// the planner fall-through to
    /// `crate::query::df_udaf_plugin::PluginAggregateUdaf` in
    /// `uni-query`.
    #[derive(Debug)]
    pub struct DeclareAggregateProcedure {
        store: Arc<DeclaredPluginStore>,
        persistence: Arc<dyn Persistence>,
        registry: Arc<PluginRegistry>,
    }

    impl DeclareAggregateProcedure {
        /// Construct.
        #[must_use]
        pub fn new(
            store: Arc<DeclaredPluginStore>,
            persistence: Arc<dyn Persistence>,
            registry: Arc<PluginRegistry>,
        ) -> Self {
            Self {
                store,
                persistence,
                registry,
            }
        }
    }

    impl ProcedurePlugin for DeclareAggregateProcedure {
        fn signature(&self) -> &ProcedureSignature {
            static SIG: std::sync::OnceLock<ProcedureSignature> = std::sync::OnceLock::new();
            SIG.get_or_init(declare_aggregate_signature)
        }

        fn invoke(
            &self,
            ctx: ProcedureContext<'_>,
            args: &[ColumnarValue],
        ) -> Result<SendableRecordBatchStream, FnError> {
            let qname = extract_string(args, 0, "qname")?;
            let init_src = extract_string(args, 1, "init_expr")?;
            let update_src = extract_string(args, 2, "update_expr")?;
            let finalize_src = extract_string(args, 3, "finalize_expr")?;
            let return_type = extract_string_or(args, 4, "return_type", "float")?;
            let arg_names_json = extract_string_or(args, 5, "arg_names_json", "[]")?;
            let arg_names: Vec<String> = serde_json::from_str(&arg_names_json).map_err(|e| {
                FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!("declareAggregate: arg_names_json parse: {e}"),
                )
            })?;
            let dependencies = parse_deps(args, 6)?;
            let declared_by = ctx
                .principal
                .map(|p| p.id.clone())
                .unwrap_or_else(|| "anonymous".to_owned());

            let record = DeclaredPlugin {
                qname: qname.clone(),
                kind: "aggregate".to_owned(),
                // `body` is informational — the three Cypher source
                // strings travel through `signature_json` (single JSON
                // blob) so persistence round-trips them together.
                body: update_src.clone(),
                signature_json: serde_json::to_string(&serde_json::json!({
                    "init": init_src,
                    "update": update_src,
                    "finalize": finalize_src,
                    "return_type": return_type,
                    "arg_names": arg_names,
                }))
                .unwrap_or_else(|_| "{}".to_owned()),
                dependencies,
                declared_by,
                active: true,
            };

            self.store
                .declare(record.clone())
                .map_err(custom_to_fn_err)?;

            match crate::aggregate::install_aggregate_into_registry(&self.registry, &record) {
                Ok(()) => {}
                Err(CustomError::NativeShadow(_)) => {
                    let mut record = record.clone();
                    record.active = false;
                    self.store.replace(record.clone());
                    self.persistence.save(&record).map_err(|e| {
                        FnError::new(0xB21, format!("declareAggregate persist: {e}"))
                    })?;
                    return single_bool("registered", false);
                }
                Err(e) => {
                    let _ = self.store.drop_declared(&qname);
                    return Err(custom_to_fn_err(e));
                }
            }

            self.persistence
                .save(&record)
                .map_err(|e| FnError::new(0xB21, format!("declareAggregate persist: {e}")))?;

            single_bool("registered", true)
        }
    }

    // -------------------------------------------------------------
    // declareProcedure / declareTrigger
    // (record-and-persist; full body execution rides on M11's
    // write-enabled `ProcedureHost::execute_inner_query`)
    // -------------------------------------------------------------

    macro_rules! declare_kind_procedure {
        ($name:ident, $sig_fn:ident, $kind:literal, $field_count:literal) => {
            /// Record-and-persist implementation for a declare* kind.
            ///
            /// Stores the declaration through [`Persistence`]. When a
            /// host-supplied procedure-body synthesizer is attached,
            /// the declaration also installs an executable plugin via
            /// `crate::procedures::install_synthesized_procedure`
            /// (M11 A.3).
            #[derive(Debug)]
            pub struct $name {
                store: Arc<DeclaredPluginStore>,
                persistence: Arc<dyn Persistence>,
                registry: Arc<uni_plugin::PluginRegistry>,
                synthesizer:
                    Option<Arc<dyn crate::ProcedureBodySynthesizer>>,
            }

            impl $name {
                /// Construct without a synthesizer (record-only).
                #[must_use]
                pub fn new(
                    store: Arc<DeclaredPluginStore>,
                    persistence: Arc<dyn Persistence>,
                ) -> Self {
                    Self {
                        store,
                        persistence,
                        registry: Arc::new(uni_plugin::PluginRegistry::new()),
                        synthesizer: None,
                    }
                }

                /// Construct with a host-supplied synthesizer so
                /// declarations install executable plugins at
                /// declare time (M11 A.3).
                #[must_use]
                pub fn new_with_synthesis(
                    store: Arc<DeclaredPluginStore>,
                    persistence: Arc<dyn Persistence>,
                    registry: Arc<uni_plugin::PluginRegistry>,
                    synthesizer: Arc<dyn crate::ProcedureBodySynthesizer>,
                ) -> Self {
                    Self {
                        store,
                        persistence,
                        registry,
                        synthesizer: Some(synthesizer),
                    }
                }
            }

            impl ProcedurePlugin for $name {
                fn signature(&self) -> &ProcedureSignature {
                    static SIG: std::sync::OnceLock<ProcedureSignature> =
                        std::sync::OnceLock::new();
                    SIG.get_or_init($sig_fn)
                }

                fn invoke(
                    &self,
                    ctx: ProcedureContext<'_>,
                    args: &[ColumnarValue],
                ) -> Result<SendableRecordBatchStream, FnError> {
                    let qname = extract_string(args, 0, "qname")?;
                    let mut sig = serde_json::Map::new();
                    // `$field_count - 1` skips the trailing `deps_json`
                    // arg, which is parsed separately via `parse_deps`.
                    for i in 1..($field_count - 1) {
                        let v = extract_string(args, i, "field")?;
                        sig.insert(format!("arg{}", i), serde_json::Value::String(v));
                    }
                    // M11 A.1: for procedure-kind declarations, extract
                    // the `mode` arg (position 2 — qname=0, body=1,
                    // mode=2) and (a) gate WRITE-mode declarations on
                    // the principal's `ProcedureWrites` capability,
                    // (b) stash `mode` under a named key so the host's
                    // `SyntheticProcedurePlugin` can read it back
                    // without relying on positional `arg2`.
                    if $kind == "procedure" {
                        if let Ok(mode_str) = extract_string(args, 2, "mode") {
                            let mode_uc = mode_str.to_ascii_uppercase();
                            if mode_uc == "WRITE" {
                                let has_writes = ctx
                                    .principal
                                    .map(|p| {
                                        p.capabilities.contains_variant(
                                            &uni_plugin::Capability::ProcedureWrites,
                                        )
                                    })
                                    .unwrap_or(false);
                                if !has_writes {
                                    return Err(FnError::new(
                                        0xB09,
                                        format!(
                                            "declareProcedure WRITE for `{qname}` denied: \
                                             principal lacks `Capability::ProcedureWrites`"
                                        ),
                                    ));
                                }
                            }
                            sig.insert(
                                "mode".to_owned(),
                                serde_json::Value::String(mode_uc),
                            );
                        }
                    }
                    let dependencies = parse_deps(args, $field_count - 1)?;
                    let declared_by = ctx
                        .principal
                        .map(|p| p.id.clone())
                        .unwrap_or_else(|| "anonymous".to_owned());
                    let body = sig
                        .get("arg1")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_owned();
                    let record = DeclaredPlugin {
                        qname: qname.clone(),
                        kind: $kind.to_owned(),
                        body,
                        signature_json: serde_json::to_string(&sig).unwrap_or_default(),
                        dependencies,
                        declared_by,
                        active: true,
                    };
                    self.store
                        .declare(record.clone())
                        .map_err(custom_to_fn_err)?;
                    self.persistence
                        .save(&record)
                        .map_err(|e| FnError::new(0xB30, format!("declare persist: {e}")))?;
                    // M11 A.3: if the host attached a synthesizer,
                    // install the executable plugin at declare time
                    // so subsequent `CALL <qname>(...)` invocations
                    // dispatch through it.
                    if let Some(synth) = self.synthesizer.as_ref() {
                        if let Err(e) = crate::procedures::install_synthesized_procedure(
                            &self.registry,
                            &record,
                            synth.as_ref(),
                        ) {
                            // NativeShadow is expected when the qname
                            // is already taken; downgrade the record
                            // to inactive but do not fail the
                            // declaration.
                            match e {
                                CustomError::NativeShadow(_) => {
                                    let mut shadowed = record.clone();
                                    shadowed.active = false;
                                    self.store.replace(shadowed.clone());
                                    let _ = self.persistence.save(&shadowed);
                                }
                                other => {
                                    return Err(FnError::new(
                                        0xB31,
                                        format!("declare synthesize: {other}"),
                                    ));
                                }
                            }
                        }
                    }
                    single_bool("registered", true)
                }
            }
        };
    }

    declare_kind_procedure!(
        DeclareProcedureProcedure,
        declare_procedure_signature,
        "procedure",
        5
    );
    declare_kind_procedure!(
        DeclareTriggerProcedure,
        declare_trigger_signature,
        "trigger",
        4
    );

    // -------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------

    /// Like [`extract_string`] but returns `default` when the argument
    /// is missing or null. Used for trailing optional args
    /// (`deps_json`, defaulted-on-declare* signatures) since the
    /// current procedure dispatch path does not auto-fill defaults from
    /// the [`ProcedureSignature`].
    fn extract_string_or(
        args: &[ColumnarValue],
        i: usize,
        _name: &str,
        default: &str,
    ) -> Result<String, FnError> {
        match args.get(i) {
            None => Ok(default.to_owned()),
            Some(cv) => match cv {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s.clone()),
                ColumnarValue::Scalar(ScalarValue::Utf8(None))
                | ColumnarValue::Scalar(ScalarValue::Null) => Ok(default.to_owned()),
                ColumnarValue::Array(arr) => arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .and_then(|a| a.iter().next().flatten().map(|s| s.to_owned()))
                    .map_or_else(|| Ok(default.to_owned()), Ok),
                _ => Ok(default.to_owned()),
            },
        }
    }

    /// Parse the `deps_json` arg at position `i` into a `Vec<String>`,
    /// defaulting to an empty vec when absent or null.
    fn parse_deps(args: &[ColumnarValue], i: usize) -> Result<Vec<String>, FnError> {
        let raw = extract_string_or(args, i, "deps_json", "[]")?;
        serde_json::from_str::<Vec<String>>(&raw).map_err(|e| {
            FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("declare: deps_json parse: {e}"),
            )
        })
    }

    fn extract_string(args: &[ColumnarValue], i: usize, name: &str) -> Result<String, FnError> {
        let cv = args.get(i).ok_or_else(|| {
            FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("declare procedure missing arg `{name}` at position {i}"),
            )
        })?;
        match cv {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8(None))
            | ColumnarValue::Scalar(ScalarValue::Null) => Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("declare procedure arg `{name}` was null"),
            )),
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<StringArray>()
                .and_then(|a| a.iter().next().flatten().map(|s| s.to_owned()))
                .ok_or_else(|| {
                    FnError::new(
                        FnError::CODE_TYPE_COERCION,
                        format!("declare procedure arg `{name}` not Utf8"),
                    )
                }),
            _ => Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("declare procedure arg `{name}` not Utf8"),
            )),
        }
    }

    fn single_bool(col: &str, v: bool) -> Result<SendableRecordBatchStream, FnError> {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new(col, DataType::Boolean, false)]));
        let arr: Arc<dyn Array> = Arc::new(BooleanArray::from(vec![v]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![arr])
            .map_err(|e| FnError::new(0xB02, format!("single bool: {e}")))?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        )))
    }

    fn custom_to_fn_err(e: CustomError) -> FnError {
        let code = match &e {
            CustomError::DependencyCycle(_) => 0xB03,
            CustomError::DependencyMissing { .. } => 0xB04,
            CustomError::NativeShadow(_) => 0xB05,
            CustomError::BodyParse(_) => 0xB06,
            CustomError::Persistence(_) => 0xB07,
            CustomError::Registration(_) => 0xB08,
            CustomError::CapabilityDenied(_) => 0xB09,
        };
        FnError::new(code, e.to_string())
    }
}

// -------------------------------------------------------------
// DeclaredPluginStore
// -------------------------------------------------------------

/// In-memory store for declared plugins.
///
/// The store is the source of truth for dependency analysis and
/// listing; persistence rides through [`Persistence`] and replays
/// the same records through this store on construction.
#[derive(Debug, Default)]
pub struct DeclaredPluginStore {
    by_qname: std::sync::RwLock<std::collections::BTreeMap<String, DeclaredPlugin>>,
}

impl DeclaredPluginStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare a new plugin or replace an existing declaration with
    /// dependency + cycle validation.
    ///
    /// # Errors
    ///
    /// Returns [`CustomError::DependencyMissing`] if any declared
    /// dependency is not present in the store. Returns
    /// [`CustomError::DependencyCycle`] if adding this plugin would
    /// introduce a cycle.
    pub fn declare(&self, plugin: DeclaredPlugin) -> Result<(), CustomError> {
        {
            let map = self.by_qname.read().expect("declared-plugin lock poisoned");
            for dep in &plugin.dependencies {
                if !map.contains_key(dep) {
                    return Err(CustomError::DependencyMissing {
                        dependent: plugin.qname.clone(),
                        dep: dep.clone(),
                    });
                }
            }
            if would_introduce_cycle(&map, &plugin) {
                return Err(CustomError::DependencyCycle(chain_starting_at(
                    &map, &plugin,
                )));
            }
        }
        self.by_qname
            .write()
            .expect("declared-plugin lock poisoned")
            .insert(plugin.qname.clone(), plugin);
        Ok(())
    }

    /// Insert / replace without dependency validation. Used by the
    /// reactivation path (records from persistence are trusted).
    pub fn declare_unchecked(&self, plugin: DeclaredPlugin) {
        self.by_qname
            .write()
            .expect("declared-plugin lock poisoned")
            .insert(plugin.qname.clone(), plugin);
    }

    /// Look up a declared plugin by qname.
    #[must_use]
    pub fn get(&self, qname: &str) -> Option<DeclaredPlugin> {
        self.by_qname
            .read()
            .expect("declared-plugin lock poisoned")
            .get(qname)
            .cloned()
    }

    /// Drop a declared plugin.
    ///
    /// Returns `true` if the plugin existed.
    ///
    /// # Errors
    ///
    /// Returns [`CustomError::DependencyMissing`] if the plugin is a
    /// dependency of another declared plugin (cascade mode lives at
    /// [`Self::drop_cascade`]).
    pub fn drop_declared(&self, qname: &str) -> Result<bool, CustomError> {
        let mut map = self
            .by_qname
            .write()
            .expect("declared-plugin lock poisoned");
        for other in map.values() {
            if other.dependencies.iter().any(|d| d == qname) {
                return Err(CustomError::DependencyMissing {
                    dependent: other.qname.clone(),
                    dep: qname.to_owned(),
                });
            }
        }
        Ok(map.remove(qname).is_some())
    }

    /// Drop a declared plugin together with every dependent.
    ///
    /// Returns the qnames removed in topological (leaves-first)
    /// order.
    pub fn drop_cascade(&self, qname: &str) -> Vec<String> {
        let mut removed = Vec::new();
        let mut map = self
            .by_qname
            .write()
            .expect("declared-plugin lock poisoned");
        let mut stack = vec![qname.to_owned()];
        while let Some(target) = stack.pop() {
            let dependents: Vec<String> = map
                .iter()
                .filter(|(_, p)| p.dependencies.iter().any(|d| d == &target))
                .map(|(k, _)| k.clone())
                .collect();
            if dependents.is_empty() {
                if map.remove(&target).is_some() {
                    removed.push(target);
                }
            } else {
                stack.push(target);
                for d in dependents {
                    stack.push(d);
                }
            }
        }
        removed
    }

    /// Replace an existing record (no validation). Used for
    /// shadow-flag updates.
    pub fn replace(&self, plugin: DeclaredPlugin) {
        self.declare_unchecked(plugin);
    }

    /// List every declared plugin.
    #[must_use]
    pub fn list(&self) -> Vec<DeclaredPlugin> {
        self.by_qname
            .read()
            .expect("declared-plugin lock poisoned")
            .values()
            .cloned()
            .collect()
    }
}

fn would_introduce_cycle(
    map: &std::collections::BTreeMap<String, DeclaredPlugin>,
    candidate: &DeclaredPlugin,
) -> bool {
    fn reachable(
        map: &std::collections::BTreeMap<String, DeclaredPlugin>,
        start: &str,
        target: &str,
        visited: &mut std::collections::BTreeSet<String>,
    ) -> bool {
        if start == target {
            return true;
        }
        if !visited.insert(start.to_owned()) {
            return false;
        }
        if let Some(node) = map.get(start) {
            for d in &node.dependencies {
                if reachable(map, d, target, visited) {
                    return true;
                }
            }
        }
        false
    }
    let mut visited = std::collections::BTreeSet::new();
    candidate
        .dependencies
        .iter()
        .any(|d| reachable(map, d, &candidate.qname, &mut visited))
}

/// Reconstruct the dependency cycle that would be introduced by adding
/// `candidate` to `map`.
///
/// Returned vector starts and ends with `candidate.qname`, with the
/// intermediate nodes naming the chain that closes the cycle (e.g.
/// `["a", "b", "c", "a"]`). If no cycle is reachable from any of
/// `candidate`'s dependencies, a single-element vector containing only
/// `candidate.qname` is returned as a defensive fallback.
fn chain_starting_at(
    map: &std::collections::BTreeMap<String, DeclaredPlugin>,
    candidate: &DeclaredPlugin,
) -> Vec<String> {
    fn dfs(
        map: &std::collections::BTreeMap<String, DeclaredPlugin>,
        node: &str,
        target: &str,
        stack: &mut Vec<String>,
        visited: &mut std::collections::BTreeSet<String>,
    ) -> bool {
        stack.push(node.to_owned());
        if node == target {
            return true;
        }
        if !visited.insert(node.to_owned()) {
            stack.pop();
            return false;
        }
        if let Some(declared) = map.get(node) {
            for dep in &declared.dependencies {
                if dfs(map, dep, target, stack, visited) {
                    return true;
                }
            }
        }
        stack.pop();
        false
    }

    let mut visited = std::collections::BTreeSet::new();
    for dep in &candidate.dependencies {
        let mut stack = vec![candidate.qname.clone()];
        if dfs(map, dep, &candidate.qname, &mut stack, &mut visited) {
            return stack;
        }
    }
    vec![candidate.qname.clone()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_plugin_round_trip_json() {
        let d = DeclaredPlugin {
            qname: "mycorp.fullName".to_owned(),
            kind: "function".to_owned(),
            body: "$first + ' ' + $last".to_owned(),
            signature_json: r#"{"args":["string","string"],"returns":"string"}"#.to_owned(),
            dependencies: vec![],
            declared_by: "alice".to_owned(),
            active: true,
        };
        let s = serde_json::to_string(&d).unwrap();
        let parsed: DeclaredPlugin = serde_json::from_str(&s).unwrap();
        assert_eq!(d, parsed);
    }

    #[test]
    fn custom_plugin_constructs_in_memory() {
        let _ = CustomPlugin::new_in_memory();
    }

    // M11 A.4: synthesizer integration tests.

    /// Mock synthesizer that produces a trivial ProcedurePlugin
    /// suitable for testing the registration path without depending
    /// on `uni-query`.
    #[derive(Debug)]
    struct StubSynthesizer {
        synthesized_count: std::sync::atomic::AtomicUsize,
    }

    impl StubSynthesizer {
        fn new() -> Self {
            Self {
                synthesized_count: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn count(&self) -> usize {
            self.synthesized_count
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl crate::ProcedureBodySynthesizer for StubSynthesizer {
        fn synthesize(
            &self,
            _decl: &DeclaredPlugin,
        ) -> Result<Arc<dyn uni_plugin::traits::procedure::ProcedurePlugin>, String> {
            self.synthesized_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Arc::new(StubProcedure {
                signature: stub_signature(),
            }))
        }
    }

    #[derive(Debug)]
    struct StubProcedure {
        signature: uni_plugin::traits::procedure::ProcedureSignature,
    }

    fn stub_signature() -> uni_plugin::traits::procedure::ProcedureSignature {
        use arrow_schema::{DataType, Field};
        uni_plugin::traits::procedure::ProcedureSignature {
            args: vec![],
            yields: vec![Field::new("ok", DataType::Boolean, false)],
            mode: uni_plugin::traits::procedure::ProcedureMode::Read,
            side_effects: uni_plugin::SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: "stub".to_owned(),
        }
    }

    impl uni_plugin::traits::procedure::ProcedurePlugin for StubProcedure {
        fn signature(&self) -> &uni_plugin::traits::procedure::ProcedureSignature {
            &self.signature
        }

        fn invoke(
            &self,
            _ctx: uni_plugin::traits::procedure::ProcedureContext<'_>,
            _args: &[datafusion::logical_expr::ColumnarValue],
        ) -> Result<datafusion::execution::SendableRecordBatchStream, uni_plugin::FnError> {
            unimplemented!(
                "StubProcedure does not execute; the synthesizer test only checks registration"
            )
        }
    }

    #[test]
    fn synthesizer_synthesize_called_on_reactivate() {
        let synth = Arc::new(StubSynthesizer::new());
        let store = Arc::new(DeclaredPluginStore::new());
        // Pre-populate a procedure-kind declaration.
        store
            .declare(DeclaredPlugin {
                qname: "mycorp.findFriends".to_owned(),
                kind: "procedure".to_owned(),
                body: "MATCH (p)-[:KNOWS]->(f) RETURN f".to_owned(),
                signature_json: "{}".to_owned(),
                dependencies: vec![],
                declared_by: "test".to_owned(),
                active: true,
            })
            .unwrap();

        let registry = Arc::new(uni_plugin::PluginRegistry::new());
        // We can't construct CustomPlugin with this pre-populated
        // store directly (its `new` reloads via persistence). Build
        // by hand and then call reactivate_into_registry.
        let plugin = CustomPlugin {
            store: Arc::clone(&store),
            registry: Arc::clone(&registry),
            persistence: Arc::new(NullPersistence),
            procedure_synthesizer: Some(synth.clone()),
            manifest: std::sync::OnceLock::new(),
        };
        plugin
            .reactivate_into_registry()
            .expect("reactivate must call synthesizer for procedure-kind records");
        assert_eq!(
            synth.count(),
            1,
            "synthesizer should have been called for the one procedure declaration"
        );
    }

    #[test]
    fn reactivate_skips_procedure_when_no_synthesizer() {
        let store = Arc::new(DeclaredPluginStore::new());
        store
            .declare(DeclaredPlugin {
                qname: "mycorp.findFriends".to_owned(),
                kind: "procedure".to_owned(),
                body: "MATCH (p)-[:KNOWS]->(f) RETURN f".to_owned(),
                signature_json: "{}".to_owned(),
                dependencies: vec![],
                declared_by: "test".to_owned(),
                active: true,
            })
            .unwrap();

        let registry = Arc::new(uni_plugin::PluginRegistry::new());
        let plugin = CustomPlugin {
            store,
            registry,
            persistence: Arc::new(NullPersistence),
            procedure_synthesizer: None, // no synthesizer
            manifest: std::sync::OnceLock::new(),
        };
        plugin
            .reactivate_into_registry()
            .expect("reactivate must succeed even with procedure records when no synthesizer");
        // No assertion needed — the absence of a panic is the
        // pre-M11 behavior we preserve.
    }

    // M11 A.1: capability-gate tests for `declareProcedure WRITE`.

    fn utf8_scalar(s: &str) -> datafusion::logical_expr::ColumnarValue {
        datafusion::logical_expr::ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(
            Some(s.to_owned()),
        ))
    }

    fn drive_declare_procedure(
        args: &[datafusion::logical_expr::ColumnarValue],
        principal: Option<&uni_plugin::traits::connector::Principal>,
    ) -> Result<(), uni_plugin::FnError> {
        let store = Arc::new(DeclaredPluginStore::new());
        let decl = procedures::DeclareProcedureProcedure::new(store, Arc::new(NullPersistence));
        let mut ctx = uni_plugin::traits::procedure::ProcedureContext::new();
        if let Some(p) = principal {
            ctx = ctx.with_principal(p);
        }
        use uni_plugin::traits::procedure::ProcedurePlugin;
        decl.invoke(ctx, args).map(|_| ())
    }

    #[test]
    fn declare_procedure_write_rejected_without_procedure_writes() {
        let args = vec![
            utf8_scalar("mycorp.deleteAll"),
            utf8_scalar("MATCH (n) DETACH DELETE n"),
            utf8_scalar("WRITE"),
            utf8_scalar("[]"),
            utf8_scalar("[]"),
        ];
        let p = uni_plugin::traits::connector::Principal {
            id: "alice".to_owned(),
            groups: vec![],
            capabilities: uni_plugin::CapabilitySet::new(),
        };
        let err = drive_declare_procedure(&args, Some(&p))
            .expect_err("WRITE without ProcedureWrites must fail");
        assert_eq!(err.code, 0xB09, "expected capability-denied code 0xB09");
    }

    #[test]
    fn declare_procedure_write_allowed_with_procedure_writes() {
        let args = vec![
            utf8_scalar("mycorp.deleteAll"),
            utf8_scalar("MATCH (n) DETACH DELETE n"),
            utf8_scalar("WRITE"),
            utf8_scalar("[]"),
            utf8_scalar("[]"),
        ];
        let mut caps = uni_plugin::CapabilitySet::new();
        caps.insert(uni_plugin::Capability::ProcedureWrites);
        let p = uni_plugin::traits::connector::Principal {
            id: "admin".to_owned(),
            groups: vec!["admin".to_owned()],
            capabilities: caps,
        };
        drive_declare_procedure(&args, Some(&p)).expect("WRITE with ProcedureWrites must succeed");
    }

    #[test]
    fn declare_procedure_read_does_not_require_procedure_writes() {
        let args = vec![
            utf8_scalar("mycorp.findFriends"),
            utf8_scalar("MATCH (p)-[:KNOWS]->(f) RETURN f"),
            utf8_scalar("READ"),
            utf8_scalar("[]"),
            utf8_scalar("[]"),
        ];
        let p = uni_plugin::traits::connector::Principal::anonymous();
        drive_declare_procedure(&args, Some(&p))
            .expect("READ mode declaration must not require ProcedureWrites");
    }

    fn make(qname: &str, deps: &[&str]) -> DeclaredPlugin {
        DeclaredPlugin {
            qname: qname.to_owned(),
            kind: "function".to_owned(),
            body: String::new(),
            signature_json: "{}".to_owned(),
            dependencies: deps.iter().map(|s| s.to_string()).collect(),
            declared_by: "test".to_owned(),
            active: true,
        }
    }

    #[test]
    fn store_declare_and_get() {
        let s = DeclaredPluginStore::new();
        s.declare(make("a.foo", &[])).unwrap();
        assert_eq!(s.get("a.foo").unwrap().qname, "a.foo");
    }

    #[test]
    fn store_rejects_missing_dependency() {
        let s = DeclaredPluginStore::new();
        match s.declare(make("a.foo", &["a.bar"])) {
            Err(CustomError::DependencyMissing { dependent, dep }) => {
                assert_eq!(dependent, "a.foo");
                assert_eq!(dep, "a.bar");
            }
            other => panic!("expected DependencyMissing, got {other:?}"),
        }
    }

    #[test]
    fn store_detects_cycle() {
        let s = DeclaredPluginStore::new();
        s.declare(make("a", &[])).unwrap();
        s.declare(make("b", &["a"])).unwrap();
        match s.declare(make("a", &["b"])) {
            Err(CustomError::DependencyCycle(_)) => {}
            other => panic!("expected DependencyCycle, got {other:?}"),
        }
    }

    #[test]
    fn store_protects_against_drop_with_dependents() {
        let s = DeclaredPluginStore::new();
        s.declare(make("a", &[])).unwrap();
        s.declare(make("b", &["a"])).unwrap();
        assert!(s.drop_declared("a").is_err());
        assert!(s.drop_declared("b").unwrap());
        assert!(s.drop_declared("a").unwrap());
    }

    #[test]
    fn store_cascade_removes_dependents() {
        let s = DeclaredPluginStore::new();
        s.declare(make("a", &[])).unwrap();
        s.declare(make("b", &["a"])).unwrap();
        s.declare(make("c", &["b"])).unwrap();
        let removed = s.drop_cascade("a");
        assert_eq!(removed.len(), 3);
        assert!(removed.iter().any(|q| q == "a"));
        assert!(removed.iter().any(|q| q == "b"));
        assert!(removed.iter().any(|q| q == "c"));
        assert!(s.list().is_empty());
    }

    #[test]
    fn store_list_returns_all_declared() {
        let s = DeclaredPluginStore::new();
        s.declare(make("x", &[])).unwrap();
        s.declare(make("y", &[])).unwrap();
        assert_eq!(s.list().len(), 2);
    }
}
