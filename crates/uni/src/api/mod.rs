// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uni_common::core::fork::ForkId;

/// Streaming appender, re-exported from `uni-bulk`.
///
/// Shim kept so `crate::api::appender::*` paths resolve unchanged after
/// the bulk-engine extraction.
pub mod appender {
    pub use uni_bulk::appender::*;
}
pub mod builder;
/// Bulk writer engine, re-exported from `uni-bulk`.
///
/// Shim kept so `crate::api::bulk::*` paths resolve unchanged after the
/// bulk-engine extraction.
pub mod bulk {
    pub use uni_bulk::bulk::*;
}
pub mod compaction;
pub(crate) mod for_update;
pub mod fork;
/// Fork diff/promote types and engine, re-exported from `uni-fork`.
///
/// Shim kept so `crate::api::fork_diff::*` paths resolve unchanged after the
/// fork-engine extraction. `compute_diff`/`run_promote` are generic over the
/// `uni_fork` host traits, which uni-db implements for `Session`/`Transaction`.
pub mod fork_diff {
    pub use uni_fork::diff::{compute_diff, run_promote};
    pub use uni_fork::types::*;
}
pub(crate) mod fork_maintenance;
pub mod fork_schema;
pub mod functions;
/// Session/commit hooks — moved to `uni-plugin-host`; re-exported to keep the
/// `uni_db::api::hooks::*` path stable.
pub mod hooks {
    pub use uni_plugin_host::hooks::*;
}
pub(crate) mod host_executor;
pub mod impl_locy;
pub mod impl_query;
pub mod indexes;
pub mod locy_builder;
pub mod locy_result;
pub mod locy_rule_catalog;
pub mod multi_agent;
pub mod plugin_trust;
/// Commit notifications — moved to `uni-plugin-host`; re-exported to keep the
/// `uni_db::api::notifications::*` path stable.
pub mod notifications {
    pub use uni_plugin_host::notifications::*;
}
pub mod prepared;
pub mod query_builder;
pub mod retry;
pub mod rule_registry;
pub mod schema;
pub mod session;
pub mod sync;
pub mod template;
pub mod transaction;
/// Trigger dispatch engine — moved to `uni-plugin-host`; re-exported to keep
/// the `uni_db::api::triggers::*` path stable.
pub mod triggers {
    pub use uni_plugin_host::triggers::*;
}
pub mod xervo;

use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::local::LocalFileSystem;
use tracing::info;
use uni_common::core::snapshot::SnapshotManifest;
use uni_common::{CloudStorageConfig, UniConfig};
use uni_common::{Result, UniError};
use uni_store::cloud::build_cloud_store;
use uni_xervo::api::{ModelAliasSpec, ModelTask};
use uni_xervo::runtime::ModelRuntime;

use uni_common::core::schema::SchemaManager;
use uni_store::runtime::id_allocator::IdAllocator;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::wal::WriteAheadLog;
use uni_store::storage::manager::StorageManager;

use uni_store::runtime::writer::Writer;

use crate::shutdown::ShutdownHandle;

use std::collections::HashMap;

/// Map a [`uni_plugin::PluginError`] to a [`UniError`] for user-facing
/// surfaces (`Uni::add_plugin`). The catch-all variant is
/// [`UniError::InvalidArgument`] so we preserve the plugin id /
/// capability detail via the `Display` impl.
pub(crate) fn plugin_err_to_uni(e: uni_plugin::PluginError) -> UniError {
    UniError::InvalidArgument {
        arg: "plugin".to_string(),
        message: e.to_string(),
    }
}

/// Run a plugin-loader pass under a freshly-built placeholder registrar.
///
/// Every loader (`load_wasm_extism` / `load_wasm_component` /
/// `load_rhai_plugin` / `load_python_plugin`, plus the session-scoped
/// Python add/finalize paths) follows the same three-step dance:
/// construct a [`uni_plugin::PluginRegistrar`] under a placeholder plugin
/// id (the loader rewrites the real id from the manifest into the returned
/// `LoadOutcome`), run the loader's `load*` call, then atomically commit
/// the staged registrations into `registry`.
///
/// `f` performs the loader-specific `load*` call and maps the loader's
/// error enum to a [`UniError`] — that mapping stays at the call site so
/// each loader preserves its exact error-variant handling. This helper
/// only owns the placeholder/registrar construction and the final commit.
pub(crate) fn with_loading_registrar<T>(
    registry: &Arc<uni_plugin::PluginRegistry>,
    placeholder: &str,
    caps: &uni_plugin::CapabilitySet,
    f: impl FnOnce(&mut uni_plugin::PluginRegistrar) -> Result<T>,
) -> Result<T> {
    use uni_plugin::{PluginId, PluginRegistrar};
    let mut r = PluginRegistrar::new(PluginId::new(placeholder), caps, registry);
    let outcome = f(&mut r)?;
    // Snapshot the aggregate qnames staged by the loader *before* the commit
    // consumes the registrar, so we can publish their Cypher routing hints
    // once the commit succeeds.
    let staged_aggregates: Vec<String> = r
        .staged_aggregate_qnames()
        .iter()
        .map(|q| format!("{}.{}", q.namespace(), q.local()))
        .collect();
    r.commit_to_registry().map_err(plugin_err_to_uni)?;
    // Publish each committed aggregate to the Cypher planner's plugin-aggregate
    // hint set so `RETURN ns.myAgg(x)` (and `GROUP BY`) routes through aggregate
    // translation instead of scalar-UDF resolution. This is the single point
    // every dynamic loader (rhai / pyo3 / wasm / extism) passes through, so all
    // of them are covered without each depending on `uni-cypher`. Mirrors the
    // declared-aggregate path in `uni-plugin-custom` (`declareAggregate`).
    // Idempotent: the hint set is a deduplicating set.
    for dotted in staged_aggregates {
        uni_cypher::register_plugin_aggregate(dotted);
    }
    Ok(outcome)
}

/// Register the framework-wide built-in plugins into a fresh
/// `PluginRegistry`. Called once at `Uni::build()` time.
///
/// - `BuiltinPlugin` is always registered (closed-enum replacement
///   infrastructure: Locy aggregates, storage backends, CRDTs, collations,
///   hooks, logical types, plus a handful of system procedures).
/// - `ApocCorePlugin` is registered when the `apoc-core` cargo feature is
///   on (default). Library embedders who don't want APOC content disable
///   the feature.
fn register_builtin_plugins(
    registry: &Arc<uni_plugin::PluginRegistry>,
    data_path: Option<&std::path::Path>,
) -> std::result::Result<Option<Arc<crate::persistence::LazyCypherSink>>, uni_plugin::PluginError> {
    use uni_plugin::{Plugin, PluginRegistrar};

    // BuiltinPlugin — always.
    {
        let plugin = uni_plugin_builtin::BuiltinPlugin::new();
        let manifest = plugin.manifest();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, registry);
        plugin.register(&mut r)?;
        r.commit_to_registry()?;
    }

    // ApocCorePlugin — feature-gated, default-on.
    #[cfg(feature = "apoc-core")]
    {
        let plugin = uni_plugin_apoc_core::ApocCorePlugin::new();
        let manifest = plugin.manifest();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, registry);
        plugin.register(&mut r)?;
        r.commit_to_registry()?;
    }

    // Host-coupled built-in procedures (uni.schema.*, uni.vector.query,
    // uni.fts.query, uni.search, uni.algo.*). These live in `uni-query`
    // rather than `uni-plugin-builtin` because they depend on
    // `uni-store` / `uni-algo` types that the latter cannot reach
    // without inverting the crate layering.
    {
        use uni_plugin::{Capability, CapabilitySet, PluginId};

        let plugin_id = PluginId::new("uni");
        let caps = CapabilitySet::from_iter_of([
            Capability::Procedure,
            Capability::ProcedureSchema,
            // M11: background-job registration gate. The three built-in
            // maintenance jobs (`uni.system.ttl_sweep` /
            // `statistics_refresh` / `compaction`) register through
            // this plugin id; the registrar's variant-match treats any
            // `BackgroundJob` cap as sufficient regardless of
            // `max_concurrent`.
            Capability::BackgroundJob { max_concurrent: 0 },
        ]);
        let mut caps = caps;
        // M5c.1: this block also registers `AlgorithmProvider`s so the
        // `Capability::Algorithm` must be in scope. The host's "uni"
        // plugin owns both procedure registrations (`uni.algo.*`
        // adapters) and the AlgorithmProvider chain.
        caps.insert(Capability::Algorithm);
        let mut r = PluginRegistrar::new(plugin_id, &caps, registry);
        let algo_registry: Arc<uni_algo::algo::AlgorithmRegistry> =
            Arc::new(uni_algo::algo::AlgorithmRegistry::new());
        uni_query::procedures_plugin::register_into(&mut r, Some(&algo_registry))?;
        // M5c.1: register each algorithm as a phased `AlgorithmProvider`
        // so consumers can `registry.iter_algorithms()` /
        // `registry.algorithm(qname)`. The static `AlgorithmRegistry`
        // path above is the M4 adapter and stays in place during M5c.1
        // — both surfaces resolve to the same underlying `AlgoProcedure`
        // impls.
        uni_plugin_builtin::algorithms::register_into(&mut r)?;
        // M11: the three built-in maintenance jobs (`ttl_sweep`,
        // `statistics_refresh`, `compaction`). The host scheduler
        // driver in `crates/uni/src/scheduler.rs` looks each up by
        // qname and dispatches per its `Schedule::Periodic` interval.
        uni_plugin_builtin::background_jobs::register_into(&mut r)?;
        r.commit_to_registry()?;
    }

    // CustomPlugin — apoc.custom-style meta-plugin always-on. Exposes
    // `uni.plugin.declareFunction/Procedure/Aggregate/Trigger` plus
    // `listDeclared` / `dropDeclared`. The plugin holds a shared
    // `Arc<PluginRegistry>` so its declare* procedures can register
    // new scalar functions at runtime; persistence rides through
    // `SystemLabelPersistence` (M11 A.2) when the instance has a
    // local data directory, else `NullPersistence` (in-memory /
    // object-store-backed instances).
    let (persistence, cypher_sink) = crate::persistence::persistence_for_data_path(data_path);
    {
        let synthesizer: Arc<dyn uni_plugin_custom::ProcedureBodySynthesizer> =
            Arc::new(crate::synthetic_procedure::CypherProcedureSynthesizer::new());
        let plugin = uni_plugin_custom::CustomPlugin::new(Arc::clone(registry), persistence)
            .map_err(|e| uni_plugin::PluginError::internal(format!("uni-plugin-custom: {e}")))?
            .with_procedure_synthesizer(synthesizer);
        plugin.reactivate_into_registry().map_err(|e| {
            uni_plugin::PluginError::internal(format!("uni-plugin-custom reactivate: {e}"))
        })?;
        let manifest = plugin.manifest();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, registry);
        plugin.register(&mut r)?;
        r.commit_to_registry()?;
    }

    Ok(cypher_sink)
}

/// Join a storage base URI with a dataset name into a `*.lance` URI,
/// inserting a `/` separator only when the base lacks a trailing slash.
///
/// Delegates to [`uni_store::fork::recovery::join_uri_with`] so the
/// fork-op join sites (`drop_fork` / `tag_fork` / `untag_fork` /
/// `list_fork_tags`) share one source of truth with the recovery path.
fn dataset_uri(base_uri: &str, dataset: &str) -> String {
    uni_store::fork::recovery::join_uri_with(base_uri.to_string())(dataset)
}

/// Whether a schema element (label or edge type) is present and `Active`.
///
/// Shared by `label_exists` / `edge_type_exists`; `state` is the looked-up
/// element's state (`None` when the element is absent from the schema).
fn element_active(state: Option<&uni_common::core::schema::SchemaElementState>) -> bool {
    matches!(
        state,
        Some(uni_common::core::schema::SchemaElementState::Active)
    )
}

/// Build the `PropertyInfo` projection for a label or edge type.
///
/// Shared by [`Uni::get_label_info`] and [`Uni::get_edge_type_info`];
/// `is_indexed` is supplied per element kind because labels consult more
/// index variants (vector / JSON-FTS) than edge types do — keeping the
/// exact per-kind predicate preserves the original behavior.
fn property_infos_for(
    schema: &uni_common::core::schema::Schema,
    name: &str,
    is_indexed: impl Fn(&uni_common::core::schema::IndexDefinition, &str, &str) -> bool,
) -> Vec<crate::api::schema::PropertyInfo> {
    let mut properties = Vec::new();
    if let Some(props) = schema.properties.get(name) {
        for (prop_name, prop_meta) in props {
            properties.push(crate::api::schema::PropertyInfo {
                name: prop_name.clone(),
                data_type: format!("{:?}", prop_meta.r#type),
                nullable: prop_meta.nullable,
                is_indexed: schema
                    .indexes
                    .iter()
                    .any(|idx| is_indexed(idx, name, prop_name)),
                description: prop_meta.description.clone(),
            });
        }
    }
    properties
}

/// Build the `IndexInfo` projection for a label or edge type.
///
/// `descriptor` maps each index targeting `name` to its `(type, props)`
/// pair, returning `None` to skip variants that do not apply to this
/// element kind (e.g. edge types skip vector / JSON-FTS indexes).
fn index_infos_for(
    schema: &uni_common::core::schema::Schema,
    name: &str,
    descriptor: impl Fn(
        &uni_common::core::schema::IndexDefinition,
    ) -> Option<(&'static str, Vec<String>)>,
) -> Vec<crate::api::schema::IndexInfo> {
    let mut indexes = Vec::new();
    for idx in schema.indexes.iter().filter(|i| i.label() == name) {
        let Some((idx_type, idx_props)) = descriptor(idx) else {
            continue;
        };
        indexes.push(crate::api::schema::IndexInfo {
            name: idx.name().to_string(),
            index_type: idx_type.to_string(),
            properties: idx_props,
            status: "ONLINE".to_string(), // TODO: Check actual status
        });
    }
    indexes
}

/// Build the `ConstraintInfo` projection for a label or edge type.
///
/// `target_matches` selects the constraints whose target matches `name`
/// (`ConstraintTarget::Label` for labels, `EdgeType` for edge types).
fn constraint_infos_for(
    schema: &uni_common::core::schema::Schema,
    target_matches: impl Fn(&uni_common::core::schema::Constraint) -> bool,
) -> Vec<crate::api::schema::ConstraintInfo> {
    use uni_common::core::schema::ConstraintType;
    let mut constraints = Vec::new();
    for c in &schema.constraints {
        if !target_matches(c) {
            continue;
        }
        let (ctype, cprops) = match &c.constraint_type {
            ConstraintType::Unique { properties } => ("UNIQUE", properties.clone()),
            ConstraintType::Exists { property } => ("EXISTS", vec![property.clone()]),
            ConstraintType::Check { expression } => ("CHECK", vec![expression.clone()]),
            _ => ("UNKNOWN", vec![]),
        };
        constraints.push(crate::api::schema::ConstraintInfo {
            name: c.name.clone(),
            constraint_type: ctype.to_string(),
            properties: cprops,
            enabled: c.enabled,
        });
    }
    constraints
}

/// `is_indexed` predicate for label properties (consults vector, scalar,
/// full-text, inverted, and JSON-FTS index variants).
fn label_property_is_indexed(
    idx: &uni_common::core::schema::IndexDefinition,
    name: &str,
    prop_name: &str,
) -> bool {
    use uni_common::core::schema::IndexDefinition;
    match idx {
        IndexDefinition::Vector(v) => v.label == name && v.property.as_str() == prop_name,
        IndexDefinition::Scalar(s) => {
            s.label == name && s.properties.iter().any(|p| p == prop_name)
        }
        IndexDefinition::FullText(f) => {
            f.label == name && f.properties.iter().any(|p| p == prop_name)
        }
        IndexDefinition::Inverted(inv) => inv.label == name && inv.property.as_str() == prop_name,
        IndexDefinition::JsonFullText(j) => j.label == name,
        _ => false,
    }
}

/// `is_indexed` predicate for edge-type properties (scalar, full-text,
/// and inverted only — edges carry no vector / JSON-FTS indexes).
fn edge_property_is_indexed(
    idx: &uni_common::core::schema::IndexDefinition,
    name: &str,
    prop_name: &str,
) -> bool {
    use uni_common::core::schema::IndexDefinition;
    match idx {
        IndexDefinition::Scalar(s) => {
            s.label == name && s.properties.iter().any(|p| p == prop_name)
        }
        IndexDefinition::FullText(f) => {
            f.label == name && f.properties.iter().any(|p| p == prop_name)
        }
        IndexDefinition::Inverted(inv) => inv.label == name && inv.property.as_str() == prop_name,
        _ => false,
    }
}

/// Index `(type, props)` descriptor for labels (maps all five variants).
fn label_index_descriptor(
    idx: &uni_common::core::schema::IndexDefinition,
) -> Option<(&'static str, Vec<String>)> {
    use uni_common::core::schema::IndexDefinition;
    match idx {
        IndexDefinition::Vector(v) => Some(("VECTOR", vec![v.property.clone()])),
        IndexDefinition::Scalar(s) => Some(("SCALAR", s.properties.clone())),
        IndexDefinition::FullText(f) => Some(("FULLTEXT", f.properties.clone())),
        IndexDefinition::Inverted(inv) => Some(("INVERTED", vec![inv.property.clone()])),
        IndexDefinition::JsonFullText(j) => Some(("JSON_FTS", vec![j.column.clone()])),
        _ => None,
    }
}

/// Index `(type, props)` descriptor for edge types (skips vector /
/// JSON-FTS variants).
fn edge_index_descriptor(
    idx: &uni_common::core::schema::IndexDefinition,
) -> Option<(&'static str, Vec<String>)> {
    use uni_common::core::schema::IndexDefinition;
    match idx {
        IndexDefinition::Scalar(s) => Some(("SCALAR", s.properties.clone())),
        IndexDefinition::FullText(f) => Some(("FULLTEXT", f.properties.clone())),
        IndexDefinition::Inverted(inv) => Some(("INVERTED", vec![inv.property.clone()])),
        _ => None,
    }
}

/// Shared inner state of a Uni database instance.
///
/// Wrapped in `Arc` by [`Uni`] so that [`Session`](session::Session) and
/// [`Transaction`](transaction::Transaction) can hold cheap, owned references
/// without lifetime parameters.
/// One live entry in the `UniInner::active_connectors` map.
///
/// Holds the `Arc<dyn Connector>` so the trait object outlives the
/// plugin-registry snapshot it was started from, the underlying
/// plugin-reported `ConnectorHandle` for the eventual `stop()`
/// dispatch, and the protocol name for diagnostics / `Uni::active_connectors`.
#[derive(Clone)]
#[doc(hidden)]
pub struct ActiveConnector {
    /// Protocol name (`"bolt"`, `"graphql"`, …).
    pub protocol: String,
    /// Connector handle as returned by the plugin's `start()` —
    /// passed back to `Connector::stop()` verbatim.
    pub handle: uni_plugin::traits::connector::ConnectorHandle,
    /// Connector trait object kept alive for the lifecycle.
    pub connector: Arc<dyn uni_plugin::traits::connector::Connector>,
}

impl std::fmt::Debug for ActiveConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveConnector")
            .field("protocol", &self.protocol)
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

/// A live plugin entry tracked by `UniInner.plugins`.
///
/// Holds the installed plugin object, the lifecycle handle the reload
/// driver advances through `Active → Draining → Removed`, and the
/// monotonic `generation` exposed via [`uni_plugin::PluginHandle`]. The
/// `generation` is bumped on every successful reload so handles handed
/// to callers identify the *epoch* of the plugin, not just its id.
#[derive(Clone)]
pub struct UniPluginEntry {
    /// The installed plugin object (Arc-shared so `shutdown()` can run
    /// after the registry has dropped its references).
    pub plugin: Arc<dyn uni_plugin::Plugin>,
    /// Shared lifecycle handle the `EpochFencedReload` driver advances.
    pub lifecycle: Arc<uni_plugin::lifecycle::PluginLifecycle>,
    /// Monotonic generation counter; bumped per successful reload.
    pub generation: u64,
}

impl std::fmt::Debug for UniPluginEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UniPluginEntry")
            .field("plugin_id", &self.lifecycle.plugin())
            .field("state", &self.lifecycle.state())
            .field("generation", &self.generation)
            .finish()
    }
}

/// Shared inner state of a Uni database instance. Not intended for direct use.
#[doc(hidden)]
pub struct UniInner {
    pub(crate) storage: Arc<StorageManager>,
    pub(crate) schema: Arc<SchemaManager>,
    pub(crate) properties: Arc<PropertyManager>,
    pub(crate) writer: Option<Arc<Writer>>,
    pub(crate) xervo_runtime: Option<Arc<ModelRuntime>>,
    pub(crate) config: UniConfig,
    pub(crate) procedure_registry: Arc<uni_query::ProcedureRegistry>,
    /// Framework-wide plugin registry — `BuiltinPlugin` and (optionally)
    /// `ApocCorePlugin` register here at construction time. The
    /// `procedure_registry` holds an `Arc` of this same registry so
    /// `CALL` dispatch can resolve plugin-registered procedures.
    pub(crate) plugin_registry: Arc<uni_plugin::PluginRegistry>,
    /// Per-installed-plugin lifecycle bookkeeping for M10 reload.
    ///
    /// Keyed by [`uni_plugin::PluginId`]. The value holds a clone of the
    /// installed plugin object (so `shutdown()` runs on removal), the
    /// shared [`uni_plugin::lifecycle::PluginLifecycle`] handle the
    /// `EpochFencedReload` driver advances, and the monotonic generation
    /// counter exposed through [`uni_plugin::PluginHandle::generation`].
    ///
    /// Shared by `Arc` across `at_snapshot` / `at_fork` clones because
    /// the underlying `plugin_registry` is shared too — reloading a
    /// plugin on any session must be observable to siblings.
    pub(crate) plugins: Arc<parking_lot::RwLock<HashMap<uni_plugin::PluginId, UniPluginEntry>>>,
    /// In-memory deferral queue for `TriggerOutcome::Defer` (M11 v1).
    /// Persistent backing is `TODO(M11-persist)`. The background tick
    /// task spawned in `Uni::build` drives this queue; the trigger
    /// router pushes to it on `Defer`.
    pub(crate) defer_queue: Arc<crate::api::triggers::DeferralQueue>,
    /// M11 background-job scheduler host. Owns the
    /// [`uni_plugin::scheduler::Scheduler`] primitive that the
    /// `uni.periodic.*` procedures register jobs against. Driver task
    /// is tracked by the shared [`Self::shutdown_handle`].
    pub(crate) scheduler_host: Arc<crate::scheduler::SchedulerHost>,
    pub(crate) shutdown_handle: Arc<ShutdownHandle>,
    /// Global registry of pre-compiled Locy rules.
    ///
    /// Cloned into every new Session. Use `db.register_rules()` to add rules
    /// globally, or `session.register_rules()` for session-scoped rules.
    pub(crate) locy_rule_registry: Arc<std::sync::RwLock<impl_locy::LocyRuleRegistry>>,
    /// Durable backing for the database-level Locy rule registry.
    ///
    /// `Some` only on the primary database inner; `None` on session, fork, and
    /// snapshot inners (set in [`Self::derived_clone`]) so those registries
    /// stay ephemeral and never write the catalog.
    pub(crate) locy_rule_persister: Option<Arc<locy_rule_catalog::LocyRulePersister>>,
    /// Timestamp when this database instance was built.
    pub(crate) start_time: Instant,
    /// Broadcast channel for commit notifications.
    pub(crate) commit_tx: tokio::sync::broadcast::Sender<Arc<notifications::CommitNotification>>,
    /// Write lease configuration for multi-agent access.
    pub(crate) write_lease: Option<multi_agent::WriteLease>,
    /// Host plugin trust policy — signature enforcement + trust root.
    /// Consulted at every plugin-load site. Default: Disabled + empty root
    /// (accept everything, as before).
    pub(crate) plugin_trust: Arc<plugin_trust::PluginTrustConfig>,
    /// Number of currently active sessions.
    pub(crate) active_session_count: AtomicUsize,
    /// Total queries executed across all sessions.
    pub(crate) total_queries: AtomicU64,
    /// Total transactions committed across all sessions.
    pub(crate) total_commits: AtomicU64,
    /// Database-level registry of custom scalar functions.
    pub(crate) custom_functions: Arc<std::sync::RwLock<uni_query::CustomFunctionRegistry>>,
    /// DataFusion `SessionContext` template with all Cypher UDFs
    /// pre-registered. Cloned per query (O(1) Arc bump) when the executor
    /// has no custom UDFs installed, skipping the ~140 µs cost of building
    /// a fresh `SessionContext` and re-registering UDFs every call.
    ///
    /// **Safe to share** because: (a) no code path mutates the session via
    /// `.write()` outside of the cold-path custom-UDF branch in
    /// `create_datafusion_planner` (verified by grep); (b) custom UDFs are
    /// registered on a fresh, isolated `SessionContext` to avoid leaking
    /// into this template.
    pub(crate) df_session_template: Arc<datafusion::execution::context::SessionContext>,
    /// Pre-configured `Executor` template with all session-constant fields
    /// already populated (storage, config, xervo_runtime, procedure_registry,
    /// writer, df_session_template, prop_manager). Cloned per query
    /// (cheap Arc bumps + a fresh `warnings` Mutex via manual `Clone` impl),
    /// after which only per-query fields (transaction_l0, id_reservoir,
    /// custom_functions, cancellation_token) need to be set.
    ///
    /// Skips ~25 µs/query of `Executor::new` + repeated setter dispatches.
    pub(crate) executor_template: Arc<uni_query::Executor>,
    /// Fork registry — persists `catalog/fork_registry.json` and runs
    /// the create/drop 2PC. Built once during `Uni::open` and shared
    /// by the primary `UniInner` and every forked-session inner.
    pub(crate) fork_registry: Arc<uni_store::fork::ForkRegistryHandle>,
    /// Phase 2 Day 11 — number of `Transaction`s currently alive on
    /// this `UniInner`. A transaction increments at construction and
    /// decrements on `Drop` (whether committed, rolled back, or
    /// silently dropped). `Uni::drop_fork` peeks this counter via the
    /// `fork_inners` cache to surface uncommitted-tx state as a
    /// typed `UniError::ForkInflightTx` instead of letting the drop
    /// proceed and silently discard the work.
    pub(crate) inflight_tx_count: Arc<AtomicUsize>,
    /// M6a.3 — registry of active connector lifecycles.
    ///
    /// Map key is the `ConnectorHandle.0` returned by
    /// `Connector::start`. The value carries the protocol name (so
    /// `stop_connector` can dispatch back to the right
    /// `Connector::stop` impl) and a shared `Arc<dyn Connector>` so
    /// the connector trait object stays alive for the duration of
    /// the lifecycle even if the plugin registry is later swapped
    /// (the current `PluginRegistry::connectors` returns an `Arc`
    /// snapshot, so this is mostly belt-and-braces).
    pub(crate) active_connectors: Arc<DashMap<u64, ActiveConnector>>,
    /// M6a.3 — monotonically increasing id used to disambiguate
    /// `ConnectorHandle`s when a plugin returns id=0 for every
    /// `start` (the trait doesn't require unique ids).
    pub(crate) next_connector_seq: AtomicU64,
    /// Phase 2 Day 8 cache: same-fork-name `Session::fork(name)` calls
    /// share the same `Arc<UniInner>` so sibling sessions on the same
    /// fork see each other's commits without flushing through Lance
    /// (which would otherwise be the only synchronization point at the
    /// branch level). Held as `Weak` so the inner is reclaimed when
    /// the last session drops; `ForkBuilder::build` rebuilds on the
    /// next call. Initialized empty on the primary `UniInner`; each
    /// forked inner clones the same `Arc<DashMap>` so siblings see
    /// the registry from any direction.
    pub(crate) fork_inners: Arc<DashMap<ForkId, Weak<UniInner>>>,

    // ── Cached metrics (updated on commit, read by sync `metrics()`) ─────
    /// Cached L0 mutation count (updated after every commit).
    pub(crate) cached_l0_mutation_count: AtomicUsize,
    /// Cached L0 estimated size in bytes (updated after every commit).
    pub(crate) cached_l0_estimated_size: AtomicUsize,
    /// Cached WAL log sequence number (updated after every commit).
    pub(crate) cached_wal_lsn: AtomicU64,
    /// Temp directory guard — auto-deletes on drop. Only set for `Uni::temporary()`.
    pub(crate) _temp_dir: Option<TempDir>,
    /// Transparent plan cache for the transaction write path.
    ///
    /// Caches the pre-rewrite logical plan keyed by query-text hash + schema
    /// version, so repeated `Transaction::execute` of the same statement shape
    /// (e.g. ingest `UNWIND … CREATE`) skips parse and planning. Shared db-wide
    /// via `Arc`. Forks/snapshots get a fresh empty cache in `derived_clone`
    /// because their storage layout (and thus the fork-fusion rewrite) differs.
    /// The logical-plan rewrites (`rewrite_for_fork_fusion`, `fuse_create_set`)
    /// and parameter binding still run per execution, so cached reuse is
    /// parameter-value independent.
    pub(crate) plan_cache: Arc<std::sync::Mutex<crate::api::session::PlanCache>>,
}

/// Capacity of the transaction-write-path plan cache (entries).
///
/// Matches the read-path [`crate::api::session`] cache (1000 entries, LFU
/// eviction). Large enough to retain every distinct ingest statement shape a
/// workload uses; raising it only helps when a session cycles through more than
/// this many *distinct* query texts.
pub(crate) const TX_PLAN_CACHE_CAPACITY: usize = 1000;

/// Write throttle pressure as a value in 0.0–1.0.
///
/// Indicates how much back-pressure the storage layer is exerting.
/// 0.0 means no throttling; 1.0 means fully throttled.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct ThrottlePressure(f64);

impl ThrottlePressure {
    /// Create a new throttle pressure value, clamped to 0.0–1.0.
    pub fn new(value: f64) -> Self {
        Self(value.clamp(0.0, 1.0))
    }

    /// The raw pressure value (0.0–1.0).
    pub fn value(&self) -> f64 {
        self.0
    }

    /// Returns `true` if any throttle pressure is active.
    pub fn is_throttled(&self) -> bool {
        self.0 > 0.0
    }
}

impl std::fmt::Display for ThrottlePressure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.1}%", self.0 * 100.0)
    }
}

impl Default for ThrottlePressure {
    fn default() -> Self {
        Self(0.0)
    }
}

/// Snapshot of database-level metrics.
#[derive(Debug, Clone)]
pub struct DatabaseMetrics {
    /// Current L0 mutation count (cumulative since last flush).
    pub l0_mutation_count: usize,
    /// Estimated L0 buffer size in bytes.
    pub l0_estimated_size_bytes: usize,
    /// Schema version number.
    pub schema_version: u64,
    /// Time since the database instance was created.
    pub uptime: Duration,
    /// Number of currently active sessions.
    pub active_sessions: usize,
    /// Number of L1 compaction runs completed (0 until storage instrumentation).
    pub l1_run_count: usize,
    /// Write throttle pressure (0.0–1.0, 0 until instrumentation).
    pub write_throttle_pressure: ThrottlePressure,
    /// Current compaction status.
    pub compaction_status: uni_store::CompactionStatus,
    /// WAL size in bytes (0 until storage instrumentation).
    pub wal_size_bytes: u64,
    /// Highest WAL log sequence number that has been flushed (0 when no WAL is configured).
    pub wal_lsn: u64,
    /// Total queries executed across all sessions.
    pub total_queries: u64,
    /// Total transactions committed across all sessions.
    pub total_commits: u64,
}

/// Main entry point for Uni embedded database.
///
/// `Uni` is the lifecycle and admin handle. All data access goes through
/// [`Session`](session::Session) (reads) and [`Transaction`](transaction::Transaction) (writes).
///
/// # Examples
///
/// ```no_run
/// use uni_db::Uni;
///
/// #[tokio::main]
/// async fn main() -> Result<(), uni_db::UniError> {
///     let db = Uni::open("./my_db").build().await?;
///
///     // All data access goes through sessions
///     let session = db.session();
///     let results = session.query("MATCH (n) RETURN count(n)").await?;
///     println!("Count: {:?}", results);
///     Ok(())
/// }
/// ```
pub struct Uni {
    pub(crate) inner: Arc<UniInner>,
}

// No Deref<Target = UniInner> — Uni is an opaque handle.
// All field access goes through `self.inner.field` explicitly.

/// Build the cached `Arc<Executor>` template held on `UniInner`.
///
/// Populates every session-constant field on `Executor` so each query can
/// clone this template (cheap Arc bumps + a fresh `warnings` Mutex via the
/// manual `Clone` impl) instead of running `Executor::new` + six setters.
#[allow(clippy::too_many_arguments)]
fn build_executor_template(
    storage: Arc<StorageManager>,
    config: UniConfig,
    writer: Option<Arc<uni_store::runtime::writer::Writer>>,
    xervo_runtime: Option<Arc<ModelRuntime>>,
    procedure_registry: Arc<uni_query::ProcedureRegistry>,
    properties: Arc<PropertyManager>,
    df_session_template: Arc<datafusion::execution::context::SessionContext>,
) -> Arc<uni_query::Executor> {
    let mut e = uni_query::Executor::new(storage);
    e.set_config(config);
    e.set_xervo_runtime(xervo_runtime);
    e.set_procedure_registry(procedure_registry);
    if let Some(w) = writer {
        e.set_writer(w);
    }
    e.set_prop_manager(properties);
    e.set_df_session_template(df_session_template);
    Arc::new(e)
}

impl UniInner {
    /// Build a [`uni_bulk::BulkBackend`] handle bundle from this inner's
    /// fields for the bulk-write driver (`bulk_writer`/`appender`).
    pub(crate) fn bulk_backend(self: &Arc<Self>) -> uni_bulk::BulkBackend {
        uni_bulk::BulkBackend {
            storage: self.storage.clone(),
            writer: self.writer.clone(),
            schema: self.schema.clone(),
            shutdown: self.shutdown_handle.clone(),
            config: self.config.clone(),
        }
    }

    /// Build a derived `UniInner` that shares most of `self`'s state but
    /// swaps in a different storage view (a pinned snapshot or a fork
    /// branch).
    ///
    /// The five arguments are the only fields that differ between a
    /// snapshot/fork inner and `self`: `storage`, `schema`, `properties`,
    /// `writer`, `locy_rule_registry`, and the `executor_template` built
    /// from them. Everything else is either cloned from `self` (registries,
    /// trust config, fork bookkeeping, …) or reset fresh per the spec's
    /// per-view isolation contract (cancellation token, broadcast channel,
    /// metrics counters). Used by both [`Self::at_snapshot`] and
    /// [`Self::at_fork`] so a new field is added in exactly one place.
    fn derived_clone(
        &self,
        storage: Arc<StorageManager>,
        schema: Arc<SchemaManager>,
        properties: Arc<PropertyManager>,
        writer: Option<Arc<Writer>>,
        locy_rule_registry: Arc<std::sync::RwLock<impl_locy::LocyRuleRegistry>>,
        executor_template: Arc<uni_query::Executor>,
    ) -> UniInner {
        let (commit_tx, _) = tokio::sync::broadcast::channel(256);
        UniInner {
            storage,
            schema,
            properties,
            writer,
            xervo_runtime: self.xervo_runtime.clone(),
            config: self.config.clone(),
            procedure_registry: self.procedure_registry.clone(),
            plugin_registry: self.plugin_registry.clone(),
            plugins: self.plugins.clone(),
            defer_queue: self.defer_queue.clone(),
            scheduler_host: Arc::clone(&self.scheduler_host),
            shutdown_handle: Arc::new(ShutdownHandle::new(Duration::from_secs(30))),
            locy_rule_registry,
            // Fork/snapshot inners must not persist rule mutations: keep them
            // ephemeral so a fork's registrations never touch the primary
            // catalog.
            locy_rule_persister: None,
            start_time: Instant::now(),
            commit_tx,
            write_lease: None,
            plugin_trust: self.plugin_trust.clone(),
            active_session_count: AtomicUsize::new(0),
            total_queries: AtomicU64::new(0),
            total_commits: AtomicU64::new(0),
            custom_functions: self.custom_functions.clone(),
            df_session_template: self.df_session_template.clone(),
            executor_template,
            fork_registry: self.fork_registry.clone(),
            fork_inners: self.fork_inners.clone(),
            inflight_tx_count: Arc::new(AtomicUsize::new(0)),
            active_connectors: Arc::new(DashMap::new()),
            next_connector_seq: AtomicU64::new(1),
            cached_l0_mutation_count: AtomicUsize::new(0),
            cached_l0_estimated_size: AtomicUsize::new(0),
            cached_wal_lsn: AtomicU64::new(0),
            _temp_dir: None,
            // Fork/snapshot inners read a different storage layout, so they
            // must not reuse the primary's cached (fork-fusion-shaped) plans.
            plan_cache: Arc::new(std::sync::Mutex::new(crate::api::session::PlanCache::new(
                TX_PLAN_CACHE_CAPACITY,
            ))),
        }
    }

    /// Open a point-in-time view of the database at the given snapshot.
    ///
    /// Returns a new `UniInner` that is pinned to the specified snapshot state.
    /// The returned instance is read-only.
    pub(crate) async fn at_snapshot(&self, snapshot_id: &str) -> Result<UniInner> {
        let manifest = self
            .storage
            .snapshot_manager()
            .load_snapshot(snapshot_id)
            .await
            .map_err(UniError::Internal)?;

        let pinned_storage = Arc::new(self.storage.pinned(manifest));

        let prop_manager = Arc::new(PropertyManager::with_plugin_registry(
            pinned_storage.clone(),
            self.schema.clone(),
            self.properties.cache_size(),
            self.plugin_registry.clone(),
        ));

        let executor_template = build_executor_template(
            pinned_storage.clone(),
            self.config.clone(),
            None,
            self.xervo_runtime.clone(),
            self.procedure_registry.clone(),
            prop_manager.clone(),
            self.df_session_template.clone(),
        );
        Ok(self.derived_clone(
            pinned_storage,
            self.schema.clone(),
            prop_manager,
            None,
            Arc::new(std::sync::RwLock::new(
                impl_locy::LocyRuleRegistry::default(),
            )),
            executor_template,
        ))
    }

    /// Construct a fork-scoped clone of this `UniInner`.
    ///
    /// Mirror of [`Self::at_snapshot`] for forks: the returned inner
    /// reads through the fork's Lance branches via `base_paths`, and
    /// its schema is `primary_schema ⊕ overlay`. In Phase 1 the writer
    /// is `None` — fork-scoped writes are gated at the API layer in
    /// `Session::tx`. Phase 2 will populate `writer` once L0 routing
    /// lands.
    ///
    /// The cancellation token, broadcast channel, and metrics are all
    /// fresh per the spec §4.3–4.6 contract: a forked session has
    /// per-fork notifications, hooks, params, and metrics. The Locy
    /// rule registry is a deep clone of primary's so rule registration
    /// on a forked session does not leak to primary.
    pub(crate) async fn at_fork(&self, scope: Arc<uni_store::fork::ForkScope>) -> Result<UniInner> {
        // Phase 3 (nested forks): `self` may itself be a fork-scoped
        // UniInner, in which case `self.schema` already encodes
        // `primary ⊕ parent_overlay`. Layering the child's overlay on
        // top here gives `primary ⊕ parent_overlay ⊕ child_overlay`
        // without any explicit chain walk — `with_overlay` clones the
        // current manager's view into a fresh merged snapshot
        // (`schema.rs:929-966`), so each level produces its own frozen
        // snapshot at session-open time. Additions made on the parent
        // *after* the child was created stay isolated from the child by
        // construction, which matches the spec's fork-point snapshot
        // isolation.
        let merged_schema = self.schema.with_overlay(&scope.overlay());
        let forked_storage = Arc::new(
            self.storage
                .at_fork_with_schema(scope.clone(), merged_schema.clone()),
        );

        let prop_manager = Arc::new(PropertyManager::with_plugin_registry(
            forked_storage.clone(),
            merged_schema.clone(),
            self.properties.cache_size(),
            self.plugin_registry.clone(),
        ));

        // Deep-copy the rule registry so fork-local rule registrations
        // do not bleed into primary. Mirrors today's `Session::clone`
        // semantics for `rule_registry` (`session.rs:189`).
        let rule_registry = {
            let primary = self
                .locy_rule_registry
                .read()
                .map_err(|e| UniError::Internal(anyhow::anyhow!("rule_registry poisoned: {e}")))?;
            Arc::new(std::sync::RwLock::new(primary.clone()))
        };

        // Phase 2 Day 4: build a fork-scoped Writer so that
        // `forked.tx().commit()` can land mutations on the fork's
        // branches. The Writer uses a per-fork IdAllocator (Day 3),
        // a per-fork WAL stream (Day 5), and the fork-scoped storage's
        // BranchedBackend (Day 2). User writes are still gated at
        // `Session::tx()` until Day 7.
        let forked_writer = uni_store::fork::writer_factory::new_for_fork(
            forked_storage.clone(),
            merged_schema.clone(),
            &scope.fork_id(),
            // Bootstrap the fork's MVCC version floor to the parent's
            // fork-point HWM so in-tx fork reads see inherited rows. WAL
            // replay below advances the counter for the fork's own writes.
            scope.fork_info().fork_point_version_hwm,
            self.config.clone(),
        )
        .await
        .map_err(UniError::Internal)?;

        // Phase 2 Day 6: replay any persisted WAL entries for this
        // fork into the freshly-built L0. Without this, a process
        // restart would silently drop committed-but-not-yet-flushed
        // fork mutations.
        //
        // Gate replay on the fork's own persisted `wal_high_water_mark`
        // (review M2). The fork-scoped SnapshotManager (review C1) records
        // it at each fork flush under `catalog/forks/{fork_id}/latest`; a
        // crash between the durable branch write and complete WAL truncation
        // would otherwise replay already-flushed segments from 0 and
        // double-apply them. A fork that has never flushed (or a pre-fix
        // on-disk fork) has no per-fork snapshot, so we fall back to 0 —
        // correct, since nothing has been moved out of the WAL yet.
        let fork_wal_hwm = forked_storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await
            .map_err(UniError::Internal)?
            .map(|s| s.wal_high_water_mark)
            .unwrap_or(0);
        let replayed = forked_writer
            .replay_wal(fork_wal_hwm)
            .await
            .map_err(UniError::Internal)?;
        if replayed > 0 {
            tracing::info!(
                fork_id = %scope.fork_id(),
                replayed,
                "fork WAL replay restored persisted mutations into L0"
            );
        }

        let forked_writer_arc = Arc::new(forked_writer);
        let executor_template = build_executor_template(
            forked_storage.clone(),
            self.config.clone(),
            Some(forked_writer_arc.clone()),
            self.xervo_runtime.clone(),
            self.procedure_registry.clone(),
            prop_manager.clone(),
            self.df_session_template.clone(),
        );
        Ok(self.derived_clone(
            forked_storage,
            merged_schema,
            prop_manager,
            Some(forked_writer_arc),
            rule_registry,
            executor_template,
        ))
    }
}

impl Uni {
    /// Borrow this instance's background-job scheduler host.
    ///
    /// The host owns a [`uni_plugin::scheduler::Scheduler`] primitive
    /// driven by a tokio loop spawned at `Uni::build` time. The
    /// preferred Rust entry point is [`Uni::periodic_schedule`], which
    /// routes through the host's `SchedulerControl` impl so the
    /// schedule kind is captured by the durable persistence backend
    /// and survives restart:
    ///
    /// ```no_run
    /// # async fn ex(db: uni_db::Uni) {
    /// use std::time::Duration;
    /// use uni_plugin::QName;
    /// use uni_plugin::traits::background::Schedule;
    ///
    /// db.periodic_schedule(
    ///     QName::new("myorg", "nightly"),
    ///     Schedule::Periodic(Duration::from_secs(86_400)),
    /// );
    /// # }
    /// ```
    ///
    /// The job's [`BackgroundJobProvider`](
    /// uni_plugin::traits::background::BackgroundJobProvider) must
    /// have been registered into the [`uni_plugin::PluginRegistry`]
    /// (via `PluginRegistrar::background_job`) before its qname can
    /// be scheduled.
    #[must_use]
    pub fn scheduler_host(&self) -> &Arc<crate::scheduler::SchedulerHost> {
        &self.inner.scheduler_host
    }

    /// Register a background job to fire on `schedule`.
    ///
    /// This is the Rust analogue of `CALL uni.periodic.schedule(...)`
    /// — the Cypher wrapper procedure registers via this same path.
    /// The job's [`BackgroundJobProvider`](
    /// uni_plugin::traits::background::BackgroundJobProvider) must
    /// already be registered in the [`uni_plugin::PluginRegistry`]
    /// (via `PluginRegistrar::background_job` during plugin
    /// registration); otherwise the scheduler driver logs a warning
    /// on each tick that `id` is due.
    pub fn periodic_schedule(
        &self,
        id: uni_plugin::QName,
        schedule: uni_plugin::traits::background::Schedule,
    ) {
        // Route through the `SchedulerHost`'s `SchedulerControl` impl
        // (not the bare `Scheduler`) so the persistence layer captures
        // the schedule kind for restart durability.
        <crate::scheduler::SchedulerHost as uni_plugin::scheduler::SchedulerControl>::add_scheduled_job(
            &self.inner.scheduler_host,
            id,
            schedule,
        );
    }

    /// Cancel a scheduled job. Returns `true` if a job with this id
    /// was registered; `false` otherwise. Rust analogue of
    /// `CALL uni.periodic.cancel(...)`.
    pub fn periodic_cancel(&self, id: &uni_plugin::QName) -> bool {
        self.inner.scheduler_host.scheduler().cancel(id)
    }

    /// Snapshot every known job and its current lifecycle state.
    /// Rust analogue of `CALL uni.periodic.list()`.
    #[must_use]
    pub fn periodic_list(&self) -> Vec<uni_plugin::scheduler::SchedulerJobRecord> {
        self.inner.scheduler_host.scheduler().list()
    }

    /// Open or create a database at the given path.
    ///
    /// If the database does not exist, it will be created.
    ///
    /// # Arguments
    ///
    /// * `uri` - Local path or object store URI.
    ///
    /// # Returns
    ///
    /// A [`UniBuilder`] to configure and build the database instance.
    pub fn open(uri: impl Into<String>) -> UniBuilder {
        UniBuilder::new(uri.into())
    }

    /// Open an existing database at the given path. Fails if it does not exist.
    pub fn open_existing(uri: impl Into<String>) -> UniBuilder {
        let mut builder = UniBuilder::new(uri.into());
        builder.create_if_missing = false;
        builder
    }

    /// Create a new database at the given path. Fails if it already exists.
    pub fn create(uri: impl Into<String>) -> UniBuilder {
        let mut builder = UniBuilder::new(uri.into());
        builder.fail_if_exists = true;
        builder
    }

    /// Create a temporary database that is deleted when dropped.
    ///
    /// Useful for tests and short-lived processing.
    /// The underlying directory is automatically cleaned up when the `Uni` is dropped.
    pub fn temporary() -> UniBuilder {
        let temp_dir = tempfile::Builder::new()
            .prefix("uni_mem_")
            .tempdir()
            .expect("failed to create temporary directory");
        let uri = temp_dir.path().to_string_lossy().to_string();
        let mut builder = UniBuilder::new(uri);
        builder.temp_dir = Some(temp_dir);
        builder
    }

    /// Open an in-memory database (alias for temporary).
    pub fn in_memory() -> UniBuilder {
        Self::temporary()
    }

    // ── Session Factory (primary entry point for data access) ────────

    /// Create a new Session for data access.
    ///
    /// Sessions are cheap, synchronous, and infallible. All reads go through
    /// sessions, and sessions are the factory for transactions (writes).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use uni_db::Uni;
    /// # async fn example(db: &Uni) -> uni_db::Result<()> {
    /// let session = db.session();
    /// let rows = session.query("MATCH (n) RETURN n LIMIT 10").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn session(&self) -> session::Session {
        session::Session::new(self.inner.clone())
    }

    /// Open a session authenticated as the given credentials (M5i).
    ///
    /// Iterates the registered [`uni_plugin::traits::connector::AuthProvider`]s
    /// in registration order; the first provider whose `scheme()`
    /// matches the credential type is asked to `authenticate`. On
    /// success, the resulting [`uni_plugin::traits::connector::Principal`]
    /// is attached to the session and propagates into downstream
    /// authorization checks.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::AuthenticationFailed`] when no registered
    /// provider matches the credential scheme or the matched
    /// provider's `authenticate` returned an error.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use uni_plugin::traits::connector::Credentials;
    /// let creds = Credentials::Basic {
    ///     username: "alice".into(),
    ///     password: "hunter2".into(),
    /// };
    /// let session = db.session_with_credentials(creds)?;
    /// ```
    pub fn session_with_credentials(
        &self,
        creds: uni_plugin::traits::connector::Credentials,
    ) -> Result<session::Session> {
        let scheme = match &creds {
            uni_plugin::traits::connector::Credentials::Basic { .. } => "basic",
            uni_plugin::traits::connector::Credentials::Bearer(_) => "bearer",
            uni_plugin::traits::connector::Credentials::MtlsCert(_) => "mtls",
        };
        let providers = self.inner.plugin_registry.auth_providers();
        // Try each matching provider in registration order; succeed on
        // the first one that authenticates. This lets a host stack its
        // own provider alongside the built-in one — either may hold the
        // credentials. `matched_any` distinguishes "no provider for this
        // scheme" from "providers were tried and all rejected".
        let mut matched_any = false;
        let mut last_error: Option<String> = None;
        for provider in providers.iter().filter(|p| p.scheme() == scheme) {
            matched_any = true;
            match provider.authenticate(&creds) {
                Ok(principal) => {
                    return Ok(self.session().with_principal(Arc::new(principal)));
                }
                Err(e) => {
                    last_error = Some(e.0);
                }
            }
        }
        if !matched_any {
            return Err(UniError::AuthenticationFailed {
                reason: format!("no AuthProvider registered for scheme `{scheme}`"),
            });
        }
        Err(UniError::AuthenticationFailed {
            reason: last_error.unwrap_or_else(|| "all matching providers rejected".to_owned()),
        })
    }

    /// List every active fork on this database.
    ///
    /// Returns metadata snapshots — see [`uni_common::core::fork::ForkInfo`].
    /// Pending or Tombstoned entries are omitted; recovery resumes them
    /// on the next [`Uni::open`].
    pub async fn list_forks(&self) -> Vec<uni_common::core::fork::ForkInfo> {
        self.inner.fork_registry.list_active().await
    }

    /// Look up a fork by name.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::ForkNotFound`] when no fork has this name.
    pub async fn fork_info(&self, name: &str) -> Result<uni_common::core::fork::ForkInfo> {
        self.inner.fork_registry.get(name).await
    }

    /// Wait (bounded) for a fork's `holder_count` to drain to zero,
    /// returning the final count.
    ///
    /// Under async-flush a fork's `FlushCoordinator` finalizer is an
    /// orphan tokio task that transitively pins the fork's
    /// `ForkHolderGuard`, so `holder_count_for` can sit briefly above
    /// zero after the last session drops. This polls up to 100 times,
    /// yielding to the runtime for the first 20 iterations (to let
    /// pending destructors run) then sleeping 10 ms thereafter. Shared
    /// by `drop_fork` (ignores the count) and `drop_fork_cascade` (uses
    /// it to build the blocker message).
    async fn wait_for_holders_drained(&self, fork_id: ForkId) -> usize {
        let mut holders = self.inner.fork_registry.holder_count_for(fork_id).await;
        if holders == 0 {
            return 0;
        }
        for i in 0..100 {
            if i < 20 {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            holders = self.inner.fork_registry.holder_count_for(fork_id).await;
            if holders == 0 {
                break;
            }
        }
        holders
    }

    /// Drop a fork by name (Phase 1: read-only forks only).
    ///
    /// Runs the full drop 2PC: tombstone → delete branches → clear
    /// registry → delete tombstone + schema overlay. Recovery resumes
    /// from any in-progress state if the process dies mid-drop.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] when the name is unknown.
    /// - [`UniError::ForkInUse`] when forked sessions are still live
    ///   on this fork. Drop again after they're released.
    ///
    /// # Examples
    ///
    /// ```
    /// # use uni_db::Uni;
    /// # async fn example() -> uni_db::Result<()> {
    /// let db = Uni::in_memory().build().await?;
    /// let session = db.session();
    /// let forked = session.fork("ephemeral").await?;
    /// drop(forked);
    /// db.drop_fork("ephemeral").await?;
    /// # db.shutdown().await
    /// # }
    /// ```
    pub async fn drop_fork(&self, name: &str) -> Result<()> {
        // Hold the per-name lock for the whole drop sequence. `fork(name).build()`
        // holds the same lock for its entire open-or-create flow (api/fork.rs),
        // so create/open and drop are mutually exclusive per name: a concurrent
        // builder can never observe `Active` + register a holder while we are
        // tombstoning and force-deleting the Lance branches (review H2/M9). The
        // cascade path (`drop_fork_cascade`) drops each node through here, so it
        // inherits the same per-node serialization.
        let name_lock = self.inner.fork_registry.name_lock(name).await;
        let _name_guard = name_lock.lock().await;

        // Phase 2 Day 11: surface in-flight transactions before the
        // registry transitions to Tombstoned. The `ForkInUse` check in
        // `begin_drop` catches *session* holders; this catches the
        // case where a session is alive AND has at least one alive
        // `Transaction` on the fork's UniInner. We track this via an
        // `inflight_tx_count` AtomicUsize that `Transaction::new`
        // increments and `Transaction::drop` decrements unconditionally
        // (so commit/rollback/silent-drop all converge to zero).
        let preview = self.inner.fork_registry.get(name).await?;

        // Phase 3: refuse to drop a parent that still has children.
        // Callers should use `drop_fork_cascade` to remove the subtree.
        let children = self.inner.fork_registry.list_children(preview.id).await;
        if !children.is_empty() {
            return Err(UniError::ForkHasChildren {
                name: name.to_string(),
                children: children.into_iter().map(|c| c.name).collect(),
            });
        }

        if let Some(weak) = self
            .inner
            .fork_inners
            .get(&preview.id)
            .map(|e| e.value().clone())
            && let Some(inner) = weak.upgrade()
        {
            if inner.inflight_tx_count.load(Ordering::Acquire) > 0 {
                return Err(UniError::ForkInflightTx {
                    name: name.to_string(),
                });
            }
            // Drain any pending async flushes, THEN shut down the
            // coordinator so its finalizer task exits. Both steps are
            // required: drain waits for in-flight streams to finalize
            // (pending_count → 0), but the finalizer task itself stays
            // parked at submit_rx.recv() holding Arc<StorageManager>.
            // Storage pins Arc<ForkScope> (manager.rs:364), which holds
            // the ForkHolderGuard. Without the explicit shutdown, the
            // task lives until Writer/Coordinator drop transitively,
            // which never happens before drop_fork's holder-count check.
            // See async-flush plan §3.9 / L8.
            if let Some(writer) = inner.writer.as_ref()
                && let Some(coord) = writer.flush_coordinator()
            {
                if coord
                    .drain(self.inner.config.drop_fork_drain_timeout)
                    .await
                    .is_err()
                {
                    return Err(UniError::PendingFlushTimeout {
                        name: name.to_string(),
                    });
                }
                // Drop submit_tx + await finalizer task exit so
                // Arc<storage> (+ Arc<ForkScope>) drops on this writer.
                coord.shutdown().await;
            }
            // Drop our local Arc clone of `inner` so the only strong
            // ref to fork's UniInner is gone. ForkHolderGuard drops
            // when ForkScope drops, which happens once storage Arc → 0.
            drop(inner);
        }
        // Wait for the fork's holder_count to drop to zero. Under async-
        // flush, the fork's FlushCoordinator's finalizer task is an
        // orphan tokio task that holds Arc<StorageManager> via
        // SharedFlushCtx. Storage pins Arc<ForkScope> which holds the
        // ForkHolderGuard. When the fork's Session drops at scope-end,
        // UniInner drops (so the `weak.upgrade()` above returns None
        // and we never enter the drain/shutdown branch), but the orphan
        // finalizer task is STILL alive in tokio's queue holding the
        // chain that ultimately pins the holder counter at 1.
        //
        // The fix is to wait: the orphan task exits the moment its
        // mpsc receiver sees a closed channel, which happens when
        // FlushCoordinator drops submit_tx in its own Drop. That Drop
        // ran transitively when UniInner dropped, but the spawned
        // task's destructor may still be pending in the scheduler
        // queue. yield_now repeatedly lets the runtime work through
        // those destructors before we check holder_count.
        self.wait_for_holders_drained(preview.id).await;
        let info = self.inner.fork_registry.begin_drop(name).await?;
        // Phase 2 Day 8: evict the cached `Weak<UniInner>` (if any)
        // before deleting branches. The registry has already
        // transitioned the fork to Tombstoned, so concurrent
        // `fork(name)` calls now error out before reaching the cache;
        // this eviction is purely cleanup so the map doesn't accumulate
        // dead Weak entries across the lifetime of the database.
        self.inner.fork_inners.remove(&info.id);
        // Step 3: walk branches and force-delete each. Track failures: if any
        // branch delete fails we must NOT finish_drop, because finish_drop
        // deletes the recovery tombstone — the only anchor that lets boot-time
        // recovery retry the deletion. Dropping it would orphan the surviving
        // branches permanently (review M3). Leave the fork Tombstoned instead.
        let storage_uri = self.inner.storage.base_uri().to_string();
        let mut delete_failure: Option<String> = None;
        for (dataset, branch) in &info.datasets {
            let dataset_uri = dataset_uri(&storage_uri, dataset);
            if let Err(e) =
                uni_store::backend::lance_branch::delete_branch(&dataset_uri, branch).await
            {
                tracing::warn!(
                    dataset = %dataset,
                    branch = %branch,
                    "delete_branch during drop_fork failed: {e}"
                );
                delete_failure = Some(format!("{dataset}/{branch}: {e}"));
            }
        }
        if let Some(detail) = delete_failure {
            // Tombstone + registry entry remain; `recover_forks` will retry
            // delete_all_branches + finish_drop on the next open.
            return Err(UniError::ForkLifecycle {
                name: name.to_string(),
                stage: "delete_branch",
                source: format!(
                    "branch delete failed; fork left Tombstoned for recovery ({detail})"
                )
                .into(),
            });
        }
        // Step 4 + 5: clear the registry entry, delete tombstone + schema
        // overlay files.
        self.inner.fork_registry.finish_drop(&info).await?;
        // Step 6: remove the fork's storage-side artifacts (WAL, id allocator,
        // fork-scoped snapshot manifests) so a dropped fork leaves no disk
        // residue (review H3). On the storage object store, not the registry's.
        uni_store::fork::delete_fork_artifacts(&self.inner.storage.store(), &info.id).await;
        Ok(())
    }

    /// Drop a fork and every descendant in its subtree (Phase 3).
    ///
    /// Pre-validates the entire subtree before tombstoning anything:
    /// every node must pass the same `ForkInUse` + `ForkInflightTx`
    /// checks `drop_fork` applies for a single node. On any blocker
    /// the call errors with [`UniError::ForkSubtreeInUse`] and no
    /// branch is deleted. Once validation passes, the cascade drops
    /// each node deepest-first via the single-fork `drop_fork` path,
    /// so a crash mid-cascade resumes cleanly through existing
    /// tombstone recovery.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] if `name` is unknown.
    /// - [`UniError::ForkSubtreeInUse`] if any node in the subtree has
    ///   live sessions or open transactions.
    pub async fn drop_fork_cascade(&self, name: &str) -> Result<()> {
        // 1. Resolve the root and walk descendants depth-first.
        let root = self.inner.fork_registry.get(name).await?;
        let mut order: Vec<uni_common::core::fork::ForkInfo> = Vec::new();
        let mut stack = vec![root.clone()];
        while let Some(node) = stack.pop() {
            let kids = self.inner.fork_registry.list_children(node.id).await;
            for k in &kids {
                stack.push(k.clone());
            }
            order.push(node);
        }
        // `order` is roots-first by construction. Reversing it yields
        // deepest-first, which is the order we drop in.
        order.reverse();

        // 2. Pre-validate every node. Aggregate blockers; refuse before
        // tombstoning if any node is held or has in-flight tx.
        //
        // Under async-flush, holder_count may transiently sit at 1 for a
        // brief window after the last session drops, while orphan
        // FlushCoordinator finalizer tasks finish exiting (they hold
        // Arc<storage> → Arc<ForkScope> → ForkHolderGuard). Apply the
        // same bounded wait we use in `drop_fork`.
        let mut blockers: Vec<String> = Vec::new();
        for node in &order {
            if let Some(weak) = self
                .inner
                .fork_inners
                .get(&node.id)
                .map(|e| e.value().clone())
                && let Some(inner) = weak.upgrade()
                && inner.inflight_tx_count.load(Ordering::Acquire) > 0
            {
                blockers.push(format!("{}: in-flight tx", node.name));
                continue;
            }
            // Wait briefly for orphan finalizer tasks to exit.
            let holders = self.wait_for_holders_drained(node.id).await;
            if holders > 0 {
                blockers.push(format!("{}: {} live session(s)", node.name, holders));
            }
        }
        if !blockers.is_empty() {
            return Err(UniError::ForkSubtreeInUse { blockers });
        }

        // 3. Drop deepest-first using the single-fork path. Each call
        // re-checks holders/inflight inside `drop_fork`, which is
        // belt-and-braces against a session opening between validation
        // and drop; that race surfaces as a normal ForkInUse error.
        for node in order {
            self.drop_fork(&node.name).await?;
        }
        Ok(())
    }

    /// Structural diff between two forks.
    ///
    /// Returns the delta that would turn `a` into `b`: `added` rows
    /// are present in `b` only, `deleted` in `a` only. Identity is
    /// content-addressed UID (Phase 6b) for vertices and an
    /// edge-content UID (Phase 7d) for edges, so the diff is correct
    /// even between two unrelated forks that happen to have rolled
    /// the same VIDs.
    ///
    /// `diff(a, b).invert() == diff(b, a)` by construction — see
    /// [`fork_diff::ForkDiff::invert`].
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] when either name is unknown.
    /// - Any error from opening a fork session on either side.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use uni_db::{DataType, Uni};
    /// # async fn example() -> uni_db::Result<()> {
    /// let db = Uni::in_memory().build().await?;
    /// db.schema().label("Person").property("name", DataType::String).apply().await?;
    /// let primary = db.session();
    /// {
    ///     let a = primary.fork("scenario_a").await?;
    ///     let tx = a.tx().await?;
    ///     tx.execute("CREATE (:Person {name: 'A-only'})").await?;
    ///     tx.commit().await?;
    /// }
    /// {
    ///     let b = primary.fork("scenario_b").await?;
    ///     let tx = b.tx().await?;
    ///     tx.execute("CREATE (:Person {name: 'B-only'})").await?;
    ///     tx.commit().await?;
    /// }
    /// let diff = db.diff_forks("scenario_a", "scenario_b").await?;
    /// assert_eq!(diff.vertices.added.len(), 1);   // B-only
    /// assert_eq!(diff.vertices.deleted.len(), 1); // A-only
    /// # db.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn diff_forks(&self, a: &str, b: &str) -> Result<fork_diff::ForkDiff> {
        let primary = self.session();
        let sess_a = primary.fork(a).await?;
        let sess_b = primary.fork(b).await?;
        fork_diff::compute_diff(&sess_a, &sess_b).await
    }

    /// Structural diff between a fork and primary.
    ///
    /// Equivalent to `diff(primary, fork)`: rows the fork has added
    /// since the fork point appear in `added`; rows it has dropped
    /// appear in `deleted`. Identity is content-addressed UID
    /// (vertices) / edge-content UID (edges), so unrelated forks
    /// pair correctly. See [`fork_diff::ForkDiff`] for the data
    /// model.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] when the fork name is unknown.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use uni_db::{DataType, Uni};
    /// # async fn example() -> uni_db::Result<()> {
    /// let db = Uni::in_memory().build().await?;
    /// db.schema().label("Person").property("name", DataType::String).apply().await?;
    /// let primary = db.session();
    /// {
    ///     let fork = primary.fork("audit").await?;
    ///     let tx = fork.tx().await?;
    ///     tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    ///     tx.commit().await?;
    /// }
    /// let diff = db.diff_fork_primary("audit").await?;
    /// assert_eq!(diff.vertices.added.len(), 1); // Bob
    /// # db.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn diff_fork_primary(&self, fork_name: &str) -> Result<fork_diff::ForkDiff> {
        let primary = self.session();
        let sess_fork = primary.fork(fork_name).await?;
        fork_diff::compute_diff(&primary, &sess_fork).await
    }

    /// Promote matched fork rows onto primary.
    ///
    /// For each [`fork_diff::PromotePattern`] in `patterns`:
    ///
    /// - **`PromotePattern::Vertex`** — scan the fork for vertices
    ///   with the given label, compute a content-derived UID for
    ///   each match, skip rows that already exist on primary by UID,
    ///   bulk-insert the rest.
    /// - **`PromotePattern::Edge`** — scan the fork for edges of the
    ///   given type, resolve endpoint UIDs against primary, skip
    ///   rows whose endpoints aren't on primary (counted in
    ///   [`fork_diff::PromoteReport::edges_skipped_no_endpoint`]),
    ///   dedup against existing parallel edges by content UID
    ///   (Phase 7d multi-edge identity), and bulk-insert the rest.
    ///
    /// All inserts run inside one primary-targeted transaction that
    /// commits on success. Mixing vertex and edge patterns in one
    /// call is supported — endpoints inserted by an earlier vertex
    /// pattern are visible to a subsequent edge pattern via an
    /// in-memory cache.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] when the fork name is unknown.
    /// - [`UniError::LabelNotFound`] when a vertex pattern targets a
    ///   label that does not exist on primary.
    /// - [`UniError::EdgeTypeNotFound`] when an edge pattern targets
    ///   an edge type that does not exist on primary.
    /// - Any error from the primary write path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use uni_db::{DataType, PromotePattern, Uni};
    /// # async fn example() -> uni_db::Result<()> {
    /// let db = Uni::in_memory().build().await?;
    /// db.schema().label("Person").property("name", DataType::String).apply().await?;
    /// let primary = db.session();
    /// {
    ///     let fork = primary.fork("publish").await?;
    ///     let tx = fork.tx().await?;
    ///     tx.execute("CREATE (:Person {name: 'NewKid'})").await?;
    ///     tx.commit().await?;
    /// }
    /// let report = db.promote_from_fork(
    ///     "publish",
    ///     &[PromotePattern::label("Person")],
    /// ).await?;
    /// assert!(report.vertices_inserted >= 1);
    /// # db.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn promote_from_fork(
        &self,
        fork_name: &str,
        patterns: &[fork_diff::PromotePattern],
    ) -> Result<fork_diff::PromoteReport> {
        self.promote_from_fork_with_options(
            fork_name,
            patterns,
            &fork_diff::PromoteOptions::default(),
        )
        .await
    }

    /// Promote fork changes to primary with explicit merge [`options`].
    ///
    /// Same as [`Self::promote_from_fork`] but lets the caller enable
    /// ext_id-keyed upsert (`PromoteOptions::with_upsert`): a fork edit to
    /// a vertex that already exists on primary is applied in place instead
    /// of inserting a twin. The default options reproduce the insert-only
    /// behavior of `promote_from_fork`, so existing callers are unaffected.
    ///
    /// # Errors
    /// Returns [`UniError::LabelNotFound`] / [`UniError::EdgeTypeNotFound`]
    /// when a pattern targets a label or edge type absent on primary, or
    /// any error from the underlying fork flush, transaction, or commit.
    ///
    /// [`options`]: fork_diff::PromoteOptions
    pub async fn promote_from_fork_with_options(
        &self,
        fork_name: &str,
        patterns: &[fork_diff::PromotePattern],
        options: &fork_diff::PromoteOptions,
    ) -> Result<fork_diff::PromoteReport> {
        let primary = self.session();
        let fork = primary.fork(fork_name).await?;
        // Persist any pending tx commits on the fork to Lance so the
        // promote engine's reads see them. Without this, edges
        // committed via a now-dropped fork session may not be visible
        // to the fresh fork session we just opened.
        fork.flush().await?;
        // Ensure every pattern's target (label or edge type) exists on
        // primary; surfacing a clear error is preferable to letting
        // bulk_insert_* fail mid-flight.
        let primary_schema = self.inner.schema.schema();
        for pat in patterns {
            if pat.is_edge() {
                let edge_type = pat.edge_type_name();
                if !primary_schema.edge_types.contains_key(edge_type) {
                    return Err(UniError::EdgeTypeNotFound {
                        edge_type: edge_type.to_string(),
                    });
                }
            } else {
                let label = pat.label_name();
                if !primary_schema.labels.contains_key(label) {
                    return Err(UniError::LabelNotFound {
                        label: label.to_string(),
                    });
                }
            }
        }
        let primary_tx = primary.tx().await?;
        let report =
            fork_diff::run_promote(&fork, &primary, &primary_tx, patterns, options).await?;
        primary_tx.commit().await?;
        Ok(report)
    }

    /// Tag a fork with a Lance tag (Phase 4a).
    ///
    /// Creates one tag per dataset the fork has branched, named
    /// `fork_{tag}_{dataset}`. Lance tags are GC-exempt — the tagged
    /// versions survive compaction's retention sweep — so a tagged
    /// fork's state is preserved on disk even after the fork itself
    /// is dropped (cascade or otherwise). Useful for audit hold,
    /// regulatory snapshots, or named pre-publish checkpoints.
    ///
    /// The tag pins the branch's *current* version: subsequent fork
    /// writes do not "follow" the tag.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] if the fork is unknown.
    /// - [`UniError::ForkLifecycle`] (stage = `tag`) on Lance failures
    ///   (tag-name conflict, IO).
    pub async fn tag_fork(&self, fork_name: &str, tag: &str) -> Result<()> {
        let info = self.inner.fork_registry.get(fork_name).await?;
        let storage_uri = self.inner.storage.base_uri().to_string();
        for (dataset, branch) in &info.datasets {
            let dataset_uri = dataset_uri(&storage_uri, dataset);
            let lance_tag = format!("fork_{tag}_{dataset}");
            uni_store::backend::lance_branch::create_tag(&dataset_uri, &lance_tag, branch)
                .await
                .map_err(|e| UniError::ForkLifecycle {
                    name: fork_name.to_string(),
                    stage: "tag",
                    source: e.into(),
                })?;
        }
        Ok(())
    }

    /// Remove a tag previously applied via [`Self::tag_fork`] (Phase 4a).
    /// Idempotent per dataset — missing tags are treated as success so
    /// partial cleanup retries are safe.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] if the fork is unknown.
    /// - [`UniError::ForkLifecycle`] (stage = `untag`) on Lance failures.
    pub async fn untag_fork(&self, fork_name: &str, tag: &str) -> Result<()> {
        let info = self.inner.fork_registry.get(fork_name).await?;
        let storage_uri = self.inner.storage.base_uri().to_string();
        for dataset in info.datasets.keys() {
            let dataset_uri = dataset_uri(&storage_uri, dataset);
            let lance_tag = format!("fork_{tag}_{dataset}");
            uni_store::backend::lance_branch::delete_tag(&dataset_uri, &lance_tag)
                .await
                .map_err(|e| UniError::ForkLifecycle {
                    name: fork_name.to_string(),
                    stage: "untag",
                    source: e.into(),
                })?;
        }
        Ok(())
    }

    /// List the unique tag names applied to this fork (Phase 4a).
    ///
    /// A fork's tag is stored as one Lance tag per dataset under the
    /// namespace `fork_{tag}_{dataset}`. This method enumerates the
    /// distinct `tag` values present on at least one of the fork's
    /// branched datasets.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] if the fork is unknown.
    /// - [`UniError::ForkLifecycle`] (stage = `list_tags`) on Lance failures.
    pub async fn list_fork_tags(&self, fork_name: &str) -> Result<Vec<String>> {
        let info = self.inner.fork_registry.get(fork_name).await?;
        let storage_uri = self.inner.storage.base_uri().to_string();
        let mut tags: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for dataset in info.datasets.keys() {
            let dataset_uri = dataset_uri(&storage_uri, dataset);
            let suffix = format!("_{dataset}");
            let prefix = "fork_";
            let on_disk = uni_store::backend::lance_branch::list_tags(&dataset_uri)
                .await
                .map_err(|e| UniError::ForkLifecycle {
                    name: fork_name.to_string(),
                    stage: "list_tags",
                    source: e.into(),
                })?;
            for (name, _) in on_disk {
                if let Some(rest) = name.strip_prefix(prefix)
                    && let Some(tag) = rest.strip_suffix(&suffix)
                {
                    tags.insert(tag.to_string());
                }
            }
        }
        Ok(tags.into_iter().collect())
    }

    /// Create a session template builder for pre-configured session factories.
    ///
    /// Templates pre-compile Locy rules, bind parameters, and attach hooks
    /// once, then cheaply stamp out sessions per-request.
    pub fn session_template(&self) -> template::SessionTemplateBuilder {
        template::SessionTemplateBuilder::new(self.inner.clone())
    }

    // ── Database Metrics ──────────────────────────────────────────────

    /// Snapshot the database-level metrics.
    ///
    /// This is a cheap, synchronous read of cached atomic values.
    /// L0 metrics (`l0_mutation_count`, `l0_estimated_size_bytes`, `wal_lsn`)
    /// reflect the state as of the last successful commit.
    pub fn metrics(&self) -> DatabaseMetrics {
        let schema_version = self.inner.schema.schema().schema_version as u64;
        let compaction_status = self.inner.storage.compaction_status().unwrap_or_default();
        DatabaseMetrics {
            l0_mutation_count: self.inner.cached_l0_mutation_count.load(Ordering::Relaxed),
            l0_estimated_size_bytes: self.inner.cached_l0_estimated_size.load(Ordering::Relaxed),
            schema_version,
            uptime: self.inner.start_time.elapsed(),
            active_sessions: self.inner.active_session_count.load(Ordering::Relaxed),
            l1_run_count: compaction_status.l1_runs,
            write_throttle_pressure: ThrottlePressure::default(),
            compaction_status,
            wal_size_bytes: 0u64,
            wal_lsn: self.inner.cached_wal_lsn.load(Ordering::Relaxed),
            total_queries: self.inner.total_queries.load(Ordering::Relaxed),
            total_commits: self.inner.total_commits.load(Ordering::Relaxed),
        }
    }

    /// Returns the write lease configuration, if any.
    /// Write lease enforcement is Phase 2.
    pub fn write_lease(&self) -> Option<&multi_agent::WriteLease> {
        self.inner.write_lease.as_ref()
    }

    // ── Global Locy Rule Management ───────────────────────────────────

    /// Access the global rule registry for managing pre-compiled Locy rules.
    ///
    /// Rules registered here are cloned into every new Session.
    pub fn rules(&self) -> rule_registry::RuleRegistry<'_> {
        match &self.inner.locy_rule_persister {
            Some(persister) => rule_registry::RuleRegistry::with_persister(
                &self.inner.locy_rule_registry,
                persister,
            ),
            None => rule_registry::RuleRegistry::new(&self.inner.locy_rule_registry),
        }
    }

    // ── Configuration & Introspection ─────────────────────────────────

    /// Get configuration.
    pub fn config(&self) -> &UniConfig {
        &self.inner.config
    }

    /// Returns the procedure registry for registering test procedures.
    #[doc(hidden)]
    pub fn procedure_registry(&self) -> &Arc<uni_query::ProcedureRegistry> {
        &self.inner.procedure_registry
    }

    /// Returns the framework-wide [`uni_plugin::PluginRegistry`].
    ///
    /// Built once at `Uni::build()` time and populated with `BuiltinPlugin`
    /// (always) and `ApocCorePlugin` (when the `apoc-core` feature is on).
    /// Future user plugins added via [`Uni::add_plugin`] register into the
    /// same instance.
    pub fn plugin_registry(&self) -> &Arc<uni_plugin::PluginRegistry> {
        &self.inner.plugin_registry
    }

    /// Install a user plugin into this database's [`uni_plugin::PluginRegistry`].
    ///
    /// Runs the standard registrar dance: clone the plugin's
    /// [`uni_plugin::PluginManifest`], build a [`uni_plugin::PluginRegistrar`] scoped
    /// to the manifest's capability set, invoke
    /// [`uni_plugin::Plugin::register`], and commit the pending
    /// registrations atomically.
    ///
    /// This is the recommended replacement for the deprecated
    /// `Session::add_hook` / `Uni::add_hook` legacy API: callers wrap
    /// their legacy [`crate::api::hooks::SessionHook`] in a
    /// [`crate::api::hooks::BuiltinHookPlugin`] and pass it here.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::InvalidArgument`] if the plugin's
    /// `register()` fails or any pending registration collides with an
    /// existing qname.
    pub fn add_plugin<P: uni_plugin::Plugin>(&self, plugin: P) -> Result<()> {
        use uni_plugin::PluginRegistrar;
        use uni_plugin::lifecycle::{LifecycleState, PluginLifecycle};

        let plugin: Arc<dyn uni_plugin::Plugin> = Arc::new(plugin);
        let manifest = plugin.manifest();
        // Enforce the host signature policy. Default (Disabled) is a no-op;
        // RequireSigned rejects an unsigned manifest or an untrusted key.
        self.inner
            .plugin_trust
            .enforce(manifest)
            .map_err(plugin_err_to_uni)?;
        let plugin_id = manifest.id.clone();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(plugin_id.clone(), &caps, &self.inner.plugin_registry);
        plugin.register(&mut r).map_err(plugin_err_to_uni)?;
        r.commit_to_registry().map_err(plugin_err_to_uni)?;

        // Lifecycle: Loaded → Linked → Initialized → Active.
        let lifecycle = Arc::new(PluginLifecycle::new(plugin_id.clone()));
        lifecycle.set(LifecycleState::Active);
        self.inner.plugins.write().insert(
            plugin_id,
            UniPluginEntry {
                plugin,
                lifecycle,
                generation: 0,
            },
        );
        Ok(())
    }

    /// Snapshot every installed plugin's id, handle, and current state.
    ///
    /// Returns a vector ordered by the order plugins were inserted into
    /// the host's internal map (note: iteration order across the map is
    /// not stable across reloads — callers needing stable ordering
    /// should sort on `PluginId`).
    pub fn plugins(
        &self,
    ) -> Vec<(
        uni_plugin::PluginId,
        uni_plugin::PluginHandle,
        uni_plugin::lifecycle::LifecycleState,
    )> {
        self.inner
            .plugins
            .read()
            .iter()
            .map(|(id, entry)| {
                let handle = uni_plugin::PluginHandle::new(id.clone(), entry.generation);
                (id.clone(), handle, entry.lifecycle.state())
            })
            .collect()
    }

    /// Look up a plugin handle by id.
    ///
    /// Returns `None` when the id is not installed (or has been removed
    /// via [`Self::remove_plugin`]).
    #[must_use]
    pub fn plugin(&self, id: &uni_plugin::PluginId) -> Option<uni_plugin::PluginHandle> {
        self.inner
            .plugins
            .read()
            .get(id)
            .map(|entry| uni_plugin::PluginHandle::new(id.clone(), entry.generation))
    }

    /// Remove an installed plugin, draining in-flight references first.
    ///
    /// Implements the §11.2 cutover for the removal direction: snapshot
    /// the old plugin's per-kind state, evict its registry footprint,
    /// drive `EpochFencedReload::begin_drain → wait_for_drain →
    /// finalize`, then run the plugin's `shutdown()` callback and drop
    /// the entry.
    ///
    /// # Errors
    ///
    /// - [`UniError::InvalidArgument`] if the handle's id is not
    ///   installed or the generation does not match (stale handle).
    /// - [`UniError::Internal`] if the drain times out (default 30 s).
    pub fn remove_plugin(&self, handle: &uni_plugin::PluginHandle) -> Result<()> {
        let _outcome = self.reload_internal(handle, None)?;
        Ok(())
    }

    /// Reload a plugin, swapping in a new instance under the same id.
    ///
    /// Implements the §11.2 epoch-fenced cutover. Drains in-flight
    /// references to the old instance, runs the per-kind reload
    /// discipline (CRDT schema-compat check, logical-type contract
    /// check), evicts the old registry footprint, runs the new
    /// plugin's `register()` + `init()`, and bumps the handle's
    /// generation counter on success.
    ///
    /// # Errors
    ///
    /// - [`UniError::InvalidArgument`] if the handle is stale or the
    ///   id is not installed.
    /// - [`UniError::InvalidArgument`] if a per-kind compat check or
    ///   the new plugin's `register()` rejects the swap (the old
    ///   plugin remains installed and active on rejection).
    pub fn reload<P: uni_plugin::Plugin>(
        &self,
        handle: &uni_plugin::PluginHandle,
        new_plugin: P,
    ) -> Result<uni_plugin::PluginHandle> {
        let new_arc: Arc<dyn uni_plugin::Plugin> = Arc::new(new_plugin);
        self.reload_internal(handle, Some(new_arc))
    }

    fn reload_internal(
        &self,
        handle: &uni_plugin::PluginHandle,
        new_plugin: Option<Arc<dyn uni_plugin::Plugin>>,
    ) -> Result<uni_plugin::PluginHandle> {
        use uni_plugin::PluginRegistrar;
        use uni_plugin::lifecycle::{EpochFencedReload, LifecycleState, PluginLifecycle};
        use uni_plugin::reload::{OldProviders, ReloadDispatcher};

        let plugin_id = handle.id.clone();

        // Step 1: validate handle + extract the live plugin / lifecycle /
        // generation. We do **not** keep a clone of the entry around —
        // the only `Arc<PluginLifecycle>` clones we want at drain time
        // are (a) the driver's and (b) whichever in-flight captures
        // still hold one. Holding extra clones here would inflate the
        // strong-count and force the drain wait to time out.
        let (old_plugin, old_lifecycle, old_generation) = {
            let map = self.inner.plugins.read();
            let entry = map
                .get(&plugin_id)
                .ok_or_else(|| UniError::InvalidArgument {
                    arg: "handle".to_owned(),
                    message: format!("plugin {plugin_id} not installed"),
                })?;
            if entry.generation != handle.generation {
                return Err(UniError::InvalidArgument {
                    arg: "handle".to_owned(),
                    message: format!(
                        "stale handle for plugin {plugin_id}: expected generation {}, got {}",
                        entry.generation, handle.generation
                    ),
                });
            }
            (
                Arc::clone(&entry.plugin),
                Arc::clone(&entry.lifecycle),
                entry.generation,
            )
        };

        // Step 2: snapshot the per-kind providers the old plugin owned
        // for the dispatcher's schema-compat check.
        let snapshot = self
            .inner
            .plugin_registry
            .iter_for_plugin(&plugin_id)
            .unwrap_or_default();
        let mut old_providers = OldProviders::default();
        for kind in &snapshot.crdt_kinds {
            if let Some(p) = self.inner.plugin_registry.crdt_kind(kind) {
                old_providers.crdt_kinds.insert(kind.clone(), p);
            }
        }

        // Step 3: begin drain on the old lifecycle.
        let driver = EpochFencedReload::new(Arc::clone(&old_lifecycle));
        driver
            .begin_drain()
            .map_err(|e| UniError::Internal(anyhow::anyhow!("reload drain begin: {e}")))?;

        // Step 4: evict the old plugin's registry footprint.
        self.inner.plugin_registry.remove_plugin(&plugin_id);

        // Step 5: if reloading, run the new plugin's registrar dance.
        if let Some(new) = new_plugin.as_ref() {
            let manifest = new.manifest();
            if manifest.id != plugin_id {
                let _ = self.replay_register_for(&old_plugin);
                old_lifecycle.set(LifecycleState::Active);
                return Err(UniError::InvalidArgument {
                    arg: "new_plugin".to_owned(),
                    message: format!(
                        "reload plugin id mismatch: handle is {plugin_id}, new plugin id is {}",
                        manifest.id
                    ),
                });
            }
            let caps = manifest.capabilities.clone();
            let mut r = PluginRegistrar::new(plugin_id.clone(), &caps, &self.inner.plugin_registry);
            new.register(&mut r).map_err(plugin_err_to_uni)?;
            r.commit_to_registry().map_err(plugin_err_to_uni)?;

            // Step 6: per-kind compat checks on the now-committed new
            // registry. Compat failures abort by re-replaying the old
            // plugin's registrations.
            let dispatcher = ReloadDispatcher::new(&snapshot, &self.inner.plugin_registry);
            if let Err(e) = dispatcher.check_compat(&old_providers) {
                self.inner.plugin_registry.remove_plugin(&plugin_id);
                let _ = self.replay_register_for(&old_plugin);
                old_lifecycle.set(LifecycleState::Active);
                return Err(UniError::InvalidArgument {
                    arg: "new_plugin".to_owned(),
                    message: format!("reload compat-check rejected: {e}"),
                });
            }
        }

        // Step 7: replace (or remove) the host's `plugins` map entry
        // **before** the drain wait so the map's `Arc<PluginLifecycle>`
        // is no longer counted. After this, the only lifecycle Arcs
        // outstanding from the host should be: (a) the driver's `old`
        // ref, (b) our local `old_lifecycle`, plus any in-flight
        // captures. Threshold=2 lets the wait succeed as soon as no
        // in-flight capture survives.
        let new_handle = {
            let mut map = self.inner.plugins.write();
            if let Some(new) = new_plugin.clone() {
                let new_lifecycle = Arc::new(PluginLifecycle::new(plugin_id.clone()));
                new_lifecycle.set(LifecycleState::Active);
                let new_generation = old_generation.wrapping_add(1);
                map.insert(
                    plugin_id.clone(),
                    UniPluginEntry {
                        plugin: new,
                        lifecycle: new_lifecycle,
                        generation: new_generation,
                    },
                );
                uni_plugin::PluginHandle::new(plugin_id.clone(), new_generation)
            } else {
                map.remove(&plugin_id);
                uni_plugin::PluginHandle::new(plugin_id.clone(), old_generation)
            }
        };

        // Step 8: wait for in-flight references to drain. Threshold 2
        // accounts for the driver's own `old` Arc plus our local
        // `old_lifecycle`. If captures outlast the wait, surface a
        // warning but proceed — the new plugin is already live in the
        // registry.
        if let Err(e) = driver.wait_for_drain(
            2,
            std::time::Duration::from_millis(10),
            std::time::Duration::from_secs(30),
        ) {
            tracing::warn!(
                plugin_id = %plugin_id,
                error = %e,
                "reload drain wait timed out; proceeding with cutover"
            );
        }
        driver.finalize();

        // Step 9: run shutdown on the old plugin object after the
        // drain. Safe to call even if other Arcs outlive us because
        // shutdown is on `&self` and `Plugin: Send + Sync`.
        old_plugin.shutdown();

        Ok(new_handle)
    }

    /// Re-run the registrar dance for the given plugin object.
    ///
    /// Used as a best-effort rollback when [`Self::reload_internal`]
    /// rejects a reload after evicting the old plugin's registry
    /// footprint.
    fn replay_register_for(
        &self,
        plugin: &Arc<dyn uni_plugin::Plugin>,
    ) -> std::result::Result<(), UniError> {
        use uni_plugin::PluginRegistrar;
        let manifest = plugin.manifest();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &self.inner.plugin_registry);
        plugin.register(&mut r).map_err(plugin_err_to_uni)?;
        r.commit_to_registry().map_err(plugin_err_to_uni)?;
        Ok(())
    }

    /// Load an Extism-shaped WASM plugin from raw bytes.
    ///
    /// The two-pass dance defined by
    /// [`uni_plugin_extism::ExtismLoader::load`] is executed against the
    /// database's plugin registry: the plugin's `manifest` export is
    /// read, declared capabilities are intersected with `host_grants`,
    /// and the plugin's `register` export is consulted to surface every
    /// qname through an Extism-backed adapter.
    ///
    /// `registrar_caps` is the [`uni_plugin::CapabilitySet`] the
    /// inner [`uni_plugin::PluginRegistrar`] runs under — it gates
    /// **which surfaces** the plugin may register (e.g.,
    /// [`uni_plugin::Capability::ScalarFn`]). It must include every
    /// surface kind a plugin entry will use, or registration will
    /// fail with [`uni_plugin::PluginError::CapabilityRequired`].
    ///
    /// `host_grants` is the [`uni_plugin::CapabilitySet`] the host grants the
    /// plugin for **host-fn access** (e.g. `Capability::Network { allow }` with
    /// an attenuation allow-list). It is intersected with the plugin manifest's
    /// declared capabilities to compute the effective grant set; only host fns
    /// whose `required_capability` variant is in that set become part of the
    /// plugin's
    /// import table.
    ///
    /// # Errors
    ///
    /// Wraps [`uni_plugin_extism::ExtismError`] in
    /// [`UniError::InvalidArgument`] for plugin-side faults and
    /// [`UniError::Internal`] for host-side faults.
    ///
    /// # Feature
    ///
    /// Requires the `extism-plugins` feature.
    #[cfg(feature = "extism-plugins")]
    pub fn load_wasm_extism(
        &self,
        loader: &uni_plugin_extism::ExtismLoader,
        bytes: &[u8],
        host_grants: &uni_plugin::CapabilitySet,
        registrar_caps: &uni_plugin::CapabilitySet,
    ) -> Result<uni_plugin_extism::loader::LoadOutcome> {
        // The placeholder plugin id is rewritten by the loader with the
        // real id from the manifest into the returned LoadOutcome. We
        // need a non-empty placeholder because QName::namespace()
        // comparisons in `validate_qname` require a non-builtin
        // namespace; we let the registrar accept any qname by leaning on
        // `validate_qname`'s `is_builtin` short-circuit (M6a.2 expands
        // this with a per-plugin namespace gate).
        with_loading_registrar(
            &self.inner.plugin_registry,
            "extism.loading",
            registrar_caps,
            |r| {
                loader.load(bytes, host_grants, r).map_err(|e| match e {
                    uni_plugin_extism::ExtismError::Instantiate(m)
                    | uni_plugin_extism::ExtismError::InvalidPlugin(m)
                    | uni_plugin_extism::ExtismError::ManifestInvalid(m)
                    | uni_plugin_extism::ExtismError::OutputDecode(m) => {
                        UniError::InvalidArgument {
                            arg: "bytes".to_owned(),
                            message: format!("extism plugin: {m}"),
                        }
                    }
                    other => UniError::Internal(anyhow::anyhow!(other.to_string())),
                })
            },
        )
    }

    /// Load a Component Model WASM plugin from raw bytes.
    ///
    /// The two-pass dance defined by
    /// [`uni_plugin_wasm::WasmLoader::load`] is executed against the
    /// database's plugin registry: the plugin's `manifest` export is
    /// called, declared capabilities are intersected with `host_grants`,
    /// and the plugin's `register` export is consulted to surface every
    /// qname through a Component Model-backed adapter.
    ///
    /// `registrar_caps` gates which **surfaces** the plugin may
    /// register; `host_grants` gates which **host fns** become part of
    /// the plugin's import table (per-major Linker absence for
    /// capabilities outside the grant set — structural enforcement,
    /// proposal §5.6.2).
    ///
    /// # Errors
    ///
    /// Wraps [`uni_plugin_wasm::WasmError`] in
    /// [`UniError::InvalidArgument`] for plugin-side faults
    /// (invalid wasm, missing required exports, manifest parse) and
    /// [`UniError::Internal`] for host-side faults.
    ///
    /// # Feature
    ///
    /// Requires the `wasm-plugins` feature.
    #[cfg(feature = "wasm-plugins")]
    pub fn load_wasm_component(
        &self,
        loader: &uni_plugin_wasm::WasmLoader,
        bytes: &[u8],
        host_grants: &uni_plugin::CapabilitySet,
        registrar_caps: &uni_plugin::CapabilitySet,
    ) -> Result<uni_plugin_wasm::loader::LoadOutcome> {
        with_loading_registrar(
            &self.inner.plugin_registry,
            "wasm.loading",
            registrar_caps,
            |r| {
                loader.load(bytes, host_grants, r).map_err(|e| match e {
                    uni_plugin_wasm::WasmError::Instantiate(m)
                    | uni_plugin_wasm::WasmError::Invoke(m)
                    | uni_plugin_wasm::WasmError::InvalidWasm(m)
                    | uni_plugin_wasm::WasmError::ResourceLimit(m) => UniError::InvalidArgument {
                        arg: "bytes".to_owned(),
                        message: format!("wasm component: {m}"),
                    },
                    other => UniError::Internal(anyhow::anyhow!(other.to_string())),
                })
            },
        )
    }

    /// Load a Rhai-script plugin from source text.
    ///
    /// Rhai is a pure-Rust embedded scripting language; no WASM wrapper,
    /// no C toolchain. The Rhai engine is sandboxed by language design —
    /// scripts have no built-in I/O, every effectful operation comes
    /// from a host-registered function. The loader's three-phase shape
    /// mirrors `Self::load_wasm_extism`: read the script's
    /// `uni_manifest()` to discover declared entries, intersect declared
    /// capabilities with `registrar_caps`, then register each entry on
    /// the inner [`uni_plugin::PluginRegistrar`] as a Rhai-backed
    /// adapter.
    ///
    /// `registrar_caps` is **both** the registration gate (it must
    /// include `Capability::ScalarFn`/`AggregateFn`/`Procedure` matching
    /// the script's entries) **and** the host-fn grant set (host fns
    /// like `uni_fs_read` are only registered on the engine if the
    /// matching `Capability::Filesystem` etc. is present). Rhai's
    /// capability-enforcement layer 2 is *Engine-import absence* —
    /// ungranted host fns are not registered, so any call to them
    /// fails at parse-resolution with `ErrorFunctionNotFound`.
    ///
    /// # Errors
    ///
    /// Wraps [`uni_plugin_rhai::RhaiError`] in
    /// [`UniError::InvalidArgument`] for plugin-side faults and
    /// [`UniError::Internal`] for host-side faults.
    ///
    /// # Feature
    ///
    /// Requires the `rhai-plugins` feature (on by default).
    #[cfg(feature = "rhai-plugins")]
    pub fn load_rhai_plugin(
        &self,
        loader: &uni_plugin_rhai::RhaiLoader,
        script: &str,
        registrar_caps: &uni_plugin::CapabilitySet,
    ) -> Result<uni_plugin_rhai::LoadOutcome> {
        with_loading_registrar(
            &self.inner.plugin_registry,
            "rhai.loading",
            registrar_caps,
            |r| {
                loader.load(script, r, registrar_caps).map_err(|e| match e {
                    uni_plugin_rhai::RhaiError::ParseFailed(m) => UniError::InvalidArgument {
                        arg: "script".to_owned(),
                        message: format!("rhai parse: {m}"),
                    },
                    uni_plugin_rhai::RhaiError::InvalidPlugin(m)
                    | uni_plugin_rhai::RhaiError::ManifestInvalid(m)
                    | uni_plugin_rhai::RhaiError::Conversion(m)
                    | uni_plugin_rhai::RhaiError::RuntimeError(m) => UniError::InvalidArgument {
                        arg: "script".to_owned(),
                        message: format!("rhai plugin: {m}"),
                    },
                    other => UniError::Internal(anyhow::anyhow!(other.to_string())),
                })
            },
        )
    }

    /// Load a PyO3 (Python source) plugin into this Uni instance.
    ///
    /// The supplied [`PyPluginLoader`](uni_plugin_pyo3::PythonPluginLoader)
    /// holds the loader's default plugin id (used when the module
    /// doesn't call `db.set_plugin_id(...)`). `module_src` is Python
    /// source code; `module_name` is the simulated `__name__`. The
    /// loader executes the source against a fresh module namespace
    /// that includes a `_uni_decorator_sink` / `db` global; each
    /// `@db.scalar_fn(...)` / `@db.aggregate_fn(...)` / `@db.procedure(...)`
    /// decorator records into a builder and the loader drains it on
    /// completion. Scalar / aggregate / procedure adapters are pushed
    /// onto a fresh [`PluginRegistrar`](uni_plugin::PluginRegistrar)
    /// and committed atomically.
    ///
    /// **M8 scope:** the plugin is added to the *instance* registry.
    /// Session-scoped registration (proposal §5.4.2 default) is the
    /// `M8-followup.session-scope` work item; until then, callers that
    /// want session-scoped behavior should drop the plugin on session
    /// drop themselves via `Uni::remove_plugin`.
    ///
    /// # Errors
    ///
    /// - [`UniError::InvalidArgument`] for plugin-side faults (parse,
    ///   manifest, unknown type name).
    /// - [`UniError::Internal`] for host-side faults.
    ///
    /// # Feature
    ///
    /// Requires the `pyo3-plugins` feature.
    #[cfg(feature = "pyo3-plugins")]
    pub fn load_python_plugin(
        &self,
        py: pyo3::Python<'_>,
        loader: &uni_plugin_pyo3::PythonPluginLoader,
        module_src: &str,
        module_name: &str,
        registrar_caps: &uni_plugin::CapabilitySet,
    ) -> Result<uni_plugin_pyo3::LoadOutcome> {
        with_loading_registrar(
            &self.inner.plugin_registry,
            "pyo3.loading",
            registrar_caps,
            |r| {
                loader
                    .load(py, module_src, module_name, r, registrar_caps)
                    .map_err(|e| match e {
                        uni_plugin_pyo3::PyPluginError::PythonException {
                            qname,
                            message,
                            traceback,
                        } => UniError::InvalidArgument {
                            arg: "module_src".to_owned(),
                            message: format!("python exception in {qname}: {message}\n{traceback}"),
                        },
                        uni_plugin_pyo3::PyPluginError::ManifestInvalid(m) => {
                            UniError::InvalidArgument {
                                arg: "module_src".to_owned(),
                                message: format!("python plugin manifest: {m}"),
                            }
                        }
                        uni_plugin_pyo3::PyPluginError::ArrowConversion(m) => {
                            UniError::InvalidArgument {
                                arg: "module_src".to_owned(),
                                message: format!("python plugin arrow conversion: {m}"),
                            }
                        }
                        other => UniError::Internal(anyhow::anyhow!(other.to_string())),
                    })
            },
        )
    }

    // ── Connector lifecycle (M6a.3) ─────────────────────────────────

    /// Start a registered wire-protocol connector.
    ///
    /// Looks up the first [`Connector`] in the plugin registry whose
    /// `protocol()` matches `protocol`, calls its `start(cfg)` with the
    /// supplied configuration, and records the returned handle so that
    /// [`Self::stop_connector`] can later route to the right `stop()`.
    /// The returned `u64` is a host-side handle that disambiguates
    /// connectors that all return the same plugin-side
    /// `ConnectorHandle(0)`; pass it back to [`Self::stop_connector`]
    /// to shut the connector down.
    ///
    /// # Errors
    ///
    /// - [`UniError::NotFound`] if no registered connector advertises
    ///   `protocol`.
    /// - [`UniError::Internal`] (wrapping the connector's `FnError`) if
    ///   `Connector::start` itself fails.
    ///
    /// [`Connector`]: uni_plugin::traits::connector::Connector
    pub fn start_connector(
        &self,
        protocol: &str,
        config: uni_plugin::traits::connector::ConnectorConfig,
    ) -> Result<u64> {
        let connectors = self.inner.plugin_registry.connectors();
        let connector = connectors
            .iter()
            .find(|c| c.protocol() == protocol)
            .ok_or_else(|| UniError::InvalidArgument {
                arg: "protocol".to_owned(),
                message: format!("no connector registered for protocol `{protocol}`"),
            })?;
        let plugin_handle = connector.start(config).map_err(|e| {
            UniError::Internal(anyhow::anyhow!(
                "connector `{protocol}` start failed (code={}): {}",
                e.code,
                e.message
            ))
        })?;
        let host_handle = self.inner.next_connector_seq.fetch_add(1, Ordering::SeqCst);
        self.inner.active_connectors.insert(
            host_handle,
            ActiveConnector {
                protocol: protocol.to_owned(),
                handle: plugin_handle,
                connector: Arc::clone(connector),
            },
        );
        Ok(host_handle)
    }

    /// Stop a previously-started connector by its host handle.
    ///
    /// Removes the connector from the active map and calls
    /// `Connector::stop()` on the trait object recorded at start time.
    /// Stopping a handle that was never recorded — or that was already
    /// stopped — returns [`UniError::NotFound`].
    ///
    /// # Errors
    ///
    /// - [`UniError::NotFound`] if `host_handle` does not name an
    ///   active connector.
    /// - [`UniError::Internal`] (wrapping the connector's `FnError`)
    ///   if `Connector::stop` itself fails. The entry is removed from
    ///   the active map regardless — `stop` is expected to be
    ///   idempotent host-side.
    pub fn stop_connector(&self, host_handle: u64) -> Result<()> {
        let (_, active) = self
            .inner
            .active_connectors
            .remove(&host_handle)
            .ok_or_else(|| UniError::InvalidArgument {
                arg: "host_handle".to_owned(),
                message: format!("no active connector with handle {host_handle}"),
            })?;
        active.connector.stop(active.handle).map_err(|e| {
            UniError::Internal(anyhow::anyhow!(
                "connector `{}` stop failed (code={}): {}",
                active.protocol,
                e.code,
                e.message
            ))
        })
    }

    /// Snapshot the active-connector map for diagnostics.
    ///
    /// Returns `(host_handle, protocol)` pairs for every connector
    /// currently running on this `Uni` instance. Order is unspecified.
    #[must_use]
    pub fn active_connectors(&self) -> Vec<(u64, String)> {
        self.inner
            .active_connectors
            .iter()
            .map(|kv| (*kv.key(), kv.value().protocol.clone()))
            .collect()
    }

    /// Get schema manager.
    #[doc(hidden)]
    pub fn schema_manager(&self) -> Arc<SchemaManager> {
        self.inner.schema.clone()
    }

    #[doc(hidden)]
    pub fn writer(&self) -> Option<Arc<Writer>> {
        self.inner.writer.clone()
    }

    #[doc(hidden)]
    pub fn storage(&self) -> Arc<StorageManager> {
        self.inner.storage.clone()
    }

    /// Flush all uncommitted changes to persistent storage (L1).
    ///
    /// This forces a write of the current in-memory buffer (L0) to columnar files.
    /// It also creates a new snapshot.
    pub async fn flush(&self) -> Result<()> {
        if let Some(writer) = &self.inner.writer {
            writer
                .flush_to_l1(None)
                .await
                .map(|_| ())
                .map_err(UniError::Internal)
        } else {
            Err(UniError::ReadOnly {
                operation: "flush".to_string(),
            })
        }
    }

    /// Create a named point-in-time snapshot of the database.
    ///
    /// Flushes current changes, records the state, and persists the snapshot
    /// under the given name so it can be retrieved later.
    /// Returns the snapshot ID.
    pub async fn create_snapshot(&self, name: &str) -> Result<String> {
        if name.is_empty() {
            return Err(UniError::Internal(anyhow::anyhow!(
                "Snapshot name cannot be empty"
            )));
        }

        let snapshot_id = if let Some(writer) = &self.inner.writer {
            writer
                .flush_to_l1(Some(name.to_string()))
                .await
                .map_err(UniError::Internal)?
        } else {
            return Err(UniError::ReadOnly {
                operation: "create_snapshot".to_string(),
            });
        };

        self.inner
            .storage
            .snapshot_manager()
            .save_named_snapshot(name, &snapshot_id)
            .await
            .map_err(UniError::Internal)?;

        Ok(snapshot_id)
    }

    /// List all available snapshots.
    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotManifest>> {
        let sm = self.inner.storage.snapshot_manager();
        let ids = sm.list_snapshots().await.map_err(UniError::Internal)?;
        let mut manifests = Vec::new();
        for id in ids {
            if let Ok(m) = sm.load_snapshot(&id).await {
                manifests.push(m);
            }
        }
        Ok(manifests)
    }

    /// Restore the database to a specific snapshot.
    ///
    /// **Note**: This currently requires a restart or re-opening of Uni to fully take effect
    /// as it only updates the latest pointer.
    pub async fn restore_snapshot(&self, snapshot_id: &str) -> Result<()> {
        self.inner
            .storage
            .snapshot_manager()
            .set_latest_snapshot(snapshot_id)
            .await
            .map_err(UniError::Internal)
    }

    /// Check if a label exists in the schema.
    pub async fn label_exists(&self, name: &str) -> Result<bool> {
        let schema = self.inner.schema.schema();
        Ok(element_active(schema.labels.get(name).map(|l| &l.state)))
    }

    /// Check if an edge type exists in the schema.
    pub async fn edge_type_exists(&self, name: &str) -> Result<bool> {
        let schema = self.inner.schema.schema();
        Ok(element_active(
            schema.edge_types.get(name).map(|e| &e.state),
        ))
    }

    /// Get all label names.
    /// Returns the union of schema-registered labels (Active state) and labels
    /// discovered from data (for schemaless mode where labels may not be in the
    /// schema). This is consistent with `list_edge_types()` for schema labels
    /// while also supporting schemaless workflows.
    pub async fn list_labels(&self) -> Result<Vec<String>> {
        let mut all_labels = std::collections::HashSet::new();

        // Schema labels (covers schema-defined labels that may not have data yet)
        for (name, label) in self.inner.schema.schema().labels.iter() {
            if matches!(
                label.state,
                uni_common::core::schema::SchemaElementState::Active
            ) {
                all_labels.insert(name.clone());
            }
        }

        // Data labels (covers schemaless labels that aren't in the schema)
        let query = "MATCH (n) RETURN DISTINCT labels(n) AS labels";
        let result = self.inner.execute_internal(query, HashMap::new()).await?;
        for row in result.rows() {
            if let Ok(labels_list) = row.get::<Vec<String>>("labels") {
                for label in labels_list {
                    all_labels.insert(label);
                }
            }
        }

        Ok(all_labels.into_iter().collect())
    }

    /// Get all edge type names.
    pub async fn list_edge_types(&self) -> Result<Vec<String>> {
        Ok(self
            .inner
            .schema
            .schema()
            .edge_types
            .iter()
            .filter(|(_, e)| {
                matches!(
                    e.state,
                    uni_common::core::schema::SchemaElementState::Active
                )
            })
            .map(|(name, _)| name.clone())
            .collect())
    }

    // (schema-projection helpers `property_infos_for` / `index_infos_for`
    //  / `constraint_infos_for` are free functions defined below this impl.)

    /// Get detailed information about a label.
    pub async fn get_label_info(
        &self,
        name: &str,
    ) -> Result<Option<crate::api::schema::LabelInfo>> {
        let schema = self.inner.schema.schema();
        if let Some(label_meta) = schema.labels.get(name) {
            let count = if let Ok(ds) = self.inner.storage.vertex_dataset(name) {
                if let Ok(raw) = ds.open_raw().await {
                    raw.count_rows(None)
                        .await
                        .map_err(|e| UniError::Internal(anyhow::anyhow!(e)))?
                } else {
                    0
                }
            } else {
                0
            };

            Ok(Some(crate::api::schema::LabelInfo {
                name: name.to_string(),
                count,
                properties: property_infos_for(&schema, name, label_property_is_indexed),
                indexes: index_infos_for(&schema, name, label_index_descriptor),
                constraints: constraint_infos_for(
                    &schema,
                    |c| matches!(&c.target, uni_common::core::schema::ConstraintTarget::Label(l) if l == name),
                ),
                description: label_meta.description.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    /// Get detailed information about an edge type.
    pub async fn get_edge_type_info(
        &self,
        name: &str,
    ) -> Result<Option<crate::api::schema::EdgeTypeInfo>> {
        let schema = self.inner.schema.schema();
        let edge_meta = match schema.edge_types.get(name) {
            Some(meta) => meta,
            None => return Ok(None),
        };

        // Count edges via internal query
        let count = {
            let query = format!("MATCH ()-[r:{}]->() RETURN count(r) AS cnt", name);
            match self.inner.execute_internal(&query, HashMap::new()).await {
                Ok(result) => result
                    .rows()
                    .first()
                    .and_then(|r| r.get::<i64>("cnt").ok())
                    .unwrap_or(0) as usize,
                Err(_) => 0,
            }
        };

        let source_labels = edge_meta.src_labels.clone();
        let target_labels = edge_meta.dst_labels.clone();

        Ok(Some(crate::api::schema::EdgeTypeInfo {
            name: name.to_string(),
            count,
            source_labels,
            target_labels,
            properties: property_infos_for(&schema, name, edge_property_is_indexed),
            indexes: index_infos_for(&schema, name, edge_index_descriptor),
            constraints: constraint_infos_for(
                &schema,
                |c| matches!(&c.target, uni_common::core::schema::ConstraintTarget::EdgeType(et) if et == name),
            ),
            description: edge_meta.description.clone(),
        }))
    }

    // ── Compaction ──────────────────────────────────────────────────────

    /// Access compaction operations.
    pub fn compaction(&self) -> compaction::Compaction<'_> {
        compaction::Compaction { inner: &self.inner }
    }

    // ── Indexes ──────────────────────────────────────────────────────────

    /// Access index management operations.
    pub fn indexes(&self) -> indexes::Indexes<'_> {
        indexes::Indexes { inner: &self.inner }
    }

    // ── Custom Functions ──────────────────────────────────────────────

    /// Access custom Cypher function management.
    pub fn functions(&self) -> functions::Functions<'_> {
        functions::Functions { inner: &self.inner }
    }

    /// Shutdown the database gracefully, flushing pending data and stopping background tasks.
    ///
    /// This method flushes any pending data and waits for all background tasks to complete
    /// (with a timeout). After calling this method, the database instance should not be used.
    pub async fn shutdown(self) -> Result<()> {
        // Flush pending data.
        if let Some(writer) = &self.inner.writer {
            if let Err(e) = writer.flush_to_l1(None).await {
                tracing::error!("Error flushing during shutdown: {}", e);
            }
            // Close the async-flush coordinator's submit channel so its
            // finalizer task exits now. The finalizer's JoinHandle is
            // tracked by `shutdown_handle`, but the loop blocks on
            // `submit_rx.recv()` and never sees the shutdown broadcast — so
            // without this the `shutdown_async` below would await it for the
            // full grace period. `shutdown()` drops the sender (the
            // finalizer then receives `None` and exits) and is idempotent.
            if let Some(coord) = writer.flush_coordinator() {
                coord.shutdown().await;
            }
        }

        self.inner
            .shutdown_handle
            .shutdown_async()
            .await
            .map_err(UniError::Internal)
    }
}

impl Drop for Uni {
    fn drop(&mut self) {
        self.inner.shutdown_handle.shutdown_blocking();
        tracing::debug!("Uni dropped, shutdown signal sent");
    }
}

/// Builder for configuring and opening a `Uni` database instance.
#[must_use = "builders do nothing until .build() is called"]
pub struct UniBuilder {
    uri: String,
    config: UniConfig,
    schema_file: Option<PathBuf>,
    xervo_catalog: Option<Vec<ModelAliasSpec>>,
    /// Pre-built Xervo runtime (bypasses catalog-based builder when set).
    prebuilt_xervo_runtime: Option<Arc<ModelRuntime>>,
    hybrid_remote_url: Option<String>,
    cloud_config: Option<CloudStorageConfig>,
    create_if_missing: bool,
    fail_if_exists: bool,
    read_only: bool,
    write_lease: Option<multi_agent::WriteLease>,
    plugin_trust: Arc<plugin_trust::PluginTrustConfig>,
    temp_dir: Option<TempDir>,
    /// When true, persisted Locy rules that no longer compile are skipped
    /// (with a warning) on open instead of failing the open.
    skip_invalid_locy_rules: bool,
}

impl UniBuilder {
    /// Creates a new builder for the given URI.
    pub fn new(uri: String) -> Self {
        Self {
            uri,
            config: UniConfig::default(),
            schema_file: None,
            xervo_catalog: None,
            prebuilt_xervo_runtime: None,
            hybrid_remote_url: None,
            cloud_config: None,
            create_if_missing: true,
            fail_if_exists: false,
            read_only: false,
            write_lease: None,
            plugin_trust: Arc::new(plugin_trust::PluginTrustConfig::default()),
            temp_dir: None,
            skip_invalid_locy_rules: false,
        }
    }

    /// Skips persisted Locy rules that no longer compile, instead of failing.
    ///
    /// By default, opening a database whose `catalog/locy_rules.json` contains
    /// a rule that no longer compiles (for example after a grammar change)
    /// fails with an error naming the offending rule. Enabling this skips such
    /// rules with a warning and retains them in the catalog file, so a fixed
    /// binary can recover them.
    pub fn skip_invalid_locy_rules(mut self, skip: bool) -> Self {
        self.skip_invalid_locy_rules = skip;
        self
    }

    /// Load schema from JSON file on initialization.
    pub fn schema_file(mut self, path: impl AsRef<Path>) -> Self {
        self.schema_file = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set Uni-Xervo catalog explicitly.
    pub fn xervo_catalog(mut self, catalog: Vec<ModelAliasSpec>) -> Self {
        self.xervo_catalog = Some(catalog);
        self
    }

    /// Set a pre-built Xervo runtime directly.
    ///
    /// This bypasses the catalog-based provider registration and uses the
    /// provided runtime as-is. Useful for testing with mock providers or
    /// for advanced scenarios where the caller controls runtime construction.
    ///
    /// Mutually exclusive with [`xervo_catalog()`](Self::xervo_catalog) —
    /// when both are set, this takes precedence.
    pub fn xervo_runtime(mut self, runtime: Arc<ModelRuntime>) -> Self {
        self.prebuilt_xervo_runtime = Some(runtime);
        self
    }

    /// Configure remote storage for data, keeping local path for WAL/IDs.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use uni_common::CloudStorageConfig;
    ///
    /// let config = CloudStorageConfig::S3 {
    ///     bucket: "my-bucket".to_string(),
    ///     region: Some("us-east-1".to_string()),
    ///     endpoint: None,
    ///     access_key_id: None,
    ///     secret_access_key: None,
    ///     session_token: None,
    ///     virtual_hosted_style: false,
    /// };
    ///
    /// let db = Uni::open("./local_meta")
    ///     .remote_storage("s3://my-bucket/graph-data", config)
    ///     .build()
    ///     .await?;
    /// ```
    pub fn remote_storage(mut self, remote_url: &str, config: CloudStorageConfig) -> Self {
        self.hybrid_remote_url = Some(remote_url.to_string());
        self.cloud_config = Some(config);
        self
    }

    /// Open the database in read-only mode.
    ///
    /// In read-only mode, no writer is created. All write operations
    /// (`tx()`, `execute()`, `bulk_writer()`, `appender()`) will return
    /// `ReadOnly` errors. Reads work normally.
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    /// Set the write lease strategy for multi-agent access.
    ///
    /// This configures how write access is coordinated when multiple
    /// processes share the same database.
    pub fn write_lease(mut self, lease: multi_agent::WriteLease) -> Self {
        self.write_lease = Some(lease);
        self
    }

    /// Set the host plugin trust policy (signature enforcement + trust root).
    ///
    /// Applies to externally-loaded plugins (`add_plugin` and, as the
    /// signing subsystem lands, the WASM/Extism/Rhai/Python loaders).
    /// Compile-time built-in plugins are implicitly trusted. The default
    /// is [`SignaturePolicy::Disabled`](uni_plugin::verify::SignaturePolicy)
    /// with an empty trust root — accept everything, identical to prior
    /// behavior.
    pub fn plugin_trust(mut self, cfg: plugin_trust::PluginTrustConfig) -> Self {
        self.plugin_trust = Arc::new(cfg);
        self
    }

    /// Configure database options using `UniConfig`.
    pub fn config(mut self, config: UniConfig) -> Self {
        self.config = config;
        self
    }

    /// Open the database (async).
    pub async fn build(self) -> Result<Uni> {
        let uri = self.uri.clone();
        let is_remote_uri = uri.contains("://");
        let is_hybrid = self.hybrid_remote_url.is_some();

        if is_hybrid && is_remote_uri {
            return Err(UniError::Internal(anyhow::anyhow!(
                "Hybrid mode requires a local path as primary URI, found: {}",
                uri
            )));
        }

        let (storage_uri, data_store, local_store_opt) = if is_hybrid {
            let remote_url = self.hybrid_remote_url.as_ref().unwrap();

            // Remote Store (Data) - use explicit cloud_config if provided
            let remote_store: Arc<dyn ObjectStore> = if let Some(cloud_cfg) = &self.cloud_config {
                build_cloud_store(cloud_cfg).map_err(UniError::Internal)?
            } else {
                let url = url::Url::parse(remote_url).map_err(|e| {
                    UniError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        e.to_string(),
                    ))
                })?;
                let (os, _path) =
                    object_store::parse_url(&url).map_err(|e| UniError::Internal(e.into()))?;
                Arc::from(os)
            };

            // Local Store (WAL, IDs)
            let path = PathBuf::from(&uri);
            if path.exists() {
                if self.fail_if_exists {
                    return Err(UniError::Internal(anyhow::anyhow!(
                        "Database already exists at {}",
                        uri
                    )));
                }
            } else {
                if !self.create_if_missing {
                    return Err(UniError::NotFound { path: path.clone() });
                }
                std::fs::create_dir_all(&path).map_err(UniError::Io)?;
            }

            let local_store = Arc::new(
                LocalFileSystem::new_with_prefix(&path).map_err(|e| UniError::Io(e.into()))?,
            );

            // For hybrid, storage_uri is the remote URL (since StorageManager loads datasets from there)
            // But we must provide the correct store to other components manually.
            (
                remote_url.clone(),
                remote_store,
                Some(local_store as Arc<dyn ObjectStore>),
            )
        } else if is_remote_uri {
            // Remote Only - use explicit cloud_config if provided
            let remote_store: Arc<dyn ObjectStore> = if let Some(cloud_cfg) = &self.cloud_config {
                build_cloud_store(cloud_cfg).map_err(UniError::Internal)?
            } else {
                let url = url::Url::parse(&uri).map_err(|e| {
                    UniError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        e.to_string(),
                    ))
                })?;
                let (os, _path) =
                    object_store::parse_url(&url).map_err(|e| UniError::Internal(e.into()))?;
                Arc::from(os)
            };

            (uri.clone(), remote_store, None)
        } else {
            // Local Only
            let path = PathBuf::from(&uri);
            let storage_path = path.join("storage");

            if path.exists() {
                if self.fail_if_exists {
                    return Err(UniError::Internal(anyhow::anyhow!(
                        "Database already exists at {}",
                        uri
                    )));
                }
            } else {
                if !self.create_if_missing {
                    return Err(UniError::NotFound { path: path.clone() });
                }
                std::fs::create_dir_all(&path).map_err(UniError::Io)?;
            }

            // Ensure storage directory exists
            if !storage_path.exists() {
                std::fs::create_dir_all(&storage_path).map_err(UniError::Io)?;
            }

            let store = Arc::new(
                LocalFileSystem::new_with_prefix(&path).map_err(|e| UniError::Io(e.into()))?,
            );
            (
                storage_path.to_string_lossy().to_string(),
                store.clone() as Arc<dyn ObjectStore>,
                Some(store as Arc<dyn ObjectStore>),
            )
        };

        // Canonical schema location in metadata catalog.
        let schema_obj_path = object_store::path::Path::from("catalog/schema.json");
        // Legacy schema location used by older builds.
        let legacy_schema_obj_path = object_store::path::Path::from("schema.json");

        // Backward-compatible schema path migration:
        // if catalog/schema.json is missing but root schema.json exists,
        // copy root schema.json to catalog/schema.json.
        let has_catalog_schema = match data_store.get(&schema_obj_path).await {
            Ok(_) => true,
            Err(object_store::Error::NotFound { .. }) => false,
            Err(e) => return Err(UniError::Internal(e.into())),
        };
        if !has_catalog_schema {
            match data_store.get(&legacy_schema_obj_path).await {
                Ok(result) => {
                    let bytes = result
                        .bytes()
                        .await
                        .map_err(|e| UniError::Internal(e.into()))?;
                    data_store
                        .put(&schema_obj_path, bytes.into())
                        .await
                        .map_err(|e| UniError::Internal(e.into()))?;
                    info!(
                        legacy = %legacy_schema_obj_path,
                        target = %schema_obj_path,
                        "Migrated legacy schema path to catalog path"
                    );
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(UniError::Internal(e.into())),
            }
        }

        // Load schema (SchemaManager::load creates a default if missing)
        // Schema is always in data_store (Remote or Local)
        let schema_manager = Arc::new(
            SchemaManager::load_from_store(data_store.clone(), &schema_obj_path)
                .await
                .map_err(UniError::Internal)?,
        );

        // Load and recompile persisted Locy rules (catalog/locy_rules.json).
        // A missing file yields an empty registry; a rule that no longer
        // compiles fails the open unless `skip_invalid_locy_rules` is set.
        let locy_rules_obj_path = object_store::path::Path::from("catalog/locy_rules.json");
        let persisted_locy_sources =
            locy_rule_catalog::LocyRulePersister::load(data_store.clone(), &locy_rules_obj_path)
                .await?;
        let loaded_locy_registry = impl_locy::build_locy_registry_from_persisted(
            &persisted_locy_sources,
            self.skip_invalid_locy_rules,
        )?;
        let locy_rule_persister = Arc::new(locy_rule_catalog::LocyRulePersister::new(
            data_store.clone(),
            locy_rules_obj_path,
        ));

        let lancedb_storage_options = self
            .cloud_config
            .as_ref()
            .map(Self::cloud_config_to_lancedb_storage_options);

        let storage = if is_hybrid || is_remote_uri {
            // Preserve explicit cloud settings (endpoint, credentials, path style)
            // by reusing the constructed remote store.
            StorageManager::new_with_store_and_storage_options(
                &storage_uri,
                data_store.clone(),
                schema_manager.clone(),
                self.config.clone(),
                lancedb_storage_options.clone(),
            )
            .await
            .map_err(UniError::Internal)?
        } else {
            // Local mode keeps using a storage-path-scoped local store.
            StorageManager::new_with_config(
                &storage_uri,
                schema_manager.clone(),
                self.config.clone(),
            )
            .await
            .map_err(UniError::Internal)?
        };

        let storage = Arc::new(storage);

        // Create shutdown handle
        let shutdown_handle = Arc::new(ShutdownHandle::new(Duration::from_secs(30)));

        // Start background compaction with shutdown signal
        let compaction_handle = storage
            .clone()
            .start_background_compaction(shutdown_handle.subscribe());
        shutdown_handle.track_task(compaction_handle);

        // Plugin registry is built early so `PropertyManager` can
        // share it for registry-dispatched CRDT merges. Built-ins are
        // registered against this same Arc below; the registry is
        // shared by-reference, so the registrations are visible to
        // every later consumer.
        let plugin_registry = Arc::new(uni_plugin::PluginRegistry::new());
        // M11 A.2: pass the data directory so `SystemLabelPersistence`
        // can be wired as the meta-plugin persistence backend. Remote /
        // object-store URIs (those containing "://") have no local
        // sidecar root — for those, persistence falls back to
        // `NullPersistence`.
        let persistence_data_path: Option<std::path::PathBuf> = if is_remote_uri {
            None
        } else {
            Some(std::path::PathBuf::from(&uri))
        };
        let custom_persistence_sink =
            register_builtin_plugins(&plugin_registry, persistence_data_path.as_deref()).expect(
                "BuiltinPlugin / ApocCorePlugin registration must succeed against fresh registry",
            );

        // Initialize property manager
        let prop_cache_capacity = self.config.cache_size / 1024;

        let prop_manager = Arc::new(PropertyManager::with_plugin_registry(
            storage.clone(),
            schema_manager.clone(),
            prop_cache_capacity,
            plugin_registry.clone(),
        ));

        // Setup stores for WAL and IdAllocator (needed for version recovery check)
        let id_store = local_store_opt
            .clone()
            .unwrap_or_else(|| data_store.clone());
        let wal_store = local_store_opt
            .clone()
            .unwrap_or_else(|| data_store.clone());

        // Reconcile an interrupted bulk load before reading the latest snapshot:
        // a crash between the per-label and main table commits would otherwise
        // leave them divergent. Recovery rolls an uncommitted load back, or rolls
        // a committed-but-unfinalized one forward (it may flip the latest pointer,
        // so it must run first). A no-op when no marker is present (H9).
        uni_bulk::recover_interrupted_bulk_load(&storage)
            .await
            .map_err(UniError::Internal)?;

        // Determine start version and WAL high water mark from latest snapshot.
        // Detects and recovers from a lost manifest pointer.
        let latest_snapshot = storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await
            .map_err(UniError::Internal)?;

        let (start_version, wal_high_water_mark) = if let Some(ref snapshot) = latest_snapshot {
            (
                snapshot.version_high_water_mark + 1,
                snapshot.wal_high_water_mark,
            )
        } else {
            // No latest snapshot — fresh DB or lost manifest?
            let has_manifests = storage
                .snapshot_manager()
                .has_any_manifests()
                .await
                .unwrap_or(false);

            let wal_check =
                WriteAheadLog::new(wal_store.clone(), object_store::path::Path::from("wal"));
            let has_wal = wal_check.has_segments().await.unwrap_or(false);

            if has_manifests {
                // Manifests exist but latest pointer is missing — try to recover from manifests
                let snapshot_ids = storage
                    .snapshot_manager()
                    .list_snapshots()
                    .await
                    .map_err(UniError::Internal)?;
                if let Some(last_id) = snapshot_ids.last() {
                    let manifest = storage
                        .snapshot_manager()
                        .load_snapshot(last_id)
                        .await
                        .map_err(UniError::Internal)?;
                    tracing::warn!(
                        "Latest snapshot pointer missing but found manifest '{}'. \
                         Recovering version {}.",
                        last_id,
                        manifest.version_high_water_mark
                    );
                    (
                        manifest.version_high_water_mark + 1,
                        manifest.wal_high_water_mark,
                    )
                } else {
                    return Err(UniError::Internal(anyhow::anyhow!(
                        "Snapshot manifests directory exists but contains no valid manifests. \
                         Possible data corruption."
                    )));
                }
            } else if has_wal {
                // WAL exists but no manifests at all — data exists but unrecoverable version
                return Err(UniError::Internal(anyhow::anyhow!(
                    "Database has WAL segments but no snapshot manifest. \
                     Cannot safely determine version counter -- starting at 0 would cause \
                     version conflicts and data corruption. \
                     Restore the snapshot manifest or delete WAL to start fresh."
                )));
            } else {
                // Truly fresh database
                (0, 0)
            }
        };

        let allocator = Arc::new(
            IdAllocator::new(
                id_store,
                object_store::path::Path::from("id_allocator.json"),
                1000,
            )
            .await
            .map_err(UniError::Internal)?,
        );

        // When WAL is enabled the construction is identical for every
        // storage layout (remote-only, hybrid, or local): the only
        // difference is which `wal_store` was resolved above, and
        // `local_store` maps to the FS even behind the ObjectStore trait.
        // For local layouts the data directory is passed as the WAL's
        // local root, enabling fsync-on-flush (LocalFileSystem `put` does
        // not fsync; without it a power loss can drop acknowledged
        // commits). Remote layouts rely on the PUT ack.
        let wal = if self.config.wal_enabled {
            Some(Arc::new(
                WriteAheadLog::new(wal_store, object_store::path::Path::from("wal"))
                    .with_local_root(persistence_data_path.clone()),
            ))
        } else {
            None
        };

        let writer = Arc::new(
            Writer::new_with_config(
                storage.clone(),
                schema_manager.clone(),
                start_version,
                self.config.clone(),
                wal,
                Some(allocator),
            )
            .await
            .map_err(UniError::Internal)?,
        );

        let required_embed_aliases: std::collections::BTreeSet<String> = schema_manager
            .schema()
            .indexes
            .iter()
            .filter_map(|idx| {
                if let uni_common::core::schema::IndexDefinition::Vector(cfg) = idx {
                    cfg.embedding_config.as_ref().map(|emb| emb.alias.clone())
                } else {
                    None
                }
            })
            .collect();

        if !required_embed_aliases.is_empty() && self.xervo_catalog.is_none() {
            return Err(UniError::Internal(anyhow::anyhow!(
                "Uni-Xervo catalog is required because schema has vector indexes with embedding aliases"
            )));
        }

        let xervo_runtime = if let Some(runtime) = self.prebuilt_xervo_runtime {
            Some(runtime)
        } else if let Some(catalog) = self.xervo_catalog {
            for alias in &required_embed_aliases {
                let spec = catalog.iter().find(|s| &s.alias == alias).ok_or_else(|| {
                    UniError::Internal(anyhow::anyhow!(
                        "Missing Uni-Xervo alias '{}' referenced by vector index embedding config",
                        alias
                    ))
                })?;
                if spec.task != ModelTask::Embed {
                    return Err(UniError::Internal(anyhow::anyhow!(
                        "Uni-Xervo alias '{}' must be an embed task",
                        alias
                    )));
                }
            }

            // `mut` is conditional on at least one provider-* feature being
            // enabled; a slim build with no providers leaves it unused.
            #[allow(unused_mut)]
            let mut runtime_builder = ModelRuntime::builder().catalog(catalog);
            #[cfg(feature = "provider-candle")]
            {
                runtime_builder = runtime_builder
                    .register_provider(uni_xervo::provider::LocalCandleProvider::new());
            }
            #[cfg(feature = "provider-openai")]
            {
                runtime_builder = runtime_builder
                    .register_provider(uni_xervo::provider::RemoteOpenAIProvider::new());
            }
            #[cfg(feature = "provider-gemini")]
            {
                runtime_builder = runtime_builder
                    .register_provider(uni_xervo::provider::RemoteGeminiProvider::new());
            }
            #[cfg(feature = "provider-vertexai")]
            {
                runtime_builder = runtime_builder
                    .register_provider(uni_xervo::provider::RemoteVertexAIProvider::new());
            }
            #[cfg(feature = "provider-mistral")]
            {
                runtime_builder = runtime_builder
                    .register_provider(uni_xervo::provider::RemoteMistralProvider::new());
            }
            #[cfg(feature = "provider-anthropic")]
            {
                runtime_builder = runtime_builder
                    .register_provider(uni_xervo::provider::RemoteAnthropicProvider::new());
            }
            #[cfg(feature = "provider-voyageai")]
            {
                runtime_builder = runtime_builder
                    .register_provider(uni_xervo::provider::RemoteVoyageAIProvider::new());
            }
            #[cfg(feature = "provider-cohere")]
            {
                runtime_builder = runtime_builder
                    .register_provider(uni_xervo::provider::RemoteCohereProvider::new());
            }
            #[cfg(feature = "provider-azure-openai")]
            {
                runtime_builder = runtime_builder
                    .register_provider(uni_xervo::provider::RemoteAzureOpenAIProvider::new());
            }
            #[cfg(feature = "provider-mistralrs")]
            {
                runtime_builder = runtime_builder
                    .register_provider(uni_xervo::provider::LocalMistralRsProvider::new());
            }
            #[cfg(feature = "provider-onnx")]
            {
                runtime_builder = runtime_builder
                    .register_provider(uni_xervo::provider::LocalOnnxProvider::new());
            }

            Some(
                runtime_builder
                    .build()
                    .await
                    .map_err(|e| UniError::Internal(anyhow::anyhow!(e.to_string())))?,
            )
        } else {
            None
        };

        if let Some(ref runtime) = xervo_runtime {
            writer
                .set_xervo_runtime(runtime.clone())
                .map_err(UniError::Internal)?;
        }

        // Replay WAL to restore any uncommitted mutations from previous session
        // Only replay mutations with LSN > wal_high_water_mark to avoid double-applying
        {
            let replayed = writer
                .replay_wal(wal_high_water_mark)
                .await
                .map_err(UniError::Internal)?;
            if replayed > 0 {
                info!("WAL recovery: replayed {} mutations", replayed);
            }
        }

        // Wire up IndexRebuildManager for post-flush automatic rebuild scheduling
        if self.config.index_rebuild.auto_rebuild_enabled {
            let rebuild_manager = Arc::new(
                uni_store::storage::IndexRebuildManager::new(
                    storage.clone(),
                    schema_manager.clone(),
                    self.config.index_rebuild.clone(),
                )
                .await
                .map_err(UniError::Internal)?,
            );

            let handle = rebuild_manager
                .clone()
                .start_background_worker(shutdown_handle.subscribe());
            shutdown_handle.track_task(handle);

            writer
                .set_index_rebuild_manager(rebuild_manager)
                .map_err(UniError::Internal)?;
        }

        // Start background flush checker for time-based auto-flush
        if let Some(interval) = self.config.auto_flush_interval {
            let writer_clone = writer.clone();
            let mut shutdown_rx = shutdown_handle.subscribe();

            let handle = tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            if let Err(e) = writer_clone.check_flush().await {
                                tracing::warn!("Background flush check failed: {}", e);
                            }
                        }
                        _ = shutdown_rx.recv() => {
                            tracing::info!("Auto-flush shutting down, performing final flush");
                            let _ = writer_clone.flush_to_l1(None).await;
                            break;
                        }
                    }
                }
            });

            shutdown_handle.track_task(handle);
        }

        // Track the FlushCoordinator's single-task finalizer (if async
        // flush is enabled) so Uni::shutdown_blocking awaits its exit.
        // Without this, a graceful shutdown may proceed before the
        // finalizer drains its in-heap submissions — losing some
        // recently-streamed flushes (data is still recoverable via
        // WAL replay on next start, but we'd rather not leak fragments
        // unnecessarily).
        if let Some(coord) = writer.flush_coordinator()
            && let Some(handle) = coord.take_finalizer_handle()
        {
            shutdown_handle.track_task(handle);
        }

        let (commit_tx, _) = tokio::sync::broadcast::channel(256);
        let writer_field = if self.read_only { None } else { Some(writer) };

        // Build the fork registry from the metadata store (the same
        // store the snapshot manager uses), then run recovery before
        // any session is exposed. Recovery resumes any partial fork
        // create or drop left behind by an earlier crash.
        let fork_registry = Arc::new(
            uni_store::fork::ForkRegistryHandle::load(data_store.clone())
                .await
                .map_err(|e| match e {
                    UniError::Internal(inner) => UniError::Internal(inner),
                    other => UniError::Internal(anyhow::anyhow!(other.to_string())),
                })?,
        );
        // Phase 4a: apply the configured fork budget cap.
        fork_registry.set_max_forks(self.config.max_forks).await;
        let storage_uri_for_recovery = storage_uri.clone();
        let recovery_store = storage.store();
        let recovered = uni_store::fork::recovery::recover_forks(
            &fork_registry,
            &recovery_store,
            uni_store::fork::recovery::join_uri_with(storage_uri_for_recovery),
        )
        .await
        .map_err(|e| match e {
            UniError::Internal(inner) => UniError::Internal(inner),
            other => UniError::Internal(anyhow::anyhow!(other.to_string())),
        })?;
        if recovered > 0 {
            tracing::info!(reconciled = recovered, "fork registry recovery completed");
        }

        // Phase 4a: capture sweeper config + a shutdown subscription
        // before the config is consumed into UniInner.
        let sweeper_interval = self.config.fork_sweeper_interval;
        let sweeper_disabled = self.config.disable_fork_sweeper;
        let sweeper_shutdown_rx = shutdown_handle.subscribe();
        // Phase 5a-impl Step 7: same for the fork index builder.
        let index_builder_interval = self.config.fork_index_builder_interval;
        let index_builder_threshold = self.config.fork_index_build_threshold;
        let index_builder_disabled = self.config.disable_fork_index_builder;
        let index_builder_shutdown_rx = shutdown_handle.subscribe();

        // Build the cached DataFusion SessionContext template once with all
        // Cypher UDFs pre-registered. Subsequent queries clone this Arc
        // instead of paying ~140 µs to construct a fresh SessionContext and
        // re-register the UDFs every call.
        let df_session_template = {
            let ctx = datafusion::execution::context::SessionContext::new();
            uni_query_functions::df_udfs::register_cypher_udfs(&ctx)
                .map_err(|e| UniError::Internal(anyhow::anyhow!(e)))?;
            Arc::new(ctx)
        };

        // (The framework-wide plugin registry was built earlier in
        // this function so `PropertyManager` could share it for
        // registry-dispatched CRDT merges. `register_builtin_plugins`
        // already ran there.)
        let procedure_registry = Arc::new(uni_query::ProcedureRegistry::new());
        procedure_registry.set_plugin_registry(Arc::clone(&plugin_registry));

        let executor_template = build_executor_template(
            storage.clone(),
            self.config.clone(),
            writer_field.clone(),
            xervo_runtime.clone(),
            procedure_registry.clone(),
            prop_manager.clone(),
            df_session_template.clone(),
        );

        // M5i: start every registered Connector once at DB build.
        // Failures log + continue — connectors are external wire
        // protocols, not critical paths. Stop hooks fire from
        // `Uni::shutdown`.
        {
            use uni_plugin::traits::connector::ConnectorConfig;
            let connectors = plugin_registry.connectors();
            for c in connectors.iter() {
                let cfg = ConnectorConfig::default();
                match c.start(cfg) {
                    Ok(_handle) => {
                        tracing::debug!(protocol = %c.protocol(), "Connector started");
                    }
                    Err(e) => {
                        tracing::warn!(
                            protocol = %c.protocol(),
                            error = %e,
                            "Connector start failed; continuing without"
                        );
                    }
                }
            }
        }

        // M11 v1 + FU-5: spawn the deferral-queue tick task. When a
        // local `data_path` is available, use the JSON-sidecar
        // persistence backend (`<data_path>/_system/deferred_triggers.json`)
        // so the queue survives restarts; otherwise fall back to the
        // in-memory queue.
        let defer_queue = match persistence_data_path.as_deref() {
            Some(p) => crate::api::triggers::DeferralQueue::with_persistence(p.to_path_buf()),
            None => crate::api::triggers::DeferralQueue::new(),
        };
        // FU-5: replay any persisted items now that triggers have been
        // re-registered by `register_builtin_plugins` + user
        // `add_plugin`s above this point.
        let _restored = defer_queue.load_from_sidecar(&plugin_registry);

        // FU-4: spawn the CDC runtime. Snapshots registered CDC
        // providers, resumes each from its last persisted LSN, and
        // forwards every commit notification as a `CdcBatch`.
        let _cdc_runtime = crate::cdc_runtime::CdcRuntime::spawn(
            &plugin_registry,
            commit_tx.subscribe(),
            persistence_data_path.clone(),
            &shutdown_handle,
        );
        {
            let queue = Arc::clone(&defer_queue);
            let mut shutdown_rx = shutdown_handle.subscribe();
            let handle = tokio::spawn(async move {
                let mut ticker = tokio::time::interval(std::time::Duration::from_millis(50));
                loop {
                    tokio::select! {
                        _ = ticker.tick() => { queue.tick(); }
                        _ = shutdown_rx.recv() => { break; }
                    }
                }
            });
            shutdown_handle.track_task(handle);
        }

        // M11: spawn the background-job scheduler driver. The driver
        // polls `Scheduler::tick_at(now)` every
        // `crate::scheduler::DEFAULT_TICK_INTERVAL`, looks up each due
        // job's `BackgroundJobProvider` in the plugin registry, and
        // dispatches it on `spawn_blocking`. Persistence defaults to
        // `MemoryPersistence` until the durable
        // `SystemLabelPersistence` (writes through
        // `uni_system.background_jobs` via the write-enabled
        // `execute_inner_query`) lands in `uni-query`.
        //
        // M11 A.3: the `SchedulerJobHost` is constructed with the
        // storage manager now and the `UniInner` weak ref later
        // (after the inner is wrapped in an Arc) so built-in jobs can
        // reach host services via `JobContext::host`.
        let scheduler_job_host = Arc::new(crate::scheduler::SchedulerJobHost::new(Arc::clone(
            &storage,
        )));
        // M11 A.6: pick durable scheduler persistence when a
        // local data directory is available; fall back to
        // `MemoryPersistence` for remote / in-memory instances.
        let (scheduler_persistence, scheduler_persist_sink) =
            crate::scheduler_persistence::scheduler_persistence_for_data_path(
                persistence_data_path.as_deref(),
            );
        let scheduler_host = crate::scheduler::SchedulerHost::spawn_with_job_host(
            Arc::clone(&plugin_registry),
            scheduler_persistence,
            &shutdown_handle,
            crate::scheduler::DEFAULT_TICK_INTERVAL,
            Some(Arc::clone(&scheduler_job_host)),
        );

        // M11 B.5: register `uni.periodic.{schedule,cancel,list}`
        // procedures with a `SchedulerControl` trait object pointing
        // at the live scheduler. Registration happens after
        // `SchedulerHost::spawn` so the procedures hold a handle to
        // the actual scheduler the driver loop is polling.
        {
            use uni_plugin::{
                AbiRange, Capability, CapabilitySet, Determinism, PluginId, PluginManifest,
                PluginRegistrar, ProvidedSurfaces, Scope, SideEffects as PluginSideEffects,
            };

            // M11 A.2: hand the periodic procedures a control handle
            // pointing at the host (not the bare `Scheduler` primitive)
            // so `uni.periodic.submit` / `iterate` reach
            // `JobHost::execute_write_cypher` via the
            // `SchedulerHost::submit_cypher` override.
            let scheduler_ctrl: Arc<dyn uni_plugin::scheduler::SchedulerControl> =
                Arc::clone(&scheduler_host) as Arc<dyn uni_plugin::scheduler::SchedulerControl>;
            let plugin_id = PluginId::new("uni");
            let caps =
                CapabilitySet::from_iter_of([Capability::Procedure, Capability::ProcedureWrites]);
            let manifest = PluginManifest {
                id: plugin_id.clone(),
                version: env!("CARGO_PKG_VERSION")
                    .parse()
                    .unwrap_or_else(|_| "1.0.0".parse().expect("static version parses")),
                abi: AbiRange::parse("^1").expect("manifest ABI range is valid"),
                depends_on: vec![],
                capabilities: caps.clone(),
                determinism: Determinism::Pure,
                side_effects: PluginSideEffects::Writes,
                scope: Scope::Instance,
                hash: None,
                signature: None,
                provides: ProvidedSurfaces::default(),
                docs: "uni.periodic.* procedures (M11 B.5).".to_owned(),
                metadata: std::collections::BTreeMap::new(),
            };
            // Apply the host's signature policy before activation. The
            // built-in `uni` plugin ships unsigned today; the default
            // `Disabled` policy accepts it. Embedders that opt into
            // `RequireSigned` must also sign this manifest with a key
            // in their trust root.
            uni_plugin::verify::verify_manifest_with_policy(
                &manifest,
                &uni_plugin::verify::TrustRoot::new(),
                uni_plugin::verify::SignaturePolicy::default(),
            )
            .expect("builtin uni manifest must pass the default Disabled policy");
            let mut r = PluginRegistrar::new(plugin_id, &caps, &plugin_registry);
            uni_plugin_builtin::procedures::periodic::register_into(&mut r, scheduler_ctrl)
                .expect("uni.periodic.* registration");
            r.commit_to_registry().expect("uni.periodic.* commit");
        }

        let db = Uni {
            inner: Arc::new(UniInner {
                storage,
                schema: schema_manager,
                properties: prop_manager,
                writer: writer_field,
                xervo_runtime,
                config: self.config,
                procedure_registry,
                plugin_registry,
                plugins: Arc::new(parking_lot::RwLock::new(HashMap::new())),
                defer_queue,
                scheduler_host: Arc::clone(&scheduler_host),
                shutdown_handle,
                locy_rule_registry: Arc::new(std::sync::RwLock::new(loaded_locy_registry)),
                locy_rule_persister: Some(locy_rule_persister),
                start_time: Instant::now(),
                commit_tx,
                write_lease: self.write_lease,
                plugin_trust: self.plugin_trust,
                active_session_count: AtomicUsize::new(0),
                total_queries: AtomicU64::new(0),
                total_commits: AtomicU64::new(0),
                custom_functions: Arc::new(std::sync::RwLock::new(
                    uni_query::CustomFunctionRegistry::new(),
                )),
                df_session_template,
                executor_template,
                fork_registry,
                fork_inners: Arc::new(DashMap::new()),
                inflight_tx_count: Arc::new(AtomicUsize::new(0)),
                active_connectors: Arc::new(DashMap::new()),
                next_connector_seq: AtomicU64::new(1),
                cached_l0_mutation_count: AtomicUsize::new(0),
                cached_l0_estimated_size: AtomicUsize::new(0),
                cached_wal_lsn: AtomicU64::new(0),
                _temp_dir: self.temp_dir,
                plan_cache: Arc::new(std::sync::Mutex::new(crate::api::session::PlanCache::new(
                    TX_PLAN_CACHE_CAPACITY,
                ))),
            }),
        };

        // The single `HostCypherExecutor` impl the moved plugin-host engines
        // (scheduler job host + persistence sinks) call back through for
        // write-mode Cypher (replaces the per-engine `Weak<UniInner>` they used
        // to hold directly). The executor itself only weakly references
        // `UniInner`, so the host ↔ engine cycle stays leak-free even though the
        // engines hold a strong `Arc<dyn ...>`.
        let host_cypher_exec: Arc<dyn uni_plugin_host::host::HostCypherExecutor> = Arc::new(
            host_executor::UniInnerCypherExecutor::new(Arc::downgrade(&db.inner)),
        );

        // M11 A.3: wire the host Cypher executor into the scheduler's job host
        // so built-in background jobs can reach the host for write-mode Cypher
        // (ttl_sweep, etc.).
        scheduler_job_host.set_host_executor(Arc::clone(&host_cypher_exec));

        // M11 A.7: wire the executor into the meta-plugin persistence sink so
        // subsequent `declareFunction` / `declareProcedure` calls dual-write
        // into the `_DeclaredPlugin` graph label (in addition to the JSON
        // sidecar source-of-truth).
        if let Some(sink) = &custom_persistence_sink {
            sink.set_host_executor(Arc::clone(&host_cypher_exec));
        }
        // M11 A.6: same lazy-wire pattern for the durable scheduler
        // persistence sink (`_BackgroundJob` graph nodes).
        if let Some(sink) = &scheduler_persist_sink {
            sink.set_host_executor(Arc::clone(&host_cypher_exec));
        }

        // Phase 4a: spawn the TTL sweeper (no-op when disabled).
        //
        // The host holds a `Weak<UniInner>` so the task does not extend the
        // database's lifetime; the scheduling/shutdown loop lives in
        // `uni_fork::maintenance`.
        let sweeper_host = Arc::new(fork_maintenance::ForkMaintenanceHostImpl::new(
            Arc::downgrade(&db.inner),
        ));
        if let Some(handle) = uni_fork::maintenance::spawn_sweeper(
            sweeper_host,
            sweeper_interval,
            sweeper_disabled,
            sweeper_shutdown_rx,
        ) {
            db.inner.shutdown_handle.track_task(handle);
        }

        // Phase 5a-impl Step 7: spawn the fork index builder (no-op
        // when disabled).
        let index_builder_host = Arc::new(fork_maintenance::ForkMaintenanceHostImpl::new(
            Arc::downgrade(&db.inner),
        ));
        if let Some(handle) = uni_fork::maintenance::spawn_index_builder(
            index_builder_host,
            index_builder_interval,
            index_builder_threshold,
            index_builder_disabled,
            index_builder_shutdown_rx,
        ) {
            db.inner.shutdown_handle.track_task(handle);
        }

        Ok(db)
    }

    /// Open the database (blocking)
    pub fn build_sync(self) -> Result<Uni> {
        let rt = tokio::runtime::Runtime::new().map_err(UniError::Io)?;
        rt.block_on(self.build())
    }

    fn cloud_config_to_lancedb_storage_options(
        config: &CloudStorageConfig,
    ) -> std::collections::HashMap<String, String> {
        let mut opts = std::collections::HashMap::new();

        match config {
            CloudStorageConfig::S3 {
                bucket,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                session_token,
                virtual_hosted_style,
            } => {
                opts.insert("bucket".to_string(), bucket.clone());
                opts.insert(
                    "virtual_hosted_style_request".to_string(),
                    virtual_hosted_style.to_string(),
                );

                if let Some(r) = region {
                    opts.insert("region".to_string(), r.clone());
                }
                if let Some(ep) = endpoint {
                    opts.insert("endpoint".to_string(), ep.clone());
                    if ep.starts_with("http://") {
                        opts.insert("allow_http".to_string(), "true".to_string());
                    }
                }
                if let Some(v) = access_key_id {
                    opts.insert("access_key_id".to_string(), v.clone());
                }
                if let Some(v) = secret_access_key {
                    opts.insert("secret_access_key".to_string(), v.clone());
                }
                if let Some(v) = session_token {
                    opts.insert("session_token".to_string(), v.clone());
                }
            }
            CloudStorageConfig::Gcs {
                bucket,
                service_account_path,
                service_account_key,
            } => {
                opts.insert("bucket".to_string(), bucket.clone());
                if let Some(v) = service_account_path {
                    opts.insert("service_account".to_string(), v.clone());
                    opts.insert("application_credentials".to_string(), v.clone());
                }
                if let Some(v) = service_account_key {
                    opts.insert("service_account_key".to_string(), v.clone());
                }
            }
            CloudStorageConfig::Azure {
                container,
                account,
                access_key,
                sas_token,
            } => {
                opts.insert("account_name".to_string(), account.clone());
                opts.insert("container_name".to_string(), container.clone());
                if let Some(v) = access_key {
                    opts.insert("access_key".to_string(), v.clone());
                }
                if let Some(v) = sas_token {
                    opts.insert("sas_token".to_string(), v.clone());
                }
            }
        }

        opts
    }
}

#[cfg(test)]
mod fork_inner_tests {
    use super::*;
    use uni_common::core::fork::{ForkId, ForkInfo, SchemaDelta};
    use uni_store::fork::{ForkRegistryHandle, ForkScope};

    /// Smoke test for `UniInner::at_fork`: a fork-scoped inner reads
    /// through the fork's branches and writes through it are gated.
    /// Phase 1 wiring; Day 7's `Session::fork` will exercise it via
    /// the public API end-to-end.
    #[tokio::test]
    async fn at_fork_returns_inner_with_fork_scoped_storage() {
        let db = Uni::in_memory().build().await.unwrap();
        let primary_inner = db.inner.as_ref();

        // Build a registry on a fresh local store. We don't share the
        // primary's object store here — Phase 1's at_fork is a
        // structural test of UniInner construction; the registry only
        // needs to provide an Active ForkInfo to wrap into a ForkScope.
        let dir = tempfile::TempDir::new().unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let registry = Arc::new(ForkRegistryHandle::load(store).await.unwrap());

        let info = ForkInfo::new_pending(ForkId::new(), "smoke", "snap-1", 1);
        registry.begin_create(info).await.unwrap();
        let active = registry
            .finish_create("smoke", Default::default())
            .await
            .unwrap();

        let scope = Arc::new(ForkScope::new(
            Arc::new(active),
            SchemaDelta::empty(),
            registry,
        ));

        let forked_inner = primary_inner.at_fork(scope.clone()).await.unwrap();
        assert!(forked_inner.storage.fork_scope().is_some());
        // Phase 2 Day 4: a forked UniInner now carries its own Writer.
        // The Writer's storage is the fork-scoped clone; its allocator
        // is fork-local.
        let writer = forked_inner
            .writer
            .as_ref()
            .expect("Phase 2 fork must carry its own Writer");
        assert!(
            std::sync::Arc::ptr_eq(&writer.storage, &forked_inner.storage),
            "fork Writer's storage should be the fork-scoped storage"
        );
        // Schema is a *fresh* Arc (overlay-merged), not pointer-equal to primary's.
        assert!(!Arc::ptr_eq(&forked_inner.schema, &primary_inner.schema));

        db.shutdown().await.unwrap();
    }
}
