//! The [`PluginRegistry`] — per-surface trait-object tables.
//!
//! All registrations land here. Reads are wait-free via `arc-swap`; writes
//! are CAS-style. Hot reload swaps a per-plugin entry; queries holding an
//! `Arc::clone()` of the old entry continue against the old version until
//! their reference is dropped.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use smol_str::SmolStr;

use crate::capability::CapabilitySet;
use crate::errors::PluginError;
use crate::plugin::PluginId;
use crate::qname::QName;
use crate::traits::aggregate::{AggSignature, AggregatePluginFn};
use crate::traits::algorithm::AlgorithmProvider;
use crate::traits::background::BackgroundJobProvider;
use crate::traits::catalog::{CatalogProvider, ReplacementScanProvider};
use crate::traits::cdc::CdcOutputProvider;
use crate::traits::collation::CollationProvider;
use crate::traits::connector::{AuthProvider, AuthzPolicy};
use crate::traits::crdt::{CrdtKind, CrdtKindProvider};
use crate::traits::hook::SessionHook;
use crate::traits::index::{IndexHandle, IndexKind, IndexKindProvider};
use crate::traits::locy::{LocyAggregate, LocyPredicate, PredSignature};
use crate::traits::operator::OptimizerRuleProvider;
use crate::traits::procedure::{ProcedurePlugin, ProcedureSignature};
use crate::traits::scalar::{FnSignature, ScalarPluginFn};
use crate::traits::trigger::TriggerPlugin;
use crate::traits::types::LogicalTypeProvider;
use crate::traits::window::{WindowPluginFn, WindowSignature};

/// A single scalar-fn registry entry.
pub struct ScalarEntry {
    /// Owning plugin id.
    pub plugin: PluginId,
    /// Function signature.
    pub signature: FnSignature,
    /// The registered function.
    pub function: Arc<dyn ScalarPluginFn>,
}

impl std::fmt::Debug for ScalarEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScalarEntry")
            .field("plugin", &self.plugin)
            .field("signature", &self.signature)
            .finish_non_exhaustive()
    }
}

/// A single aggregate-fn registry entry.
pub struct AggregateEntry {
    /// Owning plugin id.
    pub plugin: PluginId,
    /// Aggregate signature.
    pub signature: AggSignature,
    /// The registered aggregate.
    pub aggregate: Arc<dyn AggregatePluginFn>,
}

impl std::fmt::Debug for AggregateEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregateEntry")
            .field("plugin", &self.plugin)
            .field("signature", &self.signature)
            .finish_non_exhaustive()
    }
}

/// A single window-fn registry entry.
pub struct WindowEntry {
    /// Owning plugin id.
    pub plugin: PluginId,
    /// Window signature.
    pub signature: WindowSignature,
    /// The registered window function.
    pub window: Arc<dyn WindowPluginFn>,
}

impl std::fmt::Debug for WindowEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowEntry")
            .field("plugin", &self.plugin)
            .field("signature", &self.signature)
            .finish_non_exhaustive()
    }
}

/// A single graph-algorithm registry entry.
///
/// Carries the owning plugin's effective capability set so the CALL
/// dispatcher can enforce host-access grants (e.g. `HostQuery`) when
/// building the algorithm host at invocation time.
pub struct AlgorithmEntry {
    /// Owning plugin id.
    pub plugin: PluginId,
    /// Effective capabilities granted to the owning plugin.
    pub effective_caps: CapabilitySet,
    /// The registered algorithm provider.
    pub provider: Arc<dyn AlgorithmProvider>,
}

impl std::fmt::Debug for AlgorithmEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgorithmEntry")
            .field("plugin", &self.plugin)
            .field("effective_caps", &self.effective_caps)
            .finish_non_exhaustive()
    }
}

/// A single procedure registry entry.
pub struct ProcedureEntry {
    /// Owning plugin id.
    pub plugin: PluginId,
    /// Procedure signature.
    pub signature: ProcedureSignature,
    /// The registered procedure.
    pub procedure: Arc<dyn ProcedurePlugin>,
}

impl std::fmt::Debug for ProcedureEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcedureEntry")
            .field("plugin", &self.plugin)
            .field("signature", &self.signature)
            .finish_non_exhaustive()
    }
}

/// A Locy aggregate entry.
pub struct LocyAggregateEntry {
    /// Owning plugin id.
    pub plugin: PluginId,
    /// The registered aggregate.
    pub aggregate: Arc<dyn LocyAggregate>,
}

impl std::fmt::Debug for LocyAggregateEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocyAggregateEntry")
            .field("plugin", &self.plugin)
            .finish_non_exhaustive()
    }
}

/// A Locy predicate entry.
pub struct LocyPredicateEntry {
    /// Owning plugin id.
    pub plugin: PluginId,
    /// Predicate signature.
    pub signature: PredSignature,
    /// The registered predicate.
    pub predicate: Arc<dyn LocyPredicate>,
}

impl std::fmt::Debug for LocyPredicateEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocyPredicateEntry")
            .field("plugin", &self.plugin)
            .field("signature", &self.signature)
            .finish_non_exhaustive()
    }
}

/// A live index handle keyed by index *name* (e.g., `"vec_idx_embedding"`).
///
/// Unlike `IndexKindProvider`, which is plugin-registered via the
/// `PluginRegistrar` and describes a *kind* of index, an `IndexHandleEntry`
/// represents a *specific* live index — the runtime object produced by
/// `IndexKindProvider::build().finalize()` (or `IndexKindProvider::open()`).
/// Handles are inserted by the host (not by the plugin's `register()` call)
/// because their lifetime tracks the storage layer rather than plugin
/// metadata.
///
/// The planner consults this table by index name when dispatching a vector
/// KNN query (see `plan_vector_knn`). When `Some`, the planner routes the
/// probe through the plugin handle; when `None`, the native storage path
/// runs (preserving the "no behavior change for built-ins" invariant).
#[derive(Clone)]
pub struct IndexHandleEntry {
    /// Kind that produced this handle (informational; matches the
    /// `IndexKindProvider::kind` that built it).
    pub kind: IndexKind,
    /// The live handle.
    pub handle: Arc<dyn IndexHandle>,
}

impl std::fmt::Debug for IndexHandleEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexHandleEntry")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// One slot in the virtual label / edge-type allocation table — bundles
/// the name the planner saw with the `CatalogTable` that owns its rows.
///
/// Used by [`PluginRegistry::register_virtual_label`] / `_edge_type`.
/// Lookups by ID (via `virtual_label_by_id`) return a cheap clone of
/// this entry so the planner's physical-scan layer can route directly
/// to `table.scan(...)` without re-consulting the providers.
#[derive(Clone)]
pub struct VirtualEntry {
    /// The user-typed name (e.g. `"External"`).
    pub name: SmolStr,
    /// The catalog table that owns the rows for this virtual identifier.
    pub table: Arc<dyn crate::traits::catalog::CatalogTable>,
}

impl std::fmt::Debug for VirtualEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VirtualEntry")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// A virtual identifier type (label `u16` or edge-type `u32`) that the
/// allocator can hand out. Captures the per-type `START`/`SENTINEL`
/// bounds and the saturating increment so the allocator body can be
/// written once, generically.
trait VirtualId:
    Copy + Eq + Ord + std::hash::Hash + std::fmt::Debug + std::fmt::LowerHex + 'static
{
    /// First ID handed out (inclusive lower bound of the virtual range).
    const START: Self;
    /// Reserved upper bound (exclusive); reaching it means the space is
    /// exhausted.
    const SENTINEL: Self;
    /// Human-facing label for the kind of identifier, used in the
    /// exhaustion error message (e.g. `"label"`, `"edge-type"`).
    const KIND_LABEL: &'static str;

    /// Increment without overflow (the allocator never relies on the
    /// wrapped value because it bails at `SENTINEL` first).
    fn next(self) -> Self;
}

impl VirtualId for u16 {
    const START: Self = uni_common::core::schema::VIRTUAL_LABEL_ID_START;
    const SENTINEL: Self = uni_common::core::schema::VIRTUAL_LABEL_ID_SENTINEL;
    const KIND_LABEL: &'static str = "label";

    fn next(self) -> Self {
        self.saturating_add(1)
    }
}

impl VirtualId for u32 {
    const START: Self = uni_common::core::edge_type::VIRTUAL_EDGE_TYPE_ID_START;
    const SENTINEL: Self = uni_common::core::edge_type::VIRTUAL_EDGE_TYPE_ID_SENTINEL;
    const KIND_LABEL: &'static str = "edge-type";

    fn next(self) -> Self {
        self.saturating_add(1)
    }
}

/// Inner mutable state for a virtual-ID allocator (labels use `u16`,
/// edge-types use `u32`). Held behind a `parking_lot::Mutex` because
/// allocations are rare (one per first reference to a previously-unseen
/// name) and the contention surface is tiny.
#[derive(Debug)]
struct VirtualIdSpace<Id: VirtualId> {
    name_to_id: HashMap<SmolStr, Id>,
    id_to_entry: HashMap<Id, VirtualEntry>,
    next_id: Id,
}

impl<Id: VirtualId> Default for VirtualIdSpace<Id> {
    fn default() -> Self {
        Self {
            name_to_id: HashMap::new(),
            id_to_entry: HashMap::new(),
            next_id: Id::START,
        }
    }
}

impl<Id: VirtualId> VirtualIdSpace<Id> {
    /// Allocate (or look up) an ID for `name`, replacing the stored
    /// table on re-registration. Returns `Err` when the virtual range is
    /// exhausted.
    fn register(
        &mut self,
        name: SmolStr,
        table: Arc<dyn crate::traits::catalog::CatalogTable>,
    ) -> Result<Id, PluginError> {
        if let Some(&id) = self.name_to_id.get(&name) {
            self.id_to_entry.insert(
                id,
                VirtualEntry {
                    name: name.clone(),
                    table,
                },
            );
            return Ok(id);
        }
        if self.next_id >= Id::SENTINEL {
            return Err(PluginError::Internal(format!(
                "virtual {}-ID space exhausted ({} slots taken; sentinel {:#x})",
                Id::KIND_LABEL,
                self.id_to_entry.len(),
                Id::SENTINEL,
            )));
        }
        let id = self.next_id;
        self.next_id = self.next_id.next();
        self.name_to_id.insert(name.clone(), id);
        self.id_to_entry.insert(id, VirtualEntry { name, table });
        Ok(id)
    }
}

/// Per-plugin record of *what* this plugin registered, for unregister /
/// hot-reload.
///
/// `pub(crate)` (with `pub(crate)` fields) so the family-ops traits in
/// [`crate::surfaces`] can update the record without an accessor for each
/// surface during the Phase 4 migration.
#[derive(Default, Debug)]
pub(crate) struct PluginRecord {
    pub(crate) scalars: Vec<QName>,
    pub(crate) aggregates: Vec<QName>,
    pub(crate) windows: Vec<QName>,
    /// Procedures are arity-overloaded: a given `QName` may be registered
    /// multiple times with different arities (see `procedure_with_arity`).
    /// The `usize` is the procedure's positional argument count, used by
    /// `remove_plugin` to drop the exact overload this plugin owns.
    pub(crate) procedures: Vec<(QName, usize)>,
    pub(crate) locy_aggregates: Vec<QName>,
    pub(crate) locy_predicates: Vec<QName>,
    pub(crate) algorithms: Vec<QName>,
    pub(crate) index_kinds: Vec<IndexKind>,
    pub(crate) label_storages: Vec<SmolStr>,
    pub(crate) crdt_kinds: Vec<CrdtKind>,
    /// Logical-type extension names this plugin registered. Tracked
    /// per-key (not count-only) so `remove_plugin` can drop the entries
    /// on hot reload.
    pub(crate) logical_types: Vec<SmolStr>,
    /// Collation names this plugin registered.
    pub(crate) collations: Vec<SmolStr>,
    /// CDC output sink names this plugin registered.
    pub(crate) cdc_outputs: Vec<SmolStr>,
    /// Catalog names this plugin registered.
    pub(crate) catalogs: Vec<SmolStr>,
    pub(crate) hook_count: usize,
    pub(crate) auth_count: usize,
    pub(crate) authz_count: usize,
    pub(crate) trigger_count: usize,
    pub(crate) replacement_scan_count: usize,
    pub(crate) optimizer_rule_count: usize,
    pub(crate) background_job_count: usize,
}

impl PluginRecord {
    /// Merge another record's surfaces into this one: append every owned-key
    /// vector and sum the count-only tallies.
    ///
    /// `apply_pending` must merge, not overwrite, when a plugin id commits more
    /// than once (e.g. two `declareFunction` calls that each run their own
    /// registrar under the same namespace id). Overwriting the record drops the
    /// earlier commit's surfaces from the ownership map, so `remove_plugin` later
    /// leaks them (they stay live in the registry slots but are untracked).
    fn merge(&mut self, other: PluginRecord) {
        self.scalars.extend(other.scalars);
        self.aggregates.extend(other.aggregates);
        self.windows.extend(other.windows);
        self.procedures.extend(other.procedures);
        self.locy_aggregates.extend(other.locy_aggregates);
        self.locy_predicates.extend(other.locy_predicates);
        self.algorithms.extend(other.algorithms);
        self.index_kinds.extend(other.index_kinds);
        self.label_storages.extend(other.label_storages);
        self.crdt_kinds.extend(other.crdt_kinds);
        self.logical_types.extend(other.logical_types);
        self.collations.extend(other.collations);
        self.cdc_outputs.extend(other.cdc_outputs);
        self.catalogs.extend(other.catalogs);
        self.hook_count += other.hook_count;
        self.auth_count += other.auth_count;
        self.authz_count += other.authz_count;
        self.trigger_count += other.trigger_count;
        self.replacement_scan_count += other.replacement_scan_count;
        self.optimizer_rule_count += other.optimizer_rule_count;
        self.background_job_count += other.background_job_count;
    }
}

/// A deep-clone snapshot of one plugin's registry footprint.
///
/// Produced by [`PluginRegistry::iter_for_plugin`] and consumed by
/// [`crate::reload::ReloadDispatcher`]. The snapshot is **not** kept
/// in sync with the live registry; it represents the surfaces a
/// plugin owned at the moment the snapshot was taken.
#[derive(Clone, Debug, Default)]
pub struct PluginRecordSnapshot {
    /// Scalar fns this plugin registered.
    pub scalars: Vec<QName>,
    /// Aggregate fns this plugin registered.
    pub aggregates: Vec<QName>,
    /// Window fns this plugin registered.
    pub windows: Vec<QName>,
    /// Procedures (qname + arity) this plugin registered.
    pub procedures: Vec<(QName, usize)>,
    /// Locy aggregates this plugin registered.
    pub locy_aggregates: Vec<QName>,
    /// Locy predicates this plugin registered.
    pub locy_predicates: Vec<QName>,
    /// Algorithms this plugin registered.
    pub algorithms: Vec<QName>,
    /// Index kinds this plugin registered.
    pub index_kinds: Vec<IndexKind>,
    /// Label storages this plugin registered.
    pub label_storages: Vec<SmolStr>,
    /// CRDT kinds this plugin registered.
    pub crdt_kinds: Vec<CrdtKind>,
    /// Logical-type extension names this plugin registered.
    pub logical_types: Vec<SmolStr>,
    /// Collation names this plugin registered.
    pub collations: Vec<SmolStr>,
    /// CDC output sink names this plugin registered.
    pub cdc_outputs: Vec<SmolStr>,
    /// Catalog names this plugin registered.
    pub catalogs: Vec<SmolStr>,
    /// Number of `SessionHook`s this plugin registered.
    pub hook_count: usize,
    /// Number of `AuthProvider`s this plugin registered.
    pub auth_count: usize,
    /// Number of `AuthzPolicy`s this plugin registered.
    pub authz_count: usize,
    /// Number of `TriggerPlugin`s this plugin registered.
    pub trigger_count: usize,
    /// Number of `ReplacementScanProvider`s this plugin registered.
    pub replacement_scan_count: usize,
    /// Number of `OptimizerRuleProvider`s this plugin registered.
    pub optimizer_rule_count: usize,
    /// Number of `BackgroundJobProvider`s this plugin registered.
    pub background_job_count: usize,
}

impl From<&PluginRecord> for PluginRecordSnapshot {
    /// Deep-clone a live `PluginRecord` into a standalone snapshot. The
    /// field list lives only on the two struct definitions; this clones
    /// each (`Vec`s deep-clone their elements, counts are `Copy`).
    fn from(r: &PluginRecord) -> Self {
        Self {
            scalars: r.scalars.clone(),
            aggregates: r.aggregates.clone(),
            windows: r.windows.clone(),
            procedures: r.procedures.clone(),
            locy_aggregates: r.locy_aggregates.clone(),
            locy_predicates: r.locy_predicates.clone(),
            algorithms: r.algorithms.clone(),
            index_kinds: r.index_kinds.clone(),
            label_storages: r.label_storages.clone(),
            crdt_kinds: r.crdt_kinds.clone(),
            logical_types: r.logical_types.clone(),
            collations: r.collations.clone(),
            cdc_outputs: r.cdc_outputs.clone(),
            catalogs: r.catalogs.clone(),
            hook_count: r.hook_count,
            auth_count: r.auth_count,
            authz_count: r.authz_count,
            trigger_count: r.trigger_count,
            replacement_scan_count: r.replacement_scan_count,
            optimizer_rule_count: r.optimizer_rule_count,
            background_job_count: r.background_job_count,
        }
    }
}

/// All-surfaces plugin registry.
///
/// Per-surface tables wrapped in `arc-swap` for wait-free reads. The
/// registry tracks per-plugin ownership so `remove_plugin` can clean up
/// all of a plugin's registrations in one pass.
#[derive(Default)]
pub struct PluginRegistry {
    pub(crate) scalars: DashMap<QName, Arc<ScalarEntry>>,
    pub(crate) aggregates: DashMap<QName, Arc<AggregateEntry>>,
    pub(crate) windows: DashMap<QName, Arc<WindowEntry>>,
    /// Procedures keyed by qname. Each qname may carry multiple overload
    /// entries discriminated by `entry.signature.args.len()` so callers can
    /// register two registrations under the same name with different
    /// arities (M5c.2: legacy 5-arg + new 2-arg algorithm signatures).
    /// `procedure(&q)` returns the first registration; arity-aware callers
    /// use `procedure_with_arity(&q, arity)`.
    pub(crate) procedures: DashMap<QName, Vec<Arc<ProcedureEntry>>>,
    pub(crate) locy_aggregates: DashMap<QName, Arc<LocyAggregateEntry>>,
    pub(crate) locy_predicates: DashMap<QName, Arc<LocyPredicateEntry>>,
    pub(crate) optimizer_rules:
        ArcSwap<Vec<crate::surfaces::AppendEntry<dyn OptimizerRuleProvider>>>,
    pub(crate) algorithms: DashMap<QName, Arc<AlgorithmEntry>>,
    pub(crate) index_kinds: DashMap<IndexKind, Arc<dyn IndexKindProvider>>,
    index_handles: DashMap<SmolStr, IndexHandleEntry>,
    /// Per-label plugin storage (M5h.2). Keyed by *label name* and
    /// resolves to an already-open `Storage`. The host's
    /// `StorageManager::scan_vertex_table` consults this map before
    /// falling through to the native backend so a third-party plugin
    /// can serve a native-schema label from its own storage.
    pub(crate) label_storages: DashMap<SmolStr, Arc<dyn crate::traits::storage::Storage>>,
    pub(crate) crdt_kinds: DashMap<CrdtKind, Arc<dyn CrdtKindProvider>>,
    pub(crate) hooks: ArcSwap<Vec<crate::surfaces::AppendEntry<dyn SessionHook>>>,
    pub(crate) logical_types: DashMap<SmolStr, Arc<dyn LogicalTypeProvider>>,
    pub(crate) auth_providers: ArcSwap<Vec<crate::surfaces::AppendEntry<dyn AuthProvider>>>,
    pub(crate) authz_policies: ArcSwap<Vec<crate::surfaces::AppendEntry<dyn AuthzPolicy>>>,
    pub(crate) triggers: ArcSwap<Vec<crate::surfaces::AppendEntry<dyn TriggerPlugin>>>,
    pub(crate) collations: DashMap<SmolStr, Arc<dyn CollationProvider>>,
    pub(crate) cdc_outputs: DashMap<SmolStr, Arc<dyn CdcOutputProvider>>,
    pub(crate) catalogs: DashMap<SmolStr, Arc<dyn CatalogProvider>>,
    pub(crate) replacement_scans:
        ArcSwap<Vec<crate::surfaces::AppendEntry<dyn ReplacementScanProvider>>>,
    pub(crate) background_jobs:
        ArcSwap<Vec<crate::surfaces::AppendEntry<dyn BackgroundJobProvider>>>,
    /// Virtual label-ID allocator. Allocates IDs in the schema's reserved
    /// virtual range (`uni_common::core::schema::VIRTUAL_LABEL_ID_START..
    /// VIRTUAL_LABEL_ID_SENTINEL`) on first observation of an unknown label
    /// name that a `CatalogProvider` or `ReplacementScanProvider` claims.
    /// See [`Self::register_virtual_label`] / [`Self::virtual_label_by_id`].
    virtual_labels: Mutex<VirtualIdSpace<u16>>,
    /// Virtual edge-type allocator. Allocates IDs in
    /// `uni_common::core::edge_type::VIRTUAL_EDGE_TYPE_ID_START..
    /// VIRTUAL_EDGE_TYPE_ID_SENTINEL`. Same first-observation semantics.
    virtual_edge_types: Mutex<VirtualIdSpace<u32>>,
    per_plugin: RwLock<dashmap::DashMap<PluginId, PluginRecord>>,
}

impl std::fmt::Debug for PluginRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginRegistry")
            .field("scalar_fns", &self.scalars.len())
            .field("aggregates", &self.aggregates.len())
            .field("procedures", &self.procedures.len())
            .field("locy_aggregates", &self.locy_aggregates.len())
            .field("algorithms", &self.algorithms.len())
            .field("index_kinds", &self.index_kinds.len())
            .field("hooks", &self.hooks.load().len())
            .field("plugins", &self.per_plugin.read().len())
            .finish()
    }
}

impl PluginRegistry {
    /// Construct an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up a registered scalar function by qname.
    #[must_use]
    pub fn scalar_fn(&self, q: &QName) -> Option<Arc<ScalarEntry>> {
        self.scalars.get(q).map(|e| Arc::clone(e.value()))
    }

    /// Iterate every registered scalar function — `(QName, ScalarEntry)`.
    ///
    /// Collects into a `Vec` so the iteration does not hold a long-lived
    /// reference to the underlying `DashMap` (avoids subtle aliasing
    /// hazards when callers register or remove plugins mid-iteration).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// for (qname, entry) in registry.iter_scalars() {
    ///     ctx.register_udf(ScalarUDF::new_from_impl(adapt(qname, entry)));
    /// }
    /// ```
    #[must_use]
    pub fn iter_scalars(&self) -> Vec<(QName, Arc<ScalarEntry>)> {
        self.scalars
            .iter()
            .map(|kv| (kv.key().clone(), Arc::clone(kv.value())))
            .collect()
    }

    /// Iterate every registered procedure — `(QName, ProcedureEntry)`.
    ///
    /// Arity-overloaded names yield one tuple per registered overload.
    #[must_use]
    pub fn iter_procedures(&self) -> Vec<(QName, Arc<ProcedureEntry>)> {
        self.procedures
            .iter()
            .flat_map(|kv| {
                let q = kv.key().clone();
                kv.value()
                    .iter()
                    .map(move |e| (q.clone(), Arc::clone(e)))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Iterate every registered Locy aggregate — `(QName, LocyAggregateEntry)`.
    #[must_use]
    pub fn iter_locy_aggregates(&self) -> Vec<(QName, Arc<LocyAggregateEntry>)> {
        self.locy_aggregates
            .iter()
            .map(|kv| (kv.key().clone(), Arc::clone(kv.value())))
            .collect()
    }

    /// Iterate every registered algorithm — `(QName, AlgorithmProvider)`.
    #[must_use]
    pub fn iter_algorithms(&self) -> Vec<(QName, Arc<dyn AlgorithmProvider>)> {
        self.algorithms
            .iter()
            .map(|kv| (kv.key().clone(), Arc::clone(&kv.value().provider)))
            .collect()
    }

    /// Iterate every registered index kind — `(IndexKind, IndexKindProvider)`.
    #[must_use]
    pub fn iter_index_kinds(&self) -> Vec<(IndexKind, Arc<dyn IndexKindProvider>)> {
        self.index_kinds
            .iter()
            .map(|kv| (kv.key().clone(), Arc::clone(kv.value())))
            .collect()
    }

    /// Snapshot the registered catalog providers.
    ///
    /// Returns a `Vec` so the iteration does not hold a long-lived reference
    /// to the underlying `DashMap`.
    #[must_use]
    pub fn catalogs(&self) -> Vec<Arc<dyn CatalogProvider>> {
        self.catalogs
            .iter()
            .map(|kv| Arc::clone(kv.value()))
            .collect()
    }

    /// Look up a registered aggregate by qname.
    #[must_use]
    pub fn aggregate(&self, q: &QName) -> Option<Arc<AggregateEntry>> {
        self.aggregates.get(q).map(|e| Arc::clone(e.value()))
    }

    /// Look up a registered window function by qname.
    #[must_use]
    pub fn window(&self, q: &QName) -> Option<Arc<WindowEntry>> {
        self.windows.get(q).map(|e| Arc::clone(e.value()))
    }

    /// Look up a registered procedure by qname.
    ///
    /// If the qname carries multiple arity overloads (M5c.2), this returns
    /// the *first* registered entry, which preserves the legacy
    /// single-arity lookup contract. Arity-aware callers should use
    /// [`Self::procedure_with_arity`] instead.
    #[must_use]
    pub fn procedure(&self, q: &QName) -> Option<Arc<ProcedureEntry>> {
        self.procedures
            .get(q)
            .and_then(|e| e.value().first().map(Arc::clone))
    }

    /// Look up a registered procedure by qname *and* positional argument
    /// count. Returns the entry whose signature has exactly `arity`
    /// arguments, or `None` if no overload matches.
    ///
    /// Procedures may be registered with the same qname under multiple
    /// arities (e.g. an algorithm's legacy 5-arg form alongside the new
    /// `(graphRef, config)` 2-arg form). Resolution sites that know the
    /// call's argument count should prefer this method; the bare
    /// [`Self::procedure`] is preserved for callers that only need the
    /// first registration.
    #[must_use]
    pub fn procedure_with_arity(&self, q: &QName, arity: usize) -> Option<Arc<ProcedureEntry>> {
        self.procedures.get(q).and_then(|e| {
            e.value()
                .iter()
                .find(|entry| entry.signature.args.len() == arity)
                .map(Arc::clone)
        })
    }

    /// Return all arity overloads registered under `q`.
    ///
    /// The returned `Vec` is empty when nothing is registered. Useful for
    /// diagnostic surfaces (e.g. `EXPLAIN` of an ambiguous call) and for
    /// listing API.
    #[must_use]
    pub fn procedure_overloads(&self, q: &QName) -> Vec<Arc<ProcedureEntry>> {
        self.procedures
            .get(q)
            .map(|e| e.value().iter().map(Arc::clone).collect())
            .unwrap_or_default()
    }

    /// Look up a registered Locy aggregate by qname.
    #[must_use]
    pub fn locy_aggregate(&self, q: &QName) -> Option<Arc<LocyAggregateEntry>> {
        self.locy_aggregates.get(q).map(|e| Arc::clone(e.value()))
    }

    /// Look up a registered Locy predicate by qname.
    #[must_use]
    pub fn locy_predicate(&self, q: &QName) -> Option<Arc<LocyPredicateEntry>> {
        self.locy_predicates.get(q).map(|e| Arc::clone(e.value()))
    }

    /// Look up the plugin `Storage` (if any) registered to serve the
    /// given native label name (M5h.2). Consulted by the host's
    /// `StorageManager::scan_vertex_table` before the native backend
    /// fallback — when this returns `Some`, the planner's graph-scan
    /// path is routed through plugin storage instead of Lance.
    #[must_use]
    pub fn lookup_label_storage(
        &self,
        label: &str,
    ) -> Option<Arc<dyn crate::traits::storage::Storage>> {
        self.label_storages
            .get(&SmolStr::new(label))
            .map(|e| Arc::clone(e.value()))
    }

    /// Look up a registered index-kind by kind.
    #[must_use]
    pub fn index_kind(&self, k: &IndexKind) -> Option<Arc<dyn IndexKindProvider>> {
        self.index_kinds.get(k).map(|e| Arc::clone(e.value()))
    }

    /// Register a live `IndexHandle` under an index name.
    ///
    /// The host calls this after building a handle from a custom
    /// `IndexKindProvider` (or after `open()` from persisted bytes). The
    /// planner consults this table from `plan_vector_knn` to route probes
    /// through the plugin handle instead of the native storage path.
    ///
    /// If an entry already exists under the same name, it is replaced.
    pub fn register_index_handle(
        &self,
        name: impl Into<SmolStr>,
        kind: IndexKind,
        handle: Arc<dyn IndexHandle>,
    ) {
        self.index_handles
            .insert(name.into(), IndexHandleEntry { kind, handle });
    }

    /// Look up a live `IndexHandle` by index name. Returns a cheap clone
    /// (the inner handle is `Arc`-wrapped).
    #[must_use]
    pub fn index_handle(&self, name: &str) -> Option<IndexHandleEntry> {
        self.index_handles
            .get(&SmolStr::new(name))
            .map(|e| e.value().clone())
    }

    /// Remove a live `IndexHandle`. Returns the removed entry if one
    /// existed.
    pub fn deregister_index_handle(&self, name: &str) -> Option<IndexHandleEntry> {
        self.index_handles
            .remove(&SmolStr::new(name))
            .map(|(_, v)| v)
    }

    /// Allocate (or look up) a virtual label ID for `name`, owned by
    /// `table`. The host's `QueryPlanner` calls this when an unknown
    /// label name is claimed by a `CatalogProvider` or
    /// `ReplacementScanProvider`; subsequent references to the same name
    /// return the cached ID without re-running discovery.
    ///
    /// Idempotent: a second call with the same name returns the
    /// previously-allocated ID and *replaces* the stored `CatalogTable`
    /// (so cached `LogicalPlan`s naturally pick up the latest table on
    /// next execute). Returns `Err` if the virtual range is exhausted
    /// (255 slots, see `uni_common::core::schema`).
    pub fn register_virtual_label(
        &self,
        name: impl Into<SmolStr>,
        table: Arc<dyn crate::traits::catalog::CatalogTable>,
    ) -> Result<u16, PluginError> {
        self.virtual_labels.lock().register(name.into(), table)
    }

    /// Look up a virtual label by name. Returns `None` if no provider
    /// has claimed it yet (the caller hasn't called
    /// `register_virtual_label`).
    #[must_use]
    pub fn virtual_label_by_name(&self, name: &str) -> Option<u16> {
        let inner = self.virtual_labels.lock();
        inner.name_to_id.get(&SmolStr::new(name)).copied()
    }

    /// Look up the catalog table behind a virtual label ID. Returns the
    /// entry cheaply cloned (inner `Arc<dyn CatalogTable>`).
    #[must_use]
    pub fn virtual_label_by_id(&self, id: u16) -> Option<VirtualEntry> {
        self.virtual_labels.lock().id_to_entry.get(&id).cloned()
    }

    /// Allocate (or look up) a virtual edge-type ID for `name`. Same
    /// semantics as [`Self::register_virtual_label`] but for the
    /// `u32` edge-type ID space.
    pub fn register_virtual_edge_type(
        &self,
        name: impl Into<SmolStr>,
        table: Arc<dyn crate::traits::catalog::CatalogTable>,
    ) -> Result<u32, PluginError> {
        self.virtual_edge_types.lock().register(name.into(), table)
    }

    /// Look up a virtual edge type by name.
    #[must_use]
    pub fn virtual_edge_type_by_name(&self, name: &str) -> Option<u32> {
        let inner = self.virtual_edge_types.lock();
        inner.name_to_id.get(&SmolStr::new(name)).copied()
    }

    /// Look up the catalog table behind a virtual edge-type ID.
    #[must_use]
    pub fn virtual_edge_type_by_id(&self, id: u32) -> Option<VirtualEntry> {
        self.virtual_edge_types.lock().id_to_entry.get(&id).cloned()
    }

    /// Look up a registered algorithm provider by qname.
    #[must_use]
    pub fn algorithm(&self, q: &QName) -> Option<Arc<dyn AlgorithmProvider>> {
        self.algorithms
            .get(q)
            .map(|e| Arc::clone(&e.value().provider))
    }

    /// Look up a registered algorithm's full entry by qname.
    ///
    /// Unlike [`Self::algorithm`], the returned [`AlgorithmEntry`] also
    /// carries the owning plugin's effective capabilities, which the CALL
    /// dispatcher needs to gate host graph access.
    #[must_use]
    pub fn algorithm_entry(&self, q: &QName) -> Option<Arc<AlgorithmEntry>> {
        self.algorithms.get(q).map(|e| Arc::clone(e.value()))
    }

    /// Look up a registered CRDT kind.
    #[must_use]
    pub fn crdt_kind(&self, k: &CrdtKind) -> Option<Arc<dyn CrdtKindProvider>> {
        self.crdt_kinds.get(k).map(|e| Arc::clone(e.value()))
    }

    /// Look up a registered logical type by its Arrow extension name.
    #[must_use]
    pub fn logical_type(&self, name: &SmolStr) -> Option<Arc<dyn LogicalTypeProvider>> {
        self.logical_types.get(name).map(|e| Arc::clone(e.value()))
    }

    /// Snapshot the registered hook chain.
    #[must_use]
    pub fn hooks(&self) -> Arc<Vec<Arc<dyn SessionHook>>> {
        Self::project_append(&self.hooks)
    }

    /// Snapshot the registered optimizer-rule providers (M5h).
    #[must_use]
    pub fn optimizer_rules(&self) -> Arc<Vec<Arc<dyn OptimizerRuleProvider>>> {
        Self::project_append(&self.optimizer_rules)
    }

    /// Snapshot the registered trigger chain.
    #[must_use]
    pub fn triggers(&self) -> Arc<Vec<Arc<dyn TriggerPlugin>>> {
        Self::project_append(&self.triggers)
    }

    /// Snapshot every registered [`CdcOutputProvider`] keyed by name (FU-4).
    ///
    /// Used by `Uni::build` to start a CDC stream per provider before
    /// the commit broadcaster begins pushing `CdcBatch`es.
    #[must_use]
    pub fn cdc_outputs_snapshot(&self) -> Vec<(SmolStr, Arc<dyn CdcOutputProvider>)> {
        self.cdc_outputs
            .iter()
            .map(|e| (e.key().clone(), Arc::clone(e.value())))
            .collect()
    }

    /// `true` when no [`CdcOutputProvider`] is registered.
    ///
    /// Used by the commit hot-path to skip mutation-row materialization
    /// when there are no CDC subscribers — preserves the empty-registry
    /// fast path.
    #[must_use]
    pub fn cdc_outputs_is_empty(&self) -> bool {
        self.cdc_outputs.is_empty()
    }

    /// Snapshot the registered authentication providers (M5i).
    #[must_use]
    pub fn auth_providers(&self) -> Arc<Vec<Arc<dyn AuthProvider>>> {
        Self::project_append(&self.auth_providers)
    }

    /// Snapshot the registered authorization policies (M5i).
    #[must_use]
    pub fn authz_policies(&self) -> Arc<Vec<Arc<dyn AuthzPolicy>>> {
        Self::project_append(&self.authz_policies)
    }

    /// Snapshot the registered replacement-scan providers.
    #[must_use]
    pub fn replacement_scans(&self) -> Arc<Vec<Arc<dyn ReplacementScanProvider>>> {
        Self::project_append(&self.replacement_scans)
    }

    /// Apply a batch of pending registrations atomically.
    ///
    /// Preflights every entry against the live registry first, then
    /// applies them in order. Dispatch is per-family (see
    /// [`crate::surfaces`]): static-typed `*Ops` impls handle storage and
    /// per-plugin record-keeping; the `DynPendingRegistration` boxes
    /// erase the family type so a heterogeneous batch can be queued.
    ///
    /// # Errors
    ///
    /// Returns the first preflight failure (e.g.
    /// [`PluginError::DuplicateRegistration`] or
    /// [`PluginError::StorageSchemeConflict`]); nothing in the batch is
    /// applied in that case.
    pub(crate) fn apply_pending(
        &self,
        plugin_id: &PluginId,
        pending: Vec<Box<dyn crate::surfaces::DynPendingRegistration>>,
    ) -> Result<(), PluginError> {
        // Preflight against the live registry, and — because that only sees the
        // live registry, not the rest of this batch — also reject duplicate
        // unique keys WITHIN the batch (two entries for the same qname in one
        // register() call would otherwise both pass and silently last-write-win).
        let mut seen: std::collections::HashSet<QName> = std::collections::HashSet::new();
        for reg in &pending {
            reg.preflight(self)?;
            if let Some(qname) = reg.dedup_key()
                && !seen.insert(qname.clone())
            {
                return Err(PluginError::DuplicateRegistration(qname));
            }
        }

        let mut record = PluginRecord::default();
        for reg in pending {
            reg.apply(self, plugin_id.clone(), &mut record);
        }

        // Merge (do NOT overwrite) so a second commit under the same plugin id
        // keeps the surfaces the earlier commit registered.
        self.per_plugin
            .read()
            .entry(plugin_id.clone())
            .or_default()
            .merge(record);

        Ok(())
    }

    /// Snapshot the registered background jobs.
    #[must_use]
    pub fn background_jobs(&self) -> Arc<Vec<Arc<dyn BackgroundJobProvider>>> {
        Self::project_append(&self.background_jobs)
    }

    /// Materialize an `Arc<Vec<Arc<dyn P>>>` view of an append-family slot,
    /// stripping the per-entry `AppendEntry` ownership tag.
    ///
    /// The legacy public read-accessor signature returns `Arc<Vec<Arc<dyn
    /// P>>>` for wait-free callers (`hooks()`, `triggers()`, …). The
    /// owner-tagged storage required for proper `remove_plugin`
    /// implementation (closes the M5e gap; see [`crate::surfaces`]
    /// foundation work) carries the plugin id inline, so projecting back to
    /// the legacy shape costs one allocation + N `Arc` clones per call.
    /// Phase 4f will retire this helper in favour of returning the typed
    /// `AppendEntry` slice directly.
    fn project_append<P: ?Sized>(
        slot: &ArcSwap<Vec<crate::surfaces::AppendEntry<P>>>,
    ) -> Arc<Vec<Arc<P>>> {
        let snap = slot.load();
        let v: Vec<Arc<P>> = snap.iter().map(|e| Arc::clone(&e.provider)).collect();
        Arc::new(v)
    }

    /// Snapshot the surfaces a plugin currently owns.
    ///
    /// Returns `None` when the plugin has never registered anything (or
    /// has already been removed). Used by
    /// [`crate::reload::ReloadDispatcher`] to determine which per-kind
    /// reload protocols to invoke for the old plugin.
    ///
    /// The snapshot is a deep clone of the registry's internal
    /// `PluginRecord`; mutating the registry afterward does not affect
    /// the snapshot.
    #[must_use]
    pub fn iter_for_plugin(&self, plugin: &PluginId) -> Option<PluginRecordSnapshot> {
        let guard = self.per_plugin.read();
        guard.get(plugin).map(|r| PluginRecordSnapshot::from(&*r))
    }

    /// Remove a single named-unique surface (scalar or aggregate) that `plugin`
    /// registered under `qname`, leaving the plugin's other surfaces intact.
    ///
    /// [`Self::remove_plugin`] drops an entire plugin id at once; declared-function
    /// stores pack many functions under one namespace id (e.g. `mycorp.f1`,
    /// `mycorp.f2` both under `mycorp`), so dropping one must not unregister its
    /// siblings. It is also used to drop the prior entry when a declared qname is
    /// re-declared, so re-registration is not mistaken for shadowing a native fn.
    ///
    /// Returns whether anything was removed.
    pub fn remove_named_unique(&self, plugin: &PluginId, qname: &QName) -> bool {
        use crate::surfaces::{AggregateSurface, NamedUniqueOps, ScalarSurface};
        let mut removed = false;
        if let Some(mut rec) = self.per_plugin.read().get_mut(plugin) {
            if let Some(pos) = rec.scalars.iter().position(|q| q == qname) {
                rec.scalars.remove(pos);
                <ScalarSurface as NamedUniqueOps>::remove(self, qname);
                removed = true;
            }
            if let Some(pos) = rec.aggregates.iter().position(|q| q == qname) {
                rec.aggregates.remove(pos);
                <AggregateSurface as NamedUniqueOps>::remove(self, qname);
                removed = true;
            }
        }
        removed
    }

    /// Remove all registrations for the given plugin.
    ///
    /// Used by `Uni::remove_plugin` and as part of hot reload's drain step.
    /// Dispatches per family via the `*Ops` traits in [`crate::surfaces`];
    /// the label-storage / logical-type / collation / cdc / catalog
    /// surfaces are dropped here too (the per-key tracking lifts the old
    /// count-only gap where hot reload leaked entries on those slots).
    pub fn remove_plugin(&self, plugin: &PluginId) {
        use crate::surfaces::{
            AggregateSurface, AlgorithmSurface, AppendOps, AuthSurface, AuthzSurface,
            BackgroundJobSurface, CatalogSurface, CdcSurface, CollationSurface, CrdtSurface,
            Discriminator, HookSurface, IndexKindSurface, KeyedUniqueOps, LabelStorageSurface,
            LocyAggregateSurface, LocyPredicateSurface, LogicalTypeSurface, NamedUniqueOps,
            OptimizerRuleSurface, ProcedureSurface, ReplacementScanSurface, ScalarSurface,
            TriggerSurface, VersionedOps, WindowSurface,
        };

        let record = self.per_plugin.read().remove(plugin).map(|(_, r)| r);
        let Some(record) = record else { return };

        for q in record.scalars {
            <ScalarSurface as NamedUniqueOps>::remove(self, &q);
        }
        for q in record.aggregates {
            <AggregateSurface as NamedUniqueOps>::remove(self, &q);
        }
        for q in record.windows {
            <WindowSurface as NamedUniqueOps>::remove(self, &q);
        }
        for (q, arity) in record.procedures {
            <ProcedureSurface as VersionedOps>::remove(self, &q, Discriminator::Arity(arity));
        }
        for q in record.locy_aggregates {
            <LocyAggregateSurface as NamedUniqueOps>::remove(self, &q);
        }
        for q in record.locy_predicates {
            <LocyPredicateSurface as NamedUniqueOps>::remove(self, &q);
        }
        for q in record.algorithms {
            <AlgorithmSurface as NamedUniqueOps>::remove(self, &q);
        }
        for k in record.index_kinds {
            <IndexKindSurface as KeyedUniqueOps>::remove(self, &k);
        }
        for l in record.label_storages {
            <LabelStorageSurface as KeyedUniqueOps>::remove(self, &l);
        }
        for k in record.crdt_kinds {
            <CrdtSurface as KeyedUniqueOps>::remove(self, &k);
        }
        for k in record.logical_types {
            <LogicalTypeSurface as KeyedUniqueOps>::remove(self, &k);
        }
        for k in record.collations {
            <CollationSurface as KeyedUniqueOps>::remove(self, &k);
        }
        for k in record.cdc_outputs {
            <CdcSurface as KeyedUniqueOps>::remove(self, &k);
        }
        for k in record.catalogs {
            <CatalogSurface as KeyedUniqueOps>::remove(self, &k);
        }

        <OptimizerRuleSurface as AppendOps>::remove_plugin(self, plugin);
        <HookSurface as AppendOps>::remove_plugin(self, plugin);
        <AuthSurface as AppendOps>::remove_plugin(self, plugin);
        <AuthzSurface as AppendOps>::remove_plugin(self, plugin);
        <TriggerSurface as AppendOps>::remove_plugin(self, plugin);
        <ReplacementScanSurface as AppendOps>::remove_plugin(self, plugin);
        <BackgroundJobSurface as AppendOps>::remove_plugin(self, plugin);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_default_is_empty() {
        let r = PluginRegistry::new();
        assert!(r.scalar_fn(&QName::builtin("anything")).is_none());
        assert!(r.procedure(&QName::builtin("anything")).is_none());
        assert_eq!(r.hooks().len(), 0);
    }

    #[test]
    fn debug_smoke() {
        let r = PluginRegistry::new();
        let s = format!("{r:?}");
        assert!(s.contains("PluginRegistry"));
    }
}
