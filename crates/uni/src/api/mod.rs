// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uni_common::core::fork::ForkId;

pub mod appender;
pub mod builder;
pub mod bulk;
pub mod compaction;
pub mod fork;
pub mod fork_diff;
pub(crate) mod fork_index_builder;
pub mod fork_schema;
pub(crate) mod fork_sweeper;
pub mod functions;
pub mod hooks;
pub mod impl_locy;
pub mod impl_query;
pub mod indexes;
pub mod locy_builder;
pub mod locy_result;
pub mod multi_agent;
pub mod notifications;
pub mod prepared;
pub mod query_builder;
pub mod rule_registry;
pub mod schema;
pub mod session;
pub mod sync;
pub mod template;
pub mod transaction;
pub mod xervo;

use object_store::ObjectStore;
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

/// Shared inner state of a Uni database instance.
///
/// Wrapped in `Arc` by [`Uni`] so that [`Session`](session::Session) and
/// [`Transaction`](transaction::Transaction) can hold cheap, owned references
/// without lifetime parameters.
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
    pub(crate) shutdown_handle: Arc<ShutdownHandle>,
    /// Global registry of pre-compiled Locy rules.
    ///
    /// Cloned into every new Session. Use `db.register_rules()` to add rules
    /// globally, or `session.register_rules()` for session-scoped rules.
    pub(crate) locy_rule_registry: Arc<std::sync::RwLock<impl_locy::LocyRuleRegistry>>,
    /// Timestamp when this database instance was built.
    pub(crate) start_time: Instant,
    /// Broadcast channel for commit notifications.
    pub(crate) commit_tx: tokio::sync::broadcast::Sender<Arc<notifications::CommitNotification>>,
    /// Write lease configuration for multi-agent access.
    pub(crate) write_lease: Option<multi_agent::WriteLease>,
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
}

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

        let prop_manager = Arc::new(PropertyManager::new(
            pinned_storage.clone(),
            self.schema.clone(),
            self.properties.cache_size(),
        ));

        let shutdown_handle = Arc::new(ShutdownHandle::new(Duration::from_secs(30)));

        let (commit_tx, _) = tokio::sync::broadcast::channel(256);
        let executor_template = build_executor_template(
            pinned_storage.clone(),
            self.config.clone(),
            None,
            self.xervo_runtime.clone(),
            self.procedure_registry.clone(),
            prop_manager.clone(),
            self.df_session_template.clone(),
        );
        Ok(UniInner {
            storage: pinned_storage,
            schema: self.schema.clone(),
            properties: prop_manager,
            writer: None,
            xervo_runtime: self.xervo_runtime.clone(),
            config: self.config.clone(),
            procedure_registry: self.procedure_registry.clone(),
            shutdown_handle,
            locy_rule_registry: Arc::new(std::sync::RwLock::new(
                impl_locy::LocyRuleRegistry::default(),
            )),
            start_time: Instant::now(),
            commit_tx,
            write_lease: None,
            active_session_count: AtomicUsize::new(0),
            total_queries: AtomicU64::new(0),
            total_commits: AtomicU64::new(0),
            custom_functions: self.custom_functions.clone(),
            df_session_template: self.df_session_template.clone(),
            executor_template,
            fork_registry: self.fork_registry.clone(),
            fork_inners: self.fork_inners.clone(),
            inflight_tx_count: Arc::new(AtomicUsize::new(0)),
            cached_l0_mutation_count: AtomicUsize::new(0),
            cached_l0_estimated_size: AtomicUsize::new(0),
            cached_wal_lsn: AtomicU64::new(0),
            _temp_dir: None,
        })
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

        let prop_manager = Arc::new(PropertyManager::new(
            forked_storage.clone(),
            merged_schema.clone(),
            self.properties.cache_size(),
        ));

        let shutdown_handle = Arc::new(ShutdownHandle::new(Duration::from_secs(30)));
        let (commit_tx, _) = tokio::sync::broadcast::channel(256);

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
            self.config.clone(),
        )
        .await
        .map_err(UniError::Internal)?;

        // Phase 2 Day 6: replay any persisted WAL entries for this
        // fork into the freshly-built L0. Without this, a process
        // restart would silently drop committed-but-not-yet-flushed
        // fork mutations. `replay_wal(0)` replays from the beginning;
        // the WAL's own LSN tracking (initialized inside the writer
        // factory) advances correctly past durable segments.
        let replayed = forked_writer
            .replay_wal(0)
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
        Ok(UniInner {
            storage: forked_storage,
            schema: merged_schema,
            properties: prop_manager,
            writer: Some(forked_writer_arc),
            xervo_runtime: self.xervo_runtime.clone(),
            config: self.config.clone(),
            procedure_registry: self.procedure_registry.clone(),
            shutdown_handle,
            locy_rule_registry: rule_registry,
            start_time: Instant::now(),
            commit_tx,
            write_lease: None,
            active_session_count: AtomicUsize::new(0),
            total_queries: AtomicU64::new(0),
            total_commits: AtomicU64::new(0),
            custom_functions: self.custom_functions.clone(),
            df_session_template: self.df_session_template.clone(),
            executor_template,
            fork_registry: self.fork_registry.clone(),
            fork_inners: self.fork_inners.clone(),
            inflight_tx_count: Arc::new(AtomicUsize::new(0)),
            cached_l0_mutation_count: AtomicUsize::new(0),
            cached_l0_estimated_size: AtomicUsize::new(0),
            cached_wal_lsn: AtomicU64::new(0),
            _temp_dir: None,
        })
    }
}

impl Uni {
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
        for i in 0..100 {
            if self.inner.fork_registry.holder_count_for(preview.id).await == 0 {
                break;
            }
            if i < 20 {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
        let info = self.inner.fork_registry.begin_drop(name).await?;
        // Phase 2 Day 8: evict the cached `Weak<UniInner>` (if any)
        // before deleting branches. The registry has already
        // transitioned the fork to Tombstoned, so concurrent
        // `fork(name)` calls now error out before reaching the cache;
        // this eviction is purely cleanup so the map doesn't accumulate
        // dead Weak entries across the lifetime of the database.
        self.inner.fork_inners.remove(&info.id);
        // Step 3: walk branches and force-delete each.
        let storage_uri = self.inner.storage.base_uri().to_string();
        for (dataset, branch) in &info.datasets {
            let dataset_uri = if storage_uri.ends_with('/') {
                format!("{storage_uri}{dataset}.lance")
            } else {
                format!("{storage_uri}/{dataset}.lance")
            };
            if let Err(e) =
                uni_store::backend::lance_branch::delete_branch(&dataset_uri, branch).await
            {
                tracing::warn!(
                    dataset = %dataset,
                    branch = %branch,
                    "delete_branch during drop_fork failed: {e}"
                );
            }
        }
        // Step 4 + 5: clear the registry entry, delete tombstone +
        // schema overlay files.
        self.inner.fork_registry.finish_drop(&info).await?;
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
            let mut holders = self.inner.fork_registry.holder_count_for(node.id).await;
            if holders > 0 {
                for i in 0..100 {
                    if i < 20 {
                        tokio::task::yield_now().await;
                    } else {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    holders = self.inner.fork_registry.holder_count_for(node.id).await;
                    if holders == 0 {
                        break;
                    }
                }
            }
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
            match pat {
                fork_diff::PromotePattern::Vertex { label, .. } => {
                    if !primary_schema.labels.contains_key(label) {
                        return Err(UniError::LabelNotFound {
                            label: label.clone(),
                        });
                    }
                }
                fork_diff::PromotePattern::Edge { edge_type, .. } => {
                    if !primary_schema.edge_types.contains_key(edge_type) {
                        return Err(UniError::EdgeTypeNotFound {
                            edge_type: edge_type.clone(),
                        });
                    }
                }
            }
        }
        let primary_tx = primary.tx().await?;
        let report = fork_diff::run_promote(&fork, &primary, &primary_tx, patterns).await?;
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
            let dataset_uri = if storage_uri.ends_with('/') {
                format!("{storage_uri}{dataset}.lance")
            } else {
                format!("{storage_uri}/{dataset}.lance")
            };
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
            let dataset_uri = if storage_uri.ends_with('/') {
                format!("{storage_uri}{dataset}.lance")
            } else {
                format!("{storage_uri}/{dataset}.lance")
            };
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
            let dataset_uri = if storage_uri.ends_with('/') {
                format!("{storage_uri}{dataset}.lance")
            } else {
                format!("{storage_uri}/{dataset}.lance")
            };
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
        rule_registry::RuleRegistry::new(&self.inner.locy_rule_registry)
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
        Ok(self
            .inner
            .schema
            .schema()
            .labels
            .get(name)
            .is_some_and(|l| {
                matches!(
                    l.state,
                    uni_common::core::schema::SchemaElementState::Active
                )
            }))
    }

    /// Check if an edge type exists in the schema.
    pub async fn edge_type_exists(&self, name: &str) -> Result<bool> {
        Ok(self
            .inner
            .schema
            .schema()
            .edge_types
            .get(name)
            .is_some_and(|e| {
                matches!(
                    e.state,
                    uni_common::core::schema::SchemaElementState::Active
                )
            }))
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

            let mut properties = Vec::new();
            if let Some(props) = schema.properties.get(name) {
                for (prop_name, prop_meta) in props {
                    let is_indexed = schema.indexes.iter().any(|idx| match idx {
                        uni_common::core::schema::IndexDefinition::Vector(v) => {
                            v.label == name && v.property == *prop_name
                        }
                        uni_common::core::schema::IndexDefinition::Scalar(s) => {
                            s.label == name && s.properties.contains(prop_name)
                        }
                        uni_common::core::schema::IndexDefinition::FullText(f) => {
                            f.label == name && f.properties.contains(prop_name)
                        }
                        uni_common::core::schema::IndexDefinition::Inverted(inv) => {
                            inv.label == name && inv.property == *prop_name
                        }
                        uni_common::core::schema::IndexDefinition::JsonFullText(j) => {
                            j.label == name
                        }
                        _ => false,
                    });

                    properties.push(crate::api::schema::PropertyInfo {
                        name: prop_name.clone(),
                        data_type: format!("{:?}", prop_meta.r#type),
                        nullable: prop_meta.nullable,
                        is_indexed,
                        description: prop_meta.description.clone(),
                    });
                }
            }

            let mut indexes = Vec::new();
            for idx in schema.indexes.iter().filter(|i| i.label() == name) {
                use uni_common::core::schema::IndexDefinition;
                let (idx_type, idx_props) = match idx {
                    IndexDefinition::Vector(v) => ("VECTOR", vec![v.property.clone()]),
                    IndexDefinition::Scalar(s) => ("SCALAR", s.properties.clone()),
                    IndexDefinition::FullText(f) => ("FULLTEXT", f.properties.clone()),
                    IndexDefinition::Inverted(inv) => ("INVERTED", vec![inv.property.clone()]),
                    IndexDefinition::JsonFullText(j) => ("JSON_FTS", vec![j.column.clone()]),
                    _ => continue,
                };

                indexes.push(crate::api::schema::IndexInfo {
                    name: idx.name().to_string(),
                    index_type: idx_type.to_string(),
                    properties: idx_props,
                    status: "ONLINE".to_string(), // TODO: Check actual status
                });
            }

            let mut constraints = Vec::new();
            for c in &schema.constraints {
                if let uni_common::core::schema::ConstraintTarget::Label(l) = &c.target
                    && l == name
                {
                    let (ctype, cprops) = match &c.constraint_type {
                        uni_common::core::schema::ConstraintType::Unique { properties } => {
                            ("UNIQUE", properties.clone())
                        }
                        uni_common::core::schema::ConstraintType::Exists { property } => {
                            ("EXISTS", vec![property.clone()])
                        }
                        uni_common::core::schema::ConstraintType::Check { expression } => {
                            ("CHECK", vec![expression.clone()])
                        }
                        _ => ("UNKNOWN", vec![]),
                    };

                    constraints.push(crate::api::schema::ConstraintInfo {
                        name: c.name.clone(),
                        constraint_type: ctype.to_string(),
                        properties: cprops,
                        enabled: c.enabled,
                    });
                }
            }

            Ok(Some(crate::api::schema::LabelInfo {
                name: name.to_string(),
                count,
                properties,
                indexes,
                constraints,
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

        let mut properties = Vec::new();
        if let Some(props) = schema.properties.get(name) {
            for (prop_name, prop_meta) in props {
                let is_indexed = schema.indexes.iter().any(|idx| match idx {
                    uni_common::core::schema::IndexDefinition::Scalar(s) => {
                        s.label == name && s.properties.contains(prop_name)
                    }
                    uni_common::core::schema::IndexDefinition::FullText(f) => {
                        f.label == name && f.properties.contains(prop_name)
                    }
                    uni_common::core::schema::IndexDefinition::Inverted(inv) => {
                        inv.label == name && inv.property == *prop_name
                    }
                    _ => false,
                });

                properties.push(crate::api::schema::PropertyInfo {
                    name: prop_name.clone(),
                    data_type: format!("{:?}", prop_meta.r#type),
                    nullable: prop_meta.nullable,
                    is_indexed,
                    description: prop_meta.description.clone(),
                });
            }
        }

        let mut indexes = Vec::new();
        for idx in schema.indexes.iter().filter(|i| i.label() == name) {
            use uni_common::core::schema::IndexDefinition;
            let (idx_type, idx_props) = match idx {
                IndexDefinition::Scalar(s) => ("SCALAR", s.properties.clone()),
                IndexDefinition::FullText(f) => ("FULLTEXT", f.properties.clone()),
                IndexDefinition::Inverted(inv) => ("INVERTED", vec![inv.property.clone()]),
                _ => continue,
            };

            indexes.push(crate::api::schema::IndexInfo {
                name: idx.name().to_string(),
                index_type: idx_type.to_string(),
                properties: idx_props,
                status: "ONLINE".to_string(),
            });
        }

        let mut constraints = Vec::new();
        for c in &schema.constraints {
            if let uni_common::core::schema::ConstraintTarget::EdgeType(et) = &c.target
                && et == name
            {
                let (ctype, cprops) = match &c.constraint_type {
                    uni_common::core::schema::ConstraintType::Unique { properties } => {
                        ("UNIQUE", properties.clone())
                    }
                    uni_common::core::schema::ConstraintType::Exists { property } => {
                        ("EXISTS", vec![property.clone()])
                    }
                    uni_common::core::schema::ConstraintType::Check { expression } => {
                        ("CHECK", vec![expression.clone()])
                    }
                    _ => ("UNKNOWN", vec![]),
                };

                constraints.push(crate::api::schema::ConstraintInfo {
                    name: c.name.clone(),
                    constraint_type: ctype.to_string(),
                    properties: cprops,
                    enabled: c.enabled,
                });
            }
        }

        Ok(Some(crate::api::schema::EdgeTypeInfo {
            name: name.to_string(),
            count,
            source_labels,
            target_labels,
            properties,
            indexes,
            constraints,
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
        // Flush pending data
        if let Some(writer) = &self.inner.writer
            && let Err(e) = writer.flush_to_l1(None).await
        {
            tracing::error!("Error flushing during shutdown: {}", e);
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
    temp_dir: Option<TempDir>,
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
            temp_dir: None,
        }
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

        // Initialize property manager
        let prop_cache_capacity = self.config.cache_size / 1024;

        let prop_manager = Arc::new(PropertyManager::new(
            storage.clone(),
            schema_manager.clone(),
            prop_cache_capacity,
        ));

        // Setup stores for WAL and IdAllocator (needed for version recovery check)
        let id_store = local_store_opt
            .clone()
            .unwrap_or_else(|| data_store.clone());
        let wal_store = local_store_opt
            .clone()
            .unwrap_or_else(|| data_store.clone());

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

        let wal = if !self.config.wal_enabled {
            // WAL disabled by config
            None
        } else if is_remote_uri && !is_hybrid {
            // Remote-only WAL (ObjectStoreWal)
            Some(Arc::new(WriteAheadLog::new(
                wal_store,
                object_store::path::Path::from("wal"),
            )))
        } else if is_hybrid || !is_remote_uri {
            // Local WAL (using local_store)
            // Even if local_store uses ObjectStore trait, it maps to FS.
            Some(Arc::new(WriteAheadLog::new(
                wal_store,
                object_store::path::Path::from("wal"),
            )))
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
        let recovered = uni_store::fork::recovery::recover_forks(
            &fork_registry,
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

        let procedure_registry = Arc::new(uni_query::ProcedureRegistry::new());
        let executor_template = build_executor_template(
            storage.clone(),
            self.config.clone(),
            writer_field.clone(),
            xervo_runtime.clone(),
            procedure_registry.clone(),
            prop_manager.clone(),
            df_session_template.clone(),
        );

        let db = Uni {
            inner: Arc::new(UniInner {
                storage,
                schema: schema_manager,
                properties: prop_manager,
                writer: writer_field,
                xervo_runtime,
                config: self.config,
                procedure_registry,
                shutdown_handle,
                locy_rule_registry: Arc::new(std::sync::RwLock::new(
                    impl_locy::LocyRuleRegistry::default(),
                )),
                start_time: Instant::now(),
                commit_tx,
                write_lease: self.write_lease,
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
                cached_l0_mutation_count: AtomicUsize::new(0),
                cached_l0_estimated_size: AtomicUsize::new(0),
                cached_wal_lsn: AtomicU64::new(0),
                _temp_dir: self.temp_dir,
            }),
        };

        // Phase 4a: spawn the TTL sweeper (no-op when disabled).
        if let Some(handle) = fork_sweeper::spawn(
            db.inner.clone(),
            sweeper_interval,
            sweeper_disabled,
            sweeper_shutdown_rx,
        ) {
            db.inner.shutdown_handle.track_task(handle);
        }

        // Phase 5a-impl Step 7: spawn the fork index builder (no-op
        // when disabled).
        if let Some(handle) = fork_index_builder::spawn(
            db.inner.clone(),
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
