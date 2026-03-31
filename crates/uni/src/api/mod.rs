// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

pub mod appender;
pub mod builder;
pub mod bulk;
pub mod compaction;
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

use tokio::sync::RwLock;
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
    pub(crate) writer: Option<Arc<RwLock<Writer>>>,
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

    // ── Cached metrics (updated on commit, read by sync `metrics()`) ─────
    /// Cached L0 mutation count (updated after every commit).
    pub(crate) cached_l0_mutation_count: AtomicUsize,
    /// Cached L0 estimated size in bytes (updated after every commit).
    pub(crate) cached_l0_estimated_size: AtomicUsize,
    /// Cached WAL log sequence number (updated after every commit).
    pub(crate) cached_wal_lsn: AtomicU64,
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
            cached_l0_mutation_count: AtomicUsize::new(0),
            cached_l0_estimated_size: AtomicUsize::new(0),
            cached_wal_lsn: AtomicU64::new(0),
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
    /// Note: Currently uses a temporary directory on the filesystem.
    pub fn temporary() -> UniBuilder {
        let temp_dir = std::env::temp_dir().join(format!("uni_mem_{}", uuid::Uuid::new_v4()));
        UniBuilder::new(temp_dir.to_string_lossy().to_string())
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
    pub fn writer(&self) -> Option<Arc<RwLock<Writer>>> {
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
        if let Some(writer_lock) = &self.inner.writer {
            let mut writer = writer_lock.write().await;
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

        let snapshot_id = if let Some(writer_lock) = &self.inner.writer {
            let mut writer = writer_lock.write().await;
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
        if schema.labels.contains_key(name) {
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

    /// Shutdown the database gracefully, flushing pending data and stopping background tasks.
    ///
    /// This method flushes any pending data and waits for all background tasks to complete
    /// (with a timeout). After calling this method, the database instance should not be used.
    pub async fn shutdown(self) -> Result<()> {
        // Flush pending data
        if let Some(ref writer) = self.inner.writer {
            let mut w = writer.write().await;
            if let Err(e) = w.flush_to_l1(None).await {
                tracing::error!("Error flushing during shutdown: {}", e);
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
    hybrid_remote_url: Option<String>,
    cloud_config: Option<CloudStorageConfig>,
    create_if_missing: bool,
    fail_if_exists: bool,
    read_only: bool,
    write_lease: Option<multi_agent::WriteLease>,
}

impl UniBuilder {
    /// Creates a new builder for the given URI.
    pub fn new(uri: String) -> Self {
        Self {
            uri,
            config: UniConfig::default(),
            schema_file: None,
            xervo_catalog: None,
            hybrid_remote_url: None,
            cloud_config: None,
            create_if_missing: true,
            fail_if_exists: false,
            read_only: false,
            write_lease: None,
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

        let writer = Arc::new(RwLock::new(
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
        ));

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

        let xervo_runtime = if let Some(catalog) = self.xervo_catalog {
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

            let mut runtime_builder = ModelRuntime::builder().catalog(catalog);
            #[cfg(feature = "provider-candle")]
            {
                runtime_builder = runtime_builder
                    .register_provider(uni_xervo::provider::LocalCandleProvider::new());
            }
            #[cfg(feature = "provider-fastembed")]
            {
                runtime_builder = runtime_builder
                    .register_provider(uni_xervo::provider::LocalFastEmbedProvider::new());
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
            let mut writer_guard = writer.write().await;
            writer_guard.set_xervo_runtime(runtime.clone());
        }

        // Replay WAL to restore any uncommitted mutations from previous session
        // Only replay mutations with LSN > wal_high_water_mark to avoid double-applying
        {
            let w = writer.read().await;
            let replayed = w
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

            {
                let mut writer_guard = writer.write().await;
                writer_guard.set_index_rebuild_manager(rebuild_manager);
            }
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
                            let mut w = writer_clone.write().await;
                            if let Err(e) = w.check_flush().await {
                                tracing::warn!("Background flush check failed: {}", e);
                            }
                        }
                        _ = shutdown_rx.recv() => {
                            tracing::info!("Auto-flush shutting down, performing final flush");
                            let mut w = writer_clone.write().await;
                            let _ = w.flush_to_l1(None).await;
                            break;
                        }
                    }
                }
            });

            shutdown_handle.track_task(handle);
        }

        let (commit_tx, _) = tokio::sync::broadcast::channel(256);
        let writer_field = if self.read_only { None } else { Some(writer) };

        Ok(Uni {
            inner: Arc::new(UniInner {
                storage,
                schema: schema_manager,
                properties: prop_manager,
                writer: writer_field,
                xervo_runtime,
                config: self.config,
                procedure_registry: Arc::new(uni_query::ProcedureRegistry::new()),
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
                cached_l0_mutation_count: AtomicUsize::new(0),
                cached_l0_estimated_size: AtomicUsize::new(0),
                cached_wal_lsn: AtomicU64::new(0),
            }),
        })
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
