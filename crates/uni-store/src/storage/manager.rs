// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::compaction::{CompactionStats, CompactionStatus, CompactionTask};
use crate::lancedb::LanceDbStore;
use crate::runtime::WorkingGraph;
use crate::runtime::context::QueryContext;
use crate::runtime::l0::L0Buffer;
use crate::storage::adjacency::AdjacencyDataset;
use crate::storage::compaction::Compactor;
use crate::storage::delta::{DeltaDataset, ENTRY_SIZE_ESTIMATE, Op};
use crate::storage::direction::Direction;
use crate::storage::edge::EdgeDataset;
use crate::storage::index::UidIndex;
use crate::storage::inverted_index::InvertedIndex;
use crate::storage::main_edge::MainEdgeDataset;
use crate::storage::main_vertex::MainVertexDataset;
use crate::storage::vertex::VertexDataset;
use anyhow::{Result, anyhow};
use arrow_array::{Array, Float32Array, TimestampNanosecondArray, UInt64Array};
use dashmap::DashMap;
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;
use uni_common::config::UniConfig;
use uni_common::core::id::{Eid, UniId, Vid};
use uni_common::core::schema::{DistanceMetric, IndexDefinition, SchemaManager};
use uni_common::sync::acquire_mutex;

use crate::snapshot::manager::SnapshotManager;
use crate::storage::IndexManager;
use crate::storage::adjacency_manager::AdjacencyManager;
use crate::storage::resilient_store::ResilientObjectStore;

use uni_common::core::snapshot::SnapshotManifest;

use uni_common::graph::simple_graph::Direction as GraphDirection;

/// Edge state during subgraph loading - tracks version and deletion status.
struct EdgeState {
    neighbor: Vid,
    version: u64,
    deleted: bool,
}

pub struct StorageManager {
    base_uri: String,
    store: Arc<dyn ObjectStore>,
    schema_manager: Arc<SchemaManager>,
    snapshot_manager: Arc<SnapshotManager>,
    adjacency_manager: Arc<AdjacencyManager>,
    /// Cache of opened LanceDB tables by label name for vector search performance
    table_cache: DashMap<String, lancedb::Table>,
    pub config: UniConfig,
    pub compaction_status: Arc<Mutex<CompactionStatus>>,
    /// Optional pinned snapshot for time-travel
    pinned_snapshot: Option<SnapshotManifest>,
    /// LanceDB store for DataFusion-powered queries.
    lancedb_store: Arc<LanceDbStore>,
    /// In-memory VID-to-labels index for O(1) lookups (optional, configurable)
    vid_labels_index: Option<Arc<parking_lot::RwLock<crate::storage::vid_labels::VidLabelsIndex>>>,
}

/// Helper to manage compaction_in_progress flag
struct CompactionGuard {
    status: Arc<Mutex<CompactionStatus>>,
}

impl CompactionGuard {
    fn new(status: Arc<Mutex<CompactionStatus>>) -> Option<Self> {
        let mut s = acquire_mutex(&status, "compaction_status").ok()?;
        if s.compaction_in_progress {
            return None;
        }
        s.compaction_in_progress = true;
        Some(Self {
            status: status.clone(),
        })
    }
}

impl Drop for CompactionGuard {
    fn drop(&mut self) {
        // CRITICAL: Never panic in Drop - panicking in drop() = process ABORT.
        // See issue #18/#150. If the lock is poisoned, log and continue gracefully.
        match uni_common::sync::acquire_mutex(&self.status, "compaction_status") {
            Ok(mut s) => {
                s.compaction_in_progress = false;
                s.last_compaction = Some(std::time::SystemTime::now());
            }
            Err(e) => {
                // Lock is poisoned but we're in Drop - cannot panic.
                // Log the error and continue. System state may be inconsistent but at least
                // we don't abort the process.
                log::error!(
                    "CompactionGuard drop failed to acquire poisoned lock: {}. \
                     Compaction status may be inconsistent. Issue #18/#150",
                    e
                );
            }
        }
    }
}

impl StorageManager {
    /// Create a new StorageManager with LanceDB integration.
    pub async fn new(base_uri: &str, schema_manager: Arc<SchemaManager>) -> Result<Self> {
        Self::new_with_config(base_uri, schema_manager, UniConfig::default()).await
    }

    /// Create a new StorageManager with custom cache size.
    pub async fn new_with_cache(
        base_uri: &str,
        schema_manager: Arc<SchemaManager>,
        adjacency_cache_size: usize,
    ) -> Result<Self> {
        let config = UniConfig {
            cache_size: adjacency_cache_size,
            ..Default::default()
        };
        Self::new_with_config(base_uri, schema_manager, config).await
    }

    /// Create a new StorageManager with custom configuration.
    pub async fn new_with_config(
        base_uri: &str,
        schema_manager: Arc<SchemaManager>,
        config: UniConfig,
    ) -> Result<Self> {
        let store = Self::build_store_from_uri(base_uri)?;
        Self::new_with_store_and_config(base_uri, store, schema_manager, config).await
    }

    /// Create a new StorageManager using an already-constructed object store.
    ///
    /// This is used by higher layers that need explicit store configuration
    /// (for example custom S3 endpoints in hybrid/cloud modes).
    pub async fn new_with_store_and_config(
        base_uri: &str,
        store: Arc<dyn ObjectStore>,
        schema_manager: Arc<SchemaManager>,
        config: UniConfig,
    ) -> Result<Self> {
        Self::new_with_store_and_storage_options(base_uri, store, schema_manager, config, None)
            .await
    }

    /// Create a new StorageManager using an already-constructed object store
    /// and explicit LanceDB storage options.
    pub async fn new_with_store_and_storage_options(
        base_uri: &str,
        store: Arc<dyn ObjectStore>,
        schema_manager: Arc<SchemaManager>,
        config: UniConfig,
        lancedb_storage_options: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        let resilient_store: Arc<dyn ObjectStore> = Arc::new(ResilientObjectStore::new(
            store,
            config.object_store.clone(),
        ));

        let snapshot_manager = Arc::new(SnapshotManager::new(resilient_store.clone()));

        // Connect to LanceDB
        let lancedb_store =
            LanceDbStore::connect_with_storage_options(base_uri, lancedb_storage_options).await?;

        // Perform crash recovery for all known table patterns
        Self::recover_all_staging_tables(&lancedb_store, &schema_manager).await?;

        let mut sm = Self {
            base_uri: base_uri.to_string(),
            store: resilient_store,
            schema_manager,
            snapshot_manager,
            adjacency_manager: Arc::new(AdjacencyManager::new(config.cache_size)),
            table_cache: DashMap::new(),
            config,
            compaction_status: Arc::new(Mutex::new(CompactionStatus::default())),
            pinned_snapshot: None,
            lancedb_store: Arc::new(lancedb_store),
            vid_labels_index: None,
        };

        // Rebuild VidLabelsIndex if enabled
        if sm.config.enable_vid_labels_index
            && let Err(e) = sm.rebuild_vid_labels_index().await
        {
            warn!(
                "Failed to rebuild VidLabelsIndex on startup: {}. Falling back to LanceDB queries.",
                e
            );
        }

        Ok(sm)
    }

    /// Recover all staging tables for known table patterns.
    ///
    /// This runs on startup to handle crash recovery. It checks for staging tables
    /// for all vertex labels, adjacency tables, delta tables, and main tables.
    async fn recover_all_staging_tables(
        lancedb_store: &LanceDbStore,
        schema_manager: &SchemaManager,
    ) -> Result<()> {
        let schema = schema_manager.schema();

        // Recover main vertex and edge tables
        lancedb_store
            .recover_staging(LanceDbStore::main_vertex_table_name())
            .await?;
        lancedb_store
            .recover_staging(LanceDbStore::main_edge_table_name())
            .await?;

        // Recover per-label vertex tables
        for label in schema.labels.keys() {
            let table_name = LanceDbStore::vertex_table_name(label);
            lancedb_store.recover_staging(&table_name).await?;
        }

        // Recover adjacency and delta tables for each edge type and direction
        for edge_type in schema.edge_types.keys() {
            for direction in &["fwd", "bwd"] {
                // Recover delta tables
                let delta_table_name = LanceDbStore::delta_table_name(edge_type, direction);
                lancedb_store.recover_staging(&delta_table_name).await?;

                // Recover adjacency tables for each label
                for _label in schema.labels.keys() {
                    let adj_table_name = LanceDbStore::adjacency_table_name(edge_type, direction);
                    lancedb_store.recover_staging(&adj_table_name).await?;
                }
            }
        }

        Ok(())
    }

    fn build_store_from_uri(base_uri: &str) -> Result<Arc<dyn ObjectStore>> {
        if base_uri.contains("://") {
            let parsed = url::Url::parse(base_uri).map_err(|e| anyhow!("Invalid base URI: {e}"))?;
            let (store, _path) = object_store::parse_url(&parsed)
                .map_err(|e| anyhow!("Failed to parse object store URL: {e}"))?;
            Ok(Arc::from(store))
        } else {
            // If local path, ensure it exists.
            std::fs::create_dir_all(base_uri)?;
            Ok(Arc::new(LocalFileSystem::new_with_prefix(base_uri)?))
        }
    }

    pub fn pinned(&self, snapshot: SnapshotManifest) -> Self {
        Self {
            base_uri: self.base_uri.clone(),
            store: self.store.clone(),
            schema_manager: self.schema_manager.clone(),
            snapshot_manager: self.snapshot_manager.clone(),
            // Separate AdjacencyManager for snapshot isolation (Issue #73):
            // warm() will load only edges visible at the snapshot's HWM.
            // This prevents live DB's CSR (with all edges) from leaking into snapshots.
            adjacency_manager: Arc::new(AdjacencyManager::new(self.adjacency_manager.max_bytes())),
            table_cache: DashMap::new(),
            config: self.config.clone(),
            compaction_status: Arc::new(Mutex::new(CompactionStatus::default())),
            pinned_snapshot: Some(snapshot),
            lancedb_store: self.lancedb_store.clone(),
            vid_labels_index: self.vid_labels_index.clone(),
        }
    }

    pub fn get_edge_version_by_id(&self, edge_type_id: u32) -> Option<u64> {
        let schema = self.schema_manager.schema();
        let name = schema.edge_type_name_by_id(edge_type_id)?;
        self.pinned_snapshot
            .as_ref()
            .and_then(|s| s.edges.get(name).map(|es| es.lance_version))
    }

    /// Returns the version_high_water_mark from the pinned snapshot if present.
    ///
    /// This is used for time-travel queries to filter data by version.
    /// When a snapshot is pinned, only rows with `_version <= version_high_water_mark`
    /// should be considered visible.
    pub fn version_high_water_mark(&self) -> Option<u64> {
        self.pinned_snapshot
            .as_ref()
            .map(|s| s.version_high_water_mark)
    }

    /// Apply version filtering to a base filter expression.
    ///
    /// If a snapshot is pinned, wraps `base_filter` with an additional
    /// `_version <= hwm` clause. Otherwise returns `base_filter` unchanged.
    pub fn apply_version_filter(&self, base_filter: String) -> String {
        if let Some(hwm) = self.version_high_water_mark() {
            format!("({}) AND (_version <= {})", base_filter, hwm)
        } else {
            base_filter
        }
    }

    /// Build a filter expression that excludes soft-deleted rows and optionally
    /// includes a user-provided filter.
    fn build_active_filter(user_filter: Option<&str>) -> String {
        match user_filter {
            Some(expr) => format!("({}) AND (_deleted = false)", expr),
            None => "_deleted = false".to_string(),
        }
    }

    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Get current compaction status.
    ///
    /// # Errors
    ///
    /// Returns error if the compaction status lock is poisoned (see issue #18/#150).
    pub fn compaction_status(
        &self,
    ) -> Result<CompactionStatus, uni_common::sync::LockPoisonedError> {
        let guard = uni_common::sync::acquire_mutex(&self.compaction_status, "compaction_status")?;
        Ok(guard.clone())
    }

    pub async fn compact(&self) -> Result<CompactionStats> {
        // LanceDB handles compaction internally via optimize()
        // For now, call optimize on vertex tables
        let start = std::time::Instant::now();
        let schema = self.schema_manager.schema();
        let mut files_compacted = 0;

        for label in schema.labels.keys() {
            let table_name = LanceDbStore::vertex_table_name(label);
            if self.lancedb_store.table_exists(&table_name).await? {
                let table = self.lancedb_store.open_table(&table_name).await?;
                table.optimize(lancedb::table::OptimizeAction::All).await?;
                files_compacted += 1;
                self.invalidate_table_cache(label);
            }
        }

        Ok(CompactionStats {
            files_compacted,
            bytes_before: 0,
            bytes_after: 0,
            duration: start.elapsed(),
            crdt_merges: 0,
        })
    }

    pub async fn compact_label(&self, label: &str) -> Result<CompactionStats> {
        let _guard = CompactionGuard::new(self.compaction_status.clone())
            .ok_or_else(|| anyhow!("Compaction already in progress"))?;

        let start = std::time::Instant::now();
        let table_name = LanceDbStore::vertex_table_name(label);

        if self.lancedb_store.table_exists(&table_name).await? {
            let table = self.lancedb_store.open_table(&table_name).await?;
            table.optimize(lancedb::table::OptimizeAction::All).await?;
            self.invalidate_table_cache(label);
        }

        Ok(CompactionStats {
            files_compacted: 1,
            bytes_before: 0,
            bytes_after: 0,
            duration: start.elapsed(),
            crdt_merges: 0,
        })
    }

    pub async fn compact_edge_type(&self, edge_type: &str) -> Result<CompactionStats> {
        let _guard = CompactionGuard::new(self.compaction_status.clone())
            .ok_or_else(|| anyhow!("Compaction already in progress"))?;

        let start = std::time::Instant::now();
        let mut files_compacted = 0;

        for dir in ["fwd", "bwd"] {
            let table_name = LanceDbStore::delta_table_name(edge_type, dir);
            if self.lancedb_store.table_exists(&table_name).await? {
                let table = self.lancedb_store.open_table(&table_name).await?;
                table.optimize(lancedb::table::OptimizeAction::All).await?;
                files_compacted += 1;
            }
        }

        Ok(CompactionStats {
            files_compacted,
            bytes_before: 0,
            bytes_after: 0,
            duration: start.elapsed(),
            crdt_merges: 0,
        })
    }

    pub async fn wait_for_compaction(&self) -> Result<()> {
        loop {
            let in_progress = {
                acquire_mutex(&self.compaction_status, "compaction_status")?.compaction_in_progress
            };
            if !in_progress {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    pub fn start_background_compaction(
        self: Arc<Self>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        if !self.config.compaction.enabled {
            return tokio::spawn(async {});
        }

        tokio::spawn(async move {
            // Use interval_at to delay the first tick. tokio::time::interval fires
            // immediately on the first tick, which can race with queries that run
            // right after database open. Delaying by the check_interval gives
            // initial queries time to complete before compaction modifies tables
            // (optimize(All) can GC index files that concurrent queries depend on).
            let start = tokio::time::Instant::now() + self.config.compaction.check_interval;
            let mut interval =
                tokio::time::interval_at(start, self.config.compaction.check_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = self.update_compaction_status().await {
                            log::error!("Failed to update compaction status: {}", e);
                            continue;
                        }

                        if let Some(task) = self.pick_compaction_task() {
                            log::info!("Triggering background compaction: {:?}", task);
                            if let Err(e) = Self::execute_compaction(Arc::clone(&self), task).await {
                                log::error!("Compaction failed: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        log::info!("Background compaction shutting down");
                        let _ = self.wait_for_compaction().await;
                        break;
                    }
                }
            }
        })
    }

    async fn update_compaction_status(&self) -> Result<()> {
        let schema = self.schema_manager.schema();
        let mut total_tables = 0;
        let mut total_rows: usize = 0;
        let mut oldest_ts: Option<i64> = None;

        for name in schema.edge_types.keys() {
            for dir in ["fwd", "bwd"] {
                let table_name = LanceDbStore::delta_table_name(name, dir);
                let Ok(table) = self.lancedb_store.open_table(&table_name).await else {
                    continue;
                };
                let row_count = table.count_rows(None).await.unwrap_or(0);
                if row_count == 0 {
                    continue;
                }
                total_tables += 1;
                total_rows += row_count;

                // Query oldest _created_at for age tracking
                let Ok(stream) = table
                    .query()
                    .select(Select::Columns(vec!["_created_at".to_string()]))
                    .execute()
                    .await
                else {
                    continue;
                };
                let Ok(batches) = stream.try_collect::<Vec<_>>().await else {
                    continue;
                };
                for batch in batches {
                    let Some(col) = batch
                        .column_by_name("_created_at")
                        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
                    else {
                        continue;
                    };
                    for i in 0..col.len() {
                        if !col.is_null(i) {
                            let ts = col.value(i);
                            oldest_ts = Some(oldest_ts.map_or(ts, |prev| prev.min(ts)));
                        }
                    }
                }
            }
        }

        let oldest_l1_age = oldest_ts
            .and_then(|ts| {
                let created = UNIX_EPOCH + Duration::from_nanos(ts as u64);
                SystemTime::now().duration_since(created).ok()
            })
            .unwrap_or(Duration::ZERO);

        let mut status = acquire_mutex(&self.compaction_status, "compaction_status")?;
        status.l1_runs = total_tables;
        status.l1_size_bytes = (total_rows * ENTRY_SIZE_ESTIMATE) as u64;
        status.oldest_l1_age = oldest_l1_age;
        Ok(())
    }

    fn pick_compaction_task(&self) -> Option<CompactionTask> {
        let status = acquire_mutex(&self.compaction_status, "compaction_status").ok()?;

        if status.l1_runs >= self.config.compaction.max_l1_runs {
            return Some(CompactionTask::ByRunCount);
        }
        if status.l1_size_bytes >= self.config.compaction.max_l1_size_bytes {
            return Some(CompactionTask::BySize);
        }
        if status.oldest_l1_age >= self.config.compaction.max_l1_age
            && status.oldest_l1_age > Duration::ZERO
        {
            return Some(CompactionTask::ByAge);
        }

        None
    }

    /// Open a table by name and run `optimize(All)`, returning `true` on success.
    async fn optimize_table(store: &LanceDbStore, table_name: &str) -> bool {
        let Ok(table) = store.open_table(table_name).await else {
            return false;
        };
        if let Err(e) = table.optimize(lancedb::table::OptimizeAction::All).await {
            log::warn!("Failed to optimize table {}: {}", table_name, e);
            return false;
        }
        true
    }

    async fn execute_compaction(this: Arc<Self>, _task: CompactionTask) -> Result<CompactionStats> {
        let start = std::time::Instant::now();
        let _guard = CompactionGuard::new(this.compaction_status.clone())
            .ok_or_else(|| anyhow!("Compaction already in progress"))?;

        let schema = this.schema_manager.schema();
        let mut files_compacted = 0;

        // ── Tier 2: Semantic compaction ──
        // Dedup vertices, merge CRDTs, consolidate L1→L2 deltas, clean tombstones
        let compactor = Compactor::new(Arc::clone(&this));
        let compaction_results = compactor.compact_all().await.unwrap_or_else(|e| {
            log::error!(
                "Semantic compaction failed (continuing with Lance optimize): {}",
                e
            );
            Vec::new()
        });

        // Re-warm adjacency CSR after semantic compaction
        let am = this.adjacency_manager();
        for info in &compaction_results {
            let direction = match info.direction.as_str() {
                "fwd" => Direction::Outgoing,
                "bwd" => Direction::Incoming,
                _ => continue,
            };
            if let Some(etid) = schema.edge_type_id_unified_case_insensitive(&info.edge_type)
                && let Err(e) = am.warm(&this, etid, direction, None).await
            {
                log::warn!(
                    "Failed to re-warm adjacency for {}/{}: {}",
                    info.edge_type,
                    info.direction,
                    e
                );
            }
        }

        // ── Tier 3: Lance optimize ──
        let store = &this.lancedb_store;

        // Optimize edge delta and adjacency tables
        for name in schema.edge_types.keys() {
            for dir in ["fwd", "bwd"] {
                let delta = LanceDbStore::delta_table_name(name, dir);
                if Self::optimize_table(store, &delta).await {
                    files_compacted += 1;
                }
                let adj = LanceDbStore::adjacency_table_name(name, dir);
                if Self::optimize_table(store, &adj).await {
                    files_compacted += 1;
                }
            }
        }

        // Optimize vertex tables
        for label in schema.labels.keys() {
            let table_name = LanceDbStore::vertex_table_name(label);
            if Self::optimize_table(store, &table_name).await {
                files_compacted += 1;
                this.invalidate_table_cache(label);
            }
        }

        // Optimize main vertex and edge tables
        for table_name in [
            LanceDbStore::main_vertex_table_name(),
            LanceDbStore::main_edge_table_name(),
        ] {
            if Self::optimize_table(store, table_name).await {
                files_compacted += 1;
            }
        }

        {
            let mut status = acquire_mutex(&this.compaction_status, "compaction_status")?;
            status.total_compactions += 1;
        }

        Ok(CompactionStats {
            files_compacted,
            bytes_before: 0,
            bytes_after: 0,
            duration: start.elapsed(),
            crdt_merges: 0,
        })
    }

    /// Get or open a cached table for a label
    pub async fn get_cached_table(&self, label: &str) -> Result<lancedb::Table> {
        // Check cache first
        if let Some(table) = self.table_cache.get(label) {
            return Ok(table.clone());
        }

        // Open and cache
        let table_name = LanceDbStore::vertex_table_name(label);
        let table = self.lancedb_store.open_table(&table_name).await?;

        self.table_cache.insert(label.to_string(), table.clone());
        Ok(table)
    }

    /// Invalidate cached table (call after writes)
    pub fn invalidate_table_cache(&self, label: &str) {
        self.table_cache.remove(label);
    }

    /// Clear all cached tables
    pub fn clear_table_cache(&self) {
        self.table_cache.clear();
    }

    pub fn base_path(&self) -> &str {
        &self.base_uri
    }

    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }

    pub fn schema_manager_arc(&self) -> Arc<SchemaManager> {
        self.schema_manager.clone()
    }

    /// Get the adjacency manager for the dual-CSR architecture.
    pub fn adjacency_manager(&self) -> Arc<AdjacencyManager> {
        Arc::clone(&self.adjacency_manager)
    }

    /// Warm the adjacency manager for a specific edge type and direction.
    ///
    /// Builds the Main CSR from L2 adjacency + L1 delta data in storage.
    /// Called lazily on first access per edge type or at startup.
    pub async fn warm_adjacency(
        &self,
        edge_type_id: u32,
        direction: crate::storage::direction::Direction,
        version: Option<u64>,
    ) -> anyhow::Result<()> {
        self.adjacency_manager
            .warm(self, edge_type_id, direction, version)
            .await
    }

    /// Coalesced warm_adjacency() to prevent cache stampede (Issue #13).
    ///
    /// Uses double-checked locking to ensure only one concurrent warm() per
    /// (edge_type, direction) key. Subsequent callers wait for the first to complete.
    pub async fn warm_adjacency_coalesced(
        &self,
        edge_type_id: u32,
        direction: crate::storage::direction::Direction,
        version: Option<u64>,
    ) -> anyhow::Result<()> {
        self.adjacency_manager
            .warm_coalesced(self, edge_type_id, direction, version)
            .await
    }

    /// Check whether the adjacency manager has a CSR for the given edge type and direction.
    pub fn has_adjacency_csr(
        &self,
        edge_type_id: u32,
        direction: crate::storage::direction::Direction,
    ) -> bool {
        self.adjacency_manager.has_csr(edge_type_id, direction)
    }

    /// Get neighbors at a specific version for snapshot queries.
    pub fn get_neighbors_at_version(
        &self,
        vid: uni_common::core::id::Vid,
        edge_type: u32,
        direction: crate::storage::direction::Direction,
        version: u64,
    ) -> Vec<(uni_common::core::id::Vid, uni_common::core::id::Eid)> {
        self.adjacency_manager
            .get_neighbors_at_version(vid, edge_type, direction, version)
    }

    /// Get the LanceDB store.
    pub fn lancedb_store(&self) -> &LanceDbStore {
        &self.lancedb_store
    }

    /// Get the LanceDB store as an Arc.
    pub fn lancedb_store_arc(&self) -> Arc<LanceDbStore> {
        self.lancedb_store.clone()
    }

    /// Rebuild the VidLabelsIndex from the main vertex table.
    /// This is called on startup if enable_vid_labels_index is true.
    async fn rebuild_vid_labels_index(&mut self) -> Result<()> {
        use crate::lancedb::LanceDbStore;
        use crate::storage::vid_labels::VidLabelsIndex;

        let lancedb_store = self.lancedb_store();

        // Open the main vertex table
        let table = match lancedb_store
            .open_table(LanceDbStore::main_vertex_table_name())
            .await
        {
            Ok(t) => t,
            Err(_) => {
                // Table doesn't exist yet (fresh database)
                self.vid_labels_index =
                    Some(Arc::new(parking_lot::RwLock::new(VidLabelsIndex::new())));
                return Ok(());
            }
        };

        // Scan all non-deleted vertices and collect (VID, labels)
        let batches = table
            .query()
            .only_if("_deleted = false")
            .limit(100_000) // Reasonable batch size
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to query main vertex table: {}", e))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| anyhow!("Failed to collect vertex data: {}", e))?;

        let mut index = VidLabelsIndex::new();
        for batch in batches {
            let vid_col = batch
                .column_by_name("_vid")
                .ok_or_else(|| anyhow!("Missing _vid column"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| anyhow!("Invalid _vid column type"))?;

            let labels_col = batch
                .column_by_name("labels")
                .ok_or_else(|| anyhow!("Missing labels column"))?
                .as_any()
                .downcast_ref::<arrow_array::ListArray>()
                .ok_or_else(|| anyhow!("Invalid labels column type"))?;

            for row_idx in 0..batch.num_rows() {
                let vid = Vid::from(vid_col.value(row_idx));
                let labels_array = labels_col.value(row_idx);
                let labels_str_array = labels_array
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| anyhow!("Invalid labels array element type"))?;

                let labels: Vec<String> = (0..labels_str_array.len())
                    .map(|i| labels_str_array.value(i).to_string())
                    .collect();

                index.insert(vid, labels);
            }
        }

        self.vid_labels_index = Some(Arc::new(parking_lot::RwLock::new(index)));
        Ok(())
    }

    /// Get labels for a VID from the in-memory index.
    /// Returns None if the index is disabled or the VID is not found.
    pub fn get_labels_from_index(&self, vid: Vid) -> Option<Vec<String>> {
        self.vid_labels_index.as_ref().and_then(|idx| {
            let index = idx.read();
            index.get_labels(vid).map(|labels| labels.to_vec())
        })
    }

    /// Update the VID-to-labels mapping in the index.
    /// No-op if the index is disabled.
    pub fn update_vid_labels_index(&self, vid: Vid, labels: Vec<String>) {
        if let Some(idx) = &self.vid_labels_index {
            let mut index = idx.write();
            index.insert(vid, labels);
        }
    }

    /// Remove a VID from the labels index.
    /// No-op if the index is disabled.
    pub fn remove_from_vid_labels_index(&self, vid: Vid) {
        if let Some(idx) = &self.vid_labels_index {
            let mut index = idx.write();
            index.remove_vid(vid);
        }
    }

    pub async fn load_subgraph_cached(
        &self,
        start_vids: &[Vid],
        edge_types: &[u32],
        max_hops: usize,
        direction: GraphDirection,
        _l0: Option<Arc<RwLock<L0Buffer>>>,
    ) -> Result<WorkingGraph> {
        let mut graph = WorkingGraph::new();

        let dir = match direction {
            GraphDirection::Outgoing => crate::storage::direction::Direction::Outgoing,
            GraphDirection::Incoming => crate::storage::direction::Direction::Incoming,
        };

        let neighbor_is_dst = matches!(direction, GraphDirection::Outgoing);

        // Initialize frontier
        let mut frontier: Vec<Vid> = start_vids.to_vec();
        let mut visited: HashSet<Vid> = HashSet::new();

        // Initialize start vids
        for &vid in start_vids {
            graph.add_vertex(vid);
        }

        for _hop in 0..max_hops {
            let mut next_frontier = HashSet::new();

            for &vid in &frontier {
                if visited.contains(&vid) {
                    continue;
                }
                visited.insert(vid);
                graph.add_vertex(vid);

                for &etype_id in edge_types {
                    // Warm adjacency with coalescing to prevent cache stampede (Issue #13)
                    let edge_ver = self.version_high_water_mark();
                    self.adjacency_manager
                        .warm_coalesced(self, etype_id, dir, edge_ver)
                        .await?;

                    // Get neighbors from AdjacencyManager (Main CSR + overlay)
                    let edges = self.adjacency_manager.get_neighbors(vid, etype_id, dir);

                    for (neighbor_vid, eid) in edges {
                        graph.add_vertex(neighbor_vid);
                        if !visited.contains(&neighbor_vid) {
                            next_frontier.insert(neighbor_vid);
                        }

                        if neighbor_is_dst {
                            graph.add_edge(vid, neighbor_vid, eid, etype_id);
                        } else {
                            graph.add_edge(neighbor_vid, vid, eid, etype_id);
                        }
                    }
                }
            }
            frontier = next_frontier.into_iter().collect();

            // Early termination: if frontier is empty, no more vertices to explore
            if frontier.is_empty() {
                break;
            }
        }

        Ok(graph)
    }

    pub fn snapshot_manager(&self) -> &SnapshotManager {
        &self.snapshot_manager
    }

    pub fn index_manager(&self) -> IndexManager {
        IndexManager::new(
            &self.base_uri,
            self.schema_manager.clone(),
            self.lancedb_store.clone(),
        )
    }

    pub fn vertex_dataset(&self, label: &str) -> Result<VertexDataset> {
        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;
        Ok(VertexDataset::new(&self.base_uri, label, label_meta.id))
    }

    pub fn edge_dataset(
        &self,
        edge_type: &str,
        src_label: &str,
        dst_label: &str,
    ) -> Result<EdgeDataset> {
        Ok(EdgeDataset::new(
            &self.base_uri,
            edge_type,
            src_label,
            dst_label,
        ))
    }

    pub fn delta_dataset(&self, edge_type: &str, direction: &str) -> Result<DeltaDataset> {
        Ok(DeltaDataset::new(&self.base_uri, edge_type, direction))
    }

    pub fn adjacency_dataset(
        &self,
        edge_type: &str,
        label: &str,
        direction: &str,
    ) -> Result<AdjacencyDataset> {
        Ok(AdjacencyDataset::new(
            &self.base_uri,
            edge_type,
            label,
            direction,
        ))
    }

    /// Get the main vertex dataset for unified vertex storage.
    ///
    /// The main vertex dataset contains all vertices regardless of label,
    /// enabling fast ID-based lookups without knowing the label.
    pub fn main_vertex_dataset(&self) -> MainVertexDataset {
        MainVertexDataset::new(&self.base_uri)
    }

    /// Get the main edge dataset for unified edge storage.
    ///
    /// The main edge dataset contains all edges regardless of type,
    /// enabling fast ID-based lookups without knowing the edge type.
    pub fn main_edge_dataset(&self) -> MainEdgeDataset {
        MainEdgeDataset::new(&self.base_uri)
    }

    pub fn uid_index(&self, label: &str) -> Result<UidIndex> {
        Ok(UidIndex::new(&self.base_uri, label))
    }

    pub async fn inverted_index(&self, label: &str, property: &str) -> Result<InvertedIndex> {
        let schema = self.schema_manager.schema();
        let config = schema
            .indexes
            .iter()
            .find_map(|idx| match idx {
                IndexDefinition::Inverted(cfg)
                    if cfg.label == label && cfg.property == property =>
                {
                    Some(cfg.clone())
                }
                _ => None,
            })
            .ok_or_else(|| anyhow!("Inverted index not found for {}.{}", label, property))?;

        InvertedIndex::new(&self.base_uri, config).await
    }

    pub async fn vector_search(
        &self,
        label: &str,
        property: &str,
        query: &[f32],
        k: usize,
        filter: Option<&str>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<(Vid, f32)>> {
        // Look up vector index config to get the correct distance metric.
        let schema = self.schema_manager.schema();
        let metric = schema
            .vector_index_for_property(label, property)
            .map(|config| config.metric.clone())
            .unwrap_or(DistanceMetric::L2);

        // Try to open the cached table; if the label has no data yet the Lance
        // table won't exist. In that case fall back to L0-only results.
        let table = self.get_cached_table(label).await.ok();

        let mut results = Vec::new();

        if let Some(table) = table {
            let distance_type = match &metric {
                DistanceMetric::L2 => lancedb::DistanceType::L2,
                DistanceMetric::Cosine => lancedb::DistanceType::Cosine,
                DistanceMetric::Dot => lancedb::DistanceType::Dot,
                _ => lancedb::DistanceType::L2,
            };

            // Use LanceDB's vector search API
            let mut query_builder = table
                .vector_search(query.to_vec())
                .map_err(|e| anyhow!("Failed to create vector search: {}", e))?
                .column(property)
                .distance_type(distance_type)
                .limit(k);

            query_builder = query_builder.only_if(Self::build_active_filter(filter));

            // Apply version filtering if snapshot is pinned
            if ctx.is_some()
                && let Some(hwm) = self.version_high_water_mark()
            {
                query_builder = query_builder.only_if(format!("_version <= {}", hwm));
            }

            let batches = query_builder
                .execute()
                .await
                .map_err(|e| anyhow!("Vector search execution failed: {}", e))?
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| anyhow!("Failed to collect vector search results: {}", e))?;

            results = extract_vid_score_pairs(&batches, "_vid", "_distance")?;
        }

        // Merge L0 buffer vertices into results for visibility of unflushed data.
        if let Some(qctx) = ctx {
            merge_l0_into_vector_results(&mut results, qctx, label, property, query, k, &metric);
        }

        Ok(results)
    }

    /// Perform a full-text search with BM25 scoring.
    ///
    /// Returns vertices matching the search query along with their BM25 scores.
    /// Results are sorted by score descending (most relevant first).
    ///
    /// # Arguments
    /// * `label` - The label to search within
    /// * `property` - The property column to search (must have FTS index)
    /// * `query` - The search query text
    /// * `k` - Maximum number of results to return
    /// * `filter` - Optional Lance filter expression
    /// * `ctx` - Optional query context for visibility checks
    ///
    /// # Returns
    /// Vector of (Vid, score) tuples, where score is the BM25 relevance score.
    pub async fn fts_search(
        &self,
        label: &str,
        property: &str,
        query: &str,
        k: usize,
        filter: Option<&str>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<(Vid, f32)>> {
        use lance_index::scalar::FullTextSearchQuery;
        use lance_index::scalar::inverted::query::MatchQuery;

        // Try to open the cached table; if the label has no data yet the Lance
        // table won't exist. In that case return empty results.
        let table = match self.get_cached_table(label).await {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };

        // Build the FTS query with specific column
        let match_query =
            MatchQuery::new(query.to_string()).with_column(Some(property.to_string()));
        let fts_query = FullTextSearchQuery {
            query: match_query.into(),
            limit: Some(k as i64),
            wand_factor: None,
        };

        let mut query_builder = table.query().full_text_search(fts_query).limit(k);

        query_builder = query_builder.only_if(Self::build_active_filter(filter));

        // Apply version filtering if snapshot is pinned
        if ctx.is_some()
            && let Some(hwm) = self.version_high_water_mark()
        {
            query_builder = query_builder.only_if(format!("_version <= {}", hwm));
        }

        let batches = query_builder
            .execute()
            .await
            .map_err(|e| anyhow!("FTS search execution failed: {}", e))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| anyhow!("Failed to collect FTS search results: {}", e))?;

        let mut results = extract_vid_score_pairs(&batches, "_vid", "_score")?;

        // Results should already be sorted by score from Lance, but ensure descending order
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        Ok(results)
    }

    pub async fn get_vertex_by_uid(&self, uid: &UniId, label: &str) -> Result<Option<Vid>> {
        let index = self.uid_index(label)?;
        index.get_vid(uid).await
    }

    pub async fn insert_vertex_with_uid(&self, label: &str, vid: Vid, uid: UniId) -> Result<()> {
        let index = self.uid_index(label)?;
        index.write_mapping(&[(uid, vid)]).await
    }

    pub async fn load_subgraph(
        &self,
        start_vids: &[Vid],
        edge_types: &[u32],
        max_hops: usize,
        direction: GraphDirection,
        l0: Option<&L0Buffer>,
    ) -> Result<WorkingGraph> {
        let mut graph = WorkingGraph::new();
        let schema = self.schema_manager.schema();

        // Build maps for ID lookups
        let label_map: HashMap<u16, String> = schema
            .labels
            .values()
            .map(|meta| {
                (
                    meta.id,
                    schema.label_name_by_id(meta.id).unwrap().to_owned(),
                )
            })
            .collect();

        let edge_type_map: HashMap<u32, String> = schema
            .edge_types
            .values()
            .map(|meta| {
                (
                    meta.id,
                    schema.edge_type_name_by_id(meta.id).unwrap().to_owned(),
                )
            })
            .collect();

        let target_edge_types: HashSet<u32> = edge_types.iter().cloned().collect();

        // Initialize frontier
        let mut frontier: Vec<Vid> = start_vids.to_vec();
        let mut visited: HashSet<Vid> = HashSet::new();

        // Add start vertices to graph
        for &vid in start_vids {
            graph.add_vertex(vid);
        }

        for _hop in 0..max_hops {
            let mut next_frontier = HashSet::new();

            for &vid in &frontier {
                if visited.contains(&vid) {
                    continue;
                }
                visited.insert(vid);
                graph.add_vertex(vid);

                // For each edge type we want to traverse
                for &etype_id in &target_edge_types {
                    let etype_name = edge_type_map
                        .get(&etype_id)
                        .ok_or_else(|| anyhow!("Unknown edge type ID: {}", etype_id))?;

                    // Determine directions
                    // Storage direction: "fwd" or "bwd".
                    // Query direction: Outgoing -> "fwd", Incoming -> "bwd".
                    let (dir_str, neighbor_is_dst) = match direction {
                        GraphDirection::Outgoing => ("fwd", true),
                        GraphDirection::Incoming => ("bwd", false),
                    };

                    let mut edges: HashMap<Eid, EdgeState> = HashMap::new();

                    // 1. L2: Adjacency (Base)
                    // In the new storage model, VIDs don't embed label info.
                    // We need to try all labels to find the adjacency data.
                    // Edge version from snapshot (reserved for future version filtering)
                    let _edge_ver = self
                        .pinned_snapshot
                        .as_ref()
                        .and_then(|s| s.edges.get(etype_name).map(|es| es.lance_version));

                    // Try each label until we find adjacency data
                    let lancedb_store = self.lancedb_store();
                    for current_src_label in label_map.values() {
                        let adj_ds =
                            match self.adjacency_dataset(etype_name, current_src_label, dir_str) {
                                Ok(ds) => ds,
                                Err(_) => continue,
                            };
                        if let Some((neighbors, eids)) =
                            adj_ds.read_adjacency_lancedb(lancedb_store, vid).await?
                        {
                            for (n, eid) in neighbors.into_iter().zip(eids) {
                                edges.insert(
                                    eid,
                                    EdgeState {
                                        neighbor: n,
                                        version: 0,
                                        deleted: false,
                                    },
                                );
                            }
                            break; // Found adjacency data for this vid, no need to try other labels
                        }
                    }

                    // 2. L1: Delta
                    let delta_ds = self.delta_dataset(etype_name, dir_str)?;
                    let delta_entries = delta_ds
                        .read_deltas_lancedb(
                            lancedb_store,
                            vid,
                            &schema,
                            self.version_high_water_mark(),
                        )
                        .await?;
                    Self::apply_delta_to_edges(&mut edges, delta_entries, neighbor_is_dst);

                    // 3. L0: Buffer
                    if let Some(l0) = l0 {
                        Self::apply_l0_to_edges(&mut edges, l0, vid, etype_id, direction);
                    }

                    // Add resulting edges to graph
                    Self::add_edges_to_graph(
                        &mut graph,
                        edges,
                        vid,
                        etype_id,
                        neighbor_is_dst,
                        &visited,
                        &mut next_frontier,
                    );
                }
            }
            frontier = next_frontier.into_iter().collect();

            // Early termination: if frontier is empty, no more vertices to explore
            if frontier.is_empty() {
                break;
            }
        }

        Ok(graph)
    }

    /// Apply delta entries to edge state map, handling version conflicts.
    fn apply_delta_to_edges(
        edges: &mut HashMap<Eid, EdgeState>,
        delta_entries: Vec<crate::storage::delta::L1Entry>,
        neighbor_is_dst: bool,
    ) {
        for entry in delta_entries {
            let neighbor = if neighbor_is_dst {
                entry.dst_vid
            } else {
                entry.src_vid
            };
            let current_ver = edges.get(&entry.eid).map(|s| s.version).unwrap_or(0);

            if entry.version > current_ver {
                edges.insert(
                    entry.eid,
                    EdgeState {
                        neighbor,
                        version: entry.version,
                        deleted: matches!(entry.op, Op::Delete),
                    },
                );
            }
        }
    }

    /// Apply L0 buffer edges and tombstones to edge state map.
    fn apply_l0_to_edges(
        edges: &mut HashMap<Eid, EdgeState>,
        l0: &L0Buffer,
        vid: Vid,
        etype_id: u32,
        direction: GraphDirection,
    ) {
        let l0_neighbors = l0.get_neighbors(vid, etype_id, direction);
        for (neighbor, eid, ver) in l0_neighbors {
            let current_ver = edges.get(&eid).map(|s| s.version).unwrap_or(0);
            if ver > current_ver {
                edges.insert(
                    eid,
                    EdgeState {
                        neighbor,
                        version: ver,
                        deleted: false,
                    },
                );
            }
        }

        // Check tombstones in L0
        for (eid, state) in edges.iter_mut() {
            if l0.is_tombstoned(*eid) {
                state.deleted = true;
            }
        }
    }

    /// Add non-deleted edges to graph and collect next frontier.
    fn add_edges_to_graph(
        graph: &mut WorkingGraph,
        edges: HashMap<Eid, EdgeState>,
        vid: Vid,
        etype_id: u32,
        neighbor_is_dst: bool,
        visited: &HashSet<Vid>,
        next_frontier: &mut HashSet<Vid>,
    ) {
        for (eid, state) in edges {
            if state.deleted {
                continue;
            }
            graph.add_vertex(state.neighbor);

            if !visited.contains(&state.neighbor) {
                next_frontier.insert(state.neighbor);
            }

            if neighbor_is_dst {
                graph.add_edge(vid, state.neighbor, eid, etype_id);
            } else {
                graph.add_edge(state.neighbor, vid, eid, etype_id);
            }
        }
    }
}

/// Extracts `(Vid, f32)` pairs from record batches using the given VID and score column names.
fn extract_vid_score_pairs(
    batches: &[arrow_array::RecordBatch],
    vid_column: &str,
    score_column: &str,
) -> Result<Vec<(Vid, f32)>> {
    let mut results = Vec::new();
    for batch in batches {
        let vid_col = batch
            .column_by_name(vid_column)
            .ok_or_else(|| anyhow!("Missing {} column", vid_column))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("Invalid {} column type", vid_column))?;

        let score_col = batch
            .column_by_name(score_column)
            .ok_or_else(|| anyhow!("Missing {} column", score_column))?
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| anyhow!("Invalid {} column type", score_column))?;

        for i in 0..batch.num_rows() {
            results.push((Vid::from(vid_col.value(i)), score_col.value(i)));
        }
    }
    Ok(results)
}

/// Extracts a `Vec<f32>` from a JSON property value.
///
/// Returns `None` if the property is missing, not an array, or contains
/// non-numeric elements.
fn extract_embedding_from_props(
    props: &uni_common::Properties,
    property: &str,
) -> Option<Vec<f32>> {
    let arr = props.get(property)?.as_array()?;
    arr.iter().map(|v| v.as_f64().map(|f| f as f32)).collect()
}

/// Merges L0 buffer vertices into LanceDB vector search results.
///
/// Visits L0 buffers in precedence order (pending flush → main → transaction),
/// collects tombstoned VIDs and candidate embeddings, then merges them with the
/// existing LanceDB results so that:
/// - Tombstoned VIDs are removed (unless re-created in a later L0).
/// - VIDs present in both L0 and LanceDB use the L0 distance.
/// - New L0-only VIDs are appended.
/// - Results are re-sorted by distance ascending and truncated to `k`.
fn merge_l0_into_vector_results(
    results: &mut Vec<(Vid, f32)>,
    ctx: &QueryContext,
    label: &str,
    property: &str,
    query: &[f32],
    k: usize,
    metric: &DistanceMetric,
) {
    // Collect all L0 buffers in precedence order (earliest first, last writer wins).
    let mut buffers: Vec<Arc<parking_lot::RwLock<L0Buffer>>> =
        ctx.pending_flush_l0s.iter().map(Arc::clone).collect();
    buffers.push(Arc::clone(&ctx.l0));
    if let Some(ref txn) = ctx.transaction_l0 {
        buffers.push(Arc::clone(txn));
    }

    // Maps VID → distance for L0 candidates (last writer wins).
    let mut l0_candidates: HashMap<Vid, f32> = HashMap::new();
    // Tombstoned VIDs across all L0 buffers.
    let mut tombstoned: HashSet<Vid> = HashSet::new();

    for buf_arc in &buffers {
        let buf = buf_arc.read();

        // Accumulate tombstones.
        for &vid in &buf.vertex_tombstones {
            tombstoned.insert(vid);
        }

        // Scan vertices with the target label.
        for (&vid, labels) in &buf.vertex_labels {
            if !labels.iter().any(|l| l == label) {
                continue;
            }
            if let Some(props) = buf.vertex_properties.get(&vid)
                && let Some(emb) = extract_embedding_from_props(props, property)
            {
                if emb.len() != query.len() {
                    continue; // dimension mismatch
                }
                let dist = metric.compute_distance(&emb, query);
                // Last writer wins: later buffer overwrites earlier.
                l0_candidates.insert(vid, dist);
                // If re-created in a later L0, remove from tombstones.
                tombstoned.remove(&vid);
            }
        }
    }

    // If no L0 activity affects this search, skip merge.
    if l0_candidates.is_empty() && tombstoned.is_empty() {
        return;
    }

    // Remove tombstoned VIDs from LanceDB results.
    results.retain(|(vid, _)| !tombstoned.contains(vid));

    // Overwrite or append L0 candidates.
    for (vid, dist) in &l0_candidates {
        if let Some(existing) = results.iter_mut().find(|(v, _)| v == vid) {
            existing.1 = *dist;
        } else {
            results.push((*vid, *dist));
        }
    }

    // Re-sort by distance ascending.
    results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    results.truncate(k);
}
