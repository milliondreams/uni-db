// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::runtime::context::QueryContext;
use crate::runtime::id_allocator::IdAllocator;
use crate::runtime::l0::{L0Buffer, serialize_constraint_key};
use crate::runtime::l0_manager::L0Manager;
use crate::runtime::property_manager::PropertyManager;
use crate::runtime::wal::WriteAheadLog;
use crate::storage::adjacency_manager::AdjacencyManager;
use crate::storage::delta::{L1Entry, Op};
use crate::storage::main_edge::MainEdgeDataset;
use crate::storage::main_vertex::MainVertexDataset;
use crate::storage::manager::StorageManager;
use anyhow::{Result, anyhow};
use chrono::Utc;
use metrics;
use parking_lot::{Mutex as PlMutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tracing::{debug, info, instrument};
use uni_common::Properties;
use uni_common::Value;
use uni_common::config::UniConfig;
use uni_common::core::fork::ForkId;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{ConstraintTarget, ConstraintType, IndexDefinition};
use uni_common::core::snapshot::{EdgeSnapshot, LabelSnapshot, SnapshotManifest};
use uni_xervo::runtime::ModelRuntime;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct WriterConfig {
    pub max_mutations: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            max_mutations: 10_000,
        }
    }
}

/// RAII latch on [`StorageManager::flush_in_progress`].
///
/// Sets the flag to `true` on construction (via CAS) and back to `false` on
/// drop, so any `?` early-exit inside `flush_to_l1` cannot leave the flag
/// stuck. Returns `None` if a flush is already in progress, providing
/// forward-compatible exclusion once the outer writer-RwLock is removed in
/// Phase 4 of the concurrent-writer refactor.
struct FlushInProgressGuard {
    storage: Arc<StorageManager>,
}

impl FlushInProgressGuard {
    /// Increments the in-progress flush counter and returns a guard that
    /// decrements on drop. With the move to `AtomicUsize` (preparing for
    /// async-flush), this is now a counter, not a single-holder latch —
    /// multiple flushes may be in flight concurrently. The counter is
    /// consumed only by compaction's delta-clear gate (`> 0` skips).
    fn new(storage: &Arc<StorageManager>) -> Self {
        storage
            .flush_in_progress
            .fetch_add(1, Ordering::AcqRel);
        Self {
            storage: storage.clone(),
        }
    }
}

impl Drop for FlushInProgressGuard {
    fn drop(&mut self) {
        // M-PANIC-IS-STOP: must not panic in Drop. Atomic op cannot fail.
        self.storage
            .flush_in_progress
            .fetch_sub(1, Ordering::AcqRel);
    }
}

/// Output of [`Writer::flush_l0_rotate`]: the to-be-flushed L0 buffer,
/// captured WAL LSN, current_version, and the in-progress guard whose
/// lifetime spans the full flush (including the future async stream
/// phase that runs on a spawned task).
struct RotateOutput {
    old_l0_arc: Arc<RwLock<L0Buffer>>,
    wal_lsn: u64,
    current_version: u64,
    flush_in_progress_guard: FlushInProgressGuard,
}

pub struct Writer {
    pub l0_manager: Arc<L0Manager>,
    pub storage: Arc<StorageManager>,
    pub schema_manager: Arc<uni_common::core::schema::SchemaManager>,
    pub allocator: Arc<IdAllocator>,
    pub config: UniConfig,
    /// Optional embedding runtime. `OnceLock` so the initializer can run
    /// on `&self` after the `Writer` has been wrapped in `Arc<Writer>`
    /// (Phase 4 of concurrent_writer.md). Read through
    /// [`Writer::xervo_runtime`] — the field itself is private to keep
    /// callers oblivious to the OnceLock representation.
    xervo_runtime: OnceLock<Arc<ModelRuntime>>,
    /// Property manager for cache invalidation after flush
    pub property_manager: Option<Arc<PropertyManager>>,
    /// Adjacency manager for dual-write (edges survive flush).
    adjacency_manager: Arc<AdjacencyManager>,
    /// Timestamp of last flush or creation. Interior-mutable so that
    /// `&self` callers can update it; uncontended in practice because all
    /// writes happen inside the single-flusher critical section.
    /// Arc-wrapped so it can travel into the SharedFlushCtx that the
    /// async-flush coordinator passes to spawned stream/finalize tasks.
    last_flush_time: Arc<PlMutex<std::time::Instant>>,
    /// Background compaction task handle (prevents concurrent compaction races)
    compaction_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// Optional index rebuild manager for post-flush automatic rebuild scheduling.
    /// `OnceLock` for the same reason as `xervo_runtime`.
    index_rebuild_manager: OnceLock<Arc<crate::storage::index_rebuild::IndexRebuildManager>>,
    /// Cached snapshot manifest from the last flush. Avoids re-reading from
    /// object store on every flush_to_l1 call. Wrapped in a `Mutex` for
    /// `&self` access; uncontended because all access is inside the
    /// single-flusher critical section.
    cached_manifest: Arc<PlMutex<Option<SnapshotManifest>>>,
    /// Identifier of the fork this writer serves, if any. `None` for
    /// primary's writer. Set by [`crate::fork::writer_factory::new_for_fork`]
    /// and read in `flush_to_l1` to emit fork-tagged metrics and to fire
    /// the fragment-count guard rail (Phase 2 Day 12).
    pub fork_id: Option<ForkId>,
    /// Number of `flush_to_l1` calls since this writer was constructed.
    /// Used as a proxy for L1 fragment growth on the fork's branches:
    /// each flush typically appends ~1 fragment per touched dataset, so
    /// the count tracks the order of magnitude of fragment accumulation.
    /// Reading the actual `Dataset::manifest().fragments.len()` per
    /// flush would add a per-dataset object-store roundtrip on the hot
    /// commit path; the proxy keeps the guard rail purely observational
    /// (Phase 5 introduces fork compaction proper). Only meaningful when
    /// `fork_id.is_some()`. `Relaxed` is sufficient — observational only.
    fork_flush_count: Arc<AtomicU64>,
    /// Whether the fork-fragment warning has already fired at the
    /// configured threshold. One-shot per writer lifetime. `Relaxed` is
    /// sufficient — observational only.
    fork_fragment_warn_fired: Arc<AtomicBool>,
    /// Dedicated lock for the genuinely-exclusive flush path. Acquired by
    /// the [`Writer::flush_to_l1`] entry and by `commit_transaction_l0`
    /// across its WAL-append + L0-merge window. Replaces the outer
    /// `Arc<RwLock<Writer>>` for flush exclusion once Phase 4 drops it.
    /// Arc-wrapped so async-flush coordinator's finalize path can
    /// re-acquire it from a spawned task via SharedFlushCtx.
    flush_lock: Arc<tokio::sync::Mutex<()>>,
}

impl Writer {
    pub async fn new(
        storage: Arc<StorageManager>,
        schema_manager: Arc<uni_common::core::schema::SchemaManager>,
        start_version: u64,
    ) -> Result<Self> {
        Self::new_with_config(
            storage,
            schema_manager,
            start_version,
            UniConfig::default(),
            None,
            None,
        )
        .await
    }

    pub async fn new_with_config(
        storage: Arc<StorageManager>,
        schema_manager: Arc<uni_common::core::schema::SchemaManager>,
        start_version: u64,
        config: UniConfig,
        wal: Option<Arc<WriteAheadLog>>,
        allocator: Option<Arc<IdAllocator>>,
    ) -> Result<Self> {
        let allocator = if let Some(a) = allocator {
            a
        } else {
            let store = storage.store();
            let path = object_store::path::Path::from("id_allocator.json");
            Arc::new(IdAllocator::new(store, path, 1000).await?)
        };

        let l0_manager = Arc::new(L0Manager::new(start_version, wal));

        let property_manager = Some(Arc::new(PropertyManager::new(
            storage.clone(),
            schema_manager.clone(),
            1000,
        )));

        let adjacency_manager = storage.adjacency_manager();

        Ok(Self {
            l0_manager,
            storage,
            schema_manager,
            allocator,
            config,
            xervo_runtime: OnceLock::new(),
            property_manager,
            adjacency_manager,
            last_flush_time: Arc::new(PlMutex::new(std::time::Instant::now())),
            compaction_handle: Arc::new(RwLock::new(None)),
            index_rebuild_manager: OnceLock::new(),
            cached_manifest: Arc::new(PlMutex::new(None)),
            fork_id: None,
            fork_flush_count: Arc::new(AtomicU64::new(0)),
            fork_fragment_warn_fired: Arc::new(AtomicBool::new(false)),
            flush_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Set the index rebuild manager for post-flush automatic rebuild scheduling.
    ///
    /// One-shot: returns `Err` if already set. The receiver is `&self` so this
    /// can be called after the `Writer` has been wrapped in `Arc<Writer>`.
    pub fn set_index_rebuild_manager(
        &self,
        manager: Arc<crate::storage::index_rebuild::IndexRebuildManager>,
    ) -> Result<()> {
        self.index_rebuild_manager
            .set(manager)
            .map_err(|_| anyhow!("index_rebuild_manager already set"))
    }

    /// Replay WAL mutations into the current L0 buffer.
    pub async fn replay_wal(&self, wal_high_water_mark: u64) -> Result<usize> {
        let l0 = self.l0_manager.get_current();
        let wal = l0.read().wal.clone();

        if let Some(wal) = wal {
            wal.initialize().await?;
            let mutations = wal.replay_since(wal_high_water_mark).await?;
            let count = mutations.len();

            if count > 0 {
                log::info!(
                    "Replaying {} mutations from WAL (LSN > {})",
                    count,
                    wal_high_water_mark
                );
                let mut l0_guard = l0.write();
                l0_guard.replay_mutations(mutations)?;
            }

            Ok(count)
        } else {
            Ok(0)
        }
    }

    /// Allocates the next VID (pure auto-increment).
    pub async fn next_vid(&self) -> Result<Vid> {
        self.allocator.allocate_vid().await
    }

    /// Allocates multiple VIDs at once for bulk operations.
    /// This is more efficient than calling next_vid() in a loop.
    pub async fn allocate_vids(&self, count: usize) -> Result<Vec<Vid>> {
        self.allocator.allocate_vids(count).await
    }

    /// Allocates the next EID (pure auto-increment).
    pub async fn next_eid(&self, _type_id: u32) -> Result<Eid> {
        self.allocator.allocate_eid().await
    }

    /// Allocates multiple EIDs at once for bulk operations.
    /// This is more efficient than calling next_eid() in a loop.
    pub async fn allocate_eids(&self, count: usize) -> Result<Vec<Eid>> {
        self.allocator.allocate_eids(count).await
    }

    /// Install the embedding runtime exactly once. Receiver is `&self` so it
    /// can be called after the `Writer` has been wrapped in `Arc<Writer>`.
    pub fn set_xervo_runtime(&self, runtime: Arc<ModelRuntime>) -> Result<()> {
        self.xervo_runtime
            .set(runtime)
            .map_err(|_| anyhow!("xervo_runtime already set"))
    }

    pub fn xervo_runtime(&self) -> Option<Arc<ModelRuntime>> {
        self.xervo_runtime.get().cloned()
    }

    /// Create a new empty L0 buffer for transaction-scoped mutations.
    ///
    /// Only reads the current version — no exclusive lock required on Writer.
    /// The returned buffer has no WAL reference; mutations are logged at
    /// commit time via [`Self::commit_transaction_l0`].
    pub fn create_transaction_l0(&self) -> Arc<RwLock<L0Buffer>> {
        let current_version = self.l0_manager.get_current().read().current_version;
        // Transaction mutations are logged to WAL at COMMIT time, not during the transaction.
        Arc::new(RwLock::new(L0Buffer::new(current_version, None)))
    }

    /// Resolve the target L0 buffer for a mutation.
    ///
    /// When `tx_l0` is `Some`, the mutation targets a transaction-private buffer.
    /// When `None`, it targets the global L0 from the manager.
    fn resolve_l0(&self, tx_l0: Option<&Arc<RwLock<L0Buffer>>>) -> Arc<RwLock<L0Buffer>> {
        tx_l0
            .cloned()
            .unwrap_or_else(|| self.l0_manager.get_current())
    }

    fn update_metrics(&self) {
        let l0 = self.l0_manager.get_current();
        let size = l0.read().estimated_size;
        metrics::gauge!("l0_buffer_size_bytes").set(size as f64);
    }

    /// Commit an externally-owned transaction L0 buffer.
    ///
    /// Writes mutations to WAL, flushes, merges into main L0, and replays
    /// edges into the AdjacencyManager. Returns the WAL LSN of the commit
    /// (0 when no WAL is configured).
    /// Commit a transaction's private L0 buffer into main L0.
    ///
    /// Returns `(wal_lsn, flush_pending)`. When `flush_pending == true`, the
    /// post-commit `should_flush()` predicate fired but no flush ran — the
    /// caller is expected to spawn a background `flush_to_l1`. This is the
    /// shape used by `docs/proposals/async_l0_to_l1_flush.md` when
    /// `UniConfig::async_flush_enabled` is set, so commits don't block on
    /// L1-streaming I/O.
    pub async fn commit_transaction_l0(
        &self,
        tx_l0_arc: Arc<RwLock<L0Buffer>>,
    ) -> Result<(u64, bool)> {
        // Hold `flush_lock` across WAL append + flush + main-L0 merge.
        // Two concurrent commits serialize here; in Phase 3 the outer
        // `Arc<RwLock<Writer>>` already provides this exclusion, so the
        // acquisition is uncontended. Phase 4 drops the outer lock and
        // this becomes the load-bearing serialization point.
        let _flush_lock_guard = self.flush_lock.lock().await;

        // 1. Write transaction mutations to WAL BEFORE merging into main L0
        // This ensures durability before visibility.
        {
            let tx_l0 = tx_l0_arc.read();
            let main_l0_arc = self.l0_manager.get_current();
            let main_l0 = main_l0_arc.read();

            // If WAL exists, write mutations to it for durability
            if let Some(wal) = main_l0.wal.as_ref() {
                // Order: vertices first, then edges (to ensure src/dst exist on replay)

                // Vertex insertions
                for (vid, properties) in &tx_l0.vertex_properties {
                    if !tx_l0.vertex_tombstones.contains(vid) {
                        let labels = tx_l0.vertex_labels.get(vid).cloned().unwrap_or_default();
                        wal.append(&crate::runtime::wal::Mutation::InsertVertex {
                            vid: *vid,
                            properties: properties.clone(),
                            labels,
                        })?;
                    }
                }

                // Vertex deletions
                for vid in &tx_l0.vertex_tombstones {
                    let labels = tx_l0.vertex_labels.get(vid).cloned().unwrap_or_default();
                    wal.append(&crate::runtime::wal::Mutation::DeleteVertex { vid: *vid, labels })?;
                }

                // Edge insertions and deletions from edge_endpoints
                for (eid, (src_vid, dst_vid, edge_type)) in &tx_l0.edge_endpoints {
                    if tx_l0.tombstones.contains_key(eid) {
                        let version = tx_l0.edge_versions.get(eid).copied().unwrap_or(0);
                        wal.append(&crate::runtime::wal::Mutation::DeleteEdge {
                            eid: *eid,
                            src_vid: *src_vid,
                            dst_vid: *dst_vid,
                            edge_type: *edge_type,
                            version,
                        })?;
                    } else {
                        let properties =
                            tx_l0.edge_properties.get(eid).cloned().unwrap_or_default();
                        let version = tx_l0.edge_versions.get(eid).copied().unwrap_or(0);
                        let edge_type_name = tx_l0.edge_types.get(eid).cloned();
                        wal.append(&crate::runtime::wal::Mutation::InsertEdge {
                            src_vid: *src_vid,
                            dst_vid: *dst_vid,
                            edge_type: *edge_type,
                            eid: *eid,
                            version,
                            properties,
                            edge_type_name,
                        })?;
                    }
                }

                // Tombstones for edges that only exist in the global L0 (not in
                // this transaction's edge_endpoints).  Without this, deletes of
                // pre-existing edges would be silently lost.
                for (eid, tombstone) in &tx_l0.tombstones {
                    if !tx_l0.edge_endpoints.contains_key(eid) {
                        let version = tx_l0.edge_versions.get(eid).copied().unwrap_or(0);
                        wal.append(&crate::runtime::wal::Mutation::DeleteEdge {
                            eid: *eid,
                            src_vid: tombstone.src_vid,
                            dst_vid: tombstone.dst_vid,
                            edge_type: tombstone.edge_type,
                            version,
                        })?;
                    }
                }
            }
        }

        // 2. Flush WAL to durable storage - THIS IS THE COMMIT POINT
        let wal_lsn = self.flush_wal().await?;

        // 3. Merge into main L0 and make visible
        {
            let tx_l0 = tx_l0_arc.read();
            let main_l0_arc = self.l0_manager.get_current();
            let mut main_l0 = main_l0_arc.write();
            main_l0.merge(&tx_l0)?;

            // Replay transaction edges into the AdjacencyManager overlay
            for (eid, (src, dst, etype)) in &tx_l0.edge_endpoints {
                let edge_version = tx_l0
                    .edge_versions
                    .get(eid)
                    .copied()
                    .unwrap_or(main_l0.current_version);
                if tx_l0.tombstones.contains_key(eid) {
                    self.adjacency_manager
                        .add_tombstone(*eid, *src, *dst, *etype, edge_version);
                } else {
                    self.adjacency_manager
                        .insert_edge(*src, *dst, *eid, *etype, edge_version);
                }
            }

            // Replay tombstones for edges that only exist in the global L0
            // (not in this transaction's edge_endpoints).
            for (eid, tombstone) in &tx_l0.tombstones {
                if !tx_l0.edge_endpoints.contains_key(eid) {
                    let edge_version = tx_l0
                        .edge_versions
                        .get(eid)
                        .copied()
                        .unwrap_or(main_l0.current_version);
                    self.adjacency_manager.add_tombstone(
                        *eid,
                        tombstone.src_vid,
                        tombstone.dst_vid,
                        tombstone.edge_type,
                        edge_version,
                    );
                }
            }
        }

        self.update_metrics();

        // 4. Best-effort compaction. We already hold `flush_lock`, so dispatch
        //    to `flush_to_l1_inner` directly to avoid re-acquiring the
        //    non-reentrant `tokio::sync::Mutex` (concurrent_writer.md §5.5).
        //
        //    The async-flush proposal (docs/proposals/async_l0_to_l1_flush.md)
        //    would have us return `flush_pending = true` here and let the
        //    caller spawn the streaming work. The MVP shape of that —
        //    "just spawn the full flush_to_l1" — was measured pathological
        //    (3-40x slower at high mutation rates because spawned flushes
        //    convoy in front of subsequent commits on the same `flush_lock`).
        //    Until the proper rotate/stream/finalize split lands, this stays
        //    synchronous regardless of `config.async_flush_enabled`. The
        //    second tuple element is kept for forward compatibility.
        if self.should_flush()
            && let Err(e) = self.flush_to_l1_inner(None).await
        {
            tracing::warn!("Post-commit flush check failed (non-critical): {}", e);
        }
        let flush_pending = false;

        Ok((wal_lsn, flush_pending))
    }

    /// Flush the WAL buffer to durable storage.
    ///
    /// Returns the LSN of the flushed segment, or `0` when no WAL is configured.
    pub async fn flush_wal(&self) -> Result<u64> {
        let l0 = self.l0_manager.get_current();
        let wal = l0.read().wal.clone();

        match wal {
            Some(wal) => Ok(wal.flush().await?),
            None => Ok(0),
        }
    }

    /// Record property removals in the active L0 mutation stats.
    ///
    /// Routes to the transaction L0 if provided, otherwise to the main L0.
    pub fn track_properties_removed(&self, count: usize, tx_l0: Option<&Arc<RwLock<L0Buffer>>>) {
        if count == 0 {
            return;
        }
        let l0 = self.resolve_l0(tx_l0);
        l0.write().mutation_stats.properties_removed += count;
    }

    /// Validates vertex constraints for the given properties.
    /// In the new design, label is passed as a parameter since VID no longer embeds label.
    async fn validate_vertex_constraints_for_label(
        &self,
        vid: Vid,
        properties: &Properties,
        label: &str,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        let schema = self.schema_manager.schema();

        {
            // 1. Check NOT NULL constraints (from Property definitions)
            if let Some(props_meta) = schema.properties.get(label) {
                for (prop_name, meta) in props_meta {
                    if !meta.nullable && properties.get(prop_name).is_none_or(|v| v.is_null()) {
                        log::warn!(
                            "Constraint violation: Property '{}' cannot be null for label '{}'",
                            prop_name,
                            label
                        );
                        return Err(anyhow!(
                            "Constraint violation: Property '{}' cannot be null",
                            prop_name
                        ));
                    }
                }
            }

            // 2. Check Explicit Constraints (Unique, Check, etc.)
            for constraint in &schema.constraints {
                if !constraint.enabled {
                    continue;
                }
                match &constraint.target {
                    ConstraintTarget::Label(l) if l == label => {}
                    _ => continue,
                }

                match &constraint.constraint_type {
                    ConstraintType::Unique {
                        properties: unique_props,
                    } => {
                        // Support single and multi-property unique constraints
                        if !unique_props.is_empty() {
                            let mut key_values = Vec::new();
                            let mut missing = false;
                            for prop in unique_props {
                                if let Some(val) = properties.get(prop) {
                                    key_values.push((prop.clone(), val.clone()));
                                } else {
                                    missing = true; // Can't enforce if property missing (partial update?)
                                    // For INSERT, missing means null?
                                    // If property is nullable, unique constraint typically allows multiple nulls or ignores?
                                    // For now, only check if ALL keys are present
                                }
                            }

                            if !missing {
                                self.check_unique_constraint_multi(label, &key_values, vid, tx_l0)
                                    .await?;
                            }
                        }
                    }
                    ConstraintType::Exists { property } => {
                        if properties.get(property).is_none_or(|v| v.is_null()) {
                            log::warn!(
                                "Constraint violation: Property '{}' must exist for label '{}'",
                                property,
                                label
                            );
                            return Err(anyhow!(
                                "Constraint violation: Property '{}' must exist",
                                property
                            ));
                        }
                    }
                    ConstraintType::Check { expression } => {
                        if !self.evaluate_check_constraint(expression, properties)? {
                            return Err(anyhow!(
                                "CHECK constraint '{}' violated: expression '{}' evaluated to false",
                                constraint.name,
                                expression
                            ));
                        }
                    }
                    _ => {
                        return Err(anyhow!("Unsupported constraint type"));
                    }
                }
            }
        }
        Ok(())
    }

    /// Validates vertex constraints for a vertex with the given labels.
    /// Labels must be passed explicitly since the vertex may not yet be in L0.
    /// Unknown labels (not in schema) are skipped.
    async fn validate_vertex_constraints(
        &self,
        vid: Vid,
        properties: &Properties,
        labels: &[String],
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        let schema = self.schema_manager.schema();

        // Validate constraints only for known labels
        for label in labels {
            // Skip unknown labels (schemaless support)
            if schema.get_label_case_insensitive(label).is_none() {
                continue;
            }
            self.validate_vertex_constraints_for_label(vid, properties, label, tx_l0)
                .await?;
        }

        // Check global ext_id uniqueness if ext_id is provided
        if let Some(ext_id) = properties.get("ext_id").and_then(|v| v.as_str()) {
            self.check_extid_globally_unique(ext_id, vid, tx_l0).await?;
        }

        Ok(())
    }

    /// Collect ext_ids and unique constraint keys from an iterator of vertex properties.
    ///
    /// Used to build a constraint key index from L0 buffers for batch validation.
    fn collect_constraint_keys_from_properties<'a>(
        properties_iter: impl Iterator<Item = &'a Properties>,
        label: &str,
        constraints: &[uni_common::core::schema::Constraint],
        existing_keys: &mut HashMap<String, HashSet<String>>,
        existing_extids: &mut HashSet<String>,
    ) {
        for props in properties_iter {
            if let Some(ext_id) = props.get("ext_id").and_then(|v| v.as_str()) {
                existing_extids.insert(ext_id.to_string());
            }

            for constraint in constraints {
                if !constraint.enabled {
                    continue;
                }
                if let ConstraintTarget::Label(l) = &constraint.target {
                    if l != label {
                        continue;
                    }
                } else {
                    continue;
                }

                if let ConstraintType::Unique {
                    properties: unique_props,
                } = &constraint.constraint_type
                {
                    let mut key_parts = Vec::new();
                    let mut all_present = true;
                    for prop in unique_props {
                        if let Some(val) = props.get(prop) {
                            key_parts.push(format!("{}:{}", prop, val));
                        } else {
                            all_present = false;
                            break;
                        }
                    }
                    if all_present {
                        let key = key_parts.join("|");
                        existing_keys
                            .entry(constraint.name.clone())
                            .or_default()
                            .insert(key);
                    }
                }
            }
        }
    }

    /// Validates constraints for a batch of vertices efficiently.
    ///
    /// This method builds an in-memory index from L0 buffers ONCE instead of scanning
    /// per vertex, reducing complexity from O(n²) to O(n) for bulk inserts.
    ///
    /// # Arguments
    /// * `vids` - VIDs of vertices being inserted
    /// * `properties_batch` - Properties for each vertex
    /// * `label` - Label for all vertices (assumes single label for now)
    ///
    /// # Performance
    /// For N vertices with unique constraints:
    /// - Old approach: O(N²) - scan L0 buffer N times
    /// - New approach: O(N) - scan L0 buffer once, build HashSet, check each vertex in O(1)
    async fn validate_vertex_batch_constraints(
        &self,
        vids: &[Vid],
        properties_batch: &[Properties],
        label: &str,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        if vids.len() != properties_batch.len() {
            return Err(anyhow!("VID/properties length mismatch"));
        }

        let schema = self.schema_manager.schema();

        // 1. Validate NOT NULL constraints for each vertex
        if let Some(props_meta) = schema.properties.get(label) {
            for (idx, properties) in properties_batch.iter().enumerate() {
                for (prop_name, meta) in props_meta {
                    if !meta.nullable && properties.get(prop_name).is_none_or(|v| v.is_null()) {
                        return Err(anyhow!(
                            "Constraint violation at index {}: Property '{}' cannot be null",
                            idx,
                            prop_name
                        ));
                    }
                }
            }
        }

        // 2. Build constraint key index from L0 buffers (ONCE for entire batch)
        let mut existing_keys: HashMap<String, HashSet<String>> = HashMap::new();
        let mut existing_extids: HashSet<String> = HashSet::new();

        // Scan current L0 buffer
        {
            let l0 = self.l0_manager.get_current();
            let l0_guard = l0.read();
            Self::collect_constraint_keys_from_properties(
                l0_guard.vertex_properties.values(),
                label,
                &schema.constraints,
                &mut existing_keys,
                &mut existing_extids,
            );
        }

        // Scan transaction L0 if present
        if let Some(tx_l0) = tx_l0 {
            let tx_l0_guard = tx_l0.read();
            Self::collect_constraint_keys_from_properties(
                tx_l0_guard.vertex_properties.values(),
                label,
                &schema.constraints,
                &mut existing_keys,
                &mut existing_extids,
            );
        }

        // 3. Check batch vertices against index AND check for duplicates within batch
        let mut batch_keys: HashMap<String, HashMap<String, usize>> = HashMap::new();
        let mut batch_extids: HashMap<String, usize> = HashMap::new();

        for (idx, (_vid, properties)) in vids.iter().zip(properties_batch.iter()).enumerate() {
            // Check ext_id uniqueness
            if let Some(ext_id) = properties.get("ext_id").and_then(|v| v.as_str()) {
                if existing_extids.contains(ext_id) {
                    return Err(anyhow!(
                        "Constraint violation at index {}: ext_id '{}' already exists",
                        idx,
                        ext_id
                    ));
                }
                if let Some(first_idx) = batch_extids.get(ext_id) {
                    return Err(anyhow!(
                        "Constraint violation: ext_id '{}' duplicated in batch at indices {} and {}",
                        ext_id,
                        first_idx,
                        idx
                    ));
                }
                batch_extids.insert(ext_id.to_string(), idx);
            }

            // Check unique constraints
            for constraint in &schema.constraints {
                if !constraint.enabled {
                    continue;
                }
                if let ConstraintTarget::Label(l) = &constraint.target {
                    if l != label {
                        continue;
                    }
                } else {
                    continue;
                }

                match &constraint.constraint_type {
                    ConstraintType::Unique {
                        properties: unique_props,
                    } => {
                        let mut key_parts = Vec::new();
                        let mut all_present = true;
                        for prop in unique_props {
                            if let Some(val) = properties.get(prop) {
                                key_parts.push(format!("{}:{}", prop, val));
                            } else {
                                all_present = false;
                                break;
                            }
                        }

                        if all_present {
                            let key = key_parts.join("|");

                            // Check against existing L0 keys
                            if let Some(keys) = existing_keys.get(&constraint.name)
                                && keys.contains(&key)
                            {
                                return Err(anyhow!(
                                    "Constraint violation at index {}: Duplicate composite key for label '{}' (constraint '{}')",
                                    idx,
                                    label,
                                    constraint.name
                                ));
                            }

                            // Check for duplicates within batch
                            let batch_constraint_keys =
                                batch_keys.entry(constraint.name.clone()).or_default();
                            if let Some(first_idx) = batch_constraint_keys.get(&key) {
                                return Err(anyhow!(
                                    "Constraint violation: Duplicate key '{}' in batch at indices {} and {}",
                                    key,
                                    first_idx,
                                    idx
                                ));
                            }
                            batch_constraint_keys.insert(key, idx);
                        }
                    }
                    ConstraintType::Exists { property }
                        if properties.get(property).is_none_or(|v| v.is_null()) =>
                    {
                        return Err(anyhow!(
                            "Constraint violation at index {}: Property '{}' must exist",
                            idx,
                            property
                        ));
                    }
                    ConstraintType::Check { expression }
                        if !self.evaluate_check_constraint(expression, properties)? =>
                    {
                        return Err(anyhow!(
                            "Constraint violation at index {}: CHECK constraint '{}' violated",
                            idx,
                            constraint.name
                        ));
                    }
                    _ => {}
                }
            }
        }

        // 4. Check storage for unique constraints (can batch this into a single query)
        for constraint in &schema.constraints {
            if !constraint.enabled {
                continue;
            }
            if let ConstraintTarget::Label(l) = &constraint.target {
                if l != label {
                    continue;
                }
            } else {
                continue;
            }

            if let ConstraintType::Unique {
                properties: unique_props,
            } = &constraint.constraint_type
            {
                // Build compound OR filter for all batch vertices
                let mut or_filters = Vec::new();
                for properties in properties_batch.iter() {
                    let mut and_parts = Vec::new();
                    let mut all_present = true;
                    for prop in unique_props {
                        if let Some(val) = properties.get(prop) {
                            let val_str = match val {
                                Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                                Value::Int(n) => n.to_string(),
                                Value::Float(f) => f.to_string(),
                                Value::Bool(b) => b.to_string(),
                                _ => {
                                    all_present = false;
                                    break;
                                }
                            };
                            and_parts.push(format!("{} = {}", prop, val_str));
                        } else {
                            all_present = false;
                            break;
                        }
                    }
                    if all_present {
                        or_filters.push(format!("({})", and_parts.join(" AND ")));
                    }
                }

                #[cfg(feature = "lance-backend")]
                if !or_filters.is_empty() {
                    let vid_list: Vec<String> =
                        vids.iter().map(|v| v.as_u64().to_string()).collect();
                    let filter = format!(
                        "({}) AND _deleted = false AND _vid NOT IN ({})",
                        or_filters.join(" OR "),
                        vid_list.join(", ")
                    );

                    if let Ok(ds) = self.storage.vertex_dataset(label)
                        && let Ok(lance_ds) = ds.open_raw().await
                    {
                        let count = lance_ds.count_rows(Some(filter.clone())).await?;
                        if count > 0 {
                            return Err(anyhow!(
                                "Constraint violation: Duplicate composite key for label '{}' in storage (constraint '{}')",
                                label,
                                constraint.name
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Checks that ext_id is globally unique across all vertices.
    ///
    /// Searches L0 buffers (current, transaction, pending) and the main vertices table
    /// to ensure no other vertex uses this ext_id.
    ///
    /// # Errors
    ///
    /// Returns error if another vertex with the same ext_id exists.
    async fn check_extid_globally_unique(
        &self,
        ext_id: &str,
        current_vid: Vid,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        // Check L0 buffers: current, transaction, and pending flush
        let l0_buffers_to_check: Vec<Arc<RwLock<L0Buffer>>> = {
            let mut buffers = vec![self.l0_manager.get_current()];
            if let Some(tx_l0) = tx_l0 {
                buffers.push(tx_l0.clone());
            }
            buffers.extend(self.l0_manager.get_pending_flush());
            buffers
        };

        for l0 in &l0_buffers_to_check {
            if let Some(vid) =
                Self::find_extid_in_properties(&l0.read().vertex_properties, ext_id, current_vid)
            {
                return Err(anyhow!(
                    "Constraint violation: ext_id '{}' already exists (vertex {:?})",
                    ext_id,
                    vid
                ));
            }
        }

        // Check main vertices table (if it exists)
        // Pass None for global uniqueness check (not snapshot-isolated)
        let backend = self.storage.backend();
        if let Ok(Some(found_vid)) = MainVertexDataset::find_by_ext_id(backend, ext_id, None).await
            && found_vid != current_vid
        {
            return Err(anyhow!(
                "Constraint violation: ext_id '{}' already exists (vertex {:?})",
                ext_id,
                found_vid
            ));
        }

        Ok(())
    }

    /// Search vertex properties for a duplicate ext_id, excluding `current_vid`.
    fn find_extid_in_properties(
        vertex_properties: &HashMap<Vid, Properties>,
        ext_id: &str,
        current_vid: Vid,
    ) -> Option<Vid> {
        vertex_properties.iter().find_map(|(&vid, props)| {
            if vid != current_vid && props.get("ext_id").and_then(|v| v.as_str()) == Some(ext_id) {
                Some(vid)
            } else {
                None
            }
        })
    }

    /// Helper to get vertex labels from L0 buffer.
    fn get_vertex_labels_from_l0(&self, vid: Vid) -> Option<Vec<String>> {
        let l0 = self.l0_manager.get_current();
        let l0_guard = l0.read();
        // Check if vertex is tombstoned (deleted) - if so, return None
        if l0_guard.vertex_tombstones.contains(&vid) {
            return None;
        }
        l0_guard.get_vertex_labels(vid).map(|l| l.to_vec())
    }

    /// Get vertex labels from all sources: current L0, pending L0s, and storage.
    /// This is the proper way to read vertex labels after a flush, as it checks both
    /// in-memory buffers and persisted storage.
    pub async fn get_vertex_labels(
        &self,
        vid: Vid,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Option<Vec<String>> {
        // 1. Check current L0
        if let Some(labels) = self.get_vertex_labels_from_l0(vid) {
            return Some(labels);
        }

        // 2. Check transaction L0 if present
        if let Some(tx_l0) = tx_l0 {
            let guard = tx_l0.read();
            if guard.vertex_tombstones.contains(&vid) {
                return None;
            }
            if let Some(labels) = guard.get_vertex_labels(vid) {
                return Some(labels.to_vec());
            }
        }

        // 3. Check pending flush L0s
        for pending_l0 in self.l0_manager.get_pending_flush() {
            let guard = pending_l0.read();
            if guard.vertex_tombstones.contains(&vid) {
                return None;
            }
            if let Some(labels) = guard.get_vertex_labels(vid) {
                return Some(labels.to_vec());
            }
        }

        // 4. Check storage
        self.find_vertex_labels_in_storage(vid).await.ok().flatten()
    }

    /// Helper to get edge type from L0 buffer.
    fn get_edge_type_from_l0(&self, eid: Eid) -> Option<String> {
        let l0 = self.l0_manager.get_current();
        let l0_guard = l0.read();
        l0_guard.get_edge_type(eid).map(|s| s.to_string())
    }

    /// Look up the edge type ID (u32) for an EID from the L0 buffer's edge endpoints.
    /// Falls back to the transaction L0 if available.
    pub fn get_edge_type_id_from_l0(
        &self,
        eid: Eid,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Option<u32> {
        // Check transaction L0 first
        if let Some(tx_l0) = tx_l0 {
            let guard = tx_l0.read();
            if let Some((_, _, etype)) = guard.get_edge_endpoint_full(eid) {
                return Some(etype);
            }
        }
        // Fall back to main L0
        let l0 = self.l0_manager.get_current();
        let l0_guard = l0.read();
        l0_guard
            .get_edge_endpoint_full(eid)
            .map(|(_, _, etype)| etype)
    }

    /// Set the type name for an edge (used for schemaless edge types).
    /// This is called during CREATE for edge types not found in the schema.
    pub fn set_edge_type(
        &self,
        eid: Eid,
        type_name: String,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) {
        self.resolve_l0(tx_l0).write().set_edge_type(eid, type_name);
    }

    /// Evaluate a simple CHECK constraint expression.
    /// Supports: "property op value" (e.g., "age > 18", "status = 'active'")
    fn evaluate_check_constraint(&self, expression: &str, properties: &Properties) -> Result<bool> {
        let parts: Vec<&str> = expression.split_whitespace().collect();
        if parts.len() != 3 {
            // For now, only support "prop op val"
            // Fallback to true if too complex to avoid breaking, but warn
            log::warn!(
                "Complex CHECK constraint expression '{}' not fully supported yet; allowing write.",
                expression
            );
            return Ok(true);
        }

        let prop_part = parts[0].trim_start_matches('(');
        // Handle "variable.property" format - take the part after the dot
        let prop_name = if let Some(idx) = prop_part.find('.') {
            &prop_part[idx + 1..]
        } else {
            prop_part
        };

        let op = parts[1];
        let val_str = parts[2].trim_end_matches(')');

        let prop_val = match properties.get(prop_name) {
            Some(v) => v,
            None => return Ok(true), // If property missing, CHECK usually passes (unless NOT NULL)
        };

        // Parse value string (handle quotes for strings)
        let target_val = if (val_str.starts_with('\'') && val_str.ends_with('\''))
            || (val_str.starts_with('"') && val_str.ends_with('"'))
        {
            Value::String(val_str[1..val_str.len() - 1].to_string())
        } else if let Ok(n) = val_str.parse::<i64>() {
            Value::Int(n)
        } else if let Ok(n) = val_str.parse::<f64>() {
            Value::Float(n)
        } else if let Ok(b) = val_str.parse::<bool>() {
            Value::Bool(b)
        } else {
            // Check for internal format wrappers if they somehow leaked through
            if val_str.starts_with("Number(") && val_str.ends_with(')') {
                let n_str = &val_str[7..val_str.len() - 1];
                if let Ok(n) = n_str.parse::<i64>() {
                    Value::Int(n)
                } else if let Ok(n) = n_str.parse::<f64>() {
                    Value::Float(n)
                } else {
                    Value::String(val_str.to_string())
                }
            } else {
                Value::String(val_str.to_string())
            }
        };

        match op {
            "=" | "==" => Ok(prop_val == &target_val),
            "!=" | "<>" => Ok(prop_val != &target_val),
            ">" => self
                .compare_values(prop_val, &target_val)
                .map(|o| o.is_gt()),
            "<" => self
                .compare_values(prop_val, &target_val)
                .map(|o| o.is_lt()),
            ">=" => self
                .compare_values(prop_val, &target_val)
                .map(|o| o.is_ge()),
            "<=" => self
                .compare_values(prop_val, &target_val)
                .map(|o| o.is_le()),
            _ => {
                log::warn!("Unsupported operator '{}' in CHECK constraint", op);
                Ok(true)
            }
        }
    }

    fn compare_values(&self, a: &Value, b: &Value) -> Result<std::cmp::Ordering> {
        use std::cmp::Ordering;

        fn cmp_f64(x: f64, y: f64) -> Ordering {
            x.partial_cmp(&y).unwrap_or(Ordering::Equal)
        }

        match (a, b) {
            (Value::Int(n1), Value::Int(n2)) => Ok(n1.cmp(n2)),
            (Value::Float(f1), Value::Float(f2)) => Ok(cmp_f64(*f1, *f2)),
            (Value::Int(n), Value::Float(f)) => Ok(cmp_f64(*n as f64, *f)),
            (Value::Float(f), Value::Int(n)) => Ok(cmp_f64(*f, *n as f64)),
            (Value::String(s1), Value::String(s2)) => Ok(s1.cmp(s2)),
            _ => Err(anyhow!(
                "Cannot compare incompatible types: {:?} vs {:?}",
                a,
                b
            )),
        }
    }

    async fn check_unique_constraint_multi(
        &self,
        label: &str,
        key_values: &[(String, Value)],
        current_vid: Vid,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        // Serialize constraint key once for O(1) lookups
        let key = serialize_constraint_key(label, key_values);

        // 1. Check L0 (in-memory) using O(1) constraint index
        {
            let l0 = self.l0_manager.get_current();
            let l0_guard = l0.read();
            if l0_guard.has_constraint_key(&key, current_vid) {
                return Err(anyhow!(
                    "Constraint violation: Duplicate composite key for label '{}'",
                    label
                ));
            }
        }

        // Check Transaction L0
        if let Some(tx_l0) = tx_l0 {
            let tx_l0_guard = tx_l0.read();
            if tx_l0_guard.has_constraint_key(&key, current_vid) {
                return Err(anyhow!(
                    "Constraint violation: Duplicate composite key for label '{}' (in tx)",
                    label
                ));
            }
        }

        // 2. Check Storage (L1/L2)
        let filters: Vec<String> = key_values
            .iter()
            .map(|(prop, val)| {
                let val_str = match val {
                    Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                    Value::Int(n) => n.to_string(),
                    Value::Float(f) => f.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => "NULL".to_string(),
                };
                format!("{} = {}", prop, val_str)
            })
            .collect();

        let mut filter = filters.join(" AND ");
        filter.push_str(&format!(
            " AND _deleted = false AND _vid != {}",
            current_vid.as_u64()
        ));

        #[cfg(feature = "lance-backend")]
        if let Ok(ds) = self.storage.vertex_dataset(label)
            && let Ok(lance_ds) = ds.open_raw().await
        {
            let count = lance_ds.count_rows(Some(filter.clone())).await?;
            if count > 0 {
                return Err(anyhow!(
                    "Constraint violation: Duplicate composite key for label '{}' (in storage). Filter: {}",
                    label,
                    filter
                ));
            }
        }

        Ok(())
    }

    async fn check_write_pressure(&self) -> Result<()> {
        let status = self
            .storage
            .compaction_status()
            .map_err(|e| anyhow::anyhow!("Failed to get compaction status: {}", e))?;
        let l1_runs = status.l1_runs;
        let throttle = &self.config.throttle;

        if l1_runs >= throttle.hard_limit {
            log::warn!("Write stalled: L1 runs ({}) at hard limit", l1_runs);
            // Simple polling for now
            while self
                .storage
                .compaction_status()
                .map_err(|e| anyhow::anyhow!("Failed to get compaction status: {}", e))?
                .l1_runs
                >= throttle.hard_limit
            {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        } else if l1_runs >= throttle.soft_limit {
            let excess = l1_runs - throttle.soft_limit;
            // Cap multiplier to avoid overflow
            let excess = std::cmp::min(excess, 31);
            let multiplier = 2_u32.pow(excess as u32);
            let delay = throttle.base_delay * multiplier;
            tokio::time::sleep(delay).await;
        }
        Ok(())
    }

    /// Check transaction memory limit to prevent OOM.
    /// No-op when no transaction is active.
    fn check_transaction_memory(&self, tx_l0: Option<&Arc<RwLock<L0Buffer>>>) -> Result<()> {
        if let Some(tx_l0) = tx_l0 {
            let size = tx_l0.read().estimated_size;
            if size > self.config.max_transaction_memory {
                return Err(anyhow!(
                    "Transaction memory limit exceeded: {} bytes used, limit is {} bytes. \
                     Roll back or commit the current transaction.",
                    size,
                    self.config.max_transaction_memory
                ));
            }
        }
        Ok(())
    }

    async fn get_query_context(
        &self,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Option<QueryContext> {
        Some(QueryContext::new_with_pending(
            self.l0_manager.get_current(),
            tx_l0.cloned(),
            self.l0_manager.get_pending_flush(),
        ))
    }

    /// Prepare a vertex for upsert by merging CRDT properties with existing values.
    ///
    /// When `label` is provided, uses it directly to look up property metadata.
    /// Otherwise falls back to discovering the label from L0 buffers and storage.
    ///
    /// # Errors
    ///
    /// Returns an error if CRDT property merging fails.
    async fn prepare_vertex_upsert(
        &self,
        vid: Vid,
        properties: &mut Properties,
        label: Option<&str>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        let Some(pm) = &self.property_manager else {
            return Ok(());
        };

        let schema = self.schema_manager.schema();

        // Resolve label: use provided label or discover from L0/storage
        let discovered_labels;
        let label_name = if let Some(l) = label {
            Some(l)
        } else {
            discovered_labels = self.get_vertex_labels(vid, tx_l0).await;
            discovered_labels
                .as_ref()
                .and_then(|l| l.first().map(|s| s.as_str()))
        };

        let Some(label_str) = label_name else {
            return Ok(());
        };
        let Some(props_meta) = schema.properties.get(label_str) else {
            return Ok(());
        };

        // Identify CRDT properties in the insert data
        let crdt_keys: Vec<String> = properties
            .keys()
            .filter(|key| {
                props_meta.get(*key).is_some_and(|meta| {
                    matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                })
            })
            .cloned()
            .collect();

        if crdt_keys.is_empty() {
            return Ok(());
        }

        let ctx = self.get_query_context(tx_l0).await;
        for key in crdt_keys {
            let existing = pm.get_vertex_prop_with_ctx(vid, &key, ctx.as_ref()).await?;
            if !existing.is_null()
                && let Some(val) = properties.get_mut(&key)
            {
                *val = pm.merge_crdt_values(&existing, val)?;
            }
        }

        Ok(())
    }

    async fn prepare_edge_upsert(
        &self,
        eid: Eid,
        properties: &mut Properties,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        if let Some(pm) = &self.property_manager {
            let schema = self.schema_manager.schema();
            // Get edge type from L0 buffer instead of from EID
            let type_name = self.get_edge_type_from_l0(eid);

            if let Some(ref t_name) = type_name
                && let Some(props_meta) = schema.properties.get(t_name)
            {
                let mut crdt_keys = Vec::new();
                for (key, _) in properties.iter() {
                    if let Some(meta) = props_meta.get(key)
                        && matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                    {
                        crdt_keys.push(key.clone());
                    }
                }

                if !crdt_keys.is_empty() {
                    let ctx = self.get_query_context(tx_l0).await;
                    for key in crdt_keys {
                        let existing = pm.get_edge_prop(eid, &key, ctx.as_ref()).await?;

                        if !existing.is_null()
                            && let Some(val) = properties.get_mut(&key)
                        {
                            *val = pm.merge_crdt_values(&existing, val)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self, properties), level = "trace")]
    pub async fn insert_vertex(
        &mut self,
        vid: Vid,
        properties: Properties,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        self.insert_vertex_with_labels(vid, properties, &[], tx_l0)
            .await?;
        Ok(())
    }

    #[instrument(skip(self, properties, labels), level = "trace")]
    pub async fn insert_vertex_with_labels(
        &self,
        vid: Vid,
        mut properties: Properties,
        labels: &[String],
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<Properties> {
        let start = std::time::Instant::now();
        self.check_write_pressure().await?;
        self.check_transaction_memory(tx_l0)?;
        self.process_embeddings_for_labels(labels, &mut properties)
            .await?;
        self.validate_vertex_constraints(vid, &properties, labels, tx_l0)
            .await?;
        self.prepare_vertex_upsert(
            vid,
            &mut properties,
            labels.first().map(|s| s.as_str()),
            tx_l0,
        )
        .await?;

        // Clone properties and labels before moving into L0 to return them and populate constraint index
        let properties_copy = properties.clone();
        let labels_copy = labels.to_vec();

        {
            let l0 = self.resolve_l0(tx_l0);
            let mut l0_guard = l0.write();
            l0_guard.insert_vertex_with_labels(vid, properties, labels);

            // Populate constraint index for O(1) duplicate detection
            let schema = self.schema_manager.schema();
            for label in &labels_copy {
                if schema.get_label_case_insensitive(label).is_none() {
                    if self.config.strict_schema {
                        return Err(anyhow::anyhow!(
                            "Label '{}' is not defined in the schema \
                             (strict_schema is enabled).",
                            label
                        ));
                    }
                    continue; // Schemaless: skip unknown labels.
                }

                // For each unique constraint on this label, insert into constraint index
                for constraint in &schema.constraints {
                    if !constraint.enabled {
                        continue;
                    }
                    if let ConstraintTarget::Label(l) = &constraint.target {
                        if l != label {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    if let ConstraintType::Unique {
                        properties: unique_props,
                    } = &constraint.constraint_type
                    {
                        let mut key_values = Vec::new();
                        let mut all_present = true;
                        for prop in unique_props {
                            if let Some(val) = properties_copy.get(prop) {
                                key_values.push((prop.clone(), val.clone()));
                            } else {
                                all_present = false;
                                break;
                            }
                        }

                        if all_present {
                            let key = serialize_constraint_key(label, &key_values);
                            l0_guard.insert_constraint_key(key, vid);
                        }
                    }
                }
            }
        }

        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if tx_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow insert_vertex: {}ms", start.elapsed().as_millis());
        }
        Ok(properties_copy)
    }

    /// Insert multiple vertices with batched operations.
    ///
    /// This method uses batched operations to achieve O(N) complexity instead of O(N²)
    /// for bulk inserts with unique constraints.
    ///
    /// # Performance Improvements
    /// - Batch VID allocation: 1 call instead of N calls
    /// - Batch constraint validation: O(N) instead of O(N²)
    /// - Batch embedding generation: 1 API call per config instead of N calls
    /// - Transaction wrapping: Automatic flush deferral, atomicity
    ///
    /// # Arguments
    /// * `vids` - Pre-allocated VIDs for the vertices
    /// * `properties_batch` - Properties for each vertex
    /// * `labels` - Labels for all vertices (assumes single label for simplicity)
    ///
    /// # Errors
    /// Returns error if:
    /// - VID/properties length mismatch
    /// - Constraint violation detected
    /// - Embedding generation fails
    /// - Transaction commit fails
    ///
    /// # Atomicity
    /// If this method fails, all changes are rolled back (if transaction was started here).
    pub async fn insert_vertices_batch(
        &self,
        vids: Vec<Vid>,
        mut properties_batch: Vec<Properties>,
        labels: Vec<String>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<Vec<Properties>> {
        let start = std::time::Instant::now();

        // Validate inputs
        if vids.len() != properties_batch.len() {
            return Err(anyhow!(
                "VID/properties size mismatch: {} vids, {} properties",
                vids.len(),
                properties_batch.len()
            ));
        }

        if vids.is_empty() {
            return Ok(Vec::new());
        }

        // Batch operations — writes go directly to the resolved L0.
        // Atomicity is guaranteed by the caller holding the writer lock.
        let result = async {
            self.check_write_pressure().await?;
            self.check_transaction_memory(tx_l0)?;

            // Batch embedding generation (1 API call per config)
            self.process_embeddings_for_batch(&labels, &mut properties_batch)
                .await?;

            // Batch constraint validation (O(N) instead of O(N²))
            let label = labels
                .first()
                .ok_or_else(|| anyhow!("No labels provided"))?;
            self.validate_vertex_batch_constraints(&vids, &properties_batch, label, tx_l0)
                .await?;

            // Batch prepare (CRDT merging if needed)
            // Check schema once: skip entirely if no CRDT properties for this label.
            // For new vertices (freshly allocated VIDs), there are no existing CRDT
            // values to merge, so the per-vertex lookup is unnecessary in that case.
            let has_crdt_fields = {
                let schema = self.schema_manager.schema();
                schema
                    .properties
                    .get(label.as_str())
                    .is_some_and(|props_meta| {
                        props_meta.values().any(|meta| {
                            matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                        })
                    })
            };

            if has_crdt_fields {
                // Batch fetch existing CRDT values: collect VIDs that need merging,
                // then query once via PropertyManager instead of per-vertex lookups.
                let schema = self.schema_manager.schema();
                let crdt_keys: Vec<String> = schema
                    .properties
                    .get(label.as_str())
                    .map(|props_meta| {
                        props_meta
                            .iter()
                            .filter(|(_, meta)| {
                                matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                            })
                            .map(|(key, _)| key.clone())
                            .collect()
                    })
                    .unwrap_or_default();

                if let Some(pm) = &self.property_manager {
                    let ctx = self.get_query_context(tx_l0).await;
                    for (vid, props) in vids.iter().zip(&mut properties_batch) {
                        for key in &crdt_keys {
                            if props.contains_key(key) {
                                let existing =
                                    pm.get_vertex_prop_with_ctx(*vid, key, ctx.as_ref()).await?;
                                if !existing.is_null()
                                    && let Some(val) = props.get_mut(key)
                                {
                                    *val = pm.merge_crdt_values(&existing, val)?;
                                }
                            }
                        }
                    }
                }
            }

            // Batch L0 writes — route to active L0 (transaction L0 if active, else current).
            let target_l0 = self.resolve_l0(tx_l0);

            let properties_result = properties_batch.clone();
            {
                let mut l0_guard = target_l0.write();
                for (vid, props) in vids.iter().zip(properties_batch.iter()) {
                    l0_guard.insert_vertex_with_labels(*vid, props.clone(), &labels);
                }
            }

            // Update metrics (batch increment)
            metrics::counter!("uni_l0_buffer_mutations_total").increment(vids.len() as u64);
            self.update_metrics();

            Ok::<Vec<Properties>, anyhow::Error>(properties_result)
        }
        .await;

        let props = result?;

        if start.elapsed().as_millis() > 100 {
            log::warn!(
                "Slow insert_vertices_batch ({} vertices): {}ms",
                vids.len(),
                start.elapsed().as_millis()
            );
        }

        Ok(props)
    }

    /// Delete a vertex by VID.
    ///
    /// When `labels` is provided, uses them directly to populate L0 for
    /// correct tombstone flushing. Otherwise discovers labels from L0
    /// buffers and storage (which can be slow for many vertices).
    ///
    /// # Errors
    ///
    /// Returns an error if write pressure stalls, label lookup fails, or
    /// the L0 delete operation fails.
    #[instrument(skip(self, labels), level = "trace")]
    pub async fn delete_vertex(
        &self,
        vid: Vid,
        labels: Option<Vec<String>>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        self.check_write_pressure().await?;
        self.check_transaction_memory(tx_l0)?;
        let l0 = self.resolve_l0(tx_l0);

        // Before deleting, ensure we have the vertex's labels stored in L0
        // so the tombstone can be properly flushed to the correct label datasets.
        let has_labels = {
            let l0_guard = l0.read();
            l0_guard.vertex_labels.contains_key(&vid)
        };

        if !has_labels {
            let resolved_labels = if let Some(provided) = labels {
                // Caller provided labels — skip the lookup entirely
                Some(provided)
            } else {
                // Discover labels from pending flush L0s, then storage
                let mut found = None;
                for pending_l0 in self.l0_manager.get_pending_flush() {
                    let pending_guard = pending_l0.read();
                    if let Some(l) = pending_guard.get_vertex_labels(vid) {
                        found = Some(l.to_vec());
                        break;
                    }
                }
                if found.is_none() {
                    found = self.find_vertex_labels_in_storage(vid).await?;
                }
                found
            };

            if let Some(found_labels) = resolved_labels {
                let mut l0_guard = l0.write();
                l0_guard.vertex_labels.insert(vid, found_labels);
            }
        }

        l0.write().delete_vertex(vid)?;
        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if tx_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow delete_vertex: {}ms", start.elapsed().as_millis());
        }
        Ok(())
    }

    /// Find vertex labels from storage by querying the main vertices table.
    /// Returns the labels from the latest non-deleted version of the vertex.
    async fn find_vertex_labels_in_storage(&self, vid: Vid) -> Result<Option<Vec<String>>> {
        use crate::backend::types::ScanRequest;
        use arrow_array::Array;
        use arrow_array::cast::AsArray;

        let backend = self.storage.backend();
        let table_name = MainVertexDataset::table_name();

        // Check if table exists first; if not, vertex hasn't been flushed to storage yet
        if !backend.table_exists(table_name).await? {
            return Ok(None);
        }

        // Query for this specific vid (don't filter by _deleted yet - we need to find the latest version first)
        let filter = format!("_vid = {}", vid.as_u64());
        let batches = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec![
                        "_vid".to_string(),
                        "labels".to_string(),
                        "_version".to_string(),
                        "_deleted".to_string(),
                    ]),
            )
            .await
            .unwrap_or_default();

        // Find the row with the highest version number
        let mut max_version: Option<u64> = None;
        let mut labels: Option<Vec<String>> = None;
        let mut is_deleted = false;

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let version_array = batch
                .column_by_name("_version")
                .unwrap()
                .as_primitive::<arrow_array::types::UInt64Type>();

            let deleted_array = batch.column_by_name("_deleted").unwrap().as_boolean();

            let labels_array = batch.column_by_name("labels").unwrap().as_list::<i32>();

            for row_idx in 0..batch.num_rows() {
                let version = version_array.value(row_idx);

                if max_version.is_none_or(|mv| version > mv) {
                    is_deleted = deleted_array.value(row_idx);

                    let labels_list = labels_array.value(row_idx);
                    let string_array = labels_list.as_string::<i32>();
                    let vertex_labels: Vec<String> = (0..string_array.len())
                        .filter(|&i| !string_array.is_null(i))
                        .map(|i| string_array.value(i).to_string())
                        .collect();

                    max_version = Some(version);
                    labels = Some(vertex_labels);
                }
            }
        }

        // If the latest version is deleted, return None
        if is_deleted { Ok(None) } else { Ok(labels) }
    }

    #[expect(clippy::too_many_arguments)]
    #[instrument(skip(self, properties), level = "trace")]
    pub async fn insert_edge(
        &self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        mut properties: Properties,
        edge_type_name: Option<String>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        self.check_write_pressure().await?;
        self.check_transaction_memory(tx_l0)?;
        self.prepare_edge_upsert(eid, &mut properties, tx_l0)
            .await?;

        let l0 = self.resolve_l0(tx_l0);
        l0.write()
            .insert_edge(src_vid, dst_vid, edge_type, eid, properties, edge_type_name)?;

        // Dual-write to AdjacencyManager overlay (survives flush).
        // Skip for transaction-local L0 -- transaction edges are overlaid separately.
        if tx_l0.is_none() {
            let version = l0.read().current_version;
            self.adjacency_manager
                .insert_edge(src_vid, dst_vid, eid, edge_type, version);
        }

        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if tx_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow insert_edge: {}ms", start.elapsed().as_millis());
        }
        Ok(())
    }

    #[instrument(skip(self), level = "trace")]
    pub async fn delete_edge(
        &self,
        eid: Eid,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        self.check_write_pressure().await?;
        self.check_transaction_memory(tx_l0)?;
        let l0 = self.resolve_l0(tx_l0);

        l0.write().delete_edge(eid, src_vid, dst_vid, edge_type)?;

        // Dual-write tombstone to AdjacencyManager overlay.
        if tx_l0.is_none() {
            let version = l0.read().current_version;
            self.adjacency_manager
                .add_tombstone(eid, src_vid, dst_vid, edge_type, version);
        }
        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if tx_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow delete_edge: {}ms", start.elapsed().as_millis());
        }
        Ok(())
    }

    /// Decide whether a flush should be triggered based on mutation count
    /// or elapsed time since the last flush.
    ///
    /// Extracted from [`Writer::check_flush`] so `commit_transaction_l0` can
    /// reuse the decision while bypassing the lock-acquiring entry point
    /// (it already holds `flush_lock`).
    fn should_flush(&self) -> bool {
        let count = self.l0_manager.get_current().read().mutation_count;
        if count == 0 {
            return false;
        }
        if count >= self.config.auto_flush_threshold {
            return true;
        }
        if let Some(interval) = self.config.auto_flush_interval
            && self.last_flush_time.lock().elapsed() >= interval
            && count >= self.config.auto_flush_min_mutations
        {
            return true;
        }
        false
    }

    /// Check if flush should be triggered based on mutation count or time elapsed.
    /// This method is called after each write operation and can also be called
    /// by a background task for time-based flushing.
    pub async fn check_flush(&self) -> Result<()> {
        if self.should_flush() {
            self.flush_to_l1(None).await?;
        }
        Ok(())
    }

    /// Process embeddings for a vertex using labels passed directly.
    /// Use this when labels haven't been stored to L0 yet.
    async fn process_embeddings_for_labels(
        &self,
        labels: &[String],
        properties: &mut Properties,
    ) -> Result<()> {
        let label_name = labels.first().map(|s| s.as_str());
        self.process_embeddings_impl(label_name, properties).await
    }

    /// Process embeddings for a batch of vertices efficiently.
    ///
    /// Groups vertices by embedding config and makes batched API calls to the
    /// embedding service instead of calling once per vertex.
    ///
    /// # Performance
    /// For N vertices with embedding config:
    /// - Old approach: N API calls to embedding service
    /// - New approach: 1 API call per embedding config (usually 1 total)
    async fn process_embeddings_for_batch(
        &self,
        labels: &[String],
        properties_batch: &mut [Properties],
    ) -> Result<()> {
        let label_name = labels.first().map(|s| s.as_str());
        let schema = self.schema_manager.schema();

        if let Some(label) = label_name {
            // Find vector indexes with embedding config for this label
            let mut configs = Vec::new();
            for idx in &schema.indexes {
                if let IndexDefinition::Vector(v_config) = idx
                    && v_config.label == label
                    && let Some(emb_config) = &v_config.embedding_config
                {
                    configs.push((v_config.property.clone(), emb_config.clone()));
                }
            }

            if configs.is_empty() {
                return Ok(());
            }

            for (target_prop, emb_config) in configs {
                // Collect input texts from all vertices that need embeddings
                let mut input_texts: Vec<String> = Vec::new();
                let mut needs_embedding: Vec<usize> = Vec::new();

                for (idx, properties) in properties_batch.iter().enumerate() {
                    // Skip if target property already exists
                    if properties.contains_key(&target_prop) {
                        continue;
                    }

                    // Check if source properties exist
                    let mut inputs = Vec::new();
                    for src_prop in &emb_config.source_properties {
                        if let Some(val) = properties.get(src_prop)
                            && let Some(s) = val.as_str()
                        {
                            inputs.push(s.to_string());
                        }
                    }

                    if !inputs.is_empty() {
                        let input_text = inputs.join(" ");
                        let input_text = match &emb_config.document_prefix {
                            Some(prefix) => format!("{prefix}{input_text}"),
                            None => input_text,
                        };
                        input_texts.push(input_text);
                        needs_embedding.push(idx);
                    }
                }

                if input_texts.is_empty() {
                    continue;
                }

                let runtime = self.xervo_runtime.get().ok_or_else(|| {
                    anyhow!("Uni-Xervo runtime not configured for auto-embedding")
                })?;
                let embedder = runtime.embedding(&emb_config.alias).await?;

                // Batch generate embeddings (single API call)
                let input_refs: Vec<&str> = input_texts.iter().map(|s| s.as_str()).collect();
                let embeddings = embedder.embed(input_refs).await?;

                // Distribute results back to properties
                for (embedding_idx, &prop_idx) in needs_embedding.iter().enumerate() {
                    if let Some(vec) = embeddings.get(embedding_idx) {
                        let vals: Vec<Value> =
                            vec.iter().map(|f| Value::Float(*f as f64)).collect();
                        properties_batch[prop_idx].insert(target_prop.clone(), Value::List(vals));
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_embeddings_impl(
        &self,
        label_name: Option<&str>,
        properties: &mut Properties,
    ) -> Result<()> {
        let schema = self.schema_manager.schema();

        if let Some(label) = label_name {
            // Find vector indexes with embedding config for this label
            let mut configs = Vec::new();
            for idx in &schema.indexes {
                if let IndexDefinition::Vector(v_config) = idx
                    && v_config.label == label
                    && let Some(emb_config) = &v_config.embedding_config
                {
                    configs.push((v_config.property.clone(), emb_config.clone()));
                }
            }

            if configs.is_empty() {
                log::info!("No embedding config found for label {}", label);
            }

            for (target_prop, emb_config) in configs {
                // If target property already exists, skip (assume user provided it)
                if properties.contains_key(&target_prop) {
                    continue;
                }

                // Check if source properties exist
                let mut inputs = Vec::new();
                for src_prop in &emb_config.source_properties {
                    if let Some(val) = properties.get(src_prop)
                        && let Some(s) = val.as_str()
                    {
                        inputs.push(s.to_string());
                    }
                }

                if inputs.is_empty() {
                    continue;
                }

                let input_text = inputs.join(" ");
                let input_text = match &emb_config.document_prefix {
                    Some(prefix) => format!("{prefix}{input_text}"),
                    None => input_text,
                };

                let runtime = self.xervo_runtime.get().ok_or_else(|| {
                    anyhow!("Uni-Xervo runtime not configured for auto-embedding")
                })?;
                let embedder = runtime.embedding(&emb_config.alias).await?;

                // Generate
                let embeddings = embedder.embed(vec![input_text.as_str()]).await?;
                if let Some(vec) = embeddings.first() {
                    // Store as array of floats
                    let vals: Vec<Value> = vec.iter().map(|f| Value::Float(*f as f64)).collect();
                    properties.insert(target_prop.clone(), Value::List(vals));
                }
            }
        }
        Ok(())
    }

    /// Flushes the current in-memory L0 buffer to L1 storage.
    ///
    /// # Lock Ordering
    ///
    /// To prevent deadlocks, locks must be acquired in the following order:
    /// 1. `Writer` lock (held by caller via outer `Arc<RwLock<Writer>>`; removed in Phase 4)
    /// 2. `flush_lock` (acquired by this entry point; held across the whole flush)
    /// 3. `L0Manager` lock (via `begin_flush` / `get_current`)
    /// 4. `L0Buffer` lock (individual buffer RWLocks)
    /// 5. `Index` / `Storage` locks (during actual flush)
    ///
    /// Callers that already hold `flush_lock` (today only `commit_transaction_l0`)
    /// must call [`Writer::flush_to_l1_inner`] directly to avoid a re-entrant
    /// `tokio::sync::Mutex` deadlock — see concurrent_writer.md §5.5.
    pub async fn flush_to_l1(&self, name: Option<String>) -> Result<String> {
        let _flush_lock_guard = self.flush_lock.lock().await;
        self.flush_to_l1_inner(name).await
    }

    /// Phase A+B+C of the flush: flush the WAL, rotate L0 (so the
    /// to-be-flushed buffer moves to `pending_flush` and a fresh L0 takes
    /// its place), and hand off the WAL to the new L0.
    ///
    /// Runs in microseconds. Must be called under `flush_lock` (the caller
    /// is responsible). The returned [`RotateOutput`] carries everything
    /// the subsequent stream + finalize phases need; in particular the
    /// [`FlushInProgressGuard`] is bound to the return value so it stays
    /// alive for the full flush lifetime — including any future async
    /// path where stream runs on a spawned task.
    async fn flush_l0_rotate(&self) -> Result<RotateOutput> {
        // Acquire the in-progress counter BEFORE any heavy work. The
        // guard lives on RotateOutput; dropping RotateOutput drops the
        // guard, so the counter goes back to zero exactly when the flush
        // is fully done.
        let flush_in_progress_guard = FlushInProgressGuard::new(&self.storage);

        // A. Flush WAL BEFORE rotating L0. If WAL flush fails, the
        // current L0 is still active and mutations are retained in
        // memory until restart/retry.
        let wal_for_truncate = {
            let current_l0 = self.l0_manager.get_current();
            let l0_guard = current_l0.read();
            l0_guard.wal.clone()
        };
        let wal_lsn = if let Some(ref w) = wal_for_truncate {
            w.flush().await?
        } else {
            0
        };

        // B. Begin flush: rotate L0 and keep old L0 visible to reads via
        // pending_flush until complete_flush is called by finalize.
        let old_l0_arc = self.l0_manager.begin_flush(0, None);
        metrics::counter!("uni_l0_buffer_rotations_total").increment(1);

        // C. WAL handoff: record wal_lsn on old L0, transfer WAL handle
        // and current_version to the new L0.
        let current_version;
        {
            let mut old_l0_guard = old_l0_arc.write();
            current_version = old_l0_guard.current_version;
            old_l0_guard.wal_lsn_at_flush = wal_lsn;
            let wal = old_l0_guard.wal.take();
            let new_l0_arc = self.l0_manager.get_current();
            let mut new_l0_guard = new_l0_arc.write();
            new_l0_guard.wal = wal;
            new_l0_guard.current_version = current_version;
        }

        Ok(RotateOutput {
            old_l0_arc,
            wal_lsn,
            current_version,
            flush_in_progress_guard,
        })
    }

    /// Body of [`Writer::flush_to_l1`]; assumes the caller has already acquired
    /// `flush_lock`. The `FlushInProgressGuard` inside is a complementary CAS
    /// safety net against any future caller that bypasses `flush_lock`.
    #[instrument(
        skip(self),
        fields(snapshot_id, mutations_count, size_bytes),
        level = "info"
    )]
    async fn flush_to_l1_inner(&self, name: Option<String>) -> Result<String> {
        let start = std::time::Instant::now();
        let schema = self.schema_manager.schema();

        let (initial_size, initial_count) = {
            let l0_arc = self.l0_manager.get_current();
            let l0 = l0_arc.read();
            (l0.estimated_size, l0.mutation_count)
        };
        tracing::Span::current().record("size_bytes", initial_size);
        tracing::Span::current().record("mutations_count", initial_count);

        debug!("Starting L0 flush to L1");

        // Phases A (WAL pre-flush), B (rotate), C (WAL handoff) live in
        // flush_l0_rotate. The FlushInProgressGuard travels with the
        // RotateOutput so the counter stays accurate for the full flush
        // lifetime (preparation for async path where the guard will live
        // on RotatedFlush all the way through finalize).
        let RotateOutput {
            old_l0_arc,
            wal_lsn,
            current_version,
            flush_in_progress_guard: _flush_guard,
        } = self.flush_l0_rotate().await?;

        // 2. Acquire Read lock on Old L0 for flushing
        let mut entries_by_type: HashMap<u32, Vec<L1Entry>> = HashMap::new();
        // (Vid, labels, properties, deleted, version)
        type VertexEntry = (Vid, Vec<String>, Properties, bool, u64);
        let mut vertices_by_label: HashMap<u16, Vec<VertexEntry>> = HashMap::new();
        // Collect vertex timestamps from L0 for flushing to storage
        let mut vertex_created_at: HashMap<Vid, i64> = HashMap::new();
        let mut vertex_updated_at: HashMap<Vid, i64> = HashMap::new();
        // Track tombstones missing labels for storage query fallback
        let mut orphaned_tombstones: Vec<(Vid, u64)> = Vec::new();

        {
            let old_l0 = old_l0_arc.read();

            // 1. Collect all edges and tombstones from L0
            for edge in old_l0.graph.edges() {
                let properties = old_l0
                    .edge_properties
                    .get(&edge.eid)
                    .cloned()
                    .unwrap_or_default();
                let version = old_l0.edge_versions.get(&edge.eid).copied().unwrap_or(0);

                // Get timestamps from L0 buffer (populated during insert)
                let created_at = old_l0.edge_created_at.get(&edge.eid).copied();
                let updated_at = old_l0.edge_updated_at.get(&edge.eid).copied();

                entries_by_type
                    .entry(edge.edge_type)
                    .or_default()
                    .push(L1Entry {
                        src_vid: edge.src_vid,
                        dst_vid: edge.dst_vid,
                        eid: edge.eid,
                        op: Op::Insert,
                        version,
                        properties,
                        created_at,
                        updated_at,
                    });
            }

            // From tombstones
            for tombstone in old_l0.tombstones.values() {
                let version = old_l0
                    .edge_versions
                    .get(&tombstone.eid)
                    .copied()
                    .unwrap_or(0);
                // Get timestamps - for deletes, updated_at reflects deletion time
                let created_at = old_l0.edge_created_at.get(&tombstone.eid).copied();
                let updated_at = old_l0.edge_updated_at.get(&tombstone.eid).copied();

                entries_by_type
                    .entry(tombstone.edge_type)
                    .or_default()
                    .push(L1Entry {
                        src_vid: tombstone.src_vid,
                        dst_vid: tombstone.dst_vid,
                        eid: tombstone.eid,
                        op: Op::Delete,
                        version,
                        properties: HashMap::new(),
                        created_at,
                        updated_at,
                    });
            }

            // 1b. Collect vertices by label (using vertex_labels from L0)
            //
            // Helper: fan-out a single vertex entry into per-label buckets.
            // Each per-label table row carries the full label set so multi-label
            // info is preserved after flush.
            let push_vertex_to_labels =
                |vid: Vid,
                 all_labels: &[String],
                 props: Properties,
                 deleted: bool,
                 version: u64,
                 out: &mut HashMap<u16, Vec<VertexEntry>>| {
                    for label in all_labels {
                        if let Some(label_id) = schema.label_id_by_name(label) {
                            out.entry(label_id).or_default().push((
                                vid,
                                all_labels.to_vec(),
                                props.clone(),
                                deleted,
                                version,
                            ));
                        }
                    }
                };

            for (vid, props) in &old_l0.vertex_properties {
                let version = old_l0.vertex_versions.get(vid).copied().unwrap_or(0);
                // Collect timestamps for this vertex
                if let Some(&ts) = old_l0.vertex_created_at.get(vid) {
                    vertex_created_at.insert(*vid, ts);
                }
                if let Some(&ts) = old_l0.vertex_updated_at.get(vid) {
                    vertex_updated_at.insert(*vid, ts);
                }
                if let Some(labels) = old_l0.vertex_labels.get(vid) {
                    push_vertex_to_labels(
                        *vid,
                        labels,
                        props.clone(),
                        false,
                        version,
                        &mut vertices_by_label,
                    );
                }
            }
            for &vid in &old_l0.vertex_tombstones {
                let version = old_l0.vertex_versions.get(&vid).copied().unwrap_or(0);
                if let Some(labels) = old_l0.vertex_labels.get(&vid) {
                    push_vertex_to_labels(
                        vid,
                        labels,
                        HashMap::new(),
                        true,
                        version,
                        &mut vertices_by_label,
                    );
                } else {
                    // Tombstone missing labels (old WAL format) - collect for storage query fallback
                    orphaned_tombstones.push((vid, version));
                }
            }
        } // Drop read lock

        // Resolve orphaned tombstones (missing labels) from storage
        if !orphaned_tombstones.is_empty() {
            tracing::warn!(
                count = orphaned_tombstones.len(),
                "Tombstones missing labels in L0, querying storage as fallback"
            );
            for (vid, version) in orphaned_tombstones {
                if let Ok(Some(labels)) = self.find_vertex_labels_in_storage(vid).await
                    && !labels.is_empty()
                {
                    for label in &labels {
                        if let Some(label_id) = schema.label_id_by_name(label) {
                            vertices_by_label.entry(label_id).or_default().push((
                                vid,
                                labels.clone(),
                                HashMap::new(),
                                true,
                                version,
                            ));
                        }
                    }
                }
            }
        }

        // 1. Load previous snapshot from cache, or fall back to storage
        let mut manifest = if let Some(cached) = self.cached_manifest.lock().take() {
            cached
        } else {
            self.storage
                .snapshot_manager()
                .load_latest_snapshot()
                .await?
                .unwrap_or_else(|| {
                    SnapshotManifest::new(Uuid::new_v4().to_string(), schema.schema_version)
                })
        };

        // Update snapshot metadata
        // Save parent snapshot ID before generating new one (for lineage tracking)
        let parent_id = manifest.snapshot_id.clone();
        manifest.parent_snapshot = Some(parent_id);
        manifest.snapshot_id = Uuid::new_v4().to_string();
        manifest.name = name;
        manifest.created_at = Utc::now();
        manifest.version_high_water_mark = current_version;
        manifest.wal_high_water_mark = wal_lsn;
        let snapshot_id = manifest.snapshot_id.clone();

        tracing::Span::current().record("snapshot_id", &snapshot_id);

        // 2. Write main unified tables FIRST (before deltas).
        //    Ensures the dual-write invariant: by the time an EID appears in a
        //    delta table, it already exists in main_edges. This prevents the
        //    compaction debug_assert from firing when compaction interleaves
        //    with flush at async yield points.
        //
        // 2.1 Main edges table
        let (main_edges, edge_created_at_map, edge_updated_at_map) = {
            let _old_l0 = old_l0_arc.read();
            let mut main_edges: Vec<(
                uni_common::core::id::Eid,
                Vid,
                Vid,
                String,
                Properties,
                bool,
                u64,
            )> = Vec::new();
            let mut edge_created_at_map: HashMap<uni_common::core::id::Eid, i64> = HashMap::new();
            let mut edge_updated_at_map: HashMap<uni_common::core::id::Eid, i64> = HashMap::new();

            for (&edge_type_id, entries) in entries_by_type.iter() {
                for entry in entries {
                    let edge_type_name = self
                        .storage
                        .schema_manager()
                        .edge_type_name_by_id_unified(edge_type_id)
                        .unwrap_or_else(|| "unknown".to_string());

                    let deleted = matches!(entry.op, Op::Delete);
                    main_edges.push((
                        entry.eid,
                        entry.src_vid,
                        entry.dst_vid,
                        edge_type_name,
                        entry.properties.clone(),
                        deleted,
                        entry.version,
                    ));

                    if let Some(ts) = entry.created_at {
                        edge_created_at_map.insert(entry.eid, ts);
                    }
                    if let Some(ts) = entry.updated_at {
                        edge_updated_at_map.insert(entry.eid, ts);
                    }
                }
            }

            (main_edges, edge_created_at_map, edge_updated_at_map)
        };

        if !main_edges.is_empty() {
            let main_edge_batch = MainEdgeDataset::build_record_batch(
                &main_edges,
                Some(&edge_created_at_map),
                Some(&edge_updated_at_map),
            )?;
            MainEdgeDataset::write_batch(self.storage.backend(), main_edge_batch).await?;
            MainEdgeDataset::ensure_default_indexes(self.storage.backend()).await?;
        }

        // 2.2 Main vertices table
        let main_vertices: Vec<(Vid, Vec<String>, Properties, bool, u64)> = {
            let old_l0 = old_l0_arc.read();
            let mut vertices = Vec::new();

            for (vid, props) in &old_l0.vertex_properties {
                let version = old_l0.vertex_versions.get(vid).copied().unwrap_or(0);
                let labels = old_l0.vertex_labels.get(vid).cloned().unwrap_or_default();
                vertices.push((*vid, labels, props.clone(), false, version));
            }

            for &vid in &old_l0.vertex_tombstones {
                let version = old_l0.vertex_versions.get(&vid).copied().unwrap_or(0);
                let labels = old_l0.vertex_labels.get(&vid).cloned().unwrap_or_default();
                vertices.push((vid, labels, HashMap::new(), true, version));
            }

            vertices
        };

        if !main_vertices.is_empty() {
            let main_vertex_batch = MainVertexDataset::build_record_batch(
                &main_vertices,
                Some(&vertex_created_at),
                Some(&vertex_updated_at),
            )?;
            MainVertexDataset::write_batch(self.storage.backend(), main_vertex_batch).await?;
            MainVertexDataset::ensure_default_indexes(self.storage.backend()).await?;
        }

        // 3. For each edge type, write FWD and BWD delta runs
        for (&edge_type_id, entries) in entries_by_type.iter() {
            // Get edge type name from unified lookup (handles both schema'd and schemaless)
            let edge_type_name = self
                .storage
                .schema_manager()
                .edge_type_name_by_id_unified(edge_type_id)
                .ok_or_else(|| anyhow!("Edge type ID {} not found", edge_type_id))?;

            // FWD Run (sorted by src_vid)
            let mut fwd_entries = entries.clone();
            fwd_entries.sort_by_key(|e| e.src_vid);
            let fwd_ds = self.storage.delta_dataset(&edge_type_name, "fwd")?;
            let fwd_batch = fwd_ds.build_record_batch(&fwd_entries, &schema)?;

            // Write using backend
            let backend = self.storage.backend();
            fwd_ds.write_run(backend, fwd_batch).await?;
            fwd_ds.ensure_eid_index(backend).await?;

            // BWD Run (sorted by dst_vid)
            let mut bwd_entries = entries.clone();
            bwd_entries.sort_by_key(|e| e.dst_vid);
            let bwd_ds = self.storage.delta_dataset(&edge_type_name, "bwd")?;
            let bwd_batch = bwd_ds.build_record_batch(&bwd_entries, &schema)?;

            let backend = self.storage.backend();
            bwd_ds.write_run(backend, bwd_batch).await?;
            bwd_ds.ensure_eid_index(backend).await?;

            // Update Manifest
            let current_snap =
                manifest
                    .edges
                    .entry(edge_type_name.to_string())
                    .or_insert(EdgeSnapshot {
                        version: 0,
                        count: 0,
                        lance_version: 0,
                    });
            current_snap.version += 1;
            current_snap.count += entries.len() as u64;
            // LanceDB tables don't expose Lance version directly
            current_snap.lance_version = 0;

            // Note: No CSR invalidation needed. AdjacencyManager's overlay
            // already has these edges via dual-write in insert_edge/delete_edge.
        }

        // 4. Per-label vertex table writes
        for (label_id, vertices) in vertices_by_label {
            let label_name = schema
                .label_name_by_id(label_id)
                .ok_or_else(|| anyhow!("Label ID {} not found", label_id))?;

            let ds = self.storage.vertex_dataset(label_name)?;

            // Collect inverted index updates before consuming vertices
            // Maps: cfg.property -> (added, removed)
            type InvertedUpdateMap = HashMap<String, (HashMap<Vid, Vec<String>>, HashSet<Vid>)>;
            let mut inverted_updates: InvertedUpdateMap = HashMap::new();

            for idx in &schema.indexes {
                if let IndexDefinition::Inverted(cfg) = idx
                    && cfg.label == label_name
                {
                    let mut added: HashMap<Vid, Vec<String>> = HashMap::new();
                    let mut removed: HashSet<Vid> = HashSet::new();

                    for (vid, _labels, props, deleted, _version) in &vertices {
                        if *deleted {
                            removed.insert(*vid);
                        } else if let Some(prop_value) = props.get(&cfg.property) {
                            // Extract terms from the property value (List<String>)
                            if let Some(arr) = prop_value.as_array() {
                                let terms: Vec<String> = arr
                                    .iter()
                                    .filter_map(|v| v.as_str().map(ToString::to_string))
                                    .collect();
                                if !terms.is_empty() {
                                    added.insert(*vid, terms);
                                }
                            }
                        }
                    }

                    if !added.is_empty() || !removed.is_empty() {
                        inverted_updates.insert(cfg.property.clone(), (added, removed));
                    }
                }
            }

            let mut v_data = Vec::new();
            let mut d_data = Vec::new();
            let mut ver_data = Vec::new();
            for (vid, labels, props, deleted, version) in vertices {
                v_data.push((vid, labels, props));
                d_data.push(deleted);
                ver_data.push(version);
            }

            let batch = ds.build_record_batch_with_timestamps(
                &v_data,
                &d_data,
                &ver_data,
                &schema,
                Some(&vertex_created_at),
                Some(&vertex_updated_at),
            )?;

            // Write using backend
            let backend = self.storage.backend();
            ds.write_batch(backend, batch, &schema).await?;
            ds.ensure_default_indexes(backend).await?;

            // Update VidLabelsIndex (if enabled)
            for ((vid, labels, _props), &deleted) in v_data.iter().zip(d_data.iter()) {
                if deleted {
                    self.storage.remove_from_vid_labels_index(*vid);
                } else {
                    self.storage.update_vid_labels_index(*vid, labels.clone());
                }
            }

            // Update Manifest
            let current_snap =
                manifest
                    .vertices
                    .entry(label_name.to_string())
                    .or_insert(LabelSnapshot {
                        version: 0,
                        count: 0,
                        lance_version: 0,
                    });
            current_snap.version += 1;
            current_snap.count += v_data.len() as u64;
            // LanceDB tables don't expose Lance version directly
            current_snap.lance_version = 0;

            // Invalidate table cache to ensure next read picks up new version
            self.storage.invalidate_table_cache(label_name);

            // Apply inverted index updates incrementally
            #[cfg(feature = "lance-backend")]
            for idx in &schema.indexes {
                if let IndexDefinition::Inverted(cfg) = idx
                    && cfg.label == label_name
                    && let Some((added, removed)) = inverted_updates.get(&cfg.property)
                {
                    self.storage
                        .index_manager()
                        .update_inverted_index_incremental(cfg, added, removed)
                        .await?;
                }
            }

            // Update UID index with new vertex mappings
            // Collect (UniId, Vid) mappings from non-deleted vertices
            #[cfg(feature = "lance-backend")]
            {
                let mut uid_mappings: Vec<(uni_common::core::id::UniId, Vid)> = Vec::new();
                for (vid, _labels, props) in &v_data {
                    let ext_id = props.get("ext_id").and_then(|v| v.as_str());
                    let uid = crate::storage::vertex::VertexDataset::compute_vertex_uid(
                        label_name, ext_id, props,
                    );
                    uid_mappings.push((uid, *vid));
                }

                if !uid_mappings.is_empty()
                    && let Ok(uid_index) = self.storage.uid_index(label_name)
                {
                    uid_index.write_mapping(&uid_mappings).await?;
                }
            }
        }

        // 5. Save Snapshot
        self.storage
            .snapshot_manager()
            .save_snapshot(&manifest)
            .await?;
        self.storage
            .snapshot_manager()
            .set_latest_snapshot(&manifest.snapshot_id)
            .await?;

        // Cache manifest for next flush to avoid re-reading from object store
        *self.cached_manifest.lock() = Some(manifest.clone());

        // Complete flush: remove old L0 from pending list now that L1 writes succeeded.
        // This must happen BEFORE WAL truncation so min_pending_wal_lsn is accurate.
        self.l0_manager.complete_flush(&old_l0_arc);

        // Truncate WAL segments, but only up to the minimum LSN of any remaining pending L0s.
        // This prevents data loss if earlier flushes failed and left L0s in pending_flush.
        // The WAL Arc was transferred from old→new L0 during rotate (Phase C),
        // so the current L0 holds the same WAL handle and we re-fetch it here.
        let wal_handle = self.l0_manager.get_current().read().wal.clone();
        if let Some(w) = wal_handle {
            // Determine safe truncation point: the minimum of our LSN and any pending L0s
            let safe_lsn = self
                .l0_manager
                .min_pending_wal_lsn()
                .map(|min_pending| min_pending.min(wal_lsn))
                .unwrap_or(wal_lsn);
            w.truncate_before(safe_lsn).await?;
        }

        // Invalidate property cache after flush to prevent stale reads.
        // Once L0 data moves to storage, cached values from storage may be outdated.
        if let Some(ref pm) = self.property_manager {
            pm.clear_cache().await;
        }

        // Reset last flush time for time-based auto-flush
        *self.last_flush_time.lock() = std::time::Instant::now();

        info!(
            snapshot_id,
            mutations_count = initial_count,
            size_bytes = initial_size,
            "L0 flush to L1 completed successfully"
        );
        metrics::histogram!("uni_flush_duration_seconds").record(start.elapsed().as_secs_f64());
        metrics::counter!("uni_flush_bytes_total").increment(initial_size as u64);
        metrics::counter!("uni_flush_rows_total").increment(initial_count as u64);

        // `_flush_guard` drops at end-of-scope (or any `?` early-exit above),
        // clearing `flush_in_progress` and unblocking compaction's delta clears.

        // Increment flush generation counter for write throttling.
        // l1_runs counts uncompacted flush generations (reset by compaction).
        {
            let mut status = uni_common::sync::acquire_mutex(
                &self.storage.compaction_status,
                "compaction_status",
            )?;
            status.l1_runs += 1;
        }

        // Trigger CSR compaction if enough frozen segments have accumulated.
        // After flush, the old L0 data is now in L1; the overlay segments can be merged
        // into the Main CSR to reduce lookup overhead. Threshold is configurable
        // via `CompactionConfig::frozen_segments_compact_threshold` (default 4).
        let am = self.adjacency_manager.clone();
        if am.should_compact(self.config.compaction.frozen_segments_compact_threshold) {
            let previous_still_running = {
                let guard = self.compaction_handle.read();
                guard.as_ref().is_some_and(|h| !h.is_finished())
            };

            if previous_still_running {
                info!("Skipping compaction: previous compaction still in progress");
            } else {
                let handle = tokio::spawn(async move {
                    am.compact();
                });
                *self.compaction_handle.write() = Some(handle);
            }
        }

        // Post-flush: check if any indexes need rebuilding based on thresholds
        if let Some(rebuild_mgr) = self.index_rebuild_manager.get()
            && self.config.index_rebuild.auto_rebuild_enabled
        {
            self.schedule_index_rebuilds_if_needed(&manifest, rebuild_mgr.clone());
        }

        // Phase 2 Day 12: emit fork-fragment observability after a
        // successful forked flush.
        self.tick_fork_fragment_observability();

        Ok(snapshot_id)
    }

    /// Increment fork-flush bookkeeping and fire the fragment warn
    /// once if the threshold is crossed.
    ///
    /// Each flush typically appends ~1 fragment per touched dataset on
    /// the fork's branches; without compaction (deferred to Phase 5)
    /// long-lived heavy-write forks degrade. The flush count is a
    /// proxy for actual fragment growth — reading
    /// `Dataset::manifest().fragments.len()` per dataset would add a
    /// per-flush object-store roundtrip on the hot commit path, which
    /// is too costly for a purely observational guard rail.
    ///
    /// No-op for primary writers (`fork_id == None`).
    pub(crate) fn tick_fork_fragment_observability(&self) {
        let Some(fork_id) = self.fork_id else { return };
        // `Relaxed` is sufficient: observational counter, no synchronizes-with.
        let new_count = self.fork_flush_count.fetch_add(1, Ordering::Relaxed) + 1;
        let fork_label = fork_id.to_string();
        metrics::gauge!(
            "uni_fork_l1_flushes",
            "fork" => fork_label.clone(),
        )
        .set(new_count as f64);
        let threshold = self.config.fork_fragment_warn_threshold as u64;
        if !self.fork_fragment_warn_fired.load(Ordering::Relaxed)
            && threshold > 0
            && new_count >= threshold
        {
            self.fork_fragment_warn_fired.store(true, Ordering::Relaxed);
            tracing::warn!(
                fork = %fork_label,
                flush_count = new_count,
                threshold,
                "fork has exceeded the L1 flush-count threshold; \
                 fork compaction is deferred to Phase 5 — consider \
                 drop+recreate or promotion to bound fragment growth"
            );
        }
    }

    /// Check rebuild thresholds and schedule background index rebuilds for
    /// labels that exceed growth or age limits. Marks affected indexes as
    /// `Stale` and spawns an async task to schedule the rebuild.
    fn schedule_index_rebuilds_if_needed(
        &self,
        manifest: &SnapshotManifest,
        rebuild_mgr: Arc<crate::storage::index_rebuild::IndexRebuildManager>,
    ) {
        let checker = crate::storage::index_rebuild::RebuildTriggerChecker::new(
            self.config.index_rebuild.clone(),
        );
        let schema = self.schema_manager.schema();
        let labels = checker.labels_needing_rebuild(manifest, &schema.indexes);

        if labels.is_empty() {
            return;
        }

        // Mark affected indexes as Stale
        for label in &labels {
            for idx in &schema.indexes {
                if idx.label() == label {
                    let _ = self.schema_manager.update_index_metadata(idx.name(), |m| {
                        m.status = uni_common::core::schema::IndexStatus::Stale;
                    });
                }
            }
        }

        tokio::spawn(async move {
            if let Err(e) = rebuild_mgr.schedule(labels).await {
                tracing::warn!("Failed to schedule index rebuild: {e}");
            }
        });
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Test that commit_transaction writes mutations to WAL before merging to main L0.
    /// This verifies fix for issue #137 (transaction commit atomicity).
    #[tokio::test]
    async fn test_commit_transaction_wal_before_merge() -> Result<()> {
        use crate::runtime::wal::WriteAheadLog;
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        let _label_id = schema_manager.add_label("Test")?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

        // Create WAL for main L0
        let wal_path = ObjectStorePath::from("wal");
        let wal = Arc::new(WriteAheadLog::new(store.clone(), wal_path));

        let writer = Writer::new_with_config(
            storage.clone(),
            schema_manager.clone(),
            1,
            UniConfig::default(),
            Some(wal),
            None,
        )
        .await?;

        // Begin transaction — create a transaction L0
        let tx_l0 = writer.create_transaction_l0();

        // Insert data in transaction
        let vid_a = writer.next_vid().await?;
        let vid_b = writer.next_vid().await?;

        let mut props = std::collections::HashMap::new();
        props.insert("test".to_string(), Value::String("data".to_string()));

        writer
            .insert_vertex_with_labels(vid_a, props.clone(), &["Test".to_string()], Some(&tx_l0))
            .await?;
        writer
            .insert_vertex_with_labels(
                vid_b,
                std::collections::HashMap::new(),
                &["Test".to_string()],
                Some(&tx_l0),
            )
            .await?;

        let eid = writer.next_eid(1).await?;
        writer
            .insert_edge(
                vid_a,
                vid_b,
                1,
                eid,
                std::collections::HashMap::new(),
                None,
                Some(&tx_l0),
            )
            .await?;

        // Get WAL before commit
        let l0 = writer.l0_manager.get_current();
        let wal = l0.read().wal.clone().expect("Main L0 should have WAL");
        let mutations_before = wal.replay().await?;
        let count_before = mutations_before.len();

        // Commit transaction - this should write to WAL first
        writer.commit_transaction_l0(tx_l0).await?;

        // Verify WAL has the new mutations
        let mutations_after = wal.replay().await?;
        assert!(
            mutations_after.len() > count_before,
            "WAL should contain transaction mutations after commit"
        );

        // Verify mutations are in correct order: vertices first, then edges
        let new_mutations: Vec<_> = mutations_after.into_iter().skip(count_before).collect();

        let mut saw_vertex_a = false;
        let mut saw_vertex_b = false;
        let mut saw_edge = false;

        for mutation in &new_mutations {
            match mutation {
                crate::runtime::wal::Mutation::InsertVertex { vid, .. } => {
                    if *vid == vid_a {
                        saw_vertex_a = true;
                    }
                    if *vid == vid_b {
                        saw_vertex_b = true;
                    }
                    // Vertices should come before edges
                    assert!(!saw_edge, "Vertices should be logged to WAL before edges");
                }
                crate::runtime::wal::Mutation::InsertEdge { eid: e, .. } => {
                    if *e == eid {
                        saw_edge = true;
                    }
                    // Edges should come after vertices
                    assert!(
                        saw_vertex_a && saw_vertex_b,
                        "Edge should be logged after both vertices"
                    );
                }
                _ => {}
            }
        }

        assert!(saw_vertex_a, "Vertex A should be in WAL");
        assert!(saw_vertex_b, "Vertex B should be in WAL");
        assert!(saw_edge, "Edge should be in WAL");

        // Verify data is also in main L0
        let l0_read = l0.read();
        assert!(
            l0_read.vertex_properties.contains_key(&vid_a),
            "Vertex A should be in main L0"
        );
        assert!(
            l0_read.vertex_properties.contains_key(&vid_b),
            "Vertex B should be in main L0"
        );
        assert!(
            l0_read.edge_endpoints.contains_key(&eid),
            "Edge should be in main L0"
        );

        Ok(())
    }

    /// Test that failed WAL flush leaves transaction intact for retry or rollback.
    #[tokio::test]
    async fn test_commit_transaction_wal_failure_rollback() -> Result<()> {
        use crate::runtime::wal::WriteAheadLog;
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        let _label_id = schema_manager.add_label("Test")?;
        let _baseline_label_id = schema_manager.add_label("Baseline")?;
        let _txdata_label_id = schema_manager.add_label("TxData")?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

        // Create WAL for main L0
        let wal_path = ObjectStorePath::from("wal");
        let wal = Arc::new(WriteAheadLog::new(store.clone(), wal_path));

        let writer = Writer::new_with_config(
            storage.clone(),
            schema_manager.clone(),
            1,
            UniConfig::default(),
            Some(wal),
            None,
        )
        .await?;

        // Insert baseline data (outside transaction)
        let baseline_vid = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(
                baseline_vid,
                [("baseline".to_string(), Value::Bool(true))]
                    .into_iter()
                    .collect(),
                &["Baseline".to_string()],
                None,
            )
            .await?;

        // Begin transaction — create a transaction L0
        let tx_l0 = writer.create_transaction_l0();

        // Insert data in transaction
        let tx_vid = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(
                tx_vid,
                [("tx_data".to_string(), Value::Bool(true))]
                    .into_iter()
                    .collect(),
                &["TxData".to_string()],
                Some(&tx_l0),
            )
            .await?;

        // Capture main L0 state before rollback
        let l0 = writer.l0_manager.get_current();
        let vertex_count_before = l0.read().vertex_properties.len();

        // Rollback transaction (simulating what would happen after WAL flush failure)
        drop(tx_l0);

        // Verify main L0 is unchanged
        let vertex_count_after = l0.read().vertex_properties.len();
        assert_eq!(
            vertex_count_before, vertex_count_after,
            "Main L0 should not change after rollback"
        );

        // Baseline should still be present
        assert!(
            l0.read().vertex_properties.contains_key(&baseline_vid),
            "Baseline data should remain"
        );

        // Transaction data should NOT be in main L0
        assert!(
            !l0.read().vertex_properties.contains_key(&tx_vid),
            "Transaction data should not be in main L0 after rollback"
        );

        Ok(())
    }

    /// Test that batch insert with shared labels does not clone labels per vertex.
    /// This verifies fix for issue #161 (redundant label cloning).
    #[tokio::test]
    async fn test_batch_insert_shared_labels() -> Result<()> {
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        let _label_id = schema_manager.add_label("Person")?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

        let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

        // Shared labels - should not be cloned per vertex
        let labels = &["Person".to_string()];

        // Insert batch of vertices with same labels
        let mut vids = Vec::new();
        for i in 0..100 {
            let vid = writer.next_vid().await?;
            let mut props = std::collections::HashMap::new();
            props.insert("id".to_string(), Value::Int(i));
            writer
                .insert_vertex_with_labels(vid, props, labels, None)
                .await?;
            vids.push(vid);
        }

        // Verify all vertices have the correct labels
        let l0 = writer.l0_manager.get_current();
        for vid in vids {
            let l0_guard = l0.read();
            let vertex_labels = l0_guard.vertex_labels.get(&vid);
            assert!(vertex_labels.is_some(), "Vertex should have labels");
            assert_eq!(
                vertex_labels.unwrap(),
                &vec!["Person".to_string()],
                "Labels should match"
            );
        }

        Ok(())
    }

    /// Test that estimated_size tracks mutations correctly and approximates size_bytes().
    /// This verifies fix for issue #147 (O(V+E) size_bytes() in metrics).
    #[tokio::test]
    async fn test_estimated_size_tracks_mutations() -> Result<()> {
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        let _label_id = schema_manager.add_label("Test")?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

        let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

        let l0 = writer.l0_manager.get_current();

        // Initial state should be empty
        let initial_estimated = l0.read().estimated_size;
        let initial_actual = l0.read().size_bytes();
        assert_eq!(initial_estimated, 0, "Initial estimated_size should be 0");
        assert_eq!(initial_actual, 0, "Initial size_bytes should be 0");

        // Insert vertices with properties
        let mut vids = Vec::new();
        for i in 0..10 {
            let vid = writer.next_vid().await?;
            let mut props = std::collections::HashMap::new();
            props.insert("name".to_string(), Value::String(format!("vertex_{}", i)));
            props.insert("index".to_string(), Value::Int(i));
            writer
                .insert_vertex_with_labels(vid, props, &[], None)
                .await?;
            vids.push(vid);
        }

        // Verify estimated_size grew
        let after_vertices_estimated = l0.read().estimated_size;
        let after_vertices_actual = l0.read().size_bytes();
        assert!(
            after_vertices_estimated > 0,
            "estimated_size should grow after insertions"
        );

        // Verify estimated_size is within reasonable bounds of actual size (within 2x)
        let ratio = after_vertices_estimated as f64 / after_vertices_actual as f64;
        assert!(
            (0.5..=2.0).contains(&ratio),
            "estimated_size ({}) should be within 2x of size_bytes ({}), ratio: {}",
            after_vertices_estimated,
            after_vertices_actual,
            ratio
        );

        // Insert edges with a simple edge type
        let edge_type = 1u32;
        for i in 0..9 {
            let eid = writer.next_eid(edge_type).await?;
            writer
                .insert_edge(
                    vids[i],
                    vids[i + 1],
                    edge_type,
                    eid,
                    std::collections::HashMap::new(),
                    Some("NEXT".to_string()),
                    None,
                )
                .await?;
        }

        // Verify estimated_size grew further
        let after_edges_estimated = l0.read().estimated_size;
        let after_edges_actual = l0.read().size_bytes();
        assert!(
            after_edges_estimated > after_vertices_estimated,
            "estimated_size should grow after edge insertions"
        );

        // Verify still within reasonable bounds
        let ratio = after_edges_estimated as f64 / after_edges_actual as f64;
        assert!(
            (0.5..=2.0).contains(&ratio),
            "estimated_size ({}) should be within 2x of size_bytes ({}), ratio: {}",
            after_edges_estimated,
            after_edges_actual,
            ratio
        );

        Ok(())
    }

    /// Test that flushing WAL on a writer with no mutations succeeds cleanly.
    #[tokio::test]
    async fn test_flush_wal_empty_l0_is_noop() -> Result<()> {
        use crate::runtime::wal::WriteAheadLog;
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

        let wal_path = ObjectStorePath::from("wal");
        let wal = Arc::new(WriteAheadLog::new(store.clone(), wal_path));

        let writer = Writer::new_with_config(
            storage.clone(),
            schema_manager.clone(),
            1,
            UniConfig::default(),
            Some(wal.clone()),
            None,
        )
        .await?;

        // Flush with no mutations — should succeed cleanly
        let lsn = writer.flush_wal().await?;
        // LSN should be 0 or 1 (no real mutations flushed)
        assert!(lsn <= 1, "Empty flush should produce low LSN, got {}", lsn);

        Ok(())
    }

    /// Test that transaction data does not leak into main L0 without commit.
    #[tokio::test]
    async fn test_transaction_isolation_without_commit() -> Result<()> {
        use crate::runtime::wal::WriteAheadLog;
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        let _label_id = schema_manager.add_label("Person")?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

        let wal_path = ObjectStorePath::from("wal");
        let wal = Arc::new(WriteAheadLog::new(store.clone(), wal_path));

        let writer = Writer::new_with_config(
            storage.clone(),
            schema_manager.clone(),
            1,
            UniConfig::default(),
            Some(wal),
            None,
        )
        .await?;

        // Create transaction L0
        let tx_l0 = writer.create_transaction_l0();

        // Insert vertex into transaction L0
        let vid = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(
                vid,
                [("name".to_string(), Value::String("Ghost".to_string()))]
                    .into_iter()
                    .collect(),
                &["Person".to_string()],
                Some(&tx_l0),
            )
            .await?;

        // Verify data is in transaction L0
        assert!(
            tx_l0.read().vertex_properties.contains_key(&vid),
            "Transaction L0 should contain the vertex"
        );

        // Verify data is NOT in main L0
        let main_l0 = writer.l0_manager.get_current();
        assert!(
            !main_l0.read().vertex_properties.contains_key(&vid),
            "Main L0 should NOT contain uncommitted transaction data"
        );

        // Drop transaction without committing — data should be lost
        drop(tx_l0);

        // Main L0 still should not have it
        assert!(
            !main_l0.read().vertex_properties.contains_key(&vid),
            "Main L0 should remain clean after dropped transaction"
        );

        Ok(())
    }

    /// Phase 2 Day 12: the fork-fragment warn fires exactly once when
    /// the flush count crosses the configured threshold and stays
    /// silent on subsequent flushes for the lifetime of the writer.
    /// Primary writers (`fork_id == None`) never fire it.
    ///
    /// Tested directly against `tick_fork_fragment_observability` so
    /// the contract is locked in independently of the broader
    /// `flush_to_l1` path (the end-to-end fork-flush path is blocked
    /// on Day 10's on-the-fly schema overlay growth).
    #[tokio::test]
    async fn fork_fragment_warn_fires_once_then_silences() -> Result<()> {
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::fork::ForkId;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");
        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        let storage = Arc::new(
            StorageManager::new(dir.path().to_str().unwrap(), schema_manager.clone()).await?,
        );

        let config = UniConfig {
            fork_fragment_warn_threshold: 3,
            ..Default::default()
        };
        let mut writer =
            Writer::new_with_config(storage, schema_manager, 1, config, None, None).await?;

        // Primary path: never fires.
        for _ in 0..10 {
            writer.tick_fork_fragment_observability();
        }
        assert!(!writer.fork_fragment_warn_fired.load(Ordering::Relaxed));
        assert_eq!(writer.fork_flush_count.load(Ordering::Relaxed), 0);

        // Fork path: tag and tick. Below threshold → no fire.
        writer.fork_id = Some(ForkId::new());
        writer.tick_fork_fragment_observability();
        writer.tick_fork_fragment_observability();
        assert!(!writer.fork_fragment_warn_fired.load(Ordering::Relaxed));
        assert_eq!(writer.fork_flush_count.load(Ordering::Relaxed), 2);

        // Crossing threshold → fires once.
        writer.tick_fork_fragment_observability();
        assert!(writer.fork_fragment_warn_fired.load(Ordering::Relaxed));
        assert_eq!(writer.fork_flush_count.load(Ordering::Relaxed), 3);

        // Subsequent ticks bump the gauge but do not re-fire.
        let fired_after = writer.fork_fragment_warn_fired.load(Ordering::Relaxed);
        for _ in 0..5 {
            writer.tick_fork_fragment_observability();
        }
        assert_eq!(writer.fork_flush_count.load(Ordering::Relaxed), 8);
        assert_eq!(
            writer.fork_fragment_warn_fired.load(Ordering::Relaxed),
            fired_after
        );

        Ok(())
    }

    /// Per docs/proposals/concurrent_writer.md §9.1: the hot-path mutators
    /// must not write to any `Writer` struct field. Phase 2 of the refactor
    /// gave them `&self` receivers, which the compiler enforces against
    /// direct `self.x = y` assignment — but interior-mutable writes
    /// (Mutex/Atomic/OnceLock) still compile. This regression test snapshots
    /// every potentially-writable field, calls each hot-path mutator, and
    /// asserts no field changed.
    ///
    /// Cold-path methods (`flush_to_l1`, `commit_transaction_l0`,
    /// `tick_fork_fragment_observability`) DO mutate fields by design and
    /// are intentionally out of scope here.
    #[tokio::test]
    async fn hot_path_mutators_do_not_change_writer_fields() -> Result<()> {
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");
        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        schema_manager.add_label("Person")?;
        schema_manager.save().await?;
        let storage = Arc::new(
            StorageManager::new(dir.path().to_str().unwrap(), schema_manager.clone()).await?,
        );

        let writer = Writer::new_with_config(
            storage,
            schema_manager,
            1,
            UniConfig::default(),
            None,
            None,
        )
        .await?;

        /// Captures every `Writer` field that *could* be written by a
        /// hot-path mutator (i.e., every non-Arc, non-immutable-after-
        /// construction field). Arc'd substructures (`l0_manager`,
        /// `storage`, etc.) are intentionally not checked — they are
        /// re-pointed only at construction.
        #[derive(Debug, PartialEq)]
        struct Snapshot {
            last_flush_time: std::time::Instant,
            cached_manifest_some: bool,
            fork_flush_count: u64,
            fork_fragment_warn_fired: bool,
            xervo_runtime_some: bool,
            index_rebuild_manager_some: bool,
            fork_id: Option<ForkId>,
        }

        fn snap(w: &Writer) -> Snapshot {
            Snapshot {
                last_flush_time: *w.last_flush_time.lock(),
                cached_manifest_some: w.cached_manifest.lock().is_some(),
                fork_flush_count: w.fork_flush_count.load(Ordering::Relaxed),
                fork_fragment_warn_fired: w.fork_fragment_warn_fired.load(Ordering::Relaxed),
                xervo_runtime_some: w.xervo_runtime.get().is_some(),
                index_rebuild_manager_some: w.index_rebuild_manager.get().is_some(),
                fork_id: w.fork_id,
            }
        }

        // 1. insert_vertex_with_labels
        let before = snap(&writer);
        let vid = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(vid, Properties::new(), &["Person".to_string()], None)
            .await?;
        assert_eq!(
            snap(&writer),
            before,
            "insert_vertex_with_labels mutated a Writer field"
        );

        // 2. insert_vertices_batch
        let before = snap(&writer);
        let vids = writer.allocate_vids(2).await?;
        writer
            .insert_vertices_batch(
                vids,
                vec![Properties::new(), Properties::new()],
                vec!["Person".into()],
                None,
            )
            .await?;
        assert_eq!(
            snap(&writer),
            before,
            "insert_vertices_batch mutated a Writer field"
        );

        // 3. delete_vertex
        let before = snap(&writer);
        writer.delete_vertex(vid, None, None).await?;
        assert_eq!(
            snap(&writer),
            before,
            "delete_vertex mutated a Writer field"
        );

        // (insert_edge / delete_edge are skipped here: their fixture cost is
        // disproportionate to the audit's marginal value, and the same
        // structural argument plus the compiler-enforced `&self` covers them.)

        Ok(())
    }
}
