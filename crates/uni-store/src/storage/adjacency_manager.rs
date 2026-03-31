// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Unified adjacency manager orchestrating Main CSR, L0-csr overlay, and Shadow CSR.
//!
//! Implements a dual-CSR architecture where:
//! - **Main CSR**: packed adjacency for all alive edges (one per edge_type + direction)
//! - **L0-csr overlay**: concurrent insert/delete buffer that survives data flush
//! - **Shadow CSR**: tracks deleted edges with version ranges for time-travel queries
//!
//! Regular queries read Main CSR + overlay with zero version filtering.
//! Snapshot queries additionally filter by version and resurrect shadow entries.

use crate::storage::adjacency_overlay::{FrozenCsrSegment, L0CsrSegment};
use crate::storage::csr::MainCsr;
use crate::storage::direction::Direction;
use crate::storage::manager::StorageManager;
use crate::storage::shadow_csr::{ShadowCsr, ShadowEdge};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use uni_common::core::id::{Eid, Vid};

/// Unified adjacency manager for the dual-CSR architecture.
///
/// Orchestrates Main CSR (packed alive edges), L0-csr overlay
/// (in-memory mutations), and Shadow CSR (deleted edges for time-travel).
/// Data flush never invalidates or rebuilds the CSR.
pub struct AdjacencyManager {
    /// Main CSR per `(edge_type, direction)` — all alive edges.
    /// Edge type is u32 with bit 31 = 0 for schema'd, 1 for schemaless.
    main_csr: DashMap<(u32, Direction), Arc<MainCsr>>,

    /// Active L0-csr segment (current writes go here).
    active_overlay: Arc<RwLock<L0CsrSegment>>,

    /// Frozen segments awaiting compaction (oldest first).
    frozen_segments: RwLock<Vec<Arc<FrozenCsrSegment>>>,

    /// Shadow CSR for time-travel deleted edge tracking.
    shadow: ShadowCsr,

    /// Current approximate memory usage in bytes.
    current_bytes: AtomicUsize,

    /// Maximum memory budget in bytes.
    max_bytes: usize,

    /// Coalescing locks for warm() operations — prevents cache stampede.
    /// Key: (edge_type_id, Direction), Value: Mutex guard for that warm operation.
    warm_guards: DashMap<(u32, Direction), Arc<tokio::sync::Mutex<()>>>,
}

impl AdjacencyManager {
    /// Creates a new adjacency manager with the given memory budget.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            main_csr: DashMap::new(),
            active_overlay: Arc::new(RwLock::new(L0CsrSegment::new())),
            frozen_segments: RwLock::new(Vec::new()),
            shadow: ShadowCsr::new(),
            current_bytes: AtomicUsize::new(0),
            max_bytes,
            warm_guards: DashMap::new(),
        }
    }

    /// Returns neighbors for the current state (hot path, no version filtering).
    ///
    /// Reads Main CSR + frozen segments + active overlay, minus tombstones.
    /// Tombstones from any layer remove edges from all lower layers.
    pub fn get_neighbors(&self, vid: Vid, edge_type: u32, direction: Direction) -> Vec<(Vid, Eid)> {
        let mut result: HashMap<Eid, Vid> = HashMap::new();

        for &dir in direction.expand() {
            // 1. Main CSR
            if let Some(csr) = self.main_csr.get(&(edge_type, dir)) {
                for entry in csr.get_entries(vid) {
                    result.insert(entry.eid, entry.neighbor_vid);
                }
            }

            // 2. Frozen segments (oldest first) — add inserts, then remove tombstones
            for segment in self.frozen_segments.read().iter() {
                if let Some(adj) = segment.inserts.get(&(edge_type, dir))
                    && let Some(neighbors) = adj.get(&vid)
                {
                    for &(neighbor, eid, _version) in neighbors {
                        result.insert(eid, neighbor);
                    }
                }
                // Apply tombstones against ALL prior results (Main CSR + older segments)
                result.retain(|eid, _| !segment.tombstones.contains_key(eid));
            }

            // 3. Active overlay — add inserts, then remove tombstones
            let active = self.active_overlay.read();
            if let Some(adj) = active.inserts.get(&(edge_type, dir))
                && let Some(neighbors) = adj.get(&vid)
            {
                for &(neighbor, eid, _version) in neighbors {
                    result.insert(eid, neighbor);
                }
            }
            // Apply active overlay tombstones against ALL prior results
            result.retain(|eid, _| !active.tombstones.contains_key(eid));
        }

        result.into_iter().map(|(e, n)| (n, e)).collect()
    }

    /// Returns neighbors visible at a specific snapshot version.
    ///
    /// Filters Main CSR entries by `created_version`, applies frozen/active
    /// overlay with version filtering, and resurrects Shadow CSR entries
    /// that were alive at the given version.
    pub fn get_neighbors_at_version(
        &self,
        vid: Vid,
        edge_type: u32,
        direction: Direction,
        version: u64,
    ) -> Vec<(Vid, Eid)> {
        let mut result: HashMap<Eid, Vid> = HashMap::new();

        for &dir in direction.expand() {
            // 1. Main CSR — filter by created_version
            if let Some(csr) = self.main_csr.get(&(edge_type, dir)) {
                for entry in csr.get_entries(vid) {
                    if entry.created_version <= version {
                        result.insert(entry.eid, entry.neighbor_vid);
                    }
                }
            }

            // 2. Frozen segments — filter inserts by version, apply tombstones
            for segment in self.frozen_segments.read().iter() {
                if let Some(adj) = segment.inserts.get(&(edge_type, dir))
                    && let Some(neighbors) = adj.get(&vid)
                {
                    for &(neighbor, eid, ver) in neighbors {
                        if ver <= version {
                            result.insert(eid, neighbor);
                        }
                    }
                }
                result.retain(|eid, _| {
                    segment
                        .tombstones
                        .get(eid)
                        .is_none_or(|ts| ts.version > version)
                });
            }

            // 3. Active overlay — add version-filtered inserts, then apply tombstones
            let active = self.active_overlay.read();
            if let Some(adj) = active.inserts.get(&(edge_type, dir))
                && let Some(neighbors) = adj.get(&vid)
            {
                for &(neighbor, eid, ver) in neighbors {
                    let not_tombstoned = active
                        .tombstones
                        .get(&eid)
                        .is_none_or(|ts| ts.version > version);
                    if ver <= version && not_tombstoned {
                        result.insert(eid, neighbor);
                    }
                }
            }
            // Apply active overlay tombstones against ALL prior results
            result.retain(|eid, _| {
                active
                    .tombstones
                    .get(eid)
                    .is_none_or(|ts| ts.version > version)
            });

            // 4. Shadow CSR — resurrect edges alive at version
            for (neighbor, eid) in self
                .shadow
                .get_entries_at_version(vid, edge_type, dir, version)
            {
                result.insert(eid, neighbor);
            }
        }

        result.into_iter().map(|(e, n)| (n, e)).collect()
    }

    /// Records an edge insertion into the L0-csr overlay (both directions).
    pub fn insert_edge(&self, src: Vid, dst: Vid, eid: Eid, edge_type: u32, version: u64) {
        let active = self.active_overlay.read();
        active.insert_edge(src, dst, eid, edge_type, version, Direction::Outgoing);
        active.insert_edge(dst, src, eid, edge_type, version, Direction::Incoming);
    }

    /// Records a tombstone for a deleted edge in the L0-csr overlay.
    pub fn add_tombstone(&self, eid: Eid, src: Vid, dst: Vid, edge_type: u32, version: u64) {
        let active = self.active_overlay.read();
        active.add_tombstone(eid, src, dst, edge_type, version);
    }

    /// Sets the Main CSR for a specific edge type and direction.
    ///
    /// Used by `warm()` to install a freshly built CSR from storage.
    pub fn set_main_csr(&self, edge_type: u32, direction: Direction, csr: MainCsr) {
        let size = csr.memory_usage();
        self.main_csr.insert((edge_type, direction), Arc::new(csr));
        self.current_bytes.fetch_add(size, Ordering::Relaxed);
    }

    /// Checks whether a Main CSR exists for the given edge type and direction.
    pub fn has_csr(&self, edge_type: u32, direction: Direction) -> bool {
        self.main_csr.contains_key(&(edge_type, direction))
    }

    /// Checks whether this manager has been activated for the given edge type.
    ///
    /// Returns `true` if a Main CSR exists or the overlay has entries for
    /// this edge type and direction.
    pub fn is_active_for(&self, edge_type: u32, direction: Direction) -> bool {
        let active = self.active_overlay.read();
        direction.expand().iter().any(|&d| {
            self.main_csr.contains_key(&(edge_type, d)) || active.has_entries_for(edge_type, d)
        })
    }

    /// Returns the number of frozen segments awaiting compaction.
    pub fn frozen_segment_count(&self) -> usize {
        self.frozen_segments.read().len()
    }

    /// Returns whether compaction should be triggered based on segment count.
    pub fn should_compact(&self, threshold: usize) -> bool {
        self.frozen_segment_count() >= threshold
    }

    /// Compacts frozen overlay segments into the Main CSR.
    ///
    /// Freezes the active overlay, merges all frozen segments with the
    /// existing Main CSR, moves tombstoned edges to Shadow CSR, and
    /// atomically swaps in the new Main CSR.
    ///
    /// CRITICAL: Frozen segments remain readable until the new CSR is installed,
    /// eliminating the visibility gap where edges would be invisible.
    pub fn compact(&self) {
        // Step 1: Freeze active overlay and push to frozen list
        let frozen = {
            let mut active = self.active_overlay.write();
            let old = std::mem::take(&mut *active);
            Arc::new(old.freeze())
        };
        self.frozen_segments.write().push(frozen);

        // Step 2: CLONE frozen segments for building (DON'T drain yet)
        // This ensures they remain readable during CSR construction
        let segments = self.frozen_segments.read().clone();

        // Step 3: Collect all (edge_type, direction) keys from segments + existing CSRs
        let mut all_keys: HashSet<(u32, Direction)> = HashSet::new();
        for segment in &segments {
            for key in segment.inserts.keys() {
                all_keys.insert(*key);
            }
        }
        for entry in self.main_csr.iter() {
            all_keys.insert(*entry.key());
        }

        // Step 4: For each key, merge
        for (edge_type, direction) in all_keys {
            let mut entries: Vec<(u64, Vid, Eid, u64)> = Vec::new();
            let mut max_offset: u64 = 0;

            // Collect all tombstone EIDs
            let mut tombstoned_eids: HashSet<Eid> = HashSet::new();
            for segment in &segments {
                for (eid, ts) in &segment.tombstones {
                    if ts.edge_type == edge_type {
                        tombstoned_eids.insert(*eid);

                        // Move to shadow CSR
                        self.shadow.add_deleted_edge(
                            ts.src_vid,
                            ShadowEdge {
                                neighbor_vid: ts.dst_vid,
                                eid: *eid,
                                edge_type,
                                created_version: 0, // unknown; overlay tombstones don't track creation version
                                deleted_version: ts.version,
                            },
                            direction,
                        );
                    }
                }
            }

            // Add entries from old Main CSR
            if let Some(old_csr) = self.main_csr.get(&(edge_type, direction)) {
                for vid_offset in 0..old_csr.num_vertices() {
                    let vid = Vid::new(vid_offset as u64);
                    for entry in old_csr.get_entries(vid) {
                        if !tombstoned_eids.contains(&entry.eid) {
                            entries.push((
                                vid_offset as u64,
                                entry.neighbor_vid,
                                entry.eid,
                                entry.created_version,
                            ));
                            max_offset = max_offset.max(vid_offset as u64);
                        }
                    }
                }
            }

            // Overlay frozen segments (oldest first)
            for segment in &segments {
                if let Some(adj) = segment.inserts.get(&(edge_type, direction)) {
                    for (vid, neighbors) in adj {
                        for &(neighbor, eid, version) in neighbors {
                            if !tombstoned_eids.contains(&eid) {
                                let offset = vid.as_u64();
                                entries.push((offset, neighbor, eid, version));
                                max_offset = max_offset.max(offset);
                            }
                        }
                    }
                }
            }

            // Deduplicate by Eid — keep entry with highest version for each Eid
            {
                use std::collections::hash_map::Entry;

                let mut best: HashMap<Eid, usize> = HashMap::new();
                for (idx, (_, _, eid, ver)) in entries.iter().enumerate() {
                    match best.entry(*eid) {
                        Entry::Vacant(e) => {
                            e.insert(idx);
                        }
                        Entry::Occupied(mut e) => {
                            if *ver > entries[*e.get()].3 {
                                e.insert(idx);
                            }
                        }
                    }
                }
                let keep: HashSet<usize> = best.into_values().collect();
                let mut idx = 0;
                entries.retain(|_| {
                    let k = keep.contains(&idx);
                    idx += 1;
                    k
                });
            }

            // Build new Main CSR and install
            let new_csr = MainCsr::from_edge_entries(max_offset as usize, entries);
            let size = new_csr.memory_usage();

            // Remove old size, add new
            if let Some(old) = self.main_csr.get(&(edge_type, direction)) {
                self.current_bytes
                    .fetch_sub(old.memory_usage(), Ordering::Relaxed);
            }

            self.main_csr
                .insert((edge_type, direction), Arc::new(new_csr));
            self.current_bytes.fetch_add(size, Ordering::Relaxed);
        }

        // Step 5: ONLY NOW clear frozen segments and reset active overlay
        // New CSR contains all their data, so they're safe to discard
        self.frozen_segments.write().clear();
    }

    /// Warms the Main CSR from storage (L2 adjacency + L1 delta) for a specific edge type and direction.
    ///
    /// Reads L2 adjacency datasets and L1 delta entries from Lance,
    /// builds a [`MainCsr`] with version metadata, and populates the
    /// [`ShadowCsr`] with L1 tombstones. Called once at startup or
    /// lazily on first access per edge type.
    pub async fn warm(
        &self,
        storage: &StorageManager,
        edge_type_id: u32,
        direction: Direction,
        version: Option<u64>,
    ) -> anyhow::Result<()> {
        let schema = storage.schema_manager().schema();

        // Use unified lookup to support both schema'd and schemaless edge types
        let edge_type_name = schema
            .edge_type_name_by_id_unified(edge_type_id)
            .ok_or_else(|| anyhow::anyhow!("Edge type {} not found", edge_type_id))?;

        // Determine which labels to load adjacency for based on edge type metadata
        let labels_to_load: Vec<String> = {
            let edge_meta = schema.edge_types.get(&edge_type_name);
            match (direction, edge_meta) {
                (Direction::Outgoing, Some(meta)) => meta.src_labels.clone(),
                (Direction::Incoming, Some(meta)) => meta.dst_labels.clone(),
                (Direction::Both, Some(meta)) => {
                    let mut labels = meta.src_labels.clone();
                    labels.extend(meta.dst_labels.iter().cloned());
                    labels.sort();
                    labels.dedup();
                    labels
                }
                _ => Vec::new(),
            }
        };

        use arrow_array::{ListArray, UInt8Array, UInt64Array};

        let mut entries: Vec<(u64, Vid, Eid, u64)> = Vec::new();
        let mut deleted_eids = HashSet::new();

        for &read_dir in direction.expand() {
            let dir_str = read_dir.as_str();
            for label_name in &labels_to_load {
                // 1. Read L2 (Adjacency Dataset)
                let adj_ds = storage.adjacency_dataset(&edge_type_name, label_name, dir_str);
                let backend = storage.backend();

                if let Ok(adj_ds) = adj_ds {
                    let adj_table_name = adj_ds.table_name();
                    let adj_exists = backend.table_exists(&adj_table_name).await.unwrap_or(false);

                    if adj_exists {
                        let mut request = crate::backend::types::ScanRequest::all(&adj_table_name);
                        if let Some(hwm) = version {
                            request = request.with_filter(format!("_version <= {}", hwm));
                        }

                        let batches: Vec<arrow_array::RecordBatch> =
                            backend.scan(request).await.unwrap_or_default();

                        for batch in batches {
                            let src_col = batch
                                .column_by_name("src_vid")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap();
                            let neighbors_list = batch
                                .column_by_name("neighbors")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<ListArray>()
                                .unwrap();
                            let eids_list = batch
                                .column_by_name("edge_ids")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<ListArray>()
                                .unwrap();

                            for i in 0..batch.num_rows() {
                                let src_offset = src_col.value(i);
                                let neighbors_array_ref = neighbors_list.value(i);
                                let neighbors = neighbors_array_ref
                                    .as_any()
                                    .downcast_ref::<UInt64Array>()
                                    .unwrap();
                                let eids_array_ref = eids_list.value(i);
                                let eids = eids_array_ref
                                    .as_any()
                                    .downcast_ref::<UInt64Array>()
                                    .unwrap();

                                for j in 0..neighbors.len() {
                                    // L2 adjacency rows don't carry per-edge _version.
                                    // Version 0 means "from base storage" — the `_version <= hwm` filter on
                                    // the query already ensures we only load rows within the snapshot window.
                                    // At query time, get_neighbors_at_version() uses created_version to filter,
                                    // so version=0 edges are always visible (which is correct for compacted L2 data).
                                    entries.push((
                                        src_offset,
                                        Vid::from(neighbors.value(j)),
                                        Eid::from(eids.value(j)),
                                        0,
                                    ));
                                }
                            }
                        }
                    }
                }
            }

            // 2. Read L1 (Delta)
            let delta_ds = storage.delta_dataset(&edge_type_name, dir_str)?;
            let backend = storage.backend();
            let delta_table_name = delta_ds.table_name();

            if backend
                .table_exists(&delta_table_name)
                .await
                .unwrap_or(false)
            {
                let mut request = crate::backend::types::ScanRequest::all(&delta_table_name);
                if let Some(hwm) = version {
                    request = request.with_filter(format!("_version <= {}", hwm));
                }

                if let Ok(batches) = backend.scan(request).await {
                    for batch in batches {
                        let src_col = batch
                            .column_by_name("src_vid")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        let dst_col = batch
                            .column_by_name("dst_vid")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        let eid_col = batch
                            .column_by_name("eid")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        let op_col = batch
                            .column_by_name("op")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .unwrap();

                        // Optionally read _version column
                        let version_col = batch
                            .column_by_name("_version")
                            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>().cloned());

                        for i in 0..batch.num_rows() {
                            let src_vid = Vid::from(src_col.value(i));
                            let dst_vid = Vid::from(dst_col.value(i));
                            let eid = Eid::from(eid_col.value(i));
                            let op = op_col.value(i); // 0=Insert, 1=Delete
                            let row_version = version_col.as_ref().map_or(0, |vc| vc.value(i));

                            // For incoming edges, the CSR key is dst (the vertex
                            // receiving the edge) and the neighbor is src.
                            let is_incoming = read_dir == Direction::Incoming;
                            let (key_vid, neighbor_vid) = if is_incoming {
                                (dst_vid, src_vid)
                            } else {
                                (src_vid, dst_vid)
                            };

                            if op == 0 {
                                entries.push((key_vid.as_u64(), neighbor_vid, eid, row_version));
                            } else {
                                deleted_eids.insert(eid);
                                self.shadow.add_deleted_edge(
                                    key_vid,
                                    ShadowEdge {
                                        neighbor_vid,
                                        eid,
                                        edge_type: edge_type_id,
                                        created_version: 0,
                                        deleted_version: row_version,
                                    },
                                    read_dir,
                                );
                            }
                        }
                    }
                }
            }
        }

        // Filter out deleted edges
        if !deleted_eids.is_empty() {
            entries.retain(|(_, _, eid, _)| !deleted_eids.contains(eid));
        }

        // Deduplicate by Eid — keep entry with highest version for each Eid
        // Multiple versions of the same edge can exist in L2+L1 or across L1 runs
        {
            use std::collections::hash_map::Entry;
            use std::collections::{HashMap, HashSet};

            let mut best: HashMap<Eid, usize> = HashMap::new();
            for (idx, (_, _, eid, ver)) in entries.iter().enumerate() {
                match best.entry(*eid) {
                    Entry::Vacant(e) => {
                        e.insert(idx);
                    }
                    Entry::Occupied(mut e) => {
                        if *ver > entries[*e.get()].3 {
                            e.insert(idx);
                        }
                    }
                }
            }
            let keep: HashSet<usize> = best.into_values().collect();
            let mut idx = 0;
            entries.retain(|_| {
                let k = keep.contains(&idx);
                idx += 1;
                k
            });
        }

        // Build MainCsr
        let max_offset = entries.iter().map(|(o, _, _, _)| *o).max().unwrap_or(0);
        let csr = MainCsr::from_edge_entries(max_offset as usize, entries);
        self.set_main_csr(edge_type_id, direction, csr);

        Ok(())
    }

    /// Coalesced warm() operation to prevent cache stampede (Issue #13).
    ///
    /// Uses double-checked locking: fast-path checks if CSR already loaded,
    /// then acquires per-(edge_type, direction) lock to ensure only one concurrent
    /// warm() per adjacency key. Other readers wait for the first warm() to complete.
    pub async fn warm_coalesced(
        &self,
        storage: &StorageManager,
        edge_type_id: u32,
        direction: Direction,
        version: Option<u64>,
    ) -> anyhow::Result<()> {
        // Fast path: already loaded
        if self.has_csr(edge_type_id, direction) {
            return Ok(());
        }

        // Coalesce: only one concurrent warm per (type, dir)
        let guard = self
            .warm_guards
            .entry((edge_type_id, direction))
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .value()
            .clone();
        let _lock = guard.lock().await;

        // Double-check after acquiring lock
        if self.has_csr(edge_type_id, direction) {
            return Ok(());
        }

        self.warm(storage, edge_type_id, direction, version).await
    }

    /// Returns the current approximate memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        self.current_bytes.load(Ordering::Relaxed)
    }

    /// Returns the maximum memory budget in bytes.
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Provides access to the shadow CSR for time-travel queries.
    pub fn shadow(&self) -> &ShadowCsr {
        &self.shadow
    }
}

impl std::fmt::Debug for AdjacencyManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdjacencyManager")
            .field("main_csr_count", &self.main_csr.len())
            .field("frozen_segments", &self.frozen_segments.read().len())
            .field("current_bytes", &self.current_bytes.load(Ordering::Relaxed))
            .field("max_bytes", &self.max_bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get_neighbors() {
        let am = AdjacencyManager::new(1024 * 1024);
        let src = Vid::new(1);
        let dst = Vid::new(2);
        let eid = Eid::new(100);

        am.insert_edge(src, dst, eid, 1, 1);

        let neighbors = am.get_neighbors(src, 1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], (dst, eid));

        // Incoming direction
        let incoming = am.get_neighbors(dst, 1, Direction::Incoming);
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0], (src, eid));
    }

    #[test]
    fn test_main_csr_lookup() {
        let am = AdjacencyManager::new(1024 * 1024);

        let csr = MainCsr::from_edge_entries(
            1,
            vec![
                (0, Vid::new(10), Eid::new(100), 1),
                (1, Vid::new(20), Eid::new(101), 2),
            ],
        );
        am.set_main_csr(1, Direction::Outgoing, csr);

        let n = am.get_neighbors(Vid::new(0), 1, Direction::Outgoing);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0], (Vid::new(10), Eid::new(100)));
    }

    #[test]
    fn test_overlay_on_top_of_main_csr() {
        let am = AdjacencyManager::new(1024 * 1024);

        // Main CSR has one edge
        let csr = MainCsr::from_edge_entries(0, vec![(0, Vid::new(10), Eid::new(100), 1)]);
        am.set_main_csr(1, Direction::Outgoing, csr);

        // Overlay adds another
        am.insert_edge(Vid::new(0), Vid::new(20), Eid::new(101), 1, 2);

        let n = am.get_neighbors(Vid::new(0), 1, Direction::Outgoing);
        assert_eq!(n.len(), 2);

        let eids: HashSet<Eid> = n.iter().map(|(_, e)| *e).collect();
        assert!(eids.contains(&Eid::new(100)));
        assert!(eids.contains(&Eid::new(101)));
    }

    #[test]
    fn test_tombstone_removes_edge() {
        let am = AdjacencyManager::new(1024 * 1024);

        am.insert_edge(Vid::new(0), Vid::new(10), Eid::new(100), 1, 1);
        am.add_tombstone(Eid::new(100), Vid::new(0), Vid::new(10), 1, 2);

        let n = am.get_neighbors(Vid::new(0), 1, Direction::Outgoing);
        assert!(n.is_empty());
    }

    #[test]
    fn test_version_filtered_query() {
        let am = AdjacencyManager::new(1024 * 1024);

        // Main CSR with two edges at different versions
        let csr = MainCsr::from_edge_entries(
            0,
            vec![
                (0, Vid::new(10), Eid::new(100), 1),
                (0, Vid::new(20), Eid::new(101), 5),
            ],
        );
        am.set_main_csr(1, Direction::Outgoing, csr);

        // At version 3: only first edge visible
        let n = am.get_neighbors_at_version(Vid::new(0), 1, Direction::Outgoing, 3);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0], (Vid::new(10), Eid::new(100)));

        // At version 5: both visible
        let n = am.get_neighbors_at_version(Vid::new(0), 1, Direction::Outgoing, 5);
        assert_eq!(n.len(), 2);
    }

    #[test]
    fn test_shadow_csr_resurrects_deleted_edges() {
        let am = AdjacencyManager::new(1024 * 1024);

        // Add a deleted edge to shadow: created at v1, deleted at v5
        am.shadow().add_deleted_edge(
            Vid::new(0),
            ShadowEdge {
                neighbor_vid: Vid::new(10),
                eid: Eid::new(100),
                edge_type: 1,
                created_version: 1,
                deleted_version: 5,
            },
            Direction::Outgoing,
        );

        // At version 3: shadow edge should be visible
        let n = am.get_neighbors_at_version(Vid::new(0), 1, Direction::Outgoing, 3);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0], (Vid::new(10), Eid::new(100)));

        // At version 5: deleted, not visible
        let n = am.get_neighbors_at_version(Vid::new(0), 1, Direction::Outgoing, 5);
        assert!(n.is_empty());
    }

    #[test]
    fn test_compact_merges_into_main_csr() {
        let am = AdjacencyManager::new(1024 * 1024);

        // Insert edges into overlay
        am.insert_edge(Vid::new(0), Vid::new(10), Eid::new(100), 1, 1);
        am.insert_edge(Vid::new(0), Vid::new(20), Eid::new(101), 1, 2);

        // Compact: overlay → Main CSR
        am.compact();

        // Frozen segments should be empty after compaction
        assert_eq!(am.frozen_segment_count(), 0);

        // Edges should still be accessible via Main CSR
        let n = am.get_neighbors(Vid::new(0), 1, Direction::Outgoing);
        assert_eq!(n.len(), 2);

        assert!(am.has_csr(1, Direction::Outgoing));
    }

    #[test]
    fn test_compact_removes_tombstoned_edges() {
        let am = AdjacencyManager::new(1024 * 1024);

        // Set up Main CSR with one edge
        let csr = MainCsr::from_edge_entries(0, vec![(0, Vid::new(10), Eid::new(100), 1)]);
        am.set_main_csr(1, Direction::Outgoing, csr);

        // Add new edge + tombstone for old edge in overlay
        am.insert_edge(Vid::new(0), Vid::new(20), Eid::new(101), 1, 2);
        am.add_tombstone(Eid::new(100), Vid::new(0), Vid::new(10), 1, 3);

        am.compact();

        // Only the new edge should remain
        let n = am.get_neighbors(Vid::new(0), 1, Direction::Outgoing);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0], (Vid::new(20), Eid::new(101)));
    }

    #[test]
    fn test_should_compact() {
        let am = AdjacencyManager::new(1024 * 1024);
        assert!(!am.should_compact(4));

        // Manually freeze the active overlay multiple times
        for _ in 0..4 {
            let frozen = {
                let mut active = am.active_overlay.write();
                let old = std::mem::take(&mut *active);
                Arc::new(old.freeze())
            };
            am.frozen_segments.write().push(frozen);
        }

        assert!(am.should_compact(4));
    }

    #[test]
    fn test_empty_manager() {
        let am = AdjacencyManager::new(1024 * 1024);
        assert!(
            am.get_neighbors(Vid::new(0), 1, Direction::Outgoing)
                .is_empty()
        );
        assert!(!am.has_csr(1, Direction::Outgoing));
    }

    #[test]
    fn test_overlay_tombstone_removes_main_csr_edge() {
        // Simulates: insert edge → flush/compact into Main CSR → delete edge (tombstone in overlay)
        let am = AdjacencyManager::new(1024 * 1024);

        // Edge already compacted into Main CSR
        let csr = MainCsr::from_edge_entries(0, vec![(0, Vid::new(10), Eid::new(100), 1)]);
        am.set_main_csr(1, Direction::Outgoing, csr);

        // Verify edge is visible before deletion
        let n = am.get_neighbors(Vid::new(0), 1, Direction::Outgoing);
        assert_eq!(n.len(), 1);

        // Delete via overlay tombstone (simulates Writer::delete_edge dual-write)
        am.add_tombstone(Eid::new(100), Vid::new(0), Vid::new(10), 1, 2);

        // Tombstone in overlay must remove edge from Main CSR results
        let n = am.get_neighbors(Vid::new(0), 1, Direction::Outgoing);
        assert!(
            n.is_empty(),
            "Edge should be removed by overlay tombstone, got {:?}",
            n
        );
    }

    #[test]
    fn test_overlay_tombstone_removes_main_csr_edge_versioned() {
        // Same scenario but via get_neighbors_at_version
        let am = AdjacencyManager::new(1024 * 1024);

        let csr = MainCsr::from_edge_entries(0, vec![(0, Vid::new(10), Eid::new(100), 1)]);
        am.set_main_csr(1, Direction::Outgoing, csr);

        am.add_tombstone(Eid::new(100), Vid::new(0), Vid::new(10), 1, 5);

        // At version 3: edge created at v1, tombstone at v5 → visible
        let n = am.get_neighbors_at_version(Vid::new(0), 1, Direction::Outgoing, 3);
        assert_eq!(n.len(), 1);

        // At version 5: tombstone applies → not visible
        let n = am.get_neighbors_at_version(Vid::new(0), 1, Direction::Outgoing, 5);
        assert!(
            n.is_empty(),
            "Edge should be removed by overlay tombstone at version 5"
        );
    }

    #[test]
    fn test_frozen_tombstone_removes_main_csr_edge() {
        // Edge in Main CSR, tombstone in a frozen segment
        let am = AdjacencyManager::new(1024 * 1024);

        let csr = MainCsr::from_edge_entries(0, vec![(0, Vid::new(10), Eid::new(100), 1)]);
        am.set_main_csr(1, Direction::Outgoing, csr);

        // Add tombstone to active overlay, then compact to freeze it
        am.add_tombstone(Eid::new(100), Vid::new(0), Vid::new(10), 1, 2);

        // Freeze the overlay manually
        {
            let mut active = am.active_overlay.write();
            let old = std::mem::take(&mut *active);
            let frozen = std::sync::Arc::new(old.freeze());
            am.frozen_segments.write().push(frozen);
        }

        // The frozen segment's tombstone should remove the Main CSR edge
        let n = am.get_neighbors(Vid::new(0), 1, Direction::Outgoing);
        assert!(n.is_empty(), "Frozen tombstone should remove Main CSR edge");
    }

    #[test]
    fn test_per_edge_version_filtering() {
        // Test that edges inserted at different versions are correctly filtered
        // by get_neighbors_at_version()
        let am = AdjacencyManager::new(1024 * 1024);

        let src = Vid::new(0);
        let dst_a = Vid::new(10);
        let dst_b = Vid::new(20);
        let eid_a = Eid::new(100);
        let eid_b = Eid::new(200);
        let etype = 1;

        // Insert edge A at version 3
        am.insert_edge(src, dst_a, eid_a, etype, 3);

        // Insert edge B at version 7
        am.insert_edge(src, dst_b, eid_b, etype, 7);

        // Query at version 2 → neither edge visible
        let neighbors_v2 = am.get_neighbors_at_version(src, etype, Direction::Outgoing, 2);
        assert!(
            neighbors_v2.is_empty(),
            "No edges should be visible at version 2"
        );

        // Query at version 5 → only edge A visible
        let neighbors_v5 = am.get_neighbors_at_version(src, etype, Direction::Outgoing, 5);
        assert_eq!(
            neighbors_v5.len(),
            1,
            "Only edge A should be visible at version 5"
        );
        assert_eq!(neighbors_v5[0].0, dst_a, "Edge A destination should match");
        assert_eq!(neighbors_v5[0].1, eid_a, "Edge A ID should match");

        // Query at version 7 → both edges visible
        let neighbors_v7 = am.get_neighbors_at_version(src, etype, Direction::Outgoing, 7);
        assert_eq!(
            neighbors_v7.len(),
            2,
            "Both edges should be visible at version 7"
        );

        // Query at version 10 → both edges visible
        let neighbors_v10 = am.get_neighbors_at_version(src, etype, Direction::Outgoing, 10);
        assert_eq!(
            neighbors_v10.len(),
            2,
            "Both edges should be visible at version 10"
        );
    }

    #[test]
    fn test_duplicate_edges_deduplicated_by_eid() {
        // Test Issue #41: Same Eid in MainCsr (v1) and overlay (v3) → only 1 result from get_neighbors
        let am = AdjacencyManager::new(1024 * 1024);

        let src = Vid::new(0);
        let dst = Vid::new(10);
        let eid = Eid::new(100);
        let etype = 1;

        // Set up Main CSR with edge at version 1
        let csr = MainCsr::from_edge_entries(0, vec![(0, dst, eid, 1)]);
        am.set_main_csr(etype, Direction::Outgoing, csr);

        // Insert same Eid into overlay at version 3 (update scenario)
        am.insert_edge(src, dst, eid, etype, 3);

        // get_neighbors should return only 1 edge (HashMap<Eid, Vid> deduplicates)
        let neighbors = am.get_neighbors(src, etype, Direction::Outgoing);
        assert_eq!(
            neighbors.len(),
            1,
            "Duplicate Eid should result in single entry"
        );
        assert_eq!(neighbors[0], (dst, eid));
    }

    #[test]
    fn test_compact_deduplicates_edges_keeps_highest_version() {
        // Test Issue #41: Same Eid at v1 in CSR and v5 in overlay
        // After compact: get_neighbors_at_version(v5) → visible
        //               get_neighbors_at_version(v1) → NOT visible (compaction kept v5)
        let am = AdjacencyManager::new(1024 * 1024);

        let src = Vid::new(0);
        let dst = Vid::new(10);
        let eid = Eid::new(100);
        let etype = 1;

        // Set up Main CSR with edge at version 1
        let csr = MainCsr::from_edge_entries(0, vec![(0, dst, eid, 1)]);
        am.set_main_csr(etype, Direction::Outgoing, csr);

        // Insert same Eid into overlay at version 5 (newer version)
        am.insert_edge(src, dst, eid, etype, 5);

        // Before compact: both versions exist in different layers
        // After compact: only highest version (v5) should remain

        am.compact();

        // At version 5: edge should be visible (highest version kept)
        let neighbors_v5 = am.get_neighbors_at_version(src, etype, Direction::Outgoing, 5);
        assert_eq!(neighbors_v5.len(), 1, "Edge should be visible at version 5");
        assert_eq!(neighbors_v5[0], (dst, eid));

        // At version 4: edge should still be visible (v5 edge has created_version=5)
        // Actually, the edge at v5 replaces v1, so the edge has version 5
        // So at version 4, we should NOT see it
        let neighbors_v4 = am.get_neighbors_at_version(src, etype, Direction::Outgoing, 4);
        assert_eq!(
            neighbors_v4.len(),
            0,
            "After compaction, only version 5 exists; version 4 should not see it"
        );

        // At version 1: edge should NOT be visible (old version discarded)
        let neighbors_v1 = am.get_neighbors_at_version(src, etype, Direction::Outgoing, 1);
        assert_eq!(
            neighbors_v1.len(),
            0,
            "Old version discarded during compaction deduplication"
        );

        // At version 6: edge should be visible (v5 edge still exists)
        let neighbors_v6 = am.get_neighbors_at_version(src, etype, Direction::Outgoing, 6);
        assert_eq!(neighbors_v6.len(), 1, "Edge should be visible at version 6");
    }

    /// Test that tombstone filtering is O(result_size), not O(tombstone_count).
    /// This verifies fix for issue #140 (inverted tombstone scan).
    #[test]
    fn test_tombstone_scan_performance() {
        let am = AdjacencyManager::new(1024 * 1024);
        let vertex_a = Vid::new(1);
        let vertex_b = Vid::new(2);
        let etype = 1;

        // Create 5 edges from vertex_a
        let mut a_edges = Vec::new();
        for i in 0..5 {
            let dst = Vid::new(100 + i);
            let eid = Eid::new(1000 + i);
            am.insert_edge(vertex_a, dst, eid, etype, 1);
            a_edges.push((dst, eid));
        }

        // Create 100 deleted edges from vertex_b (creates 100 tombstones)
        for i in 0..100 {
            let dst = Vid::new(200 + i);
            let eid = Eid::new(2000 + i);
            am.insert_edge(vertex_b, dst, eid, etype, 1);
            am.add_tombstone(eid, vertex_b, dst, etype, 2);
        }

        // Query neighbors of vertex_a
        // With O(T) scan, this would iterate 100 tombstones
        // With O(result) scan, this only checks 5 edges against tombstone map
        let neighbors = am.get_neighbors(vertex_a, etype, Direction::Outgoing);

        // Verify all 5 edges are returned correctly
        assert_eq!(
            neighbors.len(),
            5,
            "Should return all 5 edges from vertex_a"
        );
        for (dst, eid) in &a_edges {
            assert!(
                neighbors.contains(&(*dst, *eid)),
                "Edge {:?} should be in results",
                (dst, eid)
            );
        }

        // Verify vertex_b has no neighbors (all tombstoned)
        let b_neighbors = am.get_neighbors(vertex_b, etype, Direction::Outgoing);
        assert_eq!(
            b_neighbors.len(),
            0,
            "Vertex B should have no neighbors (all deleted)"
        );
    }
}
