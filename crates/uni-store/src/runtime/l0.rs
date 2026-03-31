// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::runtime::wal::{Mutation, WriteAheadLog};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{instrument, trace};
use uni_common::core::id::{Eid, Vid};
use uni_common::graph::simple_graph::{Direction, SimpleGraph};
use uni_common::{Properties, Value};
use uni_crdt::Crdt;

/// Returns the current timestamp in nanoseconds since Unix epoch.
fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Serialize a constraint key for O(1) uniqueness checks.
/// Format: label + separator + sorted (prop_name, value) pairs.
pub fn serialize_constraint_key(label: &str, key_values: &[(String, Value)]) -> Vec<u8> {
    let mut buf = label.as_bytes().to_vec();
    buf.push(0); // separator
    let mut sorted = key_values.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    for (k, v) in &sorted {
        buf.extend(k.as_bytes());
        buf.push(0);
        // Use serde_json serialization for deterministic value encoding
        buf.extend(serde_json::to_vec(v).unwrap_or_default());
        buf.push(0);
    }
    buf
}

/// Per-type mutation counters accumulated by the L0 buffer.
///
/// Used to provide detailed mutation statistics (e.g., `nodes_created`,
/// `relationships_deleted`) on `ExecuteResult`. Callers snapshot before/after
/// execution and call [`diff()`](MutationStats::diff) to get the delta.
#[derive(Debug, Clone, Default)]
pub struct MutationStats {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
    pub properties_removed: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
}

impl MutationStats {
    /// Compute the field-wise difference `self - before`.
    pub fn diff(&self, before: &Self) -> Self {
        Self {
            nodes_created: self.nodes_created.saturating_sub(before.nodes_created),
            nodes_deleted: self.nodes_deleted.saturating_sub(before.nodes_deleted),
            relationships_created: self
                .relationships_created
                .saturating_sub(before.relationships_created),
            relationships_deleted: self
                .relationships_deleted
                .saturating_sub(before.relationships_deleted),
            properties_set: self.properties_set.saturating_sub(before.properties_set),
            properties_removed: self
                .properties_removed
                .saturating_sub(before.properties_removed),
            labels_added: self.labels_added.saturating_sub(before.labels_added),
            labels_removed: self.labels_removed.saturating_sub(before.labels_removed),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TombstoneEntry {
    pub eid: Eid,
    pub src_vid: Vid,
    pub dst_vid: Vid,
    pub edge_type: u32,
}

pub struct L0Buffer {
    /// Graph topology using simple adjacency lists
    pub graph: SimpleGraph,
    /// Soft-deleted edges (tombstones for LSM-style merging)
    pub tombstones: HashMap<Eid, TombstoneEntry>,
    /// Soft-deleted vertices
    pub vertex_tombstones: HashSet<Vid>,
    /// Edge version tracking for MVCC
    pub edge_versions: HashMap<Eid, u64>,
    /// Vertex version tracking for MVCC
    pub vertex_versions: HashMap<Vid, u64>,
    /// Edge properties (stored separately from topology)
    pub edge_properties: HashMap<Eid, Properties>,
    /// Vertex properties (stored separately from topology)
    pub vertex_properties: HashMap<Vid, Properties>,
    /// Edge endpoint lookup: eid -> (src, dst, type)
    pub edge_endpoints: HashMap<Eid, (Vid, Vid, u32)>,
    /// Vertex labels (VID -> list of label names)
    /// New in storage design: vertices can have multiple labels
    pub vertex_labels: HashMap<Vid, Vec<String>>,
    /// Edge types (EID -> type name)
    pub edge_types: HashMap<Eid, String>,
    /// Current version counter
    pub current_version: u64,
    /// Mutation count for flush decisions
    pub mutation_count: usize,
    /// Per-type mutation counters for detailed statistics.
    pub mutation_stats: MutationStats,
    /// Write-ahead log for durability
    pub wal: Option<Arc<WriteAheadLog>>,
    /// WAL LSN at the time this L0 was rotated for flush.
    /// Used to ensure WAL truncation doesn't remove entries needed by pending flushes.
    pub wal_lsn_at_flush: u64,
    /// Vertex creation timestamps (nanoseconds since epoch)
    pub vertex_created_at: HashMap<Vid, i64>,
    /// Vertex update timestamps (nanoseconds since epoch)
    pub vertex_updated_at: HashMap<Vid, i64>,
    /// Edge creation timestamps (nanoseconds since epoch)
    pub edge_created_at: HashMap<Eid, i64>,
    /// Edge update timestamps (nanoseconds since epoch)
    pub edge_updated_at: HashMap<Eid, i64>,
    /// Estimated size in bytes for memory limit enforcement.
    /// Incremented O(1) on each mutation to avoid O(V+E) size_bytes() calls.
    pub estimated_size: usize,
    /// Per-constraint index for O(1) unique key checks.
    /// Key: constraint composite key (label + sorted property values serialized).
    /// Value: Vid that owns this key.
    pub constraint_index: HashMap<Vec<u8>, Vid>,
}

impl std::fmt::Debug for L0Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L0Buffer")
            .field("vertex_count", &self.graph.vertex_count())
            .field("edge_count", &self.graph.edge_count())
            .field("tombstones", &self.tombstones.len())
            .field("vertex_tombstones", &self.vertex_tombstones.len())
            .field("current_version", &self.current_version)
            .field("mutation_count", &self.mutation_count)
            .finish()
    }
}

impl Clone for L0Buffer {
    /// Clone the L0 buffer for fork/restore (ASSUME/ABDUCE).
    ///
    /// The cloned buffer does NOT share the WAL reference — forked L0s are
    /// ephemeral and should not write to the WAL.
    fn clone(&self) -> Self {
        Self {
            graph: self.graph.clone(),
            tombstones: self.tombstones.clone(),
            vertex_tombstones: self.vertex_tombstones.clone(),
            edge_versions: self.edge_versions.clone(),
            vertex_versions: self.vertex_versions.clone(),
            edge_properties: self.edge_properties.clone(),
            vertex_properties: self.vertex_properties.clone(),
            edge_endpoints: self.edge_endpoints.clone(),
            vertex_labels: self.vertex_labels.clone(),
            edge_types: self.edge_types.clone(),
            current_version: self.current_version,
            mutation_count: self.mutation_count,
            mutation_stats: self.mutation_stats.clone(),
            wal: None, // Forked L0s don't share the WAL
            wal_lsn_at_flush: self.wal_lsn_at_flush,
            vertex_created_at: self.vertex_created_at.clone(),
            vertex_updated_at: self.vertex_updated_at.clone(),
            edge_created_at: self.edge_created_at.clone(),
            edge_updated_at: self.edge_updated_at.clone(),
            estimated_size: self.estimated_size,
            constraint_index: self.constraint_index.clone(),
        }
    }
}

impl L0Buffer {
    /// Append labels to a vec, skipping duplicates.
    fn append_unique_labels(existing: &mut Vec<String>, labels: &[String]) {
        for label in labels {
            if !existing.contains(label) {
                existing.push(label.clone());
            }
        }
    }

    /// Merge CRDT properties into an existing property map.
    /// Attempts CRDT merge if both values are valid CRDTs, falls back to overwrite.
    fn merge_crdt_properties(entry: &mut Properties, properties: Properties) {
        for (k, v) in properties {
            // Attempt merge if CRDT — convert to serde_json::Value for CRDT deserialization
            let json_v: serde_json::Value = v.clone().into();
            if let Ok(mut new_crdt) = serde_json::from_value::<Crdt>(json_v)
                && let Some(existing_v) = entry.get(&k)
                && let Ok(existing_crdt) = serde_json::from_value::<Crdt>(existing_v.clone().into())
            {
                // Use try_merge to avoid panic on type mismatch
                if new_crdt.try_merge(&existing_crdt).is_ok()
                    && let Ok(merged_json) = serde_json::to_value(new_crdt)
                {
                    entry.insert(k, uni_common::Value::from(merged_json));
                    continue;
                }
                // try_merge failed (type mismatch) - fall through to overwrite
            }
            // Fallback: Overwrite
            entry.insert(k, v);
        }
    }

    /// Helper function to estimate property map size in bytes.
    fn estimate_properties_size(props: &Properties) -> usize {
        props.keys().map(|k| k.len() + 32).sum()
    }

    /// Returns an estimate of the buffer size in bytes.
    /// Includes all fields for accurate memory accounting.
    pub fn size_bytes(&self) -> usize {
        let mut total = 0;

        // Topology
        total += self.graph.vertex_count() * 8;
        total += self.graph.edge_count() * 24;

        // Properties (rough estimate: key string + 32 bytes for value)
        for props in self.vertex_properties.values() {
            total += Self::estimate_properties_size(props);
        }
        for props in self.edge_properties.values() {
            total += Self::estimate_properties_size(props);
        }

        // Metadata
        total += self.tombstones.len() * 64;
        total += self.vertex_tombstones.len() * 8;
        total += self.edge_versions.len() * 16;
        total += self.vertex_versions.len() * 16;
        total += self.edge_endpoints.len() * 28; // (Vid, Vid, u32) = 8+8+4 + overhead

        // Vertex labels
        for labels in self.vertex_labels.values() {
            total += labels.iter().map(|l| l.len() + 24).sum::<usize>();
        }

        // Edge types
        for type_name in self.edge_types.values() {
            total += type_name.len() + 24;
        }

        // Timestamps (4 maps, each entry is 16 bytes: 8-byte key + 8-byte i64 value)
        total += self.vertex_created_at.len() * 16;
        total += self.vertex_updated_at.len() * 16;
        total += self.edge_created_at.len() * 16;
        total += self.edge_updated_at.len() * 16;

        total
    }

    pub fn new(start_version: u64, wal: Option<Arc<WriteAheadLog>>) -> Self {
        Self {
            graph: SimpleGraph::new(),
            tombstones: HashMap::new(),
            vertex_tombstones: HashSet::new(),
            edge_versions: HashMap::new(),
            vertex_versions: HashMap::new(),
            edge_properties: HashMap::new(),
            vertex_properties: HashMap::new(),
            edge_endpoints: HashMap::new(),
            vertex_labels: HashMap::new(),
            edge_types: HashMap::new(),
            current_version: start_version,
            mutation_count: 0,
            mutation_stats: MutationStats::default(),
            wal,
            wal_lsn_at_flush: 0,
            vertex_created_at: HashMap::new(),
            vertex_updated_at: HashMap::new(),
            edge_created_at: HashMap::new(),
            edge_updated_at: HashMap::new(),
            estimated_size: 0,
            constraint_index: HashMap::new(),
        }
    }

    pub fn insert_vertex(&mut self, vid: Vid, properties: Properties) {
        self.insert_vertex_with_labels(vid, properties, &[]);
    }

    /// Insert a vertex with associated labels.
    pub fn insert_vertex_with_labels(
        &mut self,
        vid: Vid,
        properties: Properties,
        labels: &[String],
    ) {
        self.current_version += 1;
        let version = self.current_version;
        let now = now_nanos();

        if let Some(wal) = &self.wal {
            let _ = wal.append(&Mutation::InsertVertex {
                vid,
                properties: properties.clone(),
                labels: labels.to_vec(),
            });
        }

        self.vertex_tombstones.remove(&vid);

        let entry = self.vertex_properties.entry(vid).or_default();
        Self::merge_crdt_properties(entry, properties.clone());
        self.vertex_versions.insert(vid, version);

        // Set timestamps - created_at only set if this is a new vertex
        self.vertex_created_at.entry(vid).or_insert(now);
        self.vertex_updated_at.insert(vid, now);

        // Track labels — always create an entry so unlabeled vertices are
        // distinguishable from "not in L0" when queried via get_vertex_labels.
        let labels_size: usize = labels.iter().map(|l| l.len() + 24).sum();
        let existing = self.vertex_labels.entry(vid).or_default();
        Self::append_unique_labels(existing, labels);

        self.graph.add_vertex(vid);
        self.mutation_count += 1;
        self.mutation_stats.nodes_created += 1;
        self.mutation_stats.properties_set += properties.len();
        self.mutation_stats.labels_added += labels.len();

        let props_size = Self::estimate_properties_size(&properties);
        self.estimated_size += 8 + props_size + 16 + labels_size + 32;
    }

    /// Add labels to an existing vertex.
    pub fn add_vertex_labels(&mut self, vid: Vid, labels: &[String]) {
        let existing = self.vertex_labels.entry(vid).or_default();
        Self::append_unique_labels(existing, labels);
    }

    /// Remove a label from an existing vertex.
    /// Returns true if the label was found and removed, false otherwise.
    pub fn remove_vertex_label(&mut self, vid: Vid, label: &str) -> bool {
        if let Some(labels) = self.vertex_labels.get_mut(&vid)
            && let Some(pos) = labels.iter().position(|l| l == label)
        {
            labels.remove(pos);
            self.current_version += 1;
            self.mutation_count += 1;
            self.mutation_stats.labels_removed += 1;
            // Note: WAL logging for label mutations not yet implemented
            // Currently consistent with add_vertex_labels behavior
            return true;
        }
        false
    }

    /// Set the type for an edge.
    pub fn set_edge_type(&mut self, eid: Eid, edge_type: String) {
        self.edge_types.insert(eid, edge_type);
    }

    pub fn delete_vertex(&mut self, vid: Vid) -> Result<()> {
        self.current_version += 1;

        if let Some(wal) = &mut self.wal {
            let labels = self.vertex_labels.get(&vid).cloned().unwrap_or_default();
            wal.append(&Mutation::DeleteVertex { vid, labels })?;
        }

        self.apply_vertex_deletion(vid);
        Ok(())
    }

    /// Cascade-delete a vertex: tombstone all connected edges and remove the vertex.
    ///
    /// Shared between `delete_vertex` (live mutations) and `replay_mutations` (WAL recovery).
    fn apply_vertex_deletion(&mut self, vid: Vid) {
        let version = self.current_version;

        // Collect edges to delete using O(degree) neighbors() instead of O(E) scan
        let mut edges_to_remove = HashSet::new();

        // Collect outgoing edges
        for entry in self.graph.neighbors(vid, Direction::Outgoing) {
            edges_to_remove.insert(entry.eid);
        }

        // Collect incoming edges
        for entry in self.graph.neighbors(vid, Direction::Incoming) {
            edges_to_remove.insert(entry.eid); // HashSet handles self-loop deduplication
        }

        let cascaded_edges_count = edges_to_remove.len();

        // Tombstone and remove all collected edges
        for eid in edges_to_remove {
            // Retrieve edge endpoints from the map to create tombstone
            if let Some((src, dst, etype)) = self.edge_endpoints.get(&eid) {
                self.tombstones.insert(
                    eid,
                    TombstoneEntry {
                        eid,
                        src_vid: *src,
                        dst_vid: *dst,
                        edge_type: *etype,
                    },
                );
                self.edge_versions.insert(eid, version);
                self.edge_endpoints.remove(&eid);
                self.edge_properties.remove(&eid);
                self.graph.remove_edge(eid);
                self.mutation_count += 1;
                self.mutation_stats.relationships_deleted += 1;
            }
        }

        self.vertex_tombstones.insert(vid);
        self.vertex_properties.remove(&vid);
        self.vertex_versions.insert(vid, version);
        self.graph.remove_vertex(vid);
        self.mutation_count += 1;
        self.mutation_stats.nodes_deleted += 1;

        // Remove constraint index entries for this vertex
        self.constraint_index.retain(|_, v| *v != vid);

        // 64 bytes per edge tombstone + 8 for vertex tombstone
        self.estimated_size += cascaded_edges_count * 72 + 8;
    }

    pub fn insert_edge(
        &mut self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        properties: Properties,
        edge_type_name: Option<String>,
    ) -> Result<()> {
        self.current_version += 1;
        let now = now_nanos();

        if let Some(wal) = &mut self.wal {
            wal.append(&Mutation::InsertEdge {
                src_vid,
                dst_vid,
                edge_type,
                eid,
                version: self.current_version,
                properties: properties.clone(),
                edge_type_name: edge_type_name.clone(),
            })?;
        }

        self.apply_edge_insertion(src_vid, dst_vid, edge_type, eid, properties)?;

        // Store edge type name in metadata if provided
        let type_name_size = if let Some(ref name) = edge_type_name {
            let size = name.len() + 24;
            self.edge_types.insert(eid, name.clone());
            size
        } else {
            0
        };

        // Set timestamps - created_at only set if this is a new edge
        self.edge_created_at.entry(eid).or_insert(now);
        self.edge_updated_at.insert(eid, now);

        self.estimated_size += type_name_size;

        Ok(())
    }

    /// Core edge insertion logic: add vertices, add edge, merge properties, update metadata.
    ///
    /// Shared between `insert_edge` (live mutations) and `replay_mutations` (WAL recovery).
    ///
    /// # Errors
    ///
    /// Returns error if either endpoint vertex has been deleted (exists in vertex_tombstones).
    /// This prevents "ghost vertex" resurrection via edge insertion. See issue #77.
    fn apply_edge_insertion(
        &mut self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        properties: Properties,
    ) -> Result<()> {
        let version = self.current_version;

        // Check if either endpoint has been deleted. Inserting an edge to a deleted
        // vertex would resurrect it as a "ghost vertex" with no properties. See issue #77.
        if self.vertex_tombstones.contains(&src_vid) {
            anyhow::bail!(
                "Cannot insert edge: source vertex {} has been deleted (issue #77)",
                src_vid
            );
        }
        if self.vertex_tombstones.contains(&dst_vid) {
            anyhow::bail!(
                "Cannot insert edge: destination vertex {} has been deleted (issue #77)",
                dst_vid
            );
        }

        // Add vertices to graph topology if they don't exist.
        // IMPORTANT: Only add to graph structure, do NOT call insert_vertex.
        // insert_vertex creates a new version with empty properties, which would
        // cause MVCC to pick the empty version as "latest", losing original properties.
        if !self.graph.contains_vertex(src_vid) {
            self.graph.add_vertex(src_vid);
        }
        if !self.graph.contains_vertex(dst_vid) {
            self.graph.add_vertex(dst_vid);
        }

        self.graph.add_edge(src_vid, dst_vid, eid, edge_type);

        // Store metadata with CRDT merge logic
        let props_size = Self::estimate_properties_size(&properties);
        let props_count = properties.len();
        if !properties.is_empty() {
            let entry = self.edge_properties.entry(eid).or_default();
            Self::merge_crdt_properties(entry, properties);
        }

        self.edge_versions.insert(eid, version);
        self.edge_endpoints
            .insert(eid, (src_vid, dst_vid, edge_type));
        self.tombstones.remove(&eid);
        self.mutation_count += 1;
        self.mutation_stats.relationships_created += 1;
        self.mutation_stats.properties_set += props_count;

        // 24 edge + props + 16 version + 28 endpoints + 32 timestamps
        self.estimated_size += 24 + props_size + 16 + 28 + 32;

        Ok(())
    }

    pub fn delete_edge(
        &mut self,
        eid: Eid,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
    ) -> Result<()> {
        self.current_version += 1;
        let now = now_nanos();

        if let Some(wal) = &mut self.wal {
            wal.append(&Mutation::DeleteEdge {
                eid,
                src_vid,
                dst_vid,
                edge_type,
                version: self.current_version,
            })?;
        }

        self.apply_edge_deletion(eid, src_vid, dst_vid, edge_type);

        // Update timestamp - deletion is an update
        self.edge_updated_at.insert(eid, now);

        Ok(())
    }

    /// Core edge deletion logic: tombstone the edge, update version, remove from graph.
    ///
    /// Shared between `delete_edge` (live mutations) and `replay_mutations` (WAL recovery).
    fn apply_edge_deletion(&mut self, eid: Eid, src_vid: Vid, dst_vid: Vid, edge_type: u32) {
        let version = self.current_version;

        self.tombstones.insert(
            eid,
            TombstoneEntry {
                eid,
                src_vid,
                dst_vid,
                edge_type,
            },
        );
        self.edge_versions.insert(eid, version);
        self.graph.remove_edge(eid);
        self.mutation_count += 1;
        self.mutation_stats.relationships_deleted += 1;

        // 64 bytes tombstone + 16 bytes version
        self.estimated_size += 80;
    }

    /// Returns neighbors in the specified direction.
    /// O(degree) complexity - iterates only edges connected to the vertex.
    pub fn get_neighbors(
        &self,
        vid: Vid,
        edge_type: u32,
        direction: Direction,
    ) -> Vec<(Vid, Eid, u64)> {
        let edges = self.graph.neighbors(vid, direction);

        edges
            .iter()
            .filter(|e| e.edge_type == edge_type && !self.is_tombstoned(e.eid))
            .map(|e| {
                let neighbor = match direction {
                    Direction::Outgoing => e.dst_vid,
                    Direction::Incoming => e.src_vid,
                };
                let version = self.edge_versions.get(&e.eid).copied().unwrap_or(0);
                (neighbor, e.eid, version)
            })
            .collect()
    }

    pub fn is_tombstoned(&self, eid: Eid) -> bool {
        self.tombstones.contains_key(&eid)
    }

    /// Returns all VIDs in vertex_labels that match the given label name.
    /// Used for L0 overlay during vertex scanning.
    pub fn vids_for_label(&self, label_name: &str) -> Vec<Vid> {
        self.vertex_labels
            .iter()
            .filter(|(_, labels)| labels.iter().any(|l| l == label_name))
            .map(|(vid, _)| *vid)
            .collect()
    }

    /// Returns all vertex VIDs in the L0 buffer.
    ///
    /// Used for schemaless scanning (MATCH (n) without label).
    pub fn all_vertex_vids(&self) -> Vec<Vid> {
        self.vertex_properties.keys().copied().collect()
    }

    /// Returns all VIDs in vertex_labels that match any of the given label names.
    /// Used for L0 overlay during multi-label vertex scanning.
    pub fn vids_for_labels(&self, label_names: &[&str]) -> Vec<Vid> {
        self.vertex_labels
            .iter()
            .filter(|(_, labels)| label_names.iter().any(|ln| labels.iter().any(|l| l == *ln)))
            .map(|(vid, _)| *vid)
            .collect()
    }

    /// Returns all VIDs that have ALL specified labels.
    pub fn vids_with_all_labels(&self, label_names: &[&str]) -> Vec<Vid> {
        self.vertex_labels
            .iter()
            .filter(|(_, labels)| label_names.iter().all(|ln| labels.iter().any(|l| l == *ln)))
            .map(|(vid, _)| *vid)
            .collect()
    }

    /// Gets the labels for a VID.
    pub fn get_vertex_labels(&self, vid: Vid) -> Option<&[String]> {
        self.vertex_labels.get(&vid).map(|v| v.as_slice())
    }

    /// Gets the edge type for an EID.
    pub fn get_edge_type(&self, eid: Eid) -> Option<&str> {
        self.edge_types.get(&eid).map(|s| s.as_str())
    }

    /// Returns all EIDs in edge_types that match the given type name.
    /// Used for L0 overlay during schemaless edge scanning.
    pub fn eids_for_type(&self, type_name: &str) -> Vec<Eid> {
        self.edge_types
            .iter()
            .filter(|(eid, etype)| *etype == type_name && !self.tombstones.contains_key(eid))
            .map(|(eid, _)| *eid)
            .collect()
    }

    /// Returns all edge EIDs in the L0 buffer (non-tombstoned).
    ///
    /// Used for schemaless scanning (`MATCH ()-[r]->()`) without type.
    pub fn all_edge_eids(&self) -> Vec<Eid> {
        self.edge_endpoints
            .keys()
            .filter(|eid| !self.tombstones.contains_key(eid))
            .copied()
            .collect()
    }

    /// Returns edge endpoint data (src_vid, dst_vid) for an EID.
    pub fn get_edge_endpoints(&self, eid: Eid) -> Option<(Vid, Vid)> {
        self.edge_endpoints
            .get(&eid)
            .map(|(src, dst, _)| (*src, *dst))
    }

    /// Returns full edge endpoint data (src_vid, dst_vid, edge_type_id) for an EID.
    pub fn get_edge_endpoint_full(&self, eid: Eid) -> Option<(Vid, Vid, u32)> {
        self.edge_endpoints.get(&eid).copied()
    }

    /// Insert a constraint key into the index for O(1) duplicate detection.
    pub fn insert_constraint_key(&mut self, key: Vec<u8>, vid: Vid) {
        self.constraint_index.insert(key, vid);
    }

    /// Check if a constraint key exists in the index, excluding a specific VID.
    /// Returns true if the key exists and is owned by a different vertex.
    pub fn has_constraint_key(&self, key: &[u8], exclude_vid: Vid) -> bool {
        self.constraint_index
            .get(key)
            .is_some_and(|&v| v != exclude_vid)
    }

    #[instrument(skip(self, other), level = "trace")]
    pub fn merge(&mut self, other: &L0Buffer) -> Result<()> {
        trace!(
            other_mutation_count = other.mutation_count,
            "Merging L0 buffer"
        );
        // Merge Vertices
        for &vid in &other.vertex_tombstones {
            self.delete_vertex(vid)?;
        }

        for (vid, props) in &other.vertex_properties {
            let labels = other.vertex_labels.get(vid).cloned().unwrap_or_default();
            self.insert_vertex_with_labels(*vid, props.clone(), &labels);
        }

        // Merge vertex labels that might not have properties
        for (vid, labels) in &other.vertex_labels {
            if !self.vertex_labels.contains_key(vid) {
                self.vertex_labels.insert(*vid, labels.clone());
            }
        }

        // Merge Edges - insert all edges from edge_endpoints, using empty props if none exist
        for (eid, (src, dst, etype)) in &other.edge_endpoints {
            if other.tombstones.contains_key(eid) {
                self.delete_edge(*eid, *src, *dst, *etype)?;
            } else {
                let props = other.edge_properties.get(eid).cloned().unwrap_or_default();
                let etype_name = other.edge_types.get(eid).cloned();
                self.insert_edge(*src, *dst, *etype, *eid, props, etype_name)?;
            }
        }

        // Merge tombstones for edges that only exist in the target buffer (self),
        // not in the source buffer's edge_endpoints.  Without this, transaction
        // DELETEs of pre-existing edges are silently lost on commit.
        for (eid, tombstone) in &other.tombstones {
            if !other.edge_endpoints.contains_key(eid) {
                self.delete_edge(
                    *eid,
                    tombstone.src_vid,
                    tombstone.dst_vid,
                    tombstone.edge_type,
                )?;
            }
        }

        // Edge types are now merged inside insert_edge, so no separate loop needed

        // Merge timestamps - preserve semantics of or_insert (keep oldest created_at)
        // and insert (use latest updated_at)
        for (vid, ts) in &other.vertex_created_at {
            self.vertex_created_at.entry(*vid).or_insert(*ts); // keep oldest
        }
        for (vid, ts) in &other.vertex_updated_at {
            self.vertex_updated_at.insert(*vid, *ts); // use latest (tx wins)
        }

        for (eid, ts) in &other.edge_created_at {
            self.edge_created_at.entry(*eid).or_insert(*ts); // keep oldest
        }
        for (eid, ts) in &other.edge_updated_at {
            self.edge_updated_at.insert(*eid, *ts); // use latest (tx wins)
        }

        // Conservatively add other's estimated size (may overcount due to
        // deduplication, but that's safe for a memory limit).
        self.estimated_size += other.estimated_size;

        // Merge constraint index
        for (key, vid) in &other.constraint_index {
            self.constraint_index.insert(key.clone(), *vid);
        }

        Ok(())
    }

    /// Replay mutations from WAL without re-logging them.
    /// Used during startup recovery to restore L0 state from persisted WAL.
    /// Uses CRDT merge semantics to ensure recovered state matches pre-crash state.
    #[instrument(skip(self, mutations), level = "debug")]
    pub fn replay_mutations(&mut self, mutations: Vec<Mutation>) -> Result<()> {
        trace!(count = mutations.len(), "Replaying mutations");
        for mutation in mutations {
            match mutation {
                Mutation::InsertVertex {
                    vid,
                    properties,
                    labels,
                } => {
                    // Apply without WAL logging, with CRDT merge semantics
                    self.current_version += 1;
                    let version = self.current_version;

                    self.vertex_tombstones.remove(&vid);
                    let entry = self.vertex_properties.entry(vid).or_default();
                    Self::merge_crdt_properties(entry, properties);
                    self.vertex_versions.insert(vid, version);
                    self.graph.add_vertex(vid);
                    self.mutation_count += 1;

                    // Restore vertex labels from WAL
                    let existing = self.vertex_labels.entry(vid).or_default();
                    Self::append_unique_labels(existing, &labels);
                }
                Mutation::DeleteVertex { vid, labels } => {
                    self.current_version += 1;
                    // Restore labels BEFORE apply_vertex_deletion
                    if !labels.is_empty() {
                        let existing = self.vertex_labels.entry(vid).or_default();
                        Self::append_unique_labels(existing, &labels);
                    }
                    self.apply_vertex_deletion(vid);
                }
                Mutation::InsertEdge {
                    src_vid,
                    dst_vid,
                    edge_type,
                    eid,
                    version: _,
                    properties,
                    edge_type_name,
                } => {
                    self.current_version += 1;
                    self.apply_edge_insertion(src_vid, dst_vid, edge_type, eid, properties)?;
                    // Restore edge type name metadata if present
                    if let Some(name) = edge_type_name {
                        self.edge_types.insert(eid, name);
                    }
                }
                Mutation::DeleteEdge {
                    eid,
                    src_vid,
                    dst_vid,
                    edge_type,
                    version: _,
                } => {
                    self.current_version += 1;
                    self.apply_edge_deletion(eid, src_vid, dst_vid, edge_type);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l0_buffer_ops() -> Result<()> {
        let mut l0 = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let eid_ab = Eid::new(101);

        l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new(), None)?;

        let neighbors = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, vid_b);
        assert_eq!(neighbors[0].1, eid_ab);

        l0.delete_edge(eid_ab, vid_a, vid_b, 1)?;
        assert!(l0.is_tombstoned(eid_ab));

        // Verify neighbors are empty after deletion
        let neighbors_after = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors_after.len(), 0);

        Ok(())
    }

    #[test]
    fn test_l0_buffer_multiple_edges() -> Result<()> {
        let mut l0 = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let vid_c = Vid::new(3);
        let eid_ab = Eid::new(101);
        let eid_ac = Eid::new(102);

        l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new(), None)?;
        l0.insert_edge(vid_a, vid_c, 1, eid_ac, HashMap::new(), None)?;

        let neighbors = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 2);

        // Delete one edge
        l0.delete_edge(eid_ab, vid_a, vid_b, 1)?;

        // Should still have one neighbor
        let neighbors_after = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors_after.len(), 1);
        assert_eq!(neighbors_after[0].0, vid_c);

        Ok(())
    }

    #[test]
    fn test_l0_buffer_edge_type_filter() -> Result<()> {
        let mut l0 = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let vid_c = Vid::new(3);
        let eid_ab = Eid::new(101);
        let eid_ac = Eid::new(201); // Different edge type

        l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new(), None)?;
        l0.insert_edge(vid_a, vid_c, 2, eid_ac, HashMap::new(), None)?;

        // Filter by edge type 1
        let type1_neighbors = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(type1_neighbors.len(), 1);
        assert_eq!(type1_neighbors[0].0, vid_b);

        // Filter by edge type 2
        let type2_neighbors = l0.get_neighbors(vid_a, 2, Direction::Outgoing);
        assert_eq!(type2_neighbors.len(), 1);
        assert_eq!(type2_neighbors[0].0, vid_c);

        Ok(())
    }

    #[test]
    fn test_l0_buffer_incoming_edges() -> Result<()> {
        let mut l0 = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let vid_c = Vid::new(3);
        let eid_ab = Eid::new(101);
        let eid_cb = Eid::new(102);

        // a -> b and c -> b
        l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new(), None)?;
        l0.insert_edge(vid_c, vid_b, 1, eid_cb, HashMap::new(), None)?;

        // Check incoming edges to b
        let incoming = l0.get_neighbors(vid_b, 1, Direction::Incoming);
        assert_eq!(incoming.len(), 2);

        Ok(())
    }

    /// Regression test: merge should preserve edges without properties
    #[test]
    fn test_merge_empty_props_edge() -> Result<()> {
        let mut main_l0 = L0Buffer::new(0, None);
        let mut tx_l0 = L0Buffer::new(0, None);

        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let eid_ab = Eid::new(101);

        // Insert edge with empty properties in transaction L0
        tx_l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new(), None)?;

        // Verify edge exists in tx_l0
        assert!(tx_l0.edge_endpoints.contains_key(&eid_ab));
        assert!(!tx_l0.edge_properties.contains_key(&eid_ab)); // No properties entry

        // Merge into main L0
        main_l0.merge(&tx_l0)?;

        // Edge should exist in main L0 after merge
        assert!(main_l0.edge_endpoints.contains_key(&eid_ab));
        let neighbors = main_l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, vid_b);

        Ok(())
    }

    /// Regression test: WAL replay should use CRDT merge semantics
    #[test]
    fn test_replay_crdt_merge() -> Result<()> {
        use crate::runtime::wal::Mutation;
        use serde_json::json;
        use uni_common::Value;

        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // Create GCounter CRDT values using correct serde format:
        // {"t": "gc", "d": {"counts": {...}}}
        let counter1: Value = json!({
            "t": "gc",
            "d": {"counts": {"node1": 5}}
        })
        .into();
        let counter2: Value = json!({
            "t": "gc",
            "d": {"counts": {"node2": 3}}
        })
        .into();

        // First mutation: insert vertex with counter1
        let mut props1 = HashMap::new();
        props1.insert("counter".to_string(), counter1.clone());
        l0.replay_mutations(vec![Mutation::InsertVertex {
            vid,
            properties: props1,
            labels: vec![],
        }])?;

        // Second mutation: insert same vertex with counter2 (should merge)
        let mut props2 = HashMap::new();
        props2.insert("counter".to_string(), counter2.clone());
        l0.replay_mutations(vec![Mutation::InsertVertex {
            vid,
            properties: props2,
            labels: vec![],
        }])?;

        // Verify CRDT was merged (both node1 and node2 counts present)
        let stored_props = l0.vertex_properties.get(&vid).unwrap();
        let stored_counter = stored_props.get("counter").unwrap();

        // Convert back to serde_json::Value for nested access
        let stored_json: serde_json::Value = stored_counter.clone().into();
        // The merged counter should have both node1: 5 and node2: 3
        let data = stored_json.get("d").unwrap();
        let counts = data.get("counts").unwrap();
        assert_eq!(counts.get("node1"), Some(&json!(5)));
        assert_eq!(counts.get("node2"), Some(&json!(3)));

        Ok(())
    }

    #[test]
    fn test_merge_preserves_vertex_timestamps() -> Result<()> {
        let mut l0_main = L0Buffer::new(0, None);
        let mut l0_tx = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // Main buffer: insert vertex with timestamp T1
        let ts_main_created = 1000;
        let ts_main_updated = 1100;
        l0_main.insert_vertex(vid, HashMap::new());
        l0_main.vertex_created_at.insert(vid, ts_main_created);
        l0_main.vertex_updated_at.insert(vid, ts_main_updated);

        // Transaction buffer: update same vertex with timestamp T2 (later)
        let ts_tx_created = 2000; // should be ignored (main has older created_at)
        let ts_tx_updated = 2100; // should win (tx has newer updated_at)
        l0_tx.insert_vertex(vid, HashMap::new());
        l0_tx.vertex_created_at.insert(vid, ts_tx_created);
        l0_tx.vertex_updated_at.insert(vid, ts_tx_updated);

        // Merge transaction into main
        l0_main.merge(&l0_tx)?;

        // Verify created_at is oldest (from main)
        assert_eq!(
            *l0_main.vertex_created_at.get(&vid).unwrap(),
            ts_main_created,
            "created_at should preserve oldest timestamp"
        );

        // Verify updated_at is latest (from tx)
        assert_eq!(
            *l0_main.vertex_updated_at.get(&vid).unwrap(),
            ts_tx_updated,
            "updated_at should use latest timestamp"
        );

        Ok(())
    }

    #[test]
    fn test_merge_preserves_edge_timestamps() -> Result<()> {
        let mut l0_main = L0Buffer::new(0, None);
        let mut l0_tx = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let eid = Eid::new(100);

        // Main buffer: insert edge with timestamp T1
        let ts_main_created = 1000;
        let ts_main_updated = 1100;
        l0_main.insert_edge(vid_a, vid_b, 1, eid, HashMap::new(), None)?;
        l0_main.edge_created_at.insert(eid, ts_main_created);
        l0_main.edge_updated_at.insert(eid, ts_main_updated);

        // Transaction buffer: update same edge with timestamp T2 (later)
        let ts_tx_created = 2000; // should be ignored
        let ts_tx_updated = 2100; // should win
        l0_tx.insert_edge(vid_a, vid_b, 1, eid, HashMap::new(), None)?;
        l0_tx.edge_created_at.insert(eid, ts_tx_created);
        l0_tx.edge_updated_at.insert(eid, ts_tx_updated);

        // Merge transaction into main
        l0_main.merge(&l0_tx)?;

        // Verify created_at is oldest (from main)
        assert_eq!(
            *l0_main.edge_created_at.get(&eid).unwrap(),
            ts_main_created,
            "edge created_at should preserve oldest timestamp"
        );

        // Verify updated_at is latest (from tx)
        assert_eq!(
            *l0_main.edge_updated_at.get(&eid).unwrap(),
            ts_tx_updated,
            "edge updated_at should use latest timestamp"
        );

        Ok(())
    }

    #[test]
    fn test_merge_created_at_not_overwritten_for_existing_vertex() -> Result<()> {
        use uni_common::Value;

        let mut l0_main = L0Buffer::new(0, None);
        let mut l0_tx = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // Main buffer: vertex created at T1
        let ts_original = 1000;
        l0_main.insert_vertex(vid, HashMap::new());
        l0_main.vertex_created_at.insert(vid, ts_original);
        l0_main.vertex_updated_at.insert(vid, ts_original);

        // Transaction buffer: update vertex (created_at would be T2 if set)
        let ts_tx = 2000;
        let mut props = HashMap::new();
        props.insert("updated".to_string(), Value::String("yes".to_string()));
        l0_tx.insert_vertex(vid, props);
        l0_tx.vertex_created_at.insert(vid, ts_tx);
        l0_tx.vertex_updated_at.insert(vid, ts_tx);

        // Merge transaction into main
        l0_main.merge(&l0_tx)?;

        // Verify created_at was NOT overwritten (still T1, not T2)
        assert_eq!(
            *l0_main.vertex_created_at.get(&vid).unwrap(),
            ts_original,
            "created_at must not be overwritten for existing vertex"
        );

        // Verify updated_at WAS updated (now T2)
        assert_eq!(
            *l0_main.vertex_updated_at.get(&vid).unwrap(),
            ts_tx,
            "updated_at should reflect transaction timestamp"
        );

        // Verify properties were merged
        assert!(
            l0_main
                .vertex_properties
                .get(&vid)
                .unwrap()
                .contains_key("updated")
        );

        Ok(())
    }

    /// Test for Issue #23: Vertex labels preserved through replay_mutations
    #[test]
    fn test_replay_mutations_preserves_vertex_labels() -> Result<()> {
        use crate::runtime::wal::Mutation;

        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(42);

        // Create InsertVertex mutation with labels
        let mutations = vec![Mutation::InsertVertex {
            vid,
            properties: {
                let mut props = HashMap::new();
                props.insert(
                    "name".to_string(),
                    uni_common::Value::String("Alice".to_string()),
                );
                props
            },
            labels: vec!["Person".to_string(), "User".to_string()],
        }];

        // Replay mutations
        l0.replay_mutations(mutations)?;

        // Verify vertex exists in L0
        assert!(l0.vertex_properties.contains_key(&vid));

        // Verify labels are preserved
        let labels = l0.get_vertex_labels(vid).expect("Labels should exist");
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&"Person".to_string()));
        assert!(labels.contains(&"User".to_string()));

        // Verify vertex is findable by label
        let person_vids = l0.vids_for_label("Person");
        assert_eq!(person_vids.len(), 1);
        assert_eq!(person_vids[0], vid);

        let user_vids = l0.vids_for_label("User");
        assert_eq!(user_vids.len(), 1);
        assert_eq!(user_vids[0], vid);

        Ok(())
    }

    /// Test for Issue #23: DeleteVertex labels preserved for tombstone flushing
    #[test]
    fn test_replay_mutations_preserves_delete_vertex_labels() -> Result<()> {
        use crate::runtime::wal::Mutation;

        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(99);

        // First insert vertex with labels
        l0.insert_vertex_with_labels(
            vid,
            HashMap::new(),
            &["Person".to_string(), "Admin".to_string()],
        );

        // Verify vertex and labels exist
        assert!(l0.vertex_properties.contains_key(&vid));
        let labels = l0.get_vertex_labels(vid).expect("Labels should exist");
        assert_eq!(labels.len(), 2);

        // Create DeleteVertex mutation with labels
        let mutations = vec![Mutation::DeleteVertex {
            vid,
            labels: vec!["Person".to_string(), "Admin".to_string()],
        }];

        // Replay deletion
        l0.replay_mutations(mutations)?;

        // Verify vertex is tombstoned
        assert!(l0.vertex_tombstones.contains(&vid));

        // Verify labels are preserved in L0 (needed for Issue #76 tombstone flushing)
        // The labels should still be accessible for the flush logic to know which tables to update
        let labels = l0.get_vertex_labels(vid);
        assert!(
            labels.is_some(),
            "Labels should be preserved even after deletion for tombstone flushing"
        );

        Ok(())
    }

    /// Test for Issue #28: Edge type name preserved through replay_mutations
    #[test]
    fn test_replay_mutations_preserves_edge_type_name() -> Result<()> {
        use crate::runtime::wal::Mutation;

        let mut l0 = L0Buffer::new(0, None);
        let src = Vid::new(1);
        let dst = Vid::new(2);
        let eid = Eid::new(500);
        let edge_type = 100;

        // Create InsertEdge mutation with edge_type_name
        let mutations = vec![Mutation::InsertEdge {
            src_vid: src,
            dst_vid: dst,
            edge_type,
            eid,
            version: 1,
            properties: {
                let mut props = HashMap::new();
                props.insert("since".to_string(), uni_common::Value::Int(2020));
                props
            },
            edge_type_name: Some("KNOWS".to_string()),
        }];

        // Replay mutations
        l0.replay_mutations(mutations)?;

        // Verify edge exists in L0
        assert!(l0.edge_endpoints.contains_key(&eid));

        // Verify edge type name is preserved
        let type_name = l0.get_edge_type(eid).expect("Edge type name should exist");
        assert_eq!(type_name, "KNOWS");

        // Verify edge is findable by type name
        let knows_eids = l0.eids_for_type("KNOWS");
        assert_eq!(knows_eids.len(), 1);
        assert_eq!(knows_eids[0], eid);

        Ok(())
    }

    /// Test for Issue #28: Edge type mapping survives multiple replay cycles
    #[test]
    fn test_edge_type_mapping_survives_multiple_replays() -> Result<()> {
        use crate::runtime::wal::Mutation;

        let mut l0 = L0Buffer::new(0, None);

        // Replay multiple edge insertions with different types
        let mutations = vec![
            Mutation::InsertEdge {
                src_vid: Vid::new(1),
                dst_vid: Vid::new(2),
                edge_type: 100,
                eid: Eid::new(1000),
                version: 1,
                properties: HashMap::new(),
                edge_type_name: Some("KNOWS".to_string()),
            },
            Mutation::InsertEdge {
                src_vid: Vid::new(2),
                dst_vid: Vid::new(3),
                edge_type: 101,
                eid: Eid::new(1001),
                version: 2,
                properties: HashMap::new(),
                edge_type_name: Some("LIKES".to_string()),
            },
            Mutation::InsertEdge {
                src_vid: Vid::new(3),
                dst_vid: Vid::new(1),
                edge_type: 100,
                eid: Eid::new(1002),
                version: 3,
                properties: HashMap::new(),
                edge_type_name: Some("KNOWS".to_string()),
            },
        ];

        l0.replay_mutations(mutations)?;

        // Verify all edge type mappings are preserved
        assert_eq!(l0.get_edge_type(Eid::new(1000)), Some("KNOWS"));
        assert_eq!(l0.get_edge_type(Eid::new(1001)), Some("LIKES"));
        assert_eq!(l0.get_edge_type(Eid::new(1002)), Some("KNOWS"));

        // Verify edges can be queried by type
        let knows_edges = l0.eids_for_type("KNOWS");
        assert_eq!(knows_edges.len(), 2);
        assert!(knows_edges.contains(&Eid::new(1000)));
        assert!(knows_edges.contains(&Eid::new(1002)));

        let likes_edges = l0.eids_for_type("LIKES");
        assert_eq!(likes_edges.len(), 1);
        assert_eq!(likes_edges[0], Eid::new(1001));

        Ok(())
    }

    /// Test for Issue #23 + #28: Combined vertex labels and edge types in replay
    #[test]
    fn test_replay_mutations_combined_labels_and_edge_types() -> Result<()> {
        use crate::runtime::wal::Mutation;

        let mut l0 = L0Buffer::new(0, None);
        let alice = Vid::new(1);
        let bob = Vid::new(2);
        let eid = Eid::new(100);

        // Simulate crash recovery scenario: replay full transaction log
        let mutations = vec![
            // Insert Alice with Person label
            Mutation::InsertVertex {
                vid: alice,
                properties: {
                    let mut props = HashMap::new();
                    props.insert(
                        "name".to_string(),
                        uni_common::Value::String("Alice".to_string()),
                    );
                    props
                },
                labels: vec!["Person".to_string()],
            },
            // Insert Bob with Person label
            Mutation::InsertVertex {
                vid: bob,
                properties: {
                    let mut props = HashMap::new();
                    props.insert(
                        "name".to_string(),
                        uni_common::Value::String("Bob".to_string()),
                    );
                    props
                },
                labels: vec!["Person".to_string()],
            },
            // Create KNOWS edge between them
            Mutation::InsertEdge {
                src_vid: alice,
                dst_vid: bob,
                edge_type: 1,
                eid,
                version: 3,
                properties: HashMap::new(),
                edge_type_name: Some("KNOWS".to_string()),
            },
        ];

        // Replay all mutations
        l0.replay_mutations(mutations)?;

        // Verify vertex labels preserved
        assert_eq!(l0.get_vertex_labels(alice).unwrap().len(), 1);
        assert_eq!(l0.get_vertex_labels(bob).unwrap().len(), 1);
        assert_eq!(l0.vids_for_label("Person").len(), 2);

        // Verify edge type name preserved
        assert_eq!(l0.get_edge_type(eid).unwrap(), "KNOWS");
        assert_eq!(l0.eids_for_type("KNOWS").len(), 1);

        // Verify graph structure
        let alice_neighbors = l0.get_neighbors(alice, 1, Direction::Outgoing);
        assert_eq!(alice_neighbors.len(), 1);
        assert_eq!(alice_neighbors[0].0, bob);

        Ok(())
    }

    /// Test for Issue #23: Empty labels should deserialize correctly (backward compat)
    #[test]
    fn test_replay_mutations_backward_compat_empty_labels() -> Result<()> {
        use crate::runtime::wal::Mutation;

        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // Simulate old WAL format: InsertVertex with empty labels
        // (This tests #[serde(default)] behavior)
        let mutations = vec![Mutation::InsertVertex {
            vid,
            properties: HashMap::new(),
            labels: vec![], // Empty labels (old format compatibility)
        }];

        l0.replay_mutations(mutations)?;

        // Vertex should exist
        assert!(l0.vertex_properties.contains_key(&vid));

        // Labels should be empty but entry should exist in vertex_labels
        let labels = l0.get_vertex_labels(vid);
        assert!(labels.is_some(), "Labels entry should exist even if empty");
        assert_eq!(labels.unwrap().len(), 0);

        Ok(())
    }

    #[test]
    fn test_now_nanos_returns_nanosecond_range() {
        // Test that now_nanos() returns a value in nanosecond range
        // As of 2025, Unix timestamp in nanoseconds should be > 1.7e18
        // (2025-01-01 is approximately 1,735,689,600 seconds = 1.735e18 nanoseconds)
        let now = now_nanos();

        // Verify it's in nanosecond range (not microseconds which would be 1000x smaller)
        assert!(
            now > 1_700_000_000_000_000_000,
            "now_nanos() returned {}, expected > 1.7e18 for nanoseconds",
            now
        );

        // Sanity check: should also be less than year 2100 in nanoseconds (4.1e18)
        assert!(
            now < 4_100_000_000_000_000_000,
            "now_nanos() returned {}, expected < 4.1e18",
            now
        );
    }
}
