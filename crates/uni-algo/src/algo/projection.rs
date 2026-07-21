// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Graph Projection - Dense CSR representation for algorithm execution.
//!
//! A `GraphProjection` is a materialized, algorithm-optimized view of a subgraph.
//! It provides:
//! - Dense vertex indexing (0..V) for efficient array-based state
//! - CSR format for cache-friendly neighbor iteration
//! - Optional reverse edges for algorithms like PageRank
//! - Optional edge weights for weighted algorithms

use crate::algo::IdMap;
use anyhow::{Result, anyhow};
use uni_common::core::id::{Eid, Vid};
use uni_store::runtime::L0Manager;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::direction::Direction as CacheDir;
use uni_store::storage::manager::StorageManager;

/// Edge list for CSR construction: (source_slot, destination_slot, weight) pairs.
type WeightedEdgeList = Vec<(u32, u32, f64)>;

/// CSR arrays returned by [`build_csr`]: offsets `[V+1]`, neighbor slots `[E]`,
/// optional weights `[E]`, and the co-permuted edge-property columns (each `[E]`).
type CsrParts = (Vec<u32>, Vec<u32>, Option<Vec<f64>>, Vec<Vec<f64>>);

/// Configuration for building a graph projection.
#[derive(Debug, Clone, Default)]
pub struct ProjectionConfig {
    /// Node labels to include (empty = all)
    pub node_labels: Vec<String>,
    /// Edge types to include (empty = all)
    pub edge_types: Vec<String>,
    /// Property to use as edge weight
    pub weight_property: Option<String>,
    /// Whether to build reverse edges (in_neighbors)
    pub include_reverse: bool,
    /// Vertex properties to materialize as per-vertex `[V]` f64 columns.
    pub node_properties: Vec<String>,
    /// Edge properties to materialize as per-edge `[E]` f64 columns (CSR order).
    pub edge_properties: Vec<String>,
}

/// Dense CSR representation optimized for algorithm execution.
#[derive(Debug, Clone)]
pub struct GraphProjection {
    /// Number of vertices in the projection
    pub(crate) vertex_count: usize,

    /// Outbound edges: CSR format
    pub(crate) out_offsets: Vec<u32>, // [V+1] vertex slot -> edge start
    pub(crate) out_neighbors: Vec<u32>, // [E] neighbor slots

    /// Inbound edges: CSR format (optional, for PageRank/SCC)
    pub(crate) in_offsets: Vec<u32>, // [V+1]
    pub(crate) in_neighbors: Vec<u32>, // [E]

    /// Optional edge weights
    pub(crate) out_weights: Option<Vec<f64>>,

    /// Materialized per-vertex property columns (`[V]`, vertex-slot order),
    /// keyed by property name (issue #151). Empty unless requested at build.
    pub(crate) node_properties: std::collections::HashMap<String, Vec<f64>>,
    /// Materialized per-edge property columns (`[E]`, CSR out-edge order),
    /// keyed by property name (issue #151). Empty unless requested at build.
    pub(crate) edge_properties: std::collections::HashMap<String, Vec<f64>>,

    /// Identity mapping
    pub(crate) id_map: IdMap,
}

impl GraphProjection {
    /// Number of vertices in the projection.
    #[inline]
    pub fn vertex_count(&self) -> usize {
        self.vertex_count
    }

    /// Number of edges in the projection.
    #[inline]
    pub fn edge_count(&self) -> usize {
        self.out_neighbors.len()
    }

    /// Outbound neighbors of a vertex (by slot).
    #[inline]
    pub fn out_neighbors(&self, slot: u32) -> &[u32] {
        let start = self.out_offsets[slot as usize] as usize;
        let end = self.out_offsets[slot as usize + 1] as usize;
        &self.out_neighbors[start..end]
    }

    /// Outbound degree of a vertex.
    #[inline]
    pub fn out_degree(&self, slot: u32) -> u32 {
        self.out_offsets[slot as usize + 1] - self.out_offsets[slot as usize]
    }

    /// Inbound neighbors of a vertex (by slot).
    ///
    /// Panics if projection was built without `include_reverse`.
    #[inline]
    pub fn in_neighbors(&self, slot: u32) -> &[u32] {
        let start = self.in_offsets[slot as usize] as usize;
        let end = self.in_offsets[slot as usize + 1] as usize;
        &self.in_neighbors[start..end]
    }

    /// Inbound degree of a vertex.
    #[inline]
    pub fn in_degree(&self, slot: u32) -> u32 {
        self.in_offsets[slot as usize + 1] - self.in_offsets[slot as usize]
    }

    /// Get edge weight for outbound edge.
    ///
    /// Panics if projection was built without weights.
    #[inline]
    pub fn out_weight(&self, slot: u32, edge_idx: usize) -> f64 {
        let start = self.out_offsets[slot as usize] as usize;
        self.out_weights.as_ref().expect("no weights")[start + edge_idx]
    }

    /// Global index of a vertex's first outbound edge in CSR edge order.
    ///
    /// Outbound edges are numbered `0..edge_count()` in CSR layout order, so
    /// slot `u`'s `k`-th out-neighbor is edge `out_edge_start(u) + k`. This is the
    /// canonical `[E]` edge index a per-edge tensor or edge mask addresses
    /// (plugin-compute proposal §5, `Shape::E`).
    ///
    /// # Examples
    /// ```
    /// # use uni_algo::algo::projection::GraphProjection;
    /// # fn demo(g: &GraphProjection) {
    /// // The first out-edge of slot 0 is global edge index out_edge_start(0).
    /// let e0 = g.out_edge_start(0);
    /// assert_eq!(e0, 0);
    /// # }
    /// ```
    #[inline]
    pub fn out_edge_start(&self, slot: u32) -> usize {
        self.out_offsets[slot as usize] as usize
    }

    /// Check if weights are available.
    #[inline]
    pub fn has_weights(&self) -> bool {
        self.out_weights.is_some()
    }

    /// Materialized per-vertex property column (`[V]`, vertex-slot order), if the
    /// projection was built with this vertex property (issue #151).
    #[inline]
    pub fn node_property(&self, name: &str) -> Option<&[f64]> {
        self.node_properties.get(name).map(Vec::as_slice)
    }

    /// Materialized per-edge property column (`[E]`, CSR out-edge order), if the
    /// projection was built with this edge property (issue #151).
    #[inline]
    pub fn edge_property(&self, name: &str) -> Option<&[f64]> {
        self.edge_properties.get(name).map(Vec::as_slice)
    }

    /// Check if reverse edges are available.
    #[inline]
    pub fn has_reverse(&self) -> bool {
        !self.in_neighbors.is_empty()
    }

    /// Map slot back to VID.
    #[inline]
    pub fn to_vid(&self, slot: u32) -> Vid {
        self.id_map.to_vid_unchecked(slot)
    }

    /// Map VID to slot.
    #[inline]
    pub fn to_slot(&self, vid: Vid) -> Option<u32> {
        self.id_map.to_slot(vid)
    }

    /// Iterate over all vertices as (slot, vid).
    pub fn vertices(&self) -> impl Iterator<Item = (u32, Vid)> + '_ {
        self.id_map.iter()
    }

    /// Memory usage in bytes.
    pub fn memory_size(&self) -> usize {
        self.out_offsets.len() * 4
            + self.out_neighbors.len() * 4
            + self.in_offsets.len() * 4
            + self.in_neighbors.len() * 4
            + self.out_weights.as_ref().map_or(0, |w| w.len() * 8)
            + self
                .node_properties
                .values()
                .map(|c| c.len() * 8)
                .sum::<usize>()
            + self
                .edge_properties
                .values()
                .map(|c| c.len() * 8)
                .sum::<usize>()
            + self.id_map.memory_size()
    }
}

use std::sync::Arc;

/// Builder for constructing a `GraphProjection` from storage.
pub struct ProjectionBuilder {
    storage: Arc<StorageManager>,
    /// L0 manager for scanning in-memory vertices not yet flushed.
    l0_manager: Option<Arc<L0Manager>>,
    config: ProjectionConfig,
}

impl ProjectionBuilder {
    /// Create a new projection builder.
    pub fn new(storage: Arc<StorageManager>) -> Self {
        Self {
            storage,
            l0_manager: None,
            config: ProjectionConfig::default(),
        }
    }

    /// Set the L0 manager for scanning in-memory vertices.
    pub fn l0_manager(mut self, l0_manager: Option<Arc<L0Manager>>) -> Self {
        self.l0_manager = l0_manager;
        self
    }

    /// Set node labels to include.
    pub fn node_labels(mut self, labels: &[&str]) -> Self {
        self.config.node_labels = labels.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Set edge types to include.
    pub fn edge_types(mut self, types: &[&str]) -> Self {
        self.config.edge_types = types.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Set weight property.
    pub fn weight_property(mut self, prop: &str) -> Self {
        self.config.weight_property = Some(prop.to_string());
        self
    }

    /// Include reverse edges for in_neighbors access.
    pub fn include_reverse(mut self, enabled: bool) -> Self {
        self.config.include_reverse = enabled;
        self
    }

    /// Materialize these vertex properties as per-vertex `[V]` f64 columns.
    pub fn node_properties(mut self, props: &[&str]) -> Self {
        self.config.node_properties = props.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Materialize these edge properties as per-edge `[E]` f64 columns (CSR order).
    pub fn edge_properties(mut self, props: &[&str]) -> Self {
        self.config.edge_properties = props.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Build the projection.
    ///
    /// Isolation: the L0 tier is read through a snapshot pinned once at the
    /// start of the build, so a concurrent commit cannot rotate buffers
    /// between the Lance scan and the L0 overlay (torn projection). Lance
    /// data committed *during* the build may still be picked up by the scan
    /// — the projection is an analytics view, not a serializable read.
    pub async fn build(self) -> Result<GraphProjection> {
        let schema = self.storage.schema_manager().schema();

        // Pin the L0 view for the whole build. Holding the SnapshotView's
        // pin marker makes concurrent committers clone-on-freeze instead of
        // mutating the captured generation.
        let l0_snapshot = self.l0_manager.as_ref().map(|m| m.pin_snapshot());

        // 1. Resolve label and edge type IDs
        let (label_ids, edge_type_ids) = self.resolve_ids(&schema)?;

        // 2. Warm cache for all requested edge types
        self.warm_caches(&label_ids, &edge_type_ids).await?;

        // 3. Collect VIDs from storage and the pinned L0 view
        let all_vids = self
            .collect_vertices(&schema, &label_ids, l0_snapshot.as_ref())
            .await?;

        let mut id_map = IdMap::with_capacity(all_vids.len());
        for vid in all_vids {
            id_map.insert(vid);
        }
        let vertex_count = id_map.len();

        // 4. Collect edges (with weights + requested edge-property columns)
        let (out_edges, in_edges, out_edge_props) =
            self.collect_edges(&id_map, &edge_type_ids).await?;

        // 5. Materialize requested per-vertex property columns in slot order.
        let node_properties = self.collect_node_properties(&id_map).await?;

        // Compact IdMap (drops hash map, enables binary search)
        id_map.compact();

        // Split edge-property (name, column) pairs; build_csr permutes the value
        // columns into CSR `[E]` order, then re-zip with their names.
        let (edge_prop_names, edge_prop_values): (Vec<String>, Vec<Vec<f64>>) =
            out_edge_props.into_iter().unzip();
        let (out_offsets, out_neighbors, out_weights, permuted_cols) =
            build_csr(vertex_count, &out_edges, true, &edge_prop_values);
        let edge_properties: std::collections::HashMap<String, Vec<f64>> =
            edge_prop_names.into_iter().zip(permuted_cols).collect();
        let (in_offsets, in_neighbors, _, _) = if self.config.include_reverse {
            build_csr(vertex_count, &in_edges, false, &[])
        } else {
            (vec![0; vertex_count + 1], Vec::new(), None, Vec::new())
        };

        Ok(GraphProjection {
            vertex_count,
            out_offsets,
            out_neighbors,
            in_offsets,
            in_neighbors,
            out_weights,
            node_properties,
            edge_properties,
            id_map,
        })
    }

    /// Resolve label and edge type IDs from configuration.
    fn resolve_ids(
        &self,
        schema: &uni_common::core::schema::Schema,
    ) -> Result<(Vec<u16>, Vec<u32>)> {
        let mut label_ids = Vec::new();
        for label_name in &self.config.node_labels {
            let meta = schema
                .labels
                .get(label_name)
                .ok_or_else(|| anyhow!("Label {} not found", label_name))?;
            label_ids.push(meta.id);
        }

        let mut edge_type_ids = Vec::new();
        for type_name in &self.config.edge_types {
            let meta = schema
                .edge_types
                .get(type_name)
                .ok_or_else(|| anyhow!("Edge type {} not found", type_name))?;
            edge_type_ids.push(meta.id);
        }

        // If empty, include all from schema
        if label_ids.is_empty() {
            label_ids = schema.labels.values().map(|m| m.id).collect();
        }
        if edge_type_ids.is_empty() {
            edge_type_ids = schema.edge_types.values().map(|m| m.id).collect();
        }

        Ok((label_ids, edge_type_ids))
    }

    /// Warm adjacency manager for all requested edge types.
    async fn warm_caches(&self, _label_ids: &[u16], edge_type_ids: &[u32]) -> Result<()> {
        for &type_id in edge_type_ids {
            let edge_ver = self.storage.get_edge_version_by_id(type_id);
            self.storage
                .warm_adjacency(type_id, CacheDir::Outgoing, edge_ver)
                .await?;
            if self.config.include_reverse {
                self.storage
                    .warm_adjacency(type_id, CacheDir::Incoming, edge_ver)
                    .await?;
            }
        }
        Ok(())
    }

    /// Collect VIDs from storage and L0 buffers.
    async fn collect_vertices(
        &self,
        schema: &uni_common::core::schema::Schema,
        label_ids: &[u16],
        l0_snapshot: Option<&uni_store::runtime::l0_manager::SnapshotView>,
    ) -> Result<Vec<Vid>> {
        use arrow_array::UInt64Array;

        let mut all_vids = Vec::new();

        for &lid in label_ids {
            let label_name = schema.label_name_by_id(lid).unwrap();
            if let Ok(Some(batch)) = self
                .storage
                .scan_vertex_table(label_name, &["_vid"], None)
                .await
            {
                let vid_col = batch
                    .column_by_name("_vid")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    all_vids.push(Vid::from(vid_col.value(i)));
                }
            }
        }

        // Overlay L0 vertices (not yet flushed to Lance) from the snapshot
        // pinned at the start of the build — never from live manager
        // accessors, which can rotate mid-build under concurrent commits.
        if let Some(snapshot) = l0_snapshot {
            let label_names: Vec<&str> = label_ids
                .iter()
                .filter_map(|id| schema.label_name_by_id(*id))
                .collect();

            // Generations that were flushing at pin time (oldest first)
            for pending_l0_arc in &snapshot.extra {
                all_vids.extend(pending_l0_arc.read().vids_for_labels(&label_names));
            }

            // The pinned main L0 generation
            all_vids.extend(snapshot.main.read().vids_for_labels(&label_names));
        }

        // Sort and dedup to ensure IdMap is sorted for compaction
        all_vids.sort_unstable();
        all_vids.dedup();

        Ok(all_vids)
    }

    /// Collect edges from adjacency manager.
    async fn collect_edges(
        &self,
        id_map: &IdMap,
        edge_type_ids: &[u32],
    ) -> Result<(WeightedEdgeList, WeightedEdgeList, Vec<(String, Vec<f64>)>)> {
        // Phase 1: Collect topology from AdjacencyManager
        let mut raw_out_edges = Vec::new(); // (src_slot, dst_vid, eid)
        let mut raw_in_edges = Vec::new();

        for (src_slot, src_vid) in id_map.iter() {
            for &type_id in edge_type_ids {
                // Outbound
                let neighbors = self.storage.adjacency_manager().get_neighbors(
                    src_vid,
                    type_id,
                    CacheDir::Outgoing,
                );
                for (dst_vid, eid) in neighbors {
                    raw_out_edges.push((src_slot, dst_vid, eid));
                }

                // Inbound
                if self.config.include_reverse {
                    let in_neighbors = self.storage.adjacency_manager().get_neighbors(
                        src_vid,
                        type_id,
                        CacheDir::Incoming,
                    );
                    for (dst_vid, eid) in in_neighbors {
                        raw_in_edges.push((src_slot, dst_vid, eid));
                    }
                }
            }
        }

        // Phase 2: fetch the weight property AND any requested edge properties
        // (issue #151) in a single batch, then map destination slots. Property
        // *values* are read from committed storage (the projection is a
        // storage-time analytics view), so unflushed L0 weights fall back to the
        // 1.0 default — the same contract the native weighted algorithms share.
        let edge_prop_names = &self.config.edge_properties;
        let weight_prop = self.config.weight_property.as_deref();
        let need_props = weight_prop.is_some() || !edge_prop_names.is_empty();
        let pm = need_props.then(|| {
            PropertyManager::new(
                self.storage.clone(),
                self.storage.schema_manager_arc(),
                1000,
            )
        });

        // Union of property names to fetch (weight + edge props), deduplicated.
        let mut fetch_names: Vec<&str> = Vec::new();
        if let Some(w) = weight_prop {
            fetch_names.push(w);
        }
        for name in edge_prop_names {
            if !fetch_names.contains(&name.as_str()) {
                fetch_names.push(name.as_str());
            }
        }

        // eid -> (property name -> f64), populated only for fetched properties.
        let mut props_cache: std::collections::HashMap<
            Eid,
            std::collections::HashMap<String, f64>,
        > = std::collections::HashMap::new();

        if let Some(pm) = pm.as_ref().filter(|_| !fetch_names.is_empty()) {
            // Collect and deduplicate EIDs from both edge lists
            let mut all_eids: Vec<Eid> = raw_out_edges
                .iter()
                .map(|(_, _, eid)| *eid)
                .chain(
                    self.config
                        .include_reverse
                        .then(|| raw_in_edges.iter().map(|(_, _, eid)| *eid))
                        .into_iter()
                        .flatten(),
                )
                .collect();
            all_eids.sort_unstable();
            all_eids.dedup();

            let batch_props = pm
                .get_batch_edge_props(&all_eids, &fetch_names, None)
                .await?;
            for eid in all_eids {
                let vid_key = Vid::from(eid.as_u64());
                if let Some(props) = batch_props.get(&vid_key) {
                    let mut entry = std::collections::HashMap::new();
                    for name in &fetch_names {
                        if let Some(v) = props.get(*name).and_then(uni_common::Value::as_f64) {
                            entry.insert((*name).to_owned(), v);
                        }
                    }
                    if !entry.is_empty() {
                        props_cache.insert(eid, entry);
                    }
                }
            }
        }

        // Missing weight -> 1.0 (traversal default); missing edge property -> NaN
        // (an honest "no value", distinct from a real 0.0 weight).
        let weight_of = |eid: &Eid| -> f64 {
            weight_prop
                .and_then(|w| props_cache.get(eid).and_then(|m| m.get(w)).copied())
                .unwrap_or(1.0)
        };

        // Convert raw edges to weighted edges, filtering to projected vertices,
        // and build the aligned per-edge property columns in the same pass so
        // they line up with `out_edges` before the CSR permutation.
        let mut out_edges: WeightedEdgeList = Vec::with_capacity(raw_out_edges.len());
        let mut out_prop_cols: Vec<Vec<f64>> = edge_prop_names
            .iter()
            .map(|_| Vec::with_capacity(raw_out_edges.len()))
            .collect();
        for (src_slot, dst_vid, eid) in raw_out_edges {
            if let Some(dst_slot) = id_map.to_slot(dst_vid) {
                out_edges.push((src_slot, dst_slot, weight_of(&eid)));
                for (col, name) in out_prop_cols.iter_mut().zip(edge_prop_names) {
                    let v = props_cache
                        .get(&eid)
                        .and_then(|m| m.get(name))
                        .copied()
                        .unwrap_or(f64::NAN);
                    col.push(v);
                }
            }
        }

        let in_edges: WeightedEdgeList = raw_in_edges
            .into_iter()
            .filter_map(|(src_slot, dst_vid, eid)| {
                id_map
                    .to_slot(dst_vid)
                    .map(|dst_slot| (src_slot, dst_slot, weight_of(&eid)))
            })
            .collect();

        let out_edge_props: Vec<(String, Vec<f64>)> =
            edge_prop_names.iter().cloned().zip(out_prop_cols).collect();

        Ok((out_edges, in_edges, out_edge_props))
    }

    /// Fetch the configured per-vertex property columns in vertex-slot order
    /// (issue #151). Returns an empty map when no node properties are requested.
    /// Missing values become NaN (an honest "no value" for the guest tensor).
    async fn collect_node_properties(
        &self,
        id_map: &IdMap,
    ) -> Result<std::collections::HashMap<String, Vec<f64>>> {
        let names = &self.config.node_properties;
        if names.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        // Vids in slot order: column index i corresponds to vertex slot i.
        let vids: Vec<Vid> = id_map.iter().map(|(_, vid)| vid).collect();
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let pm = PropertyManager::new(
            self.storage.clone(),
            self.storage.schema_manager_arc(),
            1000,
        );
        let batch = pm.get_batch_vertex_props(&vids, &name_refs, None).await?;
        let mut cols: std::collections::HashMap<String, Vec<f64>> = names
            .iter()
            .map(|n| (n.clone(), Vec::with_capacity(vids.len())))
            .collect();
        for vid in &vids {
            let props = batch.get(vid);
            for name in names {
                let v = props
                    .and_then(|m| m.get(name))
                    .and_then(uni_common::Value::as_f64)
                    .unwrap_or(f64::NAN);
                cols.get_mut(name)
                    .expect("column pre-inserted for every name")
                    .push(v);
            }
        }
        Ok(cols)
    }
}

/// Row shape carried back from an inner Cypher projection query.
///
/// Node rows carry an `id` column (Int) plus arbitrary other columns
/// that this builder ignores; edge rows carry `source`, `target`
/// (required, Int) plus an optional weight column whose name the caller
/// passes to [`GraphProjection::from_rows`].
pub type ProjectionRow = std::collections::HashMap<String, uni_common::Value>;

impl GraphProjection {
    /// Build a [`GraphProjection`] from inner-query row data.
    ///
    /// Schema requirements:
    /// - `node_rows`: every row must carry an `id` column (Int / UInt /
    ///   any integer-typed value); other columns are ignored.
    /// - `edge_rows`: every row must carry `source` and `target` (both
    ///   Int) and may carry a column named `weight_column` (Float /
    ///   convertible numeric) when the caller passes
    ///   `weight_column = Some(name)`.
    ///
    /// Each `id` is mapped to a dense slot via [`IdMap`]; the same
    /// `IdMap` resolves `source` / `target` ids to slots. Edges whose
    /// endpoints are not in the node set are silently dropped (the
    /// node query is the canonical projection of vertex membership).
    ///
    /// `include_reverse` mirrors the equivalent flag on
    /// [`ProjectionConfig`]: when true, the in-CSR is built alongside
    /// the out-CSR so PageRank-style algorithms can iterate in-neighbors.
    ///
    /// # Errors
    ///
    /// Returns a descriptive error when a required column is missing or
    /// has the wrong type. Edges referencing unknown ids are skipped
    /// (not an error — they may represent edges out of the projection).
    pub fn from_rows(
        node_rows: &[ProjectionRow],
        edge_rows: &[ProjectionRow],
        weight_column: Option<&str>,
        include_reverse: bool,
    ) -> Result<Self> {
        let mut id_map = IdMap::with_capacity(node_rows.len());
        for (i, row) in node_rows.iter().enumerate() {
            let vid_u = row
                .get("id")
                .and_then(value_as_u64)
                .ok_or_else(|| anyhow!("node row {i} missing `id` (Int) column"))?;
            id_map.insert(Vid::new(vid_u));
        }
        let vertex_count = id_map.len();

        let mut out_edges: WeightedEdgeList = Vec::with_capacity(edge_rows.len());
        let mut in_edges: WeightedEdgeList = if include_reverse {
            Vec::with_capacity(edge_rows.len())
        } else {
            Vec::new()
        };
        for (i, row) in edge_rows.iter().enumerate() {
            let src_u = row
                .get("source")
                .and_then(value_as_u64)
                .ok_or_else(|| anyhow!("edge row {i} missing `source` (Int) column"))?;
            let dst_u = row
                .get("target")
                .and_then(value_as_u64)
                .ok_or_else(|| anyhow!("edge row {i} missing `target` (Int) column"))?;
            let weight = if let Some(name) = weight_column {
                row.get(name).and_then(value_as_f64).unwrap_or(1.0)
            } else {
                1.0
            };
            let (Some(src_slot), Some(dst_slot)) = (
                id_map.to_slot(Vid::new(src_u)),
                id_map.to_slot(Vid::new(dst_u)),
            ) else {
                log::debug!(
                    "from_rows: edge endpoint (src={src_u}, dst={dst_u}) not in node id map; dropping"
                );
                continue; // endpoint outside projection — drop silently
            };
            out_edges.push((src_slot, dst_slot, weight));
            if include_reverse {
                in_edges.push((dst_slot, src_slot, weight));
            }
        }

        // NOTE: deliberately *not* calling `id_map.compact()` — the
        // node-row insertion order is whatever the Cypher result
        // returned, which is generally unsorted, and `compact`'s
        // binary-search lookup assumes sorted insertion (see
        // `IdMap::compact` warning). For Cypher / Named projections the
        // memory overhead of keeping the hashmap is negligible relative
        // to the actual algorithm working set.

        // Cypher/Named projections have no PropertyManager-backed columns; any
        // per-vertex/edge tensors a guest wants there must come from query
        // columns (a follow-up), so the property maps are empty here.
        let (out_offsets, out_neighbors, out_weights, _) =
            build_csr(vertex_count, &out_edges, weight_column.is_some(), &[]);
        let (in_offsets, in_neighbors, _, _) = if include_reverse {
            build_csr(vertex_count, &in_edges, false, &[])
        } else {
            (vec![0; vertex_count + 1], Vec::new(), None, Vec::new())
        };

        Ok(GraphProjection {
            vertex_count,
            out_offsets,
            out_neighbors,
            in_offsets,
            in_neighbors,
            out_weights,
            node_properties: std::collections::HashMap::new(),
            edge_properties: std::collections::HashMap::new(),
            id_map,
        })
    }
}

impl GraphProjection {
    /// Builds a projection from a dense, slot-indexed edge list.
    ///
    /// [`from_rows`](Self::from_rows) exists for storage-shaped input — row maps
    /// keyed by external [`Vid`] that must be interned into slots. Synthetic
    /// structure is already dense: a guest's mutable graph arena, frozen
    /// (GraphCompute proposal §13.8), numbers its own slots `0..vertex_count`.
    /// Routing it through row maps would mean fabricating `Vid`s purely to look
    /// them straight back up, so this takes the slots directly and uses an
    /// identity id map.
    ///
    /// Sharing the private `build_csr` helper with `from_rows` is the point: a frozen arena gets
    /// the *same* canonical edge ordering as a stored projection, so every
    /// existing kernel behaves identically on it.
    #[must_use]
    pub fn from_dense_edges(
        vertex_count: usize,
        edges: &[(u32, u32, f64)],
        weighted: bool,
        include_reverse: bool,
    ) -> Self {
        let mut id_map = IdMap::with_capacity(vertex_count);
        for slot in 0..vertex_count {
            id_map.insert(Vid::new(slot as u64));
        }
        let (out_offsets, out_neighbors, out_weights, _) =
            build_csr(vertex_count, edges, weighted, &[]);
        let (in_offsets, in_neighbors) = if include_reverse {
            let rev: Vec<(u32, u32, f64)> = edges.iter().map(|&(s, d, w)| (d, s, w)).collect();
            let (o, n, _, _) = build_csr(vertex_count, &rev, false, &[]);
            (o, n)
        } else {
            (vec![0; vertex_count + 1], Vec::new())
        };
        GraphProjection {
            vertex_count,
            out_offsets,
            out_neighbors,
            in_offsets,
            in_neighbors,
            out_weights,
            node_properties: std::collections::HashMap::new(),
            edge_properties: std::collections::HashMap::new(),
            id_map,
        }
    }
}

fn value_as_u64(v: &uni_common::Value) -> Option<u64> {
    use uni_common::Value;
    match v {
        Value::Int(i) if *i >= 0 => Some(*i as u64),
        Value::Float(f) if f.is_finite() && *f >= 0.0 => Some(*f as u64),
        _ => None,
    }
}

fn value_as_f64(v: &uni_common::Value) -> Option<f64> {
    use uni_common::Value;
    match v {
        Value::Float(f) => Some(*f),
        Value::Int(i) => Some(*i as f64),
        _ => None,
    }
}

/// Build CSR from an edge list, co-permuting weights and any extra per-edge
/// property columns into the same canonical order.
///
/// The final edge order is deterministic-by-construction (GraphCompute proposal
/// §5.3): the upstream adjacency source returns neighbors in `HashMap`
/// iteration order, so edges are bucketed by source and then each row is sorted
/// by `(dst, weight, prop0, prop1, …)` using `f64::total_cmp`. A single
/// permutation drives `neighbors`, `weights`, and every `edge_prop_cols` column
/// so they stay aligned; extending the sort key with the property values keeps
/// parallel edges (same dst+weight) deterministic even when their properties
/// differ. Each column in `edge_prop_cols` has length `edges.len()` and is
/// returned permuted into the same `[E]` CSR order.
fn build_csr(
    vertex_count: usize,
    edges: &[(u32, u32, f64)],
    include_weights: bool,
    edge_prop_cols: &[Vec<f64>],
) -> CsrParts {
    let empty_cols = || -> Vec<Vec<f64>> { edge_prop_cols.iter().map(|_| Vec::new()).collect() };
    if vertex_count == 0 {
        return (vec![0], Vec::new(), None, empty_cols());
    }

    // Count degrees
    let mut degrees = vec![0u32; vertex_count];
    for &(src, _, _) in edges {
        degrees[src as usize] += 1;
    }

    // Build offsets (prefix sum)
    let mut offsets = vec![0u32; vertex_count + 1];
    for i in 0..vertex_count {
        offsets[i + 1] = offsets[i] + degrees[i];
    }

    // Bucket-fill a permutation: perm[csr_position] = original edge index.
    let mut perm = vec![0usize; edges.len()];
    let mut current = offsets.clone();
    for (edge_i, &(src, _, _)) in edges.iter().enumerate() {
        let idx = current[src as usize] as usize;
        perm[idx] = edge_i;
        current[src as usize] += 1;
    }

    // Sort each CSR row's permutation slice into canonical
    // `(dst, weight, props…)` order so identical edge sets always yield
    // byte-identical CSR arrays regardless of adjacency iteration order.
    for row in offsets.windows(2) {
        let (start, end) = (row[0] as usize, row[1] as usize);
        if end - start <= 1 {
            continue;
        }
        perm[start..end].sort_by(|&a, &b| {
            edges[a]
                .1
                .cmp(&edges[b].1)
                .then_with(|| edges[a].2.total_cmp(&edges[b].2))
                .then_with(|| {
                    for col in edge_prop_cols {
                        let ord = col[a].total_cmp(&col[b]);
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                })
        });
    }

    // Materialize every array by applying the single permutation.
    let neighbors: Vec<u32> = perm.iter().map(|&i| edges[i].1).collect();
    let weights = include_weights.then(|| perm.iter().map(|&i| edges[i].2).collect());
    let permuted_cols: Vec<Vec<f64>> = edge_prop_cols
        .iter()
        .map(|col| perm.iter().map(|&i| col[i]).collect())
        .collect();

    (offsets, neighbors, weights, permuted_cols)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_csr() {
        // Triangle: 0 -> 1, 1 -> 2, 2 -> 0
        let edges = vec![(0, 1, 1.0), (1, 2, 1.0), (2, 0, 1.0), (0, 2, 0.5)];
        let (offsets, neighbors, weights, _) = build_csr(3, &edges, true, &[]);

        assert_eq!(offsets, vec![0, 2, 3, 4]);
        // Node 0 has edges to 1 and 2
        assert_eq!(&neighbors[0..2], &[1, 2]);
        if let Some(w) = weights {
            assert_eq!(&w[0..2], &[1.0, 0.5]);
        }
        // Node 1 has edge to 2
        assert_eq!(&neighbors[2..3], &[2]);
        // Node 2 has edge to 0
        assert_eq!(&neighbors[3..4], &[0]);
    }

    #[test]
    fn csr_is_permutation_invariant() {
        // P0-1 (build-level): the same edge set in any per-row insertion order
        // yields byte-identical CSR arrays, killing the HashMap-order
        // nondeterminism inherited from the adjacency source (proposal §5.3).
        // Node 5 has out-neighbors {1, 3, 8} with distinct weights; the two
        // permutations below feed those edges to `build_csr` in different order.
        let perm_a = vec![
            (5, 8, 0.2),
            (5, 1, 0.9),
            (5, 3, 0.5),
            (2, 4, 1.0),
            (2, 0, 1.0),
        ];
        let perm_b = vec![
            (2, 0, 1.0),
            (5, 3, 0.5),
            (5, 8, 0.2),
            (2, 4, 1.0),
            (5, 1, 0.9),
        ];
        let a = build_csr(9, &perm_a, true, &[]);
        let b = build_csr(9, &perm_b, true, &[]);
        assert_eq!(a, b, "CSR must be identical across input permutations");
        // And the canonical order is ascending by dst within each row.
        let (offsets, neighbors, weights, _) = a;
        let row5 = &neighbors[offsets[5] as usize..offsets[6] as usize];
        assert_eq!(row5, &[1, 3, 8]);
        let w = weights.expect("weights requested");
        let w5 = &w[offsets[5] as usize..offsets[6] as usize];
        assert_eq!(w5, &[0.9, 0.5, 0.2]);
    }

    #[test]
    fn csr_rows_sorted_without_weights() {
        // The unweighted path sorts neighbor slots too (uses `sort_unstable`).
        let edges = vec![(0, 7, 0.0), (0, 2, 0.0), (0, 4, 0.0), (0, 2, 0.0)];
        let (offsets, neighbors, _, _) = build_csr(1, &edges, false, &[]);
        let row = &neighbors[offsets[0] as usize..offsets[1] as usize];
        assert_eq!(row, &[2, 2, 4, 7]);
    }

    #[test]
    fn csr_co_permutes_edge_property_columns() {
        // Issue #151: an edge-property column must ride the same permutation as
        // neighbors + weights, so property[k] still describes CSR out-edge k
        // after the canonical (dst, weight) row sort.
        // Node 5 -> {8 w0.2, 1 w0.9, 3 w0.5}; the prop column mirrors dst * 10.
        let edges = vec![(5u32, 8u32, 0.2), (5, 1, 0.9), (5, 3, 0.5)];
        let prop = vec![vec![80.0, 10.0, 30.0]]; // aligned to `edges` order
        let (offsets, neighbors, weights, cols) = build_csr(9, &edges, true, &prop);
        let (s, e) = (offsets[5] as usize, offsets[6] as usize);
        // Canonical order by dst -> [1, 3, 8].
        assert_eq!(&neighbors[s..e], &[1, 3, 8]);
        assert_eq!(&weights.unwrap()[s..e], &[0.9, 0.5, 0.2]);
        // The property column is permuted identically: dst * 10 for [1, 3, 8].
        assert_eq!(&cols[0][s..e], &[10.0, 30.0, 80.0]);
    }
}
