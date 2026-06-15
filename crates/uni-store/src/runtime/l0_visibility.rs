// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! L0 Visibility Chain Abstraction
//!
//! This module provides helper functions for traversing the three-tier L0 buffer
//! hierarchy in a consistent manner. The visibility chain is:
//!
//! 1. Transaction L0 (newest, transaction-local mutations)
//! 2. Main L0 (current in-memory buffer)
//! 3. Pending flush L0s (oldest to newest, being flushed to L1)
//!
//! These helpers eliminate repeated nested conditionals for L0 lookups,
//! reducing cognitive complexity in property_manager.rs.

use crate::runtime::context::QueryContext;
use crate::runtime::l0::L0Buffer;
use std::collections::HashMap;
use uni_common::Properties;
use uni_common::Value;
use uni_common::core::id::{Eid, Vid};

/// Check if a vertex is deleted in the L0 chain.
/// Returns true if a tombstone is found at any layer.
pub fn is_vertex_deleted(vid: Vid, ctx: Option<&QueryContext>) -> bool {
    let ctx = match ctx {
        Some(c) => c,
        None => return false,
    };
    // A write-tx whose RMW decision turns on a vertex's existence/liveness must
    // record that observation, or a concurrent delete/insert escapes the SSI
    // antidependency check (write-skew). No-op outside a write-tx. (review H1)
    record_vertex_read(ctx, vid);

    // Check transaction L0 first (newest)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if tx_l0.vertex_tombstones.contains(&vid) {
            return true;
        }
    }

    // Check main L0
    {
        let l0 = ctx.l0.read();
        if l0.vertex_tombstones.contains(&vid) {
            return true;
        }
    }

    // Check pending flush L0s (newest first for early exit)
    for pending_l0_arc in ctx.pending_flush_l0s.iter().rev() {
        let pending_l0 = pending_l0_arc.read();
        if pending_l0.vertex_tombstones.contains(&vid) {
            return true;
        }
    }

    false
}

/// Check if an edge is deleted in the L0 chain.
/// Returns true if a tombstone is found at any layer.
pub fn is_edge_deleted(eid: Eid, ctx: Option<&QueryContext>) -> bool {
    let ctx = match ctx {
        Some(c) => c,
        None => return false,
    };
    // See is_vertex_deleted: liveness observations under a write-tx are part of
    // the SSI read-set. No-op outside a write-tx. (review H1)
    record_edge_read(ctx, eid);

    // Check transaction L0 first (newest)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if tx_l0.tombstones.contains_key(&eid) {
            return true;
        }
    }

    // Check main L0
    {
        let l0 = ctx.l0.read();
        if l0.tombstones.contains_key(&eid) {
            return true;
        }
    }

    // Check pending flush L0s (newest first for early exit)
    for pending_l0_arc in ctx.pending_flush_l0s.iter().rev() {
        let pending_l0 = pending_l0_arc.read();
        if pending_l0.tombstones.contains_key(&eid) {
            return true;
        }
    }

    false
}

/// Records a vertex read into the transaction's SSI read-set, when tracking.
///
/// Best-effort, item-level: records the queried id whether or not it currently
/// exists, so a concurrent write to it is detected as an antidependency at
/// commit. Read-set coverage spans the primary keyed read paths; see the
/// proposal for the (in-progress) full-coverage follow-up.
fn record_vertex_read(ctx: &QueryContext, vid: Vid) {
    if let Some(tx_l0_arc) = &ctx.transaction_l0
        && let Some(read_set) = &tx_l0_arc.read().occ_read_set
    {
        read_set.lock().vertices.insert(vid);
    }
}

/// Records an edge read into the transaction's SSI read-set, when tracking.
///
/// Mirror of [`record_vertex_read`] for edges; best-effort, item-level. No-op
/// unless this is a read-write transaction (`occ_read_set` is `Some` only then),
/// so read-only and analytical queries pay nothing.
fn record_edge_read(ctx: &QueryContext, eid: Eid) {
    if let Some(tx_l0_arc) = &ctx.transaction_l0
        && let Some(read_set) = &tx_l0_arc.read().occ_read_set
    {
        read_set.lock().edges.insert(eid);
    }
}

/// Look up a vertex property in the L0 chain.
/// Returns the value if found, or None if not present in any L0 buffer.
/// Does NOT check tombstones - caller should check `is_vertex_deleted` first.
pub fn lookup_vertex_prop(vid: Vid, prop: &str, ctx: Option<&QueryContext>) -> Option<Value> {
    let ctx = ctx?;
    record_vertex_read(ctx, vid);

    // Check transaction L0 first (newest)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if let Some(props) = tx_l0.vertex_properties.get(&vid)
            && let Some(val) = props.get(prop)
        {
            return Some(val.clone());
        }
    }

    // Check main L0
    {
        let l0 = ctx.l0.read();
        if let Some(props) = l0.vertex_properties.get(&vid)
            && let Some(val) = props.get(prop)
        {
            return Some(val.clone());
        }
    }

    // Check pending flush L0s (newest first)
    for pending_l0_arc in ctx.pending_flush_l0s.iter().rev() {
        let pending_l0 = pending_l0_arc.read();
        if let Some(props) = pending_l0.vertex_properties.get(&vid)
            && let Some(val) = props.get(prop)
        {
            return Some(val.clone());
        }
    }

    None
}

/// Look up an edge property in the L0 chain.
/// Returns the value if found, or None if not present in any L0 buffer.
/// Does NOT check tombstones - caller should check `is_edge_deleted` first.
pub fn lookup_edge_prop(eid: Eid, prop: &str, ctx: Option<&QueryContext>) -> Option<Value> {
    let ctx = ctx?;
    record_edge_read(ctx, eid);

    // Check transaction L0 first (newest)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if let Some(props) = tx_l0.edge_properties.get(&eid)
            && let Some(val) = props.get(prop)
        {
            return Some(val.clone());
        }
    }

    // Check main L0
    {
        let l0 = ctx.l0.read();
        if let Some(props) = l0.edge_properties.get(&eid)
            && let Some(val) = props.get(prop)
        {
            return Some(val.clone());
        }
    }

    // Check pending flush L0s (newest first)
    for pending_l0_arc in ctx.pending_flush_l0s.iter().rev() {
        let pending_l0 = pending_l0_arc.read();
        if let Some(props) = pending_l0.edge_properties.get(&eid)
            && let Some(val) = props.get(prop)
        {
            return Some(val.clone());
        }
    }

    None
}

/// Accumulate all vertex properties from the L0 chain.
/// Properties are merged from oldest to newest, with newer values overwriting older ones.
/// Returns None if the vertex has no properties in the L0 chain.
pub fn accumulate_vertex_props(vid: Vid, ctx: Option<&QueryContext>) -> Option<Properties> {
    let ctx = ctx?;
    record_vertex_read(ctx, vid);

    let mut result: Option<Properties> = None;

    // Start from pending flush L0s (oldest first)
    for pending_l0_arc in ctx.pending_flush_l0s.iter() {
        let pending_l0 = pending_l0_arc.read();
        if let Some(props) = pending_l0.vertex_properties.get(&vid) {
            let entry = result.get_or_insert_with(HashMap::new);
            for (k, v) in props {
                entry.insert(k.clone(), v.clone());
            }
        }
    }

    // Then main L0
    {
        let l0 = ctx.l0.read();
        if let Some(props) = l0.vertex_properties.get(&vid) {
            let entry = result.get_or_insert_with(HashMap::new);
            for (k, v) in props {
                entry.insert(k.clone(), v.clone());
            }
        }
    }

    // Finally transaction L0 (newest, highest priority)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if let Some(props) = tx_l0.vertex_properties.get(&vid) {
            let entry = result.get_or_insert_with(HashMap::new);
            for (k, v) in props {
                entry.insert(k.clone(), v.clone());
            }
        }
    }

    result
}

/// Accumulate all edge properties from the L0 chain.
/// Properties are merged from oldest to newest, with newer values overwriting older ones.
/// Returns None if the edge has no properties in the L0 chain.
pub fn accumulate_edge_props(eid: Eid, ctx: Option<&QueryContext>) -> Option<Properties> {
    let ctx = ctx?;
    record_edge_read(ctx, eid);

    let mut result: Option<Properties> = None;

    // Start from pending flush L0s (oldest first)
    for pending_l0_arc in ctx.pending_flush_l0s.iter() {
        let pending_l0 = pending_l0_arc.read();
        if let Some(props) = pending_l0.edge_properties.get(&eid) {
            let entry = result.get_or_insert_with(HashMap::new);
            for (k, v) in props {
                entry.insert(k.clone(), v.clone());
            }
        }
    }

    // Then main L0
    {
        let l0 = ctx.l0.read();
        if let Some(props) = l0.edge_properties.get(&eid) {
            let entry = result.get_or_insert_with(HashMap::new);
            for (k, v) in props {
                entry.insert(k.clone(), v.clone());
            }
        }
    }

    // Finally transaction L0 (newest, highest priority)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if let Some(props) = tx_l0.edge_properties.get(&eid) {
            let entry = result.get_or_insert_with(HashMap::new);
            for (k, v) in props {
                entry.insert(k.clone(), v.clone());
            }
        }
    }

    result
}

/// Visit all L0 buffers in visibility order (newest to oldest).
/// The visitor function receives a read guard to each L0 buffer.
/// Iteration stops early if the visitor returns `true`.
pub fn visit_l0_buffers<F>(ctx: Option<&QueryContext>, mut visitor: F) -> bool
where
    F: FnMut(&L0Buffer) -> bool,
{
    let ctx = match ctx {
        Some(c) => c,
        None => return false,
    };

    // Transaction L0 first (newest)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if visitor(&tx_l0) {
            return true;
        }
    }

    // Main L0
    {
        let l0 = ctx.l0.read();
        if visitor(&l0) {
            return true;
        }
    }

    // Pending flush L0s (newest first)
    for pending_l0_arc in ctx.pending_flush_l0s.iter().rev() {
        let pending_l0 = pending_l0_arc.read();
        if visitor(&pending_l0) {
            return true;
        }
    }

    false
}

/// Overlay L0 properties onto a batch result.
/// This applies L0 modifications to a set of vertices loaded from storage.
pub fn overlay_vertex_batch(
    vid_to_idx: &HashMap<Vid, usize>,
    result: &mut [Properties],
    deleted: &mut [bool],
    ctx: Option<&QueryContext>,
) {
    let ctx = match ctx {
        Some(c) => c,
        None => return,
    };

    // Apply pending flush L0s (oldest first)
    for pending_l0_arc in ctx.pending_flush_l0s.iter() {
        let pending_l0 = pending_l0_arc.read();
        overlay_vertex_from_l0(&pending_l0, vid_to_idx, result, deleted);
    }

    // Apply main L0
    {
        let l0 = ctx.l0.read();
        overlay_vertex_from_l0(&l0, vid_to_idx, result, deleted);
    }

    // Apply transaction L0 (newest, highest priority)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        overlay_vertex_from_l0(&tx_l0, vid_to_idx, result, deleted);
    }
}

/// Helper to overlay a single L0 buffer onto the batch result.
fn overlay_vertex_from_l0(
    l0: &L0Buffer,
    vid_to_idx: &HashMap<Vid, usize>,
    result: &mut [Properties],
    deleted: &mut [bool],
) {
    // Apply tombstones
    for vid in &l0.vertex_tombstones {
        if let Some(&idx) = vid_to_idx.get(vid) {
            deleted[idx] = true;
        }
    }

    // Apply property updates
    for (vid, props) in &l0.vertex_properties {
        if let Some(&idx) = vid_to_idx.get(vid) {
            for (k, v) in props {
                result[idx].insert(k.clone(), v.clone());
            }
        }
    }
}

/// Overlay L0 edge properties onto a batch result.
pub fn overlay_edge_batch(
    eid_to_idx: &HashMap<Eid, usize>,
    result: &mut [Properties],
    deleted: &mut [bool],
    ctx: Option<&QueryContext>,
) {
    let ctx = match ctx {
        Some(c) => c,
        None => return,
    };

    // Apply pending flush L0s (oldest first)
    for pending_l0_arc in ctx.pending_flush_l0s.iter() {
        let pending_l0 = pending_l0_arc.read();
        overlay_edge_from_l0(&pending_l0, eid_to_idx, result, deleted);
    }

    // Apply main L0
    {
        let l0 = ctx.l0.read();
        overlay_edge_from_l0(&l0, eid_to_idx, result, deleted);
    }

    // Apply transaction L0 (newest, highest priority)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        overlay_edge_from_l0(&tx_l0, eid_to_idx, result, deleted);
    }
}

/// Check if a vertex exists in any L0 buffer (has topology entry).
/// This is used to distinguish between "vertex doesn't exist" and "vertex exists but has no properties".
pub fn vertex_exists_in_l0(vid: Vid, ctx: Option<&QueryContext>) -> bool {
    let ctx = match ctx {
        Some(c) => c,
        None => return false,
    };
    // Existence is an SSI-relevant observation under a write-tx. (review H1)
    record_vertex_read(ctx, vid);

    // Check transaction L0 first
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if tx_l0.vertex_properties.contains_key(&vid) {
            return true;
        }
    }

    // Check main L0
    {
        let l0 = ctx.l0.read();
        if l0.vertex_properties.contains_key(&vid) {
            return true;
        }
    }

    // Check pending flush L0s
    for pending_l0_arc in ctx.pending_flush_l0s.iter() {
        let pending_l0 = pending_l0_arc.read();
        if pending_l0.vertex_properties.contains_key(&vid) {
            return true;
        }
    }

    false
}

/// Get the labels for a vertex from the L0 chain.
/// Returns labels from the most recent L0 buffer that has the vertex.
/// Returns an empty vec if the vertex is not in any L0 buffer.
pub fn get_vertex_labels(vid: Vid, ctx: &QueryContext) -> Vec<String> {
    // A label-predicated RMW (e.g. `MATCH (n) WHERE n:Active SET ...`) depends
    // on this read; record it so a concurrent label mutation conflicts. No-op
    // outside a write-tx. (review H1)
    record_vertex_read(ctx, vid);

    // Check transaction L0 first (newest)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if let Some(labels) = tx_l0.get_vertex_labels(vid) {
            return labels.to_vec();
        }
    }

    // Check main L0
    {
        let l0 = ctx.l0.read();
        if let Some(labels) = l0.get_vertex_labels(vid) {
            return labels.to_vec();
        }
    }

    // Check pending flush L0s (newest first)
    for pending_l0_arc in ctx.pending_flush_l0s.iter().rev() {
        let pending_l0 = pending_l0_arc.read();
        if let Some(labels) = pending_l0.get_vertex_labels(vid) {
            return labels.to_vec();
        }
    }

    Vec::new()
}

/// Get vertex labels, distinguishing "not in L0" from "in L0 with no labels".
/// Returns `None` if vertex is not in any L0 buffer.
/// Returns `Some(labels)` if vertex is in L0 (may be empty for unlabeled nodes).
pub fn get_vertex_labels_optional(vid: Vid, ctx: &QueryContext) -> Option<Vec<String>> {
    // Label observation under a write-tx is part of the SSI read-set. (review H1)
    record_vertex_read(ctx, vid);
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if let Some(labels) = tx_l0.get_vertex_labels(vid) {
            return Some(labels.to_vec());
        }
    }
    {
        let l0 = ctx.l0.read();
        if let Some(labels) = l0.get_vertex_labels(vid) {
            return Some(labels.to_vec());
        }
    }
    for pending_l0_arc in ctx.pending_flush_l0s.iter().rev() {
        let pending_l0 = pending_l0_arc.read();
        if let Some(labels) = pending_l0.get_vertex_labels(vid) {
            return Some(labels.to_vec());
        }
    }
    None
}

/// Get the edge type for an edge from the L0 chain.
/// Returns the type from the most recent L0 buffer that has the edge.
/// Returns None if the edge is not in any L0 buffer.
pub fn get_edge_type(eid: Eid, ctx: &QueryContext) -> Option<String> {
    // Edge-type observation under a write-tx is part of the SSI read-set. (review H1)
    record_edge_read(ctx, eid);

    // Check transaction L0 first (newest)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if let Some(edge_type) = tx_l0.get_edge_type(eid) {
            return Some(edge_type.to_string());
        }
    }

    // Check main L0
    {
        let l0 = ctx.l0.read();
        if let Some(edge_type) = l0.get_edge_type(eid) {
            return Some(edge_type.to_string());
        }
    }

    // Check pending flush L0s (newest first)
    for pending_l0_arc in ctx.pending_flush_l0s.iter().rev() {
        let pending_l0 = pending_l0_arc.read();
        if let Some(edge_type) = pending_l0.get_edge_type(eid) {
            return Some(edge_type.to_string());
        }
    }

    None
}

/// Resolve an edge's STORED `(src_vid, dst_vid)` endpoints from the L0 chain.
///
/// Returns the endpoints in their stored (start -> end) orientation, regardless
/// of the direction a query traversed the edge. Walks the L0 visibility chain
/// (transaction L0 -> main L0 -> pending-flush L0s) and returns the first match.
/// Returns `None` if the edge is not resident in any L0 buffer (e.g. it has been
/// flushed to durable storage), in which case callers must fall back to storage.
///
/// # Examples
///
/// ```ignore
/// if let Some((src, dst)) = get_edge_endpoints(eid, &ctx) {
///     // src/dst reflect storage order, not traversal order.
/// }
/// ```
pub fn get_edge_endpoints(eid: Eid, ctx: &QueryContext) -> Option<(Vid, Vid)> {
    // An RMW that reaches an edge's endpoints off the traverse path depends on
    // this read; record it so a concurrent endpoint mutation conflicts. No-op
    // outside a write-tx. (review H1)
    record_edge_read(ctx, eid);

    // Check transaction L0 first (newest)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if let Some(endpoints) = tx_l0.get_edge_endpoints(eid) {
            return Some(endpoints);
        }
    }

    // Check main L0
    {
        let l0 = ctx.l0.read();
        if let Some(endpoints) = l0.get_edge_endpoints(eid) {
            return Some(endpoints);
        }
    }

    // Check pending flush L0s (newest first)
    for pending_l0_arc in ctx.pending_flush_l0s.iter().rev() {
        let pending_l0 = pending_l0_arc.read();
        if let Some(endpoints) = pending_l0.get_edge_endpoints(eid) {
            return Some(endpoints);
        }
    }

    None
}

/// Get the properties for a vertex from the L0 chain.
/// Returns properties from the most recent L0 buffer that has the vertex.
/// Returns None if the vertex is not in any L0 buffer.
pub fn get_vertex_properties(vid: Vid, ctx: &QueryContext) -> Option<uni_common::Properties> {
    // Check transaction L0 first (newest)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if let Some(props) = tx_l0.vertex_properties.get(&vid) {
            return Some(props.clone());
        }
    }

    // Check main L0
    {
        let l0 = ctx.l0.read();
        if let Some(props) = l0.vertex_properties.get(&vid) {
            return Some(props.clone());
        }
    }

    // Check pending flush L0s (newest first)
    for pending_l0_arc in ctx.pending_flush_l0s.iter().rev() {
        let pending_l0 = pending_l0_arc.read();
        if let Some(props) = pending_l0.vertex_properties.get(&vid) {
            return Some(props.clone());
        }
    }

    None
}

/// Get the properties for an edge from the L0 chain.
/// Returns properties from the most recent L0 buffer that has the edge.
/// Returns None if the edge is not in any L0 buffer.
pub fn get_edge_properties(eid: Eid, ctx: &QueryContext) -> Option<uni_common::Properties> {
    // Check transaction L0 first (newest)
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if let Some(props) = tx_l0.edge_properties.get(&eid) {
            return Some(props.clone());
        }
    }

    // Check main L0
    {
        let l0 = ctx.l0.read();
        if let Some(props) = l0.edge_properties.get(&eid) {
            return Some(props.clone());
        }
    }

    // Check pending flush L0s (newest first)
    for pending_l0_arc in ctx.pending_flush_l0s.iter().rev() {
        let pending_l0 = pending_l0_arc.read();
        if let Some(props) = pending_l0.edge_properties.get(&eid) {
            return Some(props.clone());
        }
    }

    None
}

/// Check if an edge exists in any L0 buffer (has topology entry in edge_endpoints).
/// This is used to distinguish between "edge doesn't exist" and "edge exists but has no properties".
pub fn edge_exists_in_l0(eid: Eid, ctx: Option<&QueryContext>) -> bool {
    let ctx = match ctx {
        Some(c) => c,
        None => return false,
    };

    // Check transaction L0 first
    if let Some(tx_l0_arc) = &ctx.transaction_l0 {
        let tx_l0 = tx_l0_arc.read();
        if tx_l0.edge_endpoints.contains_key(&eid) {
            return true;
        }
    }

    // Check main L0
    {
        let l0 = ctx.l0.read();
        if l0.edge_endpoints.contains_key(&eid) {
            return true;
        }
    }

    // Check pending flush L0s
    for pending_l0_arc in ctx.pending_flush_l0s.iter() {
        let pending_l0 = pending_l0_arc.read();
        if pending_l0.edge_endpoints.contains_key(&eid) {
            return true;
        }
    }

    false
}

/// Helper to overlay a single L0 buffer's edge data onto the batch result.
fn overlay_edge_from_l0(
    l0: &L0Buffer,
    eid_to_idx: &HashMap<Eid, usize>,
    result: &mut [Properties],
    deleted: &mut [bool],
) {
    // Apply tombstones
    for eid in l0.tombstones.keys() {
        if let Some(&idx) = eid_to_idx.get(eid) {
            deleted[idx] = true;
        }
    }

    // Apply property updates
    for (eid, props) in &l0.edge_properties {
        if let Some(&idx) = eid_to_idx.get(eid) {
            for (k, v) in props {
                result[idx].insert(k.clone(), v.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::l0::L0Buffer;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use uni_common::Value;

    fn make_ctx_with_l0(l0: L0Buffer) -> QueryContext {
        QueryContext::new(Arc::new(RwLock::new(l0)))
    }

    #[test]
    fn test_is_vertex_deleted_empty_ctx() {
        assert!(!is_vertex_deleted(Vid::from(1), None));
    }

    #[test]
    fn test_is_vertex_deleted_in_main_l0() {
        let mut l0 = L0Buffer::new(0, None);
        l0.vertex_tombstones.insert(Vid::from(1));
        let ctx = make_ctx_with_l0(l0);

        assert!(is_vertex_deleted(Vid::from(1), Some(&ctx)));
        assert!(!is_vertex_deleted(Vid::from(2), Some(&ctx)));
    }

    #[test]
    fn test_lookup_vertex_prop_in_main_l0() {
        let mut l0 = L0Buffer::new(0, None);
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        l0.vertex_properties.insert(Vid::from(1), props);
        let ctx = make_ctx_with_l0(l0);

        let result = lookup_vertex_prop(Vid::from(1), "name", Some(&ctx));
        assert_eq!(result, Some(Value::String("Alice".to_string())));

        let result = lookup_vertex_prop(Vid::from(1), "age", Some(&ctx));
        assert_eq!(result, None);
    }

    #[test]
    fn test_accumulate_vertex_props() {
        let mut l0 = L0Buffer::new(0, None);
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        l0.vertex_properties.insert(Vid::from(1), props);
        let ctx = make_ctx_with_l0(l0);

        let result = accumulate_vertex_props(Vid::from(1), Some(&ctx));
        assert!(result.is_some());
        let props = result.unwrap();
        assert_eq!(props.get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(props.get("age"), Some(&Value::Int(30)));
    }

    #[test]
    fn test_transaction_l0_takes_precedence() {
        // Main L0 has older value
        let mut main_l0 = L0Buffer::new(0, None);
        let mut main_props = HashMap::new();
        main_props.insert("name".to_string(), Value::String("Alice".to_string()));
        main_l0.vertex_properties.insert(Vid::from(1), main_props);

        // Transaction L0 has newer value
        let mut tx_l0 = L0Buffer::new(0, None);
        let mut tx_props = HashMap::new();
        tx_props.insert("name".to_string(), Value::String("Bob".to_string()));
        tx_l0.vertex_properties.insert(Vid::from(1), tx_props);

        let ctx = QueryContext::new_with_tx(
            Arc::new(RwLock::new(main_l0)),
            Some(Arc::new(RwLock::new(tx_l0))),
        );

        // Single property lookup should return transaction value
        let result = lookup_vertex_prop(Vid::from(1), "name", Some(&ctx));
        assert_eq!(result, Some(Value::String("Bob".to_string())));

        // Accumulated props should also have transaction value
        let all_props = accumulate_vertex_props(Vid::from(1), Some(&ctx));
        assert_eq!(
            all_props.unwrap().get("name"),
            Some(&Value::String("Bob".to_string()))
        );
    }

    /// H1: label / existence / liveness / edge-endpoint reads consulted under a
    /// write-tx must land in the SSI read-set — otherwise a concurrent mutation
    /// to the observed item escapes the antidependency check (write-skew).
    #[test]
    fn test_keyed_reads_recorded_in_ssi_read_set() {
        use crate::runtime::l0::OccReadSet;

        let main_l0 = L0Buffer::new(0, None);
        let mut tx_l0 = L0Buffer::new(0, None);
        let read_set = Arc::new(parking_lot::Mutex::new(OccReadSet::default()));
        tx_l0.occ_read_set = Some(read_set.clone());
        let ctx = QueryContext::new_with_tx(
            Arc::new(RwLock::new(main_l0)),
            Some(Arc::new(RwLock::new(tx_l0))),
        );

        // Ids need not exist — recording is existence-agnostic, exactly so a
        // concurrent *insert* of a currently-absent id is also caught.
        let _ = is_vertex_deleted(Vid::from(10), Some(&ctx));
        let _ = vertex_exists_in_l0(Vid::from(11), Some(&ctx));
        let _ = get_vertex_labels(Vid::from(12), &ctx);
        let _ = get_vertex_labels_optional(Vid::from(13), &ctx);
        let _ = is_edge_deleted(Eid::from(20), Some(&ctx));
        let _ = get_edge_type(Eid::from(21), &ctx);
        let _ = get_edge_endpoints(Eid::from(22), &ctx);

        let guard = read_set.lock();
        for v in [10u64, 11, 12, 13] {
            assert!(
                guard.vertices.contains(&Vid::from(v)),
                "vertex {v} read was not recorded in the SSI read-set"
            );
        }
        for e in [20u64, 21, 22] {
            assert!(
                guard.edges.contains(&Eid::from(e)),
                "edge {e} read was not recorded in the SSI read-set"
            );
        }
    }

    /// The recording is a no-op outside a write-tx (no `occ_read_set`), so
    /// read-only / analytical queries pay nothing.
    #[test]
    fn test_keyed_reads_noop_without_read_set() {
        let mut l0 = L0Buffer::new(0, None);
        l0.vertex_tombstones.insert(Vid::from(1));
        let ctx = make_ctx_with_l0(l0);
        // No transaction_l0 / occ_read_set: must not panic and must behave as before.
        assert!(is_vertex_deleted(Vid::from(1), Some(&ctx)));
        assert!(get_vertex_labels(Vid::from(2), &ctx).is_empty());
    }
}
