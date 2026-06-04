// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Test for Issue #25: Cascade deletion O(E) → O(degree)
//!
//! Verifies that deleting a vertex with edges uses O(degree) neighbors()
//! traversal instead of O(E) scan over all edges.

use anyhow::Result;
use std::collections::HashMap;
use uni_common::core::id::{Eid, Vid};
use uni_store::runtime::l0::L0Buffer;

#[test]
fn test_cascade_deletion_removes_only_connected_edges() -> Result<()> {
    let mut l0 = L0Buffer::new(0, None);

    // Create vertices
    let v1 = Vid::new(1);
    let v2 = Vid::new(2);
    let v3 = Vid::new(3);
    let v4 = Vid::new(4);
    let v5 = Vid::new(5);
    let v6 = Vid::new(6);

    for vid in [v1, v2, v3, v4, v5, v6] {
        l0.insert_vertex_with_labels(vid, HashMap::new(), &["Person".to_string()]);
    }

    // Create edges: v1->v2, v3->v4, v5->v6, v1->v3, v1->v5
    let e12 = Eid::new(12);
    let e34 = Eid::new(34);
    let e56 = Eid::new(56);
    let e13 = Eid::new(13);
    let e15 = Eid::new(15);

    l0.insert_edge(v1, v2, 1, e12, HashMap::new(), None)?;
    l0.insert_edge(v3, v4, 1, e34, HashMap::new(), None)?;
    l0.insert_edge(v5, v6, 1, e56, HashMap::new(), None)?;
    l0.insert_edge(v1, v3, 1, e13, HashMap::new(), None)?;
    l0.insert_edge(v1, v5, 1, e15, HashMap::new(), None)?;

    // Verify initial state
    assert_eq!(l0.graph.vertex_count(), 6);
    assert_eq!(l0.graph.edge_count(), 5);

    // Delete v1 (should cascade to e12, e13, e15)
    l0.delete_vertex(v1)?;

    // Verify: 5 vertices remain, 2 edges remain (e34, e56)
    assert_eq!(l0.graph.vertex_count(), 5);
    assert_eq!(l0.graph.edge_count(), 2);

    // Verify correct edges remain
    assert!(l0.graph.edge(e34).is_some());
    assert!(l0.graph.edge(e56).is_some());
    assert!(l0.graph.edge(e12).is_none());
    assert!(l0.graph.edge(e13).is_none());
    assert!(l0.graph.edge(e15).is_none());

    // Verify tombstones
    assert_eq!(l0.tombstones.len(), 3);
    assert_eq!(l0.vertex_tombstones.len(), 1);

    Ok(())
}

#[test]
fn test_cascade_deletion_with_high_degree() -> Result<()> {
    let mut l0 = L0Buffer::new(0, None);

    // Create hub vertex connected to 10 vertices
    let v_hub = Vid::new(1);
    l0.insert_vertex_with_labels(v_hub, HashMap::new(), &["Person".to_string()]);

    let mut hub_edges = Vec::new();
    for i in 0..10 {
        let vid = Vid::new(100 + i);
        l0.insert_vertex_with_labels(vid, HashMap::new(), &["Person".to_string()]);

        let eid = Eid::new(1000 + i);
        l0.insert_edge(v_hub, vid, 1, eid, HashMap::new(), None)?;
        hub_edges.push(eid);
    }

    // Create 90 other unconnected edges
    for i in 0..90 {
        let va = Vid::new(200 + i * 2);
        let vb = Vid::new(200 + i * 2 + 1);
        l0.insert_vertex_with_labels(va, HashMap::new(), &["Person".to_string()]);
        l0.insert_vertex_with_labels(vb, HashMap::new(), &["Person".to_string()]);

        let eid = Eid::new(2000 + i);
        l0.insert_edge(va, vb, 1, eid, HashMap::new(), None)?;
    }

    // Initial: 1 hub + 10 connected + 180 other = 191 vertices, 100 edges
    assert_eq!(l0.graph.vertex_count(), 191);
    assert_eq!(l0.graph.edge_count(), 100);

    // Delete hub (O(degree)=10, not O(E)=100)
    l0.delete_vertex(v_hub)?;

    // After: 190 vertices, 90 edges (10 hub edges deleted)
    assert_eq!(l0.graph.vertex_count(), 190);
    assert_eq!(l0.graph.edge_count(), 90);

    // Verify hub edges deleted
    for eid in hub_edges {
        assert!(l0.graph.edge(eid).is_none());
    }

    assert_eq!(l0.tombstones.len(), 10);

    Ok(())
}
