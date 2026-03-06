//! Test for Issue #77: Ghost Vertex Endpoints
//!
//! Verifies that inserting an edge to a deleted vertex returns an error
//! instead of resurrecting the vertex as a "ghost" with no properties.

use anyhow::Result;
use std::collections::HashMap;
use uni_common::core::id::{Eid, Vid};
use uni_store::runtime::l0::L0Buffer;

#[test]
fn test_ghost_vertex_prevention_src() -> Result<()> {
    // Issue #77: Inserting an edge to a deleted source vertex should fail

    let mut l0 = L0Buffer::new(0, None);

    // Create two vertices
    let vid_a = Vid::new(1);
    let vid_b = Vid::new(2);

    l0.insert_vertex_with_labels(vid_a, HashMap::new(), &["Person".to_string()]);
    l0.insert_vertex_with_labels(vid_b, HashMap::new(), &["Person".to_string()]);

    // Delete vertex A
    l0.delete_vertex(vid_a)?;

    // Try to insert an edge from deleted vertex A to B
    let eid = Eid::new(1);
    let result = l0.insert_edge(vid_a, vid_b, 1, eid, HashMap::new(), None);

    // Should fail with error mentioning the deleted source vertex
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("source vertex") && err_msg.contains("has been deleted"),
        "Expected error about deleted source vertex, got: {}",
        err_msg
    );

    Ok(())
}

#[test]
fn test_ghost_vertex_prevention_dst() -> Result<()> {
    // Issue #77: Inserting an edge to a deleted destination vertex should fail

    let mut l0 = L0Buffer::new(0, None);

    // Create two vertices
    let vid_a = Vid::new(1);
    let vid_b = Vid::new(2);

    l0.insert_vertex_with_labels(vid_a, HashMap::new(), &["Person".to_string()]);
    l0.insert_vertex_with_labels(vid_b, HashMap::new(), &["Person".to_string()]);

    // Delete vertex B
    l0.delete_vertex(vid_b)?;

    // Try to insert an edge from A to deleted vertex B
    let eid = Eid::new(1);
    let result = l0.insert_edge(vid_a, vid_b, 1, eid, HashMap::new(), None);

    // Should fail with error mentioning the deleted destination vertex
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("destination vertex") && err_msg.contains("has been deleted"),
        "Expected error about deleted destination vertex, got: {}",
        err_msg
    );

    Ok(())
}

#[test]
fn test_insert_edge_normal_case_still_works() -> Result<()> {
    // Sanity check: normal edge insertion (non-deleted vertices) should still work

    let mut l0 = L0Buffer::new(0, None);

    // Create two vertices
    let vid_a = Vid::new(1);
    let vid_b = Vid::new(2);

    l0.insert_vertex_with_labels(vid_a, HashMap::new(), &["Person".to_string()]);
    l0.insert_vertex_with_labels(vid_b, HashMap::new(), &["Person".to_string()]);

    // Insert edge between live vertices - should succeed
    let eid = Eid::new(1);
    let result = l0.insert_edge(vid_a, vid_b, 1, eid, HashMap::new(), None);

    assert!(result.is_ok(), "Normal edge insertion should succeed");

    Ok(())
}
