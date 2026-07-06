// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for adjacency_manager.rs:381 (finding [4]).
//!
//! `AdjacencyManager::compact()` writes Incoming-direction shadow entries
//! keyed by `ts.src_vid` with neighbor `ts.dst_vid` (the Outgoing
//! convention) instead of swapping to `(dst_vid, src_vid)` the way `warm()`
//! does for the Incoming key. After compaction an incoming time-travel read
//! on the true target vertex misses the previously-alive edge, and an
//! incoming read on the source resurrects a phantom reversed edge.

use uni_common::core::id::{Eid, Vid};
use uni_store::storage::adjacency_manager::AdjacencyManager;
use uni_store::storage::direction::Direction;

#[test]
fn repro_incoming_shadow_miskeyed_after_compaction() {
    let am = AdjacencyManager::new(1 << 20);
    let etype = 1u32;
    let src = Vid::new(0);
    let dst = Vid::new(5);
    let eid = Eid::new(1);

    // v1: insert edge src -> dst, then compact so it lands in the main CSR.
    am.insert_edge(src, dst, eid, etype, 1);
    am.compact();

    // Sanity (control): outgoing time-travel at v1 sees the edge.
    let out_v1 = am.get_neighbors_at_version(src, etype, Direction::Outgoing, 1);
    assert_eq!(
        out_v1,
        vec![(dst, eid)],
        "outgoing at v1 must see edge after compaction"
    );
    // Sanity: incoming time-travel at v1 (before delete) sees the edge on dst.
    let inc_v1_before = am.get_neighbors_at_version(dst, etype, Direction::Incoming, 1);
    assert_eq!(
        inc_v1_before,
        vec![(src, eid)],
        "incoming on dst at v1 must see edge before delete"
    );

    // v2: delete the edge, then compact so the tombstone moves into Shadow CSR.
    am.add_tombstone(eid, src, dst, etype, 2);
    am.compact();

    // Control: outgoing time-travel at v1 still resurrects the edge (unaffected).
    let out_v1_after = am.get_neighbors_at_version(src, etype, Direction::Outgoing, 1);
    assert_eq!(
        out_v1_after,
        vec![(dst, eid)],
        "outgoing time-travel at v1 must still resurrect (control)"
    );

    // FIXED (adjacency_manager.rs): the Incoming shadow entry is keyed by dst
    // (with neighbor src), so incoming time-travel on the TRUE target (dst) at v1
    // — before the v2 delete — correctly resurrects the edge.
    let inc_v1_dst = am.get_neighbors_at_version(dst, etype, Direction::Incoming, 1);
    assert_eq!(
        inc_v1_dst,
        vec![(src, eid)],
        "incoming time-travel on dst at v1 must resurrect the pre-delete edge, got {inc_v1_dst:?}"
    );

    // And there is no phantom reversed edge on the source's Incoming direction.
    let inc_v1_src = am.get_neighbors_at_version(src, etype, Direction::Incoming, 1);
    assert!(
        inc_v1_src.is_empty(),
        "no phantom reversed incoming edge on src, got {inc_v1_src:?}"
    );
}
