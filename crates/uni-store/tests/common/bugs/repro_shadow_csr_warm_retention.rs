// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for unbounded shadow-CSR retention (Tier 1.2).
//!
//! `AdjacencyManager::warm` pushes a `ShadowEdge` for every `op == 1` row it
//! scans out of the L1 delta. Unlike `warm_coalesced` it has no `has_csr`
//! short-circuit, so each warm of the same `(edge_type, direction)` re-pushed
//! the entire delete history. Growth was unbounded in the number of *warms*,
//! not in the number of deletes — a sharper leak than the one the cleanup audit
//! recorded, which was bounded at roughly two entries per deleted edge.
//!
//! It was also invisible: `AdjacencyManager::memory_usage` reads
//! `current_bytes`, which is only ever mutated on main-CSR paths, so shadow
//! growth could never trip `max_bytes`.
//!
//! Reads dedupe by `Eid` downstream, which is why this surfaced only as memory
//! rather than as duplicated neighbours.

#![cfg(feature = "lance-backend")]

use uni_common::core::id::{Eid, Vid};
use uni_store::storage::direction::Direction;
use uni_store::storage::shadow_csr::{ShadowCsr, ShadowEdge};

fn edge(eid: u64) -> ShadowEdge {
    ShadowEdge {
        neighbor_vid: Vid::new(2),
        eid: Eid::new(eid),
        edge_type: 1,
        created_version: 0,
        deleted_version: 10,
    }
}

/// Re-recording the same deletion must not grow retention.
#[test]
fn repeated_warm_does_not_duplicate_shadow_entries() {
    let shadow = ShadowCsr::new();
    let src = Vid::new(1);

    shadow.add_deleted_edge(src, edge(100), Direction::Outgoing);
    let after_first = shadow.entry_count();

    // Non-zero first, or the assertion below would be a trivial 0 == 0.
    assert_eq!(after_first, 1, "the first record must land");

    // Two further warms replaying the same delete row.
    shadow.add_deleted_edge(src, edge(100), Direction::Outgoing);
    shadow.add_deleted_edge(src, edge(100), Direction::Outgoing);

    assert_eq!(
        shadow.entry_count(),
        after_first,
        "re-warming must not re-push a deletion already recorded"
    );
}

/// Inverse guard: a genuinely new deletion must still be recorded.
///
/// A fix that skipped the whole warm, rather than deduping by `Eid`, would pass
/// the test above and fail this one — and would lose adjacency correctness for
/// edges deleted after the first warm.
#[test]
fn a_new_deletion_is_still_recorded_after_an_earlier_warm() {
    let shadow = ShadowCsr::new();
    let src = Vid::new(1);

    shadow.add_deleted_edge(src, edge(100), Direction::Outgoing);
    shadow.add_deleted_edge(src, edge(101), Direction::Outgoing);

    assert_eq!(shadow.entry_count(), 2);
    let alive = shadow.get_entries_at_version(src, 1, Direction::Outgoing, 5);
    assert_eq!(alive.len(), 2, "both edges are alive at version 5");
}

/// Retention is now observable, which it was not before.
#[test]
fn shadow_bytes_are_reported() {
    let shadow = ShadowCsr::new();
    assert_eq!(shadow.approx_bytes(), 0);
    shadow.add_deleted_edge(Vid::new(1), edge(100), Direction::Outgoing);
    assert!(shadow.approx_bytes() > 0);
}

// ── GC bound (Tier 1.2 remainder) ────────────────────────────────────────

use std::sync::Arc;
use uni_store::storage::adjacency_manager::PinnedVersions;

/// A pinned reader must keep its entries, even after GC runs.
///
/// This is the direction that catches a too-aggressive bound. The audit's
/// prescription — "the oldest live snapshot", sourced from `SnapshotManager` —
/// would have collected these, because `SnapshotManager` tracks no live
/// readers at all.
#[test]
fn gc_retains_entries_a_pinned_reader_can_still_resolve() {
    let shadow = ShadowCsr::new();
    let src = Vid::new(1);
    // Deleted at v10, so a reader pinned at v5 must still see it.
    shadow.add_deleted_edge(src, edge(100), Direction::Outgoing);

    let pins = Arc::new(PinnedVersions::default());
    let _guard = pins.pin(5);

    let floor = pins.min_pinned().expect("a pin is held");
    shadow.gc(floor);

    let alive = shadow.get_entries_at_version(src, 1, Direction::Outgoing, 5);
    assert_eq!(
        alive.len(),
        1,
        "GC must not drop an entry a pinned reader still resolves"
    );
}

/// The counterpart: once the pin is released the floor rises and the entry is
/// reclaimed. Without this, a GC that never collected anything would satisfy
/// the test above.
#[test]
fn gc_reclaims_once_the_pin_is_released() {
    let shadow = ShadowCsr::new();
    let src = Vid::new(1);
    shadow.add_deleted_edge(src, edge(100), Direction::Outgoing);

    let pins = Arc::new(PinnedVersions::default());
    {
        let _guard = pins.pin(5);
        assert_eq!(pins.min_pinned(), Some(5));
    }
    assert_eq!(pins.min_pinned(), None, "the guard must release on drop");

    // No pin: the floor is the current version, past the deletion at v10.
    shadow.gc(20);
    assert_eq!(
        shadow.entry_count(),
        0,
        "an entry no reader can reach must be reclaimed"
    );
}

/// The floor is the *minimum* pin, and refcounted so it does not rise while
/// another reader still holds the same version.
#[test]
fn the_floor_is_the_minimum_pin_and_is_refcounted() {
    let pins = Arc::new(PinnedVersions::default());

    let low = pins.pin(3);
    let high = pins.pin(9);
    let low_again = pins.pin(3);
    assert_eq!(pins.min_pinned(), Some(3));
    assert_eq!(pins.distinct_pinned(), 2);

    // One of the two v3 readers finishes; the floor must not move.
    drop(low);
    assert_eq!(
        pins.min_pinned(),
        Some(3),
        "the floor must not rise while another reader holds the same version"
    );

    drop(low_again);
    assert_eq!(pins.min_pinned(), Some(9));
    drop(high);
    assert_eq!(pins.min_pinned(), None);
}
