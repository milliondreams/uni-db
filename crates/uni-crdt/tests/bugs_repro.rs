// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Runnable repros for two verified correctness findings in `uni-crdt`.
//!
//! These exercise the real public API with real inputs (serde decode + merge,
//! and `LWWMap::put`/`get`) and now assert the CORRECT behavior, pinning the
//! fixes for both findings so a regression would fail CI.

use uni_crdt::{CrdtMerge, LWWMap, ORSet};

// ============================================================================
// [1] orset.rs:253 — v1→v2 upgrade mints colliding `__legacy__` dots, causing
//     silent element loss when two independently-upgraded replicas merge.
// ============================================================================

/// Two v1 payloads, each with one distinct live element (a concurrent,
/// independent add). Decoding each triggers the v1→v2 upgrade path. Each decode
/// now mints a fresh, globally-unique actor, so the two synthetic dots never
/// collide. On merge, both elements survive the add-wins union: `{x, z}`.
#[test]
fn repro_orset_253_legacy_actor_collision_drops_elements() {
    let a_json = r#"{"elements":{"x":["11111111-1111-1111-1111-111111111111"]}}"#;
    let b_json = r#"{"elements":{"z":["22222222-2222-2222-2222-222222222222"]}}"#;

    let mut a: ORSet<String> = serde_json::from_str(a_json).unwrap();
    let b: ORSet<String> = serde_json::from_str(b_json).unwrap();

    // Sanity: each upgraded replica sees its own element before merge.
    assert!(a.contains(&"x".to_string()), "pre-merge: a has x");
    assert!(b.contains(&"z".to_string()), "pre-merge: b has z");

    a.merge(&b);

    let x = a.contains(&"x".to_string());
    let z = a.contains(&"z".to_string());
    let n = a.len();

    // CORRECT behavior: two concurrent adds, add-wins union → {x, z}, len 2.
    assert!(x, "expected a.contains(x)==true after merge (orset.rs)");
    assert!(z, "expected a.contains(z)==true after merge (orset.rs)");
    assert_eq!(n, 2, "expected len==2 (add-wins union) (orset.rs)");
}

// ============================================================================
// [2] lww_map.rs — the empty-register sentinel timestamp is gone: a first-time
//     put now wins for EVERY i64 timestamp, including the most negative values.
// ============================================================================

/// A fresh key's first write always wins, regardless of its (possibly negative)
/// timestamp. The register is created directly from the write, so no sentinel
/// timestamp reserves any part of the `i64` range.
#[test]
fn repro_lww_map_34_negative_sentinel_drops_first_write() {
    // ts == -2: first write to a fresh key must be stored.
    let mut m: LWWMap<String, i32> = LWWMap::new();
    m.put("k".to_string(), 42, -2);
    assert_eq!(
        m.get(&"k".to_string()).copied(),
        Some(42),
        "first write to a fresh key must win (lww_map.rs)"
    );

    // The extreme boundary must work too: i64::MIN was previously below any
    // sentinel and would have been dropped.
    let mut m_min: LWWMap<String, i32> = LWWMap::new();
    m_min.put("k".to_string(), 7, i64::MIN);
    assert_eq!(
        m_min.get(&"k".to_string()).copied(),
        Some(7),
        "first write at i64::MIN must win (lww_map.rs)"
    );

    // ts == -1 (the former sentinel) is just an ordinary timestamp now.
    let mut m2: LWWMap<String, i32> = LWWMap::new();
    m2.put("k".to_string(), 42, -1);
    assert_eq!(
        m2.get(&"k".to_string()).copied(),
        Some(42),
        "ts == -1 is an ordinary user timestamp (lww_map.rs)"
    );
}
