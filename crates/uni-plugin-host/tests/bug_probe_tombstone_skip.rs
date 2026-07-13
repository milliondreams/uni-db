// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-plugin-host/src/triggers.rs:835
//
// `PreExistingProbe::from_l0_chain` used to treat a vertex tombstone as
// "skip this buffer" (`continue`) instead of "entity is dead". When an
// older pending-flush buffer (B1) still holds the CREATE's props and a
// newer buffer (B2) holds the DELETE tombstone, the probe recorded the
// stale props from B1 and classified the vid as *pre-existing* — ignoring
// the superseding tombstone. A subsequent recreate was then mis-emitted as
// NODE_UPDATE (with a stale old_value) instead of NODE_CREATE.
//
// Fixed (triggers.rs:835): a tombstone in any buffer marks the entity dead
// globally, so an older buffer's props can no longer resurrect it.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use uni_common::{Value, Vid};
use uni_store::runtime::L0Manager;
use uni_store::runtime::l0::L0Buffer;

use uni_plugin_host::triggers::PreExistingProbe;

#[test]
fn tombstone_in_newer_buffer_is_ignored_by_probe() {
    let v = Vid::new(42);

    // B1 (older pending-flush buffer): holds V's CREATE properties.
    let mut b1 = L0Buffer::new(0, None);
    let mut props = HashMap::new();
    props.insert("p".to_owned(), Value::Int(1));
    b1.insert_vertex(v, props);

    // B2 (newer pending-flush buffer): holds only V's DELETE tombstone.
    let mut b2 = L0Buffer::new(1, None);
    b2.vertex_tombstones.insert(v);

    // Empty current L0; pending_flush = [B1 (old), B2 (new)] — oldest
    // first, exactly how the probe iterates get_pending_flush().
    let current = Arc::new(RwLock::new(L0Buffer::new(2, None)));
    let pending = vec![Arc::new(RwLock::new(b1)), Arc::new(RwLock::new(b2))];
    let mgr = L0Manager::from_snapshot(current, pending);

    // tx_l0: the recreate of V (references V so the probe considers it).
    let mut tx_l0 = L0Buffer::new(3, None);
    let mut new_props = HashMap::new();
    new_props.insert("p".to_owned(), Value::Int(2));
    tx_l0.insert_vertex(v, new_props);

    let probe = PreExistingProbe::from_l0_chain(&mgr, &tx_l0);

    // Committed state: V was created (B1) then deleted (B2) → V is DEAD.
    // The probe must NOT classify V as pre-existing: B2's tombstone (the newer
    // buffer) marks it dead, so B1's stale props are ignored. On the recreate
    // this drives a correct NODE_CREATE instead of a spurious NODE_UPDATE.
    assert!(
        !probe.vertex_pre_existed(v),
        "a create-then-delete vertex must not be classified as pre-existing"
    );

    // And no stale pre-image props are recorded for it.
    assert!(
        probe.vertex_properties(v).is_none(),
        "probe must not carry stale pre-delete props for a dead vertex"
    );
}
