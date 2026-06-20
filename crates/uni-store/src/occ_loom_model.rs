// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Model-checking harness for the OCC commit core ([`crate::runtime::occ`]).
//!
//! This module exists ONLY under `--cfg loom` or `--cfg shuttle`. It drives the
//! *real* `CommitRegistry::{check, commit, record}` and `WriteSet`/`OccReadSet`
//! types through concurrent committers and asserts serializable-isolation
//! invariants, so the checked logic cannot drift from production. The same body
//! runs under loom (exhaustive) and shuttle (randomized) via [`super::sync::check`].
//!
//! ## What is modeled
//!
//! Each committer runs the production commit protocol: capture a read snapshot at
//! begin (`commit_sequence`), read data, then under a serialized critical section
//! validate against [`CommitRegistry::check`] and, if clean, allocate a sequence
//! and apply (`CommitRegistry::commit`). Two workloads:
//!
//! - [`run_counter_model`] — concurrent read-modify-write on one vertex. Exercises
//!   the write-write (lost-update) path; invariant: `final == committed_count`.
//! - [`run_bank_model`] — classic write-skew over two vertices. Exercises the SSI
//!   read-write path (the read-set check); invariant: the cross-row constraint
//!   holds in every interleaving.
//!
//! ## Soundness of the environment (no false positives)
//!
//! The `seq` bump and the data apply happen under the same store lock that a
//! begin-snapshot holds while reading `seq` *and* data, so every snapshot is
//! consistent: a transaction observing `seq = N` also observes commit `N`'s data.
//! Thus the model checks the OCC *protocol logic* (sequence allocation, registry
//! pruning, write-set and read-set conflict detection) without the artifacts a
//! naive store would produce. Modeling the finer memory-ordering of snapshot
//! *visibility* (production handles it via pinned snapshots) is a separate future
//! model; likewise the overlay-based constraint/MERGE/ext-id checks.

use std::collections::HashMap;

use crate::runtime::l0::OccReadSet;
use crate::runtime::occ::{CommitRegistry, WriteSet};
use crate::runtime::sync::{self, Arc, AtomicU64, Mutex, Ordering};
use uni_common::core::id::Vid;

/// Registry capacity for the models — comfortably exceeds the handful of commits
/// any small interleaving produces, so `HistoryTruncated` never fires here.
const REGISTRY_CAPACITY: usize = 16;

/// Committed data, versioned by the commit sequence under which each write landed.
/// A read at `read_seq` returns the newest version with `seq <= read_seq`, i.e. a
/// snapshot as of the reader's begin.
struct VersionedStore {
    versions: HashMap<Vid, Vec<(u64, i64)>>,
}

impl VersionedStore {
    /// Seeds each key with its initial value at sequence 0.
    fn new(initial: &[(Vid, i64)]) -> Self {
        let versions = initial
            .iter()
            .map(|&(vid, val)| (vid, vec![(0u64, val)]))
            .collect();
        Self { versions }
    }

    /// Snapshot read: newest value with `seq <= read_seq` (versions are appended
    /// in increasing sequence order, since commits are serialized).
    fn read_at(&self, read_seq: u64, vid: Vid) -> i64 {
        self.versions
            .get(&vid)
            .and_then(|vs| vs.iter().rev().find(|(s, _)| *s <= read_seq))
            .map(|(_, v)| *v)
            .unwrap_or(0)
    }

    /// Appends a committed version for `vid` at `seq`.
    fn apply(&mut self, seq: u64, vid: Vid, val: i64) {
        self.versions.entry(vid).or_default().push((seq, val));
    }

    /// The latest committed value for `vid`.
    fn latest(&self, vid: Vid) -> i64 {
        self.versions
            .get(&vid)
            .and_then(|vs| vs.last())
            .map(|(_, v)| *v)
            .unwrap_or(0)
    }
}

/// State shared across committers, all constructed inside the model closure (a
/// hard requirement of both loom and shuttle).
#[derive(Clone)]
struct Shared {
    /// Production commit-sequence atomic — the real type, via the shim.
    seq: Arc<AtomicU64>,
    /// The real `CommitRegistry`. Its mutex models the serialized commit critical
    /// section (production serializes via `flush_lock`; the model collapses that
    /// into the registry guard, which spans check -> commit -> apply atomically).
    registry: Arc<Mutex<CommitRegistry>>,
    /// Committed, versioned data — the faithful environment (see module docs).
    store: Arc<Mutex<VersionedStore>>,
}

impl Shared {
    fn new(initial: &[(Vid, i64)]) -> Self {
        Self::with_capacity(initial, REGISTRY_CAPACITY)
    }

    /// Like [`Shared::new`] but with an explicit registry capacity. The pruning
    /// model uses a tiny capacity to force `HistoryTruncated` under contention.
    fn with_capacity(initial: &[(Vid, i64)], capacity: usize) -> Self {
        Self {
            seq: Arc::new(AtomicU64::new(0)),
            registry: Arc::new(Mutex::new(CommitRegistry::new(capacity))),
            store: Arc::new(Mutex::new(VersionedStore::new(initial))),
        }
    }

    /// Begin + read phase: captures `read_seq` and reads `vids` at that snapshot,
    /// all under the store lock so the (seq, data) pair is consistent.
    fn snapshot(&self, vids: &[Vid]) -> (u64, Vec<i64>) {
        let st = self.store.lock().unwrap();
        let read_seq = self.seq.load(Ordering::Relaxed);
        let vals = vids.iter().map(|&v| st.read_at(read_seq, v)).collect();
        (read_seq, vals)
    }

    /// Serialized commit critical section. Validates `ws`/`rs` against everything
    /// committed since `read_seq`; on success allocates a sequence via the real
    /// [`CommitRegistry::commit`] seam and applies `writes`. Returns whether it
    /// committed (a conflict is a retryable abort in production).
    fn try_commit(
        &self,
        read_seq: u64,
        ws: WriteSet,
        rs: Option<&OccReadSet>,
        writes: &[(Vid, i64)],
    ) -> bool {
        let mut reg = self.registry.lock().unwrap();
        if reg.check(read_seq, &ws, rs).is_some() {
            return false;
        }
        // Acquire the store BEFORE allocating the sequence, so the bump and apply
        // are atomic w.r.t. a concurrent `snapshot()` (see module docs).
        let mut st = self.store.lock().unwrap();
        let next = reg.commit(&self.seq, ws);
        for &(vid, val) in writes {
            st.apply(next, vid, val);
        }
        true
    }
}

/// Counter (write-write) model: `n` committers each do a read-modify-write `+1`
/// on one vertex. With correct OCC, two committers commit only if the later one
/// snapshotted after the earlier committed (so it read the incremented value) —
/// hence the final value must equal the number of commits. A broken `check` that
/// let a stale committer through would lose an update and trip the assertion.
pub fn run_counter_model(n: usize) {
    sync::check(move || {
        let c = Vid::new(1);
        let shared = Shared::new(&[(c, 0)]);

        let handles: Vec<_> = (0..n)
            .map(|_| {
                let shared = shared.clone();
                sync::thread::spawn(move || {
                    let (read_seq, vals) = shared.snapshot(&[c]);
                    let mut ws = WriteSet::default();
                    ws.vertices.insert(c);
                    // Write-set-only detection (lost update): no read-set.
                    shared.try_commit(read_seq, ws, None, &[(c, vals[0] + 1)])
                })
            })
            .collect();

        let committed: i64 = handles
            .into_iter()
            .map(|h| i64::from(h.join().unwrap()))
            .sum();

        let final_c = shared.store.lock().unwrap().latest(c);
        assert_eq!(
            final_c, committed,
            "lost update: final counter {final_c} != {committed} committed increments",
        );
    });
}

/// Bank (read-write / write-skew) model: two vertices `x = y = 1` with the
/// cross-row constraint `x + y >= 1` (at least one "on"). Committer 0 turns `x`
/// off, committer 1 turns `y` off, each only if its own write would keep the
/// constraint — which, read in isolation, both may. Under snapshot isolation
/// alone both could commit and drive `x + y = 0`; the SSI read-set check must
/// abort one (its read-set `{x, y}` intersects the other's write-set), so the
/// constraint holds in every interleaving.
pub fn run_bank_model() {
    sync::check(|| {
        let x = Vid::new(1);
        let y = Vid::new(2);
        let shared = Shared::new(&[(x, 1), (y, 1)]);

        let handles: Vec<_> = [x, y]
            .into_iter()
            .map(|mine| {
                let shared = shared.clone();
                sync::thread::spawn(move || {
                    let (read_seq, vals) = shared.snapshot(&[x, y]);
                    // Turning `mine` off keeps the constraint iff the other is on.
                    let other_val = if mine == x { vals[1] } else { vals[0] };
                    if other_val < 1 {
                        return; // own guard refuses — would break the invariant
                    }
                    let mut ws = WriteSet::default();
                    ws.vertices.insert(mine);
                    // SSI read-write transaction: it read both rows.
                    let mut rs = OccReadSet::default();
                    rs.vertices.insert(x);
                    rs.vertices.insert(y);
                    shared.try_commit(read_seq, ws, Some(&rs), &[(mine, 0)]);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let st = shared.store.lock().unwrap();
        let (fx, fy) = (st.latest(x), st.latest(y));
        assert!(
            fx + fy >= 1 && fx >= 0 && fy >= 0,
            "write skew: constraint x + y >= 1 violated (x = {fx}, y = {fy})",
        );
    });
}

/// History-truncation model — makes the `HistoryTruncated` conservative-abort
/// guard load-bearing. Two committers contend on a hot key `h`; a third writes a
/// filler key `f`. With registry capacity 1, a filler commit landing between the
/// two `h` commits evicts `h`'s registry entry, so the write-write check can no
/// longer see the conflict and only the truncation guard (oldest retained seq >
/// read_seq + 1) can still abort the stale `h` committer. Without that guard,
/// that schedule loses an update on `h`. Invariant: `h`'s final value equals the
/// number of committers that committed a write to `h`. (Unlike a same-key
/// capacity-1 counter, here the write-write check does NOT backstop truncation —
/// so the negative control on the guard actually bites.)
pub fn run_truncation_model() {
    sync::check(|| {
        let h = Vid::new(1); // contended key
        let f = Vid::new(2); // filler key that evicts h's entry under capacity 1
        let shared = Shared::with_capacity(&[(h, 0), (f, 0)], 1);

        let mut handles = Vec::new();
        // Two read-modify-write committers on the hot key.
        for _ in 0..2 {
            let shared = shared.clone();
            handles.push(sync::thread::spawn(move || {
                let (read_seq, vals) = shared.snapshot(&[h]);
                let mut ws = WriteSet::default();
                ws.vertices.insert(h);
                shared.try_commit(read_seq, ws, None, &[(h, vals[0] + 1)])
            }));
        }
        // One filler committer whose commit can evict the hot key's entry.
        {
            let shared = shared.clone();
            handles.push(sync::thread::spawn(move || {
                let (read_seq, vals) = shared.snapshot(&[f]);
                let mut ws = WriteSet::default();
                ws.vertices.insert(f);
                shared.try_commit(read_seq, ws, None, &[(f, vals[0] + 1)])
            }));
        }

        let committed: Vec<bool> = handles.into_iter().map(|j| j.join().unwrap()).collect();
        // The first two handles are the hot-key committers.
        let committed_h = i64::from(committed[0]) + i64::from(committed[1]);
        let final_h = shared.store.lock().unwrap().latest(h);
        assert_eq!(
            final_h, committed_h,
            "lost update on h under truncation: final {final_h} != {committed_h} committed h-writes",
        );
    });
}
