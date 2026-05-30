// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Optimistic concurrency control: commit-time conflict detection (SSI/OCC).
//!
//! Commits are already serialized at the Writer's `flush_lock`, which gives the
//! validate phase a natural critical section. Each transaction captures the
//! Writer's commit-sequence at begin (`L0Buffer::occ_read_seq`); at commit it
//! checks its write-set (and, under SSI, read-set) against the write-sets of
//! every transaction that committed since. A conflict aborts the committer with
//! a retryable error. See `docs/proposals/serializable_snapshot_isolation.md`.

use std::collections::{HashSet, VecDeque};

use crate::runtime::l0::{L0Buffer, OccReadSet};
use uni_common::core::id::{Eid, Vid};

/// The set of items a transaction wrote, used for conflict detection.
#[derive(Debug, Default, Clone)]
pub struct WriteSet {
    /// Vertices created, updated, or deleted by the transaction.
    pub vertices: HashSet<Vid>,
    /// Edges created, updated, or deleted by the transaction.
    pub edges: HashSet<Eid>,
}

impl WriteSet {
    /// Builds a write-set from a transaction's private L0 buffer.
    ///
    /// Item-level granularity: every touched vertex/edge id (insert, update, or
    /// tombstone) is included. Property-key granularity is intentionally not
    /// tracked — two writers touching the same row conflict regardless of which
    /// columns they wrote, which is the correct (if conservative) lost-update rule.
    pub fn from_l0(l0: &L0Buffer) -> Self {
        let mut vertices: HashSet<Vid> = l0.vertex_properties.keys().copied().collect();
        vertices.extend(l0.vertex_tombstones.iter().copied());
        let mut edges: HashSet<Eid> = l0.edge_properties.keys().copied().collect();
        edges.extend(l0.edge_endpoints.keys().copied());
        edges.extend(l0.tombstones.keys().copied());
        Self { vertices, edges }
    }

    /// Returns `true` when the write-set touches nothing (a read-only commit).
    pub fn is_empty(&self) -> bool {
        self.vertices.is_empty() && self.edges.is_empty()
    }

    /// Returns `true` when this and `other` write any common vertex or edge.
    pub fn intersects(&self, other: &WriteSet) -> bool {
        // Iterate the smaller side for cheaper membership checks.
        let (small, large) = if self.vertices.len() <= other.vertices.len() {
            (&self.vertices, &other.vertices)
        } else {
            (&other.vertices, &self.vertices)
        };
        if small.iter().any(|v| large.contains(v)) {
            return true;
        }
        let (small, large) = if self.edges.len() <= other.edges.len() {
            (&self.edges, &other.edges)
        } else {
            (&other.edges, &self.edges)
        };
        small.iter().any(|e| large.contains(e))
    }
}

/// Returns `true` when a committed write touched something the read-set saw.
fn read_set_intersects(read_set: &OccReadSet, w: &WriteSet) -> bool {
    read_set.vertices.iter().any(|v| w.vertices.contains(v))
        || read_set.edges.iter().any(|e| w.edges.contains(e))
}

/// Outcome of a commit-time conflict check.
#[derive(Debug)]
pub enum Conflict {
    /// A concurrent commit wrote an item this transaction also wrote.
    WriteWrite { seq: u64 },
    /// A concurrent commit wrote an item this transaction read (SSI).
    ReadWrite { seq: u64 },
    /// The commit history was pruned below this transaction's read sequence,
    /// so a potential conflict cannot be ruled out; abort conservatively.
    HistoryTruncated { read_seq: u64, oldest: u64 },
}

impl std::fmt::Display for Conflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Conflict::WriteWrite { seq } => {
                write!(f, "write-write conflict with commit sequence {seq}")
            }
            Conflict::ReadWrite { seq } => {
                write!(f, "read-write antidependency with commit sequence {seq}")
            }
            Conflict::HistoryTruncated { read_seq, oldest } => write!(
                f,
                "commit history truncated below read sequence {read_seq} \
                 (oldest retained {oldest}); aborting conservatively"
            ),
        }
    }
}

/// Bounded log of recently-committed write-sets, keyed by commit sequence.
///
/// Mutated only under the Writer's `flush_lock`, so it needs no internal
/// synchronization beyond the `Mutex` the Writer wraps it in.
#[derive(Debug)]
pub struct CommitRegistry {
    entries: VecDeque<(u64, WriteSet)>,
    capacity: usize,
}

impl CommitRegistry {
    /// Creates a registry retaining at most `capacity` recent commits.
    ///
    /// # Panics
    /// Panics if `capacity` is zero (a programming error — the registry must
    /// retain at least one commit to detect any conflict).
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "CommitRegistry capacity must be non-zero");
        Self {
            entries: VecDeque::new(),
            capacity,
        }
    }

    /// Records a committed write-set under `seq`, pruning to capacity.
    pub fn record(&mut self, seq: u64, write_set: WriteSet) {
        self.entries.push_back((seq, write_set));
        while self.entries.len() > self.capacity {
            self.entries.pop_front();
        }
    }

    /// Checks a committing transaction against all commits newer than its read
    /// sequence. Returns the first [`Conflict`] found, or `None` if it may commit.
    ///
    /// `read_set` is `Some` only for SSI read-write transactions; passing `None`
    /// performs write-set-only (lost-update) detection.
    pub fn check(
        &self,
        read_seq: u64,
        write_set: &WriteSet,
        read_set: Option<&OccReadSet>,
    ) -> Option<Conflict> {
        // If the oldest retained commit is newer than read_seq+1, commits in the
        // gap were pruned and cannot be checked — abort conservatively (sound:
        // never misses a real conflict, at the cost of rare false aborts).
        if let Some(&(oldest, _)) = self.entries.front()
            && oldest > read_seq.saturating_add(1)
        {
            return Some(Conflict::HistoryTruncated { read_seq, oldest });
        }
        for (seq, committed) in &self.entries {
            if *seq <= read_seq {
                continue;
            }
            if write_set.intersects(committed) {
                return Some(Conflict::WriteWrite { seq: *seq });
            }
            if let Some(rs) = read_set
                && read_set_intersects(rs, committed)
            {
                return Some(Conflict::ReadWrite { seq: *seq });
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws(vids: &[u64]) -> WriteSet {
        WriteSet {
            vertices: vids.iter().map(|&v| Vid::from(v)).collect(),
            edges: HashSet::new(),
        }
    }

    #[test]
    fn disjoint_writes_do_not_conflict() {
        let mut reg = CommitRegistry::new(16);
        reg.record(1, ws(&[1, 2]));
        assert!(reg.check(0, &ws(&[3, 4]), None).is_none());
    }

    #[test]
    fn overlapping_write_after_read_seq_conflicts() {
        let mut reg = CommitRegistry::new(16);
        reg.record(1, ws(&[1, 2]));
        // A tx that began at read_seq 0 and writes vertex 2 must abort.
        assert!(matches!(
            reg.check(0, &ws(&[2]), None),
            Some(Conflict::WriteWrite { seq: 1 })
        ));
    }

    #[test]
    fn commit_at_or_before_read_seq_is_ignored() {
        let mut reg = CommitRegistry::new(16);
        reg.record(1, ws(&[1]));
        // A tx that began AFTER commit 1 (read_seq 1) does not conflict with it.
        assert!(reg.check(1, &ws(&[1]), None).is_none());
    }

    #[test]
    fn read_write_antidependency_detected() {
        let mut reg = CommitRegistry::new(16);
        reg.record(1, ws(&[5]));
        let mut rs = OccReadSet::default();
        rs.vertices.insert(Vid::from(5));
        assert!(matches!(
            reg.check(0, &ws(&[99]), Some(&rs)),
            Some(Conflict::ReadWrite { seq: 1 })
        ));
    }

    #[test]
    fn truncated_history_aborts_conservatively() {
        let mut reg = CommitRegistry::new(2);
        reg.record(1, ws(&[1]));
        reg.record(2, ws(&[2]));
        reg.record(3, ws(&[3])); // evicts seq 1
        // A tx with read_seq 0 cannot verify against the evicted seq 1.
        assert!(matches!(
            reg.check(0, &ws(&[42]), None),
            Some(Conflict::HistoryTruncated { read_seq: 0, oldest: 2 })
        ));
    }
}
