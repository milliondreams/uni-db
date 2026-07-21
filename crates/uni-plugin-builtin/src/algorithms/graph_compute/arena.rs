// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Session-local mutable synthetic structure — the `graph-arena@1` substrate.
//!
//! A [`GraphArena`] is the mutable counterpart to the immutable
//! [`GraphProjection`](uni_algo::algo::GraphProjection): a guest grows it during
//! an invocation (search trees, residual networks, agent populations) rather
//! than reading it from the store. It lives in the same handle table, is
//! dispatched by the same `dispatch.rs`, and crosses the same host import as
//! every other kernel — which is the whole point. The retired `ScratchGraph`
//! was a *parallel* stack with its own registry and its own ABI, and that is
//! precisely why no loader could reach it (issue #152, proposal §3).
//!
//! # Shape
//!
//! Adjacency is **CSR-with-slack**: a flat `capacity * branching` array where
//! slot `i` owns the run `[i * branching, i * branching + child_len[i])`.
//! `ScratchGraph` used `Vec<Vec<u32>>` — a heap allocation per node, which is
//! the wrong shape for a tree arena and made growth expensive.
//!
//! The slack means an arena does **not** have dense `[E]` edge numbering: the
//! run for slot `i` is padded, so `out_edge_start(u) + k` is not a global edge
//! index. Kernels that rely on that numbering must go through
//! [`GraphArena::freeze_csr`] first; see proposal §13.3.
//!
//! # Columns live here, not in tensors
//!
//! Per-slot state (`visits`, `value`, a guest's score) lives in `[capacity]`
//! `f64` columns owned by the arena and addressed by index.
//!
//! Proposal §5.1 assumed columns would be ordinary
//! [`Tensor`](super::value::Tensor) handles. They cannot be: `Tensor` is
//! Arrow-backed and **immutable**, so every in-place update — the per-step visit
//! increment and virtual loss that §12.2 proved mandatory — would mean copying
//! the whole column. That is `O(N)` per descent step and defeats the entire
//! point of candidate-scoped work. Owning plain `Vec<f64>` columns is what made
//! the measured prototype fast, and is what is reproduced here (see §13.6).
//!
//! Multiple named columns are themselves a requirement, not a convenience:
//! `ScratchGraph`'s single `f64` per node could not represent a search node
//! carrying both visits *and* value (proposal §12.7).
//
// Rust guideline compliant

use uni_plugin::errors::FnError;

use super::error;

/// Sentinel stored in an arena's parent array for a slot with no parent.
pub const NO_PARENT: u32 = u32::MAX;

/// A mutable, session-local graph arena addressed by dense slots `0..len`.
#[derive(Debug, Clone)]
pub struct GraphArena {
    capacity: u32,
    branching: u32,
    len: u32,
    /// `capacity * branching`; slot `i`'s children are the first
    /// `child_len[i]` entries of its run.
    child: Vec<u32>,
    child_len: Vec<u32>,
    parent: Vec<u32>,
    /// `[capacity]` per-slot state columns, addressed by index.
    columns: Vec<Vec<f64>>,
}

impl GraphArena {
    /// Creates an empty arena sized for `capacity` slots of `branching` children.
    ///
    /// # Errors
    /// Returns `0x86E` if either bound is zero or the flat child array would
    /// exceed `u32` addressing — rejected up front so a guest cannot induce an
    /// overflowing allocation.
    pub fn new(capacity: u32, branching: u32) -> Result<Self, FnError> {
        if capacity == 0 || branching == 0 {
            return Err(error::arg_validation(
                "arena_new: capacity and branching must both be non-zero",
            ));
        }
        let cells = u64::from(capacity) * u64::from(branching);
        if cells > u64::from(u32::MAX) {
            return Err(error::arg_validation(format!(
                "arena_new: capacity {capacity} x branching {branching} exceeds u32 addressing"
            )));
        }
        let cells = usize::try_from(cells).map_err(|_| {
            error::arg_validation("arena_new: capacity x branching exceeds this platform")
        })?;
        let cap = capacity as usize;
        Ok(Self {
            capacity,
            branching,
            len: 0,
            child: vec![0; cells],
            child_len: vec![0; cap],
            parent: vec![NO_PARENT; cap],
            columns: Vec::new(),
        })
    }

    /// Adds a zero-filled `[capacity]` state column, returning its index.
    ///
    /// The caller is responsible for charging [`GraphArena::column_bytes`]
    /// against the session arena cap *before* calling, so a guest cannot grow
    /// state without accounting for it.
    pub fn column_new(&mut self) -> u32 {
        self.columns.push(vec![0.0; self.capacity as usize]);
        #[expect(
            clippy::cast_possible_truncation,
            reason = "column count is bounded by the arena byte cap long before u32"
        )]
        let idx = (self.columns.len() - 1) as u32;
        idx
    }

    /// Bytes one column occupies — what a caller must reserve per `column_new`.
    #[must_use]
    pub fn column_bytes(&self) -> usize {
        self.capacity as usize * size_of::<f64>()
    }

    /// Number of state columns.
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Resolves a column index for reading.
    ///
    /// # Errors
    /// Returns `0x86E` when the index is not a live column.
    pub fn column(&self, idx: u32) -> Result<&[f64], FnError> {
        self.columns
            .get(idx as usize)
            .map(Vec::as_slice)
            .ok_or_else(|| {
                error::arg_validation(format!(
                    "arena: column {idx} does not exist (live columns are 0..{})",
                    self.columns.len()
                ))
            })
    }

    /// Resolves a column index for writing.
    ///
    /// # Errors
    /// Returns `0x86E` when the index is not a live column.
    pub fn column_mut(&mut self, idx: u32) -> Result<&mut [f64], FnError> {
        let n = self.columns.len();
        self.columns
            .get_mut(idx as usize)
            .map(Vec::as_mut_slice)
            .ok_or_else(|| {
                error::arg_validation(format!(
                    "arena: column {idx} does not exist (live columns are 0..{n})"
                ))
            })
    }

    /// Bump-allocates `count` slots, returning their ids.
    ///
    /// # Errors
    /// Returns `0x864` when the request would exceed the arena's capacity. The
    /// arena is left untouched, so a rejected allocation is not a partial one.
    pub fn alloc(&mut self, count: u32) -> Result<Vec<u32>, FnError> {
        let end = self
            .len
            .checked_add(count)
            .ok_or_else(|| error::arena_cap_exceeded("arena_alloc: slot counter would overflow"))?;
        if end > self.capacity {
            return Err(error::arena_cap_exceeded(format!(
                "arena_alloc: {count} slots would exceed capacity {} (live {})",
                self.capacity, self.len
            )));
        }
        let slots = (self.len..end).collect();
        self.len = end;
        Ok(slots)
    }

    /// Appends `kid` to `parent`'s child run.
    ///
    /// # Errors
    /// Returns `0x86E` for an out-of-range slot and `0x864` when the parent's
    /// run is already full (its slack is the declared `branching`).
    pub fn link(&mut self, parent: u32, kid: u32) -> Result<(), FnError> {
        self.check_slot(parent)?;
        self.check_slot(kid)?;
        let p = parent as usize;
        let n = self.child_len[p];
        if n >= self.branching {
            return Err(error::arena_cap_exceeded(format!(
                "arena_link: slot {parent} already holds its branching limit of {}",
                self.branching
            )));
        }
        self.child[p * self.branching as usize + n as usize] = kid;
        self.child_len[p] = n + 1;
        self.parent[kid as usize] = parent;
        Ok(())
    }

    /// The children of `slot`, or an empty slice for a leaf.
    #[must_use]
    pub fn children(&self, slot: u32) -> &[u32] {
        if slot >= self.len {
            return &[];
        }
        let s = slot as usize;
        let base = s * self.branching as usize;
        &self.child[base..base + self.child_len[s] as usize]
    }

    /// The parent of `slot`, or `None` for a root or an out-of-range slot.
    #[must_use]
    pub fn parent_of(&self, slot: u32) -> Option<u32> {
        if slot >= self.len {
            return None;
        }
        let p = self.parent[slot as usize];
        (p != NO_PARENT).then_some(p)
    }

    /// Number of live slots.
    #[must_use]
    pub fn len(&self) -> u32 {
        self.len
    }

    /// Whether no slot has been allocated yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The slot ceiling this arena was created with.
    #[must_use]
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    /// The per-slot child slack.
    #[must_use]
    pub fn branching(&self) -> u32 {
        self.branching
    }

    /// Total edges currently linked — the sum of every slot's child count.
    ///
    /// Note this is *not* the length of the flat child array, which includes
    /// slack; see the module docs on dense `[E]` numbering.
    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.child_len.iter().map(|&n| n as usize).sum()
    }

    /// Validates a guest-supplied slot id against the live range.
    ///
    /// # Errors
    /// Returns `0x86E` naming the offending slot and the live bound.
    pub fn check_slot(&self, slot: u32) -> Result<(), FnError> {
        if slot >= self.len {
            return Err(error::arg_validation(format!(
                "arena: slot {slot} is out of range (live slots are 0..{})",
                self.len
            )));
        }
        Ok(())
    }

    /// Heap bytes held, for the arena byte cap.
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        (self.child.len() + self.child_len.len() + self.parent.len()) * size_of::<u32>()
            + self.columns.len() * self.column_bytes()
    }

    /// Descends from each root to a leaf, choosing the best-scoring child.
    ///
    /// `score` and `visit` are column indices. At **every** step the chosen
    /// slot's visit column is incremented by 1 and its score adjusted by
    /// `vloss` (subtracted when maximizing, added when minimizing) — a virtual
    /// loss applied *in the loop*, not after the batch. §12.2 measured that
    /// omitting this collapses a 1024-rollout batch onto 16 distinct leaves,
    /// because every rollout in the batch would descend against identical
    /// statistics.
    ///
    /// Ties break by lower slot, so a given (arena, scores) always yields the
    /// same result — kernel determinism is preserved under batching (§5.6).
    ///
    /// Returns the leaf reached per root, and the number of children inspected
    /// so the caller can charge the real work done.
    ///
    /// # Errors
    /// Returns `0x86E` for an unknown column or an out-of-range root.
    pub fn descend(
        &mut self,
        roots: &[u32],
        score: u32,
        visit: u32,
        maximize: bool,
        vloss: f64,
    ) -> Result<(Vec<u32>, u64), FnError> {
        // Validate up front so a partially-applied descent is impossible.
        let _ = self.column(score)?;
        let _ = self.column(visit)?;
        if score == visit {
            return Err(error::arg_validation(
                "arena_descend: score and visit must be different columns; \
                 sharing one would make the virtual loss corrupt the visit count",
            ));
        }
        for &r in roots {
            self.check_slot(r)?;
        }

        let mut leaves = Vec::with_capacity(roots.len());
        let mut inspected: u64 = 0;
        for &root in roots {
            let mut cur = root;
            loop {
                self.apply_visit(cur, visit, score, maximize, vloss);
                let kids = self.children(cur);
                if kids.is_empty() {
                    break;
                }
                inspected += kids.len() as u64;
                let scores = &self.columns[score as usize];
                let mut best = kids[0];
                let mut best_s = scores[best as usize];
                for &k in &kids[1..] {
                    let s = scores[k as usize];
                    // Strict comparison keeps the lower slot on a tie.
                    if (maximize && s > best_s) || (!maximize && s < best_s) {
                        best = k;
                        best_s = s;
                    }
                }
                cur = best;
            }
            leaves.push(cur);
        }
        Ok((leaves, inspected))
    }

    /// Applies one visit plus its virtual loss to `slot`.
    fn apply_visit(&mut self, slot: u32, visit: u32, score: u32, maximize: bool, vloss: f64) {
        let i = slot as usize;
        self.columns[visit as usize][i] += 1.0;
        if vloss != 0.0 {
            let delta = if maximize { -vloss } else { vloss };
            self.columns[score as usize][i] += delta;
        }
    }

    /// Compacts to dense `(offsets, neighbors)` CSR, dropping the slack.
    ///
    /// This is what makes an arena usable by the kernels that assume dense `[E]`
    /// edge numbering (proposal §13.3): after freezing, `offsets[u] + k` is a
    /// genuine global edge index.
    #[must_use]
    pub fn freeze_csr(&self) -> (Vec<u32>, Vec<u32>) {
        let n = self.len as usize;
        let mut offsets = Vec::with_capacity(n + 1);
        let mut neighbors = Vec::with_capacity(self.edge_count());
        offsets.push(0);
        for slot in 0..n {
            #[expect(clippy::cast_possible_truncation, reason = "slot count is a u32")]
            let s = slot as u32;
            neighbors.extend_from_slice(self.children(s));
            #[expect(
                clippy::cast_possible_truncation,
                reason = "edge count is bounded by capacity*branching <= u32::MAX"
            )]
            let off = neighbors.len() as u32;
            offsets.push(off);
        }
        (offsets, neighbors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_is_all_or_nothing_at_the_capacity_edge() {
        let mut a = GraphArena::new(4, 2).expect("arena builds");
        assert_eq!(a.alloc(3).expect("fits"), vec![0, 1, 2]);
        let err = a.alloc(2).expect_err("2 more would exceed capacity 4");
        assert_eq!(err.code, error::ARENA_CAP_EXCEEDED);
        // The rejected request must not have consumed a partial run.
        assert_eq!(
            a.len(),
            3,
            "a rejected alloc must leave the arena untouched"
        );
        assert_eq!(a.alloc(1).expect("the last slot is still free"), vec![3]);
    }

    #[test]
    fn linking_past_the_branching_limit_fails_closed() {
        let mut a = GraphArena::new(8, 2).expect("arena builds");
        a.alloc(4).expect("slots");
        a.link(0, 1).expect("first child");
        a.link(0, 2).expect("second child");
        let err = a.link(0, 3).expect_err("branching is 2");
        assert_eq!(err.code, error::ARENA_CAP_EXCEEDED);
        assert_eq!(a.children(0), &[1, 2]);
    }

    #[test]
    fn out_of_range_slots_are_rejected_not_silently_clamped() {
        let mut a = GraphArena::new(4, 2).expect("arena builds");
        a.alloc(2).expect("slots");
        assert_eq!(
            a.link(0, 3).expect_err("slot 3 is not live").code,
            error::ARG_VALIDATION
        );
        assert_eq!(a.children(99), &[] as &[u32]);
        assert_eq!(a.parent_of(99), None);
    }

    #[test]
    fn parent_tracking_survives_linking() {
        let mut a = GraphArena::new(8, 2).expect("arena builds");
        a.alloc(3).expect("slots");
        a.link(0, 1).expect("link");
        a.link(1, 2).expect("link");
        assert_eq!(a.parent_of(0), None, "slot 0 is the root");
        assert_eq!(a.parent_of(1), Some(0));
        assert_eq!(a.parent_of(2), Some(1));
    }

    /// Freezing must drop the slack: this is the property the `DenseEdgeIds`
    /// kernels depend on, and the reason a live arena cannot serve them.
    #[test]
    fn freezing_produces_dense_edge_numbering() {
        let mut a = GraphArena::new(8, 4).expect("arena builds");
        a.alloc(4).expect("slots");
        a.link(0, 1).expect("link");
        a.link(0, 2).expect("link");
        a.link(2, 3).expect("link");

        let (offsets, neighbors) = a.freeze_csr();
        assert_eq!(offsets, vec![0, 2, 2, 3, 3]);
        assert_eq!(neighbors, vec![1, 2, 3]);
        assert_eq!(
            neighbors.len(),
            a.edge_count(),
            "dense neighbors must hold exactly the linked edges, with no slack"
        );
        // The live arena's flat array is 8*4 = 32 cells for the same 3 edges —
        // which is exactly why `out_edge_start(u) + k` is not a global index
        // before freezing.
        assert!(a.heap_bytes() > neighbors.len() * size_of::<u32>());
    }
}
