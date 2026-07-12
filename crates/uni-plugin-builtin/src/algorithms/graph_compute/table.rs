// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The per-session generational handle table.
//!
//! One table lives per GraphCompute invocation. It stores the actual values
//! behind opaque [`Handle`]s in per-kind slot vectors, each slot carrying a
//! generation counter. Resolving a handle checks the session epoch, the kind
//! tag, the slot bound, the retired flag, and the generation — so a forged,
//! stale (use-after-free), cross-session, or wrong-kind handle is rejected as a
//! typed [`FnError`] and never indexes raw memory (proposal §4.2).
//!
//! On `free` a slot's generation is bumped and the slot returned to a free list;
//! a handle minted before the free now fails the generation check. When a slot's
//! 12-bit generation would wrap it is retired instead of recycled, so old
//! handles can never alias a new allocation (fail-closed wrap, proposal §4.2).
//
// Rust guideline compliant

use std::sync::Arc;

use uni_algo::algo::GraphProjection;
use uni_plugin::errors::FnError;

use super::error;
use super::handle::{Handle, HandleKind, MAX_GENERATION};
use super::value::{PairList, Tensor, VertexSet, WalkMatrix};

/// One slot in a per-kind slab: a generation plus an optional live value.
#[derive(Debug)]
struct Slot<T> {
    /// Current generation; incremented on every free so stale handles fail.
    generation: u16,
    /// `true` once the generation wrapped and the slot was permanently retired.
    retired: bool,
    /// The live value, or `None` when the slot is free.
    value: Option<T>,
}

impl<T> Slot<T> {
    fn new(value: T) -> Self {
        Self {
            generation: 0,
            retired: false,
            value: Some(value),
        }
    }
}

/// A generational slab of one value kind with a free list.
#[derive(Debug)]
struct Slab<T> {
    slots: Vec<Slot<T>>,
    free: Vec<u32>,
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
        }
    }
}

impl<T> Slab<T> {
    /// Inserts `value`, returning `(slot, generation)` for the packed handle.
    fn insert(&mut self, value: T) -> (u32, u16) {
        if let Some(slot) = self.free.pop() {
            let s = &mut self.slots[slot as usize];
            s.value = Some(value);
            (slot, s.generation)
        } else {
            let slot = u32::try_from(self.slots.len()).expect("slot index overflow");
            self.slots.push(Slot::new(value));
            (slot, 0)
        }
    }

    /// Resolves a `(slot, generation)` to a live value reference.
    fn get(&self, slot: u32, generation: u16) -> Result<&T, FnError> {
        let s = self
            .slots
            .get(slot as usize)
            .ok_or_else(error::stale_handle)?;
        if s.retired {
            return Err(error::wrap_fail_closed(
                "handle targets a slot retired after a generation wrap",
            ));
        }
        if s.generation != generation {
            return Err(error::stale_handle());
        }
        s.value.as_ref().ok_or_else(error::stale_handle)
    }

    /// Frees a slot, bumping its generation and reclaiming it unless it wrapped.
    ///
    /// Returns the freed value so the caller can update arena accounting.
    fn free(&mut self, slot: u32, generation: u16) -> Result<T, FnError> {
        let s = self
            .slots
            .get_mut(slot as usize)
            .ok_or_else(error::stale_handle)?;
        if s.retired {
            return Err(error::wrap_fail_closed(
                "handle targets a slot retired after a generation wrap",
            ));
        }
        if s.generation != generation {
            return Err(error::stale_handle());
        }
        let value = s.value.take().ok_or_else(error::stale_handle)?;
        if s.generation >= MAX_GENERATION {
            // Fail closed: the slot has exhausted its generation space. Retire
            // it permanently rather than recycle it into ambiguity (§4.2).
            s.retired = true;
        } else {
            s.generation += 1;
            self.free.push(slot);
        }
        Ok(value)
    }

    /// Returns the number of live values in the slab.
    fn live_count(&self) -> usize {
        self.slots.iter().filter(|s| s.value.is_some()).count()
    }
}

/// The per-invocation handle table: generational slabs keyed by value kind.
///
/// Holds vertex sets, tensors, and projected graphs behind opaque handles. All
/// access is validated (proposal §4.2). Dropping the table frees every value, so
/// a guest that leaks handles cannot leak past the end of its invocation.
#[derive(Debug)]
pub struct HandleTable {
    epoch: u16,
    sets: Slab<VertexSet>,
    tensors: Slab<Tensor>,
    graphs: Slab<Arc<GraphProjection>>,
    walks: Slab<WalkMatrix>,
    pairs: Slab<PairList>,
}

impl HandleTable {
    /// Creates an empty table stamped with the session `epoch`.
    #[must_use]
    pub fn new(epoch: u16) -> Self {
        Self {
            epoch,
            sets: Slab::default(),
            tensors: Slab::default(),
            graphs: Slab::default(),
            walks: Slab::default(),
            pairs: Slab::default(),
        }
    }

    /// Returns the session epoch stamped into every handle this table mints.
    #[must_use]
    pub fn epoch(&self) -> u16 {
        self.epoch
    }

    /// Inserts a vertex set and returns its handle.
    pub fn insert_set(&mut self, set: VertexSet) -> Handle {
        let (slot, generation) = self.sets.insert(set);
        Handle::pack(self.epoch, HandleKind::VertexSet, generation, slot)
    }

    /// Inserts a tensor and returns its handle.
    pub fn insert_tensor(&mut self, tensor: Tensor) -> Handle {
        let (slot, generation) = self.tensors.insert(tensor);
        Handle::pack(self.epoch, HandleKind::Tensor, generation, slot)
    }

    /// Inserts a projected graph and returns its handle.
    pub fn insert_graph(&mut self, graph: Arc<GraphProjection>) -> Handle {
        let (slot, generation) = self.graphs.insert(graph);
        Handle::pack(self.epoch, HandleKind::Graph, generation, slot)
    }

    /// Inserts a batch of random walks and returns its handle.
    pub fn insert_walks(&mut self, walks: WalkMatrix) -> Handle {
        let (slot, generation) = self.walks.insert(walks);
        Handle::pack(self.epoch, HandleKind::Walks, generation, slot)
    }

    /// Inserts a per-edge pair list and returns its handle.
    pub fn insert_pairs(&mut self, pairs: PairList) -> Handle {
        let (slot, generation) = self.pairs.insert(pairs);
        Handle::pack(self.epoch, HandleKind::Pairs, generation, slot)
    }

    /// Validates the epoch and kind of `h`, returning the resolved kind.
    ///
    /// # Errors
    /// Returns `0x863` on an epoch mismatch (cross-session / forged) and `0x861`
    /// when the packed kind tag is not a known kind.
    fn check_epoch_and_kind(&self, h: Handle) -> Result<HandleKind, FnError> {
        if h.epoch() != self.epoch {
            return Err(error::epoch_mismatch());
        }
        h.kind().ok_or_else(|| error::kind_mismatch("a known kind"))
    }

    /// Resolves a vertex-set handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch.
    pub fn get_set(&self, h: Handle) -> Result<&VertexSet, FnError> {
        match self.check_epoch_and_kind(h)? {
            HandleKind::VertexSet => self.sets.get(h.slot(), h.generation()),
            _ => Err(error::kind_mismatch("VertexSet")),
        }
    }

    /// Resolves a tensor handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch.
    pub fn get_tensor(&self, h: Handle) -> Result<&Tensor, FnError> {
        match self.check_epoch_and_kind(h)? {
            HandleKind::Tensor => self.tensors.get(h.slot(), h.generation()),
            _ => Err(error::kind_mismatch("Tensor")),
        }
    }

    /// Resolves a graph handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch.
    pub fn get_graph(&self, h: Handle) -> Result<&Arc<GraphProjection>, FnError> {
        match self.check_epoch_and_kind(h)? {
            HandleKind::Graph => self.graphs.get(h.slot(), h.generation()),
            _ => Err(error::kind_mismatch("Graph")),
        }
    }

    /// Resolves a walks handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch.
    pub fn get_walks(&self, h: Handle) -> Result<&WalkMatrix, FnError> {
        match self.check_epoch_and_kind(h)? {
            HandleKind::Walks => self.walks.get(h.slot(), h.generation()),
            _ => Err(error::kind_mismatch("Walks")),
        }
    }

    /// Resolves a pair-list handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch.
    pub fn get_pairs(&self, h: Handle) -> Result<&PairList, FnError> {
        match self.check_epoch_and_kind(h)? {
            HandleKind::Pairs => self.pairs.get(h.slot(), h.generation()),
            _ => Err(error::kind_mismatch("Pairs")),
        }
    }

    /// Frees any handle, returning the number of heap bytes reclaimed.
    ///
    /// Graph handles report zero bytes: the projection is shared behind an `Arc`
    /// and not counted against the value arena.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch,
    /// including a double free (the generation will already have advanced).
    pub fn free(&mut self, h: Handle) -> Result<usize, FnError> {
        match self.check_epoch_and_kind(h)? {
            HandleKind::VertexSet => {
                let v = self.sets.free(h.slot(), h.generation())?;
                Ok(v.heap_bytes())
            }
            HandleKind::Tensor => {
                let v = self.tensors.free(h.slot(), h.generation())?;
                Ok(v.heap_bytes())
            }
            HandleKind::Graph => {
                let _ = self.graphs.free(h.slot(), h.generation())?;
                Ok(0)
            }
            HandleKind::Walks => {
                let v = self.walks.free(h.slot(), h.generation())?;
                Ok(v.heap_bytes())
            }
            HandleKind::Pairs => {
                let v = self.pairs.free(h.slot(), h.generation())?;
                Ok(v.heap_bytes())
            }
            HandleKind::Levels => Err(error::kind_mismatch("a supported kind")),
        }
    }

    /// Returns the total number of live handles across all kinds.
    #[must_use]
    pub fn live_handles(&self) -> usize {
        self.sets.live_count()
            + self.tensors.live_count()
            + self.graphs.live_count()
            + self.walks.live_count()
            + self.pairs.live_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithms::graph_compute::value::Tensor;

    fn tensor(v: f64) -> Tensor {
        Tensor::from_f64(vec![v])
    }

    #[test]
    fn insert_get_free_roundtrip() {
        let mut t = HandleTable::new(1);
        let h = t.insert_tensor(tensor(3.0));
        assert_eq!(t.get_tensor(h).unwrap().values(), &[3.0]);
        assert_eq!(t.live_handles(), 1);
        assert_eq!(t.free(h).unwrap(), std::mem::size_of::<f64>());
        assert_eq!(t.live_handles(), 0);
    }

    #[test]
    fn h5_generation_wrap_retires_slot() {
        // Force MAX_GENERATION + 1 free/alloc cycles on one slot; on the wrap the
        // slot is retired (never recycled), and the last-issued handle is still
        // rejected — the §4.2 fail-closed wrap.
        let mut t = HandleTable::new(1);
        let mut last = t.insert_tensor(tensor(0.0));
        for _ in 0..u32::from(MAX_GENERATION) {
            t.free(last).unwrap();
            last = t.insert_tensor(tensor(0.0));
            // Each reuse lands on the same slot (free list has exactly one entry).
            assert_eq!(last.slot(), 0);
        }
        // `last` now carries generation == MAX_GENERATION. Freeing it retires the
        // slot rather than recycling it.
        assert_eq!(last.generation(), MAX_GENERATION);
        t.free(last).unwrap();
        // A brand-new allocation must NOT reuse the retired slot 0.
        let fresh = t.insert_tensor(tensor(1.0));
        assert_ne!(fresh.slot(), 0, "retired slot must not be recycled");
        // And the retired handle stays rejected — with the distinct fail-closed
        // wrap code (0x86B), not a generic stale-handle (0x860).
        assert_eq!(
            t.get_tensor(last).unwrap_err().code,
            error::WRAP_FAIL_CLOSED,
            "a retired-slot access must report the fail-closed wrap code"
        );
    }

    #[test]
    fn h6_double_free_and_stale_rejected() {
        let mut t = HandleTable::new(1);
        let h = t.insert_tensor(tensor(1.0));
        t.free(h).unwrap();
        assert!(t.free(h).is_err(), "double free must be rejected");
        assert!(t.get_tensor(h).is_err(), "use-after-free must be rejected");
    }

    #[test]
    fn epoch_stamped_into_handles() {
        let t = HandleTable::new(0xABCD);
        let mut t = t;
        let h = t.insert_set(VertexSet::with_capacity(4));
        assert_eq!(h.epoch(), 0xABCD);
    }
}
