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

use super::arena::GraphArena;
use super::error;
use super::handle::{Handle, HandleKind, MAX_GENERATION};
use super::value::{EdgeSet, PairList, Tensor, VertexSet, WalkMatrix};

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

    /// Resolves a `(slot, generation)` to a live *mutable* value reference.
    ///
    /// Every other handle kind is immutable once inserted; the graph arena
    /// (proposal §5.1) is the first that a guest grows in place, which is why
    /// this exists. The validation is deliberately identical to [`Slab::get`] —
    /// a stale, wrapped, or forged handle must fail the same way whether the
    /// caller intends to read or to write.
    fn get_mut(&mut self, slot: u32, generation: u16) -> Result<&mut T, FnError> {
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
        s.value.as_mut().ok_or_else(error::stale_handle)
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

/// How many handle resolutions a session remembers when tracing is on.
///
/// Bounded on purpose: a long-running guest resolves handles in the millions, and
/// the point is the tail leading up to a failure, not the whole history.
const TRACE_CAPACITY: usize = 64;

/// Whether `UNI_GC_TRACE` was set, read once per process.
///
/// A per-resolution `env::var` would dominate the cost of the resolution itself.
/// Always compiled rather than feature-gated: forensics you cannot switch on in a
/// shipped build are no use against a field report you cannot reproduce.
fn tracing_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    match FORCE_TRACE.load(std::sync::atomic::Ordering::Relaxed) {
        FORCE_ON => return true,
        FORCE_OFF => return false,
        _ => {}
    }
    *ENABLED.get_or_init(|| std::env::var_os("UNI_GC_TRACE").is_some())
}

/// Test-only override for [`tracing_enabled`].
///
/// The env read is `OnceLock`-cached, so a test cannot switch tracing on by
/// setting the variable mid-process. Without this the positive test could only
/// run under a separately-invoked `UNI_GC_TRACE=1` command — which is how it was
/// first written, and since nothing in the repo set the variable, that test never
/// actually executed.
/// Three-valued on purpose: the override must be able to force tracing *off*
/// as well as on, or the "invisible when off" test fails under the CI job that
/// sets `UNI_GC_TRACE=1`. A boolean could only ever add tracing, never remove it.
static FORCE_TRACE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(FORCE_UNSET);

/// Defer to the environment.
const FORCE_UNSET: u8 = 0;
/// Trace regardless of the environment.
const FORCE_ON: u8 = 1;
/// Do not trace, even if the environment asks for it.
const FORCE_OFF: u8 = 2;

/// Forces tracing on for the current process. Tests only.
///
/// Not `#[cfg(test)]`: the loader crates' tests need it too, and the Rhai case in
/// particular is the one that matters — the hook sits in `check_epoch_and_kind`
/// precisely because Rhai bypasses the JSON dispatcher, so a test that cannot
/// reach that surface cannot demonstrate the design.
#[doc(hidden)]
pub fn force_tracing_for_test(on: bool) {
    let v = if on { FORCE_ON } else { FORCE_OFF };
    FORCE_TRACE.store(v, std::sync::atomic::Ordering::Relaxed);
}

/// Appends a breadcrumb trail to an error message.
///
/// A free function rather than a method because `HandleTable::free` holds
/// `&mut self` across the slab call and cannot also borrow `&self` to read the
/// ring — it snapshots the crumbs first and calls this.
fn attach_trace(mut err: FnError, crumbs: &[String]) -> FnError {
    if crumbs.is_empty() {
        return err;
    }
    err.message = format!(
        "{} [gc-trace, oldest first, epoch:kind:gen:slot — {}]",
        err.message,
        crumbs.join(" ")
    );
    err
}

/// The per-invocation handle table: generational slabs keyed by value kind.
///
/// Holds vertex sets, tensors, and projected graphs behind opaque handles. All
/// access is validated (proposal §4.2). Dropping the table frees every value, so
/// a guest that leaks handles cannot leak past the end of its invocation.
#[derive(Debug)]
pub struct HandleTable {
    epoch: u16,
    /// Recent handle resolutions, newest last. Empty unless `UNI_GC_TRACE` is set.
    ///
    /// Behind a `Mutex` only so the `&self` resolution path can record into it;
    /// the table is already externally serialized, and when tracing is off the
    /// lock is never taken.
    trace: std::sync::Mutex<std::collections::VecDeque<(u16, u8, u16, u32)>>,
    sets: Slab<VertexSet>,
    edge_sets: Slab<EdgeSet>,
    tensors: Slab<Tensor>,
    graphs: Slab<Arc<GraphProjection>>,
    walks: Slab<WalkMatrix>,
    pairs: Slab<PairList>,
    arenas: Slab<GraphArena>,
}

impl HandleTable {
    /// Creates an empty table stamped with the session `epoch`.
    #[must_use]
    pub fn new(epoch: u16) -> Self {
        Self {
            epoch,
            trace: std::sync::Mutex::new(std::collections::VecDeque::new()),
            sets: Slab::default(),
            edge_sets: Slab::default(),
            tensors: Slab::default(),
            graphs: Slab::default(),
            walks: Slab::default(),
            pairs: Slab::default(),
            arenas: Slab::default(),
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

    /// Inserts an edge mask and returns its handle.
    pub fn insert_edge_set(&mut self, set: EdgeSet) -> Handle {
        let (slot, generation) = self.edge_sets.insert(set);
        Handle::pack(self.epoch, HandleKind::EdgeSet, generation, slot)
    }

    /// Inserts a mutable graph arena and returns its handle.
    pub fn insert_arena(&mut self, arena: GraphArena) -> Handle {
        let (slot, generation) = self.arenas.insert(arena);
        Handle::pack(self.epoch, HandleKind::Arena, generation, slot)
    }

    /// Validates the epoch and kind of `h`, returning the resolved kind.
    ///
    /// # Errors
    /// Returns `0x863` on an epoch mismatch (cross-session / forged) and `0x861`
    /// when the packed kind tag is not a known kind.
    fn check_epoch_and_kind(&self, h: Handle) -> Result<HandleKind, FnError> {
        if h.epoch() != self.epoch {
            return Err(self.with_trace(error::epoch_mismatch()));
        }
        h.kind()
            .ok_or_else(|| self.with_trace(error::kind_mismatch("a known kind")))
    }

    /// Appends the recent-resolution trace to a failing handle error.
    ///
    /// A rejected handle is the moment the trace is worth most — what it aliased,
    /// and what was resolved just before. Riding the error means it reaches the
    /// caller through the machinery that already carries diagnostics out, with no
    /// new surfacing mechanism. Inert unless `UNI_GC_TRACE` is set.
    fn with_trace(&self, err: FnError) -> FnError {
        if !tracing_enabled() {
            return err;
        }
        attach_trace(err, &self.trace_breadcrumbs())
    }

    /// Records a handle resolution when tracing is on.
    ///
    /// Hooked here rather than in the JSON dispatcher because the Rhai loader
    /// never goes through it — `GcSession` calls `AlgoSession` directly — so a
    /// dispatcher-level hook would be blind to the surface guests most often
    /// write.
    fn trace_resolution(&self, h: Handle) {
        if !tracing_enabled() {
            return;
        }
        let Ok(mut ring) = self.trace.lock() else {
            return; // a poisoned trace must never fail a real kernel
        };
        if ring.len() == TRACE_CAPACITY {
            ring.pop_front();
        }
        ring.push_back((
            h.epoch(),
            h.kind().map_or(u8::MAX, |k| k as u8),
            h.generation(),
            h.slot(),
        ));
    }

    /// The recorded resolutions, oldest first, as `epoch:kind:gen:slot`.
    ///
    /// Empty unless `UNI_GC_TRACE` is set. Drained into a failing invocation's
    /// error so it reaches the caller through the existing incomplete-tag path,
    /// rather than needing a surfacing mechanism of its own.
    #[must_use]
    pub fn trace_breadcrumbs(&self) -> Vec<String> {
        let Ok(ring) = self.trace.lock() else {
            return Vec::new();
        };
        ring.iter()
            .map(|(e, k, g, s)| format!("{e:04x}:{k}:{g:03x}:{s}"))
            .collect()
    }

    /// Resolves a vertex-set handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch.
    pub fn get_set(&self, h: Handle) -> Result<&VertexSet, FnError> {
        self.trace_resolution(h);
        let out = {
            match self.check_epoch_and_kind(h)? {
                HandleKind::VertexSet => self.sets.get(h.slot(), h.generation()),
                _ => Err(error::kind_mismatch("VertexSet")),
            }
        };
        out.map_err(|e| self.with_trace(e))
    }

    /// Resolves a tensor handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch.
    pub fn get_tensor(&self, h: Handle) -> Result<&Tensor, FnError> {
        self.trace_resolution(h);
        let out = {
            match self.check_epoch_and_kind(h)? {
                HandleKind::Tensor => self.tensors.get(h.slot(), h.generation()),
                _ => Err(error::kind_mismatch("Tensor")),
            }
        };
        out.map_err(|e| self.with_trace(e))
    }

    /// Resolves a graph handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch.
    pub fn get_graph(&self, h: Handle) -> Result<&Arc<GraphProjection>, FnError> {
        self.trace_resolution(h);
        let out = {
            match self.check_epoch_and_kind(h)? {
                HandleKind::Graph => self.graphs.get(h.slot(), h.generation()),
                _ => Err(error::kind_mismatch("Graph")),
            }
        };
        out.map_err(|e| self.with_trace(e))
    }

    /// Resolves a walks handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch.
    pub fn get_walks(&self, h: Handle) -> Result<&WalkMatrix, FnError> {
        self.trace_resolution(h);
        let out = {
            match self.check_epoch_and_kind(h)? {
                HandleKind::Walks => self.walks.get(h.slot(), h.generation()),
                _ => Err(error::kind_mismatch("Walks")),
            }
        };
        out.map_err(|e| self.with_trace(e))
    }

    /// Resolves a pair-list handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch.
    pub fn get_pairs(&self, h: Handle) -> Result<&PairList, FnError> {
        self.trace_resolution(h);
        let out = {
            match self.check_epoch_and_kind(h)? {
                HandleKind::Pairs => self.pairs.get(h.slot(), h.generation()),
                _ => Err(error::kind_mismatch("Pairs")),
            }
        };
        out.map_err(|e| self.with_trace(e))
    }

    /// Resolves an edge-mask handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch.
    pub fn get_edge_set(&self, h: Handle) -> Result<&EdgeSet, FnError> {
        self.trace_resolution(h);
        let out = {
            match self.check_epoch_and_kind(h)? {
                HandleKind::EdgeSet => self.edge_sets.get(h.slot(), h.generation()),
                _ => Err(error::kind_mismatch("EdgeSet")),
            }
        };
        out.map_err(|e| self.with_trace(e))
    }

    /// Resolves an arena handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch.
    pub fn get_arena(&self, h: Handle) -> Result<&GraphArena, FnError> {
        self.trace_resolution(h);
        let out = {
            match self.check_epoch_and_kind(h)? {
                HandleKind::Arena => self.arenas.get(h.slot(), h.generation()),
                _ => Err(error::kind_mismatch("Arena")),
            }
        };
        out.map_err(|e| self.with_trace(e))
    }

    /// Resolves an arena handle for mutation.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] for an epoch, kind, or generation mismatch.
    pub fn get_arena_mut(&mut self, h: Handle) -> Result<&mut GraphArena, FnError> {
        self.trace_resolution(h);
        // Snapshot the crumbs before taking the mutable borrow — the same
        // reason `free` does, and why `attach_trace` is a free function.
        let crumbs = self.trace_breadcrumbs();
        let kind = self.check_epoch_and_kind(h)?;
        match kind {
            HandleKind::Arena => self.arenas.get_mut(h.slot(), h.generation()),
            _ => Err(error::kind_mismatch("Arena")),
        }
        .map_err(|e| attach_trace(e, &crumbs))
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
        self.trace_resolution(h);
        let crumbs = self.trace_breadcrumbs();
        self.free_inner(h).map_err(|e| attach_trace(e, &crumbs))
    }

    /// The body of [`Self::free`], split out so the caller can snapshot the
    /// trace before taking the mutable borrow.
    fn free_inner(&mut self, h: Handle) -> Result<usize, FnError> {
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
            HandleKind::EdgeSet => {
                let v = self.edge_sets.free(h.slot(), h.generation())?;
                Ok(v.heap_bytes())
            }
            HandleKind::Arena => {
                let v = self.arenas.free(h.slot(), h.generation())?;
                Ok(v.heap_bytes())
            }
            HandleKind::Levels => Err(error::kind_mismatch("a supported kind")),
        }
    }

    /// Returns the total number of live handles across all kinds.
    #[must_use]
    pub fn live_handles(&self) -> usize {
        self.sets.live_count()
            + self.edge_sets.live_count()
            + self.tensors.live_count()
            + self.graphs.live_count()
            + self.walks.live_count()
            + self.pairs.live_count()
            + self.arenas.live_count()
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

    /// With tracing off the feature is invisible: nothing recorded, and error
    /// messages are byte-identical to what they were before it existed.
    ///
    /// This is the property that lets the hook ship always-compiled, on the
    /// hottest path in the module.
    #[test]
    fn the_trace_is_invisible_when_off() {
        let _guard = TRACE_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        force_tracing_for_test(false);

        let mut t = HandleTable::new(7);
        let h = t.insert_set(VertexSet::with_capacity(4));
        for _ in 0..10 {
            let _ = t.get_set(h);
        }
        assert!(
            t.trace_breadcrumbs().is_empty(),
            "nothing may be recorded when tracing is off"
        );
        let forged = Handle::pack(9, HandleKind::VertexSet, 0, 0);
        let err = t
            .get_set(forged)
            .expect_err("cross-epoch handle is rejected");
        assert!(
            !err.message.contains("gc-trace"),
            "the error must be unchanged when tracing is off: {}",
            err.message
        );
    }

    /// With tracing on, the ring stays bounded and rides *every* handle error —
    /// not just the cross-epoch one.
    ///
    /// The first version of this test only exercised cross-epoch, which was also
    /// the only path `with_trace` covered, so it could not have caught that
    /// use-after-free carried nothing.
    #[test]
    fn the_trace_is_bounded_and_rides_every_handle_error() {
        let _guard = TRACE_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        force_tracing_for_test(true);

        let mut t = HandleTable::new(7);
        let h = t.insert_set(VertexSet::with_capacity(4));
        for _ in 0..(TRACE_CAPACITY * 3) {
            let _ = t.get_set(h);
        }
        assert_eq!(
            t.trace_breadcrumbs().len(),
            TRACE_CAPACITY,
            "the ring must not grow with a long-running guest"
        );

        // Cross-epoch.
        let forged = Handle::pack(9, HandleKind::VertexSet, 0, 0);
        assert!(
            t.get_set(forged)
                .expect_err("forged")
                .message
                .contains("gc-trace"),
            "cross-epoch must carry the trace"
        );
        // Kind mismatch.
        assert!(
            t.get_tensor(h)
                .expect_err("wrong kind")
                .message
                .contains("gc-trace"),
            "a kind mismatch must carry the trace"
        );
        // Use-after-free — the case the published example shows, and the one the
        // original implementation missed entirely.
        t.free(h).expect("free succeeds");
        let stale = t.get_set(h).expect_err("use-after-free");
        assert!(
            stale.message.contains("stale handle"),
            "expected the stale-handle error: {}",
            stale.message
        );
        assert!(
            stale.message.contains("gc-trace"),
            "use-after-free must carry the trace — this is the published example: {}",
            stale.message
        );
        // Double free.
        assert!(
            t.free(h)
                .expect_err("double free")
                .message
                .contains("gc-trace"),
            "a double free must carry the trace"
        );

        FORCE_TRACE.store(FORCE_UNSET, std::sync::atomic::Ordering::Relaxed);
    }

    /// Both trace tests flip a process-global switch, so they must not overlap.
    static TRACE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
}
