// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Opaque, generational, kind-tagged handles into a per-invocation table.
//!
//! A [`Handle`] is the only thing a GraphCompute guest ever holds: an opaque
//! `u64` that indexes a slot in the session's [`super::table::HandleTable`]. It
//! carries a session epoch, a value kind, a per-slot generation, and the slot
//! index, packed into 64 bits (proposal §4.2). Every field is checked on
//! resolution so a forged, stale, cross-session, or wrong-kind handle becomes a
//! typed error rather than an out-of-bounds index or a silent wrong read.
//!
//! This packed representation is the in-process (Rhai/PyO3) and Extism lowering.
//! For WASM the component-model `resource` type is used instead, where the id is
//! a runtime-owned table index the guest genuinely cannot fabricate; the packing
//! here is defense-in-depth, not capability security (proposal §4.2, decision D1).
//
// Rust guideline compliant

/// Bit width of the slot index field (low bits).
const SLOT_BITS: u32 = 32;
/// Bit width of the per-slot generation field.
const GEN_BITS: u32 = 12;
/// Bit width of the value-kind tag field.
const KIND_BITS: u32 = 4;
/// Bit width of the session-epoch field (high bits).
const EPOCH_BITS: u32 = 16;

const SLOT_SHIFT: u32 = 0;
const GEN_SHIFT: u32 = SLOT_SHIFT + SLOT_BITS;
const KIND_SHIFT: u32 = GEN_SHIFT + GEN_BITS;
const EPOCH_SHIFT: u32 = KIND_SHIFT + KIND_BITS;

const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
const GEN_MASK: u64 = (1 << GEN_BITS) - 1;
const KIND_MASK: u64 = (1 << KIND_BITS) - 1;
const EPOCH_MASK: u64 = (1 << EPOCH_BITS) - 1;

/// The largest generation a slot can reach before it must be retired.
///
/// A 12-bit generation wraps after 4096 reuse cycles; on reaching this value the
/// slot is retired rather than recycled into ambiguity, so an old handle can
/// never collide with a new allocation (proposal §4.2, fail-closed wrap).
pub const MAX_GENERATION: u16 = GEN_MASK as u16;

/// The largest session epoch before the process must reject new sessions.
///
/// A 16-bit epoch wraps after 65_536 sessions in one process; on wrap the
/// session factory fails closed (proposal §4.2).
pub const MAX_EPOCH: u16 = EPOCH_MASK as u16;

/// The kind of value a [`Handle`] refers to.
///
/// The kind is checked on every resolution so a handle minted for one kind can
/// never be silently used where another is expected (proposal §4.2 / error
/// `0x861`). v1 uses only [`HandleKind::VertexSet`], [`HandleKind::Tensor`], and
/// [`HandleKind::Graph`]; `Walks` and `Levels` are reserved for the stochastic
/// and Brandes kernels added in later phases.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum HandleKind {
    /// A set of vertex slots (a frontier / visited mask).
    VertexSet = 0,
    /// A shaped, Arrow-backed per-vertex value map.
    Tensor = 1,
    /// A projected graph (immutable CSR).
    Graph = 2,
    /// A random-walk matrix (reserved; group-7 stochastic kernels).
    Walks = 3,
    /// Per-vertex BFS depth + path counts (reserved; Brandes kernels).
    Levels = 4,
    /// A per-edge `(src, dst, value)` pair list (all-pairs overlap / k-truss).
    Pairs = 5,
    /// A set of edge indices (an edge mask, proposal §5 `Shape::E`).
    EdgeSet = 6,
}

impl HandleKind {
    /// Returns the 4-bit tag stored in a packed handle.
    #[must_use]
    fn tag(self) -> u64 {
        self as u64
    }

    /// Reconstructs a kind from its packed 4-bit tag, if valid.
    #[must_use]
    fn from_tag(tag: u64) -> Option<Self> {
        match tag {
            0 => Some(HandleKind::VertexSet),
            1 => Some(HandleKind::Tensor),
            2 => Some(HandleKind::Graph),
            3 => Some(HandleKind::Walks),
            4 => Some(HandleKind::Levels),
            5 => Some(HandleKind::Pairs),
            6 => Some(HandleKind::EdgeSet),
            _ => None,
        }
    }
}

/// An opaque, generational, kind-tagged handle into a session's handle table.
///
/// Packed as `[ epoch:16 | kind:4 | generation:12 | slot:32 ]`. The guest treats
/// it as an opaque `u64`; the host unpacks and validates it on every kernel call.
///
/// # Examples
/// ```
/// use uni_plugin_builtin::algorithms::graph_compute::handle::{Handle, HandleKind};
///
/// let h = Handle::pack(7, HandleKind::Tensor, 3, 42);
/// assert_eq!(h.epoch(), 7);
/// assert_eq!(h.kind(), Some(HandleKind::Tensor));
/// assert_eq!(h.generation(), 3);
/// assert_eq!(h.slot(), 42);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Handle(u64);

impl Handle {
    /// Packs `(epoch, kind, generation, slot)` into an opaque handle.
    ///
    /// # Panics
    /// Panics if `generation > MAX_GENERATION` — a programming error in the
    /// table, which must retire a slot before its generation overflows the field.
    #[must_use]
    pub fn pack(epoch: u16, kind: HandleKind, generation: u16, slot: u32) -> Self {
        assert!(
            u64::from(generation) <= GEN_MASK,
            "generation overflows 12-bit field; slot must be retired on wrap"
        );
        let bits = (u64::from(epoch) & EPOCH_MASK) << EPOCH_SHIFT
            | (kind.tag() & KIND_MASK) << KIND_SHIFT
            | (u64::from(generation) & GEN_MASK) << GEN_SHIFT
            | (u64::from(slot) & SLOT_MASK) << SLOT_SHIFT;
        Self(bits)
    }

    /// Returns the raw packed `u64` for transport across a loader boundary.
    #[must_use]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Reconstructs a handle from a raw `u64` received from a guest.
    ///
    /// No validation happens here — a raw `u64` is untrusted. Validation is the
    /// job of the handle table on resolution, which checks epoch, kind, and
    /// generation (proposal §4.2).
    #[must_use]
    pub fn from_u64(bits: u64) -> Self {
        Self(bits)
    }

    /// Returns the session epoch encoded in the handle.
    #[must_use]
    pub fn epoch(self) -> u16 {
        ((self.0 >> EPOCH_SHIFT) & EPOCH_MASK) as u16
    }

    /// Returns the value kind, or `None` if the tag is not a known kind.
    #[must_use]
    pub fn kind(self) -> Option<HandleKind> {
        HandleKind::from_tag((self.0 >> KIND_SHIFT) & KIND_MASK)
    }

    /// Returns the per-slot generation encoded in the handle.
    #[must_use]
    pub fn generation(self) -> u16 {
        ((self.0 >> GEN_SHIFT) & GEN_MASK) as u16
    }

    /// Returns the slot index encoded in the handle.
    #[must_use]
    pub fn slot(self) -> u32 {
        ((self.0 >> SLOT_SHIFT) & SLOT_MASK) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_unpack_roundtrips_all_fields() {
        let h = Handle::pack(0xBEEF, HandleKind::Levels, 0xABC, 0xDEAD_BEEF);
        assert_eq!(h.epoch(), 0xBEEF);
        assert_eq!(h.kind(), Some(HandleKind::Levels));
        assert_eq!(h.generation(), 0xABC);
        assert_eq!(h.slot(), 0xDEAD_BEEF);
    }

    #[test]
    fn fields_are_independent() {
        // Changing one field must not bleed into another (mask/shift correctness).
        let base = Handle::pack(1, HandleKind::VertexSet, 0, 0);
        let hi = Handle::pack(1, HandleKind::VertexSet, 0, u32::MAX);
        assert_eq!(base.epoch(), hi.epoch());
        assert_eq!(base.kind(), hi.kind());
        assert_eq!(hi.slot(), u32::MAX);
        assert_eq!(hi.generation(), 0);
    }

    #[test]
    fn max_generation_fits_field() {
        let h = Handle::pack(0, HandleKind::Tensor, MAX_GENERATION, 5);
        assert_eq!(h.generation(), MAX_GENERATION);
    }

    #[test]
    fn unknown_kind_tag_is_none() {
        // Tags 5..=15 are unused; a forged handle carrying one has no kind.
        let forged = Handle::from_u64(0xF << KIND_SHIFT);
        assert_eq!(forged.kind(), None);
    }

    #[test]
    #[should_panic(expected = "generation overflows")]
    fn pack_panics_on_generation_overflow() {
        let _ = Handle::pack(0, HandleKind::Tensor, MAX_GENERATION + 1, 0);
    }
}
