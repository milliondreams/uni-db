// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! GraphCompute — a guest-authorable coarse-kernel graph API.
//!
//! This module implements the substrate described in the GraphCompute proposal
//! (`docs/proposals/graphcompute_plugin_api_2026-07-10.md`): a per-invocation
//! session that lets guest algorithms drive coarse, native graph kernels over
//! opaque handles, so only handles and scalars ever cross the host boundary
//! ("conductor, not worker").
//!
//! # Phase 0 — determinism + budget substrate
//! The first slice shipped here is the *metering* substrate, which has value
//! even before any kernel exists:
//!
//! - [`WorkBudget`] — a native-work meter charged per kernel proportional to the
//!   work actually done (Σ degree, nnz, `|set|`, `|V|`, …). Every existing uni
//!   budget (WASM fuel, Rhai `max_operations`) counts *interpreter* ops, which a
//!   guest can sidestep: one cheap interpreter call can trigger `O(E)` native
//!   work the meter never sees. [`WorkBudget`] meters that native work instead
//!   and fails closed when drained (proposal §5.1).
//! - [`Arena`] — a hard cap on live host-side handle memory, enforced at
//!   *allocation* time because none of the loaders' own memory limits observe
//!   the host arena (proposal §5.1).
//!
//! The kernel catalog, generational handle table, and loader shims are layered
//! on this substrate in later phases.
//
// Rust guideline compliant

pub mod dispatch;
pub mod error;
pub mod first_party;
pub mod handle;
pub mod session;
pub mod table;
pub mod value;

#[cfg(test)]
mod differential_tests;

pub mod provider;

pub use dispatch::{GraphComputeRegistry, KernelRequest, KernelResponse, SharedRegistry};
pub use handle::{Handle, HandleKind};
pub use session::{
    AlgoSession, Direction, EwiseOp, GraphCompute, MapOp, Norm, Predicate, ReduceOp, Semiring,
};
pub use table::HandleTable;
pub use value::{DType, Scalar, Shape, Tensor, VertexSet};

/// Native-work budget multiplier applied to graph size to derive the default cap.
///
/// The default budget is `min(DEFAULT_WORK_EDGE_MULTIPLIER * (|V| + |E|),
/// DEFAULT_WORK_ABS_CEILING)`. The multiplier alone is unbounded on a very large
/// projection, which is why it is combined with an absolute ceiling (proposal
/// decision D3 / §12).
///
/// The multiplier is the per-invocation *pass allowance*: an iterative algorithm
/// doing `K` iterations of a few `O(V + E)` passes each needs roughly
/// `K * passes * (|V| + |E|)` work. The proposal's original starting point of
/// `100` (against `|E|` only) tripped legitimate iterative algorithms at ~25
/// iterations — inconsistent with the `DEFAULT_MAX_SUPERSTEPS = 10_000` cap and
/// undercounting the `O(V)` kernels the meter charges (§5.1). Tuned to `10_000`
/// (against `|V| + |E|`) so a default algorithm run fits comfortably below the
/// superstep cap while an unbounded loop still hits the finite ceiling. §12
/// explicitly permits tuning these starting-point values.
pub const DEFAULT_WORK_EDGE_MULTIPLIER: u64 = 10_000;

/// Absolute ceiling on the native-work budget, in work units (≈ edges touched).
///
/// Caps the budget on very large projections where the `|E|` multiple alone
/// would be effectively unbounded (proposal decision D3 / §12).
pub const DEFAULT_WORK_ABS_CEILING: u64 = 1_000_000_000;

/// In-kernel budget-check granularity, in work units.
///
/// Expensive kernels (e.g. expanding a celebrity super-node) must re-check the
/// budget every `BUDGET_CHECK_CHUNK` units of work rather than only between
/// calls, so a single kernel invocation cannot blow far past the cap. This
/// bounds the overshoot to one chunk (proposal §5.1 / test P0-4).
pub const BUDGET_CHECK_CHUNK: u64 = 65_536;

/// Default hard cap on total live bytes in the handle arena (256 MiB).
///
/// Enforced at allocation time (proposal §5.1). Host-side arena allocations are
/// invisible to the loaders' own memory limits, so this cap is the backstop.
pub const DEFAULT_ARENA_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Default hard cap on the number of simultaneously live handles.
///
/// Also the generation-wrap horizon per slot: a slot's 12-bit generation wraps
/// after this many reuse cycles, at which point the slot is retired rather than
/// recycled into ambiguity (proposal §4.2 / §12).
pub const DEFAULT_ARENA_MAX_HANDLES: usize = 4_096;

/// Default cap on convergence-loop iterations before an `IterationLimit` error.
///
/// Mirrors the pregel superstep cap; a convergence loop reaching this bound
/// without settling is reported as incomplete, never truncated (proposal §5.2 /
/// §12).
pub const DEFAULT_MAX_SUPERSTEPS: usize = 10_000;

/// Monotonic per-process session-epoch source.
///
/// Each [`AlgoSession`] is stamped with a fresh epoch so a handle minted in one
/// invocation is structurally rejected in any other (proposal §4.2). Wrapping
/// after 65_536 sessions is acceptable because handles never escape their CALL;
/// the epoch is defense-in-depth, not the primary lifetime bound.
static SESSION_EPOCH: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(1);

/// Returns the next per-process session epoch, never `0`.
///
/// Epoch `0` is skipped so the all-zeros handle `Handle::from_u64(0)` (epoch 0,
/// kind `VertexSet`, gen 0, slot 0) can never match a live session — closing a
/// forged-handle alias on epoch wrap (proposal §4.2 fail-closed intent). Full
/// wrap *rejection* (erroring after 65_535 sessions) remains a follow-up; this
/// removes the acute aliasing without changing the infallible signature.
#[must_use]
pub fn next_session_epoch() -> u16 {
    let e = SESSION_EPOCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if e == 0 {
        SESSION_EPOCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    } else {
        e
    }
}

/// The native-work meter for a single GraphCompute invocation.
///
/// A [`WorkBudget`] is charged per kernel in proportion to the native work that
/// kernel performs — `expand` charges Σ frontier degree, `spmv` charges nnz, set
/// operations charge `|set|`, and the `O(V)` kernels charge `|V|`. It fails
/// closed the moment charged work would exceed the budget, which is what bounds
/// a guest loop such as `while !is_empty(f) { f = expand(...) }` that is cheap in
/// interpreter ops but drives the host at full native speed (proposal §5.1).
///
/// # Examples
/// ```
/// use uni_plugin_builtin::algorithms::graph_compute::WorkBudget;
///
/// let mut budget = WorkBudget::new(100);
/// assert!(budget.try_charge(60).is_ok());
/// assert_eq!(budget.remaining(), 40);
/// // Charging past the budget fails closed and clamps `spent` to the total.
/// assert!(budget.try_charge(50).is_err());
/// assert_eq!(budget.remaining(), 0);
/// ```
#[derive(Debug, Clone)]
pub struct WorkBudget {
    total: u64,
    spent: u64,
}

/// A [`WorkBudget`] was drained: charged native work exceeded the budget.
///
/// Surfaced to the guest as GraphCompute error `0x865` /
/// [`uni_common::GraphComputeIncompleteReason::Exhausted`] (proposal §12).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkBudgetExhausted {
    /// Total work charged when the budget was exceeded (clamped to `budget`).
    pub spent: u64,
    /// The configured budget in work units.
    pub budget: u64,
}

impl std::fmt::Display for WorkBudgetExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "native-work budget exhausted: charged {}/{} work units",
            self.spent, self.budget
        )
    }
}

impl WorkBudget {
    /// Creates a budget admitting `total` work units before failing closed.
    #[must_use]
    pub fn new(total: u64) -> Self {
        Self { total, spent: 0 }
    }

    /// Derives the default budget from the graph size via the pinned formula.
    ///
    /// Returns a budget of `min(DEFAULT_WORK_EDGE_MULTIPLIER * (vertices + edges
    /// + 1), DEFAULT_WORK_ABS_CEILING)`. The `+ 1` keeps an edgeless graph from
    /// yielding a zero budget; the multiply saturates rather than overflows so a
    /// pathologically large graph still yields the ceiling (proposal decision D3
    /// / §12). Both `|V|` and `|E|` are included because the meter charges the
    /// `O(V)` kernels as well as the `O(E)` ones (§5.1).
    #[must_use]
    pub fn from_graph_size(vertices: u64, edges: u64) -> Self {
        let size = vertices.saturating_add(edges).saturating_add(1);
        let scaled = size.saturating_mul(DEFAULT_WORK_EDGE_MULTIPLIER);
        Self::new(scaled.min(DEFAULT_WORK_ABS_CEILING))
    }

    /// Derives the default budget from an edge count (delegates to
    /// [`WorkBudget::from_graph_size`] with no separate vertex term).
    #[must_use]
    pub fn from_edge_count(edge_count: u64) -> Self {
        Self::from_graph_size(0, edge_count)
    }

    /// Charges `units` of native work, failing closed when the budget is exceeded.
    ///
    /// On success the internal counter advances by `units`. When the charge
    /// would exceed the total, `spent` is clamped to the total and an error is
    /// returned; further charges continue to fail. Expensive kernels should call
    /// this every [`BUDGET_CHECK_CHUNK`] units so overshoot is bounded to one
    /// chunk (proposal §5.1).
    ///
    /// # Errors
    /// Returns [`WorkBudgetExhausted`] when the accumulated charge exceeds the
    /// budget.
    pub fn try_charge(&mut self, units: u64) -> Result<(), WorkBudgetExhausted> {
        let next = self.spent.saturating_add(units);
        if next > self.total {
            self.spent = self.total;
            return Err(WorkBudgetExhausted {
                spent: self.total,
                budget: self.total,
            });
        }
        self.spent = next;
        Ok(())
    }

    /// Returns the number of work units charged so far.
    #[must_use]
    pub fn spent(&self) -> u64 {
        self.spent
    }

    /// Returns the number of work units remaining before the budget fails closed.
    #[must_use]
    pub fn remaining(&self) -> u64 {
        self.total.saturating_sub(self.spent)
    }

    /// Returns the total budget the meter was configured with.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Returns `true` once the budget is fully spent.
    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.spent >= self.total
    }
}

/// A [`WorkBudget`] wrapper that charges a large kernel's work in chunks.
///
/// Drives `total_units` of work through [`WorkBudget::try_charge`] in
/// [`BUDGET_CHECK_CHUNK`]-sized increments, stopping at the first drained chunk
/// so a single super-node expansion overshoots by at most one chunk (proposal
/// §5.1 / test P0-4). Kernels that already loop naturally over their work should
/// instead call [`WorkBudget::try_charge`] once per processed chunk.
///
/// # Errors
/// Returns [`WorkBudgetExhausted`] if the budget drains before `total_units` is
/// fully charged.
pub fn charge_in_chunks(
    budget: &mut WorkBudget,
    total_units: u64,
) -> Result<(), WorkBudgetExhausted> {
    let mut remaining = total_units;
    while remaining > 0 {
        let chunk = remaining.min(BUDGET_CHECK_CHUNK);
        budget.try_charge(chunk)?;
        remaining -= chunk;
    }
    Ok(())
}

/// A hard cap on live host-side handle memory, enforced at allocation time.
///
/// Guest-visible kernels are pure: each returns a *new* `O(V)` handle, so a long
/// convergence loop would otherwise pile up dead buffers against host memory.
/// The [`Arena`] tracks live bytes and handle count and refuses an allocation
/// that would breach either cap, since the loaders' own memory limits never see
/// these host-side allocations (proposal §4.2 / §5.1). Reclaim (`free`, session
/// drop) is the mechanism; this cap is the backstop.
///
/// # Examples
/// ```
/// use uni_plugin_builtin::algorithms::graph_compute::Arena;
///
/// let mut arena = Arena::new(1024, 4);
/// arena.try_alloc(512).unwrap();
/// assert_eq!(arena.bytes_live(), 512);
/// // Exceeding the byte cap fails closed without allocating.
/// assert!(arena.try_alloc(600).is_err());
/// arena.free(512);
/// assert_eq!(arena.bytes_live(), 0);
/// ```
#[derive(Debug, Clone)]
pub struct Arena {
    max_bytes: usize,
    max_handles: usize,
    bytes_live: usize,
    handles_live: usize,
}

/// An [`Arena`] allocation was refused because it would breach a cap.
///
/// Surfaced to the guest as GraphCompute error `0x864` (`ArenaCapExceeded`,
/// proposal §12).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArenaCapExceeded {
    /// The requested allocation would exceed the live-bytes cap.
    Bytes {
        /// Bytes that would be live after the allocation.
        requested: usize,
        /// The configured live-bytes cap.
        cap: usize,
    },
    /// The requested allocation would exceed the live-handle cap.
    Handles {
        /// The configured live-handle cap.
        cap: usize,
    },
}

impl std::fmt::Display for ArenaCapExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArenaCapExceeded::Bytes { requested, cap } => write!(
                f,
                "arena byte cap exceeded: {requested} bytes would be live (cap {cap})"
            ),
            ArenaCapExceeded::Handles { cap } => {
                write!(f, "arena handle cap exceeded: cap {cap} handles")
            }
        }
    }
}

impl Arena {
    /// Creates an arena capped at `max_bytes` live bytes and `max_handles` handles.
    #[must_use]
    pub fn new(max_bytes: usize, max_handles: usize) -> Self {
        Self {
            max_bytes,
            max_handles,
            bytes_live: 0,
            handles_live: 0,
        }
    }

    /// Records a handle allocation of `bytes`, failing closed if a cap is breached.
    ///
    /// On success the live-bytes and live-handle counters advance; on failure
    /// neither is touched (the allocation must not proceed).
    ///
    /// # Errors
    /// Returns [`ArenaCapExceeded`] when the allocation would breach either the
    /// byte cap or the handle cap.
    pub fn try_alloc(&mut self, bytes: usize) -> Result<(), ArenaCapExceeded> {
        if self.handles_live + 1 > self.max_handles {
            return Err(ArenaCapExceeded::Handles {
                cap: self.max_handles,
            });
        }
        let next_bytes = self.bytes_live.saturating_add(bytes);
        if next_bytes > self.max_bytes {
            return Err(ArenaCapExceeded::Bytes {
                requested: next_bytes,
                cap: self.max_bytes,
            });
        }
        self.bytes_live = next_bytes;
        self.handles_live += 1;
        Ok(())
    }

    /// Records that a handle holding `bytes` was freed.
    ///
    /// Both counters saturate at zero so a double free cannot underflow, though
    /// the handle table's generation check is the real guard against that
    /// (proposal §4.2).
    pub fn free(&mut self, bytes: usize) {
        self.bytes_live = self.bytes_live.saturating_sub(bytes);
        self.handles_live = self.handles_live.saturating_sub(1);
    }

    /// Returns the number of bytes currently accounted live.
    #[must_use]
    pub fn bytes_live(&self) -> usize {
        self.bytes_live
    }

    /// Returns the number of handles currently accounted live.
    #[must_use]
    pub fn handles_live(&self) -> usize {
        self.handles_live
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_budget_charges_and_reports_remaining() {
        let mut b = WorkBudget::new(100);
        assert_eq!(b.remaining(), 100);
        b.try_charge(30).unwrap();
        assert_eq!(b.spent(), 30);
        assert_eq!(b.remaining(), 70);
        assert!(!b.is_exhausted());
    }

    #[test]
    fn work_budget_exhaustion_is_hard_error() {
        // P0-3 primitive: a loop that keeps charging eventually fails closed,
        // and `spent` never runs away past the total.
        let mut b = WorkBudget::new(50);
        let mut charges = 0;
        let mut hit = false;
        for _ in 0..1_000 {
            if b.try_charge(10).is_err() {
                hit = true;
                break;
            }
            charges += 1;
        }
        assert!(hit, "budget must eventually fail closed");
        assert_eq!(charges, 5);
        assert_eq!(b.spent(), 50);
        assert!(b.is_exhausted());
    }

    #[test]
    fn chunked_charge_bounds_overshoot() {
        // P0-4 primitive: charging a super-node's worth of work in chunks stops
        // at the first drained chunk, so overshoot is at most one chunk.
        let mut b = WorkBudget::new(BUDGET_CHECK_CHUNK * 3 + 10);
        let err = charge_in_chunks(&mut b, BUDGET_CHECK_CHUNK * 100)
            .expect_err("a 100-chunk charge must drain a 3-chunk budget");
        assert_eq!(err.budget, BUDGET_CHECK_CHUNK * 3 + 10);
        // Spent is clamped to the total; it never exceeds budget + one chunk.
        assert!(b.spent() <= b.total());
    }

    #[test]
    fn default_budget_from_edges_is_finite_and_capped() {
        // P0-9 primitive: the production default is finite for any graph size.
        let small = WorkBudget::from_graph_size(400, 1_000);
        assert_eq!(
            small.total(),
            (400 + 1_000 + 1) * DEFAULT_WORK_EDGE_MULTIPLIER
        );
        let huge = WorkBudget::from_graph_size(u64::MAX, u64::MAX);
        assert_eq!(huge.total(), DEFAULT_WORK_ABS_CEILING);
        // An edgeless graph still gets a non-zero budget (the `+ 1` floor).
        assert!(WorkBudget::from_graph_size(0, 0).total() > 0);
        assert!(
            huge.total() > 0,
            "default budget must be finite and non-zero"
        );
    }

    #[test]
    fn arena_cap_enforced_at_allocation() {
        // P0-5 primitive: allocations past the byte cap fail at the allocating
        // call and `bytes_live` never exceeds the cap.
        let mut a = Arena::new(1_000, 100);
        a.try_alloc(600).unwrap();
        let err = a
            .try_alloc(600)
            .expect_err("second alloc breaches byte cap");
        assert!(matches!(err, ArenaCapExceeded::Bytes { cap: 1_000, .. }));
        assert_eq!(a.bytes_live(), 600);
        assert!(a.bytes_live() <= 1_000);
    }

    #[test]
    fn arena_handle_cap_enforced() {
        let mut a = Arena::new(usize::MAX, 2);
        a.try_alloc(1).unwrap();
        a.try_alloc(1).unwrap();
        let err = a
            .try_alloc(1)
            .expect_err("third handle breaches handle cap");
        assert!(matches!(err, ArenaCapExceeded::Handles { cap: 2 }));
        assert_eq!(a.handles_live(), 2);
    }

    #[test]
    fn arena_free_reclaims() {
        let mut a = Arena::new(1_000, 10);
        a.try_alloc(400).unwrap();
        a.try_alloc(400).unwrap();
        a.free(400);
        assert_eq!(a.bytes_live(), 400);
        assert_eq!(a.handles_live(), 1);
        // Saturating free never underflows even on an over-free.
        a.free(1_000);
        a.free(1_000);
        assert_eq!(a.bytes_live(), 0);
        assert_eq!(a.handles_live(), 0);
    }
}
