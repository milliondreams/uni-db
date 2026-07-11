// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! GraphCompute error codes and constructors (block `0x860`–`0x87F`).
//!
//! These are the pinned error codes from the proposal (§12), carved out of the
//! `0x8xx` algorithm family. Every kernel returns a [`FnError`] rather than
//! panicking, so a forged handle, a drained budget, or a schema mismatch becomes
//! a typed, catchable error the guest receives (proposal §4.2, §5.4). The codes
//! are stable ABI — a guest or a conformance probe matches on them.
//
// Rust guideline compliant

use uni_plugin::errors::FnError;

/// Generation mismatch — a stale handle used after `free` (proposal §12).
pub const STALE_HANDLE: u32 = 0x860;
/// Kind mismatch — e.g. a `VertexSet` handle where a `Tensor` was expected.
pub const KIND_MISMATCH: u32 = 0x861;
/// Shape or dtype mismatch versus a kernel's requirement.
pub const SHAPE_MISMATCH: u32 = 0x862;
/// Cross-session or forged handle — the epoch does not match this session.
pub const EPOCH_MISMATCH: u32 = 0x863;
/// Allocation past the arena byte or handle cap (proposal §5.1).
pub const ARENA_CAP_EXCEEDED: u32 = 0x864;
/// The native-work meter reached zero (proposal §5.1).
pub const BUDGET_EXHAUSTED: u32 = 0x865;
/// A convergence loop hit its iteration cap (proposal §5.2).
pub const ITERATION_LIMIT: u32 = 0x866;
/// A wall-clock deadline elapsed mid-invocation (proposal §5.2).
pub const TIMEOUT: u32 = 0x867;
/// `frontier` was given a Vid absent from the projection (proposal §4.3).
pub const SEED_NOT_IN_PROJECTION: u32 = 0x868;
/// `emit` columns did not match the declared `output_fields` (proposal §4.6).
pub const EMIT_SCHEMA_MISMATCH: u32 = 0x869;
/// A manifest requested a capability slice version the host lacks (§4.3).
pub const SLICE_VERSION_MISMATCH: u32 = 0x86A;
/// A generation or epoch wrap was rejected fail-closed (proposal §4.2).
pub const WRAP_FAIL_CLOSED: u32 = 0x86B;
/// A kernel was called without the `graph-compute` capability grant (§4.6).
pub const CAPABILITY_DENIED: u32 = 0x86C;

/// Builds a `0x860` stale-handle error (use-after-free / generation mismatch).
#[must_use]
pub fn stale_handle() -> FnError {
    FnError::new(STALE_HANDLE, "GraphCompute: stale handle (use-after-free)")
}

/// Builds a `0x861` kind-mismatch error naming the expected kind.
#[must_use]
pub fn kind_mismatch(expected: &str) -> FnError {
    FnError::new(
        KIND_MISMATCH,
        format!("GraphCompute: handle kind mismatch, expected {expected}"),
    )
}

/// Builds a `0x863` epoch-mismatch error (cross-session or forged handle).
#[must_use]
pub fn epoch_mismatch() -> FnError {
    FnError::new(
        EPOCH_MISMATCH,
        "GraphCompute: handle from another session or forged (epoch mismatch)",
    )
}

/// Builds a `0x864` arena-cap-exceeded error from a formatted cause.
#[must_use]
pub fn arena_cap_exceeded(cause: impl Into<String>) -> FnError {
    FnError::new(ARENA_CAP_EXCEEDED, cause.into())
}

/// Builds a `0x865` budget-exhausted error from a formatted cause.
#[must_use]
pub fn budget_exhausted(cause: impl Into<String>) -> FnError {
    FnError::new(BUDGET_EXHAUSTED, cause.into())
}

/// Builds a `0x866` iteration-limit error for a non-converging loop.
#[must_use]
pub fn iteration_limit(cap: usize) -> FnError {
    FnError::new(
        ITERATION_LIMIT,
        format!("GraphCompute: did not converge within {cap} iterations"),
    )
}

/// Builds a `0x868` seed-not-in-projection error for an unmapped Vid.
#[must_use]
pub fn seed_not_in_projection(vid: u64) -> FnError {
    FnError::new(
        SEED_NOT_IN_PROJECTION,
        format!("GraphCompute: seed vid {vid} is not present in the projection"),
    )
}

/// Builds a `0x869` emit-schema-mismatch error from a formatted cause.
#[must_use]
pub fn emit_schema_mismatch(cause: impl Into<String>) -> FnError {
    FnError::new(EMIT_SCHEMA_MISMATCH, cause.into())
}

/// Builds a `0x86B` wrap-fail-closed error for a slot/epoch exhaustion.
#[must_use]
pub fn wrap_fail_closed(cause: impl Into<String>) -> FnError {
    FnError::new(WRAP_FAIL_CLOSED, cause.into())
}
