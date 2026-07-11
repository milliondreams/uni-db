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

/// Builds a `0x862` shape/dtype-mismatch error from a formatted cause.
///
/// Raised when a kernel's operands disagree in length or element type — e.g. an
/// `f64`-only kernel handed an `i64` tensor, or `ewise` on two different lengths
/// (proposal §4.2). Distinguishes a genuine shape fault from a forged handle.
#[must_use]
pub fn shape_mismatch(cause: impl Into<String>) -> FnError {
    FnError::new(SHAPE_MISMATCH, cause.into())
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

/// Builds a `0x867` timeout error for a wall-clock deadline elapsing mid-run.
#[must_use]
pub fn timeout() -> FnError {
    FnError::new(
        TIMEOUT,
        "GraphCompute: invocation wall-clock deadline exceeded",
    )
}

/// Builds a tagged incomplete-diagnostic message from a kernel [`FnError`].
///
/// Returns `Some(tagged_message)` when `err`'s code names a §5.2 incomplete
/// outcome — budget exhaustion (`0x865`), an iteration cap (`0x866`), or a
/// wall-clock deadline (`0x867`) — encoding the structured diagnostics behind
/// [`uni_common::GRAPH_COMPUTE_INCOMPLETE_TAG`] so the query API boundary can
/// recover a typed [`uni_common::UniError::GraphComputeIncomplete`]. Any other
/// error (a forged handle, a bad seed) is not an incomplete outcome and yields
/// `None`, so the caller reports it as an ordinary execution error.
#[must_use]
pub fn incomplete_tag_for(
    err: &FnError,
    algorithm: &str,
    elapsed_ms: u64,
    iterations: u64,
    work_charged: u64,
    work_budget: u64,
) -> Option<String> {
    let reason = uni_common::GraphComputeIncompleteReason::from_error_code(err.code)?;
    Some(
        uni_common::GraphComputeIncomplete {
            reason,
            algorithm: algorithm.to_string(),
            elapsed_ms,
            iterations,
            work_charged,
            work_budget,
        }
        .to_tagged_message(),
    )
}

/// Classifies a guest invocation that errored into a §5.2 incomplete outcome,
/// from host-observable state, if one applies.
///
/// A sandboxed guest error does not carry a numeric kernel code across the loader
/// boundary, so the loader adapters instead inspect the host-side session after
/// the guest returns: an elapsed wall-clock deadline is a `Timeout` (`0x867`); a
/// fully-drained native-work budget is `Exhausted` (`0x865`). Returns the tagged
/// diagnostic message when one applies, or `None` when the error is an ordinary
/// guest fault the adapter should report verbatim. (`IterationLimit` is a native
/// first-party outcome only — a guest expresses its own loop bound, invisible to
/// the host — so it is never inferred here.)
#[must_use]
pub fn incomplete_tag_after_guest(
    algorithm: &str,
    deadline_elapsed: bool,
    work_charged: u64,
    work_budget: u64,
    elapsed_ms: u64,
) -> Option<String> {
    let reason = if deadline_elapsed {
        uni_common::GraphComputeIncompleteReason::Timeout
    } else if work_budget > 0 && work_charged >= work_budget {
        uni_common::GraphComputeIncompleteReason::Exhausted
    } else {
        return None;
    };
    Some(
        uni_common::GraphComputeIncomplete {
            reason,
            algorithm: algorithm.to_string(),
            elapsed_ms,
            iterations: 0,
            work_charged,
            work_budget,
        }
        .to_tagged_message(),
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
