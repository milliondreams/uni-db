// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Closed-form serializability witnesses for the invariant-oracle stress tests.
//!
//! A general serializability checker (Elle-style cycle detection over the
//! read/write dependency graph) is powerful but easy to get subtly wrong — and a
//! *wrong* oracle is worse than none, because it manufactures false confidence.
//! Instead we use workloads whose serializability has a **closed-form
//! invariant**, and assert that invariant directly. Each is a sound witness:
//!
//! - **Counter RMW** (`N` blind increments, retried on conflict): under any
//!   serial order the final value equals the number of committed increments.
//!   [`assert_counter`].
//! - **Bank transfers** (move `x` from a to b, read-check-write): every serial
//!   order conserves the total and — if each transfer guards against overdraft —
//!   keeps balances non-negative. [`assert_conserved`] + [`assert_non_negative`].
//!
//! A violation of any of these is a non-serializable execution (a lost update or
//! a write skew), so the assertions ARE the serializability checks for their
//! workloads.

/// The headline lost-update check: a counter incremented by exactly the number
/// of transactions that committed must read back that many.
#[track_caller]
pub fn assert_counter(observed: i64, committed_increments: i64) {
    assert_eq!(
        observed, committed_increments,
        "lost update: counter is {observed} but {committed_increments} increments committed \
         (a serial execution would yield {committed_increments})"
    );
}

/// Write-skew / lost-update sentinel: the sum of balances must equal the seeded
/// total, regardless of how many transfers committed.
#[track_caller]
pub fn assert_conserved(balances: &[i64], expected_total: i64) {
    let sum: i64 = balances.iter().sum();
    assert_eq!(
        sum, expected_total,
        "value not conserved: balances sum to {sum}, expected {expected_total} \
         (a non-serializable transfer double-counted)"
    );
}

/// Overdraft guard: with a read-check-write that refuses to go negative, no
/// serial order ever produces a negative balance. A negative here is a write
/// skew that slipped through.
#[track_caller]
pub fn assert_non_negative(balances: &[i64]) {
    if let Some((i, b)) = balances.iter().enumerate().find(|&(_, &v)| v < 0) {
        panic!("write skew: account {i} went negative ({b}); the overdraft check was bypassed");
    }
}
