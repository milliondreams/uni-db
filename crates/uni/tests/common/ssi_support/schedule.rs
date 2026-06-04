// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Conflict-assertion predicates and a concurrency barrier for SSI tests.
//!
//! ## Deterministic interleaving without barriers
//!
//! Two `Transaction` handles can be interleaved into an exact, reproducible
//! schedule simply by awaiting their operations in the order you want, in a
//! single task:
//!
//! ```ignore
//! let ta = s_a.tx().await?;          // T1 begins
//! let tb = s_b.tx().await?;          // T2 begins
//! ta.query("MATCH (x) ...").await?;  // T1 reads
//! tb.execute("SET x ...").await?;    // T2 writes
//! tb.commit().await?;                // T2 commits (takes flush_lock alone)
//! assert_serialization_conflict(ta.commit().await);  // T1 must abort
//! ```
//!
//! No barrier is needed because nothing blocks: each await runs to completion
//! before the next begins, and the only contention point — `flush_lock` inside
//! `commit()` — is reached by one transaction at a time. Use [`barrier`] only
//! for scenarios where a step genuinely *blocks* on another task (e.g. a second
//! `FOR UPDATE` waiting on a lock the first task holds).

use std::sync::Arc;

use tokio::sync::Barrier;
use uni_db::UniError;

/// A shared rendezvous point for `n` concurrent tasks. Each calls `.wait()` to
/// proceed only once all `n` have arrived.
pub fn barrier(n: usize) -> Arc<Barrier> {
    Arc::new(Barrier::new(n))
}

/// `true` for the commit-time contention errors OCC surfaces as retriable.
pub fn is_conflict(e: &UniError) -> bool {
    matches!(
        e,
        UniError::SerializationConflict { .. } | UniError::ConstraintConflict { .. }
    )
}

/// Asserts the result is a read-write / write-write serialization conflict.
#[track_caller]
pub fn assert_serialization_conflict<T: std::fmt::Debug>(r: Result<T, UniError>) {
    match r {
        Err(UniError::SerializationConflict { .. }) => {}
        other => panic!("expected SerializationConflict, got {other:?}"),
    }
}

/// Asserts the result is a unique-key (serializable MERGE) conflict.
#[track_caller]
pub fn assert_constraint_conflict<T: std::fmt::Debug>(r: Result<T, UniError>) {
    match r {
        Err(UniError::ConstraintConflict { .. }) => {}
        other => panic!("expected ConstraintConflict, got {other:?}"),
    }
}

/// Asserts the result is *some* retriable conflict (serialization or constraint).
#[track_caller]
pub fn assert_conflict<T: std::fmt::Debug>(r: Result<T, UniError>) {
    match r {
        Err(e) if is_conflict(&e) => {}
        other => panic!("expected a conflict, got {other:?}"),
    }
}

/// Asserts the result committed cleanly.
#[track_caller]
pub fn assert_committed<T: std::fmt::Debug>(r: Result<T, UniError>) {
    if let Err(e) = r {
        panic!("expected commit to succeed, got {e:?}");
    }
}
