// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Bounded retry policy for optimistic-concurrency transactions.
//!
//! Optimistic concurrency control surfaces commit conflicts as retriable errors
//! (see [`UniError::is_retriable`](uni_common::UniError::is_retriable)).
//! [`RetryOptions`] tunes how
//! [`Session::transact_with_retry`](crate::Session::transact_with_retry) re-runs a
//! transaction closure when one occurs: how many attempts to make, and the
//! jittered exponential backoff between them.

use std::time::Duration;

/// Tuning for [`Session::transact_with_retry`](crate::Session::transact_with_retry).
///
/// Defaults are sized for low-latency contention (sub-millisecond first retry)
/// so they stay cheap under test and in hot loops; raise `base_backoff` /
/// `max_backoff` for workloads where a conflicting commit takes longer to clear.
#[derive(Debug, Clone)]
pub struct RetryOptions {
    /// Total attempts, including the first; `1` disables retrying.
    pub max_attempts: u32,
    /// Backoff before the second attempt; doubles each attempt up to `max_backoff`.
    pub base_backoff: Duration,
    /// Upper bound on any single backoff sleep.
    pub max_backoff: Duration,
    /// Fractional jitter in `0.0..=1.0` applied to each sleep (`0.5` is ±50%).
    pub jitter: f64,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            // Five attempts clears all but pathological contention while bounding
            // worst-case latency; callers needing more can raise it.
            max_attempts: 5,
            // Sub-millisecond base keeps the common 1-retry case fast; jittered
            // exponential growth de-correlates a thundering herd of retriers.
            base_backoff: Duration::from_micros(200),
            max_backoff: Duration::from_millis(50),
            jitter: 0.5,
        }
    }
}

impl RetryOptions {
    /// Sleeps before `attempt`, where `attempt == 2` is the first retry.
    ///
    /// Computes `base_backoff * 2^(attempt - 2)`, caps it at `max_backoff`, then
    /// scales by `1 ± jitter` so competing retriers spread out instead of
    /// colliding again on the same schedule.
    pub(crate) async fn backoff(&self, attempt: u32) {
        // Cap the shift so `1u32 << steps` cannot overflow; saturating_mul then
        // clamps the resulting Duration at `max_backoff` anyway.
        let steps = attempt.saturating_sub(2).min(20);
        let mut delay = self
            .base_backoff
            .saturating_mul(1u32 << steps)
            .min(self.max_backoff);
        if self.jitter > 0.0 {
            let factor: f64 = 1.0 + (rand::random::<f64>() * 2.0 - 1.0) * self.jitter;
            delay = delay.mul_f64(factor.max(0.0));
        }
        tokio::time::sleep(delay).await;
    }
}
