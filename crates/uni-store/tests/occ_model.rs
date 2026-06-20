// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Concurrency model-checking entry points for the OCC commit core.
//!
//! Built and run ONLY under `--features loom` (exhaustive) or `--features
//! shuttle` (randomized); in default builds this binary compiles to nothing. The
//! model body lives in `uni_store::occ_loom_model` so both tools share one source.
//!
//! ```text
//! RUSTC_WRAPPER="" LOOM_MAX_PREEMPTIONS=3 \
//!   cargo nextest run -p uni-store --features loom --test occ_model
//! RUSTC_WRAPPER="" \
//!   cargo nextest run -p uni-store --features shuttle --test occ_model
//! ```
#![cfg(any(feature = "loom", feature = "shuttle"))]

use uni_store::occ_loom_model::{run_bank_model, run_counter_model, run_truncation_model};

/// Write-write (lost-update) detection, two concurrent committers.
#[test]
fn counter_two_committers() {
    run_counter_model(2);
}

/// Write-write detection, three committers — larger interleaving space.
#[test]
fn counter_three_committers() {
    run_counter_model(3);
}

/// Read-write (write-skew) detection via the SSI read-set path.
#[test]
fn bank_write_skew() {
    run_bank_model();
}

/// History truncation: a capacity-1 registry + filler key evicts a conflicting
/// entry, making the `HistoryTruncated` conservative-abort guard load-bearing.
#[test]
fn truncation_history() {
    run_truncation_model();
}
