// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Concurrency-primitive shim for model checking the OCC commit core.
//!
//! The conflict-detection logic in [`super::occ`] is pure synchronous code; its
//! only shared, cross-committer state is the commit-sequence atomic and the
//! commit registry. To model-check that logic with `loom` (exhaustive) and
//! `shuttle` (randomized) we route the atomic — and, in the test harness, the
//! `Arc`/`Mutex`/thread primitives — through each tool's instrumented twin. This
//! module is the single swap point.
//!
//! - Default builds: everything aliases to `std` (zero behavior change).
//! - `--features loom`:    aliases to `loom::*`.
//! - `--features shuttle`: aliases to `shuttle::*`.
//!
//! These are gated as Cargo **features**, not a global `--cfg loom`: the latter
//! sets `cfg(loom)` for the whole dependency graph, which breaks loom-aware
//! crates (e.g. `concurrent-queue`) that activate their own loom paths without
//! loom in their dep tree. A feature confines the instrumentation to this crate.
//!
//! Enabling a feature recompiles this crate against the instrumented atomic, so
//! the alias swap reaches production signatures (`Writer::commit_sequence`,
//! `CommitRegistry::commit`). Those constructors are never *executed* under the
//! feature — only the model harness runs — so loom/shuttle's "construct all
//! shared state inside the model closure" rule holds. Consequently a build with
//! either feature must run ONLY `--test occ_model`; the full suite would drive
//! real `loom` atomics outside a model and panic.

// --- Always compiled. This atomic appears in production signatures, so it must
//     resolve in every configuration. ---
#[cfg(feature = "loom")]
pub(crate) use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(all(feature = "shuttle", not(feature = "loom")))]
pub(crate) use shuttle::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(any(feature = "loom", feature = "shuttle")))]
pub(crate) use std::sync::atomic::{AtomicU64, Ordering};

// --- Model harness only. `Arc`/`Mutex`/`thread` and the `check` runner exist
//     solely under loom/shuttle and are consumed by `crate::occ_loom_model`. In
//     default builds they are not compiled at all (no dead-code surface). ---
#[cfg(feature = "loom")]
pub(crate) use loom::{
    sync::{Arc, Mutex},
    thread,
};
#[cfg(all(feature = "shuttle", not(feature = "loom")))]
pub(crate) use shuttle::{
    sync::{Arc, Mutex},
    thread,
};

/// Runs `f` under the active model checker: exhaustively (loom) or randomized
/// (shuttle). Both re-invoke `f` once per explored interleaving, so every piece
/// of shared state `f` touches must be constructed *inside* `f`.
#[cfg(feature = "loom")]
pub(crate) fn check<F: Fn() + Send + Sync + 'static>(f: F) {
    loom::model(f);
}

/// Randomized scheduler — scales past loom's exhaustive thread/branch limit, so
/// loom covers the exhaustive small models and shuttle covers larger mixes. Not
/// sound (a pass doesn't prove correctness), but reproducible by seed on failure.
#[cfg(all(feature = "shuttle", not(feature = "loom")))]
pub(crate) fn check<F: Fn() + Send + Sync + 'static>(f: F) {
    shuttle::check_random(f, 10_000);
}
