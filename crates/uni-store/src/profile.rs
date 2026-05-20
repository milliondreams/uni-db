// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Rust guideline compliant

//! Lightweight per-stage timing accumulator for ad-hoc profiling.
//!
//! Intended for one-off latency-breakdown work where we want exact per-call
//! timing of named code paths without dragging in a tracing subscriber. Use
//! [`stage`] as an RAII guard at the entry of a function; the elapsed time
//! is added to a global accumulator keyed by stage name. Call [`reset`]
//! before a measurement window and [`dump`] after to inspect totals.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

/// Per-stage timing record (call count and total nanoseconds).
#[derive(Debug, Default, Clone, Copy)]
pub struct StageStats {
    /// Number of times this stage was entered.
    pub count: u64,
    /// Total elapsed nanoseconds across all entries.
    pub total_ns: u64,
}

fn registry() -> &'static Mutex<HashMap<&'static str, StageStats>> {
    static REG: OnceLock<Mutex<HashMap<&'static str, StageStats>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

/// RAII guard returned by [`stage`].
///
/// On drop, records the elapsed time against `name` in the global registry.
pub struct StageGuard {
    name: &'static str,
    start: Instant,
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_nanos() as u64;
        if let Ok(mut map) = registry().lock() {
            let entry = map.entry(self.name).or_default();
            entry.count += 1;
            entry.total_ns = entry.total_ns.saturating_add(elapsed);
        }
    }
}

/// Begin timing a named stage. Drop the returned guard to record elapsed time.
#[inline]
#[must_use = "stage timer is recorded on drop"]
pub fn stage(name: &'static str) -> StageGuard {
    StageGuard {
        name,
        start: Instant::now(),
    }
}

/// Reset all accumulated stats.
pub fn reset() {
    if let Ok(mut map) = registry().lock() {
        map.clear();
    }
}

/// Snapshot the current registry contents.
#[must_use]
pub fn snapshot() -> Vec<(&'static str, StageStats)> {
    let map = registry().lock().expect("profile registry poisoned");
    let mut entries: Vec<_> = map.iter().map(|(k, v)| (*k, *v)).collect();
    entries.sort_by_key(|e| std::cmp::Reverse(e.1.total_ns));
    entries
}

/// Format a snapshot as a human-readable table.
#[must_use]
pub fn dump(divisor: u64) -> String {
    let entries = snapshot();
    let mut out = String::new();
    out.push_str(&format!(
        "{:<48} {:>10} {:>14} {:>14}\n",
        "stage", "count", "total_us", "mean_ns/iter"
    ));
    out.push_str(&"-".repeat(90));
    out.push('\n');
    let div = divisor.max(1);
    for (name, s) in entries {
        let total_us = s.total_ns / 1_000;
        let mean_per_iter = s.total_ns / div;
        out.push_str(&format!(
            "{:<48} {:>10} {:>14} {:>14}\n",
            name, s.count, total_us, mean_per_iter
        ));
    }
    out
}
