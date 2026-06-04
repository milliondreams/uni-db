// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! In-process capture of emitted `metrics` counters.
//!
//! The production code emits SSI telemetry through the `metrics` facade
//! (`metrics::counter!("uni_ssi_serialization_conflicts_total", …)` etc.). The
//! facade routes to whatever global recorder is installed; in production that is
//! a Prometheus/OTel exporter, in tests it is nothing (a no-op). Here we install
//! a [`DebuggingRecorder`] so a test can read the live counter values and assert
//! on them.
//!
//! ## Why deltas, not absolutes
//!
//! `metrics::set_global_recorder` may be called **at most once per process**, and
//! counters are monotonic and process-global. Under `cargo nextest` each test is
//! its own process, so a counter starts at 0; under a shared-process runner it
//! does not. Both are handled by always measuring a **delta** around the work
//! under test ([`CounterProbe`]) rather than an absolute value.
//!
//! ## Why an accumulator (metrics-util 0.20+)
//!
//! As of metrics-util 0.20, `Snapshotter::snapshot()` **consumes** counters and
//! gauges — each read resets the underlying atomic to zero and returns only the
//! increments observed since the previous snapshot (see `DebuggingRecorder`'s
//! `swap(0, …)`). A single global snapshotter shared across probes therefore
//! cannot be read twice for cumulative values: the first reader drains every
//! counter, so a second probe would observe zero. To preserve the monotonic,
//! cumulative view the probes rely on, every snapshot is folded into a
//! process-global [`ACCUM`] running total and reads are served from there.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();

/// Cumulative counter totals, keyed by `(name, sorted labels)`. Rebuilt from the
/// consuming snapshotter on every read so values stay monotonic regardless of how
/// many probes drain the snapshotter in between.
type CounterKey = (String, Vec<(String, String)>);
static ACCUM: OnceLock<Mutex<HashMap<CounterKey, u64>>> = OnceLock::new();

/// Installs the debugging recorder once per process. Idempotent and safe to call
/// from every test; only the first call installs, later calls reuse the handle.
pub fn init() {
    SNAPSHOTTER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // If another recorder is already global (it shouldn't be in tests), the
        // install fails and our snapshotter simply observes nothing — tests that
        // assert on counters will then see 0 and fail loudly, which is correct.
        let _ = recorder.install();
        snapshotter
    });
    ACCUM.get_or_init(|| Mutex::new(HashMap::new()));
}

/// Drains the consuming snapshotter once and folds the freshly-observed counter
/// increments into the cumulative accumulator.
fn pump() {
    let snapshotter = SNAPSHOTTER
        .get()
        .expect("ssi_support::metrics::init() must be called before reading counters");
    let mut accum = ACCUM
        .get()
        .expect("ssi_support::metrics::init() must be called before reading counters")
        .lock()
        .expect("metrics accumulator lock poisoned");
    for (ckey, _unit, _desc, value) in snapshotter.snapshot().into_vec() {
        if let DebugValue::Counter(v) = value {
            let (_kind, key) = ckey.into_parts();
            let mut labels: Vec<(String, String)> = key
                .labels()
                .map(|l| (l.key().to_string(), l.value().to_string()))
                .collect();
            labels.sort();
            *accum.entry((key.name().to_string(), labels)).or_insert(0) += v;
        }
    }
}

/// Current cumulative value of counter `name` whose labels are a superset of
/// `want`. Returns 0 if the counter has never been touched.
pub fn counter_value(name: &str, want: &[(&str, &str)]) -> u64 {
    pump();
    let accum = ACCUM
        .get()
        .expect("ssi_support::metrics::init() must be called before reading counters")
        .lock()
        .expect("metrics accumulator lock poisoned");
    accum
        .iter()
        .filter(|((n, _), _)| n == name)
        .filter(|((_, labels), _)| {
            want.iter()
                .all(|(k, v)| labels.iter().any(|(lk, lv)| lk == k && lv == v))
        })
        .map(|(_, total)| *total)
        .sum()
}

/// Captures a counter's value at construction so a test can read the increment
/// caused by the work that follows. Robust to other tests sharing the process.
pub struct CounterProbe {
    name: String,
    labels: Vec<(String, String)>,
    base: u64,
}

impl CounterProbe {
    /// Snapshots `name`/`labels` now. Call [`CounterProbe::delta`] afterwards.
    pub fn start(name: &str, labels: &[(&str, &str)]) -> Self {
        init();
        let owned: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let base = counter_value(name, labels);
        Self {
            name: name.to_string(),
            labels: owned,
            base,
        }
    }

    /// Increment of the counter since [`CounterProbe::start`].
    pub fn delta(&self) -> u64 {
        let want: Vec<(&str, &str)> = self
            .labels
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        counter_value(&self.name, &want).saturating_sub(self.base)
    }
}

/// Convenience: the L0-snapshot freeze counter (clone-on-freeze fired).
pub fn freezes() -> u64 {
    counter_value("uni_l0_snapshot_freezes_total", &[])
}
