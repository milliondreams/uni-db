// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Self-certification probes for the GraphCompute kernel surface.
//!
//! A third-party graph-algorithm author runs [`run_probes`] to check that the
//! host they load into upholds the safety invariants the proposal mandates —
//! the same corpus uni gates on (proposal §9.0 conformance probes). Each probe
//! has a stable `graph.*` id and drives the real [`AlgoSession`] / kernel surface
//! (or, for slice negotiation, the [`AlgorithmSignature`] check), so a green run
//! is behavioral evidence, not a manifest smoke-test.
//!
//! The probes are deliberately dependency-free (a synthetic in-memory projection),
//! so they run anywhere the crate does without a live database.
//
// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;

use uni_algo::algo::GraphProjection;
use uni_common::Value;
use uni_plugin::traits::algorithm::{AlgorithmSignature, HOST_CAPABILITY_SLICES, SliceReq};

use super::error;
use super::handle::Handle;
use super::session::{AlgoSession, Direction, GraphCompute};
use super::{Arena, WorkBudget};

/// The outcome of one conformance probe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbeResult {
    /// Stable probe id (e.g. `"graph.handle_forgery"`).
    pub id: &'static str,
    /// Whether the invariant held.
    pub passed: bool,
    /// Human-readable detail on failure (empty on pass).
    pub detail: String,
}

impl ProbeResult {
    fn pass(id: &'static str) -> Self {
        Self {
            id,
            passed: true,
            detail: String::new(),
        }
    }

    fn fail(id: &'static str, detail: impl Into<String>) -> Self {
        Self {
            id,
            passed: false,
            detail: detail.into(),
        }
    }
}

/// Builds a tiny synthetic triangle projection for the probes.
fn probe_graph() -> GraphProjection {
    let nodes: Vec<HashMap<String, Value>> = (0..3u64)
        .map(|id| HashMap::from([("id".to_owned(), Value::Int(id as i64))]))
        .collect();
    let edges: Vec<HashMap<String, Value>> = [(0u64, 1u64), (1, 2), (2, 0)]
        .into_iter()
        .map(|(s, t)| {
            HashMap::from([
                ("source".to_owned(), Value::Int(s as i64)),
                ("target".to_owned(), Value::Int(t as i64)),
            ])
        })
        .collect();
    GraphProjection::from_rows(&nodes, &edges, None, false).expect("probe projection builds")
}

fn probe_session(budget: WorkBudget) -> (AlgoSession, Handle) {
    let mut session = AlgoSession::new(1, budget, Arena::new(1 << 20, 4096));
    let g = session.bind_graph(Arc::new(probe_graph()));
    (session, g)
}

/// Runs every GraphCompute conformance probe, returning one result per probe.
///
/// A conformant host returns `passed == true` for all of them. The ids are
/// append-only and stable so third parties and CI pin on them.
#[must_use]
pub fn run_probes() -> Vec<ProbeResult> {
    vec![
        probe_handle_forgery(),
        probe_budget(),
        probe_determinism(),
        probe_slice_version(),
    ]
}

/// `graph.handle_forgery` — a forged handle is a typed error, never a panic/OOB.
fn probe_handle_forgery() -> ProbeResult {
    const ID: &str = "graph.handle_forgery";
    let (session, _g) = probe_session(WorkBudget::new(1_000_000));
    let forged = Handle::from_u64(0x4141_4141_4141_4141);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        session.vertex_count(forged)
    }));
    match outcome {
        Ok(Err(_)) => ProbeResult::pass(ID),
        Ok(Ok(_)) => ProbeResult::fail(ID, "a forged handle unexpectedly resolved"),
        Err(_) => ProbeResult::fail(ID, "a forged handle panicked instead of erroring"),
    }
}

/// `graph.budget` — a drained native-work budget fails closed (`0x865`).
fn probe_budget() -> ProbeResult {
    const ID: &str = "graph.budget";
    // A budget too small for even one degrees() pass must fail closed.
    let (mut session, g) = probe_session(WorkBudget::new(1));
    let mut hit = false;
    // A handful of O(V) kernels drains a 1-unit budget deterministically.
    for _ in 0..8 {
        if let Err(e) = session.degrees(g, Direction::Out) {
            hit = e.code == error::BUDGET_EXHAUSTED;
            break;
        }
    }
    if hit {
        ProbeResult::pass(ID)
    } else {
        ProbeResult::fail(ID, "the native-work budget did not fail closed at 0x865")
    }
}

/// `graph.determinism` — a kernel yields byte-identical output across runs.
fn probe_determinism() -> ProbeResult {
    const ID: &str = "graph.determinism";
    let run = || {
        let (mut session, g) = probe_session(WorkBudget::new(1_000_000));
        let deg = session.degrees(g, Direction::Out).expect("degrees runs");
        session.tensor_snapshot(deg)
    };
    if run() == run() {
        ProbeResult::pass(ID)
    } else {
        ProbeResult::fail(ID, "a kernel produced non-deterministic output")
    }
}

/// `graph.slice_version` — an unavailable slice version is refused (`0x86A`).
fn probe_slice_version() -> ProbeResult {
    const ID: &str = "graph.slice_version";
    let sig = AlgorithmSignature {
        slices: vec![SliceReq {
            slice: "graph-compute".into(),
            // One past the host's implemented version must be refused.
            version: super::GRAPH_COMPUTE_SLICE_VERSION + 1,
        }],
        ..Default::default()
    };
    match sig.check_slices(HOST_CAPABILITY_SLICES) {
        Err(e) if e.code == error::SLICE_VERSION_MISMATCH => ProbeResult::pass(ID),
        Err(e) => ProbeResult::fail(ID, format!("wrong error code {:#x}", e.code)),
        Ok(()) => ProbeResult::fail(ID, "an unavailable slice version was accepted"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_conformance_probes_pass_on_this_host() {
        let results = run_probes();
        assert_eq!(results.len(), 4, "the stable probe set is append-only");
        for r in &results {
            assert!(r.passed, "probe {} failed: {}", r.id, r.detail);
        }
        // The ids are exactly the stable graph.* set (order-independent).
        let ids: std::collections::HashSet<&str> = results.iter().map(|r| r.id).collect();
        for want in [
            "graph.handle_forgery",
            "graph.budget",
            "graph.determinism",
            "graph.slice_version",
        ] {
            assert!(ids.contains(want), "missing stable probe id {want}");
        }
    }
}
