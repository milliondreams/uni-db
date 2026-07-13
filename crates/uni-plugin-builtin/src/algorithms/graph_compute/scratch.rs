// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Mode B-seq — a metered, mutable *scratch graph* for sequential guest programs
//! (plugin-compute proposal §7b).
//!
//! Mode A/B-vec are bulk/vectorized: no per-element guest callback, no mutation.
//! Adaptive search over evolving structure — MCTS, Dinic augmenting paths, VF2
//! subgraph match — needs the opposite: a guest running its *own* loop against a
//! fast **random-access read + mutable scratch graph**. This module is the
//! host-resident core of that runtime (also the `Q-5` perf baseline the JIT'd-WASM
//! random-access must stay within a pinned ratio of):
//!
//! - **Metered per random-access op.** Every `neighbors`/`get`/`set`/`add_*`/
//!   `sample` charges the [`WorkBudget`], extending the §5.1 native-work meter to
//!   pointer-chasing so a runaway guest loop is charged and halts at `0x865`
//!   (test `Q-1`).
//! - **Bounded mutable arena.** Graph growth (`add_node`/`add_edge`) charges the
//!   [`Arena`] byte cap; exceeding it is a typed error at the allocating op
//!   (`0x864`, test `Q-2`).
//! - **Deterministic sampling.** `sample` draws from the promoted counter-hash
//!   (`counter_hash(seed, iter, elem)`), so a seeded run's tie-breaks are
//!   reproducible (the basis for `Q-4`).
//!
//! The **compiled-only registration gate** ([`require_compiled_body`], `Q-6`)
//! and the **host-side guest ABI** are here too: [`ScratchGraph::call_json`] is
//! the single-session JSON dispatch, and [`ScratchRegistry`] the multi-session
//! host surface a WASM/Extism `host-graph` import wires to (unguessable session
//! ids + panic isolation, mirroring
//! [`GraphComputeRegistry`](super::dispatch::GraphComputeRegistry)).
//!
//! Deliberately **not** in this host-resident core (documented remaining work):
//! the WASM `.wasm` *guest* fixture driving this ABI end-to-end through wasmtime
//! (loader `with_scratch` wiring + `build-wasm-fixtures.sh`), the store
//! snapshot-isolation contract against a concurrent reader (`Q-3`, proposal §7b /
//! open question 3 — the structural session-local isolation is in place), and the
//! JIT'd-WASM arm of the perf gate (`Q-5`; the host baseline + JSON-ABI harness
//! is `crates/uni/benches/mode_b_seq_random_access.rs`).
//
// Rust guideline compliant

use serde::{Deserialize, Serialize};
use uni_algo::algo::rng::sample_bernoulli;
use uni_plugin::errors::FnError;

use super::error;
use super::{Arena, WorkBudget};

/// Approximate live bytes charged to the arena per scratch node.
///
/// A node owns an `f64` field plus the header of its adjacency `Vec`; the exact
/// figure is unimportant, only that growth is *charged* so an unbounded
/// `add_node` loop hits the arena cap (proposal §7b).
const NODE_BYTES: usize = 32;

/// Approximate live bytes charged to the arena per scratch edge (`u32` slot).
const EDGE_BYTES: usize = 4;

/// The class of loader authoring a Mode B-seq sequential body (proposal §7b).
///
/// Only **compiled** bodies (WASM/Rust) may drive the sequential per-step loop:
/// an interpreted per-step body (Rhai row-mode) cannot amortize per-step
/// interpretation, so it is disqualified on perf. Vectorized/bulk loaders remain
/// fine for Mode A / Mode B-vec — this gate is specific to the B-seq step loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoaderClass {
    /// A compiled body (WASM / native Rust) — permitted for Mode B-seq.
    Compiled,
    /// An interpreted body (Rhai row-mode) — rejected for Mode B-seq.
    Interpreted,
}

/// Gates Mode B-seq body registration on a **compiled** loader (test Q-6).
///
/// Returns a typed capability-denied error (`0x86C`) for an interpreted body, so
/// the compiled-only contract is enforced at registration rather than
/// discovered as a perf cliff at run time.
///
/// # Errors
/// Returns [`FnError`] `0x86C` when `loader` is [`LoaderClass::Interpreted`].
pub fn require_compiled_body(loader: LoaderClass) -> Result<(), FnError> {
    match loader {
        LoaderClass::Compiled => Ok(()),
        LoaderClass::Interpreted => Err(error::capability_denied(
            "Mode B-seq requires a compiled body (WASM/Rust); an interpreted \
             per-step body (Rhai row-mode) is disqualified on perf",
        )),
    }
}

/// A per-invocation, session-local mutable graph a Mode B-seq guest builds and
/// walks under a work + arena budget.
///
/// Slots are dense `u32` ids assigned by [`add_node`](Self::add_node). The graph
/// is **never observable by the store** — it lives and dies with the session
/// (the §7b snapshot-isolation contract; the store-visibility guarantee itself is
/// tracked as `Q-3`). Every accessor is fallible so budget/arena exhaustion and
/// out-of-range slots surface as typed errors rather than panics.
#[derive(Debug)]
pub struct ScratchGraph {
    /// Out-adjacency per node slot.
    adjacency: Vec<Vec<u32>>,
    /// One `f64` field per node slot (agent state / flow / visit count).
    fields: Vec<f64>,
    /// Native-work meter — charged per random-access op (proposal §5.1 / §9).
    budget: WorkBudget,
    /// Live-bytes cap — charged on structural growth (proposal §7b).
    arena: Arena,
    /// Base seed for [`sample`](Self::sample) (the promoted counter-hash stream).
    seed: u64,
}

impl ScratchGraph {
    /// Creates an empty scratch graph metered by `budget` and `arena`.
    ///
    /// `seed` seeds the reproducible sampling stream (proposal §8).
    #[must_use]
    pub fn new(budget: WorkBudget, arena: Arena, seed: u64) -> Self {
        Self {
            adjacency: Vec::new(),
            fields: Vec::new(),
            budget,
            arena,
            seed,
        }
    }

    /// Charges one unit of native work for a random-access op, failing closed.
    fn charge_op(&mut self) -> Result<(), FnError> {
        self.budget
            .try_charge(1)
            .map_err(|e| error::budget_exhausted(e.to_string()))
    }

    /// Validates a slot is in range, returning a typed error otherwise.
    fn check_slot(&self, slot: u32) -> Result<usize, FnError> {
        let i = slot as usize;
        if i < self.adjacency.len() {
            Ok(i)
        } else {
            Err(error::arg_validation(format!(
                "scratch slot {slot} out of range (node_count = {})",
                self.adjacency.len()
            )))
        }
    }

    /// Adds a node with initial field `value`, returning its slot.
    ///
    /// Charges one work unit and the node's arena bytes; growth past the arena
    /// cap is a typed `0x864` error at this op (test `Q-2`).
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` when the work budget is drained or `0x864`
    /// when the arena byte cap would be exceeded.
    pub fn add_node(&mut self, value: f64) -> Result<u32, FnError> {
        self.charge_op()?;
        self.arena
            .try_alloc(NODE_BYTES)
            .map_err(|e| error::arena_cap_exceeded(e.to_string()))?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "scratch graphs stay far below u32::MAX nodes under the arena cap"
        )]
        let slot = self.adjacency.len() as u32;
        self.adjacency.push(Vec::new());
        self.fields.push(value);
        Ok(slot)
    }

    /// Adds a directed edge `src → dst`.
    ///
    /// Charges one work unit and the edge's arena bytes.
    ///
    /// # Errors
    /// Returns [`FnError`] on an out-of-range slot (`0x86E`), a drained budget
    /// (`0x865`), or an exceeded arena cap (`0x864`).
    pub fn add_edge(&mut self, src: u32, dst: u32) -> Result<(), FnError> {
        self.charge_op()?;
        let s = self.check_slot(src)?;
        self.check_slot(dst)?;
        self.arena
            .try_alloc(EDGE_BYTES)
            .map_err(|e| error::arena_cap_exceeded(e.to_string()))?;
        self.adjacency[s].push(dst);
        Ok(())
    }

    /// Returns the out-neighbors of `slot` (a clone, so no borrow escapes).
    ///
    /// # Errors
    /// Returns [`FnError`] on an out-of-range slot or a drained budget.
    pub fn neighbors(&mut self, slot: u32) -> Result<Vec<u32>, FnError> {
        self.charge_op()?;
        let i = self.check_slot(slot)?;
        Ok(self.adjacency[i].clone())
    }

    /// Reads the field of `slot`.
    ///
    /// # Errors
    /// Returns [`FnError`] on an out-of-range slot or a drained budget.
    pub fn get_field(&mut self, slot: u32) -> Result<f64, FnError> {
        self.charge_op()?;
        let i = self.check_slot(slot)?;
        Ok(self.fields[i])
    }

    /// Writes the field of `slot`.
    ///
    /// # Errors
    /// Returns [`FnError`] on an out-of-range slot or a drained budget.
    pub fn set_field(&mut self, slot: u32, value: f64) -> Result<(), FnError> {
        self.charge_op()?;
        let i = self.check_slot(slot)?;
        self.fields[i] = value;
        Ok(())
    }

    /// Draws a reproducible `Bernoulli(prob)` decision for `(iter, slot)`.
    ///
    /// Uses the promoted counter-hash so a seeded run's tie-breaks/rollouts are
    /// bitwise-reproducible (proposal §8; the basis for `Q-4`).
    ///
    /// # Errors
    /// Returns [`FnError`] on a drained budget.
    pub fn sample(&mut self, prob: f64, iter: u64, slot: u32) -> Result<bool, FnError> {
        self.charge_op()?;
        Ok(sample_bernoulli(prob, self.seed, iter, u64::from(slot)))
    }

    /// Returns the number of nodes.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.adjacency.len()
    }

    /// Returns the total number of edges.
    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.adjacency.iter().map(Vec::len).sum()
    }

    /// Returns the work units charged so far (test introspection).
    #[must_use]
    pub fn work_spent(&self) -> u64 {
        self.budget.spent()
    }
}

/// A JSON request from a compiled Mode B-seq guest to drive a scratch graph.
///
/// The host-side ABI mirrors the Mode-A `KernelRequest` dispatch: a compiled
/// (WASM/Extism) guest emits one of these per random-access op through the
/// `host-graph` import, and the host runs it against the session-local
/// [`ScratchGraph`] under its budget. All fields default so each op names only
/// the ones it needs.
#[derive(Debug, Deserialize)]
pub struct ScratchRequest {
    /// The session id (from [`ScratchRegistry::open`]); `0` for the single-graph
    /// [`ScratchGraph::call_json`] path.
    #[serde(default)]
    pub session: u64,
    /// The op name (`"add_node"`, `"add_edge"`, `"neighbors"`, …).
    pub op: String,
    /// Primary integer operand (a slot / src).
    #[serde(default)]
    pub a: i64,
    /// Secondary integer operand (a dst).
    #[serde(default)]
    pub b: i64,
    /// A scalar operand (a field value / probability).
    #[serde(default)]
    pub f: f64,
    /// An iteration counter (for `sample`).
    #[serde(default)]
    pub iter: u64,
}

/// A JSON response returned to a Mode B-seq guest.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "t", content = "v")]
pub enum ScratchResponse {
    /// A slot / count result.
    #[serde(rename = "i")]
    Int(u32),
    /// A field-value result.
    #[serde(rename = "f")]
    Float(f64),
    /// A boolean (`sample`) result.
    #[serde(rename = "b")]
    Bool(bool),
    /// A neighbor-slot list.
    #[serde(rename = "l")]
    List(Vec<u32>),
    /// A no-value result (`add_edge`, `set_field`).
    #[serde(rename = "u")]
    Unit,
    /// A typed error `{code, message}`.
    #[serde(rename = "e")]
    Err {
        /// The GraphCompute error code (proposal §12).
        code: u32,
        /// The human-readable message.
        message: String,
    },
}

impl ScratchGraph {
    /// Dispatches one guest [`ScratchRequest`] against this scratch graph.
    ///
    /// This is the single host entry point a compiled Mode B-seq guest drives; it
    /// maps each op to the corresponding metered accessor so budget/arena
    /// exhaustion and bad slots surface as typed [`ScratchResponse::Err`] rather
    /// than trapping the guest. An unknown op is a `0x86E` arg-validation error.
    pub fn dispatch(&mut self, req: &ScratchRequest) -> ScratchResponse {
        #[expect(
            clippy::cast_sign_loss,
            reason = "guest slot operands are non-negative"
        )]
        let (a, b) = (req.a as u32, req.b as u32);
        let result: Result<ScratchResponse, FnError> = match req.op.as_str() {
            "add_node" => self.add_node(req.f).map(ScratchResponse::Int),
            "add_edge" => self.add_edge(a, b).map(|()| ScratchResponse::Unit),
            "neighbors" => self.neighbors(a).map(ScratchResponse::List),
            "get_field" => self.get_field(a).map(ScratchResponse::Float),
            "set_field" => self.set_field(a, req.f).map(|()| ScratchResponse::Unit),
            "sample" => self.sample(req.f, req.iter, a).map(ScratchResponse::Bool),
            "node_count" => Ok(ScratchResponse::Int(
                u32::try_from(self.node_count()).unwrap_or(u32::MAX),
            )),
            "edge_count" => Ok(ScratchResponse::Int(
                u32::try_from(self.edge_count()).unwrap_or(u32::MAX),
            )),
            other => Err(error::arg_validation(format!(
                "unknown scratch op `{other}`"
            ))),
        };
        match result {
            Ok(r) => r,
            Err(e) => ScratchResponse::Err {
                code: e.code,
                message: e.message,
            },
        }
    }

    /// Runs a guest JSON request string, returning a JSON response string.
    ///
    /// The exact wire form the WASM/Extism `host-graph` import carries: parse →
    /// [`dispatch`](Self::dispatch) → serialize. A malformed request is a typed
    /// `0x86E` error response, never a host panic.
    ///
    /// # Errors
    /// Returns a `serde_json` error only if the response fails to serialize (it
    /// never does for these value shapes).
    pub fn call_json(&mut self, request: &str) -> Result<String, serde_json::Error> {
        let resp = match serde_json::from_str::<ScratchRequest>(request) {
            Ok(req) => self.dispatch(&req),
            Err(e) => ScratchResponse::Err {
                code: error::ARG_VALIDATION,
                message: format!("bad scratch request json: {e}"),
            },
        };
        serde_json::to_string(&resp)
    }
}

/// A multi-session registry backing the `host-graph` import for Mode B-seq — the
/// host side a compiled WASM/Extism guest drives.
///
/// Mirrors [`GraphComputeRegistry`](super::dispatch::GraphComputeRegistry): the
/// host [`open`](Self::open)s a session-local [`ScratchGraph`] (under the CALL's
/// budget/arena) and hands the guest an **unguessable** session id; the guest
/// drives ops via [`call_json`](Self::call_json) supplying that id; the host
/// [`close`](Self::close)s to read results and drop the graph. Each session sits
/// behind its own `Mutex` so one guest's pointer-chasing never stalls another
/// concurrent CALL, and a kernel panic is isolated to a typed error rather than a
/// worker crash (proposal §5.4 / §7b).
#[derive(Debug, Default)]
pub struct ScratchRegistry {
    sessions: parking_lot::Mutex<
        std::collections::HashMap<u64, std::sync::Arc<parking_lot::Mutex<ScratchGraph>>>,
    >,
}

impl ScratchRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Registers `graph`, returning its unguessable opaque session id.
    ///
    /// The id is a CSPRNG-drawn non-zero `u64` (UUIDv4), so a concurrent CALL
    /// cannot enumerate and target another session's scratch graph.
    pub fn open(&self, graph: ScratchGraph) -> u64 {
        let mut sessions = self.sessions.lock();
        loop {
            let id = uuid::Uuid::new_v4().as_u64_pair().0;
            if id != 0 && !sessions.contains_key(&id) {
                sessions.insert(id, std::sync::Arc::new(parking_lot::Mutex::new(graph)));
                return id;
            }
        }
    }

    /// Removes and returns the scratch graph for `id`, if present.
    pub fn close(&self, id: u64) -> Option<ScratchGraph> {
        let arc = self.sessions.lock().remove(&id)?;
        std::sync::Arc::try_unwrap(arc)
            .ok()
            .map(parking_lot::Mutex::into_inner)
    }

    /// Dispatches one guest JSON request against the addressed session.
    ///
    /// A missing session, a malformed request, or a kernel panic is returned as a
    /// typed error response, never a worker crash (proposal §5.4).
    #[must_use]
    pub fn call_json(&self, request_json: &str) -> String {
        let req = match serde_json::from_str::<ScratchRequest>(request_json) {
            Ok(r) => r,
            Err(e) => {
                return serde_json::to_string(&ScratchResponse::Err {
                    code: error::ARG_VALIDATION,
                    message: format!("bad scratch request json: {e}"),
                })
                .unwrap_or_default();
            }
        };
        let session = {
            let sessions = self.sessions.lock();
            match sessions.get(&req.session) {
                Some(arc) => std::sync::Arc::clone(arc),
                None => {
                    return serde_json::to_string(&ScratchResponse::Err {
                        code: error::EPOCH_MISMATCH,
                        message: format!("unknown or closed scratch session {}", req.session),
                    })
                    .unwrap_or_default();
                }
            }
        };
        let mut guard = session.lock();
        let resp = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| guard.dispatch(&req)))
            .unwrap_or_else(|_| ScratchResponse::Err {
                code: 0x86D,
                message: "scratch: op panicked (isolated)".to_owned(),
            });
        serde_json::to_string(&resp).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph(budget: u64, arena_bytes: usize) -> ScratchGraph {
        ScratchGraph::new(
            WorkBudget::new(budget),
            Arena::new(arena_bytes, 1 << 20),
            0xABCD,
        )
    }

    #[test]
    fn q1_every_random_access_op_charges_the_budget() {
        // Q-1: each op charges one work unit; a runaway loop halts at 0x865.
        let mut g = graph(10, 1 << 20);
        let a = g.add_node(1.0).unwrap(); // 1
        let b = g.add_node(2.0).unwrap(); // 2
        g.add_edge(a, b).unwrap(); // 3
        assert_eq!(g.work_spent(), 3);
        // Burn the rest of the budget with reads, then the next op fails closed.
        let mut hit = false;
        for _ in 0..100 {
            if let Err(e) = g.get_field(a) {
                hit = e.code == error::BUDGET_EXHAUSTED;
                break;
            }
        }
        assert!(hit, "a runaway op loop must halt at 0x865");
    }

    #[test]
    fn q2_scratch_growth_charges_the_arena_cap() {
        // Q-2: node/edge growth charges the arena; exceeding it is a typed 0x864
        // at the allocating op, not a panic.
        // Room for exactly 2 nodes (2*NODE_BYTES), generous budget.
        let mut g = graph(1_000, NODE_BYTES * 2);
        g.add_node(0.0).unwrap();
        g.add_node(0.0).unwrap();
        let err = g
            .add_node(0.0)
            .expect_err("a third node must breach the 2-node arena cap");
        assert_eq!(err.code, error::ARENA_CAP_EXCEEDED);
        // The failed op did not grow the graph.
        assert_eq!(g.node_count(), 2);
    }

    #[test]
    fn q4_seeded_sampling_is_reproducible() {
        // Q-4 basis: an identical seeded op sequence yields identical sample
        // decisions, so a seeded MCTS-style rollout is bitwise-reproducible.
        let run = || {
            let mut g = graph(10_000, 1 << 20);
            for i in 0..50u32 {
                g.add_node(0.0).unwrap();
                let _ = i;
            }
            (0..50)
                .map(|slot| g.sample(0.5, 7, slot).unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(run(), run(), "seeded sampling must be reproducible");
    }

    #[test]
    fn out_of_range_slot_is_a_typed_error_not_a_panic() {
        let mut g = graph(100, 1 << 20);
        let a = g.add_node(0.0).unwrap();
        let err = g.get_field(a + 5).expect_err("out-of-range slot errors");
        assert_eq!(err.code, error::ARG_VALIDATION);
        // add_edge to a missing endpoint also errors, without mutating.
        let err = g.add_edge(a, a + 9).expect_err("bad endpoint errors");
        assert_eq!(err.code, error::ARG_VALIDATION);
        assert_eq!(g.edge_count(), 0);
    }

    #[test]
    fn at_mcts_lite_seeded_rollouts_are_reproducible() {
        // AT-MCTS (lite): a sequential adaptive program on the scratch graph — N
        // seeded rollouts from the root, each descending by sample-driven child
        // selection and bumping a visit-count field. Two identical seeded runs
        // produce identical visit counts, so a Mode B-seq search is bitwise-
        // reproducible (proposal §7b / Q-4 at scenario scale).
        let rollouts = |seed: u64| -> Vec<f64> {
            let mut g =
                ScratchGraph::new(WorkBudget::new(100_000), Arena::new(1 << 20, 1 << 20), seed);
            // A small binary game tree: root 0 → {1,2}, 1 → {3,4}, 2 → {5,6}.
            for _ in 0..7 {
                g.add_node(0.0).unwrap();
            }
            for (p, c) in [(0, 1), (0, 2), (1, 3), (1, 4), (2, 5), (2, 6)] {
                g.add_edge(p, c).unwrap();
            }
            for r in 0..64u64 {
                let mut node = 0u32;
                loop {
                    // Visit: bump the node's count field.
                    let v = g.get_field(node).unwrap();
                    g.set_field(node, v + 1.0).unwrap();
                    let kids = g.neighbors(node).unwrap();
                    if kids.is_empty() {
                        break;
                    }
                    // Seed-driven descent: pick the first child whose sample fires,
                    // else the last (a deterministic function of (seed, r, child)).
                    let mut chosen = *kids.last().unwrap();
                    for &c in &kids {
                        if g.sample(0.5, r, c).unwrap() {
                            chosen = c;
                            break;
                        }
                    }
                    node = chosen;
                }
            }
            (0..g.node_count() as u32)
                .map(|slot| g.get_field(slot).unwrap())
                .collect()
        };
        let a = rollouts(0xC0FFEE);
        assert_eq!(
            a,
            rollouts(0xC0FFEE),
            "seeded MCTS-lite must be reproducible"
        );
        // The root is visited every rollout; totals are consistent.
        assert_eq!(a[0], 64.0, "root visited once per rollout");
        // Different seed generally explores differently (not a hard requirement,
        // but confirms the seed actually drives selection).
        assert_ne!(a, rollouts(0x1234_5678), "the seed must drive descent");
    }

    #[test]
    fn q6_registration_rejects_an_interpreted_body() {
        // Q-6: the compiled-only contract — an interpreted (Rhai row-mode) B-seq
        // body is rejected at registration with a typed capability error (0x86C),
        // while a compiled (WASM/Rust) body is admitted.
        assert!(require_compiled_body(LoaderClass::Compiled).is_ok());
        let err = require_compiled_body(LoaderClass::Interpreted)
            .expect_err("an interpreted B-seq body must be rejected");
        assert_eq!(err.code, error::CAPABILITY_DENIED);
    }

    #[test]
    fn q3_scratch_graphs_are_isolated_by_construction() {
        // Q-3 (structural): the scratch graph is a self-contained, session-local
        // value — it holds no store/session handle, so it is structurally
        // invisible to the store, and two scratch graphs never interfere. (The
        // full concurrent-reader-sees-nothing Q-3 assertion needs store
        // integration, which the host-resident core deliberately does not wire.)
        let mut a = graph(1_000, 1 << 20);
        let mut b = graph(1_000, 1 << 20);
        let a0 = a.add_node(1.0).unwrap();
        let _b0 = b.add_node(99.0).unwrap();
        a.set_field(a0, 7.0).unwrap();
        // Mutating `a` leaves `b` untouched: no shared/observable state.
        assert_eq!(a.node_count(), 1);
        assert_eq!(b.node_count(), 1);
        assert_eq!(a.get_field(a0).unwrap(), 7.0);
        assert_eq!(b.get_field(_b0).unwrap(), 99.0);
    }

    #[test]
    fn guest_json_dispatch_drives_the_scratch_graph() {
        // The compiled-guest ABI: a WASM/Extism Mode B-seq body drives the
        // scratch graph through JSON `host-graph` calls. Simulate a guest here.
        let mut g = graph(10_000, 1 << 20);
        let call = |g: &mut ScratchGraph, s: &str| -> ScratchResponse {
            serde_json::from_str(&g.call_json(s).unwrap()).unwrap()
        };
        assert_eq!(
            call(&mut g, r#"{"op":"add_node","f":1.5}"#),
            ScratchResponse::Int(0)
        );
        assert_eq!(
            call(&mut g, r#"{"op":"add_node","f":2.5}"#),
            ScratchResponse::Int(1)
        );
        assert_eq!(
            call(&mut g, r#"{"op":"add_edge","a":0,"b":1}"#),
            ScratchResponse::Unit
        );
        assert_eq!(
            call(&mut g, r#"{"op":"neighbors","a":0}"#),
            ScratchResponse::List(vec![1])
        );
        assert_eq!(
            call(&mut g, r#"{"op":"get_field","a":1}"#),
            ScratchResponse::Float(2.5)
        );
        // A bad slot dispatches to a typed error response, not a panic.
        match call(&mut g, r#"{"op":"get_field","a":99}"#) {
            ScratchResponse::Err { code, .. } => assert_eq!(code, error::ARG_VALIDATION),
            other => panic!("expected an error response, got {other:?}"),
        }
        // Malformed JSON is a typed error too.
        match call(&mut g, r#"{"op":42}"#) {
            ScratchResponse::Err { code, .. } => assert_eq!(code, error::ARG_VALIDATION),
            other => panic!("expected an error response, got {other:?}"),
        }
        // A drained budget surfaces as an error through the JSON boundary.
        let mut tiny = graph(2, 1 << 20);
        let _ = tiny.call_json(r#"{"op":"add_node","f":0.0}"#).unwrap();
        let _ = tiny.call_json(r#"{"op":"add_node","f":0.0}"#).unwrap();
        match call(&mut tiny, r#"{"op":"add_node","f":0.0}"#) {
            ScratchResponse::Err { code, .. } => assert_eq!(code, error::BUDGET_EXHAUSTED),
            other => panic!("expected a budget error, got {other:?}"),
        }
    }

    #[test]
    fn scratch_registry_manages_isolated_guest_sessions() {
        // The host-side multi-session ABI a WASM/Extism Mode B-seq guest drives:
        // the host opens a session, the guest drives ops by id, the host closes
        // to read results. Two sessions are isolated; an unknown id is a typed
        // error, not a panic.
        let reg = ScratchRegistry::new();
        let sid_a = reg.open(graph(10_000, 1 << 20));
        let sid_b = reg.open(graph(10_000, 1 << 20));
        assert_ne!(sid_a, sid_b);
        assert_ne!(sid_a, 0);

        let call = |s: u64, op: &str, extra: &str| -> ScratchResponse {
            let req = format!(r#"{{"session":{s},"op":"{op}"{extra}}}"#);
            serde_json::from_str(&reg.call_json(&req)).unwrap()
        };
        // Build a node in each session; ids are per-session (both start at 0).
        assert_eq!(
            call(sid_a, "add_node", r#","f":1.0"#),
            ScratchResponse::Int(0)
        );
        assert_eq!(
            call(sid_b, "add_node", r#","f":2.0"#),
            ScratchResponse::Int(0)
        );
        // Session A's field is independent of B's.
        assert_eq!(
            call(sid_a, "get_field", r#","a":0"#),
            ScratchResponse::Float(1.0)
        );
        assert_eq!(
            call(sid_b, "get_field", r#","a":0"#),
            ScratchResponse::Float(2.0)
        );

        // Unknown session → typed error, not a panic.
        match call(0xDEAD, "node_count", "") {
            ScratchResponse::Err { code, .. } => assert_eq!(code, error::EPOCH_MISMATCH),
            other => panic!("expected unknown-session error, got {other:?}"),
        }

        // Close reads the graph back out; the session id is then gone.
        let a = reg.close(sid_a).expect("session A closes");
        assert_eq!(a.node_count(), 1);
        match call(sid_a, "node_count", "") {
            ScratchResponse::Err { code, .. } => assert_eq!(code, error::EPOCH_MISMATCH),
            other => panic!("expected closed-session error, got {other:?}"),
        }
    }

    #[test]
    fn at_mcts_full_program_authored_against_the_guest_abi() {
        // AT-MCTS (guest-ABI): a complete Mode B-seq program — build a game tree,
        // run N seeded rollouts bumping visit counts, derive the principal
        // variation — authored *purely* against the `ScratchRegistry` JSON ABI
        // (the exact surface a compiled WASM/Extism guest drives): the host opens
        // a session, the "guest" issues `call_json` ops by id, the host closes and
        // reads the result. Asserts reproducibility and a pinned principal
        // variation, proving the ABI is complete for a real sequential program.
        //
        // Principal variation = from the root, repeatedly descend to the
        // most-visited child, until a leaf.
        let run = |seed: u64| -> Vec<u32> {
            let reg = ScratchRegistry::new();
            let sid = reg.open(ScratchGraph::new(
                WorkBudget::new(1_000_000),
                Arena::new(1 << 20, 1 << 20),
                seed,
            ));
            // A guest helper: one JSON op through the registry, parsed back.
            let op = |req: String| -> ScratchResponse {
                serde_json::from_str(&reg.call_json(&req)).unwrap()
            };
            let node = |resp: ScratchResponse| -> u32 {
                match resp {
                    ScratchResponse::Int(i) => i,
                    other => panic!("expected int, got {other:?}"),
                }
            };
            // Build a small binary game tree (root 0 → {1,2}; 1 → {3,4}; 2 → {5,6}).
            // `add_node` returns the new node's dense slot id, so ids arrive 0..7.
            for expected in 0..7u32 {
                let got = node(op(format!(r#"{{"session":{sid},"op":"add_node","f":0.0}}"#)));
                assert_eq!(got, expected, "add_node returns dense slot ids in order");
            }
            for (p, c) in [(0, 1), (0, 2), (1, 3), (1, 4), (2, 5), (2, 6)] {
                op(format!(
                    r#"{{"session":{sid},"op":"add_edge","a":{p},"b":{c}}}"#
                ));
            }
            // Seeded rollouts (all via the JSON ABI).
            for r in 0..128u64 {
                let mut cur = 0u32;
                loop {
                    let v = match op(format!(r#"{{"session":{sid},"op":"get_field","a":{cur}}}"#)) {
                        ScratchResponse::Float(f) => f,
                        other => panic!("field: {other:?}"),
                    };
                    op(format!(
                        r#"{{"session":{sid},"op":"set_field","a":{cur},"f":{}}}"#,
                        v + 1.0
                    ));
                    let kids =
                        match op(format!(r#"{{"session":{sid},"op":"neighbors","a":{cur}}}"#)) {
                            ScratchResponse::List(l) => l,
                            other => panic!("neighbors: {other:?}"),
                        };
                    if kids.is_empty() {
                        break;
                    }
                    // Seed-driven descent: first child whose sample fires, else last.
                    let mut chosen = *kids.last().unwrap();
                    for &c in &kids {
                        let fired = matches!(
                            op(format!(
                                r#"{{"session":{sid},"op":"sample","a":{c},"f":0.5,"iter":{r}}}"#
                            )),
                            ScratchResponse::Bool(true)
                        );
                        if fired {
                            chosen = c;
                            break;
                        }
                    }
                    cur = chosen;
                }
            }
            // Host reads results back and derives the principal variation.
            let g = reg.close(sid).expect("session closes");
            let mut pv = vec![0u32];
            let mut cur = 0u32;
            loop {
                // Read neighbors directly off the closed graph (host-side).
                let kids = g_neighbors(&g, cur);
                if kids.is_empty() {
                    break;
                }
                let best = kids
                    .iter()
                    .copied()
                    .max_by(|&a, &b| {
                        g_field(&g, a)
                            .partial_cmp(&g_field(&g, b))
                            .unwrap()
                            .then(b.cmp(&a))
                    })
                    .unwrap();
                pv.push(best);
                cur = best;
            }
            pv
        };
        let pv = run(0xC0FFEE);
        assert_eq!(
            pv,
            run(0xC0FFEE),
            "the MCTS principal variation must be reproducible"
        );
        assert_eq!(pv[0], 0, "the PV starts at the root");
        assert_eq!(pv.len(), 3, "the PV descends to a depth-2 leaf");
        // The chosen leaf is one of the tree's leaves {3,4,5,6}.
        assert!(
            (3..=6).contains(pv.last().unwrap()),
            "PV ends at a real leaf"
        );
    }

    /// Host-side read of a closed scratch graph's neighbors (test introspection).
    fn g_neighbors(g: &ScratchGraph, slot: u32) -> Vec<u32> {
        g.adjacency.get(slot as usize).cloned().unwrap_or_default()
    }
    /// Host-side read of a closed scratch graph's field.
    fn g_field(g: &ScratchGraph, slot: u32) -> f64 {
        g.fields.get(slot as usize).copied().unwrap_or(0.0)
    }

    #[test]
    fn neighbors_and_fields_roundtrip() {
        let mut g = graph(1_000, 1 << 20);
        let a = g.add_node(1.5).unwrap();
        let b = g.add_node(2.5).unwrap();
        let c = g.add_node(3.5).unwrap();
        g.add_edge(a, b).unwrap();
        g.add_edge(a, c).unwrap();
        assert_eq!(g.neighbors(a).unwrap(), vec![b, c]);
        g.set_field(b, 9.0).unwrap();
        assert_eq!(g.get_field(b).unwrap(), 9.0);
        assert_eq!(g.edge_count(), 2);
    }
}
