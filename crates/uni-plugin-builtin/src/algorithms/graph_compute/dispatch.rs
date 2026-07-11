// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Loader-agnostic JSON kernel dispatch + per-CALL session registry.
//!
//! The in-process Rhai loader hands a guest a [`GcSession`](super::session)
//! object with native methods. The sandboxed loaders (WASM / Extism) cannot pass
//! a Rust object across the boundary, so they instead expose a *single* host
//! function that marshals one kernel call as JSON: the guest sends
//! `{op, session, handles, scalars}` and receives `{handle | scalar | error}`.
//! This collapses the whole kernel catalog to one host import per loader
//! (proposal §4.5) — only handles and scalars ever cross, exactly the property
//! that makes the design portable across loaders.
//!
//! A [`GraphComputeRegistry`] owns the per-CALL [`AlgoSession`]s keyed by an
//! opaque session id. The adapter [`opens`](GraphComputeRegistry::open) a session
//! before invoking the guest, passes the id in, and [`closes`] it after — so
//! concurrent CALLs never share a session, and a stateless pooled host function
//! resolves the right session by id on every call.
//!
//! [`closes`]: GraphComputeRegistry::close
//
// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uni_common::core::id::Vid;
use uni_plugin::errors::FnError;

use super::handle::Handle;
use super::session::{
    AlgoSession, Direction, EwiseOp, GraphCompute, MapOp, Norm, OverlapMetric, Predicate, ReduceOp,
    Semiring,
};
use super::value::Scalar;

/// Serde default for the node2vec bias params (unbiased = 1.0).
fn one_f64() -> f64 {
    1.0
}

/// One kernel call from a guest, deserialized from the request JSON.
///
/// A single flat struct with all-optional operands keeps the wire format simple
/// and identical across loaders; each `op` reads only the fields it needs.
#[derive(Debug, Deserialize)]
pub struct KernelRequest {
    /// The session id returned by [`GraphComputeRegistry::open`].
    pub session: u64,
    /// The kernel name (`"frontier"`, `"spmv"`, …).
    pub op: String,
    /// Primary handle operand (graph / map / set), as a packed `i64`.
    #[serde(default)]
    pub g: i64,
    /// Second handle operand.
    #[serde(default)]
    pub a: i64,
    /// Third handle operand.
    #[serde(default)]
    pub b: i64,
    /// A string enum operand (direction / predicate / norm / op).
    #[serde(default)]
    pub s: String,
    /// A second string enum operand (spmv direction alongside the semiring).
    #[serde(default)]
    pub s2: String,
    /// A scalar operand.
    #[serde(default)]
    pub f: f64,
    /// A second scalar operand (e.g. the `b` of `map_apply` `AxPlusB(a, b)`).
    #[serde(default)]
    pub f2: f64,
    /// A count operand (the `k` of `topk`, the `bucket` of `next_bucket`).
    #[serde(default)]
    pub k: u32,
    /// A boolean operand (the `want_max` of `arg_extreme`).
    #[serde(default)]
    pub want_max: bool,
    /// Walk length (`random_walks`).
    #[serde(default)]
    pub wl: u32,
    /// Walks per node (`random_walks`).
    #[serde(default)]
    pub wn: u32,
    /// node2vec return bias `p` (`random_walks`).
    #[serde(default = "one_f64")]
    pub p: f64,
    /// node2vec in-out bias `q` (`random_walks`).
    #[serde(default = "one_f64")]
    pub q: f64,
    /// Deterministic RNG seed (`random_walks`).
    #[serde(default)]
    pub seed: u64,
    /// Seed vertex ids (for `frontier`).
    #[serde(default)]
    pub seeds: Vec<i64>,
    /// Column name (for `emit`).
    #[serde(default)]
    pub name: String,
}

/// The result of a kernel call, serialized to the response JSON.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "t", content = "v")]
pub enum KernelResponse {
    /// A handle result (packed `i64`).
    #[serde(rename = "h")]
    Handle(i64),
    /// A scalar (`f64`) result.
    #[serde(rename = "f")]
    Float(f64),
    /// A boolean result.
    #[serde(rename = "b")]
    Bool(bool),
    /// A `(vertexId, scalar)` result (`arg_extreme`).
    #[serde(rename = "vs")]
    VidScalar {
        /// The external vertex id of the extremum.
        vid: i64,
        /// The extremum's scalar value.
        f: f64,
    },
    /// A ranked `(vertexId, scalar)` list result (`topk`).
    #[serde(rename = "ps")]
    Pairs(Vec<(i64, f64)>),
    /// A no-value result (`free`, `emit`).
    #[serde(rename = "u")]
    Unit,
    /// A typed error `{code, message}`.
    #[serde(rename = "e")]
    Err {
        /// The GraphCompute error code (proposal §12).
        code: u32,
        /// The human-readable error message.
        message: String,
    },
}

fn to_i64(h: Handle) -> i64 {
    #[expect(
        clippy::cast_possible_wrap,
        reason = "opaque handle round-trips bit-exact"
    )]
    let v = h.as_u64() as i64;
    v
}

fn from_i64(v: i64) -> Handle {
    #[expect(clippy::cast_sign_loss, reason = "opaque handle round-trips bit-exact")]
    let bits = v as u64;
    Handle::from_u64(bits)
}

fn dir(s: &str) -> Result<Direction, FnError> {
    match s {
        "out" => Ok(Direction::Out),
        "in" => Ok(Direction::In),
        other => Err(FnError::new(0x861, format!("bad direction `{other}`"))),
    }
}

fn semiring(s: &str) -> Result<Semiring, FnError> {
    match s {
        "reachability" => Ok(Semiring::Reachability),
        "shortest_path" => Ok(Semiring::ShortestPath),
        "propagate" => Ok(Semiring::Propagate),
        "linear_algebra" => Ok(Semiring::LinearAlgebra),
        "min_max" => Ok(Semiring::MinMax),
        other => Err(FnError::new(0x861, format!("bad semiring `{other}`"))),
    }
}

/// Decodes a generic `map_apply` op string plus its scalar operands.
///
/// Covers every [`MapOp`] variant so a guest can express affine (`ax+b`) and
/// `log` maps that the fixed `scale`/`recip`/`normalize` ops cannot reach; `a`
/// is the first scalar (`req.f`), `b` the second (`req.f2`).
fn map_op(s: &str, a: f64, b: f64) -> Result<MapOp, FnError> {
    match s {
        "recip" => Ok(MapOp::Recip),
        "scale" => Ok(MapOp::Scale(a)),
        "log" => Ok(MapOp::Log),
        "affine" => Ok(MapOp::AxPlusB(a, b)),
        "normalize_l1" => Ok(MapOp::Normalize(Norm::L1)),
        "normalize_l2" => Ok(MapOp::Normalize(Norm::L2)),
        other => Err(FnError::new(0x861, format!("bad map op `{other}`"))),
    }
}

/// A per-process registry of live GraphCompute sessions keyed by session id.
///
/// Shared (behind an `Arc`) by a loader's single graph host function and its
/// per-CALL algorithm adapter. See the [module docs](self) for the lifecycle.
///
/// The session id is **unguessable** (drawn from a CSPRNG via UUIDv4), not a
/// sequential counter: on the JSON surface the guest supplies the id on every
/// call, so a sequential id would let one concurrent CALL enumerate and target
/// another CALL's session (read its graph, drain its budget, free its handles).
/// A ~60-bit-entropy random key closes that cross-session hole (review H2).
#[derive(Debug, Default)]
pub struct GraphComputeRegistry {
    /// A short-lived map lock guards only lookup/insert/remove; each session sits
    /// behind its own `Mutex` so one guest's O(E) kernel never stalls another
    /// concurrent CALL's session (proposal §5.1 / E6).
    sessions: Mutex<HashMap<u64, Arc<Mutex<AlgoSession>>>>,
}

impl GraphComputeRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Registers `session`, returning its unguessable opaque id for the guest.
    ///
    /// The id is a CSPRNG-drawn `u64` (from UUIDv4); the loop retries on the
    /// astronomically unlikely collision or zero so the returned id is always
    /// live-unique and non-zero.
    pub fn open(&self, session: AlgoSession) -> u64 {
        let mut sessions = self.sessions.lock();
        loop {
            let id = uuid::Uuid::new_v4().as_u64_pair().0;
            if id != 0 && !sessions.contains_key(&id) {
                sessions.insert(id, Arc::new(Mutex::new(session)));
                return id;
            }
        }
    }

    /// Removes and returns the session with `id`, if present.
    ///
    /// The adapter calls this after the guest returns to read the emitted
    /// columns and drop the session (freeing every handle). The guest has
    /// returned, so no `call` still holds a clone and the `Arc` unwraps cleanly.
    pub fn close(&self, id: u64) -> Option<AlgoSession> {
        let arc = self.sessions.lock().remove(&id)?;
        Arc::try_unwrap(arc).ok().map(Mutex::into_inner)
    }

    /// Dispatches one kernel call and returns its typed response.
    ///
    /// A missing session or a kernel error is returned as [`KernelResponse::Err`]
    /// rather than panicking, so a hostile guest cannot crash the worker
    /// (proposal §5.4).
    #[must_use]
    pub fn call(&self, req: &KernelRequest) -> KernelResponse {
        // Hold the map lock only long enough to clone the session's Arc, then
        // release it so other sessions run concurrently (E6).
        let session = {
            let sessions = self.sessions.lock();
            let Some(arc) = sessions.get(&req.session) else {
                return KernelResponse::Err {
                    code: 0x863,
                    message: format!("unknown or closed session {}", req.session),
                };
            };
            Arc::clone(arc)
        };
        let mut guard = session.lock();
        // Panic isolation (proposal §5.4): a defensive panic in a kernel becomes
        // a typed error, not a worker crash, so a hostile guest driving the JSON
        // surface cannot bring down the process. parking_lot locks don't poison,
        // so the session mutex releases cleanly on unwind; the session is
        // per-CALL and discarded after, so any partial state is irrelevant.
        let dispatched = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Self::dispatch(&mut guard, req)
        }));
        match dispatched {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => KernelResponse::Err {
                code: e.code,
                message: e.message,
            },
            Err(_) => KernelResponse::Err {
                code: 0x86D,
                message: "GraphCompute: kernel panicked (isolated)".to_owned(),
            },
        }
    }

    /// Dispatches one kernel call as JSON in and JSON out.
    ///
    /// The single entry point a stateless loader host function calls. A malformed
    /// request or a serialization failure is reported in-band as an error
    /// response, never a panic.
    #[must_use]
    pub fn call_json(&self, request_json: &str) -> String {
        let resp = match serde_json::from_str::<KernelRequest>(request_json) {
            Ok(req) => self.call(&req),
            Err(e) => KernelResponse::Err {
                code: 0x802,
                message: format!("bad kernel request json: {e}"),
            },
        };
        serde_json::to_string(&resp).unwrap_or_else(|e| {
            format!("{{\"t\":\"e\",\"v\":{{\"code\":2,\"message\":\"encode: {e}\"}}}}")
        })
    }

    /// Maps one request to a kernel invocation on `session`.
    fn dispatch(session: &mut AlgoSession, req: &KernelRequest) -> Result<KernelResponse, FnError> {
        let h = |x: Handle| KernelResponse::Handle(to_i64(x));
        match req.op.as_str() {
            "vertex_count" => Ok(KernelResponse::Float(
                session.vertex_count(from_i64(req.g))? as f64,
            )),
            "frontier" => {
                let vids: Vec<Vid> = req
                    .seeds
                    .iter()
                    .map(|&i| {
                        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
                        let u = i as u64;
                        Vid::new(u)
                    })
                    .collect();
                session.frontier(from_i64(req.g), &vids).map(h)
            }
            "degrees" => session.degrees(from_i64(req.g), dir(&req.s)?).map(h),
            "vertex_ids" => session.vertex_ids(from_i64(req.g)).map(h),
            "set_to_map" => session
                .set_to_map(from_i64(req.g), Scalar::F64(req.f))
                .map(h),
            "map_to_set" => {
                let pred = match req.s.as_str() {
                    "is_zero" => Predicate::IsZero,
                    "gt" => Predicate::Gt(req.f),
                    "lt" => Predicate::Lt(req.f),
                    "eq" => Predicate::Eq(req.f),
                    other => return Err(FnError::new(0x861, format!("bad predicate `{other}`"))),
                };
                session.map_to_set(from_i64(req.g), pred).map(h)
            }
            "recip" => session.map_apply(from_i64(req.g), MapOp::Recip).map(h),
            "scale" => session
                .map_apply(from_i64(req.g), MapOp::Scale(req.f))
                .map(h),
            "normalize" => {
                let norm = match req.s.as_str() {
                    "l1" => Norm::L1,
                    "l2" => Norm::L2,
                    other => return Err(FnError::new(0x861, format!("bad norm `{other}`"))),
                };
                session
                    .map_apply(from_i64(req.g), MapOp::Normalize(norm))
                    .map(h)
            }
            "ewise" => {
                let op = match req.s.as_str() {
                    "mul" => EwiseOp::Mul,
                    "add" => EwiseOp::Add,
                    "min" => EwiseOp::Min,
                    "max" => EwiseOp::Max,
                    "axpy" => EwiseOp::Axpy(req.f),
                    other => return Err(FnError::new(0x861, format!("bad ewise op `{other}`"))),
                };
                session.ewise(from_i64(req.a), from_i64(req.b), op).map(h)
            }
            "spmv" => session
                .spmv(
                    from_i64(req.g),
                    from_i64(req.a),
                    semiring(&req.s)?,
                    dir(&req.s2)?,
                    None,
                )
                .map(h),
            "zero_map" => {
                // `s == "i64"` seeds an exact path-counting run; default f64.
                let ty = if req.s == "i64" {
                    super::value::DType::I64
                } else {
                    super::value::DType::F64
                };
                session.zero_map(from_i64(req.g), ty).map(h)
            }
            "map_apply" => session
                .map_apply(from_i64(req.g), map_op(&req.s, req.f, req.f2)?)
                .map(h),
            "edge_count" => Ok(KernelResponse::Float(
                session.edge_count(from_i64(req.g))? as f64
            )),
            "scatter" => session
                .scatter(from_i64(req.a), from_i64(req.b), Scalar::F64(req.f))
                .map(h),
            "arg_extreme" => {
                let (vid, s) = session.arg_extreme(from_i64(req.g), req.want_max)?;
                #[expect(clippy::cast_possible_wrap, reason = "vids fit i64 in practice")]
                let vid = vid.as_u64() as i64;
                Ok(KernelResponse::VidScalar { vid, f: s.as_f64() })
            }
            "random_walks" => {
                let seeds: Vec<Vid> = req
                    .seeds
                    .iter()
                    .map(|&i| {
                        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
                        let u = i as u64;
                        Vid::new(u)
                    })
                    .collect();
                session
                    .random_walks(
                        from_i64(req.g),
                        req.wl as usize,
                        req.wn as usize,
                        &seeds,
                        req.p,
                        req.q,
                        req.seed,
                    )
                    .map(h)
            }
            "walk_visit_counts" => session
                .walk_visit_counts(from_i64(req.a), from_i64(req.g))
                .map(h),
            "neighborhood_overlap" => {
                let source = req.seeds.first().copied().unwrap_or(0);
                #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
                let source = Vid::new(source as u64);
                let metric = match req.s.as_str() {
                    "jaccard" => OverlapMetric::Jaccard,
                    "overlap" => OverlapMetric::Overlap,
                    "cosine" => OverlapMetric::Cosine,
                    other => {
                        return Err(FnError::new(0x861, format!("bad overlap metric `{other}`")));
                    }
                };
                session
                    .neighborhood_overlap(from_i64(req.g), source, metric)
                    .map(h)
            }
            "next_bucket" => session.next_bucket(from_i64(req.g), req.f, req.k).map(h),
            "topk" => {
                let ranked = session.topk(from_i64(req.g), req.k)?;
                #[expect(clippy::cast_possible_wrap, reason = "vids fit i64 in practice")]
                let pairs = ranked
                    .into_iter()
                    .map(|(vid, s)| (vid.as_u64() as i64, s.as_f64()))
                    .collect();
                Ok(KernelResponse::Pairs(pairs))
            }
            "expand" => session
                .expand(
                    from_i64(req.g),
                    from_i64(req.a),
                    dir(&req.s)?,
                    Some(from_i64(req.b)),
                )
                .map(h),
            "set_union" => session.set_union(from_i64(req.a), from_i64(req.b)).map(h),
            "set_diff" => session.set_diff(from_i64(req.a), from_i64(req.b)).map(h),
            "set_intersect" => session
                .set_intersect(from_i64(req.a), from_i64(req.b))
                .map(h),
            "reduce_sum" => session
                .reduce(from_i64(req.g), ReduceOp::Sum, None)
                .map(|s| KernelResponse::Float(s.as_f64())),
            "reduce_sum_masked" => session
                .reduce(from_i64(req.g), ReduceOp::Sum, Some(from_i64(req.a)))
                .map(|s| KernelResponse::Float(s.as_f64())),
            "l1_diff" => session
                .l1_diff(from_i64(req.a), from_i64(req.b))
                .map(KernelResponse::Float),
            "set_len" => session
                .set_len(from_i64(req.g))
                .map(|v| KernelResponse::Float(v as f64)),
            "is_empty" => session.is_empty(from_i64(req.g)).map(KernelResponse::Bool),
            "free" => session.free(from_i64(req.g)).map(|()| KernelResponse::Unit),
            "emit" => session
                .emit(&[(req.name.as_str(), from_i64(req.g))])
                .map(|()| KernelResponse::Unit),
            other => Err(FnError::new(0x01, format!("unknown kernel op `{other}`"))),
        }
    }
}

/// Convenience alias for a shared registry handle passed to loaders.
pub type SharedRegistry = Arc<GraphComputeRegistry>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithms::graph_compute::{Arena, WorkBudget};
    use std::sync::Arc as StdArc;
    use uni_algo::algo::GraphProjection;
    use uni_common::Value;

    fn build_projection(nodes: &[u64], edges: &[(u64, u64)]) -> GraphProjection {
        let node_rows: Vec<HashMap<String, Value>> = nodes
            .iter()
            .map(|&id| HashMap::from([("id".to_string(), Value::Int(id as i64))]))
            .collect();
        let edge_rows: Vec<HashMap<String, Value>> = edges
            .iter()
            .map(|&(s, t)| {
                HashMap::from([
                    ("source".to_string(), Value::Int(s as i64)),
                    ("target".to_string(), Value::Int(t as i64)),
                ])
            })
            .collect();
        GraphProjection::from_rows(&node_rows, &edge_rows, None, false).expect("projection builds")
    }

    /// Drives a full PPR through the JSON dispatch protocol — no loader involved.
    /// This is the loader-agnostic proof that the wire format expresses the whole
    /// algorithm; each real loader only has to carry these JSON strings.
    #[test]
    fn json_dispatch_runs_ppr_end_to_end() {
        let nodes = vec![0u64, 1, 2, 3];
        let edges = vec![(0, 1), (1, 2), (2, 0), (0, 3)];
        let graph = build_projection(&nodes, &edges);

        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            9,
            WorkBudget::from_graph_size(nodes.len() as u64, edges.len() as u64),
            Arena::new(1 << 20, 4096),
        );
        let g = to_i64(session.bind_graph(StdArc::new(graph)));
        let sid = registry.open(session);

        // Helper: issue one JSON call and expect a handle back.
        let call = |req: KernelRequest| -> KernelResponse {
            let json = serde_json::to_string(&serde_json::json!({
                "session": req.session, "op": req.op, "g": req.g, "a": req.a,
                "b": req.b, "s": req.s, "s2": req.s2, "f": req.f, "f2": req.f2,
                "k": req.k, "want_max": req.want_max,
                "wl": req.wl, "wn": req.wn, "p": req.p, "q": req.q, "seed": req.seed,
                "seeds": req.seeds, "name": req.name,
            }))
            .unwrap();
            let resp = registry.call_json(&json);
            serde_json::from_str(&resp).unwrap()
        };
        let handle = |r: KernelResponse| match r {
            KernelResponse::Handle(h) => h,
            other => panic!("expected handle, got {other:?}"),
        };
        let mk = |op: &str| KernelRequest {
            session: sid,
            op: op.to_string(),
            g: 0,
            a: 0,
            b: 0,
            s: String::new(),
            s2: String::new(),
            f: 0.0,
            f2: 0.0,
            k: 0,
            want_max: false,
            wl: 0,
            wn: 0,
            p: 1.0,
            q: 1.0,
            seed: 0,
            seeds: vec![],
            name: String::new(),
        };

        let alpha = 0.85;
        let seed_set = handle(call(KernelRequest {
            g,
            seeds: vec![0],
            ..mk("frontier")
        }));
        let seed_map = handle(call(KernelRequest {
            g: seed_set,
            f: 1.0,
            ..mk("set_to_map")
        }));
        let teleport = handle(call(KernelRequest {
            g: seed_map,
            s: "l1".into(),
            ..mk("normalize")
        }));
        let deg = handle(call(KernelRequest { g, ..mk("degrees") }.with_s("out")));
        let inv_deg = handle(call(KernelRequest {
            g: deg,
            ..mk("recip")
        }));
        let dangling = handle(call(KernelRequest {
            g: deg,
            s: "is_zero".into(),
            ..mk("map_to_set")
        }));
        let mut rank = handle(call(KernelRequest {
            g: teleport,
            f: 1.0,
            ..mk("scale")
        }));
        for _ in 0..100 {
            let contrib = handle(call(KernelRequest {
                a: rank,
                b: inv_deg,
                s: "mul".into(),
                ..mk("ewise")
            }));
            let spread = handle(call(KernelRequest {
                g,
                a: contrib,
                s: "linear_algebra".into(),
                s2: "out".into(),
                ..mk("spmv")
            }));
            let dm = match call(KernelRequest {
                g: rank,
                a: dangling,
                ..mk("reduce_sum_masked")
            }) {
                KernelResponse::Float(v) => v,
                other => panic!("expected float, got {other:?}"),
            };
            let scaled = handle(call(KernelRequest {
                g: spread,
                f: alpha,
                ..mk("scale")
            }));
            let blend = 1.0 - alpha + alpha * dm;
            let next = handle(call(KernelRequest {
                a: scaled,
                b: teleport,
                s: "axpy".into(),
                f: blend,
                ..mk("ewise")
            }));
            let _ = call(KernelRequest {
                g: contrib,
                ..mk("free")
            });
            let _ = call(KernelRequest {
                g: spread,
                ..mk("free")
            });
            let _ = call(KernelRequest {
                g: scaled,
                ..mk("free")
            });
            let _ = call(KernelRequest {
                g: rank,
                ..mk("free")
            });
            rank = next;
        }
        let _ = call(KernelRequest {
            g: rank,
            name: "score".into(),
            ..mk("emit")
        });

        let mut closed = registry.close(sid).expect("session present");
        let emitted = closed.take_emitted();
        let scores = &emitted[0].1;
        let total: f64 = scores.iter().sum();
        assert!(
            (total - 1.0).abs() < 1e-9,
            "PPR over JSON dispatch must conserve mass, got {total}"
        );
    }

    #[test]
    fn new_kernels_reachable_via_json() {
        // W3 (B2): edge_count / topk / arg_extreme / scatter / generic map_apply
        // must all be expressible over the loader-agnostic JSON wire, so the
        // §9.3 corpus (Bellman-Ford scatter, top-k egress) is guest-authorable on
        // the sandboxed loaders too.
        let nodes = vec![0u64, 1, 2, 3];
        // out-degrees: node 0 -> 3, node 1 -> 1, nodes 2,3 -> 0.
        let edges = vec![(0, 1), (0, 2), (0, 3), (1, 2)];
        let graph = build_projection(&nodes, &edges);
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            7,
            WorkBudget::from_graph_size(4, 4),
            Arena::new(1 << 20, 4096),
        );
        let g = to_i64(session.bind_graph(StdArc::new(graph)));
        let sid = registry.open(session);

        let call = |json: String| -> KernelResponse {
            serde_json::from_str(&registry.call_json(&json)).unwrap()
        };
        let as_handle = |r: KernelResponse| match r {
            KernelResponse::Handle(h) => h,
            other => panic!("want handle, got {other:?}"),
        };

        match call(format!(r#"{{"session":{sid},"op":"edge_count","g":{g}}}"#)) {
            KernelResponse::Float(e) => assert_eq!(e, 4.0, "edge_count"),
            other => panic!("edge_count -> {other:?}"),
        }

        let deg = as_handle(call(format!(
            r#"{{"session":{sid},"op":"degrees","g":{g},"s":"out"}}"#
        )));

        match call(format!(
            r#"{{"session":{sid},"op":"topk","g":{deg},"k":2}}"#
        )) {
            KernelResponse::Pairs(p) => {
                assert_eq!(p.len(), 2, "topk returns k pairs");
                assert_eq!(p[0], (0, 3.0), "highest out-degree ranked first");
            }
            other => panic!("topk -> {other:?}"),
        }

        match call(format!(
            r#"{{"session":{sid},"op":"arg_extreme","g":{deg},"want_max":true}}"#
        )) {
            KernelResponse::VidScalar { vid, f } => {
                assert_eq!((vid, f), (0, 3.0), "max out-degree is node 0")
            }
            other => panic!("arg_extreme -> {other:?}"),
        }

        // Generic affine map 2*x+1 (unreachable via scale/recip/normalize), then a
        // scatter over a frontier — both must return live handles.
        let affine = as_handle(call(format!(
            r#"{{"session":{sid},"op":"map_apply","g":{deg},"s":"affine","f":2.0,"f2":1.0}}"#
        )));
        let f1 = as_handle(call(format!(
            r#"{{"session":{sid},"op":"frontier","g":{g},"seeds":[1]}}"#
        )));
        let _scattered = as_handle(call(format!(
            r#"{{"session":{sid},"op":"scatter","a":{affine},"b":{f1},"f":99.0}}"#
        )));
    }

    #[test]
    fn unknown_session_is_typed_error_not_panic() {
        let registry = GraphComputeRegistry::new();
        let resp = registry.call_json(r#"{"session": 999, "op": "vertex_count", "g": 0}"#);
        let parsed: KernelResponse = serde_json::from_str(&resp).unwrap();
        assert!(matches!(parsed, KernelResponse::Err { code: 0x863, .. }));
    }

    #[test]
    fn malformed_json_is_typed_error_not_panic() {
        let registry = GraphComputeRegistry::new();
        let resp = registry.call_json("not json at all");
        let parsed: KernelResponse = serde_json::from_str(&resp).unwrap();
        assert!(matches!(parsed, KernelResponse::Err { code: 0x802, .. }));
    }

    #[test]
    fn session_ids_are_unguessable_not_sequential() {
        // Review H2: a concurrent guest must not be able to enumerate another
        // CALL's session id. Open a real session, then probe every low sequential
        // id — none may resolve (the real id is a 60-bit-entropy random u64).
        let nodes = vec![0u64, 1];
        let graph = build_projection(&nodes, &[(0, 1)]);
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            5,
            WorkBudget::from_graph_size(2, 1),
            Arena::new(1 << 20, 4096),
        );
        let _g = session.bind_graph(StdArc::new(graph));
        let sid = registry.open(session);
        assert!(
            sid > u32::MAX as u64 || sid == 0 || sid.count_ones() > 4,
            "id should look random, not like a small counter (got {sid})"
        );
        for guess in 0..2_000u64 {
            let req = format!(r#"{{"session": {guess}, "op": "vertex_count", "g": 0}}"#);
            let parsed: KernelResponse = serde_json::from_str(&registry.call_json(&req)).unwrap();
            assert!(
                matches!(parsed, KernelResponse::Err { code: 0x863, .. }),
                "sequential id {guess} must not resolve to a live session"
            );
        }
        // The real (random) id still works.
        let req = format!(
            r#"{{"session": {sid}, "op": "vertex_count", "g": {}}}"#,
            _g.as_u64() as i64
        );
        let parsed: KernelResponse = serde_json::from_str(&registry.call_json(&req)).unwrap();
        assert!(matches!(parsed, KernelResponse::Float(_)));
    }

    // Small builder helpers to keep the driver above readable.
    impl KernelRequest {
        fn with_s(mut self, s: &str) -> Self {
            self.s = s.to_string();
            self
        }
    }

    impl Serialize for KernelRequest {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeStruct;
            let mut st = s.serialize_struct("KernelRequest", 18)?;
            st.serialize_field("session", &self.session)?;
            st.serialize_field("op", &self.op)?;
            st.serialize_field("g", &self.g)?;
            st.serialize_field("a", &self.a)?;
            st.serialize_field("b", &self.b)?;
            st.serialize_field("s", &self.s)?;
            st.serialize_field("s2", &self.s2)?;
            st.serialize_field("f", &self.f)?;
            st.serialize_field("f2", &self.f2)?;
            st.serialize_field("k", &self.k)?;
            st.serialize_field("want_max", &self.want_max)?;
            st.serialize_field("wl", &self.wl)?;
            st.serialize_field("wn", &self.wn)?;
            st.serialize_field("p", &self.p)?;
            st.serialize_field("q", &self.q)?;
            st.serialize_field("seed", &self.seed)?;
            st.serialize_field("seeds", &self.seeds)?;
            st.serialize_field("name", &self.name)?;
            st.end()
        }
    }
}
