//! Example Component Model plugin — a Mode B-seq scratch-graph guest (§7b).
//!
//! Builds and walks a session-local *mutable* scratch graph through the imported
//! `host-graph` interface: every op is one JSON round-trip carrying only slots +
//! scalars (no host state crosses in). The host backs `graph-call` with a
//! `ScratchRegistry` and drives the exported `run(session)` directly.
//!
//! Build: `cargo build --target wasm32-wasip2 --release`.

wit_bindgen::generate!({
    world: "scratch-guest",
    path: "wit",
});

use uni::scratch::host_graph;

struct Component;

/// One scratch op through the host, returning the response's `v` value.
fn call(req: serde_json::Value) -> serde_json::Value {
    let resp = match host_graph::graph_call(&req.to_string()) {
        Ok(s) => s,
        Err(e) => return serde_json::json!({"err": e.message}),
    };
    let parsed: serde_json::Value = match serde_json::from_str(&resp) {
        Ok(v) => v,
        Err(e) => return serde_json::json!({"err": format!("resp json: {e}")}),
    };
    parsed.get("v").cloned().unwrap_or(serde_json::Value::Null)
}

impl Guest for Component {
    fn run(session: u64) -> String {
        // Build a small mutable graph: 0->1, 0->2, 1->3.
        for _ in 0..4 {
            call(serde_json::json!({"session": session, "op": "add_node", "f": 0.0}));
        }
        for (a, b) in [(0, 1), (0, 2), (1, 3)] {
            call(serde_json::json!({"session": session, "op": "add_edge", "a": a, "b": b}));
        }
        // Mutate and read back via random access.
        call(serde_json::json!({"session": session, "op": "set_field", "a": 1, "f": 42.0}));
        let field1 = call(serde_json::json!({"session": session, "op": "get_field", "a": 1}))
            .as_f64()
            .unwrap_or(0.0);
        let deg0 = call(serde_json::json!({"session": session, "op": "neighbors", "a": 0}))
            .as_array()
            .map_or(0, Vec::len);
        let nodes = call(serde_json::json!({"session": session, "op": "node_count"}))
            .as_u64()
            .unwrap_or(0);
        // A seeded sample decision, to exercise the reproducible RNG path.
        let fired = call(
            serde_json::json!({"session": session, "op": "sample", "a": 2, "f": 1.0, "iter": 0}),
        )
        .as_bool()
        .unwrap_or(false);
        serde_json::json!({
            "nodes": nodes,
            "field1": field1,
            "deg0": deg0,
            "sampled": fired,
        })
        .to_string()
    }

    /// The JIT'd-WASM arm of the Mode B-seq perf gate (§14 Q1 / Q-5).
    ///
    /// Chases `steps` pointers over the host-built scratch graph, mirroring the
    /// host-resident baseline hop-for-hop: `neighbors(node)` → take the
    /// `k % degree`-th neighbor → accumulate `get_field(node)`. Two host
    /// crossings per step, which is the cost the gate measures.
    fn walk(session: u64, steps: u32) -> f64 {
        let mut node: u64 = 0;
        let mut acc = 0.0f64;
        for k in 0..steps as usize {
            let nb = call(serde_json::json!({
                "session": session, "op": "neighbors", "a": node
            }));
            let Some(arr) = nb.as_array() else { break };
            if arr.is_empty() {
                break;
            }
            node = arr[k % arr.len()].as_u64().unwrap_or(0);
            acc += call(serde_json::json!({
                "session": session, "op": "get_field", "a": node
            }))
            .as_f64()
            .unwrap_or(0.0);
        }
        acc
    }

    /// Batched-descent MCTS (issue #152): `batch` rollouts advance in lockstep,
    /// two crossings per level (`visit_batch` + `descend_batch`) no matter how
    /// wide the batch. The host owns the tree and the child-selection scan; the
    /// guest is the conductor driving levels and batches.
    fn mcts_batched(session: u64, rollouts: u32, batch: u32, depth: u32) -> f64 {
        let batches = if batch == 0 { 0 } else { rollouts / batch };
        for _ in 0..batches {
            let mut active: Vec<u32> = vec![0; batch as usize];
            for _ in 0..depth {
                call(serde_json::json!({
                    "session": session, "op": "visit_batch", "v": active, "f": 1.0
                }));
                let next = call(serde_json::json!({
                    "session": session, "op": "descend_batch", "v": active
                }));
                let Some(arr) = next.as_array() else { break };
                if arr.is_empty() {
                    break;
                }
                active = arr
                    .iter()
                    .filter_map(|x| x.as_u64().map(|u| u as u32))
                    .collect();
            }
        }
        call(serde_json::json!({"session": session, "op": "get_field", "a": 0}))
            .as_f64()
            .unwrap_or(0.0)
    }

    /// Batched MCTS over the **typed** imports (issue #152).
    ///
    /// Byte-for-byte the same search as [`Guest::mcts_batched`], but each level
    /// crosses as a `list<u32>` rather than a JSON string: no `serde_json`
    /// encode/parse on either side, only the canonical-ABI list copy.
    fn mcts_batched_typed(session: u64, rollouts: u32, batch: u32, depth: u32) -> f64 {
        let batches = if batch == 0 { 0 } else { rollouts / batch };
        for _ in 0..batches {
            let mut active: Vec<u32> = vec![0; batch as usize];
            for _ in 0..depth {
                if host_graph::visit_batch(session, &active, 1.0).is_err() {
                    return f64::NAN;
                }
                match host_graph::descend_batch(session, &active) {
                    Ok(next) if !next.is_empty() => active = next,
                    Ok(_) => break,
                    Err(_) => return f64::NAN,
                }
            }
        }
        call(serde_json::json!({"session": session, "op": "get_field", "a": 0}))
            .as_f64()
            .unwrap_or(0.0)
    }
}

export!(Component);
