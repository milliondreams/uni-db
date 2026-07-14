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
}

export!(Component);
