//! Example Component Model plugin — `ai.example.wasmgc.ppr`.
//!
//! A GraphCompute guest algorithm: Personalized PageRank driven through the
//! imported `host-graph` interface. Every kernel is one JSON round-trip carrying
//! only handles + scalars — no vertex data crosses into the guest. This is the
//! WASM (Component Model) half of the cross-loader GraphCompute corpus
//! (proposal §9.3).
//!
//! Build: `cargo build --target wasm32-wasip2 --release`.

wit_bindgen::generate!({
    world: "graph-algo-plugin",
    path: "wit",
});

use uni::plugin::host_graph;

struct GraphPlugin;

/// One kernel call: merges `session` into `req`, calls the host, returns the
/// response value (`v`) or an error message.
fn kernel(session: u64, mut req: serde_json::Value) -> Result<serde_json::Value, String> {
    req["session"] = session.into();
    let resp = host_graph::graph_call(&req.to_string()).map_err(|e| format!("host: {}", e.message))?;
    let v: serde_json::Value = serde_json::from_str(&resp).map_err(|e| format!("resp json: {e}"))?;
    if v.get("t").and_then(|t| t.as_str()) == Some("e") {
        return Err(format!("kernel error: {}", v.get("v").cloned().unwrap_or_default()));
    }
    Ok(v.get("v").cloned().unwrap_or(serde_json::Value::Null))
}

fn h(session: u64, req: serde_json::Value) -> Result<i64, String> {
    kernel(session, req)?.as_i64().ok_or_else(|| "expected handle".to_owned())
}

fn f(session: u64, req: serde_json::Value) -> Result<f64, String> {
    kernel(session, req)?.as_f64().ok_or_else(|| "expected float".to_owned())
}

fn ppr(session: u64, g: i64, source: i64) -> Result<(), String> {
    let alpha = 0.85;
    let seed_set = h(session, serde_json::json!({"op": "frontier", "g": g, "seeds": [source]}))?;
    let seed_map = h(session, serde_json::json!({"op": "set_to_map", "g": seed_set, "f": 1.0}))?;
    let teleport = h(session, serde_json::json!({"op": "normalize", "g": seed_map, "s": "l1"}))?;
    kernel(session, serde_json::json!({"op": "free", "g": seed_map}))?;
    kernel(session, serde_json::json!({"op": "free", "g": seed_set}))?;

    let deg = h(session, serde_json::json!({"op": "degrees", "g": g, "s": "out"}))?;
    let inv_deg = h(session, serde_json::json!({"op": "recip", "g": deg}))?;
    let dangling = h(session, serde_json::json!({"op": "map_to_set", "g": deg, "s": "is_zero", "f": 0.0}))?;
    kernel(session, serde_json::json!({"op": "free", "g": deg}))?;

    let mut rank = h(session, serde_json::json!({"op": "scale", "g": teleport, "f": 1.0}))?;
    for _ in 0..100 {
        let contrib = h(session, serde_json::json!({"op": "ewise", "a": rank, "b": inv_deg, "s": "mul"}))?;
        let spread = h(session, serde_json::json!({"op": "spmv", "g": g, "a": contrib, "s": "linear_algebra", "s2": "out"}))?;
        kernel(session, serde_json::json!({"op": "free", "g": contrib}))?;
        let dm = f(session, serde_json::json!({"op": "reduce_sum_masked", "g": rank, "a": dangling}))?;
        let scaled = h(session, serde_json::json!({"op": "scale", "g": spread, "f": alpha}))?;
        kernel(session, serde_json::json!({"op": "free", "g": spread}))?;
        let blend = 1.0 - alpha + alpha * dm;
        let next = h(session, serde_json::json!({"op": "ewise", "a": scaled, "b": teleport, "s": "axpy", "f": blend}))?;
        kernel(session, serde_json::json!({"op": "free", "g": scaled}))?;
        let diff = f(session, serde_json::json!({"op": "l1_diff", "a": rank, "b": next}))?;
        kernel(session, serde_json::json!({"op": "free", "g": rank}))?;
        rank = next;
        if diff < 1e-9 {
            break;
        }
    }

    kernel(session, serde_json::json!({"op": "free", "g": teleport}))?;
    kernel(session, serde_json::json!({"op": "free", "g": inv_deg}))?;
    kernel(session, serde_json::json!({"op": "free", "g": dangling}))?;
    kernel(session, serde_json::json!({"op": "emit", "g": rank, "name": "score"}))?;
    Ok(())
}

impl Guest for GraphPlugin {
    fn manifest() -> String {
        r#"{
            "id": "ai.example.wasmgc",
            "version": "0.1.0",
            "capabilities": [
                {"kind": "graph-compute"},
                {"kind": "host-query", "read_only": true, "scopes": []}
            ],
            "determinism": "pure",
            "description": "Personalized PageRank via the host-graph interface."
        }"#
        .to_owned()
    }

    fn register() -> String {
        r#"{
            "entries": [{
                "kind": "algorithm",
                "qname": "ai.example.wasmgc.ppr",
                "args": [{"kind": "primitive", "arrow": "int64"}],
                "yields": ["nodeId:int", "score:float"]
            }]
        }"#
        .to_owned()
    }

    /// Stub so the host's scalar bootstrap pass instantiates this component.
    fn invoke_scalar(_qname: String, _ipc: Vec<u8>) -> Result<Vec<u8>, FnError> {
        Err(FnError {
            code: 0x01,
            message: "this plugin exposes no scalar fns".to_owned(),
            retryable: false,
        })
    }

    fn invoke_algorithm(_qname: String, args_ipc: Vec<u8>) -> Result<Vec<u8>, FnError> {
        let req: serde_json::Value = serde_json::from_slice(&args_ipc).map_err(|e| FnError {
            code: 0x802,
            message: format!("input json: {e}"),
            retryable: false,
        })?;
        let session = req.get("session").and_then(serde_json::Value::as_u64).unwrap_or(0);
        let g = req.get("graph").and_then(serde_json::Value::as_i64).unwrap_or(0);
        let source = req
            .get("args")
            .and_then(|a| a.get(0))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        ppr(session, g, source).map_err(|e| FnError {
            code: 0x86D,
            message: e,
            retryable: false,
        })?;
        Ok(Vec::new())
    }
}

export!(GraphPlugin);
