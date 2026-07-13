//! Example Extism plugin — `ai.example.extismgc.ppr`.
//!
//! A GraphCompute guest algorithm: Personalized PageRank driven entirely through
//! the host's `uni_graph_call` host fn. Every kernel is one JSON round-trip
//! carrying only handles + scalars — no vertex data crosses into the guest
//! ("conductor, not worker"). This is the Extism half of the cross-loader
//! GraphCompute corpus (proposal §9.3).
//!
//! Wire contract:
//! - `manifest` — declares `graph-compute` + `host-query` capabilities.
//! - `register` — declares the `ai.example.extismgc.ppr` algorithm entry.
//! - `algo_ai_example_extismgc_ppr_invoke` — input JSON `{session, graph, args}`;
//!   drives the kernels and emits `score`; returns empty (the host reads the
//!   emitted column from the session).
//!
//! Build: `cargo build --target wasm32-unknown-unknown --release`.

use extism_pdk::*;

// Host fn import (default `ExtismHost` namespace matches the host's
// `Function::new("uni_graph_call", …)`). One JSON kernel call in, one out.
#[host_fn]
extern "ExtismHost" {
    fn uni_graph_call(req: String) -> String;
}

#[plugin_fn]
pub fn manifest(_: ()) -> FnResult<String> {
    Ok(serde_json::json!({
        "id": "ai.example.extismgc",
        "version": "0.1.0",
        "abi-extism": "^1",
        "capabilities": [
            {"kind": "graph-compute"},
            {"kind": "host-query", "read_only": true, "scopes": []}
        ],
        "determinism": "pure",
        "description": "Personalized PageRank via the GraphCompute host fn."
    })
    .to_string())
}

#[plugin_fn]
pub fn register(_: ()) -> FnResult<String> {
    Ok(serde_json::json!({
        "entries": [{
            "kind": "algorithm",
            "qname": "ai.example.extismgc.ppr",
            "args": [{"kind": "primitive", "arrow": "int64"}],
            "yields": ["nodeId:int", "score:float"]
        }]
    })
    .to_string())
}

/// One kernel call: merges `session` into `req`, calls the host, and returns the
/// parsed response value (`v`), or an error message on a typed kernel error.
fn kernel(session: u64, mut req: serde_json::Value) -> Result<serde_json::Value, String> {
    req["session"] = session.into();
    let resp = unsafe { uni_graph_call(req.to_string()) }.map_err(|e| format!("host: {e}"))?;
    let v: serde_json::Value =
        serde_json::from_str(&resp).map_err(|e| format!("resp json: {e}"))?;
    if v.get("t").and_then(|t| t.as_str()) == Some("e") {
        return Err(format!("kernel error: {}", v.get("v").cloned().unwrap_or_default()));
    }
    Ok(v.get("v").cloned().unwrap_or(serde_json::Value::Null))
}

/// A kernel call returning a handle (`i64`).
fn h(session: u64, req: serde_json::Value) -> Result<i64, String> {
    kernel(session, req)?
        .as_i64()
        .ok_or_else(|| "expected handle".to_owned())
}

/// A kernel call returning a scalar (`f64`).
fn f(session: u64, req: serde_json::Value) -> Result<f64, String> {
    kernel(session, req)?
        .as_f64()
        .ok_or_else(|| "expected float".to_owned())
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

/// The algorithm invoke export. Input JSON `{session, graph, args}`.
#[plugin_fn]
pub fn algo_ai_example_extismgc_ppr_invoke(input: Vec<u8>) -> FnResult<Vec<u8>> {
    let req: serde_json::Value = serde_json::from_slice(&input)
        .map_err(|e| WithReturnCode::new(Error::msg(format!("input json: {e}")), 2))?;
    let session = req.get("session").and_then(serde_json::Value::as_u64).unwrap_or(0);
    let g = req.get("graph").and_then(serde_json::Value::as_i64).unwrap_or(0);
    let source = req
        .get("args")
        .and_then(|a| a.get(0))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    ppr(session, g, source).map_err(|e| WithReturnCode::new(Error::msg(e), 2))?;
    Ok(Vec::new())
}
