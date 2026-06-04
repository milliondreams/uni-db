//! Example Extism plugin — `ai.example.net.fetch_status`.
//!
//! Unlike `example-extism-geo`, this plugin **imports and calls a host
//! function**: its scalar `invoke` reaches back into the host's
//! capability-gated `uni_http_get` host fn, proving the Extism host-service
//! path end to end (the geo example imports nothing, so it never exercised a
//! guest→host call).
//!
//! Wire contract:
//!
//! - `manifest` export — declares a `network` capability attenuated to
//!   `https://api.example.com/**`. Without a matching host grant the host omits
//!   `uni_http_get` from the linker and the guest's import is unresolved
//!   (link-time gating).
//! - `register` export — declares `ai.example.net.fetch_status` as a scalar fn
//!   taking one `float64` and returning one `float64`.
//! - `invoke_ai_example_net_fetch_status` export — Arrow IPC in / out. Ignores
//!   the input values, calls `uni_http_get("https://api.example.com/ping")`,
//!   parses the JSON response, and returns the HTTP status as a `float64` for
//!   every input row.
//!
//! Build:
//!     cargo build --target wasm32-unknown-unknown --release

use std::sync::Arc;

use arrow::array::{Float64Array, RecordBatch};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use extism_pdk::*;

/// The single URL this plugin fetches. Must fall within the manifest's
/// declared `network` allow-list (`https://api.example.com/**`).
const FETCH_URL: &str = "https://api.example.com/ping";

// Host fn import. The default `ExtismHost` namespace (`extism:host/user`)
// matches the host's `Function::new("uni_http_get", …)` registration, which
// uses no custom namespace. Wire: a JSON request `{"url": …}` in, a JSON
// response `{"status": u16, "body_hex": …}` out.
#[host_fn]
extern "ExtismHost" {
    fn uni_http_get(req: String) -> String;
}

#[plugin_fn]
pub fn manifest(_: ()) -> FnResult<String> {
    Ok(serde_json::json!({
        "id": "ai.example.net",
        "version": "0.1.0",
        "abi-extism": "^1",
        "capabilities": [
            {"kind": "network", "allow": ["https://api.example.com/**"]}
        ],
        "determinism": "nondeterministic",
        "description": "Fetches an HTTP status via the host-net uni_http_get host fn."
    })
    .to_string())
}

#[plugin_fn]
pub fn register(_: ()) -> FnResult<String> {
    Ok(serde_json::json!({
        "entries": [{
            "kind": "scalar",
            "qname": "ai.example.net.fetch_status",
            "signature": {
                "args": [{"kind": "primitive", "arrow": "float64"}],
                "returns": {"kind": "primitive", "arrow": "float64"},
                "volatility": "volatile",
                "null_handling": "propagate"
            }
        }]
    })
    .to_string())
}

/// Plugin-side `invoke_ai_example_net_fetch_status` export (the
/// `invoke_<qname-with-dots-as-underscores>` ABI). Calls the host's
/// `uni_http_get` and returns the HTTP status as a `float64` per input row.
#[plugin_fn]
pub fn invoke_ai_example_net_fetch_status(input: Vec<u8>) -> FnResult<Vec<u8>> {
    let batch = decode_input(&input).map_err(|e| WithReturnCode::new(Error::msg(e), 2))?;
    let rows = batch.num_rows();

    let req = serde_json::json!({ "url": FETCH_URL }).to_string();
    let resp_json = unsafe { uni_http_get(req) }
        .map_err(|e| WithReturnCode::new(Error::msg(format!("host uni_http_get: {e}")), 2))?;
    let status = parse_status(&resp_json).map_err(|e| WithReturnCode::new(Error::msg(e), 2))?;

    let out_batch =
        status_batch(status, rows).map_err(|e| WithReturnCode::new(Error::msg(e), 2))?;
    encode_output(&out_batch).map_err(|e| WithReturnCode::new(Error::msg(e), 2))
}

fn parse_status(resp_json: &str) -> Result<f64, String> {
    let v: serde_json::Value =
        serde_json::from_str(resp_json).map_err(|e| format!("response json: {e}"))?;
    v.get("status")
        .and_then(serde_json::Value::as_u64)
        .map(|s| s as f64)
        .ok_or_else(|| format!("response missing numeric `status`: {resp_json}"))
}

fn decode_input(bytes: &[u8]) -> Result<RecordBatch, String> {
    let reader = StreamReader::try_new(bytes, None).map_err(|e| format!("reader setup: {e}"))?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for r in reader {
        batches.push(r.map_err(|e| format!("read batch: {e}"))?);
    }
    batches
        .into_iter()
        .next()
        .ok_or_else(|| "empty IPC stream".to_owned())
}

fn status_batch(status: f64, rows: usize) -> Result<RecordBatch, String> {
    let arr = Arc::new(Float64Array::from(vec![status; rows]));
    let schema = Arc::new(Schema::new(vec![Field::new("status", DataType::Float64, true)]));
    RecordBatch::try_new(schema, vec![arr]).map_err(|e| format!("RecordBatch: {e}"))
}

fn encode_output(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let schema = batch.schema();
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    {
        let mut w = StreamWriter::try_new(&mut buf, schema.as_ref())
            .map_err(|e| format!("writer: {e}"))?;
        w.write(batch).map_err(|e| format!("write: {e}"))?;
        w.finish().map_err(|e| format!("finish: {e}"))?;
    }
    Ok(buf)
}
