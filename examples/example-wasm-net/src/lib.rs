//! Example Component Model plugin — `ai.example.net.fetch_status`.
//!
//! A scalar fn that imports the capability-gated `uni:plugin/host-net`
//! interface, performs an HTTP GET against a fixed URL, and returns the
//! response status code (as `float64`) for every input row. Used by the host
//! e2e (`crates/uni-plugin-wasm/tests/example_wasm_net_e2e.rs`) to prove the
//! `host-net` linker wiring + capability gating end to end.
//!
//! It also calls `host-trace-context.get-traceparent` to exercise that
//! always-available interface (the value is informational only — outbound
//! traceparent injection is handled host-side).

use std::sync::Arc;

use arrow::array::{Float64Array, RecordBatch};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

wit_bindgen::generate!({
    world: "scalar-net-plugin",
    path: "wit",
});

use uni::plugin::host_net;
use uni::plugin::host_trace_context;

/// URL the scalar fetches. The host e2e grants `Network` covering this and
/// wires a fake egress that returns a fixed status.
const FETCH_URL: &str = "https://api.example.com/ping";

struct NetPlugin;

impl Guest for NetPlugin {
    fn manifest() -> String {
        r#"{
            "id": "ai.example.net",
            "version": "0.1.0",
            "capabilities": [
                {"kind": "network", "allow": ["https://api.example.com/**"]}
            ],
            "determinism": "volatile",
            "description": "Scalar fn that fetches an HTTP status via host-net."
        }"#
        .to_owned()
    }

    fn register() -> String {
        r#"{
            "entries": [{
                "kind": "scalar",
                "qname": "ai.example.net.fetch_status",
                "signature": {
                    "args": [
                        {"kind": "primitive", "arrow": "float64"}
                    ],
                    "returns": {"kind": "primitive", "arrow": "float64"},
                    "volatility": "volatile",
                    "null_handling": "propagate"
                }
            }]
        }"#
        .to_owned()
    }

    fn invoke_scalar(qname: String, ipc_bytes: Vec<u8>) -> Result<Vec<u8>, FnError> {
        if qname != "ai.example.net.fetch_status" {
            return Err(FnError {
                code: 1,
                message: format!("unknown qname: {qname}"),
                retryable: false,
            });
        }
        // Exercise the always-available trace-context interface (informational).
        let _tp = host_trace_context::get_traceparent();
        // 0 timeout / 0 max-bytes => host defaults/ceiling.
        let resp = host_net::http_get(FETCH_URL, 0, 0)?;
        let rows = input_row_count(&ipc_bytes).map_err(|e| FnError {
            code: 2,
            message: e,
            retryable: false,
        })?;
        encode_status(rows, resp.status).map_err(|e| FnError {
            code: 3,
            message: e,
            retryable: false,
        })
    }
}

fn input_row_count(bytes: &[u8]) -> Result<usize, String> {
    let reader = StreamReader::try_new(bytes, None).map_err(|e| format!("reader: {e}"))?;
    let mut rows = 0usize;
    for r in reader {
        rows += r.map_err(|e| format!("read: {e}"))?.num_rows();
    }
    Ok(rows.max(1))
}

fn encode_status(rows: usize, status: u16) -> Result<Vec<u8>, String> {
    let arr = Arc::new(Float64Array::from(vec![f64::from(status); rows]));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Float64,
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![arr]).map_err(|e| format!("batch: {e}"))?;
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    {
        let mut w = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|e| format!("writer: {e}"))?;
        w.write(&batch).map_err(|e| format!("write: {e}"))?;
        w.finish().map_err(|e| format!("finish: {e}"))?;
    }
    Ok(buf)
}

export!(NetPlugin);
