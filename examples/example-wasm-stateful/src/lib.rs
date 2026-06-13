//! Example Component Model plugin — `ai.example.stateful`.
//!
//! Carries a module-level mutable global (`COUNTER`) to prove per-invoke
//! `Store`/instance isolation. Two scalar fns share the world's
//! `invoke-scalar` export, dispatched on `qname`:
//!
//! - `ai.example.stateful.bump` — increments `COUNTER` and returns the
//!   new value as a 1×1 Float64 batch. Under a fresh-instance-per-invoke
//!   host, every call returns `1.0` (each instance's linear memory — and
//!   thus `COUNTER` — starts zeroed). Under a host that reuses one warm
//!   instance, successive calls would return `2.0`, `3.0`, …, which is
//!   the state-leak bug the isolation test pins.
//! - `ai.example.stateful.boom` — increments `COUNTER`, then traps the
//!   guest (`unreachable`). A host that recycles the trapped store would
//!   re-trap or read poisoned memory on the next call; a host that drops
//!   it gets a clean instance next time.
//!
//! Mirrors `example-wasm-geo`: same Cargo setup, `wasm32-wasip2`, same
//! `scalar-plugin` WIT world and Arrow-IPC return shape.

use std::sync::Arc;

use arrow::array::{Float64Array, RecordBatch};
use arrow::ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

wit_bindgen::generate!({
    world: "scalar-plugin",
    path: "wit",
});

/// Module-level mutable counter.
///
/// A wasm component is single-threaded per instance, so a plain
/// `static mut` accessed through a small `unsafe` shim is sufficient and
/// avoids pulling atomics into the guest. The value lives in the
/// instance's linear memory: a fresh instance starts it at `0`, a reused
/// instance carries the prior value forward — exactly the difference the
/// host-side isolation test observes.
static mut COUNTER: u64 = 0;

/// Increment `COUNTER` and return its new value.
///
/// # Safety
///
/// A component instance is single-threaded, so there is no concurrent
/// access to `COUNTER`; the read-modify-write is therefore data-race
/// free within one instance.
fn bump_counter() -> u64 {
    // SAFETY: single-threaded guest instance; no aliasing or concurrent
    // access to `COUNTER` is possible across this read-modify-write.
    unsafe {
        let next = COUNTER + 1;
        COUNTER = next;
        next
    }
}

struct StatefulPlugin;

impl Guest for StatefulPlugin {
    fn manifest() -> String {
        r#"{
            "id": "ai.example.stateful",
            "version": "0.1.0",
            "capabilities": [],
            "determinism": "pure",
            "description": "Mutable-global plugin for per-invoke isolation tests (CM)."
        }"#
            .to_owned()
    }

    fn register() -> String {
        r#"{
            "entries": [
                {
                    "kind": "scalar",
                    "qname": "ai.example.stateful.bump",
                    "signature": {
                        "args": [{"kind": "primitive", "arrow": "float64"}],
                        "returns": {"kind": "primitive", "arrow": "float64"},
                        "volatility": "immutable",
                        "null_handling": "propagate"
                    }
                },
                {
                    "kind": "scalar",
                    "qname": "ai.example.stateful.boom",
                    "signature": {
                        "args": [{"kind": "primitive", "arrow": "float64"}],
                        "returns": {"kind": "primitive", "arrow": "float64"},
                        "volatility": "immutable",
                        "null_handling": "propagate"
                    }
                }
            ]
        }"#
            .to_owned()
    }

    fn invoke_scalar(qname: String, _ipc_bytes: Vec<u8>) -> Result<Vec<u8>, FnError> {
        match qname.as_str() {
            "ai.example.stateful.bump" => {
                let n = bump_counter();
                encode_scalar_f64(n as f64).map_err(|e| FnError {
                    code: 2,
                    message: e,
                    retryable: false,
                })
            }
            "ai.example.stateful.boom" => {
                // Mutate state first so a buggy host that recycles the
                // trapped store would observe the bump on the *next*
                // call, then trap the guest.
                let _ = bump_counter();
                // Aborts the guest with a wasm trap; surfaces host-side as
                // a wasmtime `Trap` (mapped to `WasmError::Invoke`).
                unreachable!("ai.example.stateful.boom intentionally traps the guest")
            }
            other => Err(FnError {
                code: 1,
                message: format!("unknown qname: {other}"),
                retryable: false,
            }),
        }
    }
}

/// Encode a single f64 as a 1-row × 1-col Arrow IPC stream.
fn encode_scalar_f64(value: f64) -> Result<Vec<u8>, String> {
    let arr = Arc::new(Float64Array::from(vec![value]));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "counter",
        DataType::Float64,
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![arr]).map_err(|e| format!("batch: {e}"))?;
    let mut buf: Vec<u8> = Vec::with_capacity(512);
    {
        let mut w =
            StreamWriter::try_new(&mut buf, schema.as_ref()).map_err(|e| format!("writer: {e}"))?;
        w.write(&batch).map_err(|e| format!("write: {e}"))?;
        w.finish().map_err(|e| format!("finish: {e}"))?;
    }
    Ok(buf)
}

export!(StatefulPlugin);
