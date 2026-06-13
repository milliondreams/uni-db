//! Example Extism plugin — `ai.example.stateful`.
//!
//! Carries a module-level mutable `COUNTER` to prove the extism loader
//! builds a *fresh* `extism::Plugin` per invoke. Two scalar exports,
//! named per the host's `invoke_<sanitized-qname>` convention
//! (`uni-plugin-extism/src/adapter.rs::scalar_export_name`):
//!
//! - `invoke_ai_example_stateful_bump` — increments `COUNTER`, returns
//!   the new value as a 1×1 Float64 IPC batch. With a fresh plugin per
//!   call, every invocation returns `1.0`; a reused instance would
//!   return `2.0`, `3.0`, … (the leak this fixture pins).
//! - `invoke_ai_example_stateful_boom` — increments `COUNTER`, then
//!   traps the guest. A recycled trapped instance would re-trap or leak
//!   state on the next call.
//!
//! Mirrors `example-extism-geo`: same Cargo setup, `wasm32-unknown-unknown`.

use std::sync::Arc;

use arrow::array::{Float64Array, RecordBatch};
use arrow::ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use extism_pdk::*;

/// Module-level mutable counter, living in the plugin's linear memory.
///
/// A fresh `extism::Plugin` starts it at `0`; a reused instance carries
/// the prior value forward — exactly the difference the host-side
/// per-invoke-freshness test observes.
static mut COUNTER: u64 = 0;

/// Increment `COUNTER` and return its new value.
fn bump_counter() -> u64 {
    // SAFETY: an extism plugin instance is single-threaded, so there is
    // no concurrent access to `COUNTER` across this read-modify-write.
    unsafe {
        let next = COUNTER + 1;
        COUNTER = next;
        next
    }
}

#[plugin_fn]
pub fn manifest(_: ()) -> FnResult<String> {
    Ok(serde_json::json!({
        "id": "ai.example.stateful",
        "version": "0.1.0",
        "abi-extism": "^1",
        "capabilities": [],
        "determinism": "pure",
        "description": "Mutable-global plugin for per-invoke isolation tests."
    })
    .to_string())
}

#[plugin_fn]
pub fn register(_: ()) -> FnResult<String> {
    Ok(serde_json::json!({
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
    })
    .to_string())
}

/// `ai.example.stateful.bump` — increment the counter, return the new value.
#[plugin_fn]
pub fn invoke_ai_example_stateful_bump(_input: Vec<u8>) -> FnResult<Vec<u8>> {
    let n = bump_counter();
    let out = encode_scalar_f64(n as f64).map_err(|e| WithReturnCode::new(Error::msg(e), 2))?;
    Ok(out)
}

/// `ai.example.stateful.boom` — mutate state, then trap the guest.
#[plugin_fn]
pub fn invoke_ai_example_stateful_boom(_input: Vec<u8>) -> FnResult<Vec<u8>> {
    // Mutate state first so a buggy host that recycles the trapped
    // instance would observe the bump on the next call.
    let _ = bump_counter();
    // Aborts the guest with a wasm trap; surfaces host-side as an error.
    panic!("ai.example.stateful.boom intentionally traps the guest")
}

/// Encode a single f64 as a 1-row × 1-col Arrow IPC stream.
fn encode_scalar_f64(value: f64) -> Result<Vec<u8>, String> {
    let arr = Arc::new(Float64Array::from(vec![value]));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "counter",
        DataType::Float64,
        true,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![arr]).map_err(|e| format!("batch: {e}"))?;
    let mut buf: Vec<u8> = Vec::with_capacity(512);
    {
        let mut w =
            StreamWriter::try_new(&mut buf, schema.as_ref()).map_err(|e| format!("writer: {e}"))?;
        w.write(&batch).map_err(|e| format!("write: {e}"))?;
        w.finish().map_err(|e| format!("finish: {e}"))?;
    }
    Ok(buf)
}
