//! Repro for crates/uni-plugin-pyo3/src/adapter_aggregate.rs:268
//!
//! `PyAccumulator::state()` returns `Utf8(Some("{}"))` for an empty
//! accumulator (one that never received a non-empty `update_batch`).
//! The comment at lines 266-267 claims this makes the receiving
//! `merge_batch` a "no-op". It does NOT: `merge_batch` (lines 240-254)
//! only skips `is_null` entries — a non-null literal `"{}"` is
//! `json.loads`'ed into an empty dict and passed as the SECOND argument
//! to the user's `merge(state, {})`. For the dict-shaped state this very
//! file documents/tests (`merge` doing `a["sum"] + b["sum"]`), this
//! evaluates `{}["sum"]` and raises Python `KeyError`, which
//! `classify_pyerr` wraps into an `FnError`, aborting the aggregation.
//!
//! This is the exact scenario of a global (no-GROUP-BY) aggregate over a
//! multi-partition scan where one partition produced zero rows: that
//! empty partition emits `"{}"` from `state()`, and the final
//! accumulator's `merge_batch` blows up instead of contributing nothing.

#![cfg(feature = "pyo3")]

use std::ffi::CString;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, StringArray};
use datafusion::scalar::ScalarValue;
use pyo3::prelude::*;
use smol_str::SmolStr;

use uni_plugin::PluginId;
use uni_plugin::traits::aggregate::AggregatePluginFn;
use uni_plugin_pyo3::{PyAggregateFn, PyPluginRuntime, build_py_agg_signature};

/// Sum-of-floats aggregate spec whose state is the dict shape
/// `{"sum": .., "n": ..}` — identical to the in-file tests at
/// adapter_aggregate.rs:420-421 / 457-458.
const SUM_SPEC: &str = r#"
def init():
    return {"sum": 0.0, "n": 0}

def accumulate(state, x):
    if x is None:
        return state
    state["sum"] += float(x)
    state["n"] += 1
    return state

def merge(a, b):
    return {"sum": a["sum"] + b["sum"], "n": a["n"] + b["n"]}

def finalize(state):
    return state["sum"]
"#;

fn runtime_with_agg(spec_src: &str) -> Arc<PyPluginRuntime> {
    let rt = PyPluginRuntime::new(PluginId::new("ai.test.aggrepro"));
    Python::attach(|py| {
        let code = CString::new(spec_src).unwrap();
        let module = pyo3::types::PyModule::from_code(
            py,
            code.as_c_str(),
            CString::new("agg_repro.py").unwrap().as_c_str(),
            CString::new("agg_repro").unwrap().as_c_str(),
        )
        .expect("module compiles");
        for method in ["init", "accumulate", "merge", "finalize"] {
            let f = module.getattr(method).unwrap().unbind();
            rt.insert(format!("sum_floats::{method}"), f);
        }
    });
    rt
}

/// Drives the two-partition merge where partition B is EMPTY (zero rows,
/// never updated). Correct behavior (now fixed): the empty partition emits
/// a NULL state, `merge_batch` skips it, and the merged result equals
/// partition A's sum (6.0).
#[test]
fn empty_partition_state_is_noop_in_merge() {
    Python::initialize();
    let rt = runtime_with_agg(SUM_SPEC);
    let sig = build_py_agg_signature(&[SmolStr::new("float")], &SmolStr::new("float"), "pure")
        .expect("sig");
    let agg = PyAggregateFn::new(Arc::clone(&rt), "sum_floats", sig);

    // Partition A: [1, 2, 3] -> proper JSON state.
    let mut acc_a = agg.create_accumulator();
    let batch_a: ArrayRef = Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0]));
    acc_a.update_batch(&[batch_a]).expect("update a");
    let state_a = acc_a.state().expect("state a");
    let state_a_json = match &state_a[0] {
        ScalarValue::Utf8(Some(s)) => s.clone(),
        other => panic!("expected Utf8 state for A, got {other:?}"),
    };

    // Partition B: EMPTY — never call update_batch. Its state() must now be
    // a NULL sentinel (adapter_aggregate.rs:268) so merge_batch skips it.
    let acc_b = agg.create_accumulator();
    let state_b = acc_b.state().expect("state b");
    match &state_b[0] {
        ScalarValue::Utf8(None) => {}
        other => panic!("empty accumulator must emit a NULL state, got {other:?}"),
    }

    // Final aggregator merges [state_a, NULL] — exactly what DataFusion
    // does when one scan partition was empty.
    let mut acc_final = agg.create_accumulator();
    let merge_arr: ArrayRef = Arc::new(StringArray::from(vec![
        Some(state_a_json),
        None, // the NULL sentinel from the empty partition
    ]));
    acc_final
        .merge_batch(&[merge_arr])
        .expect("merge_batch must skip the NULL empty-partition state");

    // FIXED: the empty partition contributes nothing, so the finalized value
    // equals partition A's sum (6.0). (fix for adapter_aggregate.rs:268)
    let result = acc_final.evaluate().expect("evaluate");
    match result {
        ScalarValue::Float64(Some(v)) => assert!(
            (v - 6.0).abs() < 1e-9,
            "empty partition must be a no-op; expected 6.0, got {v}"
        ),
        other => panic!("expected Float64(6.0), got {other:?}"),
    }
}
