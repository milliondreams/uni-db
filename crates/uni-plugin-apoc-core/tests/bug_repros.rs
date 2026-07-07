//! Runnable repros for three verified correctness findings in
//! `uni-plugin-apoc-core`. Each test drives the REAL public surface: the
//! procedure is installed into a `PluginRegistry` exactly as the host does,
//! looked up by qname, and invoked with the same `ColumnarValue` scalars the
//! executor's `value_to_columnar` would hand it.
//!
//! These are additive test-only files; no production source is modified.

use arrow_array::{Array, Float64Array, StringArray};
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use uni_plugin::traits::procedure::ProcedureContext;
use uni_plugin::{Plugin, PluginRegistrar, PluginRegistry, QName};
use uni_plugin_apoc_core::ApocCorePlugin;

/// Install `ApocCorePlugin` into a fresh registry (mirrors the host loader).
fn install() -> PluginRegistry {
    let registry = PluginRegistry::new();
    let plugin = ApocCorePlugin::new();
    let manifest = plugin.manifest();
    let caps = manifest.capabilities.clone();
    let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
    plugin.register(&mut r).expect("register");
    r.commit_to_registry().expect("commit");
    registry
}

/// Invoke `apoc-core::<local>` with `args`, returning the first result batch's
/// single column downcast to `StringArray` value, as a String.
async fn invoke_string(local: &str, args: Vec<ColumnarValue>) -> String {
    let registry = install();
    let q = QName::new("apoc-core", local);
    let entry = registry.procedure(&q).expect("procedure registered");
    let mut stream = entry
        .procedure
        .invoke(ProcedureContext::default(), &args)
        .expect("invoke");
    let batch = stream.next().await.expect("row").expect("ok");
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("StringArray");
    col.value(0).to_owned()
}

/// Invoke `apoc-core::<local>` returning the first `Float64Array` value.
async fn invoke_f64(local: &str, args: Vec<ColumnarValue>) -> f64 {
    let registry = install();
    let q = QName::new("apoc-core", local);
    let entry = registry.procedure(&q).expect("procedure registered");
    let mut stream = entry
        .procedure
        .invoke(ProcedureContext::default(), &args)
        .expect("invoke");
    let batch = stream.next().await.expect("row").expect("ok");
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64Array");
    col.value(0)
}

/// [1] number.rs:153 — number.toString widens an Int64 scalar through
/// `*v as f64`, silently corrupting integers with magnitude > 2^53 before
/// formatting.
///
/// `9007199254740993` = 2^53 + 1 is an exact i64 not representable in f64;
/// widening rounds it to 9007199254740992.0.
#[tokio::test]
async fn repro_number_tostring_int64_precision_loss() {
    let n: i64 = 9_007_199_254_740_993; // 2^53 + 1
    let out = invoke_string(
        "number.toString",
        vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(n)))],
    )
    .await;

    // BUG: expected "9007199254740993", got "9007199254740992"
    // (repro for src/procedures/number.rs:153 via support.rs:231 `*v as f64`).
    // FIXED (number.rs): Int64 is formatted exactly, no f64 widening.
    assert_eq!(out, n.to_string(), "integer must format exactly (got {out})");
}

/// [2] text.rs:331 — text.repeat caps the repeat COUNT at
/// `MAX_SYNTHESIZED_LEN` (1_000_000) instead of the total synthesized length.
/// The doc on that constant claims it bounds "text.repeat's total length".
///
/// A 100-byte base repeated 1_000_000 times yields 100_000_000 bytes — 100×
/// the intended bound — proving the cap does not bound total length.
#[tokio::test]
async fn repro_text_repeat_cap_ignores_total_length() {
    let base: String = "x".repeat(100); // 100-byte base string
    let count: i64 = 1_000_000; // == MAX_SYNTHESIZED_LEN
    let out = invoke_string(
        "text.repeat",
        vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(base))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(count))),
        ],
    )
    .await;

    // FIXED (text.rs): the cap now bounds the total synthesized length. A
    // 100-byte base capped to MAX_SYNTHESIZED_LEN (1_000_000) yields 10_000
    // repeats = exactly 1_000_000 bytes, honoring the documented bound.
    assert_eq!(
        out.len(),
        1_000_000,
        "total length must be bounded to MAX_SYNTHESIZED_LEN"
    );
}

/// [3] math.rs:164 — math.round computes `scale = 10f64.powi(precision as i32)`.
/// For precision >= 309 the scale overflows to +inf, making the result NaN
/// (inf/inf) instead of the correctly-rounded value.
#[tokio::test]
async fn repro_math_round_large_precision_yields_nan() {
    let out = invoke_f64(
        "math.round",
        vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(3.5))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(400))),
        ],
    )
    .await;

    // FIXED (math.rs): rounding 3.5 to 400 decimals is a no-op past f64
    // resolution -> 3.5, not NaN.
    assert_eq!(out, 3.5, "round(3.5, 400) must be a no-op, got {out}");
}
