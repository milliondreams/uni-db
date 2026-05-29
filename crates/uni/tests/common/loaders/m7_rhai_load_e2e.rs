#![allow(dead_code, unused_imports, clippy::all)]
//! M7 Phase 6 — end-to-end test of `Uni::load_rhai_plugin`.
//!
//! Opens an in-memory `Uni`, loads a Rhai plugin that exports a scalar
//! `score(x, y)`, then invokes it directly through the plugin registry.
//! This proves the host-level integration: `Uni::load_rhai_plugin` →
//! `PluginRegistrar::commit_to_registry` → registered scalar callable
//! through the standard registry path.

#![cfg(feature = "rhai-plugins")]

use std::sync::Arc;

use arrow_array::{Array, Float64Array};
use datafusion::logical_expr::ColumnarValue;
use uni_plugin::{Capability, CapabilitySet, QName};

#[tokio::test]
async fn load_rhai_plugin_registers_scalar_through_uni_api() {
    let db = uni_db::Uni::in_memory().build().await.expect("open");

    let script = r#"
        fn uni_manifest() {
            #{
                id: "ai.example.score",
                version: "0.1.0",
                determinism: "pure",
                scalar_fns: [
                    #{ name: "score", args: ["float","float"], returns: "float" },
                ],
            }
        }
        fn score(x, y) { x * 0.7 + y * 0.3 }
    "#;

    let loader = uni_plugin_rhai::RhaiLoader::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let outcome = db
        .load_rhai_plugin(&loader, script, &caps)
        .expect("load_rhai_plugin succeeds");

    assert_eq!(outcome.plugin_id.as_str(), "ai.example.score");
    assert_eq!(outcome.scalars_registered.len(), 1);

    // Confirm the scalar made it into Uni's plugin registry.
    let qn = QName::new("ai.example.score", "score");
    let entry = db
        .plugin_registry()
        .scalar_fn(&qn)
        .expect("scalar registered in Uni registry");

    let xs = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
    let ys = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0]));
    let args = vec![ColumnarValue::Array(xs), ColumnarValue::Array(ys)];
    let out = entry.function.invoke(&args, 3).expect("invokes");
    let arr = match out {
        ColumnarValue::Array(a) => a,
        other => panic!("expected Array, got {other:?}"),
    };
    let out = arr
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("f64 output");
    assert!((out.value(0) - 3.7).abs() < 1e-9);
    assert!((out.value(1) - 7.4).abs() < 1e-9);
    assert!((out.value(2) - 11.1).abs() < 1e-9);
}

#[tokio::test]
async fn load_rhai_plugin_rejects_bad_script() {
    let db = uni_db::Uni::in_memory().build().await.expect("open");
    let loader = uni_plugin_rhai::RhaiLoader::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let err = db
        .load_rhai_plugin(&loader, "@@@ this is not Rhai @@@", &caps)
        .expect_err("malformed Rhai source must fail");
    let msg = format!("{err}");
    assert!(msg.contains("rhai"), "error should mention rhai: {msg}");
}
