#![allow(dead_code, unused_imports, clippy::all)]
//! M8 — end-to-end test of `Uni::load_python_plugin`.
//!
//! Opens an in-memory `Uni`, loads a PyO3 plugin module whose body
//! uses `@db.scalar_fn(...)` decorators to register Python callables,
//! then invokes them directly through the plugin registry. Proves the
//! host-level integration: `Uni::load_python_plugin` → exec module →
//! decorator sink builder → `PluginRegistrar::commit_to_registry` →
//! registered scalar callable invokable via the standard registry
//! path.

#![cfg(feature = "pyo3-plugins")]

use std::sync::Arc;

use arrow_array::{Array, Float64Array};
use datafusion::logical_expr::ColumnarValue;
use pyo3::Python;
use uni_plugin::{Capability, CapabilitySet, QName};

#[tokio::test]
async fn load_python_plugin_registers_scalar_through_uni_api() {
    Python::initialize();

    let db = uni_db::Uni::in_memory().build().await.expect("open");

    let module_src = r#"
db.set_plugin_id("ai.example.pyscore")
db.set_version("0.1.0")

@db.scalar_fn("score", args=["float","float"], returns="float", determinism="pure")
def score(x, y):
    return x * 0.7 + y * 0.3
"#;

    let loader = uni_plugin_pyo3::PythonPluginLoader::with_default_plugin_id("ai.example.pyscore");
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);

    let outcome = Python::attach(|py| {
        db.load_python_plugin(py, &loader, module_src, "ai.example.pyscore", &caps)
            .expect("load_python_plugin succeeds")
    });

    assert_eq!(outcome.plugin_id.as_str(), "ai.example.pyscore");
    assert_eq!(outcome.version, "0.1.0");
    assert_eq!(outcome.scalars_registered.len(), 1);

    // Confirm the scalar landed in Uni's plugin registry.
    let qn = QName::new("ai.example.pyscore", "score");
    let entry = db
        .plugin_registry()
        .scalar_fn(&qn)
        .expect("scalar registered in Uni registry");

    let xs = Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0]));
    let ys = Arc::new(Float64Array::from(vec![10.0_f64, 20.0, 30.0]));
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
async fn load_python_plugin_rejects_bad_module() {
    Python::initialize();

    let db = uni_db::Uni::in_memory().build().await.expect("open");
    let module_src = "this is @@@ not valid python";
    let loader = uni_plugin_pyo3::PythonPluginLoader::with_default_plugin_id("ai.example.bad");
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);

    let err = Python::attach(|py| {
        db.load_python_plugin(py, &loader, module_src, "ai.example.bad", &caps)
            .unwrap_err()
    });
    let msg = format!("{err}");
    assert!(msg.to_lowercase().contains("python"), "got: {msg}");
}
