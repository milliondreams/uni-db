// Rust guideline compliant
//! Unit-test the `install_aggregate_into_registry` synthesis path.
//!
//! Builds a `DeclaredPlugin` record by hand (the shape the
//! `DeclareAggregateProcedure` would persist), installs into a fresh
//! `PluginRegistry`, then looks the entry back up and drives its
//! accumulator end-to-end. No DataFusion involvement — proves the
//! synthesis path independently of the planner.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array};
use datafusion::scalar::ScalarValue;
use uni_plugin::{PluginRegistry, QName};
use uni_plugin_custom::DeclaredPlugin;

#[test]
fn install_aggregate_round_trip() {
    let registry = Arc::new(PluginRegistry::new());
    let record = DeclaredPlugin {
        qname: "mycorp.sumSquares".to_owned(),
        kind: "aggregate".to_owned(),
        body: "$state + ($x * $x)".to_owned(),
        signature_json: serde_json::json!({
            "init": "0",
            "update": "$state + ($x * $x)",
            "finalize": "$state",
            "return_type": "int",
            "arg_names": ["x"],
        })
        .to_string(),
        dependencies: vec![],
        declared_by: "alice".to_owned(),
        active: true,
    };

    // Call through the public re-export of the install helper.
    uni_plugin_custom::install_aggregate_into_registry(&registry, &record).expect("install ok");

    // Look up via the registry under the per-namespace plugin id.
    let qn = QName::new("mycorp", "sumsquares");
    let entry = registry.aggregate(&qn).expect("registered");

    let mut acc = entry.aggregate.create_accumulator();
    let col: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4]));
    acc.update_batch(&[col]).expect("update");
    let out = acc.evaluate().expect("evaluate");
    // 1 + 4 + 9 + 16 = 30
    assert_eq!(out, ScalarValue::Int64(Some(30)));
}

#[test]
fn install_aggregate_propagates_signature_errors() {
    let registry = Arc::new(PluginRegistry::new());
    let record = DeclaredPlugin {
        qname: "broken.agg".to_owned(),
        kind: "aggregate".to_owned(),
        body: String::new(),
        // Missing the `update` key — `install_aggregate_into_registry`
        // should surface a CustomError::BodyParse.
        signature_json: serde_json::json!({
            "init": "0",
            "finalize": "$state",
            "return_type": "int",
            "arg_names": ["x"],
        })
        .to_string(),
        dependencies: vec![],
        declared_by: "alice".to_owned(),
        active: true,
    };
    let err = uni_plugin_custom::install_aggregate_into_registry(&registry, &record)
        .expect_err("missing `update` field should fail");
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("update"),
        "expected `update` in error: {msg}"
    );
}
