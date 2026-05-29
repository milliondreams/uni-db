//! End-to-end integration test for the Rhai loader's scalar fn path.
//!
//! Loads a tiny Rhai plugin, invokes its registered scalar fn against
//! constructed Arrow inputs, and asserts the output matches.

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use arrow_array::{Array, Float64Array};
use datafusion::logical_expr::ColumnarValue;

use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry, QName};
use uni_plugin_rhai::RhaiLoader;

#[test]
fn loads_and_invokes_scalar_fn_end_to_end() {
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

    let loader = RhaiLoader::new();
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("rhai.loading"), &caps, &registry);

    let outcome = loader.load(script, &mut r, &caps).expect("load succeeds");
    assert_eq!(outcome.scalars_registered.len(), 1);
    r.commit_to_registry().expect("commits");

    // Look the fn up by qname and invoke against a 3-row batch.
    let qname = QName::new("ai.example.score", "score");
    let entry = registry.scalar_fn(&qname).expect("scalar registered");

    let xs = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
    let ys = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0]));
    let args = vec![ColumnarValue::Array(xs), ColumnarValue::Array(ys)];

    let out = entry.function.invoke(&args, 3).expect("invokes");
    let arr = match out {
        ColumnarValue::Array(a) => a,
        other => panic!("expected Array, got {other:?}"),
    };
    let out = arr.as_any().downcast_ref::<Float64Array>().expect("f64");

    // 1*0.7 + 10*0.3 = 3.7, 2*0.7 + 20*0.3 = 7.4, 3*0.7 + 30*0.3 = 11.1
    assert!((out.value(0) - 3.7).abs() < 1e-9);
    assert!((out.value(1) - 7.4).abs() < 1e-9);
    assert!((out.value(2) - 11.1).abs() < 1e-9);
}

#[test]
fn loads_and_invokes_haversine_geo_plugin() {
    // Compact version of the proposal §15.3 example.
    let script = r#"
        fn uni_manifest() {
            #{
                id: "ai.example.geo",
                version: "0.3.1",
                determinism: "pure",
                scalar_fns: [
                    #{ name: "haversine",
                       args: ["float","float","float","float"],
                       returns: "float" },
                ],
            }
        }

        const R = 6371.0;

        fn haversine(lat1, lon1, lat2, lon2) {
            let rlat1 = lat1.to_radians();
            let rlat2 = lat2.to_radians();
            let dlat = (lat2 - lat1).to_radians();
            let dlon = (lon2 - lon1).to_radians();
            let a = (dlat / 2.0).sin() ** 2
                  + rlat1.cos() * rlat2.cos() * (dlon / 2.0).sin() ** 2;
            // c = 2 * asin(sqrt(a)) — equivalent form that avoids atan2,
            // which is not in Rhai's BasicMathPackage by default.
            let c = 2.0 * a.sqrt().asin();
            global::R * c
        }
    "#;

    let loader = RhaiLoader::new();
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("rhai.loading"), &caps, &registry);
    loader.load(script, &mut r, &caps).expect("load");
    r.commit_to_registry().expect("commits");

    let qname = QName::new("ai.example.geo", "haversine");
    let entry = registry.scalar_fn(&qname).expect("scalar registered");

    // NYC (40.7128, -74.0060) -> SF (37.7749, -122.4194); expected ~4129 km.
    let lat1 = Arc::new(Float64Array::from(vec![40.7128]));
    let lon1 = Arc::new(Float64Array::from(vec![-74.0060]));
    let lat2 = Arc::new(Float64Array::from(vec![37.7749]));
    let lon2 = Arc::new(Float64Array::from(vec![-122.4194]));
    let args = vec![
        ColumnarValue::Array(lat1),
        ColumnarValue::Array(lon1),
        ColumnarValue::Array(lat2),
        ColumnarValue::Array(lon2),
    ];

    let out = entry.function.invoke(&args, 1).expect("invokes");
    let arr = match out {
        ColumnarValue::Array(a) => a,
        other => panic!("expected Array, got {other:?}"),
    };
    let out = arr.as_any().downcast_ref::<Float64Array>().expect("f64");
    let km = out.value(0);
    // ULP-loose check against the canonical great-circle distance.
    assert!(
        (km - 4129.0).abs() < 5.0,
        "expected ~4129 km between NYC and SF, got {km}"
    );
}
