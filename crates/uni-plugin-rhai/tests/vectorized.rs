//! Vectorized scalar fn — column userdata path.

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use arrow_array::{Array, Float64Array};
use datafusion::logical_expr::ColumnarValue;
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry, QName};
use uni_plugin_rhai::RhaiLoader;

#[test]
fn vectorized_scalar_fn_round_trips() {
    let script = r#"
        fn uni_manifest() {
            #{
                id: "ai.test.vec",
                version: "0.1.0",
                scalar_fns: [
                    #{ name: "score_v", vectorized: true,
                       args: ["float","float"], returns: "float" },
                ],
            }
        }
        fn score_v(xs, ys) {
            let n = xs.len();
            let out = uni_float_column(n);
            for i in 0..n {
                out[i] = xs[i] * 0.7 + ys[i] * 0.3;
            }
            out
        }
    "#;

    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("rhai.loading"), &caps, &registry);
    let outcome = RhaiLoader::new()
        .load(script, &mut r, &caps)
        .expect("loads");
    assert_eq!(outcome.scalars_registered.len(), 1);
    r.commit_to_registry().expect("commits");

    let qn = QName::new("ai.test.vec", "score_v");
    let entry = registry.scalar_fn(&qn).expect("registered");

    let xs = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
    let ys = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0]));
    let args = vec![ColumnarValue::Array(xs), ColumnarValue::Array(ys)];
    let out = entry.function.invoke(&args, 3).expect("invokes");
    let arr = match out {
        ColumnarValue::Array(a) => a,
        other => panic!("expected Array, got {other:?}"),
    };
    let f = arr.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((f.value(0) - 3.7).abs() < 1e-9);
    assert!((f.value(1) - 7.4).abs() < 1e-9);
    assert!((f.value(2) - 11.1).abs() < 1e-9);
}

#[test]
fn vectorized_matches_row_mode_for_haversine() {
    // Run both row-mode and vectorized haversine; compare cells.
    let script = r#"
        fn uni_manifest() {
            #{
                id: "ai.test.hv",
                version: "0.1.0",
                scalar_fns: [
                    #{ name: "h_row", args: ["float","float","float","float"], returns: "float" },
                    #{ name: "h_vec", vectorized: true,
                       args: ["float","float","float","float"], returns: "float" },
                ],
            }
        }
        const R = 6371.0;
        fn hav(lat1, lon1, lat2, lon2) {
            let rlat1 = lat1.to_radians();
            let rlat2 = lat2.to_radians();
            let dlat = (lat2 - lat1).to_radians();
            let dlon = (lon2 - lon1).to_radians();
            let a = (dlat / 2.0).sin() ** 2
                  + rlat1.cos() * rlat2.cos() * (dlon / 2.0).sin() ** 2;
            global::R * 2.0 * a.sqrt().asin()
        }
        fn h_row(lat1, lon1, lat2, lon2) { hav(lat1, lon1, lat2, lon2) }
        fn h_vec(la1, lo1, la2, lo2) {
            let n = la1.len();
            let out = uni_float_column(n);
            for i in 0..n {
                out[i] = hav(la1[i], lo1[i], la2[i], lo2[i]);
            }
            out
        }
    "#;

    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("rhai.loading"), &caps, &registry);
    RhaiLoader::new()
        .load(script, &mut r, &caps)
        .expect("loads");
    r.commit_to_registry().expect("commits");

    let lat1 = Arc::new(Float64Array::from(vec![40.7128, 51.5074]));
    let lon1 = Arc::new(Float64Array::from(vec![-74.0060, -0.1278]));
    let lat2 = Arc::new(Float64Array::from(vec![37.7749, 48.8566]));
    let lon2 = Arc::new(Float64Array::from(vec![-122.4194, 2.3522]));
    let args = vec![
        ColumnarValue::Array(lat1.clone()),
        ColumnarValue::Array(lon1.clone()),
        ColumnarValue::Array(lat2.clone()),
        ColumnarValue::Array(lon2.clone()),
    ];

    let row_out = match registry
        .scalar_fn(&QName::new("ai.test.hv", "h_row"))
        .unwrap()
        .function
        .invoke(&args, 2)
        .unwrap()
    {
        ColumnarValue::Array(a) => a,
        _ => panic!(),
    };
    let vec_out = match registry
        .scalar_fn(&QName::new("ai.test.hv", "h_vec"))
        .unwrap()
        .function
        .invoke(&args, 2)
        .unwrap()
    {
        ColumnarValue::Array(a) => a,
        _ => panic!(),
    };
    let r_arr = row_out.as_any().downcast_ref::<Float64Array>().unwrap();
    let v_arr = vec_out.as_any().downcast_ref::<Float64Array>().unwrap();
    for i in 0..2 {
        assert!(
            (r_arr.value(i) - v_arr.value(i)).abs() < 1e-9,
            "row vs vec mismatch at {i}: row={} vec={}",
            r_arr.value(i),
            v_arr.value(i)
        );
    }
}
