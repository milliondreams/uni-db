//! End-to-end test against the real built `example_wasm_geo.wasm`.
//!
//! Mirrors `uni-plugin-extism`'s `example_extism_geo_e2e.rs`. The
//! plugin is built for `wasm32-wasip2`, which under the current
//! Rust toolchain produces a Component Model binary directly (no
//! `wasm-tools component new` post-processing required — verified
//! by inspecting the magic: `\0asm 0d 00 01 00` indicates a CM
//! component, not a plain module).

// Rust guideline compliant

use std::sync::Arc;

use arrow_array::Float64Array;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry, QName};
use uni_plugin_wasm::WasmLoader;

const WASM_MODULE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/example-wasm-geo/target/wasm32-wasip2/release/example_wasm_geo.wasm",
);

/// Read the prebuilt wasm32-wasip2 Component Model binary.
fn load_component_bytes() -> Vec<u8> {
    std::fs::read(WASM_MODULE_PATH).unwrap_or_else(|e| {
        panic!(
            "wasm component missing at {WASM_MODULE_PATH}: {e}\n\
             Run `./scripts/build-wasm-fixtures.sh` from the repo root first."
        );
    })
}

#[test]
fn loads_and_invokes_geo_haversine_end_to_end() {
    let bytes = load_component_bytes();

    let loader = WasmLoader::new();
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("wasm.test"), &caps, &registry);
    let outcome = loader.load(&bytes, &[], &mut r).expect("load");
    r.commit_to_registry().expect("commit");

    assert_eq!(outcome.plugin_id, "ai.example.geo");
    assert_eq!(outcome.version, "0.1.0");
    assert!(
        outcome
            .scalars_registered
            .iter()
            .any(|q| q == "ai.example.geo.haversine"),
        "scalars_registered: {:?}",
        outcome.scalars_registered
    );

    let qname = QName::parse("ai.example.geo.haversine").expect("valid qname");
    let entry = registry.scalar_fn(&qname).expect("registered");

    // Paris → London
    let args = vec![
        ColumnarValue::Scalar(ScalarValue::Float64(Some(48.8566))),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(2.3522))),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(51.5074))),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(-0.1278))),
    ];
    let out = entry.function.invoke(&args, 1).expect("invoke");
    let arr = match out {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => panic!("expected array, got {s:?}"),
    };
    let f64s = arr
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64Array");
    assert_eq!(f64s.len(), 1);
    let v = f64s.value(0);
    assert!(
        (v - 343.557).abs() < 0.01,
        "expected ~343.557 km, got {v} km"
    );
}

#[test]
fn invokes_handle_multi_row_batches() {
    let bytes = load_component_bytes();

    let loader = WasmLoader::new();
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("wasm.batch"), &caps, &registry);
    loader.load(&bytes, &[], &mut r).expect("load");
    r.commit_to_registry().expect("commit");

    let qname = QName::parse("ai.example.geo.haversine").expect("valid qname");
    let entry = registry.scalar_fn(&qname).expect("registered");

    let lat1: Float64Array = vec![48.8566, 0.0, 0.0].into();
    let lon1: Float64Array = vec![2.3522, 0.0, 0.0].into();
    let lat2: Float64Array = vec![51.5074, 0.0, 0.0].into();
    let lon2: Float64Array = vec![-0.1278, 0.0, 180.0].into();
    let args = vec![
        ColumnarValue::Array(Arc::new(lat1)),
        ColumnarValue::Array(Arc::new(lon1)),
        ColumnarValue::Array(Arc::new(lat2)),
        ColumnarValue::Array(Arc::new(lon2)),
    ];
    let out = entry.function.invoke(&args, 3).expect("invoke");
    let arr = match out {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(_) => panic!("expected array"),
    };
    let f = arr
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64Array");
    assert_eq!(f.len(), 3);
    assert!((f.value(0) - 343.557).abs() < 0.01, "row 0: {}", f.value(0));
    assert!(f.value(1).abs() < f64::EPSILON, "row 1: {}", f.value(1));
    let antipode = std::f64::consts::PI * 6371.0;
    assert!(
        (f.value(2) - antipode).abs() < 1.0,
        "row 2: {} (expected ~{antipode})",
        f.value(2)
    );
}
