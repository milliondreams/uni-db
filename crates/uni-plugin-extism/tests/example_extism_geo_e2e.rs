//! End-to-end test against the real built `example_extism_geo.wasm`.
//!
//! This test exercises the full Cypher-to-wasm roundtrip:
//!
//!   1. Load the prebuilt wasm artifact.
//!   2. Drive `ExtismLoader::load` (which calls `manifest` +
//!      `register` exports).
//!   3. Look up the registered `geo.haversine` adapter through
//!      `PluginRegistry::scalar_fn`.
//!   4. Invoke with four `ColumnarValue::Scalar(Float64)` args —
//!      this exercises the Arrow IPC encode → wasm → Arrow IPC
//!      decode roundtrip inside the adapter.
//!   5. Verify the returned `ColumnarValue::Array` carries one f64
//!      ≈ 343.557 km (Paris→London great-circle distance).
//!
//! Hard-fail behavior: if the artifact is missing, the test panics
//! with a message pointing at `scripts/build-wasm-fixtures.sh`. The
//! M6 plan calls for hard-fail here so a fresh checkout that hasn't
//! run the build script can't quietly mark this test as skipped.

// Rust guideline compliant

use arrow_array::Float64Array;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry, QName};
use uni_plugin_extism::ExtismLoader;

const WASM_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/example-extism-geo/target/wasm32-unknown-unknown/release/example_extism_geo.wasm",
);

fn load_wasm_bytes() -> Vec<u8> {
    std::fs::read(WASM_PATH).unwrap_or_else(|e| {
        panic!(
            "wasm artifact missing at {WASM_PATH}: {e}\n\
             Run `./scripts/build-wasm-fixtures.sh` from the repo root first."
        );
    })
}

#[test]
fn loads_and_invokes_geo_haversine_end_to_end() {
    let bytes = load_wasm_bytes();

    let loader = ExtismLoader::new();
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("extism.test"), &caps, &registry);
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

    // Paris (48.8566° N, 2.3522° E) → London (51.5074° N, -0.1278° E)
    let args = vec![
        ColumnarValue::Scalar(ScalarValue::Float64(Some(48.8566))),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(2.3522))),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(51.5074))),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(-0.1278))),
    ];
    let out = entry.function.invoke(&args, 1).expect("invoke");
    let arr = match out {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => panic!("expected array, got scalar: {s:?}"),
    };
    let f64s = arr
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64Array");
    assert_eq!(f64s.len(), 1, "expected 1 row, got {}", f64s.len());
    let v = f64s.value(0);
    assert!(
        (v - 343.557).abs() < 0.01,
        "expected ~343.557 km, got {v} km"
    );
}

#[test]
fn invokes_handle_multi_row_batches() {
    let bytes = load_wasm_bytes();

    let loader = ExtismLoader::new();
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("extism.batch"), &caps, &registry);
    loader.load(&bytes, &[], &mut r).expect("load");
    r.commit_to_registry().expect("commit");

    let qname = QName::parse("ai.example.geo.haversine").expect("valid qname");
    let entry = registry.scalar_fn(&qname).expect("registered");

    // 3 rows:
    //   row 0: Paris → London   ≈ 343.557 km
    //   row 1: (0,0)  → (0,0)   = 0
    //   row 2: (0,0)  → (0,180) ≈ π·R = 20015.087 km
    let lat1: Float64Array = vec![48.8566, 0.0, 0.0].into();
    let lon1: Float64Array = vec![2.3522, 0.0, 0.0].into();
    let lat2: Float64Array = vec![51.5074, 0.0, 0.0].into();
    let lon2: Float64Array = vec![-0.1278, 0.0, 180.0].into();
    let args = vec![
        ColumnarValue::Array(std::sync::Arc::new(lat1)),
        ColumnarValue::Array(std::sync::Arc::new(lon1)),
        ColumnarValue::Array(std::sync::Arc::new(lat2)),
        ColumnarValue::Array(std::sync::Arc::new(lon2)),
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
