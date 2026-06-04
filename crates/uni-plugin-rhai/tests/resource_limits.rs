//! Resource-limit proof tests — FuelPerCall trips on runaway loops;
//! the script terminates with the expected `FnError` code.

#![cfg(feature = "rhai-runtime")]

use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry, QName};
use uni_plugin_rhai::RhaiLoader;

#[test]
fn fuel_limit_trips_on_runaway_loop() {
    let script = r#"
        fn uni_manifest() {
            #{ id: "ai.test.fuel", version: "0.1.0",
               scalar_fns: [#{ name: "spin", args: [], returns: "int" }] }
        }
        fn spin() {
            let i = 0;
            while i < 1000000 {
                i += 1;
            }
            i
        }
    "#;
    let registry = PluginRegistry::new();
    // FuelPerCall is enforced by the engine factory; the registrar gate
    // doesn't restrict it because it isn't an extension-surface cap.
    let caps = CapabilitySet::from_iter_of([
        Capability::ScalarFn,
        // Tight budget — well below the 1M iterations the loop attempts.
        Capability::FuelPerCall(100_000),
    ]);
    let mut r = PluginRegistrar::new(PluginId::new("rhai.loading"), &caps, &registry);
    RhaiLoader::new()
        .load(script, &mut r, &caps)
        .expect("loads");
    r.commit_to_registry().expect("commits");

    let qn = QName::new("ai.test.fuel", "spin");
    let entry = registry.scalar_fn(&qn).expect("scalar registered");
    let result = entry.function.invoke(&[], 1);
    assert!(
        result.is_err(),
        "FuelPerCall(5000) must trip on 1M-iter loop"
    );
    let err = result.unwrap_err();
    // Adapter classifies ErrorTooManyOperations as 0x711.
    assert_eq!(err.code, 0x711, "expected too-many-ops code, got {err:?}");
}

#[test]
fn call_depth_limit_trips_on_deep_recursion() {
    let script = r#"
        fn uni_manifest() {
            #{ id: "ai.test.depth", version: "0.1.0",
               scalar_fns: [#{ name: "rec", args: ["int"], returns: "int" }] }
        }
        fn rec(n) {
            if n <= 0 { return 0; }
            rec(n - 1) + 1
        }
    "#;
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("rhai.loading"), &caps, &registry);
    RhaiLoader::new()
        .load(script, &mut r, &caps)
        .expect("loads");
    r.commit_to_registry().expect("commits");

    use arrow_array::Int64Array;
    use datafusion::logical_expr::ColumnarValue;
    use std::sync::Arc;

    let qn = QName::new("ai.test.depth", "rec");
    let entry = registry.scalar_fn(&qn).expect("scalar registered");
    // Default DEFAULT_MAX_CALL_LEVELS is 64. Calling rec(200) should
    // exhaust the depth.
    let n = Arc::new(Int64Array::from(vec![200_i64]));
    let result = entry.function.invoke(&[ColumnarValue::Array(n)], 1);
    assert!(
        result.is_err(),
        "200-deep recursion must hit DEFAULT_MAX_CALL_LEVELS=64"
    );
}
