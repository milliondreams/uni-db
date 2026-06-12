//! Resource-limit proof tests — FuelPerCall trips on runaway loops;
//! the script terminates with the expected `FnError` code.

#![cfg(feature = "rhai-runtime")]

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

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
            while i < 100000000 {
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
        // Budget above the DEFAULT_MAX_OPERATIONS floor so this exercises the
        // granted limit, yet well below the 100M iterations the loop attempts.
        Capability::FuelPerCall(12_000_000),
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

#[test]
fn default_op_limit_trips_unbounded_loop_without_fuel() {
    // A plugin granted ONLY `ScalarFn` (no `FuelPerCall`) running `while true
    // {}` must still be terminated by the always-applied operation-limit
    // floor — otherwise it wedges the synchronous worker thread forever.
    let script = r#"
        fn uni_manifest() {
            #{ id: "ai.test.nofuel.spin", version: "0.1.0",
               scalar_fns: [#{ name: "spin", args: [], returns: "int" }] }
        }
        fn spin() {
            let i = 0;
            while true {
                i += 1;
            }
            i
        }
    "#;
    let registry = PluginRegistry::new();
    // ScalarFn ONLY — deliberately no FuelPerCall grant.
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("rhai.loading"), &caps, &registry);
    RhaiLoader::new()
        .load(script, &mut r, &caps)
        .expect("loads");
    r.commit_to_registry().expect("commits");

    let qn = QName::new("ai.test.nofuel.spin", "spin");
    let entry = registry.scalar_fn(&qn).expect("scalar registered");

    // Run the invoke on a watchdog thread: if the fix is missing the loop
    // never returns, so we must not block the test harness forever. The
    // entry is `Send`, so it can move across the thread boundary.
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = entry.function.invoke(&[], 1);
        // Ignore send errors — the receiver may already have timed out.
        let _ = tx.send(result);
    });

    let result = match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(result) => result,
        Err(_) => panic!("Rhai op-limit did not fire (hang)"),
    };
    assert!(
        result.is_err(),
        "the default op-limit floor must trip `while true {{}}` without any fuel grant"
    );
    let err = result.unwrap_err();
    // Adapter classifies ErrorTooManyOperations as 0x711.
    assert_eq!(err.code, 0x711, "expected too-many-ops code, got {err:?}");
}

#[test]
fn bounded_loop_without_fuel_succeeds() {
    // The floor must not break legitimate plugins: a small bounded loop with
    // no fuel grant stays well under the floor and returns its value.
    let script = r#"
        fn uni_manifest() {
            #{ id: "ai.test.nofuel.bounded", version: "0.1.0",
               scalar_fns: [#{ name: "count", args: [], returns: "int" }] }
        }
        fn count() {
            let i = 0;
            while i < 1000 {
                i += 1;
            }
            i
        }
    "#;
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("rhai.loading"), &caps, &registry);
    RhaiLoader::new()
        .load(script, &mut r, &caps)
        .expect("loads");
    r.commit_to_registry().expect("commits");

    let qn = QName::new("ai.test.nofuel.bounded", "count");
    let entry = registry.scalar_fn(&qn).expect("scalar registered");

    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = entry.function.invoke(&[], 1);
        let _ = tx.send(result);
    });

    let result = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("bounded loop should return promptly under the op-limit floor");
    let value = result.expect("a bounded loop without fuel must succeed under the floor");

    use arrow_array::Int64Array;
    use datafusion::logical_expr::ColumnarValue;
    let scalar = match value {
        ColumnarValue::Array(arr) => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 result")
            .value(0),
        ColumnarValue::Scalar(s) => match s {
            datafusion::scalar::ScalarValue::Int64(Some(v)) => v,
            other => panic!("unexpected scalar return: {other:?}"),
        },
    };
    assert_eq!(scalar, 1000, "bounded loop should count to 1000");
}
