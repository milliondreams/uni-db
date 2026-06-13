//! Per-invoke `Store`/instance isolation repros for the CM loader.
//!
//! Pins two security properties of the WASM scalar invoke path, both
//! driven by the real built `example_wasm_stateful.wasm` fixture (a
//! component carrying a module-level mutable `COUNTER`):
//!
//! - **State isolation (#2)** — two `bump` invocations through the
//!   adapter must each observe a *fresh* guest state. Before the
//!   re-instantiate-per-invoke refactor the loader reused one persistent
//!   `Store<HostState>`, so the second call returned `2.0` (the leaked
//!   counter). With a fresh `Store`+instance per invoke, every call
//!   returns `1.0`.
//! - **Trap cleanliness (#3)** — invoking `boom` traps the guest; the
//!   *next* invocation of `bump` must succeed and observe a fresh state.
//!   Before the refactor the trapped store was released back to the warm
//!   pool (`Drop for PooledInstance` recycled it unconditionally), so the
//!   next call ran against a poisoned store. With per-invoke instances,
//!   the trapped store is simply dropped.
//!
//! `PoolConfig { max_instances: 1, warm_count: 1 }` forces maximal reuse
//! so that — if any `Store` pooling survived — the leak would be
//! observable. After the refactor the cap is a concurrency semaphore and
//! these properties hold regardless.

// Rust guideline compliant

use arrow_array::Float64Array;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry, QName};
use uni_plugin_wasm::WasmLoader;

const WASM_MODULE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/example-wasm-stateful/target/wasm32-wasip2/release/example_wasm_stateful.wasm",
);

/// Read the prebuilt wasm32-wasip2 Component Model binary, or skip.
fn load_component_bytes() -> Option<Vec<u8>> {
    match std::fs::read(WASM_MODULE_PATH) {
        Ok(b) => Some(b),
        Err(e) => {
            eprintln!(
                "skipping pooled_instance_isolation: fixture missing at {WASM_MODULE_PATH}: {e}\n\
                 build it with `./scripts/build-wasm-fixtures.sh`"
            );
            None
        }
    }
}

/// Load the stateful component and return a registry holding its scalar
/// adapters. `max_instances = 1` forces the tightest reuse pressure.
fn load_stateful() -> Option<PluginRegistry> {
    let bytes = load_component_bytes()?;
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("wasm.stateful"), &caps, &registry);
    WasmLoader::new()
        .load(&bytes, &CapabilitySet::new(), &mut r)
        .expect("load stateful component");
    r.commit_to_registry().expect("commit");
    Some(registry)
}

/// Invoke a 1-arg scalar fn with a single dummy Float64 row and return
/// the f64 it yields.
fn invoke_f64(registry: &PluginRegistry, qname: &str) -> Result<f64, uni_plugin::errors::FnError> {
    let q = QName::parse(qname).expect("valid qname");
    let entry = registry.scalar_fn(&q).expect("registered");
    let args = vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0)))];
    let out = entry.function.invoke(&args, 1)?;
    let arr = match out {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => panic!("expected array, got {s:?}"),
    };
    let f = arr
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64Array");
    assert_eq!(f.len(), 1, "scalar fn must return one row");
    Ok(f.value(0))
}

/// #2 — each invoke gets a fresh guest state.
///
/// `bump` increments a module-level global. With per-invoke
/// re-instantiation the counter starts at zero every call, so both
/// invocations return `1.0`. A persistent pooled store would leak the
/// counter and the second call would return `2.0`.
#[test]
fn bump_is_isolated_across_invocations() {
    let Some(registry) = load_stateful() else {
        return;
    };

    let first = invoke_f64(&registry, "ai.example.stateful.bump").expect("first bump");
    assert_eq!(first, 1.0, "first bump must see a fresh counter");

    let second = invoke_f64(&registry, "ai.example.stateful.bump").expect("second bump");
    assert_eq!(
        second, 1.0,
        "second bump must ALSO see a fresh counter (got {second}); \
         a value of 2.0 means guest state leaked across invocations (bug #2)"
    );
}

/// #3 — a trapped invocation must not poison the next one.
///
/// `boom` mutates the global then traps the guest (`Err`). The
/// subsequent `bump` must succeed against a clean instance and return
/// `1.0`. Before the refactor the trapped store was recycled into the
/// warm pool, so this `bump` ran against a poisoned store (re-trap or
/// leaked counter).
#[test]
fn trap_then_clean_invocation() {
    let Some(registry) = load_stateful() else {
        return;
    };

    let boom = invoke_f64(&registry, "ai.example.stateful.boom");
    assert!(boom.is_err(), "boom must trap and surface an error");

    let after = invoke_f64(&registry, "ai.example.stateful.bump")
        .expect("bump after a trapped call must succeed against a clean instance");
    assert_eq!(
        after, 1.0,
        "bump after boom must see a fresh counter (got {after}); \
         a recycled trapped store would re-trap or leak state (bug #3)"
    );
}
