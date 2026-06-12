//! Per-invoke freshness repros for the Extism loader.
//!
//! Extism has no in-place linear-memory reset, so per-invoke isolation
//! is achieved by building a *fresh* `extism::Plugin` per acquire (the
//! instance cache never reuses a live plugin). These tests pin that with
//! the real built `example_extism_stateful.wasm` fixture (a guest with a
//! module-level mutable `COUNTER`):
//!
//! - **State freshness** — two `bump` invocations through the adapter
//!   must each observe a fresh guest state (counter == 1 both times). A
//!   reused plugin would leak the counter and the second call would
//!   return 2.
//! - **Trap cleanliness** — invoking `boom` traps the guest; the next
//!   `bump` must succeed against a fresh plugin and return 1. A recycled
//!   trapped plugin would re-trap or leak state.

// Rust guideline compliant

use arrow_array::Float64Array;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry, QName};
use uni_plugin_extism::ExtismLoader;

const WASM_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/example-extism-stateful/target/wasm32-unknown-unknown/release/example_extism_stateful.wasm",
);

/// Read the prebuilt extism fixture, or skip if absent.
fn load_bytes() -> Option<Vec<u8>> {
    match std::fs::read(WASM_PATH) {
        Ok(b) => Some(b),
        Err(e) => {
            eprintln!(
                "skipping extism pooled_instance_isolation: fixture missing at {WASM_PATH}: {e}\n\
                 build it with `./scripts/build-wasm-fixtures.sh`"
            );
            None
        }
    }
}

/// Load the stateful extism plugin and return a registry with its
/// adapters. Default `PoolConfig` (`max_instances = 4`).
fn load_stateful() -> Option<PluginRegistry> {
    let bytes = load_bytes()?;
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("extism.stateful"), &caps, &registry);
    ExtismLoader::new()
        .load(&bytes, &CapabilitySet::new(), &mut r)
        .expect("load stateful extism plugin");
    r.commit_to_registry().expect("commit");
    Some(registry)
}

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

/// Each invoke gets a fresh `extism::Plugin` → fresh counter.
#[test]
fn bump_is_fresh_each_invocation() {
    let Some(registry) = load_stateful() else {
        return;
    };

    let first = invoke_f64(&registry, "ai.example.stateful.bump").expect("first bump");
    assert_eq!(first, 1.0, "first bump must see a fresh counter");

    let second = invoke_f64(&registry, "ai.example.stateful.bump").expect("second bump");
    assert_eq!(
        second, 1.0,
        "second bump must ALSO see a fresh counter (got {second}); \
         a value of 2.0 means the extism plugin instance was reused across invocations"
    );
}

/// A trapped invocation must not poison the next one.
#[test]
fn trap_then_clean_invocation() {
    let Some(registry) = load_stateful() else {
        return;
    };

    let boom = invoke_f64(&registry, "ai.example.stateful.boom");
    assert!(boom.is_err(), "boom must trap and surface an error");

    let after = invoke_f64(&registry, "ai.example.stateful.bump")
        .expect("bump after a trapped call must succeed against a fresh plugin");
    assert_eq!(
        after, 1.0,
        "bump after boom must see a fresh counter (got {after}); \
         a recycled trapped plugin would re-trap or leak state"
    );
}
