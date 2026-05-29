//! End-to-end integration tests for `ExtismLoader::load`.
//!
//! M6a.1.5/.1.6/.1.7 acceptance: prove the two-pass load dance + pool
//! construction + registrar integration paths work against real
//! (hand-assembled) wasm artifacts. A separately-built `extism-pdk`
//! Rust plugin would prove the full Cypher-call roundtrip; that fixture
//! lives in `examples/example-extism-geo/` (deferred build).

use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry};
use uni_plugin_extism::{ExtismLoader, error::ExtismError};

/// Minimal valid WASM that has no Extism-protocol exports. Used to
/// prove `load()` correctly fails with `InvalidPlugin` when the
/// `manifest` export is missing.
const TRIVIAL_WAT: &str = r#"
(module
  (func (export "answer") (result i32)
    i32.const 42))
"#;

fn trivial_wasm() -> Vec<u8> {
    wat::parse_str(TRIVIAL_WAT).expect("WAT must parse")
}

fn make_caps() -> CapabilitySet {
    CapabilitySet::from_iter_of([
        Capability::ScalarFn,
        Capability::AggregateFn,
        Capability::Procedure,
    ])
}

#[test]
fn load_rejects_plugin_without_manifest_export() {
    let loader = ExtismLoader::new();
    let registry = PluginRegistry::new();
    let caps = make_caps();
    let mut registrar = PluginRegistrar::new(PluginId::new("extism.test"), &caps, &registry);

    let err = loader
        .load(&trivial_wasm(), &[], &mut registrar)
        .expect_err("trivial plugin has no `manifest` export — load must fail");

    assert!(
        matches!(err, ExtismError::InvalidPlugin(_)),
        "expected InvalidPlugin, got: {err:?}"
    );
}

#[test]
fn load_rejects_garbage_bytes_with_instantiate_error() {
    let loader = ExtismLoader::new();
    let registry = PluginRegistry::new();
    let caps = make_caps();
    let mut registrar = PluginRegistrar::new(PluginId::new("extism.test"), &caps, &registry);

    let err = loader
        .load(b"obviously not wasm", &[], &mut registrar)
        .expect_err("garbage bytes can't compile — load must fail");
    assert!(
        matches!(err, ExtismError::Instantiate(_)),
        "expected Instantiate, got: {err:?}"
    );
}
