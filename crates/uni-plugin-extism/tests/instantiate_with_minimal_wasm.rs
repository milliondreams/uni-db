//! Integration tests for `ExtismLoader::build_plugin` against minimal
//! hand-assembled WASM modules.
//!
//! M6a.1.1 acceptance test: prove the SDK plumbing reaches wasmtime
//! (real instantiate, real cap-filtered host-fn wiring) without
//! requiring a built example plugin yet — that lands in M6a.1.7. We
//! use `wat::parse_str` to compile minimal WAT to bytes so the test
//! has no external fixture.

use uni_plugin::{Capability, CapabilitySet};
use uni_plugin_extism::{ExtismLoader, error::ExtismError, host_fns::HostFnSpec};

const MIN_MANIFEST_NO_CAPS: &str = r#"{"id":"ai.example.minwasm","version":"0.0.1"}"#;

/// Hand-assembled minimal WASM: a module that exports a single i32
/// constant via a function. Valid wasm — wasmtime will compile and
/// instantiate it cleanly. Extism will not see the usual PDK exports,
/// but the build_plugin path succeeds because Extism only inspects
/// exports lazily at call time.
const TRIVIAL_WAT: &str = r#"
(module
  (func (export "answer") (result i32)
    i32.const 42))
"#;

fn trivial_wasm() -> Vec<u8> {
    wat::parse_str(TRIVIAL_WAT).expect("WAT must parse")
}

#[test]
fn instantiate_succeeds_on_valid_wasm_with_no_caps() {
    let loader = ExtismLoader::new();
    let prepared = loader
        .prepare(MIN_MANIFEST_NO_CAPS.as_bytes(), &CapabilitySet::new())
        .expect("manifest prepares");

    let plugin = loader
        .build_plugin(&trivial_wasm(), &prepared)
        .expect("trivial wasm instantiates");

    // The trivial module exports `answer`, not the Extism PDK's
    // `_start`/`hs_init` — but `function_exists` reports literal
    // wasm exports, so we can sanity-check the linker accepted us.
    assert!(plugin.function_exists("answer"));
}

#[test]
fn instantiate_fails_on_invalid_wasm() {
    let loader = ExtismLoader::new();
    let prepared = loader
        .prepare(MIN_MANIFEST_NO_CAPS.as_bytes(), &CapabilitySet::new())
        .expect("manifest prepares");

    let err = loader
        .build_plugin(b"obviously not wasm", &prepared)
        .expect_err("garbage bytes must fail");
    assert!(
        matches!(err, ExtismError::Instantiate(_)),
        "expected Instantiate(_), got: {err:?}"
    );
}

#[test]
fn instantiate_filters_host_fns_through_effective_capabilities() {
    // Register two host fns: one gated by Filesystem, one always-available.
    // Grant only the always-available one's path (omit Filesystem entirely).
    // After build_plugin, the wasmtime store should have one fn registered
    // — proving the capability filter actually slices the import set.
    let mut loader = ExtismLoader::new();

    let always_fn = extism::Function::new(
        "host_log",
        [],
        [],
        extism::UserData::<()>::default(),
        |_plugin, _inp, _outp, _ud| Ok(()),
    );
    let fs_fn = extism::Function::new(
        "host_fs_read",
        [],
        [],
        extism::UserData::<()>::default(),
        |_plugin, _inp, _outp, _ud| Ok(()),
    );

    loader.register_host_function(
        HostFnSpec {
            name: "host_log".to_owned(),
            required_capability: None,
            docs: "always-available".to_owned(),
        },
        always_fn,
    );
    loader.register_host_function(
        HostFnSpec {
            name: "host_fs_read".to_owned(),
            required_capability: Some(Capability::Filesystem {
                read: vec![],
                write: vec![],
            }),
            docs: "Filesystem-gated".to_owned(),
        },
        fs_fn,
    );
    assert_eq!(loader.runtime_fn_count(), 2);

    // Plugin declares Filesystem but host doesn't grant it.
    let manifest = r#"{"id":"a.b","version":"0.0.1","capabilities":["filesystem"]}"#;
    let prepared = loader
        .prepare(manifest.as_bytes(), &CapabilitySet::new())
        .unwrap();

    // Only host_log survives the filter — Filesystem wasn't granted.
    assert_eq!(prepared.allowed_host_fns, vec!["host_log".to_owned()]);
    assert_eq!(prepared.denied_capabilities.len(), 1);
    assert!(prepared.denied_capabilities[0].contains("Filesystem"));

    // build_plugin must succeed — it sees one allowed fn and the rest
    // are absent from the import table (Extism analogue of linker absence).
    let _plugin = loader
        .build_plugin(&trivial_wasm(), &prepared)
        .expect("instantiation succeeds with cap-filtered host fns");
}

#[test]
fn instantiate_honors_manifest_resource_limits() {
    // Manifest declares a tight fuel + memory cap. The build must
    // succeed (the trivial module needs ~no resources) — this just
    // proves the resource-limit code paths run without panicking.
    let loader = ExtismLoader::new();
    let manifest = r#"{
        "id": "a.b",
        "version": "0.0.1",
        "fuel_per_call": 1000000,
        "memory_max_pages": 4,
        "timeout_ms": 5000
    }"#;
    let prepared = loader
        .prepare(manifest.as_bytes(), &CapabilitySet::new())
        .unwrap();
    let _plugin = loader
        .build_plugin(&trivial_wasm(), &prepared)
        .expect("instantiation honors resource limits");
}
