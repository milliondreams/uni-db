// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Conformance probe suite run against a real WASM component.
//!
//! Bridges `WasmLoader::load_as_plugin` to
//! [`uni_plugin_conformance::WasmConformanceLoader`] so the loaded component is
//! driven through the *same* probe suite as a live-Rust plugin
//! (`run_against_plugin`). Two tests:
//!
//! - `garbage_bytes_yield_failing_load_check` — always runs; exercises the
//!   `load_as_plugin` error path and `run_against_wasm`'s load-failure branch
//!   without needing any fixture.
//! - `geo_component_passes_conformance_suite` — runs the full suite against the
//!   prebuilt `example-wasm-geo` artifact, soft-skipping when it is absent so
//!   CI without the wasm32-wasip2 toolchain stays green. Build it with
//!   `./scripts/build-wasm-fixtures.sh`.

// Rust guideline compliant

use std::path::Path;

use uni_plugin_conformance::{WasmConformanceLoader, run_against_wasm};
use uni_plugin_wasm::WasmLoader;

const GEO_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/example-wasm-geo/target/wasm32-wasip2/release/example_wasm_geo.wasm",
);

/// Loads a component from a path via `WasmLoader::load_as_plugin`.
struct PathLoader;

impl WasmConformanceLoader for PathLoader {
    fn load(&self, path: &Path) -> Result<Box<dyn uni_plugin::Plugin + Send + Sync>, String> {
        let bytes = std::fs::read(path).map_err(|e| format!("read {path:?}: {e}"))?;
        // No host-surface grants needed: the geo plugin declares only scalar
        // functions, whose registration is gated by the synthesized manifest's
        // `Capability::ScalarFn`, not by host grants.
        WasmLoader::new()
            .load_as_plugin(&bytes, &[])
            .map_err(|e| format!("load_as_plugin: {e}"))
    }
}

/// Always-on: invalid bytes must surface as a single failing `wasm.load`
/// check (not a panic), proving the bridge's error path and
/// `run_against_wasm`'s failure branch.
#[test]
fn garbage_bytes_yield_failing_load_check() {
    struct GarbageLoader;
    impl WasmConformanceLoader for GarbageLoader {
        fn load(&self, _path: &Path) -> Result<Box<dyn uni_plugin::Plugin + Send + Sync>, String> {
            WasmLoader::new()
                .load_as_plugin(b"definitely not a wasm component", &[])
                .map_err(|e| e.to_string())
        }
    }

    let report = run_against_wasm(&GarbageLoader, Path::new("/dev/null"));
    assert!(!report.passed(), "garbage must not pass conformance");
    assert_eq!(report.checks.len(), 1, "expected a single load check");
    assert_eq!(report.checks[0].id, "wasm.load");
    assert!(!report.checks[0].passed);
}

/// Full suite against the real geo component. Soft-skips if the fixture is not
/// built so CI without the wasm toolchain stays green.
#[test]
fn geo_component_passes_conformance_suite() {
    if !Path::new(GEO_WASM).exists() {
        eprintln!(
            "skipping geo_component_passes_conformance_suite: fixture missing at {GEO_WASM}\n\
             build it with `./scripts/build-wasm-fixtures.sh` to exercise this test"
        );
        return;
    }

    let report = run_against_wasm(&PathLoader, Path::new(GEO_WASM));
    assert!(
        report.passed(),
        "geo component failed conformance: {report:?}"
    );

    // The wasm target must exercise the same probes as the live-Rust target —
    // i.e. it really reaches `run_against_plugin`, not a marker.
    let ids: Vec<&str> = report.checks.iter().map(|c| c.id.as_str()).collect();
    for expected in [
        "manifest.parse",
        "abi.in_range",
        "capabilities.declared",
        "registration.commit",
        "registration.idempotent",
    ] {
        assert!(
            ids.contains(&expected),
            "missing probe `{expected}` in {ids:?}"
        );
    }
}
