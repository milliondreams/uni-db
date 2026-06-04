// Consolidated integration-test harness: all groups link into a single
// binary instead of one-binary-per-file, cutting link steps.
#![allow(dead_code, unused_imports)]

#[path = "it/conformance_wasm.rs"]
mod conformance_wasm;
#[path = "it/example_wasm_geo_e2e.rs"]
mod example_wasm_geo_e2e;
#[path = "it/example_wasm_net_e2e.rs"]
mod example_wasm_net_e2e;
#[path = "it/instantiate_minimal_component.rs"]
mod instantiate_minimal_component;
