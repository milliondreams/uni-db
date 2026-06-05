// Consolidated integration-test harness: all groups link into a single
// binary instead of one-binary-per-file, cutting link steps.
#![allow(dead_code, unused_imports)]

#[path = "it/example_extism_geo_e2e.rs"]
mod example_extism_geo_e2e;
#[path = "it/example_extism_net_e2e.rs"]
mod example_extism_net_e2e;
#[path = "it/host_svc.rs"]
mod host_svc;
#[path = "it/instantiate_with_minimal_wasm.rs"]
mod instantiate_with_minimal_wasm;
#[path = "it/load_e2e.rs"]
mod load_e2e;
