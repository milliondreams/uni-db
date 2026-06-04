// Consolidated integration-test harness: all groups link into a single
// binary instead of one-binary-per-file, cutting link steps.
#![allow(dead_code, unused_imports)]

#[path = "it/host_services_e2e.rs"]
mod host_services_e2e;
#[path = "it/load_e2e.rs"]
mod load_e2e;
#[path = "it/resource_limits.rs"]
mod resource_limits;
#[path = "it/sandbox.rs"]
mod sandbox;
#[path = "it/vectorized.rs"]
mod vectorized;
