// Consolidated integration-test harness: all groups link into a single
// binary instead of one-binary-per-file, cutting link steps.
#![allow(dead_code, unused_imports)]

#[path = "it/declare_aggregate_unit.rs"]
mod declare_aggregate_unit;
#[path = "it/declared_by.rs"]
mod declared_by;
#[path = "it/declare_persistence.rs"]
mod declare_persistence;
#[path = "it/shadow_native.rs"]
mod shadow_native;
