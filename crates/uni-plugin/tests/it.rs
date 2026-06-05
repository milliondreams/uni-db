// Consolidated integration-test harness: all groups link into a single
// binary instead of one-binary-per-file, cutting link steps.
#![allow(dead_code, unused_imports)]

#[path = "it/end_to_end.rs"]
mod end_to_end;
#[path = "it/reload_dispatch.rs"]
mod reload_dispatch;
