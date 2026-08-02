#![allow(clippy::cloned_ref_to_slice_refs)]

// Consolidated integration-test binary: every test group links into one binary
// to minimize compile/link time. Each group's sources live under tests/common/<group>/.
#[path = "common/executor/mod.rs"]
mod executor;
#[path = "common/functions/mod.rs"]
mod functions;
#[path = "common/integration/mod.rs"]
mod integration;
#[path = "common/parser/mod.rs"]
mod parser;
#[path = "common/planner/mod.rs"]
mod planner;

// --- consolidated from former standalone binaries (see docs/test_layout.md) ---
#[path = "correctness_repros.rs"]
mod correctness_repros;
#[path = "dispatch.rs"]
mod dispatch;
#[path = "vector_agg_return_type_repro.rs"]
mod vector_agg_return_type_repro;
