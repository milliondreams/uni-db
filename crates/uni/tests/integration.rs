// Consolidated integration-test binary: every test group links into one binary
// to minimize compile/link time. Each group's sources live under tests/common/<group>/.
//
// `recursion_limit` is hoisted here (was a crate-level attr on the former `perf`
// shim); it applies harmlessly to the whole binary. The SSI suites are always
// compiled now (SSI is a runtime `UniConfig::ssi_enabled` toggle, default on);
// `ssi_default_semantics` opts out per-database to pin the legacy ssi-off path.
//
// NOTE: `reranker_integration` is intentionally NOT merged — CI builds it with
// `--no-default-features --features provider-onnx-dynamic`, a feature set under
// which the default-feature-requiring groups here would fail to compile. It
// remains a standalone binary (tests/reranker_integration.rs).
#![recursion_limit = "256"]
#![allow(dead_code, unused_imports, clippy::all)]

#[path = "common/algo/mod.rs"]
mod algo;
#[path = "common/bugs/mod.rs"]
mod bugs;
#[path = "common/crdt/mod.rs"]
mod crdt;
// Order-insensitive row-bag comparator shared by the metamorphic oracles.
#[path = "common/cypher_path/mod.rs"]
mod cypher_path;
#[path = "common/cypher_read/mod.rs"]
mod cypher_read;
#[path = "common/cypher_write/mod.rs"]
mod cypher_write;
#[path = "common/diff/mod.rs"]
mod diff;
#[path = "common/e2e/mod.rs"]
mod e2e;
#[path = "common/fork/mod.rs"]
mod fork;
// Metamorphic query-correctness oracles (G2 / Track B): query generator +
// renderer (`querygen`) and the TLP/NoREC oracles + seed (`metamorphic`), plus
// the shared order-insensitive row-bag comparator (`diff`) they depend on.
// (`gen` is a reserved keyword in edition 2024, hence `querygen`.)
#[path = "common/hybrid_localstack_e2e.rs"]
mod hybrid_localstack_e2e;
#[path = "common/index/mod.rs"]
mod index;
#[path = "common/l0_snapshot_e2e.rs"]
mod l0_snapshot_e2e;
#[path = "common/locy/mod.rs"]
mod locy;
#[path = "common/metamorphic/mod.rs"]
mod metamorphic;
#[path = "common/perf/mod.rs"]
mod perf;
#[path = "common/querygen/mod.rs"]
mod querygen;
#[path = "common/runtime/mod.rs"]
mod runtime;
#[path = "common/session_tx/mod.rs"]
mod session_tx;
// Shared infra for the SSI release-readiness suite (metrics capture, reopen
// harness, conflict assertions, invariant oracles). Must precede the modules
// that use it.
#[path = "common/sparse_resilience.rs"]
mod sparse_resilience;
#[path = "common/ssi_for_update.rs"]
mod ssi_for_update;
#[path = "common/ssi_hermitage.rs"]
mod ssi_hermitage;
#[path = "common/ssi_invariants.rs"]
mod ssi_invariants;
#[path = "common/ssi_l1_pinning.rs"]
mod ssi_l1_pinning;
#[path = "common/ssi_occ_e2e.rs"]
mod ssi_occ_e2e;
#[path = "common/ssi_read_path_matrix.rs"]
mod ssi_read_path_matrix;
#[path = "common/ssi_resilience.rs"]
mod ssi_resilience;
#[path = "common/ssi_stress.rs"]
mod ssi_stress;
#[path = "common/ssi_support/mod.rs"]
mod ssi_support;
#[path = "common/ssi_telemetry.rs"]
mod ssi_telemetry;
#[path = "common/ssi_write_set_matrix.rs"]
mod ssi_write_set_matrix;
// Backward-compat suite: opens databases with `ssi_enabled = false` to pin the
// last-writer-wins contract regardless of the global default (now SSI-on).
#[path = "common/ssi_default_semantics.rs"]
mod ssi_default_semantics;
#[path = "common/storage/mod.rs"]
mod storage;
#[path = "common/vector_search/mod.rs"]
mod vector_search;

// Folded-in former standalone test binaries (each was its own link step):
#[path = "common/auth/mod.rs"]
mod auth;
#[path = "common/connectors/mod.rs"]
mod connectors;
#[path = "common/graph_algo/mod.rs"]
mod graph_algo;
#[path = "common/hooks/mod.rs"]
mod hooks;
#[path = "common/loaders/mod.rs"]
mod loaders;
#[path = "common/plugin/mod.rs"]
mod plugin;
#[path = "common/reload/mod.rs"]
mod reload;
#[path = "common/triggers/mod.rs"]
mod triggers;
// Real test module (was tests/plugin_trust.rs), moved under common/ so it
// compiles into this binary instead of its own.
#[path = "common/plugin_trust.rs"]
mod plugin_trust;
