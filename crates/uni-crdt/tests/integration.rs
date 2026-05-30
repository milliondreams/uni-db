// Consolidated integration-test binary: every test group links into one binary
// to minimize compile/link time. Each group's sources live under tests/common/<group>/.
#[path = "common/crdt/mod.rs"]
mod crdt;
