// Consolidated integration-test binary: every test group links into one binary
// to minimize compile/link time. Each group's sources live under tests/common/<group>/.
//
// Feature-gated groups are gated here at the module level (previously each was a
// separate binary carrying a crate-level `#![cfg(...)]`):
//   - storage / fork_recovery require `lance-backend`
//   - ssi_occ_test requires `ssi`
#[path = "common/bugs/mod.rs"]
mod bugs;
#[path = "common/cloud/mod.rs"]
mod cloud;
#[path = "common/cloud_integration_test.rs"]
mod cloud_integration_test;
#[path = "common/crdt/mod.rs"]
mod crdt;
#[cfg(feature = "lance-backend")]
#[path = "common/fork_recovery/mod.rs"]
mod fork_recovery;
#[path = "common/property/mod.rs"]
mod property;
#[cfg(feature = "ssi")]
#[path = "common/ssi_occ_test.rs"]
mod ssi_occ_test;
#[cfg(feature = "lance-backend")]
#[path = "common/storage/mod.rs"]
mod storage;
