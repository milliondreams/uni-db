// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Shared infrastructure for the SSI/OCC release-readiness suite.
//!
//! These helpers are the substrate the correctness / resilience / stress tests
//! build on. They are intentionally small and dependency-light:
//!
//! - [`metrics`] — install a process-global recorder and read back the SSI
//!   telemetry counters (`uni_ssi_*`, `uni_l0_snapshot_freezes_total`) so a test
//!   can assert *exactly* how many conflicts / freezes / retries occurred.
//! - [`schedule`] — conflict-assertion predicates plus a barrier for the rare
//!   tests that need genuinely blocking concurrency (FOR UPDATE). Most anomaly
//!   tests need no barrier: two `Transaction` handles interleaved by awaiting
//!   their operations in a chosen order is already a deterministic schedule, and
//!   the only contention point (`flush_lock` at commit) is reached one tx at a
//!   time.
//! - [`reopen`] — a disk-backed database that can be closed and reopened from
//!   the same path, replaying the WAL, to test crash/recovery end-to-end.
//! - [`oracle`] — closed-form serializability witnesses (counter-sum,
//!   conserved-balance, non-negative) for the invariant-oracle stress tests.
//!
//! Gated behind `ssi`; registered once from `tests/integration.rs`.

#![allow(dead_code)] // each test file uses a different subset of helpers

pub mod metrics;
pub mod oracle;
pub mod reopen;
pub mod schedule;
