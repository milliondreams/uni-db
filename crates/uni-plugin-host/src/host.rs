// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Host callback surface.
//!
//! Some plugin-host engines (meta-plugin persistence, the background-job
//! scheduler) need to run write-mode Cypher against the live database. Rather
//! than reach back into the `uni-db` `Uni` internals (which would invert the
//! crate dependency), they hold an [`HostCypherExecutor`] trait object that the
//! `uni-db` API crate implements over its `UniInner` (open `Session` → `tx()`
//! → `execute()` → `commit()`).

/// Host-provided write-mode Cypher executor.
///
/// Implemented by the `uni-db` API crate. Best-effort callers (the persistence
/// mirror) log and swallow the `Err`; the scheduler maps it to a plugin
/// `FnError`. The current-thread-runtime guard / `block_in_place` handling
/// lives in the host's implementation, not here.
pub trait HostCypherExecutor: Send + Sync + std::fmt::Debug {
    /// Execute a write-mode Cypher statement to commit.
    fn execute_write_cypher(&self, cypher: &str) -> Result<(), String>;
}
