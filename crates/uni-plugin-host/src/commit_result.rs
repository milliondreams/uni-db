// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Commit-result value types, shared by the API crate and the plugin-host
//! hooks engine. Pure data — no `Uni` coupling.

use std::time::Duration;

/// Result of committing a transaction.
#[derive(Debug, Default)]
pub struct CommitResult {
    /// Number of mutations committed.
    pub mutations_committed: usize,
    /// Number of rules promoted to the parent session.
    pub rules_promoted: usize,
    /// Database version after commit.
    pub version: u64,
    /// Database version when the transaction was created.
    pub started_at_version: u64,
    /// WAL log sequence number of the commit (0 when no WAL is configured).
    pub wal_lsn: u64,
    /// Duration of the commit operation (lock + WAL + merge).
    pub duration: Duration,
    /// Errors encountered during rule promotion (best-effort).
    pub rule_promotion_errors: Vec<RulePromotionError>,
}

impl CommitResult {
    /// Number of versions that committed between tx start and commit.
    /// 0 means no concurrent commits occurred.
    pub fn version_gap(&self) -> u64 {
        self.version.saturating_sub(self.started_at_version + 1)
    }
}

/// Error encountered during rule promotion at commit time.
#[derive(Debug, Clone)]
pub struct RulePromotionError {
    pub rule_text: String,
    pub error: String,
}
