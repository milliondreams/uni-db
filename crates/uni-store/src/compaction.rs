// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::time::{Duration, SystemTime};

/// Trigger strategy for a compaction run.
#[derive(Debug, Clone, Copy)]
pub enum CompactionTask {
    /// Compact when the number of L1 runs exceeds a threshold.
    ByRunCount,
    /// Compact when the total L1 size exceeds a byte threshold.
    BySize,
    /// Compact when the oldest L1 run exceeds an age threshold.
    ByAge,
}

/// Statistics produced by a single compaction run.
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    pub files_compacted: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub duration: Duration,
    pub crdt_merges: usize,
}

/// Snapshot of the current compaction state for observability.
#[derive(Debug, Clone, Default)]
pub struct CompactionStatus {
    pub l1_runs: usize,
    pub l1_size_bytes: u64,
    pub oldest_l1_age: Duration,
    pub compaction_in_progress: bool,
    pub compaction_pending: usize,
    pub last_compaction: Option<SystemTime>,
    pub total_compactions: u64,
    pub total_bytes_compacted: u64,
}
