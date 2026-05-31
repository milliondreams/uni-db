// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A disk-backed database that survives close-and-reopen, for crash/recovery
//! tests.
//!
//! The harness owns a `TempDir` and opens a database rooted inside it. Because
//! the directory outlives any individual `Uni` handle, a test can:
//!
//! ```ignore
//! let h = DiskHarness::new()?;
//! {
//!     let db = h.open().await?;          // first boot
//!     // ... commit some transactions ...
//!     db.flush().await?;                 // durable
//! }                                      // db dropped == process "stops"
//! let db = h.open().await?;              // reboot: WAL replays from disk
//! // ... assert recovered state ...
//! ```
//!
//! Reopening the same path replays the WAL, exercising the real recovery path —
//! unlike in-memory tests, this validates the durability boundary end-to-end.

use anyhow::Result;
use tempfile::TempDir;
use uni_db::Uni;

/// Owns a temp directory and opens databases rooted at `<dir>/db`.
pub struct DiskHarness {
    dir: TempDir,
}

impl DiskHarness {
    /// Creates a fresh temp directory. The directory (and all data) is removed
    /// when the harness is dropped.
    pub fn new() -> Result<Self> {
        Ok(Self {
            dir: tempfile::tempdir()?,
        })
    }

    /// The database root URI (a local filesystem path under the temp dir).
    pub fn uri(&self) -> String {
        self.dir.path().join("db").to_string_lossy().into_owned()
    }

    /// Opens (or, on first call, creates) the database at this harness's path.
    /// Call repeatedly to simulate reboots; each reopen replays the WAL.
    pub async fn open(&self) -> Result<Uni> {
        Ok(Uni::open(self.uri()).build().await?)
    }
}
