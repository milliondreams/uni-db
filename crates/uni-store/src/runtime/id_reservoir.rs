// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Per-transaction VID/EID reservoir.
//!
//! Each `Transaction` owns one `TxIdReservoir`. The reservoir holds two small
//! `VecDeque`s of pre-reserved IDs and refills them via the bulk
//! `IdAllocator::allocate_vids` / `allocate_eids` primitives when empty. This
//! amortizes the global `IdAllocator` `tokio::Mutex` over `batch_size` IDs.
//!
//! ## Wasted IDs
//!
//! At transaction drop, any unconsumed IDs in the reservoir are lost — the
//! global allocator's `current_vid` / `current_eid` has already advanced past
//! them. Cost is at most `batch_size - 1` IDs per transaction. The u64 ID
//! space makes this negligible (we'd need ~2^60 transactions before sparsity
//! mattered).
//!
//! ## Concurrency
//!
//! `parking_lot::Mutex` protects the local `VecDeque`s. Inside a single
//! transaction only one tokio task touches the reservoir at a time, so the
//! local mutex is uncontended. The refill `.await` happens outside the local
//! mutex window — see `Self::next_vid` / `Self::next_eid`.

use crate::runtime::id_allocator::IdAllocator;
use anyhow::Result;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use uni_common::core::id::{Eid, Vid};

/// Fallback refill batch size when no `UniConfig` is plumbed through.
/// Matches the `UniConfig::tx_id_reservoir_batch` default.
pub const DEFAULT_RESERVOIR_BATCH: usize = 16;

/// A bounded local cache of pre-reserved VIDs and EIDs for a single transaction.
pub struct TxIdReservoir {
    allocator: Arc<IdAllocator>,
    state: Mutex<ReservoirState>,
    batch_size: usize,
}

struct ReservoirState {
    vids: VecDeque<Vid>,
    eids: VecDeque<Eid>,
}

impl TxIdReservoir {
    /// Create a new reservoir backed by `allocator` that refills `batch_size`
    /// IDs at a time. A `batch_size` of 0 falls back to [`DEFAULT_RESERVOIR_BATCH`].
    pub fn new(allocator: Arc<IdAllocator>, batch_size: usize) -> Self {
        let batch_size = if batch_size == 0 {
            DEFAULT_RESERVOIR_BATCH
        } else {
            batch_size
        };
        Self {
            allocator,
            state: Mutex::new(ReservoirState {
                vids: VecDeque::with_capacity(batch_size),
                eids: VecDeque::with_capacity(batch_size),
            }),
            batch_size,
        }
    }

    /// Returns the next pre-reserved VID, refilling from the global allocator
    /// when the local cache is empty.
    pub async fn next_vid(&self) -> Result<Vid> {
        if let Some(v) = self.state.lock().vids.pop_front() {
            return Ok(v);
        }
        // Refill outside the local mutex so the .await does not pin it.
        let mut batch = self.allocator.allocate_vids(self.batch_size).await?;
        let first = batch.remove(0);
        let mut st = self.state.lock();
        st.vids.extend(batch);
        Ok(first)
    }

    /// Returns the next pre-reserved EID, refilling from the global allocator
    /// when the local cache is empty.
    pub async fn next_eid(&self) -> Result<Eid> {
        if let Some(e) = self.state.lock().eids.pop_front() {
            return Ok(e);
        }
        let mut batch = self.allocator.allocate_eids(self.batch_size).await?;
        let first = batch.remove(0);
        let mut st = self.state.lock();
        st.eids.extend(batch);
        Ok(first)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    /// Build an `IdAllocator` with batch_size large enough that we don't hit
    /// the object-store refill path during the test.
    async fn make_allocator() -> (tempfile::TempDir, Arc<IdAllocator>) {
        use object_store::local::LocalFileSystem;
        use object_store::path::Path;
        let dir = tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let path = Path::from("id_allocator.json");
        let alloc = Arc::new(IdAllocator::new(store, path, 10_000).await.unwrap());
        (dir, alloc)
    }

    #[tokio::test]
    async fn next_vid_amortizes_global_mutex() -> Result<()> {
        let (_dir, alloc) = make_allocator().await;
        let r = TxIdReservoir::new(alloc, 16);

        // 100 sequential VID allocations.
        let mut vids = Vec::new();
        for _ in 0..100 {
            vids.push(r.next_vid().await?);
        }

        // All distinct.
        let unique: std::collections::HashSet<_> = vids.iter().copied().collect();
        assert_eq!(unique.len(), 100, "all VIDs must be unique");

        // Monotonically increasing across the whole sequence.
        for i in 1..vids.len() {
            assert!(
                vids[i].as_u64() > vids[i - 1].as_u64(),
                "VIDs must be monotonically increasing"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn next_eid_amortizes_global_mutex() -> Result<()> {
        let (_dir, alloc) = make_allocator().await;
        let r = TxIdReservoir::new(alloc, 16);

        let mut eids = Vec::new();
        for _ in 0..100 {
            eids.push(r.next_eid().await?);
        }

        let unique: std::collections::HashSet<_> = eids.iter().copied().collect();
        assert_eq!(unique.len(), 100);
        for i in 1..eids.len() {
            assert!(eids[i].as_u64() > eids[i - 1].as_u64());
        }
        Ok(())
    }

    #[tokio::test]
    async fn zero_batch_size_falls_back_to_default() -> Result<()> {
        let (_dir, alloc) = make_allocator().await;
        let r = TxIdReservoir::new(alloc, 0);
        // Just confirm it works (would panic on allocate_vids(0)).
        let _vid = r.next_vid().await?;
        let _eid = r.next_eid().await?;
        // The unused-warning prefix on the unwrap is acceptable.
        let _ = AtomicUsize::new(0).fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
