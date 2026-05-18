// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Async-flush coordination.
//!
//! Bounds the number of in-flight L0→L1 flushes (via a semaphore),
//! assigns rotate-order sequence numbers, and serializes finalize so
//! the manifest parent-chain stays consistent (proposal
//! `docs/proposals/async_l0_to_l1_flush.md` §5.3).
//!
//! ## Architecture
//!
//! ```text
//! Writer
//!   ├── flush_lock              (brief: rotate + finalize)
//!   └── flush_coordinator
//!         ├── permits: Semaphore(max_pending_flushes)
//!         ├── next_seq: AtomicU64
//!         └── submit_tx → finalizer task
//!                          └─ mpsc<FlushSubmit>
//!                          └─ BinaryHeap reorder by seq
//! ```

use crate::runtime::wal::WriteAheadLog;
use crate::storage::manager::StorageManager;
use parking_lot::RwLock as PlRwLock;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Semaphore, mpsc, oneshot};
use uni_common::core::snapshot::SnapshotManifest;

/// Result of a rotate phase: the snapshot of state needed to stream and
/// finalize. Send + 'static so it can travel into a spawned task.
pub struct RotatedFlush {
    pub seq: u64,
    pub old_l0_arc: Arc<PlRwLock<crate::runtime::l0::L0Buffer>>,
    pub wal_lsn: u64,
    pub name: Option<String>,
    /// Snapshot of `cached_manifest` taken at rotate time. Stream uses this
    /// as a tentative parent; finalize may rewrite it if predecessors
    /// finalized in between.
    pub parent_manifest: Option<SnapshotManifest>,
    /// Permit holding the back-pressure slot. Released on finalize drop.
    pub permit: tokio::sync::OwnedSemaphorePermit,
}

/// Result of a stream phase: the manifest to publish.
pub struct FlushOutcome {
    pub new_manifest: SnapshotManifest,
    pub snapshot_id: String,
}

/// Carried across the spawn boundary so a finalize step can run without
/// touching `Writer` (which is `&self` and lives in the caller).
#[derive(Clone)]
pub struct SharedFlushCtx {
    pub storage: Arc<StorageManager>,
    pub l0_manager: Arc<crate::runtime::l0_manager::L0Manager>,
    pub adjacency_manager: Arc<crate::storage::adjacency_manager::AdjacencyManager>,
    pub property_manager: Option<Arc<crate::runtime::property_manager::PropertyManager>>,
    pub cached_manifest: Arc<parking_lot::Mutex<Option<SnapshotManifest>>>,
    pub last_flush_time: Arc<parking_lot::Mutex<std::time::Instant>>,
    pub fork_id: Option<uni_common::core::fork::ForkId>,
    pub fork_flush_count: Arc<std::sync::atomic::AtomicU64>,
    pub fork_fragment_warn_fired: Arc<std::sync::atomic::AtomicBool>,
    pub fork_fragment_warn_threshold: usize,
}

/// A submission to the ordered finalizer.
struct FlushSubmit {
    seq: u64,
    rotated: RotatedFlush,
    result: anyhow::Result<FlushOutcome>,
    /// Optional notification when finalize completes (for `FlushTicket`).
    ack: Option<oneshot::Sender<anyhow::Result<String>>>,
}

/// User-facing handle to wait on an async-flush completion (proposal §5.6).
pub struct FlushTicket {
    /// `None` means the flush completed inline (sync path).
    rx: Option<oneshot::Receiver<anyhow::Result<String>>>,
}

impl FlushTicket {
    pub fn ready(snapshot_id: anyhow::Result<String>) -> Self {
        // For sync paths: pre-resolved.
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(snapshot_id);
        Self { rx: Some(rx) }
    }

    pub fn pending(rx: oneshot::Receiver<anyhow::Result<String>>) -> Self {
        Self { rx: Some(rx) }
    }

    /// Wait for the flush to finalize. Returns the snapshot id on success.
    pub async fn await_finalize(self) -> anyhow::Result<String> {
        match self.rx {
            Some(rx) => rx.await.unwrap_or_else(|_| {
                Err(anyhow::anyhow!(
                    "flush ticket dropped before completion"
                ))
            }),
            None => Err(anyhow::anyhow!("flush ticket has no completion channel")),
        }
    }
}

pub struct FlushCoordinator {
    permits: Arc<Semaphore>,
    next_seq: AtomicU64,
    submit_tx: mpsc::UnboundedSender<FlushSubmit>,
    /// Counter exposed for `drop_fork` to wait on. Incremented at rotate,
    /// decremented after finalize.
    pending_count: Arc<std::sync::atomic::AtomicUsize>,
    drain_notify: Arc<tokio::sync::Notify>,
    max_pending_flushes: usize,
}

impl FlushCoordinator {
    pub fn new(
        max_pending_flushes: usize,
        shared: SharedFlushCtx,
        finalize_fn: Arc<dyn FinalizeFn>,
    ) -> Self {
        let permits = Arc::new(Semaphore::new(max_pending_flushes.max(1)));
        let next_seq = AtomicU64::new(0);
        let (submit_tx, submit_rx) = mpsc::unbounded_channel::<FlushSubmit>();
        let pending_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let drain_notify = Arc::new(tokio::sync::Notify::new());

        let pending_count_for_task = pending_count.clone();
        let drain_notify_for_task = drain_notify.clone();
        tokio::spawn(finalizer_loop(
            submit_rx,
            shared,
            finalize_fn,
            pending_count_for_task,
            drain_notify_for_task,
        ));

        Self {
            permits,
            next_seq,
            submit_tx,
            pending_count,
            drain_notify,
            max_pending_flushes,
        }
    }

    pub fn max_pending_flushes(&self) -> usize {
        self.max_pending_flushes
    }

    pub async fn acquire_permit(&self) -> anyhow::Result<tokio::sync::OwnedSemaphorePermit> {
        self.permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("flush coordinator permit semaphore closed"))
    }

    pub fn next_rotate_seq(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::AcqRel)
    }

    pub fn note_pending(&self) {
        self.pending_count.fetch_add(1, Ordering::AcqRel);
    }

    pub fn pending_flush_count(&self) -> usize {
        self.pending_count.load(Ordering::Acquire)
    }

    /// Submit a completed-stream flush for ordered finalization.
    pub fn submit(
        &self,
        seq: u64,
        rotated: RotatedFlush,
        result: anyhow::Result<FlushOutcome>,
        ack: Option<oneshot::Sender<anyhow::Result<String>>>,
    ) {
        let _ = self.submit_tx.send(FlushSubmit {
            seq,
            rotated,
            result,
            ack,
        });
    }

    /// Wait until pending_count drops to zero. Used by `drop_fork`.
    pub async fn drain(&self, timeout: std::time::Duration) -> Result<(), &'static str> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.pending_flush_count() == 0 {
                return Ok(());
            }
            let notified = self.drain_notify.notified();
            tokio::select! {
                _ = notified => continue,
                _ = tokio::time::sleep_until(deadline) => {
                    return if self.pending_flush_count() == 0 {
                        Ok(())
                    } else {
                        Err("pending flushes did not drain before deadline")
                    };
                }
            }
        }
    }
}

/// Closure run by the finalizer task. Captures the parts of Writer that
/// finalize touches; runs without holding any Writer reference.
///
/// `Writer::flush_finalize_now` implements this and is bound to the
/// concrete WAL/storage state.
pub trait FinalizeFn: Send + Sync {
    fn finalize<'a>(
        &'a self,
        rotated: RotatedFlush,
        outcome: FlushOutcome,
        shared: SharedFlushCtx,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send + 'a>,
    >;

    fn finalize_failure<'a>(
        &'a self,
        rotated: RotatedFlush,
        err: anyhow::Error,
        shared: SharedFlushCtx,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Error> + Send + 'a>,
    >;
}

async fn finalizer_loop(
    mut submit_rx: mpsc::UnboundedReceiver<FlushSubmit>,
    shared: SharedFlushCtx,
    finalize_fn: Arc<dyn FinalizeFn>,
    pending_count: Arc<std::sync::atomic::AtomicUsize>,
    drain_notify: Arc<tokio::sync::Notify>,
) {
    // Reorder-by-seq using a min-heap; finalize strictly in seq order.
    let mut pending: BinaryHeap<Reverse<(u64, FlushSubmit)>> = BinaryHeap::new();
    let mut expected: u64 = 0;
    while let Some(submit) = submit_rx.recv().await {
        pending.push(Reverse((submit.seq, submit)));
        while let Some(Reverse((seq, _))) = pending.peek() {
            if *seq != expected {
                break;
            }
            let Reverse((_, s)) = pending.pop().unwrap();
            let FlushSubmit {
                seq: _,
                rotated,
                result,
                ack,
            } = s;
            let ack_result = match result {
                Ok(outcome) => finalize_fn.finalize(rotated, outcome, shared.clone()).await,
                Err(e) => {
                    let _err = finalize_fn
                        .finalize_failure(rotated, e, shared.clone())
                        .await;
                    Err(anyhow::anyhow!("flush stream failed: {}", _err))
                }
            };
            if let Some(ack) = ack {
                let _ = ack.send(ack_result);
            }
            pending_count.fetch_sub(1, Ordering::AcqRel);
            drain_notify.notify_waiters();
            expected += 1;
        }
    }
}

// We need a wrapper allowing FlushSubmit to be ordered by seq for the heap.
// Default Ord on tuples uses the first element so (u64, FlushSubmit) needs
// FlushSubmit to be Ord/PartialOrd. We don't actually compare FlushSubmits;
// the seq is at position 0 of the tuple and the heap is keyed off it. To
// avoid trait headaches we wrap manually:
impl PartialEq for FlushSubmit {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq
    }
}
impl Eq for FlushSubmit {}
impl PartialOrd for FlushSubmit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for FlushSubmit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.seq.cmp(&other.seq)
    }
}

// Silence unused-import warnings for items only used during full implementation:
#[allow(dead_code)]
fn _unused_wal_marker() -> Option<Arc<WriteAheadLog>> {
    None
}
