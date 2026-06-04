// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Background fork-maintenance tasks: the TTL sweeper (Phase 4a) and the
//! fork-local index builder (Phase 5a-impl Step 7).
//!
//! Both tasks own a generic *scheduling skeleton* here — an interval ticker
//! with `MissedTickBehavior::Skip` plus a `ShutdownHandle::subscribe()`-driven
//! `tokio::select!` loop — and delegate every per-tick action that touches
//! `UniInner` to a [`ForkMaintenanceHost`]. uni-db implements the host over a
//! `Weak<UniInner>`, so the tasks do not extend the database's lifetime.
//!
//! ## Why a tick-body trait rather than field accessors
//!
//! The original tasks walked `UniInner.fork_inners` (a
//! `DashMap<ForkId, Weak<UniInner>>`) and upgraded each entry to read its
//! storage/writer. Exposing those handles across the crate boundary would pull
//! `UniInner` into uni-fork and create a dependency cycle. Instead the host
//! exposes the two tick bodies (`sweep_expired_forks`,
//! `build_fork_local_indexes`) as async methods; the reusable
//! scheduling/shutdown machinery lives here.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tracing::debug;

/// Host hook for the background fork-maintenance tasks.
///
/// Implemented by uni-db (over a `Weak<UniInner>` re-wrapped per tick). Each
/// method is one full tick body; uni-fork owns only the interval/shutdown loop.
#[async_trait::async_trait]
pub trait ForkMaintenanceHost: Send + Sync + 'static {
    /// One TTL-sweeper tick: drop every fork whose TTL has expired.
    async fn sweep_expired_forks(&self);

    /// One index-builder tick: build fork-local scalar indexes for any
    /// (fork, label, column) whose fragment count crosses `threshold`.
    async fn build_fork_local_indexes(&self, threshold: u64);
}

/// Spawn an interval-driven background task that runs `tick_fn` every
/// `interval` and exits on the shutdown broadcast.
///
/// The ticker uses `MissedTickBehavior::Skip` so a long tick body doesn't
/// trigger a thundering catch-up burst on the next tick. `task_label` names
/// the task in shutdown-debug logs. Returns `None` (no task) when `disable`
/// is set.
fn spawn_ticker<F, Fut>(
    interval: Duration,
    disable: bool,
    task_label: &'static str,
    mut shutdown_rx: broadcast::Receiver<()>,
    mut tick_fn: F,
) -> Option<tokio::task::JoinHandle<()>>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    if disable {
        debug!("{task_label} disabled by config");
        return None;
    }
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    tick_fn().await;
                }
                _ = shutdown_rx.recv() => {
                    debug!("{task_label} received shutdown signal");
                    break;
                }
            }
        }
    });
    Some(handle)
}

/// Spawn the TTL sweeper task. Returns `None` (no task) when `disable` is set.
///
/// Holds the host (typically backed by a `Weak<UniInner>`) so the sweeper does
/// not extend the database's lifetime.
pub fn spawn_sweeper<H: ForkMaintenanceHost>(
    host: Arc<H>,
    interval: Duration,
    disable: bool,
    shutdown_rx: broadcast::Receiver<()>,
) -> Option<tokio::task::JoinHandle<()>> {
    spawn_ticker(interval, disable, "fork sweeper", shutdown_rx, move || {
        let host = Arc::clone(&host);
        async move { host.sweep_expired_forks().await }
    })
}

/// Spawn the fork-local index builder task. Returns `None` when `disable` is set.
pub fn spawn_index_builder<H: ForkMaintenanceHost>(
    host: Arc<H>,
    interval: Duration,
    threshold: u64,
    disable: bool,
    shutdown_rx: broadcast::Receiver<()>,
) -> Option<tokio::task::JoinHandle<()>> {
    spawn_ticker(
        interval,
        disable,
        "fork index builder",
        shutdown_rx,
        move || {
            let host = Arc::clone(&host);
            async move { host.build_fork_local_indexes(threshold).await }
        },
    )
}
