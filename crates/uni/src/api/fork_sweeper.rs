// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Background TTL sweeper for forks (Phase 4a).
//!
//! Polls the fork registry at `UniConfig::fork_sweeper_interval`, and
//! for every fork whose `ttl_expires_at` is past `Utc::now()`, runs
//! `Uni::drop_fork_cascade(name)`. Errors are logged-and-continued —
//! the next tick retries. Honors `ShutdownHandle::subscribe()` so
//! `Uni::shutdown` cleanly stops the loop.
//!
//! Disabled when `UniConfig::disable_fork_sweeper` is true (tests that
//! want deterministic control over fork lifetimes opt out).

// Rust guideline compliant

use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::broadcast;
use tracing::{debug, warn};

use super::{Uni, UniInner};

/// Spawn the sweeper task. Returns immediately when
/// `disable_fork_sweeper` is set.
///
/// Holds a `Weak<UniInner>` so the sweeper does not extend the
/// database's lifetime. Each tick attempts to upgrade; if the upgrade
/// fails the database has been dropped and the sweeper exits.
pub(crate) fn spawn(
    inner: Arc<UniInner>,
    interval: Duration,
    disable: bool,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Option<tokio::task::JoinHandle<()>> {
    if disable {
        debug!("fork sweeper disabled by config");
        return None;
    }
    let weak = Arc::downgrade(&inner);
    drop(inner); // don't accidentally extend lifetime through the spawn capture
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // `MissedTickBehavior::Skip` so a long cascade doesn't trigger
        // a thundering catch-up burst on the next tick.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = tick_once(&weak).await {
                        warn!("fork sweeper tick failed: {e}");
                    }
                }
                _ = shutdown_rx.recv() => {
                    debug!("fork sweeper received shutdown signal");
                    break;
                }
            }
        }
    });
    Some(handle)
}

async fn tick_once(weak: &Weak<UniInner>) -> anyhow::Result<()> {
    let Some(inner) = weak.upgrade() else {
        // Database dropped; let the next tick observe the same and
        // shut down via the broadcast channel instead of looping.
        return Ok(());
    };
    let now = chrono::Utc::now();
    let expired = inner.fork_registry.list_expired(now).await;
    if expired.is_empty() {
        return Ok(());
    }
    // Re-wrap into a Uni handle so we can call the public cascade API.
    // `Uni { inner }` is a thin newtype; this does not duplicate state.
    let db = Uni { inner };
    for fork in expired {
        match db.drop_fork_cascade(&fork.name).await {
            Ok(()) => {
                debug!(fork = %fork.name, "swept expired fork");
            }
            Err(e) => {
                warn!(
                    fork = %fork.name,
                    "sweeper failed to drop expired fork (will retry next tick): {e}"
                );
            }
        }
    }
    Ok(())
}
