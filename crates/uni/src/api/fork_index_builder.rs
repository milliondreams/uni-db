// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Background fork-local index builder (Phase 5a-impl Step 7).
//!
//! Polls the fork registry at `UniConfig::fork_index_builder_interval`,
//! and for every active fork whose per-table fragment count crosses
//! `UniConfig::fork_index_build_threshold`, schedules a fork-local
//! `ScalarBtree` build for any column primary has indexed.
//!
//! Phase 5a-impl scope:
//! - Auto-builds `ScalarBtree` only. `VidUid` is a planner marker
//!   (no Lance file to write); the manual `Session::build_fork_local_index`
//!   API is the natural way to opt in. `Sorted` is reserved for an
//!   explicit register call too — most users want to control which
//!   columns get sorted indexes.
//! - Build errors are logged and swallowed; the next tick retries.
//! - Honors `ShutdownHandle::subscribe()` so `Uni::shutdown` cleanly
//!   terminates the loop.
//! - Honors `UniConfig::disable_fork_index_builder` (default `false`).
//!
//! Mirrors the `fork_sweeper` pattern from Phase 4a — same `Weak<UniInner>`
//! capture, same `MissedTickBehavior::Skip`, same shutdown handling.

// Rust guideline compliant

use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::broadcast;
use tracing::{debug, warn};
use uni_common::core::schema::IndexDefinition;
use uni_store::fork::ForkLocalIndexKind;

use super::UniInner;

/// Spawn the background builder task. Returns immediately when
/// `disable_fork_index_builder` is set.
pub(crate) fn spawn(
    inner: Arc<UniInner>,
    interval: Duration,
    threshold: u64,
    disable: bool,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Option<tokio::task::JoinHandle<()>> {
    if disable {
        debug!("fork index builder disabled by config");
        return None;
    }
    let weak = Arc::downgrade(&inner);
    drop(inner);
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = tick_once(&weak, threshold).await {
                        warn!("fork index builder tick failed: {e}");
                    }
                }
                _ = shutdown_rx.recv() => {
                    debug!("fork index builder received shutdown signal");
                    break;
                }
            }
        }
    });
    Some(handle)
}

async fn tick_once(weak: &Weak<UniInner>, threshold: u64) -> anyhow::Result<()> {
    let Some(inner) = weak.upgrade() else {
        return Ok(());
    };
    let active = inner.fork_registry.list_active().await;
    if active.is_empty() {
        return Ok(());
    }

    // Snapshot which (label, column) pairs primary has scalar indexes
    // on. The schema is shared across forks, so we read it once per
    // tick rather than per-fork.
    let scalar_index_columns: Vec<(String, String)> = inner
        .schema
        .schema()
        .indexes
        .iter()
        .filter_map(|idx| match idx {
            IndexDefinition::Scalar(cfg) if cfg.properties.len() == 1 => {
                Some((cfg.label.clone(), cfg.properties[0].clone()))
            }
            _ => None,
        })
        .collect();
    if scalar_index_columns.is_empty() {
        return Ok(());
    }

    for fork_info in active {
        // Only fire builds against forks that have a live UniInner —
        // the build path runs through the fork's storage manager.
        let Some(fork_inner_weak) = inner
            .fork_inners
            .get(&fork_info.id)
            .map(|e| e.value().clone())
        else {
            continue;
        };
        let Some(fork_inner) = fork_inner_weak.upgrade() else {
            continue;
        };
        let Some(scope) = fork_inner.storage.fork_scope().cloned() else {
            warn!(
                fork = %fork_info.name,
                "fork inner cache hit but storage has no fork scope; skipping",
            );
            continue;
        };
        // Optional flush so the build sees the latest committed rows.
        // Errors here are non-fatal — the build will use whatever's
        // already on the branch and the next tick can retry.
        if let Some(writer_lock) = &fork_inner.writer {
            let writer: &uni_store::Writer = writer_lock.as_ref();
            if let Err(e) = writer.flush_to_l1(None).await {
                warn!(
                    fork = %fork_info.name,
                    "pre-build flush failed; continuing with current branch tip: {e}"
                );
            }
        }
        let base_uri = fork_inner.storage.base_uri().to_string();
        for (label, column) in &scalar_index_columns {
            let dataset_name = format!("vertices_{label}");
            let count = scope.fragment_count(&dataset_name);
            if count < threshold {
                continue;
            }
            if scope.fork_local_index(label, column).is_some() {
                continue;
            }
            match uni_store::fork::index_builder::build_fork_local_index(
                &scope,
                &base_uri,
                label,
                column,
                ForkLocalIndexKind::ScalarBtree,
            )
            .await
            {
                Ok(()) => {
                    debug!(
                        fork = %fork_info.name,
                        label = %label,
                        column = %column,
                        "auto-built fork-local scalar index"
                    );
                }
                Err(e) => {
                    warn!(
                        fork = %fork_info.name,
                        label = %label,
                        column = %column,
                        "fork-local scalar index build failed (will retry next tick): {e}"
                    );
                }
            }
        }
    }
    Ok(())
}
